from __future__ import annotations

import sys

import pytest

from cube_web.jobs import partition_batch_job


def test_partition_batch_job_uses_opengauss_domain_store(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class Store:
        def get_attempt(self, task_id):
            assert task_id == "partition-task"
            return {
                "payload": {
                    "batch_id": "batch-1",
                    "grid_type": "geohash",
                    "requested_grid_level": 5,
                    "partition_method": "logical",
                    "datasets": [],
                }
            }

        def start_attempt(self, task_id):
            assert task_id == "partition-task"
            return True

    class Workflow:
        def __init__(self, _service, *, store, domain_store, runner):
            captured.update(store=store, domain_store=domain_store, runner=runner)

        def run(self, *, task_id, request):
            assert task_id == "partition-task"
            assert request.batch_id == "batch-1"
            return {"status": "completed"}

        def on_task_succeeded(self, task_id, result):
            captured["succeeded"] = (task_id, result)

    class SceneRepository:
        def __init__(self, dsn):
            captured["scene_dsn"] = dsn

        def update_partition_task(self, task_id, status, result):
            captured.setdefault("scene_updates", []).append((task_id, status, result))

    sentinel_domain_store = object()
    monkeypatch.setattr(partition_batch_job, "get_partition_job_store", lambda: Store())
    monkeypatch.setattr(partition_batch_job, "OpenGaussPartitionDomainStore", lambda *, dsn: captured.setdefault("dsn", dsn) and sentinel_domain_store)
    monkeypatch.setattr(partition_batch_job.runtime_config, "require_postgres_dsn", lambda: "postgresql://test")
    monkeypatch.setattr(partition_batch_job, "PartitionWorkflowService", Workflow)
    monkeypatch.setattr(partition_batch_job, "OpenGaussSceneRepository", SceneRepository)
    monkeypatch.setattr(
        partition_batch_job,
        "StrictPartitionRequest",
        type("Request", (), {"model_validate": staticmethod(lambda _payload: type("Value", (), {"batch_id": "batch-1"})())}),
    )
    monkeypatch.setattr(sys, "argv", ["partition_batch_job", "--task-id", "partition-task"])

    partition_batch_job.main()

    assert captured["dsn"] == "postgresql://test"
    assert captured["domain_store"] is sentinel_domain_store
    succeeded_task_id, succeeded_result = captured["succeeded"]
    assert succeeded_task_id == "partition-task"
    assert succeeded_result["status"] == "completed"
    assert succeeded_result["timings"]["job_driver"]["scope"] == "ray_job_driver"
    assert captured["scene_updates"] == [
        ("partition-task", "running", None),
        ("partition-task", "completed", succeeded_result),
    ]


def test_partition_batch_job_marks_driver_setup_failure_failed(monkeypatch) -> None:
    captured = {"scene_updates": []}

    class Store:
        def get_attempt(self, task_id):
            return {"task_id": task_id, "payload": {"batch_id": "batch-1"}}

        def start_attempt(self, _task_id):
            return True

        def fail_attempt(self, task_id, error, *, manual_required=False, error_type=None):
            captured["failed_attempt"] = (task_id, error, manual_required, error_type)

    class SceneRepository:
        def __init__(self, _dsn):
            pass

        def update_partition_task(self, task_id, status, result):
            captured["scene_updates"].append((task_id, status, result))

    monkeypatch.setattr(partition_batch_job, "get_partition_job_store", lambda: Store())
    monkeypatch.setattr(partition_batch_job, "OpenGaussSceneRepository", SceneRepository)
    monkeypatch.setattr(partition_batch_job.runtime_config, "require_postgres_dsn", lambda: "postgresql://test")
    monkeypatch.setattr(
        partition_batch_job.StrictPartitionRequest,
        "model_validate",
        staticmethod(lambda _payload: (_ for _ in ()).throw(ValueError("invalid payload"))),
    )
    monkeypatch.setattr(sys, "argv", ["partition_batch_job", "--task-id", "partition-task"])

    with pytest.raises(ValueError, match="invalid payload"):
        partition_batch_job.main()

    assert captured["failed_attempt"] == (
        "partition-task", "invalid payload", True, "ray_job_driver_failed",
    )
    assert captured["scene_updates"] == [
        ("partition-task", "running", None),
        ("partition-task", "failed", {"error": "invalid payload"}),
    ]
