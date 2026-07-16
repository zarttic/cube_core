from __future__ import annotations

import time
from copy import deepcopy
from typing import Any
from unittest.mock import Mock

import pytest

from cube_web.services.partition_contracts import StrictPartitionRequest, make_output_version
from cube_web.services.partition_job_store import InMemoryPartitionJobStore, PartitionBatchAlreadyActiveError
from cube_web.services.partition_service import PartitionBackend, PartitionService, PartitionTask
from cube_web.services.partition_workflow import PartitionWorkflowService


def _request() -> StrictPartitionRequest:
    return StrictPartitionRequest.model_validate(
        {
            "batch_id": "batch-01",
            "grid_type": "geohash",
            "requested_grid_level": 7,
            "partition_method": "logical",
            "datasets": [
                _dataset("dataset-ok"),
            ],
        }
    )


def _request_with_datasets(*dataset_ids: str) -> StrictPartitionRequest:
    payload = _request().model_dump(mode="json")
    payload["datasets"] = [_dataset(dataset_id) for dataset_id in dataset_ids]
    return StrictPartitionRequest.model_validate(payload)


def _dataset(dataset_id: str, data_type: str = "optical") -> dict[str, Any]:
    carbon = data_type == "carbon"
    return {
        "dataset_id": dataset_id,
        "dataset_code": dataset_id.upper(),
        "dataset_title": dataset_id,
        "data_type": data_type,
        "assets": [
            {
                "source_asset_id": f"asset-{dataset_id}",
                **(
                    {
                        "source_uri": f"s3://cube/cube/source/carbon/{dataset_id}.nc4",
                        "source_kind": "raw",
                        "source_format": "netcdf",
                    }
                    if carbon
                    else {"cog_uri": f"s3://cube/loader/{dataset_id}.tif"}
                ),
                "checksum": "a" * 64,
                "bbox": [100.0, 20.0, 101.0, 21.0],
                "crs": "EPSG:4326",
                "time_start": "2026-07-01T00:00:00Z",
                "time_end": "2026-07-01T00:05:00Z",
            }
        ],
        "bands": [
            {
                "source_asset_id": f"asset-{dataset_id}",
                "band_code": "B01",
                "band_name": "Band 1",
                "band_type": "spectral",
                "display_order": 1,
            }
        ],
    }


class FakeDomainStore:
    def __init__(self) -> None:
        self.outputs: dict[tuple[str, str], dict[str, Any]] = {}
        self.datasets: dict[str, dict[str, Any]] = {}
        self.completed: list[dict[str, Any]] = []
        self.failed: list[dict[str, Any]] = []

    def start_output(self, request, dataset, task_id):
        version = make_output_version(dataset.dataset_id, task_id)
        self.outputs[(dataset.dataset_id, version)] = {
            "dataset_id": dataset.dataset_id,
            "output_version": version,
            "status": "staging",
            "task_id": task_id,
        }
        self.datasets.setdefault(dataset.dataset_id, {"partition_status": "running", "current_output_version": None})
        return version

    def complete_output(self, result):
        output = self.outputs[(result.dataset_id, result.output_version)]
        output["status"] = "completed"
        output["counts"] = {
            "tiles": len(result.tiles),
            "indexes": len(result.indexes),
            "grid_cells": len(result.grid_cells),
        }
        self.datasets[result.dataset_id].update(
            {"partition_status": "completed", "current_output_version": result.output_version}
        )
        self.completed.append(result.model_dump(mode="json"))
        return deepcopy(output)

    def fail_output(self, dataset_id, output_version, *, error_code, error_message):
        output = self.outputs[(dataset_id, output_version)]
        output.update({"status": "failed", "error_code": error_code, "error_message": error_message})
        self.datasets[dataset_id]["partition_status"] = "failed"
        self.failed.append(deepcopy(output))

    def resolve_output_version(self, dataset_id, output_version=None):
        version = output_version or self.datasets[dataset_id]["current_output_version"]
        if not version:
            raise KeyError(dataset_id)
        return version

    def get_dataset(self, dataset_id):
        return deepcopy(self.datasets[dataset_id])

    def list_indexes(self, dataset_id, **_kwargs):
        return [row for row in self.completed if row["dataset_id"] == dataset_id]


class FakeJobStore:
    def __init__(self) -> None:
        self.cancelled_tasks: set[str] = set()
        self.attempts: list[tuple[str, str]] = []

    def request_cancel(self, task_id: str):
        self.cancelled_tasks.add(task_id)

    def is_cancel_requested(self, task_id: str) -> bool:
        return task_id in self.cancelled_tasks


class FakeRunner:
    def __init__(self) -> None:
        self.failed: set[str] = set()
        self.calls: list[dict[str, Any]] = []

    def fail_dataset(self, dataset_id: str, error: Exception) -> None:
        self.failed.add(dataset_id)
        setattr(self, f"error_{dataset_id}", error)

    def run_dataset(self, *, dataset, task_id, output_version, grid_type, requested_grid_level, cover_mode):
        self.calls.append(
            {
                "dataset_id": dataset.dataset_id,
                "grid_type": grid_type,
                "requested_grid_level": requested_grid_level,
                "cover_mode": cover_mode,
            }
        )
        if dataset.dataset_id in self.failed:
            raise getattr(self, f"error_{dataset.dataset_id}")
        return {
            "dataset_id": dataset.dataset_id,
            "task_id": task_id,
            "output_version": output_version,
            "grid_type": grid_type,
            "requested_grid_level": requested_grid_level,
            "partition_method": "entity" if grid_type == "isea4h" else "logical",
            "object_prefix": "",
            "tiles": [{"output_id": f"tile-{dataset.dataset_id}"}],
            "indexes": [{"output_id": f"index-{dataset.dataset_id}"}],
            "grid_cells": [{"output_id": f"cell-{dataset.dataset_id}"}],
        }


def _workflow(domain_store, runner, job_store=None) -> PartitionWorkflowService:
    return PartitionWorkflowService(
        PartitionService({}),
        store=job_store,
        domain_store=domain_store,
        runner=runner,
    )


def test_batch_commits_successful_dataset_when_sibling_fails() -> None:
    request = _request_with_datasets("dataset-ok", "dataset-fail")
    domain_store = FakeDomainStore()
    runner = FakeRunner()
    runner.fail_dataset("dataset-fail", RuntimeError("unreadable loader COG"))

    result = _workflow(domain_store, runner, FakeJobStore()).run(task_id="task-batch", request=request)

    assert result == {
        "batch_id": "batch-01",
        "status": "partial_failure",
        "datasets": [
            {
                "dataset_id": "dataset-ok",
                "output_version": make_output_version("dataset-ok", "task-batch"),
                "status": "completed",
                "counts": {"tiles": 1, "indexes": 1, "grid_cells": 1},
            },
            {
                "dataset_id": "dataset-fail",
                "output_version": make_output_version("dataset-fail", "task-batch"),
                "status": "failed",
                "error": {"code": "partition_execution_failed", "message": "unreadable loader COG"},
            },
        ],
    }
    assert domain_store.resolve_output_version("dataset-ok") == make_output_version("dataset-ok", "task-batch")
    with pytest.raises(KeyError):
        domain_store.resolve_output_version("dataset-fail")


def test_cancel_after_ray_before_commit_cannot_switch_pointer() -> None:
    domain_store = FakeDomainStore()
    old = "old-version"
    domain_store.datasets["dataset-ok"] = {"partition_status": "completed", "current_output_version": old}
    runner = FakeRunner()
    job_store = FakeJobStore()
    workflow = _workflow(domain_store, runner, job_store)
    workflow.after_ray = lambda: job_store.request_cancel("task-batch")

    result = workflow.run(task_id="task-batch", request=_request())

    assert result["datasets"][0]["status"] == "cancelled"
    assert domain_store.resolve_output_version("dataset-ok") == old
    assert domain_store.completed == []


def test_duplicate_dataset_is_rejected_before_attempt_creation() -> None:
    request = _request()
    duplicate = request.model_copy(update={"datasets": (request.datasets[0], request.datasets[0])})
    domain_store = FakeDomainStore()
    job_store = FakeJobStore()

    with pytest.raises(ValueError, match="duplicate dataset_id"):
        _workflow(domain_store, FakeRunner(), job_store).run(task_id="task-duplicate", request=duplicate)
    assert job_store.attempts == []
    assert domain_store.outputs == {}


def test_failed_output_does_not_mutate_sibling_rows() -> None:
    request = _request_with_datasets("dataset-ok", "dataset-fail")
    domain_store = FakeDomainStore()
    runner = FakeRunner()
    runner.fail_dataset("dataset-fail", RuntimeError("unreadable loader COG"))

    _workflow(domain_store, runner, FakeJobStore()).run(task_id="task-isolation", request=request)

    assert domain_store.get_dataset("dataset-ok")["partition_status"] == "completed"
    assert domain_store.get_dataset("dataset-fail")["partition_status"] == "failed"
    assert {row["dataset_id"] for row in domain_store.list_indexes("dataset-ok")} == {"dataset-ok"}


def test_submit_strict_executes_dataset_workflow_through_domain_store() -> None:
    domain_store = FakeDomainStore()
    job_store = InMemoryPartitionJobStore()
    legacy_runner = Mock(side_effect=AssertionError("strict route must not invoke the legacy partition runner"))
    service = PartitionService({"optical": PartitionBackend(data_type="optical", run=legacy_runner)})
    workflow = PartitionWorkflowService(
        service,
        store=job_store,
        domain_store=domain_store,
        runner=FakeRunner(),
    )

    task = workflow.submit_strict("optical", _request())
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        attempt = job_store.get_attempt(task.task_id)
        if attempt and attempt["status"] == "succeeded":
            break
        time.sleep(0.01)

    attempt = job_store.get_attempt(task.task_id)
    assert attempt is not None
    assert attempt["status"] == "succeeded"
    assert domain_store.completed[0]["dataset_id"] == "dataset-ok"
    assert domain_store.completed[0]["task_id"] == task.task_id
    legacy_runner.assert_not_called()


def test_submit_strict_returns_existing_task_after_cross_worker_attempt_conflict(monkeypatch) -> None:
    job_store = InMemoryPartitionJobStore()
    workflow = PartitionWorkflowService(PartitionService({}), store=job_store, domain_store=FakeDomainStore(), runner=FakeRunner())
    existing = PartitionTask(task_id="already-running", status="running", data_type="optical", operation="run", created_at=0, updated_at=0)

    def reject_duplicate_attempt(**_kwargs):
        raise PartitionBatchAlreadyActiveError("batch is active")

    calls = 0

    def active_task_after_conflict(_batch):
        nonlocal calls
        calls += 1
        return existing if calls >= 2 else None

    monkeypatch.setattr(job_store, "create_attempt", reject_duplicate_attempt)
    monkeypatch.setattr(workflow, "_active_task_for_batch", active_task_after_conflict)

    assert workflow.submit_strict("optical", _request()) is existing


def test_submit_mixed_persists_effective_dataset_partitions_and_keeps_runner_local() -> None:
    payload = _request().model_dump(mode="json")
    payload["datasets"] = [
        {**_dataset("dataset-optical", "optical"), "partition": {"grid_type": "mgrs", "requested_grid_level": 1, "partition_method": "logical"}},
        {**_dataset("dataset-radar", "radar"), "partition": {"grid_type": "isea4h", "requested_grid_level": 1, "partition_method": "entity"}},
    ]
    payload["datasets"][1]["assets"][0]["cog_uri"] = payload["datasets"][0]["assets"][0]["cog_uri"]
    request = StrictPartitionRequest.model_validate(payload)
    store = InMemoryPartitionJobStore()
    runner = FakeRunner()
    workflow = _workflow(FakeDomainStore(), runner, store)

    task = workflow.submit_mixed(request)
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        attempt = store.get_attempt(task.task_id)
        if attempt and attempt["status"] == "succeeded":
            break
        time.sleep(0.01)

    attempt = store.get_attempt(task.task_id)
    assert attempt is not None
    assert attempt["status"] == "succeeded"
    assert attempt["payload"]["strict_partition_request"] is True
    assert attempt["payload"]["dataset_partitions"] == [
        {"dataset_id": "dataset-optical", "data_type": "optical", "grid_type": "mgrs", "requested_grid_level": 1, "partition_method": "logical", "max_observations": 0},
        {"dataset_id": "dataset-radar", "data_type": "radar", "grid_type": "isea4h", "requested_grid_level": 1, "partition_method": "entity", "max_observations": 0},
    ]
    assert {row["data_type"] for row in store.list_assets("batch-01")} == {"optical", "radar"}
    assert [(call["dataset_id"], call["grid_type"], call["requested_grid_level"]) for call in runner.calls] == [
        ("dataset-optical", "mgrs", 1),
        ("dataset-radar", "isea4h", 1),
    ]
    store.supports_remote_jobs = True
    workflow._payload_uses_remote_ray = lambda *_args: True
    assert workflow._attempt_uses_remote_ray(attempt) is False

    detail = workflow.get_batch("batch-01")
    assert {(slot["dataset_id"], slot["grid_type"], slot["requested_grid_level"]) for slot in detail["partition_slots"]} == {
        ("dataset-optical", "mgrs", 1),
        ("dataset-radar", "isea4h", 1),
    }


def test_submit_mixed_rejects_homogeneous_and_completed_dataset_partition() -> None:
    store = InMemoryPartitionJobStore()
    workflow = _workflow(FakeDomainStore(), FakeRunner(), store)
    with pytest.raises(Exception, match="at least two dataset data types"):
        workflow.submit_mixed(_request())

    payload = _request().model_dump(mode="json")
    payload["datasets"].append({**_dataset("dataset-radar", "radar"), "partition": {"grid_type": "isea4h", "requested_grid_level": 1, "partition_method": "entity"}})
    request = StrictPartitionRequest.model_validate(payload)
    first = workflow.submit_mixed(request)
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        attempt = store.get_attempt(first.task_id)
        if attempt and attempt["status"] == "succeeded":
            break
        time.sleep(0.01)

    with pytest.raises(Exception, match="already completed"):
        workflow.submit_mixed(request)


def test_submit_mixed_partial_failure_retries_only_failed_dataset() -> None:
    payload = _request().model_dump(mode="json")
    payload["datasets"] = [
        {**_dataset("dataset-optical", "optical"), "partition": {"grid_type": "mgrs", "requested_grid_level": 1, "partition_method": "logical"}},
        {**_dataset("dataset-radar", "radar"), "partition": {"grid_type": "isea4h", "requested_grid_level": 1, "partition_method": "entity"}},
    ]
    payload["datasets"][1]["assets"][0]["cog_uri"] = payload["datasets"][0]["assets"][0]["cog_uri"]
    request = StrictPartitionRequest.model_validate(payload)
    store = InMemoryPartitionJobStore()
    runner = FakeRunner()
    runner.fail_dataset("dataset-radar", RuntimeError("temporary radar failure"))
    workflow = _workflow(FakeDomainStore(), runner, store)

    first = workflow.submit_mixed(request)
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        attempt = store.get_attempt(first.task_id)
        if attempt and attempt["status"] == "succeeded":
            break
        time.sleep(0.01)

    first_attempt = store.get_attempt(first.task_id)
    assert first_attempt is not None
    assert first_attempt["runner_result"]["status"] == "partial_failure"
    assert {asset["data_type"]: asset["status"] for asset in store.list_assets("batch-01")} == {
        "optical": "succeeded",
        "radar": "manual_required",
    }
    assert {slot["dataset_id"]: slot["status"] for slot in workflow.get_batch("batch-01")["partition_slots"]} == {
        "dataset-optical": "completed",
        "dataset-radar": "failed",
    }

    runner.failed.clear()
    second = workflow.submit_mixed(request)
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        attempt = store.get_attempt(second.task_id)
        if attempt and attempt["status"] == "succeeded":
            break
        time.sleep(0.01)

    second_attempt = store.get_attempt(second.task_id)
    assert second_attempt is not None
    assert [item["dataset_id"] for item in second_attempt["payload"]["datasets"]] == ["dataset-radar"]
    assert [call["dataset_id"] for call in runner.calls] == ["dataset-optical", "dataset-radar", "dataset-radar"]
    assert {slot["dataset_id"]: slot["status"] for slot in workflow.get_batch("batch-01")["partition_slots"]} == {
        "dataset-optical": "completed",
        "dataset-radar": "completed",
    }


def test_bounded_carbon_smoke_does_not_block_different_observation_limit() -> None:
    payload = _request().model_dump(mode="json")
    payload["datasets"] = [
        _dataset("dataset-optical", "optical"),
        {
            **_dataset("dataset-carbon", "carbon"),
            "partition": {"grid_type": "geohash", "requested_grid_level": 7, "partition_method": "logical", "max_observations": 100},
        },
    ]
    request = StrictPartitionRequest.model_validate(payload)
    store = InMemoryPartitionJobStore()
    runner = FakeRunner()
    workflow = _workflow(FakeDomainStore(), runner, store)
    first = workflow.submit_mixed(request)
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        if store.get_attempt(first.task_id).get("status") == "succeeded":
            break
        time.sleep(0.01)

    payload["datasets"][1]["partition"]["max_observations"] = 200
    second_request = StrictPartitionRequest.model_validate(payload)
    second = workflow.submit_mixed(second_request)
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        if store.get_attempt(second.task_id).get("status") == "succeeded":
            break
        time.sleep(0.01)

    second_attempt = store.get_attempt(second.task_id)
    assert [item["dataset_id"] for item in second_attempt["payload"]["datasets"]] == ["dataset-carbon"]
    assert second_attempt["payload"]["dataset_partitions"][0]["max_observations"] == 200
