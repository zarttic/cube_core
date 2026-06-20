from __future__ import annotations

from types import SimpleNamespace

from cube_web.services import partition_remote_job, partition_workflow


class _Store:
    def __init__(self, attempt: dict, batch: dict) -> None:
        self._attempt = attempt
        self._batch = batch
        self.succeeded: list[tuple[str, dict]] = []
        self.failed: list[tuple[str, str]] = []
        self.cancelled: list[str] = []
        self.started: list[str] = []

    def get_attempt(self, task_id: str) -> dict | None:
        if task_id != self._attempt["task_id"]:
            return None
        return dict(self._attempt)

    def get_batch(self, batch_id: str) -> dict | None:
        if batch_id != self._batch["batch_id"]:
            return None
        return dict(self._batch)

    def is_cancel_requested(self, task_id: str) -> bool:
        return False

    def start_attempt(self, task_id: str) -> bool:
        self.started.append(task_id)
        self._attempt["status"] = "running"
        return True

    def succeed_attempt(self, task_id: str, result: dict) -> None:
        self.succeeded.append((task_id, dict(result)))

    def fail_attempt(self, task_id: str, error: str, *, manual_required: bool = False, error_type: str | None = None) -> None:
        self.failed.append((task_id, error))

    def mark_cancelled(self, task_id: str) -> dict | None:
        self.cancelled.append(task_id)
        self._attempt["status"] = "cancelled"
        return dict(self._attempt)

    def list_attempts(self, batch_id: str) -> list[dict]:
        return [dict(self._attempt)]


def test_run_task_succeeds_without_fastapi_dependency(monkeypatch) -> None:
    task_id = "partition-remote-success"
    attempt = {"task_id": task_id, "batch_id": "BATCH_A", "payload": {"batch_id": "BATCH_A"}, "status": "queued", "operation": "auto_run", "asset_ids": []}
    batch = {"batch_id": "BATCH_A", "data_type": "optical", "max_auto_retries": 0}
    store = _Store(attempt, batch)

    monkeypatch.setattr(partition_remote_job, "get_partition_job_store", lambda: store)
    monkeypatch.setattr(partition_remote_job, "_runner_for_data_type", lambda data_type: (lambda payload: {"status": "completed", "rows": 1, "data_type": data_type}))

    exit_code = partition_remote_job.run_task(task_id)

    assert exit_code == 0
    assert store.started == [task_id]
    assert store.succeeded
    stored_task_id, result = store.succeeded[0]
    assert stored_task_id == task_id
    assert result["job_id"] == task_id
    assert result["ray_task_id"] == task_id


def test_run_task_marks_cancelled_when_runner_raises_cancelled(monkeypatch) -> None:
    task_id = "partition-remote-cancelled"
    attempt = {"task_id": task_id, "batch_id": "BATCH_B", "payload": {"batch_id": "BATCH_B"}, "status": "queued", "operation": "auto_run", "asset_ids": []}
    batch = {"batch_id": "BATCH_B", "data_type": "optical", "max_auto_retries": 0}
    store = _Store(attempt, batch)

    class PartitionCancelledError(Exception):
        pass

    def raise_cancelled(_payload):
        raise PartitionCancelledError("Partition task cancelled")

    monkeypatch.setattr(partition_remote_job, "get_partition_job_store", lambda: store)
    monkeypatch.setattr(partition_remote_job, "_runner_for_data_type", lambda _data_type: raise_cancelled)

    exit_code = partition_remote_job.run_task(task_id)

    assert exit_code == 1
    assert store.cancelled == [task_id]
    assert not store.failed


def test_ray_job_runtime_env_includes_resolved_runtime_config(monkeypatch) -> None:
    for name in (
        "CUBE_WEB_POSTGRES_DSN",
        "POSTGRES_DSN",
        "DATABASE_URL",
        "CUBE_WEB_RAY_ADDRESS",
        "RAY_ADDRESS",
        "CUBE_WEB_MINIO_ENDPOINT",
        "MINIO_ENDPOINT",
        "CUBE_WEB_MINIO_ACCESS_KEY",
        "MINIO_ACCESS_KEY",
        "CUBE_WEB_MINIO_SECRET_KEY",
        "MINIO_SECRET_KEY",
        "CUBE_WEB_MINIO_BUCKET",
        "MINIO_BUCKET",
    ):
        monkeypatch.delenv(name, raising=False)

    monkeypatch.setattr(
        "cube_split.jobs.ray_logical_partition_job._ray_runtime_env_from_env",
        lambda: {"env_vars": {"PYTHONPATH": ".:./cube_encoder:./cube_split:./cube_web"}},
    )
    monkeypatch.setattr(partition_workflow.runtime_config, "postgres_dsn", lambda: "postgresql://user:pass@10.0.0.1:15400/postgres")
    monkeypatch.setattr(partition_workflow.runtime_config, "ray_address", lambda: "10.0.0.2:6379")
    monkeypatch.setattr(
        partition_workflow.runtime_config,
        "minio_settings",
        lambda: SimpleNamespace(
            endpoint="10.0.0.3:9000",
            access_key="access-key",
            secret_key="secret-key",
            bucket="cube",
        ),
    )

    runtime_env = partition_workflow._ray_job_runtime_env()

    assert runtime_env["env_vars"]["CUBE_WEB_POSTGRES_DSN"] == "postgresql://user:pass@10.0.0.1:15400/postgres"
    assert runtime_env["env_vars"]["CUBE_WEB_RAY_ADDRESS"] == "10.0.0.2:6379"
    assert runtime_env["env_vars"]["CUBE_WEB_MINIO_ENDPOINT"] == "10.0.0.3:9000"
    assert runtime_env["env_vars"]["CUBE_WEB_MINIO_ACCESS_KEY"] == "access-key"
    assert runtime_env["env_vars"]["CUBE_WEB_MINIO_SECRET_KEY"] == "secret-key"
    assert runtime_env["env_vars"]["CUBE_WEB_MINIO_BUCKET"] == "cube"
    assert runtime_env["env_vars"]["RAY_OVERRIDE_JOB_RUNTIME_ENV"] == "1"
