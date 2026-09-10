from threading import Lock
from typing import Any

from cube_web.services.partition_job_store import (
    InMemoryPartitionJobStore,
    PostgresPartitionJobStore,
)

_TERMINAL_GUARD = "AND status IN ('queued', 'running', 'retrying')"
_MANUAL_REQUIRED_GUARD = "AND status IN ('queued', 'running', 'retrying', 'succeeded')"


def _seed_running_attempt(store: InMemoryPartitionJobStore, task_id: str = "task-01") -> None:
    store.ensure_runtime_batch(
        batch_id="batch-01",
        batch_name="batch-01",
        data_type="optical",
        payload={"selected_assets": [{"source_uri": "s3://cube/scene-a.tif", "scene_id": "scene-a"}]},
    )
    store.create_attempt(
        task_id=task_id,
        batch_id="batch-01",
        operation="optical_run",
        payload={"selected_assets": [{"source_uri": "s3://cube/scene-a.tif", "scene_id": "scene-a"}]},
    )
    assert store.start_attempt(task_id)


def _record_ingest_refresh(store: InMemoryPartitionJobStore) -> list[str]:
    calls: list[str] = []
    original = store._refresh_ingest_readiness

    def recording(batch_id: str, result: dict[str, Any], *, now: str) -> None:
        calls.append(batch_id)
        original(batch_id, result, now=now)

    store._refresh_ingest_readiness = recording  # type: ignore[method-assign]
    return calls


def test_succeed_attempt_does_not_override_cancelled() -> None:
    store = InMemoryPartitionJobStore()
    _seed_running_attempt(store)
    ingest_calls = _record_ingest_refresh(store)
    store.mark_cancelled("task-01")

    store.succeed_attempt("task-01", {"status": "completed", "ingest_enabled": True})

    attempt = store.get_attempt("task-01")
    assert attempt is not None
    assert attempt["status"] == "cancelled"
    assert store.get_batch("batch-01")["status"] == "cancelled"
    assert ingest_calls == []


def test_fail_attempt_does_not_override_cancelled() -> None:
    store = InMemoryPartitionJobStore()
    _seed_running_attempt(store)
    store.mark_cancelled("task-01")

    store.fail_attempt("task-01", "late failure", manual_required=True, error_type="ray_job_failed")

    attempt = store.get_attempt("task-01")
    assert attempt is not None
    assert attempt["status"] == "cancelled"
    assert attempt["error_message"] is None
    assert store.get_batch("batch-01")["status"] == "cancelled"


def test_mark_result_manual_required_does_not_override_cancelled() -> None:
    store = InMemoryPartitionJobStore()
    _seed_running_attempt(store)
    store.mark_cancelled("task-01")

    store.mark_result_manual_required("task-01", "late partial failure", error_type="partition_failed")

    attempt = store.get_attempt("task-01")
    assert attempt is not None
    assert attempt["status"] == "cancelled"
    assert store.get_batch("batch-01")["status"] == "cancelled"


def test_succeed_attempt_does_not_override_cancel_requested() -> None:
    store = InMemoryPartitionJobStore()
    _seed_running_attempt(store)
    ingest_calls = _record_ingest_refresh(store)
    store.request_cancel("task-01")

    store.succeed_attempt("task-01", {"status": "completed"})

    attempt = store.get_attempt("task-01")
    assert attempt is not None
    assert attempt["status"] == "cancel_requested"
    assert store.get_batch("batch-01")["status"] == "cancel_requested"
    assert ingest_calls == []


def test_succeed_attempt_from_running_still_refreshes_downstream() -> None:
    store = InMemoryPartitionJobStore()
    _seed_running_attempt(store)
    ingest_calls = _record_ingest_refresh(store)

    store.succeed_attempt("task-01", {"status": "completed", "ingest_enabled": True})

    attempt = store.get_attempt("task-01")
    assert attempt is not None
    assert attempt["status"] == "succeeded"
    batch = store.get_batch("batch-01")
    assert batch["status"] == "succeeded"
    assert batch["ingest_status"] == "ingested"
    assert ingest_calls == ["batch-01"]


def test_mark_result_manual_required_demotes_succeeded_attempt() -> None:
    store = InMemoryPartitionJobStore()
    _seed_running_attempt(store)
    store.succeed_attempt("task-01", {"status": "partial_failure"})

    store.mark_result_manual_required("task-01", "one dataset failed", error_type="partition_failed")

    attempt = store.get_attempt("task-01")
    assert attempt is not None
    assert attempt["status"] == "manual_required"
    assert store.get_batch("batch-01")["status"] == "manual_required"


def test_mark_cancelled_does_not_override_terminal_states() -> None:
    store = InMemoryPartitionJobStore()
    _seed_running_attempt(store)
    store.succeed_attempt("task-01", {"status": "completed"})

    cancelled = store.mark_cancelled("task-01")

    assert cancelled is not None
    assert cancelled["status"] == "succeeded"
    assert store.get_batch("batch-01")["status"] == "succeeded"


def test_list_tasks_exposes_worker_container_limit_from_attempt_payload() -> None:
    store = InMemoryPartitionJobStore()
    payload = {
        "strict_partition_request": True,
        "worker_container_limit": 3,
        "datasets": [],
    }
    store.ensure_runtime_batch(
        batch_id="batch-workers",
        batch_name="batch-workers",
        data_type="optical",
        payload=payload,
    )
    store.create_attempt(
        task_id="task-workers",
        batch_id="batch-workers",
        operation="optical_run",
        payload=payload,
    )

    assert store.list_tasks()[0]["worker_container_limit"] == 3


class _RecordingCursor:
    def __init__(self, connection: "_RecordingConnection") -> None:
        self._connection = connection

    def __enter__(self) -> "_RecordingCursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: str, params: tuple[object, ...] = ()) -> None:
        self._connection.statements.append((sql, params))

    def fetchone(self) -> None:
        return None

    def fetchall(self) -> list[tuple[object, ...]]:
        return []


class _RecordingConnection:
    def __init__(self) -> None:
        self.statements: list[tuple[str, tuple[object, ...]]] = []

    def __enter__(self) -> "_RecordingConnection":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def cursor(self) -> _RecordingCursor:
        return _RecordingCursor(self)

    def commit(self) -> None:
        self.statements.append(("COMMIT", ()))


def _postgres_store(connection: _RecordingConnection) -> PostgresPartitionJobStore:
    store = object.__new__(PostgresPartitionJobStore)
    store.dsn = "postgresql://test"
    store._schema_ensured = True
    store._schema_lock = Lock()
    store._connect = lambda: connection  # type: ignore[method-assign]
    return store


def test_opengauss_succeed_attempt_sql_guards_terminal_states() -> None:
    connection = _RecordingConnection()
    store = _postgres_store(connection)

    store.succeed_attempt("task-01", {"status": "completed"})

    update_sql = next(sql for sql, _params in connection.statements if "SET status = 'succeeded'" in sql)
    assert _TERMINAL_GUARD in update_sql
    # Guard rejected the row (fetchone -> None): no batch/asset/ingest refresh statements follow.
    assert not any("partition_batches" in sql for sql, _params in connection.statements)


def test_opengauss_fail_attempt_sql_guards_terminal_states() -> None:
    connection = _RecordingConnection()
    store = _postgres_store(connection)

    store.fail_attempt("task-01", "boom", manual_required=True, error_type="ray_job_failed")

    update_sql = next(sql for sql, _params in connection.statements if "partition_job_attempts" in sql)
    assert _TERMINAL_GUARD in update_sql
    assert not any("partition_batches" in sql for sql, _params in connection.statements)


def test_opengauss_mark_result_manual_required_sql_guards_terminal_states() -> None:
    connection = _RecordingConnection()
    store = _postgres_store(connection)

    store.mark_result_manual_required("task-01", "boom", error_type="partition_failed")

    update_sql = next(sql for sql, _params in connection.statements if "partition_job_attempts" in sql)
    assert _MANUAL_REQUIRED_GUARD in update_sql
    assert not any("partition_batches" in sql for sql, _params in connection.statements)


def test_opengauss_cancel_check_reads_only_status() -> None:
    connection = _RecordingConnection()
    store = _postgres_store(connection)

    assert store.is_cancel_requested("task-01") is False

    sql = next(sql for sql, _params in connection.statements if "partition_job_attempts" in sql)
    assert sql == "SELECT status FROM partition_job_attempts WHERE task_id = %s"
