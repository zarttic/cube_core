"""Unit tests for the dataset-deletion worker (claim, progress, retry policy)."""

from __future__ import annotations

from typing import Any

from cube_web.services.dataset_deletion import DeletionPlan, DeletionStep
from cube_web.services.dataset_deletion_worker import (
    DeletionLease,
    claim_dataset_deletions,
    execute_deletion_job,
)

DATASET_ID = "dataset-a"
DELETION_ID = "dataset-deletion-1"


def _plan() -> DeletionPlan:
    return DeletionPlan(
        dataset_id=DATASET_ID,
        dataset_title="标题",
        requested_by="admin",
        identifiers={"scene_ids": ["scene-1"], "output_versions": ["ov-1"]},
        object_prefix=f"partition/{DATASET_ID}/",
        steps=(
            DeletionStep(name="partition.indexes", table="partition_indexes", predicate="dataset_id=%s",
                         params=(DATASET_ID,)),
            DeletionStep(name="dataset.datasets", table="datasets", predicate="dataset_id=%s",
                         params=(DATASET_ID,), group="dataset"),
        ),
    )


def _unwrap(value: Any) -> Any:
    return getattr(value, "obj", value)


class _Cursor:
    def __init__(self, conn: "_Connection") -> None:
        self._conn = conn
        self._rows: list[dict[str, Any]] = []
        self.rowcount = 0

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, statement: str, params: tuple[Any, ...] = ()) -> None:
        normalized = " ".join(statement.split())
        self._conn.executed.append((normalized, params))
        self._rows, self.rowcount = self._conn.answer(normalized, params)

    def fetchall(self) -> list[dict[str, Any]]:
        return list(self._rows)

    def fetchone(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None


class _Connection:
    def __init__(self, *, attempt_count: int = 1, queued: int = 1) -> None:
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.commits = 0
        self.attempt_count = attempt_count
        self._queued = queued

    def cursor(self, **_kwargs: object) -> _Cursor:
        return _Cursor(self)

    def commit(self) -> None:
        self.commits += 1

    def answer(self, statement: str, params: tuple[Any, ...]) -> tuple[list[dict[str, Any]], int]:
        if statement.startswith("DELETE FROM"):
            return [], 0
        if "SELECT deletion_id FROM dataset_deletion_runs" in statement:
            return [{"deletion_id": DELETION_ID} for _ in range(self._queued)], 0
        if "RETURNING attempt_count" in statement:
            return [{"attempt_count": self.attempt_count}], 1
        if "SELECT plan, attempt_count FROM dataset_deletion_runs" in statement:
            return [{"plan": _plan().to_json(), "attempt_count": self.attempt_count}], 1
        if statement.startswith("UPDATE dataset_deletion_runs"):
            return [], 1
        return [], 0

    def updates(self) -> list[tuple[str, tuple[Any, ...]]]:
        return [entry for entry in self.executed if entry[0].startswith("UPDATE dataset_deletion_runs")]

    def statuses(self) -> list[str]:
        """Statuses written by finalize_deletion, in order."""
        found: list[str] = []
        for statement, params in self.updates():
            if "SET status = %s" in statement:
                found.append(str(params[0]))
        return found

    def progress_updates(self) -> list[Any]:
        return [
            _unwrap(params[0])
            for statement, params in self.updates()
            if "SET progress = %s" in statement
        ]


def test_claim_marks_the_job_running_and_counts_the_attempt() -> None:
    connection = _Connection(attempt_count=2)

    leases = claim_dataset_deletions(connection, worker_id="w-1", limit=5)

    assert leases == [DeletionLease(DELETION_ID, "w-1", 2)]
    claim_sql = next(statement for statement, _ in connection.executed if statement.startswith("SELECT deletion_id"))
    assert "FOR UPDATE SKIP LOCKED" in claim_sql
    update_sql, params = connection.updates()[0]
    assert "status = 'running'" in update_sql
    assert params[0] == "w-1"
    assert connection.commits == 1


def test_job_records_progress_per_step_and_finishes_succeeded() -> None:
    connection = _Connection()
    cleanup_calls: list[str] = []

    outcome = execute_deletion_job(
        connection,
        lease=DeletionLease(DELETION_ID, "w-1", 1),
        batch_size=10,
        max_attempts=3,
        cleanup_objects=lambda prefix: cleanup_calls.append(prefix) or {"status": "completed", "object_count": 3},
    )

    assert cleanup_calls == [f"partition/{DATASET_ID}/"]
    assert outcome["status"] == "succeeded"
    assert outcome["result"]["object_cleanup"]["object_count"] == 3
    assert [step["name"] for step in outcome["result"]["steps"]] == [
        "partition.indexes",
        "dataset.datasets",
    ]
    assert connection.statuses() == ["succeeded"]
    progress = connection.progress_updates()
    assert progress, "progress must be persisted while the job runs"
    assert progress[-1]["phase"] == "objects"
    assert progress[0]["deleted_total"] == 0
    assert connection.commits >= 3


def test_object_cleanup_failure_requeues_then_fails_after_max_attempts() -> None:
    def boom(_prefix: str) -> dict[str, Any]:
        raise RuntimeError("MinIO 不可用")

    retrying = _Connection(attempt_count=1)
    outcome = execute_deletion_job(
        retrying,
        lease=DeletionLease(DELETION_ID, "w-1", 1),
        max_attempts=3,
        cleanup_objects=boom,
    )
    assert outcome["status"] == "retrying"
    assert retrying.statuses() == ["queued"]
    _, params = next(item for item in retrying.updates() if "SET status = %s" in item[0])
    assert params[0] == "queued"
    assert params[5] is not None, "a retry must set available_at (backoff)"
    assert "MinIO" in str(params[4])

    final = _Connection(attempt_count=3)
    outcome = execute_deletion_job(
        final,
        lease=DeletionLease(DELETION_ID, "w-1", 3),
        max_attempts=3,
        cleanup_objects=boom,
    )
    assert outcome["status"] == "retrying"
    assert final.statuses() == ["failed"]


def test_interrupted_job_goes_back_to_the_queue() -> None:
    connection = _Connection()

    outcome = execute_deletion_job(
        connection,
        lease=DeletionLease(DELETION_ID, "w-1", 1),
        max_attempts=3,
        should_stop=lambda: True,
        cleanup_objects=lambda prefix: {"status": "completed"},
    )

    assert outcome["status"] == "interrupted"
    assert connection.statuses() == ["queued"]
    assert connection.commits >= 1
