"""Background worker that finishes dataset deletions.

The HTTP layer only *plans* a deletion (one short transaction) and returns 202;
this worker owns the expensive part.  It claims queued jobs with
``FOR UPDATE SKIP LOCKED`` exactly like the quality worker does, runs the plan
in committed batches, records per-step progress (rows, batches, seconds) on the
job row, then removes the dataset's partition objects in batched requests.

Failure policy: a transient failure returns the job to ``queued`` with a growing
``available_at``; after ``max_attempts`` the job is ``failed`` and keeps the
error plus the steps it already completed.  Because every step is a predicate
over stable ids, a resumed attempt simply finds fewer rows.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import Event, Thread
from typing import Any, Callable

from cube_split import runtime_config
from minio import Minio
from psycopg.rows import dict_row

from cube_web.services.dataset_deletion import (
    DEFAULT_BATCH_SIZE,
    PLAN_VERSION,
    DatasetDeletionInterrupted,
    DeletionPlan,
    execute_deletion_plan,
    refresh_deletion_steps,
)
from cube_web.services.db_pool import _PostgresPool
from cube_web.services.partition_object_store import PartitionObjectStore

logger = logging.getLogger(__name__)

DEFAULT_POLL_SECONDS = 1.0
MIN_POLL_SECONDS = 0.2
DEFAULT_LEASE_SECONDS = 900
DEFAULT_MAX_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 60
OBJECT_CLEANUP_WORKERS = 4


@dataclass(frozen=True)
class DeletionLease:
    deletion_id: str
    claimed_by: str
    attempt_count: int


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _resolve_poll_seconds() -> float:
    raw = os.environ.get("CUBE_WEB_WORKER_POLL_SECONDS")
    if not raw:
        return DEFAULT_POLL_SECONDS
    try:
        return max(MIN_POLL_SECONDS, float(raw))
    except ValueError:
        return DEFAULT_POLL_SECONDS


def _resolve_batch_size() -> int:
    return max(1, min(_env_int("CUBE_WEB_DELETION_BATCH_SIZE", DEFAULT_BATCH_SIZE), 200_000))


def _resolve_max_attempts() -> int:
    return max(1, _env_int("CUBE_WEB_DELETION_MAX_ATTEMPTS", DEFAULT_MAX_ATTEMPTS))


def _safe_rollback(connection: Any) -> None:
    """Clear an aborted transaction before writing a terminal state."""
    rollback = getattr(connection, "rollback", None)
    if callable(rollback):
        try:
            rollback()
        except Exception:  # pragma: no cover
            pass


def _safe_error(exc: BaseException) -> str:
    """Keep an actionable message without persisting credentials or object URIs."""
    details: list[str] = []
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        name = type(current).__name__
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", name) is None:
            name = "Exception"
        message = " ".join(str(current).split())
        message = re.sub(
            r"(?i)\b(?:password|passwd|secret|token|access[_-]?key)\s*[:=]\s*[^\s,;]+",
            "[credential redacted]",
            message,
        )
        message = re.sub(r"(?i)s3://[^\s\"'<>]+", "[object URI redacted]", message)
        message = re.sub(
            r"(?i)\b(?:postgres(?:ql)?|mysql|mariadb|redis)://[^\s\"'<>]+",
            "[connection URI redacted]",
            message,
        )
        details.append(f"{name}: {message[:400]}" if message else name)
        current = current.__cause__ or current.__context__
    return "; caused by ".join(details)[:1000]


def claim_dataset_deletions(
    connection: Any,
    *,
    worker_id: str,
    limit: int = 1,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
) -> list[DeletionLease]:
    """Claim queued (or lease-expired) deletion jobs."""
    now = datetime.now(UTC)
    stale_before = now - timedelta(seconds=lease_seconds)
    leases: list[DeletionLease] = []
    with connection.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT deletion_id FROM dataset_deletion_runs WHERE "
            "(status = 'queued' AND available_at <= %s) "
            "OR (status = 'running' AND claimed_at < %s) "
            "ORDER BY available_at, requested_at FOR UPDATE SKIP LOCKED LIMIT %s",
            (now, stale_before, int(limit)),
        )
        identifiers = [str(row["deletion_id"]) for row in cur.fetchall()]
        for deletion_id in identifiers:
            cur.execute(
                """UPDATE dataset_deletion_runs
                      SET status = 'running', claimed_by = %s, claimed_at = %s,
                          started_at = COALESCE(started_at, %s), updated_at = %s,
                          attempt_count = attempt_count + 1
                    WHERE deletion_id = %s
                RETURNING attempt_count""",
                (worker_id, now, now, now, deletion_id),
            )
            row = cur.fetchone()
            leases.append(DeletionLease(deletion_id, worker_id, int(row["attempt_count"])))
    connection.commit()
    return leases


def load_deletion_plan(connection: Any, deletion_id: str) -> tuple[DeletionPlan, int]:
    with connection.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT plan, attempt_count FROM dataset_deletion_runs WHERE deletion_id=%s",
            (deletion_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"deletion job disappeared: {deletion_id}")
    return DeletionPlan.from_json(row["plan"] or {}), int(row["attempt_count"] or 0)


def heartbeat_and_progress(
    connection: Any,
    *,
    lease: DeletionLease,
    progress: dict[str, Any],
    now: datetime | None = None,
) -> None:
    """Persist progress and renew the lease (only while this worker still owns it)."""
    stamp = now or datetime.now(UTC)
    with connection.cursor() as cur:
        cur.execute(
            """UPDATE dataset_deletion_runs
                  SET progress = %s, claimed_at = %s, updated_at = %s
                WHERE deletion_id = %s AND claimed_by = %s AND attempt_count = %s""",
            (_json(progress), stamp, stamp, lease.deletion_id, lease.claimed_by, lease.attempt_count),
        )
        if cur.rowcount != 1:
            raise DatasetDeletionInterrupted(f"lease lost for {lease.deletion_id}")
    connection.commit()


def finalize_deletion(
    connection: Any,
    *,
    lease: DeletionLease,
    status: str,
    result: dict[str, Any] | None = None,
    error_message: str | None = None,
    available_at: datetime | None = None,
    progress: dict[str, Any] | None = None,
) -> None:
    now = datetime.now(UTC)
    with connection.cursor() as cur:
        cur.execute(
            """UPDATE dataset_deletion_runs
                  SET status = %s,
                      finished_at = CASE WHEN %s IN ('succeeded','failed') THEN %s ELSE NULL END,
                      result = COALESCE(%s, result),
                      error_message = %s,
                      available_at = COALESCE(%s, available_at),
                      progress = COALESCE(%s, progress),
                      updated_at = %s
                WHERE deletion_id = %s AND claimed_by = %s AND attempt_count = %s""",
            (
                status,
                status,
                now,
                _json(result) if result is not None else None,
                error_message,
                available_at,
                _json(progress) if progress is not None else None,
                now,
                lease.deletion_id,
                lease.claimed_by,
                lease.attempt_count,
            ),
        )
    connection.commit()


def cleanup_partition_objects(prefix: str, *, on_progress: Any = None) -> dict[str, Any]:
    """Remove every generated object under the dataset's own prefix, in batches."""
    settings = runtime_config.minio_settings()
    store = PartitionObjectStore(
        Minio(
            settings.endpoint,
            access_key=settings.access_key,
            secret_key=settings.secret_key,
            secure=settings.secure,
        ),
        bucket=settings.bucket,
    )
    return store.remove_prefix(prefix, workers=OBJECT_CLEANUP_WORKERS, on_progress=on_progress)


def execute_deletion_job(
    connection: Any,
    *,
    lease: DeletionLease,
    batch_size: int | None = None,
    max_attempts: int | None = None,
    should_stop: Callable[[], bool] | None = None,
    cleanup_objects: Callable[[str], dict[str, Any]] = cleanup_partition_objects,
) -> dict[str, Any]:
    """Run one claimed deletion to a terminal state.  Returns the result payload."""
    plan, attempt = load_deletion_plan(connection, lease.deletion_id)
    # A crashed/stale attempt may leave the caller's transaction aborted.
    _safe_rollback(connection)
    if plan.plan_version != PLAN_VERSION:
        # Steps are derived data: rebuild them with the running engine and keep the
        # identifiers resolved at request time (their source rows may be gone).
        plan = refresh_deletion_steps(connection, plan)
        with connection.cursor() as cur:
            cur.execute(
                "UPDATE dataset_deletion_runs SET plan=%s, updated_at=now() WHERE deletion_id=%s",
                (_json(plan.to_json()), lease.deletion_id),
            )
        connection.commit()
        logger.info(
            "dataset_deletion.plan_refreshed deletion_id=%s version=%s steps=%s",
            lease.deletion_id, PLAN_VERSION, len(plan.steps),
        )
    progress: dict[str, Any] = {
        "dataset_id": plan.dataset_id,
        "deleted_total": 0,
        "deleted_rows_by_table": {},
        "steps": [],
        "phase": "rows",
    }

    def on_step(outcome: Any, total: int) -> None:
        progress["deleted_total"] = total
        progress["steps"] = [*progress["steps"], outcome.to_json()]
        table_totals = dict(progress["deleted_rows_by_table"])
        table_totals[outcome.table] = table_totals.get(outcome.table, 0) + outcome.deleted
        progress["deleted_rows_by_table"] = table_totals
        progress["last_step"] = outcome.name
        heartbeat_and_progress(connection, lease=lease, progress=progress)

    try:
        outcome = execute_deletion_plan(
            connection,
            plan,
            batch_size=batch_size or _resolve_batch_size(),
            on_step=on_step,
            should_stop=should_stop,
        )
    except DatasetDeletionInterrupted as exc:
        _safe_rollback(connection)
        heartbeat_and_progress(connection, lease=lease, progress=progress)
        finalize_deletion(
            connection,
            lease=lease,
            status="queued",
            error_message=_safe_error(exc),
            available_at=datetime.now(UTC) + timedelta(seconds=RETRY_BACKOFF_SECONDS),
            progress=progress,
        )
        return {"status": "interrupted", "progress": progress}

    progress["phase"] = "objects"
    heartbeat_and_progress(connection, lease=lease, progress=progress)
    try:
        cleanup = cleanup_objects(plan.object_prefix)
    except Exception as exc:  # object store failure must stay retryable
        _safe_rollback(connection)
        _retry_or_fail(
            connection,
            lease=lease,
            attempt=attempt,
            max_attempts=max_attempts or _resolve_max_attempts(),
            error_message=_safe_error(exc),
            progress=progress,
            result={**outcome, "object_cleanup": {"status": "failed", "prefix": plan.object_prefix}},
        )
        return {"status": "retrying", "progress": progress}

    result = {**outcome, "dataset_title": plan.dataset_title, "object_cleanup": cleanup}
    finalize_deletion(connection, lease=lease, status="succeeded", result=result, progress=progress)
    return {"status": "succeeded", "result": result}


def _retry_or_fail(
    connection: Any,
    *,
    lease: DeletionLease,
    attempt: int,
    max_attempts: int,
    error_message: str,
    progress: dict[str, Any] | None = None,
    result: dict[str, Any] | None = None,
) -> None:
    # The failure came from a failed statement, so the transaction is aborted and
    # any status write would fail too (that is how a job used to stay "running"
    # forever).  Roll back first, then record the terminal/retry state.
    _safe_rollback(connection)
    if attempt < max_attempts:
        finalize_deletion(
            connection,
            lease=lease,
            status="queued",
            result=result,
            error_message=error_message,
            available_at=datetime.now(UTC) + timedelta(seconds=RETRY_BACKOFF_SECONDS * attempt),
            progress=progress,
        )
        return
    finalize_deletion(
        connection,
        lease=lease,
        status="failed",
        result=result,
        error_message=error_message,
        progress=progress,
    )


class DatasetDeletionRuntime:
    """One daemon thread that drains queued dataset deletions."""

    def __init__(
        self,
        *,
        worker_id: str = "cube-web-deletion",
        dsn: str | None = None,
        poll_seconds: float | None = None,
        batch_size: int | None = None,
        max_attempts: int | None = None,
    ) -> None:
        self.worker_id = worker_id
        self.dsn = dsn or runtime_config.postgres_dsn()
        self.poll_seconds = _resolve_poll_seconds() if poll_seconds is None else max(MIN_POLL_SECONDS, float(poll_seconds))
        self.batch_size = batch_size or _resolve_batch_size()
        self.max_attempts = max_attempts or _resolve_max_attempts()
        self._stop = Event()
        self._threads: list[Thread] = []

    def start(self) -> None:
        if self._threads:
            return
        self._stop.clear()
        self._threads = [Thread(target=self._loop, name="cube-web-dataset-deletion", daemon=True)]
        for thread in self._threads:
            thread.start()

    def stop(self) -> None:
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout=max(1.0, self.poll_seconds * 4))
        self._threads = []

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception:
                logger.exception("dataset deletion worker iteration failed")
            self._stop.wait(self.poll_seconds)

    def run_once(self, *, limit: int = 1) -> list[dict[str, Any]]:
        """Claim and execute up to ``limit`` jobs; returns per-job outcomes."""
        outcomes: list[dict[str, Any]] = []
        with _PostgresPool.for_dsn(self.dsn).connection() as connection:
            leases = claim_dataset_deletions(
                connection, worker_id=self.worker_id, limit=limit
            )
            for lease in leases:
                logger.info("dataset_deletion.started deletion_id=%s attempt=%s", lease.deletion_id, lease.attempt_count)
                try:
                    outcome = execute_deletion_job(
                        connection,
                        lease=lease,
                        batch_size=self.batch_size,
                        max_attempts=self.max_attempts,
                        should_stop=self._stop.is_set,
                    )
                except Exception as exc:
                    logger.exception("dataset_deletion.failed deletion_id=%s", lease.deletion_id)
                    _retry_or_fail(
                        connection,
                        lease=lease,
                        attempt=lease.attempt_count,
                        max_attempts=self.max_attempts,
                        error_message=_safe_error(exc),
                    )
                    outcomes.append({"deletion_id": lease.deletion_id, "status": "failed"})
                    continue
                logger.info(
                    "dataset_deletion.finished deletion_id=%s status=%s",
                    lease.deletion_id, outcome.get("status"),
                )
                outcomes.append({"deletion_id": lease.deletion_id, **outcome})
        return outcomes


def _json(value: Any) -> Any:
    from psycopg.types.json import Jsonb

    return Jsonb(value)
