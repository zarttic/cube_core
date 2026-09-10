from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Event, Thread
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from psycopg.rows import dict_row

from cube_split import runtime_config
from cube_web.services.ingest_worker import _resolve_ingest_max_workers, process_queued_ingest_scenes
from cube_web.services.partition_domain_store import get_partition_domain_store
from cube_web.services.quality_contracts import QualityResult
from cube_web.services.quality_ingest_bridge import create_ingest_runs_after_quality
from cube_web.services.quality_object_reader import quality_object_reader
from cube_web.services.quality_repository import (
    ERROR_BATCH_SIZE,
    NewQualityError,
    QualityCompletionConflict,
    QualityLease,
    StaleQualityLease,
    _allocate_quality_run,
    assert_quality_result_totals,
    complete_quality_run_if_current,
    finish_quality_result,
    require_open_gauss_domain_store,
    set_quality_lease_on_transaction,
    start_quality_run,
    write_quality_error_batch,
)
from cube_web.services.config_store import auto_ingest_after_quality_enabled, get_enabled_optional_quality_rules
from cube_web.services.quality_rules import (
    DEFAULT_RULE_SET_VERSION,
    QualityFinding,
    RuleContext,
    default_rule_registry,
    reduce_quality_status,
    snapshot_rules,
)

logger = logging.getLogger(__name__)

DEFAULT_QUALITY_MAX_WORKERS = 4


def _resolve_quality_max_workers() -> int:
    raw = runtime_config.env_text("CUBE_WEB_QUALITY_MAX_WORKERS")
    if not raw:
        return DEFAULT_QUALITY_MAX_WORKERS
    try:
        return max(1, int(raw))
    except ValueError:
        return DEFAULT_QUALITY_MAX_WORKERS


def _safe_execution_error(exc: Exception, prefix: str) -> str:
    """Return an actionable error without persisting credentials or object URIs."""
    details: list[str] = []
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        exception_type = type(current).__name__
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", exception_type) is None:
            exception_type = "Exception"
        message = " ".join(str(current).split())
        message = re.sub(
            r"(?i)\b(?:password|passwd|secret|token|access[_-]?key)\s*[:=]\s*[^\s,;]+",
            "[credential redacted]",
            message,
        )
        message = re.sub(r"(?i)s3://[^\s\"'<>]+", "[object URI redacted]", message)
        message = re.sub(r"(?i)\b(?:postgres(?:ql)?|mysql|mariadb|redis)://[^\s\"'<>]+", "[connection URI redacted]", message)
        message = message[:1000]
        details.append(f"{exception_type}: {message}" if message else exception_type)
        current = current.__cause__ or current.__context__
    return f"{prefix} ({'; caused by '.join(details)})"


def claim_quality_runs(tx, *, worker_id: str, limit: int = 10, lease_seconds: int = 300) -> list[QualityLease]:
    now = datetime.now(UTC)
    stale_before = now - timedelta(seconds=lease_seconds)
    with tx.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT quality_run_id FROM partition_quality_runs WHERE "
            "(status = 'pending' AND available_at <= %s) OR (status = 'running' AND claimed_at < %s) "
            "ORDER BY available_at, created_at FOR UPDATE SKIP LOCKED LIMIT %s",
            (now, stale_before, limit),
        )
        identifiers = [row["quality_run_id"] for row in cur.fetchall()]
        leases: list[QualityLease] = []
        for quality_run_id in identifiers:
            cur.execute(
                "UPDATE partition_quality_runs SET claimed_by = %s, claimed_at = %s, attempt_count = attempt_count + 1 "
                "WHERE quality_run_id = %s RETURNING attempt_count",
                (worker_id, now, quality_run_id),
            )
            row = cur.fetchone()
            leases.append(QualityLease(quality_run_id, worker_id, int(row["attempt_count"])))
    return leases


def heartbeat_quality_run(tx, *, lease: QualityLease, now: datetime) -> bool:
    with tx.cursor() as cur:
        cur.execute(
            "UPDATE partition_quality_runs SET claimed_at = %s WHERE quality_run_id = %s AND claimed_by = %s "
            "AND attempt_count = %s AND status = 'running'",
            (now, lease.quality_run_id, lease.claimed_by, lease.attempt_count),
        )
        return cur.rowcount == 1


def dispatch_quality_events(*, worker_id: str, limit: int = 100, now: datetime | None = None) -> int:
    now = now or datetime.now(UTC)
    base_store = get_partition_domain_store()
    store = require_open_gauss_domain_store()
    allocated = 0
    for event in base_store.claim_outbox(worker_id, limit=limit):
        try:
            with store.transaction() as tx:
                with tx.cursor(row_factory=dict_row) as cur:
                    cur.execute("SELECT data_type, product_type FROM partition_datasets WHERE dataset_id = %s", (event["dataset_id"],))
                    dataset = cur.fetchone()
                if dataset is None:
                    raise RuntimeError("dataset disappeared before quality dispatch")
                enabled_optional = get_enabled_optional_quality_rules()
                snapshots = snapshot_rules(
                    default_rule_registry(),
                    data_type=dataset["data_type"],
                    product_type=dataset.get("product_type"),
                    enabled_optional_rules=enabled_optional,
                )
                _, created = _allocate_quality_run(
                    tx,
                    dataset_id=event["dataset_id"],
                    output_version=event["output_version"],
                    expected_current_output_version=None,
                    quality_run_id=uuid5(NAMESPACE_URL, f"cube-quality:{event['event_id']}"),
                    trigger_event_id=UUID(str(event["event_id"])),
                    trigger="automatic",
                    requested_by="system:partition-outbox",
                    rule_set_version=DEFAULT_RULE_SET_VERSION,
                    rule_snapshot=snapshots,
                )
            base_store.acknowledge_outbox(event["event_id"])
            allocated += int(created)
        except Exception as exc:
            logger.exception(
                "quality outbox dispatch failed for event_id=%s dataset_id=%s output_version=%s",
                event.get("event_id"),
                event.get("dataset_id"),
                event.get("output_version"),
            )
            try:
                base_store.retry_outbox(
                    event["event_id"],
                    _safe_execution_error(exc, "quality outbox dispatch failed"),
                    available_at=(now + timedelta(seconds=30)).isoformat(),
                )
            except Exception:
                logger.exception("quality outbox retry failed for event_id=%s", event.get("event_id"))
    return allocated


def _errors_from_findings(quality_run_id: UUID, rule_code: str, findings: tuple[QualityFinding, ...]) -> tuple[NewQualityError, ...]:
    return tuple(
        NewQualityError(
            quality_error_id=uuid4(),
            quality_run_id=quality_run_id,
            rule_code=rule_code,
            error_code=finding.error_code,
            message=finding.message,
            source_asset_id=finding.source_asset_id,
            band_code=finding.band_code,
            tile_id=finding.tile_id,
            index_id=finding.index_id,
            output_id=finding.output_id,
            row_number=finding.row_number,
            field=finding.field,
            context=None if finding.context is None else dict(finding.context),
        )
        for finding in findings
    )


def _write_findings(tx, *, lease: QualityLease, rule_code: str, findings: tuple[QualityFinding, ...]) -> None:
    errors = _errors_from_findings(lease.quality_run_id, rule_code, findings)
    for offset in range(0, len(errors), ERROR_BATCH_SIZE):
        if not heartbeat_quality_run(tx, lease=lease, now=datetime.now(UTC)):
            raise RuntimeError(f"quality run lease expired: {lease.quality_run_id}")
        write_quality_error_batch(tx, quality_run_id=lease.quality_run_id, errors=errors[offset : offset + ERROR_BATCH_SIZE])


def _finalize_failed_quality_run(
    store: Any,
    *,
    lease: QualityLease,
    results: list[QualityResult],
    exc: Exception,
) -> None:
    """Finalize a failed run in a fresh transaction.

    A database error aborts the transaction used by the rule runner.  Calling
    ``complete_quality_run_if_current`` on that same connection only raises
    ``current transaction is aborted`` and leaves the run in ``running``.  A
    new connection/transaction is the recovery boundary.
    """
    try:
        with store.transaction() as tx:
            # The execution transaction is rolled back on failure. Recreate any
            # rule-level terminal rows before closing the run so the quality
            # detail page still shows which rule failed.
            set_quality_lease_on_transaction(tx, lease)
            for result in results:
                finish_quality_result(tx, result=result)
            complete_quality_run_if_current(
                tx,
                quality_run_id=lease.quality_run_id,
                terminal_status="error",
                error_count=sum(r.error_count for r in results),
                warning_count=sum(r.warning_count for r in results),
                results_complete=False,
                execution_error=_safe_execution_error(exc, "quality run execution failed"),
                completed_at=datetime.now(UTC),
            )
    except (StaleQualityLease, QualityCompletionConflict):
        logger.debug("quality run was finalized elsewhere: %s", lease.quality_run_id)
    except Exception:
        logger.exception("Unable to finalize failed quality run %s", lease.quality_run_id)
        # Do not report a successful worker iteration when the durable
        # terminal transition failed. The lease can then be reconciled/retried.
        raise


def execute_quality_run(lease: QualityLease) -> None:
    store = require_open_gauss_domain_store()
    results: list[QualityResult] = []

    # Commit the running transition separately. This makes the error path able
    # to finalize even when the execution transaction is later aborted.
    try:
        with store.transaction() as tx:
            run = start_quality_run(tx, lease=lease, started_at=datetime.now(UTC))
    except (StaleQualityLease, QualityCompletionConflict):
        raise
    except Exception as exc:
        _finalize_failed_quality_run(store, lease=lease, results=results, exc=exc)
        return

    try:
        with store.transaction() as tx:
            # The lease marker lives on the transaction, so it must be
            # re-established here: this transaction is separate from the one
            # that ran start_quality_run and may be handed a different pooled
            # connection. Without it every rule write fails the lease fence.
            set_quality_lease_on_transaction(tx, lease)
            with tx.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT data_type, product_type FROM partition_datasets WHERE dataset_id = %s", (run.dataset_id,))
                dataset = cur.fetchone()
            if dataset is None:
                raise RuntimeError(f"quality dataset disappeared: {run.dataset_id}")
            registry = default_rule_registry()
            object_reader = quality_object_reader()
            for snapshot in run.rule_snapshot:
                rule = registry.get(snapshot.code)
                if rule is None or rule.implementation_version != snapshot.implementation_version:
                    raise RuntimeError(f"quality rule implementation unavailable: {snapshot.code}")
                started = datetime.now(UTC)
                try:
                    findings = tuple(
                        rule.evaluate(
                            RuleContext(
                                run.dataset_id,
                                run.output_version,
                                dataset["data_type"],
                                dataset.get("product_type"),
                                tx,
                                object_reader,
                            )
                        )
                    )
                    _write_findings(tx, lease=lease, rule_code=snapshot.code, findings=findings)
                    errors = len(findings) if snapshot.mandatory else 0
                    warnings = len(findings) if not snapshot.mandatory else 0
                    status = "fail" if errors else "warn" if warnings else "pass"
                    execution_error = None
                except Exception as rule_error:
                    results.append(
                        finish_quality_result(
                            tx,
                            result=QualityResult(
                                quality_run_id=lease.quality_run_id,
                                rule_code=snapshot.code,
                                status="error",
                                finding_count=0,
                                error_count=0,
                                warning_count=0,
                                metrics={},
                                execution_error=_safe_execution_error(rule_error, "quality rule execution failed"),
                                started_at=started,
                                completed_at=datetime.now(UTC),
                            ),
                        )
                    )
                    raise RuntimeError(f"quality rule failed: {snapshot.code}") from rule_error
                result = QualityResult(
                    quality_run_id=lease.quality_run_id,
                    rule_code=snapshot.code,
                    status=status,
                    finding_count=len(findings),
                    error_count=errors,
                    warning_count=warnings,
                    metrics={},
                    execution_error=execution_error,
                    started_at=started,
                    completed_at=datetime.now(UTC),
                )
                results.append(finish_quality_result(tx, result=result))
            error_count = sum(result.error_count for result in results)
            warning_count = sum(result.warning_count for result in results)
            assert_quality_result_totals(
                tx,
                lease.quality_run_id,
                error_count=error_count,
                warning_count=warning_count,
            )
            terminal_status = reduce_quality_status(results, None)
            is_current = complete_quality_run_if_current(
                tx,
                quality_run_id=lease.quality_run_id,
                terminal_status=terminal_status,
                error_count=error_count,
                warning_count=warning_count,
                results_complete=True,
                execution_error=None,
                completed_at=datetime.now(UTC),
            )
            with tx.cursor() as cur:
                cur.execute(
                    "UPDATE partition_data_unit_grid_status SET quality_status=%s,error_message=%s,updated_at=now() "
                    "WHERE dataset_id=%s AND output_version=%s AND partition_status='completed'",
                    (terminal_status, None if terminal_status in {"pass", "warn"} else "quality validation failed", run.dataset_id, run.output_version),
                )
            if is_current and auto_ingest_after_quality_enabled():
                create_ingest_runs_after_quality(
                    tx,
                    quality_run_id=lease.quality_run_id,
                    dataset_id=run.dataset_id,
                    output_version=run.output_version,
                    quality_status=terminal_status,
                )
    except (StaleQualityLease, QualityCompletionConflict):
        raise
    except Exception as exc:
        _finalize_failed_quality_run(store, lease=lease, results=results, exc=exc)


class QualityRuntime:
    def __init__(
        self,
        *,
        worker_id: str = "cube-web-quality",
        poll_seconds: float = 1.0,
        execution_workers: int | None = None,
        ingest_workers: int | None = None,
    ) -> None:
        self.worker_id = worker_id
        self.poll_seconds = poll_seconds
        self.execution_workers = max(1, execution_workers or _resolve_quality_max_workers())
        self.ingest_workers = max(1, ingest_workers or _resolve_ingest_max_workers())
        self._stop = Event()
        self._threads: list[Thread] = []

    def start(self) -> None:
        if self._threads:
            return
        self._stop.clear()
        self._threads = [
            Thread(target=self._dispatch_loop, name="cube-web-quality-dispatch", daemon=True),
            Thread(target=self._execute_loop, name="cube-web-quality-execute", daemon=True),
            Thread(target=self._ingest_loop, name="cube-web-ingest", daemon=True),
        ]
        for thread in self._threads:
            thread.start()

    def stop(self) -> None:
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout=max(1.0, self.poll_seconds * 4))
        self._threads = []

    def _dispatch_loop(self) -> None:
        while not self._stop.is_set():
            try:
                dispatch_quality_events(worker_id=f"{self.worker_id}:dispatch")
            except Exception:
                logger.exception("quality outbox dispatch failed")
            self._stop.wait(self.poll_seconds)

    def _execute_loop(self) -> None:
        while not self._stop.is_set():
            try:
                store = require_open_gauss_domain_store()
                with store.transaction() as tx:
                    leases = claim_quality_runs(tx, worker_id=f"{self.worker_id}:execute", limit=10)
                self._execute_claimed_runs(leases)
            except Exception:
                logger.exception("quality worker iteration failed")
            self._stop.wait(self.poll_seconds)

    def _execute_claimed_runs(self, leases: list[QualityLease]) -> None:
        if self._stop.is_set() or not leases:
            return
        if self.execution_workers == 1:
            for lease in leases:
                if self._stop.is_set():
                    return
                self._execute_claimed_run(lease)
            return
        for offset in range(0, len(leases), self.execution_workers):
            if self._stop.is_set():
                return
            active = leases[offset : offset + self.execution_workers]
            with ThreadPoolExecutor(max_workers=len(active), thread_name_prefix="cube-web-quality-run") as pool:
                futures = [pool.submit(self._execute_claimed_run, lease) for lease in active]
                for future in futures:
                    future.result()

    @staticmethod
    def _execute_claimed_run(lease: QualityLease) -> None:
        try:
            execute_quality_run(lease)
        except (StaleQualityLease, QualityCompletionConflict):
            logger.debug("quality lease was completed or replaced before execution: %s", lease.quality_run_id)
        except Exception:
            # One bad run must not abort the remainder of a claimed batch.  The
            # run-level handler normally persists an error status; this log is
            # the fallback signal when even that terminal transition failed.
            logger.exception("quality run worker failed for %s", lease.quality_run_id)

    def _ingest_loop(self) -> None:
        while not self._stop.is_set():
            try:
                process_queued_ingest_scenes(limit=10, max_workers=self.ingest_workers)
            except Exception:
                logger.exception("ingest worker iteration failed")
            self._stop.wait(self.poll_seconds)
