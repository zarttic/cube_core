from __future__ import annotations

import logging
import re
from datetime import UTC, datetime, timedelta
from threading import Event, Thread
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from psycopg.rows import dict_row

from cube_web.services.ingest_worker import process_queued_ingest_scenes
from cube_web.services.partition_domain_store import get_partition_domain_store
from cube_web.services.quality_contracts import QualityResult
from cube_web.services.quality_object_reader import quality_object_reader
from cube_web.services.quality_repository import (
    ERROR_BATCH_SIZE,
    NewQualityError,
    QualityLease,
    _allocate_quality_run,
    assert_quality_result_totals,
    complete_quality_run_if_current,
    finish_quality_result,
    require_open_gauss_domain_store,
    start_quality_run,
    write_quality_error_batch,
)
from cube_web.services.config_store import get_enabled_optional_quality_rules
from cube_web.services.quality_rules import (
    DEFAULT_RULE_SET_VERSION,
    QualityFinding,
    RuleContext,
    default_rule_registry,
    reduce_quality_status,
    snapshot_rules,
)

logger = logging.getLogger(__name__)


def _safe_execution_error(exc: Exception, prefix: str) -> str:
    exception_type = type(exc).__name__
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", exception_type) is None:
        exception_type = "Exception"
    return f"{prefix} ({exception_type})"


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
            base_store.retry_outbox(
                event["event_id"],
                _safe_execution_error(exc, "quality outbox dispatch failed"),
                available_at=(now + timedelta(seconds=30)).isoformat(),
            )
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


def execute_quality_run(lease: QualityLease) -> None:
    store = require_open_gauss_domain_store()
    with store.transaction() as tx:
        run = start_quality_run(tx, lease=lease, started_at=datetime.now(UTC))
        with tx.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT data_type, product_type FROM partition_datasets WHERE dataset_id = %s", (run.dataset_id,))
            dataset = cur.fetchone()
        results: list[QualityResult] = []
        quality_completed = False
        try:
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
                except Exception as exc:
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
                                execution_error=_safe_execution_error(exc, "quality rule execution failed"),
                                started_at=started,
                                completed_at=datetime.now(UTC),
                            ),
                        )
                    )
                    raise RuntimeError(f"quality rule failed: {snapshot.code}") from exc
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
            quality_completed = True
            # Ingest is an explicit data-management action. Quality completion
            # only records the gate result; it must not enqueue ingest work.
        except Exception as exc:
            if quality_completed:
                raise
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


class QualityRuntime:
    def __init__(self, *, worker_id: str = "cube-web-quality", poll_seconds: float = 1.0) -> None:
        self.worker_id = worker_id
        self.poll_seconds = poll_seconds
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
                for lease in leases:
                    if self._stop.is_set():
                        break
                    execute_quality_run(lease)
            except Exception:
                logger.exception("quality worker iteration failed")
            self._stop.wait(self.poll_seconds)

    def _ingest_loop(self) -> None:
        while not self._stop.is_set():
            try:
                process_queued_ingest_scenes(limit=10)
            except Exception:
                logger.exception("ingest worker iteration failed")
            self._stop.wait(self.poll_seconds)
