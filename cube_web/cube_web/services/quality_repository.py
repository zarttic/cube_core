from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterator, Literal, Sequence, TypeAlias
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from cube_web.services.partition_domain_store import OpenGaussPartitionDomainStore, get_partition_domain_store
from cube_web.services.quality_contracts import (
    ERROR_BATCH_SIZE,
    QualityError,
    QualityErrorFilter,
    QualityResult,
    QualityRun,
    QualityStatus,
    RuleSnapshot,
    SortOrder,
)

PartitionDomainTransaction: TypeAlias = psycopg.Connection[Any]


class DatasetNotFound(LookupError):
    pass


class OutputVersionNotFound(LookupError):
    pass


class OutputVersionNotCompleted(RuntimeError):
    pass


class QualityRunNotFound(LookupError):
    pass


class QualityTriggerConflict(RuntimeError):
    pass


class QualityCompletionConflict(RuntimeError):
    pass


class QualitySummaryMismatch(RuntimeError):
    pass


class StaleQualityLease(RuntimeError):
    pass


@dataclass(frozen=True)
class QualityLease:
    quality_run_id: UUID
    claimed_by: str
    attempt_count: int


@dataclass(frozen=True)
class NewQualityError:
    quality_error_id: UUID
    quality_run_id: UUID
    rule_code: str
    error_code: str
    message: str
    source_asset_id: str | None = None
    tile_id: str | None = None
    index_id: str | None = None
    output_id: str | None = None
    row_number: int | None = None
    field: str | None = None
    context: dict[str, Any] | None = None
    created_at: datetime | None = None


_ERROR_FILTER_COLUMNS = {
    "rule_code": "rule_code",
    "error_code": "error_code",
    "source_asset_id": "source_asset_id",
    "output_id": "output_id",
    "field": "field_name",
}
_RUN_SORT_COLUMNS = {
    "created_at": "q.created_at",
    "completed_at": "q.completed_at",
    "generated_at": "COALESCE(q.completed_at, q.started_at, q.created_at)",
    "quality_sequence": "q.quality_sequence",
    "status": "q.status",
}
_RESULT_SORT_COLUMNS = {
    "rule_code": "rule_code",
    "completed_at": "completed_at",
    "status": "status",
}
_ERROR_SORT_COLUMNS = {
    "created_at": "created_at",
    "quality_error_id": "quality_error_id",
    "rule_code": "rule_code",
    "error_code": "error_code",
}


def require_open_gauss_domain_store() -> OpenGaussPartitionDomainStore:
    store = get_partition_domain_store()
    if not isinstance(store, OpenGaussPartitionDomainStore):
        raise RuntimeError("M3 transactional quality/publication requires OpenGaussPartitionDomainStore")
    return store


def set_quality_lease_on_transaction(tx: PartitionDomainTransaction, lease: QualityLease) -> None:
    leases = getattr(tx, "_cube_quality_leases", None)
    if leases is None:
        leases = {}
        setattr(tx, "_cube_quality_leases", leases)
    leases[lease.quality_run_id] = lease


def clear_quality_lease_from_transaction(tx: PartitionDomainTransaction, quality_run_id: UUID) -> None:
    leases = getattr(tx, "_cube_quality_leases", None)
    if leases is not None:
        leases.pop(quality_run_id, None)


def quality_lease_from_transaction(tx: PartitionDomainTransaction, quality_run_id: UUID) -> QualityLease | None:
    leases = getattr(tx, "_cube_quality_leases", None)
    return None if leases is None else leases.get(quality_run_id)


def _snapshot_json(snapshot: Sequence[RuleSnapshot]) -> list[dict[str, Any]]:
    return [item.model_dump(mode="json") for item in snapshot]


def _json_value(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _quality_run(row: dict[str, Any], *, is_current: bool) -> QualityRun:
    snapshot = _json_value(row["rule_snapshot"])
    return QualityRun(
        quality_run_id=row["quality_run_id"],
        dataset_id=row["dataset_id"],
        dataset_code=row["dataset_code"],
        batch_id=row["batch_id"],
        data_type=row["data_type"],
        product_type=row.get("product_type"),
        partition_status=row["partition_status"],
        output_version=row["output_version"],
        quality_sequence=int(row["quality_sequence"]),
        trigger_event_id=row.get("trigger_event_id"),
        trigger=row["trigger"],
        requested_by=row["requested_by"],
        rule_set_version=row["rule_set_version"],
        rule_snapshot=tuple(RuleSnapshot.model_validate(item) for item in snapshot),
        status=row["status"],
        results_complete=bool(row["result_complete"]),
        error_count=int(row["error_count"]),
        warning_count=int(row["warning_count"]),
        execution_error=row.get("last_error"),
        started_at=row.get("started_at"),
        completed_at=row.get("completed_at"),
        created_at=row["created_at"],
        is_current=is_current,
    )


def _quality_run_with_dataset(row: dict[str, Any], dataset: dict[str, Any], *, is_current: bool) -> QualityRun:
    return _quality_run({**row, **{key: dataset[key] for key in ("dataset_code", "batch_id", "data_type", "product_type", "partition_status")}}, is_current=is_current)


def _quality_result(row: dict[str, Any]) -> QualityResult:
    return QualityResult(
        quality_run_id=row["quality_run_id"],
        rule_code=row["rule_code"],
        status=row["status"],
        finding_count=int(row["finding_count"]),
        error_count=int(row["error_count"]),
        warning_count=int(row["warning_count"]),
        metrics=_json_value(row["metrics"]),
        execution_error=row.get("execution_error"),
        started_at=row["started_at"],
        completed_at=row["completed_at"],
    )


def _quality_error(row: dict[str, Any]) -> QualityError:
    return QualityError(
        quality_error_id=row["quality_error_id"],
        quality_run_id=row["quality_run_id"],
        rule_code=row["rule_code"],
        source_asset_id=row.get("source_asset_id"),
        tile_id=row.get("tile_id"),
        index_id=row.get("index_id"),
        output_id=row.get("output_id"),
        row_number=row.get("row_number"),
        field=row.get("field_name"),
        error_code=row["error_code"],
        message=row["message"],
        context=_json_value(row["context"]),
        created_at=row["created_at"],
    )


def lock_dataset(tx: PartitionDomainTransaction, dataset_id: str) -> dict[str, Any]:
    with tx.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM partition_datasets WHERE dataset_id = %s FOR UPDATE", (dataset_id,))
        dataset = cur.fetchone()
    if dataset is None:
        raise DatasetNotFound(dataset_id)
    return dataset


def lock_quality_run(tx: PartitionDomainTransaction, quality_run_id: UUID) -> dict[str, Any]:
    with tx.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM partition_quality_runs WHERE quality_run_id = %s FOR UPDATE", (quality_run_id,))
        run = cur.fetchone()
    if run is None:
        raise QualityRunNotFound(str(quality_run_id))
    return run


def _locked_output(tx: PartitionDomainTransaction, dataset_id: str, output_version: str) -> dict[str, Any]:
    with tx.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT * FROM partition_output_versions WHERE dataset_id = %s AND output_version = %s FOR KEY SHARE",
            (dataset_id, output_version),
        )
        output = cur.fetchone()
    if output is None:
        raise OutputVersionNotFound(output_version)
    if output["status"] != "completed":
        raise OutputVersionNotCompleted(output_version)
    return output


def _allocate_quality_run(
    tx: PartitionDomainTransaction,
    *,
    dataset_id: str,
    output_version: str,
    expected_current_output_version: str | None,
    quality_run_id: UUID,
    trigger_event_id: UUID | None,
    trigger: Literal["automatic", "manual"],
    requested_by: str,
    rule_set_version: str,
    rule_snapshot: Sequence[RuleSnapshot],
) -> tuple[QualityRun, bool]:
    snapshot = _snapshot_json(rule_snapshot)
    dataset = lock_dataset(tx, dataset_id)
    if expected_current_output_version is not None and dataset["current_output_version"] != expected_current_output_version:
        raise QualityTriggerConflict("current output version changed before quality allocation")
    _locked_output(tx, dataset_id, output_version)

    with tx.cursor(row_factory=dict_row) as cur:
        if trigger_event_id is not None:
            cur.execute("SELECT * FROM partition_quality_runs WHERE trigger_event_id = %s", (trigger_event_id,))
            existing = cur.fetchone()
            if existing is not None:
                if (
                    existing["dataset_id"] != dataset_id
                    or existing["output_version"] != output_version
                    or existing["trigger"] != "automatic"
                    or existing["rule_set_version"] != rule_set_version
                    or _json_value(existing["rule_snapshot"]) != snapshot
                ):
                    raise QualityTriggerConflict("trigger_event_id is already bound to another quality identity")
                return _quality_run_with_dataset(existing, dataset, is_current=existing["quality_run_id"] == dataset.get("current_quality_run_id")), False

        sequence = int(dataset["quality_sequence"]) + 1
        cur.execute(
            "UPDATE partition_datasets SET quality_sequence = %s, updated_at = now() WHERE dataset_id = %s",
            (sequence, dataset_id),
        )
        cur.execute(
            "INSERT INTO partition_quality_runs "
            "(quality_run_id, dataset_id, output_version, quality_sequence, trigger_event_id, trigger, requested_by, "
            "rule_set_version, rule_snapshot, status, result_complete, available_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', FALSE, now()) RETURNING *",
            (
                quality_run_id,
                dataset_id,
                output_version,
                sequence,
                trigger_event_id,
                trigger,
                requested_by,
                rule_set_version,
                Jsonb(snapshot),
            ),
        )
        row = cur.fetchone()
        is_current = output_version == dataset["current_output_version"]
        if is_current:
            cur.execute(
                "UPDATE partition_datasets SET current_quality_run_id = %s, quality_status = 'pending', "
                "quality_error_count = 0, quality_warning_count = 0, updated_at = now() WHERE dataset_id = %s",
                (quality_run_id, dataset_id),
            )
        return _quality_run_with_dataset(row, dataset, is_current=is_current), True


def allocate_quality_run(
    tx: PartitionDomainTransaction,
    *,
    dataset_id: str,
    output_version: str,
    expected_current_output_version: str | None,
    quality_run_id: UUID,
    trigger_event_id: UUID | None,
    trigger: Literal["automatic", "manual"],
    requested_by: str,
    rule_set_version: str,
    rule_snapshot: Sequence[RuleSnapshot],
) -> QualityRun:
    run, _ = _allocate_quality_run(
        tx,
        dataset_id=dataset_id,
        output_version=output_version,
        expected_current_output_version=expected_current_output_version,
        quality_run_id=quality_run_id,
        trigger_event_id=trigger_event_id,
        trigger=trigger,
        requested_by=requested_by,
        rule_set_version=rule_set_version,
        rule_snapshot=rule_snapshot,
    )
    return run


def start_quality_run(
    tx: PartitionDomainTransaction,
    *,
    lease: QualityLease,
    started_at: datetime,
) -> QualityRun:
    run = lock_quality_run(tx, lease.quality_run_id)
    if run["status"] not in {"pending", "running"}:
        raise QualityCompletionConflict("quality run must be pending or running")
    with tx.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "UPDATE partition_quality_runs SET status = 'running', started_at = COALESCE(started_at, %s), updated_at = %s "
            "WHERE quality_run_id = %s AND claimed_by = %s AND attempt_count = %s AND status IN ('pending', 'running') RETURNING *",
            (started_at, started_at, lease.quality_run_id, lease.claimed_by, lease.attempt_count),
        )
        row = cur.fetchone()
        if row is None:
            raise StaleQualityLease(str(lease.quality_run_id))
        cur.execute(
            "UPDATE partition_datasets SET quality_status = 'running', updated_at = %s "
            "WHERE dataset_id = %s AND current_output_version = %s AND current_quality_run_id = %s",
            (started_at, row["dataset_id"], row["output_version"], lease.quality_run_id),
        )
        cur.execute(
            "SELECT dataset_code, batch_id, data_type, product_type, partition_status, current_quality_run_id "
            "FROM partition_datasets WHERE dataset_id = %s",
            (row["dataset_id"],),
        )
        dataset = cur.fetchone()
    set_quality_lease_on_transaction(tx, lease)
    return _quality_run_with_dataset(row, dataset, is_current=dataset["current_quality_run_id"] == lease.quality_run_id)


def complete_quality_run_if_current(
    tx: PartitionDomainTransaction,
    *,
    quality_run_id: UUID,
    terminal_status: QualityStatus,
    error_count: int,
    warning_count: int,
    results_complete: bool,
    execution_error: str | None,
    completed_at: datetime,
) -> bool:
    if terminal_status not in {"pass", "warn", "fail", "error", "cancelled"}:
        raise ValueError("terminal_status must be pass, warn, fail, error, or cancelled")
    if error_count < 0 or warning_count < 0:
        raise ValueError("quality counts must be non-negative")
    run = lock_quality_run(tx, quality_run_id)
    if run["status"] != "running":
        raise QualityCompletionConflict("quality run must be running")
    lease = quality_lease_from_transaction(tx, quality_run_id)
    if lease is None:
        raise StaleQualityLease(str(quality_run_id))

    with tx.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "UPDATE partition_quality_runs SET status = %s, error_count = %s, warning_count = %s, "
            "result_complete = %s, last_error = %s, completed_at = %s, updated_at = %s, claimed_at = NULL, claimed_by = NULL "
            "WHERE quality_run_id = %s AND claimed_by = %s AND attempt_count = %s AND status = 'running' RETURNING dataset_id",
            (
                terminal_status,
                error_count,
                warning_count,
                results_complete,
                execution_error,
                completed_at,
                completed_at,
                quality_run_id,
                lease.claimed_by,
                lease.attempt_count,
            ),
        )
        if cur.fetchone() is None:
            raise StaleQualityLease(str(quality_run_id))
        cur.execute(
            "UPDATE partition_datasets d SET current_quality_run_id = %s, quality_status = %s, quality_error_count = %s, "
            "quality_warning_count = %s, updated_at = %s WHERE d.dataset_id = %s AND d.current_output_version = %s "
            "AND NOT EXISTS (SELECT 1 FROM partition_quality_runs newer WHERE newer.dataset_id = d.dataset_id "
            "AND newer.output_version = %s AND newer.quality_sequence > %s) RETURNING dataset_id",
            (
                quality_run_id,
                terminal_status,
                error_count,
                warning_count,
                completed_at,
                run["dataset_id"],
                run["output_version"],
                run["output_version"],
                run["quality_sequence"],
            ),
        )
        updated = cur.fetchone() is not None
    clear_quality_lease_from_transaction(tx, quality_run_id)
    return updated


def _error_predicate(quality_run_id: UUID, filters: QualityErrorFilter) -> tuple[str, list[Any]]:
    clauses = ["quality_run_id = %s"]
    params: list[Any] = [quality_run_id]
    for field, column in _ERROR_FILTER_COLUMNS.items():
        value = getattr(filters, field)
        if value is not None:
            clauses.append(f"{column} = %s")
            params.append(value)
    return " AND ".join(clauses), params


def write_quality_error_batch(
    tx: PartitionDomainTransaction,
    *,
    quality_run_id: UUID,
    errors: Sequence[NewQualityError],
) -> int:
    if len(errors) > ERROR_BATCH_SIZE:
        raise ValueError(f"quality error batches must contain at most {ERROR_BATCH_SIZE} rows")
    if not errors:
        return 0
    run = lock_quality_run(tx, quality_run_id)
    lease = quality_lease_from_transaction(tx, quality_run_id)
    if lease is None or run["status"] != "running":
        raise StaleQualityLease(str(quality_run_id))
    if any(error.quality_run_id != quality_run_id for error in errors):
        raise ValueError("quality errors must bind the target quality run")
    values = [
        (
            error.quality_error_id,
            error.quality_run_id,
            run["dataset_id"],
            run["output_version"],
            error.rule_code,
            error.source_asset_id,
            error.tile_id,
            error.index_id,
            error.output_id,
            error.row_number,
            error.field,
            error.error_code,
            error.message,
            Jsonb(error.context or {}),
            error.created_at,
        )
        for error in errors
    ]
    with tx.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM partition_quality_runs WHERE quality_run_id = %s AND claimed_by = %s "
            "AND attempt_count = %s AND status = 'running'",
            (quality_run_id, lease.claimed_by, lease.attempt_count),
        )
        if cur.fetchone() is None:
            raise StaleQualityLease(str(quality_run_id))
        cur.executemany(
            "INSERT INTO partition_quality_errors "
            "(quality_error_id, quality_run_id, dataset_id, output_version, rule_code, source_asset_id, tile_id, index_id, "
            "output_id, row_number, field_name, error_code, message, context, created_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, now()))",
            values,
        )
    return len(values)


def finish_quality_result(tx: PartitionDomainTransaction, *, result: QualityResult) -> QualityResult:
    run = lock_quality_run(tx, result.quality_run_id)
    lease = quality_lease_from_transaction(tx, result.quality_run_id)
    if (
        lease is None
        or run["status"] != "running"
        or run["claimed_by"] != lease.claimed_by
        or int(run["attempt_count"]) != lease.attempt_count
    ):
        raise StaleQualityLease(str(result.quality_run_id))
    with tx.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "UPDATE partition_quality_results SET status = %s, finding_count = %s, error_count = %s, warning_count = %s, "
            "metrics = %s, execution_error = %s, started_at = %s, completed_at = %s "
            "WHERE quality_run_id = %s AND rule_code = %s "
            "RETURNING *",
            (
                result.status,
                result.finding_count,
                result.error_count,
                result.warning_count,
                Jsonb(result.metrics),
                result.execution_error,
                result.started_at,
                result.completed_at,
                result.quality_run_id,
                result.rule_code,
            ),
        )
        row = cur.fetchone()
        if row is None:
            cur.execute(
                "INSERT INTO partition_quality_results "
                "(quality_run_id, dataset_id, output_version, rule_code, status, finding_count, error_count, warning_count, "
                "metrics, execution_error, started_at, completed_at) VALUES "
                "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *",
                (
                    result.quality_run_id,
                    run["dataset_id"],
                    run["output_version"],
                    result.rule_code,
                    result.status,
                    result.finding_count,
                    result.error_count,
                    result.warning_count,
                    Jsonb(result.metrics),
                    result.execution_error,
                    result.started_at,
                    result.completed_at,
                ),
            )
            row = cur.fetchone()
    if row is None:
        raise StaleQualityLease(str(result.quality_run_id))
    return _quality_result(row)


def quality_result_totals(tx: PartitionDomainTransaction, quality_run_id: UUID) -> tuple[int, int]:
    with tx.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT COALESCE(sum(error_count), 0) AS error_count, COALESCE(sum(warning_count), 0) AS warning_count "
            "FROM partition_quality_results WHERE quality_run_id = %s",
            (quality_run_id,),
        )
        row = cur.fetchone()
    return int(row["error_count"]), int(row["warning_count"])


def assert_quality_result_totals(tx: PartitionDomainTransaction, quality_run_id: UUID, *, error_count: int, warning_count: int) -> None:
    actual_errors, actual_warnings = quality_result_totals(tx, quality_run_id)
    if (actual_errors, actual_warnings) != (error_count, warning_count):
        raise QualitySummaryMismatch(
            f"quality result totals {(actual_errors, actual_warnings)} do not match run totals {(error_count, warning_count)}"
        )


def list_quality_errors(
    tx: PartitionDomainTransaction,
    *,
    quality_run_id: UUID,
    filters: QualityErrorFilter,
    limit: int,
    offset: int,
    sort_by: str = "created_at",
    sort_order: SortOrder = "asc",
) -> list[QualityError]:
    if not 1 <= limit <= 500 or offset < 0 or sort_by not in _ERROR_SORT_COLUMNS:
        raise ValueError("invalid quality error pagination or sort")
    where, params = _error_predicate(quality_run_id, filters)
    direction = "DESC" if sort_order == "desc" else "ASC"
    with tx.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT quality_error_id, quality_run_id, rule_code, source_asset_id, tile_id, index_id, output_id, row_number, "
            "field_name, error_code, message, context, created_at FROM partition_quality_errors "
            f"WHERE {where} ORDER BY {_ERROR_SORT_COLUMNS[sort_by]} {direction}, quality_error_id {direction} LIMIT %s OFFSET %s",
            (*params, limit, offset),
        )
        return [_quality_error(row) for row in cur.fetchall()]


def count_quality_errors(tx: PartitionDomainTransaction, *, quality_run_id: UUID, filters: QualityErrorFilter) -> int:
    where, params = _error_predicate(quality_run_id, filters)
    with tx.cursor(row_factory=dict_row) as cur:
        cur.execute(f"SELECT count(*) AS total FROM partition_quality_errors WHERE {where}", params)
        row = cur.fetchone()
    return int(row["total"])


def iter_quality_errors(
    tx: PartitionDomainTransaction,
    *,
    quality_run_id: UUID,
    filters: QualityErrorFilter,
    fetch_size: int = ERROR_BATCH_SIZE,
) -> Iterator[QualityError]:
    if fetch_size < 1 or fetch_size > ERROR_BATCH_SIZE:
        raise ValueError(f"fetch_size must be between 1 and {ERROR_BATCH_SIZE}")
    where, params = _error_predicate(quality_run_id, filters)
    cursor_name = f"quality_export_{quality_run_id.hex}"
    with tx.cursor(name=cursor_name, row_factory=dict_row) as cur:
        cur.itersize = fetch_size
        cur.execute(
            "SELECT quality_error_id, quality_run_id, rule_code, source_asset_id, tile_id, index_id, output_id, row_number, "
            "field_name, error_code, message, context, created_at FROM partition_quality_errors "
            f"WHERE {where} ORDER BY quality_error_id",
            params,
        )
        for row in cur:
            yield _quality_error(row)


def list_quality_results(
    tx: PartitionDomainTransaction,
    *,
    quality_run_id: UUID,
    limit: int,
    offset: int,
    sort_by: str = "rule_code",
    sort_order: SortOrder = "asc",
) -> list[QualityResult]:
    if not 1 <= limit <= 500 or offset < 0 or sort_by not in _RESULT_SORT_COLUMNS:
        raise ValueError("invalid quality result pagination or sort")
    direction = "DESC" if sort_order == "desc" else "ASC"
    with tx.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT * FROM partition_quality_results WHERE quality_run_id = %s "
            f"ORDER BY {_RESULT_SORT_COLUMNS[sort_by]} {direction}, rule_code {direction} LIMIT %s OFFSET %s",
            (quality_run_id, limit, offset),
        )
        return [_quality_result(row) for row in cur.fetchall()]


def count_quality_results(tx: PartitionDomainTransaction, *, quality_run_id: UUID) -> int:
    with tx.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT count(*) AS total FROM partition_quality_results WHERE quality_run_id = %s", (quality_run_id,))
        row = cur.fetchone()
    return int(row["total"])


def get_quality_run(tx: PartitionDomainTransaction, *, quality_run_id: UUID) -> QualityRun:
    with tx.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT q.*, d.dataset_code, d.batch_id, d.data_type, d.product_type, d.partition_status, q.quality_run_id = d.current_quality_run_id AS is_current "
            "FROM partition_quality_runs q JOIN partition_datasets d ON d.dataset_id = q.dataset_id "
            "WHERE q.quality_run_id = %s",
            (quality_run_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise QualityRunNotFound(str(quality_run_id))
    return _quality_run(row, is_current=bool(row["is_current"]))


def list_quality_runs(
    tx: PartitionDomainTransaction,
    *,
    keyword: str | None,
    dataset_id: str | None,
    output_version: str | None,
    data_type: str | None,
    status: str | None,
    trigger: str | None,
    requested_by: str | None,
    current_only: bool,
    started_from: datetime | None,
    started_to: datetime | None,
    limit: int,
    offset: int,
    sort_by: str = "generated_at",
    sort_order: SortOrder = "desc",
) -> list[QualityRun]:
    if not 1 <= limit <= 500 or offset < 0 or sort_by not in _RUN_SORT_COLUMNS:
        raise ValueError("invalid quality run pagination or sort")
    clauses = ["1=1"]
    params: list[Any] = []
    if keyword is not None:
        clauses.append("(d.dataset_code ILIKE %s OR d.dataset_title ILIKE %s)")
        params.extend((f"%{keyword}%", f"%{keyword}%"))
    for column, value in (
        ("q.dataset_id", dataset_id),
        ("q.output_version", output_version),
        ("d.data_type", data_type),
        ("q.status", status),
        ("q.trigger", trigger),
        ("q.requested_by", requested_by),
    ):
        if value is not None:
            clauses.append(f"{column} = %s")
            params.append(value)
    if current_only:
        clauses.append("q.quality_run_id = d.current_quality_run_id")
    if started_from is not None:
        clauses.append("q.started_at >= %s")
        params.append(started_from)
    if started_to is not None:
        clauses.append("q.started_at <= %s")
        params.append(started_to)
    direction = "DESC" if sort_order == "desc" else "ASC"
    with tx.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT q.*, d.dataset_code, d.batch_id, d.data_type, d.product_type, d.partition_status, q.quality_run_id = d.current_quality_run_id AS is_current "
            "FROM partition_quality_runs q JOIN partition_datasets d ON d.dataset_id = q.dataset_id "
            f"WHERE {' AND '.join(clauses)} ORDER BY {_RUN_SORT_COLUMNS[sort_by]} {direction}, q.quality_run_id {direction} LIMIT %s OFFSET %s",
            (*params, limit, offset),
        )
        return [_quality_run(row, is_current=bool(row["is_current"])) for row in cur.fetchall()]


def count_quality_runs(
    tx: PartitionDomainTransaction,
    *,
    keyword: str | None,
    dataset_id: str | None,
    output_version: str | None,
    data_type: str | None,
    status: str | None,
    trigger: str | None,
    requested_by: str | None,
    current_only: bool,
    started_from: datetime | None,
    started_to: datetime | None,
) -> int:
    clauses = ["1=1"]
    params: list[Any] = []
    if keyword is not None:
        clauses.append("(d.dataset_code ILIKE %s OR d.dataset_title ILIKE %s)")
        params.extend((f"%{keyword}%", f"%{keyword}%"))
    for column, value in (
        ("q.dataset_id", dataset_id),
        ("q.output_version", output_version),
        ("d.data_type", data_type),
        ("q.status", status),
        ("q.trigger", trigger),
        ("q.requested_by", requested_by),
    ):
        if value is not None:
            clauses.append(f"{column} = %s")
            params.append(value)
    if current_only:
        clauses.append("q.quality_run_id = d.current_quality_run_id")
    if started_from is not None:
        clauses.append("q.started_at >= %s")
        params.append(started_from)
    if started_to is not None:
        clauses.append("q.started_at <= %s")
        params.append(started_to)
    with tx.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT count(*) AS total FROM partition_quality_runs q JOIN partition_datasets d ON d.dataset_id = q.dataset_id "
            f"WHERE {' AND '.join(clauses)}",
            params,
        )
        row = cur.fetchone()
    return int(row["total"])
