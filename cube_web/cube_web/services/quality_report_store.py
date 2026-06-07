from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from cube_split.runtime_config import require_postgres_dsn


class QualityReportStore:
    def ensure_schema(self) -> None:
        raise NotImplementedError

    def upsert_report(self, data_type: str, run_dir: Path | str, report: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    def get_report(self, data_type: str, report_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def latest_report(self, data_type: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def list_reports(self, data_type: str, limit: int = 20) -> list[dict[str, Any]]:
        raise NotImplementedError


class PostgresQualityReportStore(QualityReportStore):
    def __init__(self, dsn: str) -> None:
        if not dsn:
            raise ValueError("PostgreSQL DSN is required")
        self.dsn = dsn

    def ensure_schema(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS quality_reports (
                      id BIGSERIAL PRIMARY KEY,
                      report_id UUID NOT NULL UNIQUE,
                      data_type TEXT NOT NULL,
                      run_dir TEXT NOT NULL UNIQUE,
                      run_name TEXT NOT NULL,
                      dataset TEXT NOT NULL,
                      status TEXT NOT NULL,
                      target_crs TEXT,
                      generated_at TIMESTAMPTZ NOT NULL,
                      summary JSONB NOT NULL,
                      checks JSONB NOT NULL,
                      assets JSONB NOT NULL,
                      report JSONB NOT NULL,
                      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_quality_reports_type_generated
                    ON quality_reports (data_type, generated_at DESC)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_quality_reports_status
                    ON quality_reports (status)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_quality_reports_summary_gin
                    ON quality_reports USING GIN (summary)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_quality_reports_report_gin
                    ON quality_reports USING GIN (report)
                    """
                )
            conn.commit()

    def upsert_report(self, data_type: str, run_dir: Path | str, report: dict[str, Any]) -> dict[str, Any]:
        self.ensure_schema()
        record = _quality_report_record(data_type, run_dir, report)
        params = _jsonb_record(record)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    MERGE INTO quality_reports target
                    USING (
                      SELECT
                        %(report_id)s::uuid AS report_id,
                        %(data_type)s::text AS data_type,
                        %(run_dir)s::text AS run_dir,
                        %(run_name)s::text AS run_name,
                        %(dataset)s::text AS dataset,
                        %(status)s::text AS status,
                        %(target_crs)s::text AS target_crs,
                        %(generated_at)s::timestamptz AS generated_at,
                        %(summary)s::jsonb AS summary,
                        %(checks)s::jsonb AS checks,
                        %(assets)s::jsonb AS assets,
                        %(report)s::jsonb AS report
                    ) source
                    ON (target.run_dir = source.run_dir)
                    WHEN MATCHED THEN UPDATE SET
                      report_id = source.report_id,
                      data_type = source.data_type,
                      run_name = source.run_name,
                      dataset = source.dataset,
                      status = source.status,
                      target_crs = source.target_crs,
                      generated_at = source.generated_at,
                      summary = source.summary,
                      checks = source.checks,
                      assets = source.assets,
                      report = source.report,
                      updated_at = now()
                    WHEN NOT MATCHED THEN INSERT (
                      report_id, data_type, run_dir, run_name, dataset, status,
                      target_crs, generated_at, summary, checks, assets, report
                    ) VALUES (
                      source.report_id, source.data_type, source.run_dir, source.run_name, source.dataset, source.status,
                      source.target_crs, source.generated_at, source.summary, source.checks, source.assets, source.report
                    )
                    """,
                    params,
                )
                cur.execute(
                    "SELECT report FROM quality_reports WHERE data_type = %s AND run_dir = %s",
                    (record["data_type"], record["run_dir"]),
                )
                row = cur.fetchone()
            conn.commit()
        return row[0]

    def get_report(self, data_type: str, report_id: str) -> dict[str, Any] | None:
        self.ensure_schema()
        if not _is_uuid(report_id):
            return None
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT report FROM quality_reports WHERE data_type = %s AND report_id = %s",
                    (data_type, report_id),
                )
                row = cur.fetchone()
        return None if row is None else row[0]

    def latest_report(self, data_type: str) -> dict[str, Any] | None:
        self.ensure_schema()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT report
                    FROM quality_reports
                    WHERE data_type = %s
                    ORDER BY generated_at DESC, updated_at DESC
                    LIMIT 1
                    """,
                    (data_type,),
                )
                row = cur.fetchone()
        return None if row is None else row[0]

    def list_reports(self, data_type: str, limit: int = 20) -> list[dict[str, Any]]:
        self.ensure_schema()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      report_id::text AS report_id,
                      data_type,
                      run_dir,
                      run_name,
                      dataset,
                      status,
                      target_crs,
                      generated_at,
                      summary,
                      created_at,
                      updated_at
                    FROM quality_reports
                    WHERE data_type = %s
                    ORDER BY generated_at DESC, updated_at DESC
                    LIMIT %s
                    """,
                    (data_type, limit),
                )
                rows = cur.fetchall()
        return [_history_row(row) for row in rows]

    def _connect(self):
        try:
            import psycopg
        except ModuleNotFoundError as exc:  # pragma: no cover - exercised only in incomplete installs.
            raise RuntimeError("PostgreSQL quality report storage requires `psycopg`") from exc
        return psycopg.connect(self.dsn, client_encoding="UTF8")


_store: QualityReportStore | None = None


def get_quality_report_store() -> QualityReportStore:
    global _store
    if _store is None:
        _store = PostgresQualityReportStore(require_postgres_dsn())
    return _store


def set_quality_report_store(store: QualityReportStore | None) -> None:
    global _store
    _store = store


def _quality_report_record(data_type: str, run_dir: Path | str, report: dict[str, Any]) -> dict[str, Any]:
    run_dir_path = Path(str(run_dir)).expanduser().resolve()
    report_copy = dict(report)
    report_id = str(report_copy.get("report_id") or uuid4())
    generated_at = _generated_at(report_copy)
    report_copy.update(
        {
            "report_id": report_id,
            "data_type": data_type,
            "run_dir": str(run_dir_path),
            "generated_at": generated_at.isoformat().replace("+00:00", "Z"),
        }
    )
    summary = report_copy.get("summary") if isinstance(report_copy.get("summary"), dict) else {}
    checks = report_copy.get("checks") if isinstance(report_copy.get("checks"), list) else []
    assets = report_copy.get("assets") if isinstance(report_copy.get("assets"), list) else []
    return {
        "report_id": report_id,
        "data_type": data_type,
        "run_dir": str(run_dir_path),
        "run_name": run_dir_path.name,
        "dataset": run_dir_path.parent.name,
        "status": str(report_copy.get("status") or "UNKNOWN"),
        "target_crs": report_copy.get("target_crs"),
        "generated_at": generated_at,
        "summary": summary,
        "checks": checks,
        "assets": assets,
        "report": report_copy,
    }


def _is_uuid(value: str) -> bool:
    try:
        UUID(str(value))
    except (TypeError, ValueError):
        return False
    return True


def _jsonb_record(record: dict[str, Any]) -> dict[str, Any]:
    from psycopg.types.json import Jsonb

    return {
        **record,
        "summary": Jsonb(record["summary"], dumps=lambda item: json.dumps(item, ensure_ascii=False)),
        "checks": Jsonb(record["checks"], dumps=lambda item: json.dumps(item, ensure_ascii=False)),
        "assets": Jsonb(record["assets"], dumps=lambda item: json.dumps(item, ensure_ascii=False)),
        "report": Jsonb(record["report"], dumps=lambda item: json.dumps(item, ensure_ascii=False)),
    }


def _generated_at(report: dict[str, Any]) -> datetime:
    value = str(report.get("generated_at") or "").strip()
    if value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def _history_row(row: tuple[Any, ...]) -> dict[str, Any]:
    (
        report_id,
        data_type,
        run_dir,
        run_name,
        dataset,
        status,
        target_crs,
        generated_at,
        summary,
        created_at,
        updated_at,
    ) = row
    return {
        "report_id": str(report_id),
        "data_type": data_type,
        "run_dir": run_dir,
        "run_name": run_name,
        "dataset": dataset,
        "status": status,
        "target_crs": target_crs,
        "generated_at": _datetime_text(generated_at),
        "created_at": _datetime_text(created_at),
        "updated_at": _datetime_text(updated_at),
        "summary": summary or {},
    }


def _datetime_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(value)
