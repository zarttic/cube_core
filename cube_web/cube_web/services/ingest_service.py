from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

from cube_split import runtime_config

from cube_web.services import quality_service
from cube_web.services.config_store import optical_ingest_defaults
from cube_web.services.quality_report_store import get_quality_report_store


class _LazyRayIngestJob:
    def __getattr__(self, name: str):
        module = self._load()
        return getattr(module, name)

    def _load(self):
        from cube_split.ingest import ray_ingest_job as module

        return module


ray_ingest_job = _LazyRayIngestJob()


def _ray_ingest_job():
    return ray_ingest_job


def preview_optical_ingest(payload: dict[str, Any]) -> dict[str, Any]:
    payload = _payload_with_defaults(payload, optical_ingest_defaults())
    run_dir, quality_report = _resolve_run(payload)
    versions = _resolve_versions(payload)
    rows_path = run_dir / "index_rows.jsonl"
    ray_ingest_job = _ray_ingest_job()
    rows = ray_ingest_job.load_rows(rows_path)
    asset_uri_map = {str(row["asset_path"]): str(row["asset_path"]) for row in rows}
    raw_records = ray_ingest_job.build_raw_asset_records(
        rows=rows,
        dataset=_payload_text(payload, "dataset", "demo_optical"),
        sensor=_payload_text(payload, "sensor", "optical_mosaic"),
        asset_version=versions["asset_version"],
        run_id=_preview_run_id(quality_report),
        asset_uri_map=asset_uri_map,
    )
    cube_records = ray_ingest_job.build_cube_fact_records(
        rows=rows,
        cube_version=versions["cube_version"],
        run_id=_preview_run_id(quality_report),
        quality_rule=_payload_text(payload, "quality_rule", "best_quality_wins"),
        asset_uri_map=asset_uri_map,
    )
    conflicts = _existing_conflicts(raw_records, cube_records)
    return {
        "status": "ready",
        "mode": "pre_ingest_preview",
        "run_dir": str(run_dir),
        "report_id": quality_report.get("report_id"),
        "quality_status": quality_report.get("status", "UNKNOWN"),
        "dataset": _payload_text(payload, "dataset", "demo_optical"),
        "sensor": _payload_text(payload, "sensor", "optical_mosaic"),
        "asset_version": versions["asset_version"],
        "cube_version": versions["cube_version"],
        "quality_rule": _payload_text(payload, "quality_rule", "best_quality_wins"),
        "input_rows": len(rows),
        "raw_asset_rows": len(raw_records),
        "cube_fact_rows": len(cube_records),
        "materialized_cog_assets": len(asset_uri_map),
        "existing_raw_asset_rows": conflicts["raw_asset_rows"],
        "existing_cube_fact_rows": conflicts["cube_fact_rows"],
        "would_write": False,
        "idempotent_keys": {
            "raw_asset": ["scene_id", "band", "version"],
            "cube_fact": ["grid_type", "grid_level", "space_code", "time_bucket", "band", "cube_version"],
        },
    }


def confirm_optical_ingest(payload: dict[str, Any]) -> dict[str, Any]:
    payload = _payload_with_defaults(payload, optical_ingest_defaults())
    run_dir, quality_report = _resolve_run(payload)
    quality_status = str(quality_report.get("status", "UNKNOWN"))
    if quality_status == "FAIL" and not bool(payload.get("allow_failed_quality", False)):
        raise ValueError("Quality status is FAIL; set allow_failed_quality=true to ingest anyway")
    versions = _resolve_versions(payload)
    job_id = f"demo-optical-{uuid4().hex[:12]}"
    minio = runtime_config.minio_settings(payload)
    args = SimpleNamespace(
        run_dir=str(run_dir),
        job_id=job_id,
        dataset=_payload_text(payload, "dataset", "demo_optical"),
        sensor=_payload_text(payload, "sensor", "optical_mosaic"),
        asset_version=versions["asset_version"],
        cube_version=versions["cube_version"],
        quality_rule=_payload_text(payload, "quality_rule", "best_quality_wins"),
        metadata_backend="postgres",
        postgres_dsn=_postgres_dsn(),
        db_path="",
        asset_storage_backend=_payload_text(payload, "asset_storage_backend", "minio"),
        minio_endpoint=minio.endpoint,
        minio_access_key=minio.access_key,
        minio_secret_key=minio.secret_key,
        minio_bucket=minio.bucket,
        minio_prefix=str(payload.get("minio_prefix") or "cube/raw"),
        minio_secure=bool(payload.get("minio_secure", False)),
        minio_upload_workers=int(payload.get("minio_upload_workers") or 8),
        cog_output_root=str(Path("/tmp") / "cube_web_ingest_demo" / "cog"),
        cog_materialize_mode="symlink",
    )
    stats = _ray_ingest_job().run_ingest(args)
    return {
        "status": "succeeded",
        "mode": "confirmed_ingest",
        "job_id": job_id,
        "report_id": quality_report.get("report_id"),
        "quality_status": quality_status,
        "asset_version": versions["asset_version"],
        "cube_version": versions["cube_version"],
        "would_write": True,
        **stats,
    }


def _resolve_run(payload: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
    report_id = str(payload.get("report_id") or "").strip()
    if report_id:
        report = get_quality_report_store().get_report("optical", report_id)
        if report is None:
            raise FileNotFoundError(f"Optical quality report not found: {report_id}")
        run_dir_text = str(report.get("run_dir") or "").strip()
        if not run_dir_text:
            raise ValueError(f"Optical quality report has no run_dir: {report_id}")
        return quality_service.resolve_quality_run_dir(run_dir_text), report

    run_dir_text = str(payload.get("run_dir") or "").strip()
    if not run_dir_text:
        raise ValueError("run_dir or report_id is required")
    run_dir = quality_service.resolve_quality_run_dir(run_dir_text)
    report = get_quality_report_store().latest_report("optical") or {}
    if str(report.get("run_dir") or "") != str(run_dir):
        report = {"run_dir": str(run_dir), "status": "UNKNOWN"}
    return run_dir, report


def _resolve_versions(payload: dict[str, Any]) -> dict[str, str]:
    default_version = datetime.now(timezone.utc).strftime("demo-%Y%m%d")
    return {
        "asset_version": _payload_text(payload, "asset_version", default_version),
        "cube_version": _payload_text(payload, "cube_version", default_version),
    }


def _payload_text(payload: dict[str, Any], key: str, default: str) -> str:
    value = str(payload.get(key) or default).strip()
    if not value:
        raise ValueError(f"{key} must not be empty")
    return value


def _payload_with_defaults(payload: dict[str, Any], defaults: dict[str, Any]) -> dict[str, Any]:
    result = dict(defaults)
    for key, value in payload.items():
        if value is not None and value != "":
            result[key] = value
    return result


def _preview_run_id(report: dict[str, Any]) -> str:
    report_id = str(report.get("report_id") or "").strip()
    return f"preview-{report_id}" if report_id else "preview"


def _postgres_dsn() -> str:
    return runtime_config.require_postgres_dsn()


def _existing_conflicts(raw_records: list[Any], cube_records: list[Any]) -> dict[str, int]:
    try:
        import psycopg
    except ModuleNotFoundError:
        return {"raw_asset_rows": 0, "cube_fact_rows": 0}

    with psycopg.connect(_postgres_dsn()) as conn:
        ray_ingest_job = _ray_ingest_job()
        ray_ingest_job.ensure_tables_postgres(conn)
        with conn.cursor() as cur:
            raw_count = 0
            for row in raw_records:
                cur.execute(
                    """
                    SELECT 1
                    FROM rs_raw_scene_asset
                    WHERE scene_id = %s AND band = %s AND version = %s
                    LIMIT 1
                    """,
                    (row.scene_id, row.band, row.version),
                )
                raw_count += 1 if cur.fetchone() else 0

            cube_count = 0
            for row in cube_records:
                cur.execute(
                    """
                    SELECT 1
                    FROM rs_cube_cell_fact
                    WHERE grid_type = %s
                      AND grid_level = %s
                      AND space_code = %s
                      AND time_bucket = %s
                      AND band = %s
                      AND cube_version = %s
                    LIMIT 1
                    """,
                    (row.grid_type, row.grid_level, row.space_code, row.time_bucket, row.band, row.cube_version),
                )
                cube_count += 1 if cur.fetchone() else 0
    return {"raw_asset_rows": raw_count, "cube_fact_rows": cube_count}
