"""Fresh-install schema for the production scene domain."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

SCENE_DOMAIN_SCHEMA_VERSION = "2026-07-18-scene-domain-v3"

SCENE_DOMAIN_TABLES = {
    "datasets",
    "scenes",
    "scene_assets",
    "scene_bands",
    "load_batches",
    "load_batch_scenes",
    "partition_runs",
    "partition_run_scenes",
    "ingest_runs",
    "ingest_run_scenes",
    "scene_dataset_audit",
    "scene_domain_schema_version",
}


@dataclass(frozen=True)
class SchemaInstallReport:
    schema_version: str
    datasets: int
    scenes: int
    scene_assets: int
    load_batches: int
    load_batch_scenes: int
    partition_runs: int
    ingest_runs: int


def schema_statements() -> tuple[str, ...]:
    """Return ordered PostgreSQL/OpenGauss-compatible production DDL."""
    return (
        """CREATE TABLE IF NOT EXISTS datasets (
          dataset_id TEXT PRIMARY KEY,
          dataset_code TEXT NOT NULL UNIQUE,
          dataset_title TEXT NOT NULL,
          data_type TEXT NOT NULL CHECK (data_type IN ('optical','radar','product','carbon')),
          product_type TEXT,
          status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('draft','active','archived')),
          assignment_confidence NUMERIC(4,3) NOT NULL DEFAULT 1 CHECK (assignment_confidence BETWEEN 0 AND 1),
          assignment_issue TEXT,
          auto_ingest_allowed BOOLEAN NOT NULL DEFAULT TRUE,
          current_output_version TEXT,
          attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )""",
        """CREATE TABLE IF NOT EXISTS scenes (
          scene_id TEXT PRIMARY KEY,
          dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id),
          scene_key TEXT NOT NULL,
          identity_key TEXT NOT NULL UNIQUE,
          source_asset_id TEXT,
          source_uri TEXT NOT NULL,
          checksum CHAR(64) CHECK (checksum IS NULL OR checksum ~ '^[0-9a-f]{64}$'),
          acquisition_time TIMESTAMPTZ,
          bbox JSONB,
          crs TEXT,
          resolution_native DOUBLE PRECISION CHECK (resolution_native IS NULL OR resolution_native > 0),
          resolution_unit TEXT CHECK (resolution_unit IS NULL OR resolution_unit IN ('degree','m')),
          resolution_m DOUBLE PRECISION CHECK (resolution_m IS NULL OR resolution_m > 0),
          suggested_grid_type TEXT CHECK (suggested_grid_type IS NULL OR suggested_grid_type IN ('geohash','mgrs')),
          status TEXT NOT NULL DEFAULT 'loaded' CHECK (status IN ('discovered','loaded','partitioning','partitioned','quality_pending','quality_passed','quality_failed','ingesting','available','failed','archived')),
          attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE (dataset_id, scene_key)
        )""",
        """ALTER TABLE scenes ADD COLUMN IF NOT EXISTS resolution_native DOUBLE PRECISION""",
        """ALTER TABLE scenes ADD COLUMN IF NOT EXISTS resolution_unit TEXT""",
        """ALTER TABLE scenes ADD COLUMN IF NOT EXISTS resolution_m DOUBLE PRECISION""",
        """ALTER TABLE scenes ADD COLUMN IF NOT EXISTS suggested_grid_type TEXT""",
        """CREATE TABLE IF NOT EXISTS load_batches (
          load_batch_id TEXT PRIMARY KEY,
          batch_name TEXT NOT NULL,
          source_system TEXT,
          status TEXT NOT NULL CHECK (status IN ('pending','running','succeeded','failed','cancelled','manual_required','archived','unknown')),
          attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
          loaded_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )""",
        """CREATE TABLE IF NOT EXISTS scene_assets (
          scene_id TEXT NOT NULL REFERENCES scenes(scene_id),
          asset_id TEXT NOT NULL,
          source_uri TEXT NOT NULL,
          cog_uri TEXT,
          asset_role TEXT NOT NULL DEFAULT 'data' CHECK (asset_role IN ('data','metadata','quicklook','sidecar')),
          source_kind TEXT NOT NULL DEFAULT 'raw' CHECK (source_kind IN ('raw','cog','observation','sidecar')),
          source_format TEXT NOT NULL DEFAULT 'unknown',
          checksum CHAR(64) CHECK (checksum IS NULL OR checksum ~ '^[0-9a-f]{64}$'),
          acquisition_time TIMESTAMPTZ,
          bbox JSONB,
          crs TEXT,
          attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (scene_id, asset_id)
        )""",
        """CREATE TABLE IF NOT EXISTS scene_bands (
          scene_id TEXT NOT NULL,
          asset_id TEXT NOT NULL,
          band_unit_id TEXT,
          band_code TEXT NOT NULL,
          band_name TEXT,
          band_type TEXT CHECK (band_type IS NULL OR band_type IN ('spectral','polarization','variable')),
          unit TEXT,
          display_order INT NOT NULL DEFAULT 0 CHECK (display_order >= 0),
          attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (scene_id, asset_id, band_code),
          FOREIGN KEY (scene_id, asset_id) REFERENCES scene_assets(scene_id, asset_id)
        )""",
        """ALTER TABLE scene_bands ADD COLUMN IF NOT EXISTS band_unit_id TEXT""",
        """CREATE TABLE IF NOT EXISTS load_batch_scenes (
          load_batch_id TEXT NOT NULL REFERENCES load_batches(load_batch_id),
          scene_id TEXT NOT NULL REFERENCES scenes(scene_id),
          source_asset_id TEXT,
          source_uri TEXT NOT NULL,
          checksum CHAR(64) CHECK (checksum IS NULL OR checksum ~ '^[0-9a-f]{64}$'),
          load_status TEXT NOT NULL CHECK (load_status IN ('pending','running','succeeded','failed','duplicate','cancelled','unknown')),
          error_message TEXT,
          attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (load_batch_id, scene_id)
        )""",
        """CREATE TABLE IF NOT EXISTS partition_runs (
          partition_run_id TEXT PRIMARY KEY,
          status TEXT NOT NULL CHECK (status IN ('pending','queued','running','completed','partial_failure','failed','cancelled')),
          source_load_batch_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
          requested_by TEXT NOT NULL DEFAULT 'system',
          error_message TEXT,
          attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          started_at TIMESTAMPTZ,
          completed_at TIMESTAMPTZ
        )""",
        """CREATE TABLE IF NOT EXISTS partition_run_scenes (
          partition_run_id TEXT NOT NULL REFERENCES partition_runs(partition_run_id),
          scene_id TEXT NOT NULL REFERENCES scenes(scene_id),
          dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id),
          source_load_batch_id TEXT REFERENCES load_batches(load_batch_id),
          status TEXT NOT NULL CHECK (status IN ('pending','queued','running','completed','failed','cancelled')),
          grid_config JSONB NOT NULL DEFAULT '{}'::jsonb,
          output_version TEXT,
          idempotency_key TEXT NOT NULL,
          attempt_count INT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
          error_message TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (partition_run_id, scene_id),
          UNIQUE (partition_run_id, idempotency_key)
        )""",
        """CREATE TABLE IF NOT EXISTS ingest_runs (
          ingest_run_id TEXT PRIMARY KEY,
          partition_run_id TEXT NOT NULL REFERENCES partition_runs(partition_run_id),
          dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id),
          status TEXT NOT NULL CHECK (status IN ('pending','queued','running','completed','partial_failure','failed','cancelled')),
          requested_by TEXT NOT NULL DEFAULT 'system',
          error_message TEXT,
          attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          started_at TIMESTAMPTZ,
          completed_at TIMESTAMPTZ
        )""",
        """CREATE TABLE IF NOT EXISTS ingest_run_scenes (
          ingest_run_id TEXT NOT NULL REFERENCES ingest_runs(ingest_run_id),
          scene_id TEXT NOT NULL REFERENCES scenes(scene_id),
          partition_run_id TEXT NOT NULL REFERENCES partition_runs(partition_run_id),
          output_version TEXT NOT NULL,
          status TEXT NOT NULL CHECK (status IN ('pending','queued','running','completed','failed','cancelled')),
          idempotency_key TEXT NOT NULL,
          attempt_count INT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
          error_message TEXT,
          provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (ingest_run_id, scene_id)
        )""",
        """CREATE TABLE IF NOT EXISTS scene_dataset_audit (
          audit_id TEXT PRIMARY KEY,
          scene_id TEXT NOT NULL REFERENCES scenes(scene_id),
          previous_dataset_id TEXT,
          dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id),
          action TEXT NOT NULL CHECK (action IN ('assign','reassign')),
          reason TEXT NOT NULL,
          changed_by TEXT NOT NULL,
          changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          attributes JSONB NOT NULL DEFAULT '{}'::jsonb
        )""",
        """CREATE TABLE IF NOT EXISTS scene_domain_schema_version (
          singleton BOOLEAN PRIMARY KEY CHECK (singleton),
          schema_version TEXT NOT NULL,
          installed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          install_report JSONB NOT NULL DEFAULT '{}'::jsonb
        )""",
        "CREATE INDEX IF NOT EXISTS idx_scenes_dataset_time ON scenes(dataset_id, acquisition_time)",
        "CREATE INDEX IF NOT EXISTS idx_scenes_status ON scenes(status, updated_at)",
        "CREATE INDEX IF NOT EXISTS idx_scene_assets_source_uri ON scene_assets(source_uri)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_scene_bands_unit_id ON scene_bands(band_unit_id)",
        "CREATE INDEX IF NOT EXISTS idx_load_batch_scenes_scene ON load_batch_scenes(scene_id, load_batch_id)",
        "CREATE INDEX IF NOT EXISTS idx_partition_run_scenes_dataset_status ON partition_run_scenes(dataset_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_ingest_run_scenes_status ON ingest_run_scenes(status, updated_at)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_ingest_run_scenes_idempotency ON ingest_run_scenes(idempotency_key)",
        "CREATE INDEX IF NOT EXISTS idx_scene_dataset_audit_scene ON scene_dataset_audit(scene_id, changed_at)",
    )


def _scalar(cursor: Any, query: str) -> int:
    cursor.execute(query)
    row = cursor.fetchone()
    return int(row[0] if not isinstance(row, dict) else next(iter(row.values())))


def _verify(cursor: Any) -> dict[str, int]:
    failures: list[str] = []
    orphan_queries = {
        "scene dataset references": "SELECT COUNT(*) FROM scenes s LEFT JOIN datasets d ON d.dataset_id=s.dataset_id WHERE d.dataset_id IS NULL",
        "load batch scene references": "SELECT COUNT(*) FROM load_batch_scenes l LEFT JOIN load_batches b ON b.load_batch_id=l.load_batch_id LEFT JOIN scenes s ON s.scene_id=l.scene_id WHERE b.load_batch_id IS NULL OR s.scene_id IS NULL",
        "partition run scene references": "SELECT COUNT(*) FROM partition_run_scenes r LEFT JOIN partition_runs p ON p.partition_run_id=r.partition_run_id LEFT JOIN scenes s ON s.scene_id=r.scene_id WHERE p.partition_run_id IS NULL OR s.scene_id IS NULL",
        "ingest run scene references": "SELECT COUNT(*) FROM ingest_run_scenes r LEFT JOIN ingest_runs i ON i.ingest_run_id=r.ingest_run_id LEFT JOIN scenes s ON s.scene_id=r.scene_id WHERE i.ingest_run_id IS NULL OR s.scene_id IS NULL",
    }
    for label, query in orphan_queries.items():
        count = _scalar(cursor, query)
        if count:
            failures.append(f"orphan {label}: {count}")
    if failures:
        raise RuntimeError("scene domain schema validation failed: " + "; ".join(failures))
    return {
        "datasets": _scalar(cursor, "SELECT COUNT(*) FROM datasets"),
        "scenes": _scalar(cursor, "SELECT COUNT(*) FROM scenes"),
        "scene_assets": _scalar(cursor, "SELECT COUNT(*) FROM scene_assets"),
        "load_batches": _scalar(cursor, "SELECT COUNT(*) FROM load_batches"),
        "load_batch_scenes": _scalar(cursor, "SELECT COUNT(*) FROM load_batch_scenes"),
        "partition_runs": _scalar(cursor, "SELECT COUNT(*) FROM partition_runs"),
        "ingest_runs": _scalar(cursor, "SELECT COUNT(*) FROM ingest_runs"),
    }


def apply_scene_domain_schema(connection: Any) -> SchemaInstallReport:
    """Install and validate the production schema atomically."""
    try:
        with connection.cursor() as cursor:
            for statement in schema_statements():
                cursor.execute(statement)
            counts = _verify(cursor)
            cursor.execute(
                """MERGE INTO scene_domain_schema_version target
                USING (SELECT TRUE AS singleton, %s::text AS schema_version, %s::jsonb AS install_report) source
                ON (target.singleton = source.singleton)
                WHEN MATCHED THEN UPDATE SET schema_version=source.schema_version, installed_at=now(), install_report=source.install_report
                WHEN NOT MATCHED THEN INSERT (singleton,schema_version,install_report) VALUES (source.singleton,source.schema_version,source.install_report)""",
                (SCENE_DOMAIN_SCHEMA_VERSION, json.dumps(counts, sort_keys=True)),
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return SchemaInstallReport(schema_version=SCENE_DOMAIN_SCHEMA_VERSION, **counts)


def backfill_scene_resolution_metadata(connection: Any) -> int:
    """Populate persisted resolution semantics for pre-v2 Scene rows."""
    from cube_web.services.partition_defaults import resolution_metadata_from_assets

    updated = 0
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT scene_id, crs, attributes FROM scenes")
            for scene_id, crs, attributes in cursor.fetchall():
                metadata = resolution_metadata_from_assets([{"crs": crs, "attributes": attributes or {}}])
                if not metadata:
                    continue
                cursor.execute(
                    """UPDATE scenes SET resolution_native=%s, resolution_unit=%s, resolution_m=%s,
                       suggested_grid_type=%s, updated_at=now() WHERE scene_id=%s""",
                    (
                        metadata["resolution_native"], metadata["resolution_unit"], metadata["resolution_m"],
                        metadata["suggested_grid_type"], scene_id,
                    ),
                )
                updated += 1
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return updated


def backfill_scene_band_unit_ids(connection: Any) -> int:
    """Assign stable identities to pre-v3 Scene band rows."""
    from hashlib import sha256

    updated = 0
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT scene_id, asset_id, band_code FROM scene_bands WHERE band_unit_id IS NULL")
            for scene_id, asset_id, band_code in cursor.fetchall():
                digest = sha256(f"{scene_id}\0{asset_id}\0{band_code}".encode("utf-8")).hexdigest()[:32]
                cursor.execute(
                    """UPDATE scene_bands SET band_unit_id=%s
                       WHERE scene_id=%s AND asset_id=%s AND band_code=%s""",
                    (f"band-{digest}", scene_id, asset_id, band_code),
                )
                updated += 1
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return updated
