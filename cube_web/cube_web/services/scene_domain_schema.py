"""Fresh-install schema for the production scene domain."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

SCENE_DOMAIN_SCHEMA_VERSION = "2026-07-23-scene-domain-v11"

SCENE_DOMAIN_TABLES = {
    "datasets",
    "scenes",
    "scene_assets",
    "scene_bands",
    "load_batches",
    "load_batch_scenes",
    "load_batch_sources",
    "partition_runs",
    "partition_drafts",
    "partition_run_scenes",
    "partition_data_unit_grid_status",
    "ingest_runs",
    "ingest_run_scenes",
    "scene_dataset_audit",
    "dataset_role_restrictions",
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
          suggested_grid_type TEXT CHECK (suggested_grid_type IS NULL OR suggested_grid_type IN ('geohash','mgrs','isea4h')),
          status TEXT NOT NULL DEFAULT 'loaded' CHECK (status IN ('discovered','loaded','partitioning','partitioned','quality_pending','quality_passed','quality_failed','ingesting','available','failed','archived')),
          attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE (dataset_id, scene_key)
        )""",
        """CREATE TABLE IF NOT EXISTS dataset_role_restrictions (
          dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id) ON DELETE CASCADE,
          role TEXT NOT NULL CHECK (role IN ('NORMAL','ADVANCED','SCIENTIST','ADMIN')),
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          created_by TEXT NOT NULL,
          PRIMARY KEY (dataset_id, role)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_dataset_role_restrictions_role ON dataset_role_restrictions(role, dataset_id)",
        """ALTER TABLE scenes ADD COLUMN IF NOT EXISTS resolution_native DOUBLE PRECISION""",
        """ALTER TABLE scenes ADD COLUMN IF NOT EXISTS resolution_unit TEXT""",
        """ALTER TABLE scenes ADD COLUMN IF NOT EXISTS resolution_m DOUBLE PRECISION""",
        """ALTER TABLE scenes ADD COLUMN IF NOT EXISTS suggested_grid_type TEXT""",
        "ALTER TABLE scenes DROP CONSTRAINT IF EXISTS scenes_suggested_grid_type_check",
        "ALTER TABLE scenes ADD CONSTRAINT scenes_suggested_grid_type_check CHECK (suggested_grid_type IS NULL OR suggested_grid_type IN ('geohash','mgrs','isea4h'))",
        """CREATE TABLE IF NOT EXISTS load_batches (
          load_batch_id TEXT PRIMARY KEY,
          batch_name TEXT NOT NULL,
          source_system TEXT,
          source_type TEXT NOT NULL DEFAULT 'subsystem_import' CHECK (source_type IN ('subsystem_import','dataset_reload')),
          created_by TEXT NOT NULL DEFAULT 'system',
          status TEXT NOT NULL CHECK (status IN ('pending','running','succeeded','failed','cancelled','manual_required','archived','unknown')),
          attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
          loaded_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )""",
        """ALTER TABLE load_batches ADD COLUMN IF NOT EXISTS source_type TEXT NOT NULL DEFAULT 'subsystem_import'""",
        """ALTER TABLE load_batches ADD COLUMN IF NOT EXISTS created_by TEXT NOT NULL DEFAULT 'system'""",
        "ALTER TABLE load_batches DROP CONSTRAINT IF EXISTS load_batches_source_type_check",
        "ALTER TABLE load_batches ADD CONSTRAINT load_batches_source_type_check CHECK (source_type IN ('subsystem_import','dataset_reload'))",
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
        """CREATE TABLE IF NOT EXISTS load_batch_sources (
          load_batch_id TEXT NOT NULL REFERENCES load_batches(load_batch_id) ON DELETE CASCADE,
          source_load_batch_id TEXT NOT NULL REFERENCES load_batches(load_batch_id),
          source_dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id),
          created_by TEXT NOT NULL DEFAULT 'system',
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (load_batch_id, source_load_batch_id, source_dataset_id)
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
        """CREATE TABLE IF NOT EXISTS partition_drafts (
          draft_id TEXT PRIMARY KEY,
          draft_name TEXT NOT NULL,
          data_type TEXT NOT NULL CHECK (data_type IN ('optical','radar','product','carbon')),
          source_load_batch_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
          selection JSONB NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','submitted','discarded')),
          created_by TEXT NOT NULL DEFAULT 'system',
          submitted_partition_run_id TEXT REFERENCES partition_runs(partition_run_id),
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )""",
        """ALTER TABLE partition_drafts ADD COLUMN IF NOT EXISTS draft_name TEXT NOT NULL DEFAULT '待剖分批次'""",
        """CREATE TABLE IF NOT EXISTS partition_run_scenes (
          partition_run_id TEXT NOT NULL REFERENCES partition_runs(partition_run_id),
          selection_id TEXT NOT NULL,
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
          PRIMARY KEY (partition_run_id, selection_id),
          UNIQUE (partition_run_id, idempotency_key)
        )""",
        """CREATE TABLE IF NOT EXISTS partition_data_unit_grid_status (
          dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id),
          scene_id TEXT NOT NULL REFERENCES scenes(scene_id),
          band_unit_id TEXT NOT NULL,
          grid_type TEXT NOT NULL CHECK (grid_type IN ('geohash','mgrs','isea4h')),
          grid_level INT NOT NULL CHECK (grid_level >= 0),
          partition_run_id TEXT REFERENCES partition_runs(partition_run_id),
          partition_status TEXT NOT NULL DEFAULT 'pending' CHECK (partition_status IN ('pending','queued','running','completed','failed','cancelled')),
          quality_status TEXT NOT NULL DEFAULT 'pending' CHECK (quality_status IN ('pending','running','pass','warn','fail','error','cancelled')),
          ingest_status TEXT NOT NULL DEFAULT 'pending' CHECK (ingest_status IN ('pending','queued','running','completed','failed','cancelled')),
          output_version TEXT,
          attempt_no INT NOT NULL DEFAULT 0 CHECK (attempt_no >= 0),
          error_message TEXT,
          attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (band_unit_id, grid_type, grid_level)
        )""",
        """ALTER TABLE partition_run_scenes ADD COLUMN IF NOT EXISTS selection_id TEXT""",
        """UPDATE partition_run_scenes SET selection_id=idempotency_key WHERE selection_id IS NULL""",
        """ALTER TABLE partition_run_scenes ALTER COLUMN selection_id SET NOT NULL""",
        """ALTER TABLE partition_run_scenes DROP CONSTRAINT IF EXISTS partition_run_scenes_pkey""",
        """ALTER TABLE partition_run_scenes ADD PRIMARY KEY (partition_run_id, selection_id)""",
        """ALTER TABLE partition_data_unit_grid_status DROP CONSTRAINT IF EXISTS partition_data_unit_grid_status_pkey""",
        """ALTER TABLE partition_data_unit_grid_status ADD PRIMARY KEY (band_unit_id, grid_type, grid_level)""",
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
          band_unit_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
          status TEXT NOT NULL CHECK (status IN ('pending','queued','running','completed','failed','cancelled')),
          idempotency_key TEXT NOT NULL,
          attempt_count INT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
          error_message TEXT,
          provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (ingest_run_id, scene_id)
        )""",
        """ALTER TABLE ingest_run_scenes ADD COLUMN IF NOT EXISTS band_unit_ids JSONB NOT NULL DEFAULT '[]'::jsonb""",
        """CREATE INDEX IF NOT EXISTS idx_ingest_run_scenes_band_unit
           ON ingest_run_scenes ((band_unit_ids->>0), status, updated_at)""",
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
        "CREATE INDEX IF NOT EXISTS idx_load_batch_sources_source ON load_batch_sources(source_load_batch_id, source_dataset_id)",
        "CREATE INDEX IF NOT EXISTS idx_partition_run_scenes_dataset_status ON partition_run_scenes(dataset_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_partition_drafts_pending ON partition_drafts(data_type, status, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_partition_grid_status_run ON partition_data_unit_grid_status(partition_run_id, partition_status)",
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
        "partition grid band references": "SELECT COUNT(*) FROM partition_data_unit_grid_status g LEFT JOIN scene_bands b ON b.band_unit_id=g.band_unit_id WHERE b.band_unit_id IS NULL",
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


def apply_scene_domain_schema(
    connection: Any, *, commit: bool = True, record_version: bool = True
) -> SchemaInstallReport:
    """Install and validate the production schema atomically."""
    try:
        with connection.cursor() as cursor:
            for statement in schema_statements():
                cursor.execute(statement)
            counts = _verify(cursor)
            if record_version:
                _record_schema_version(cursor, counts)
        if commit:
            connection.commit()
    except Exception:
        connection.rollback()
        raise
    return SchemaInstallReport(schema_version=SCENE_DOMAIN_SCHEMA_VERSION, **counts)


def record_scene_domain_install(connection: Any, install_report: dict[str, Any]) -> None:
    """Record v4 only after DDL and every required backfill have succeeded."""
    try:
        with connection.cursor() as cursor:
            _record_schema_version(cursor, install_report)
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def _record_schema_version(cursor: Any, install_report: dict[str, Any]) -> None:
    cursor.execute(
        """MERGE INTO scene_domain_schema_version target
        USING (SELECT TRUE AS singleton, %s::text AS schema_version, %s::jsonb AS install_report) source
        ON (target.singleton = source.singleton)
        WHEN MATCHED THEN UPDATE SET schema_version=source.schema_version, installed_at=now(), install_report=source.install_report
        WHEN NOT MATCHED THEN INSERT (singleton,schema_version,install_report) VALUES (source.singleton,source.schema_version,source.install_report)""",
        (SCENE_DOMAIN_SCHEMA_VERSION, json.dumps(install_report, sort_keys=True)),
    )


def backfill_scene_resolution_metadata(connection: Any, *, commit: bool = True) -> int:
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
        if commit:
            connection.commit()
    except Exception:
        connection.rollback()
        raise
    return updated


def backfill_scene_band_unit_ids(connection: Any, *, commit: bool = True) -> int:
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
        if commit:
            connection.commit()
    except Exception:
        connection.rollback()
        raise
    return updated


def split_legacy_ingest_band_units(connection: Any, *, commit: bool = True) -> int:
    """Split pre-v8 multi-band ingest rows without discarding their history.

    New execution records have exactly one band unit. A legacy parent keeps its
    first unit and additional units receive deterministic sibling ingest runs.
    """
    from hashlib import sha256

    updated = 0
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT ingest_run_id,scene_id,partition_run_id,output_version,band_unit_ids,status,"
                "attempt_count,error_message,provenance FROM ingest_run_scenes "
                "WHERE jsonb_array_length(band_unit_ids) <> 1 ORDER BY created_at,ingest_run_id,scene_id"
            )
            rows = cursor.fetchall()
            for ingest_run_id, scene_id, partition_run_id, output_version, raw_bands, status, attempt_count, error_message, provenance in rows:
                bands = raw_bands if isinstance(raw_bands, list) else json.loads(raw_bands or "[]")
                bands = [str(value) for value in bands if str(value).strip()]
                if not bands:
                    cursor.execute(
                        "SELECT band_unit_id FROM scene_bands WHERE scene_id=%s AND band_unit_id IS NOT NULL ORDER BY band_unit_id",
                        (scene_id,),
                    )
                    bands = [str(row[0]) for row in cursor.fetchall()]
                if not bands:
                    raise RuntimeError(f"cannot split legacy ingest row without a band unit: {ingest_run_id}/{scene_id}")
                cursor.execute("SELECT dataset_id FROM ingest_runs WHERE ingest_run_id=%s", (ingest_run_id,))
                owner = cursor.fetchone()
                if owner is None:
                    raise RuntimeError(f"legacy ingest row has no parent run: {ingest_run_id}")
                dataset_id = str(owner[0])
                for index, band_unit_id in enumerate(dict.fromkeys(bands)):
                    idempotency_key = sha256(
                        f"{dataset_id}\0{scene_id}\0{output_version}\0{band_unit_id}".encode("utf-8")
                    ).hexdigest()
                    suffix_identity = f"{scene_id}\0{band_unit_id}".encode("utf-8")
                    target_run_id = str(ingest_run_id) if index == 0 else (
                        f"{ingest_run_id}-band-{sha256(suffix_identity).hexdigest()[:12]}"
                    )
                    if index:
                        cursor.execute(
                            "INSERT INTO ingest_runs (ingest_run_id,partition_run_id,dataset_id,status,requested_by,error_message,attributes,created_at,started_at,completed_at) "
                            "SELECT %s,partition_run_id,dataset_id,status,requested_by,error_message,attributes,created_at,started_at,completed_at "
                            "FROM ingest_runs WHERE ingest_run_id=%s "
                            "AND NOT EXISTS (SELECT 1 FROM ingest_runs WHERE ingest_run_id=%s)",
                            (target_run_id, ingest_run_id, target_run_id),
                        )
                        cursor.execute("SELECT 1 FROM ingest_run_scenes WHERE idempotency_key=%s", (idempotency_key,))
                        if cursor.fetchone() is None:
                            cursor.execute(
                                "INSERT INTO ingest_run_scenes (ingest_run_id,scene_id,partition_run_id,output_version,band_unit_ids,status,idempotency_key,attempt_count,error_message,provenance) "
                                "VALUES (%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s::jsonb)",
                                (target_run_id, scene_id, partition_run_id, output_version, json.dumps([band_unit_id]), status,
                                 idempotency_key, attempt_count, error_message, json.dumps(provenance or {})),
                            )
                    else:
                        cursor.execute(
                            "UPDATE ingest_run_scenes SET band_unit_ids=%s::jsonb,idempotency_key=%s,updated_at=now() "
                            "WHERE ingest_run_id=%s AND scene_id=%s",
                            (json.dumps([band_unit_id]), idempotency_key, ingest_run_id, scene_id),
                        )
                    updated += 1
        if commit:
            connection.commit()
    except Exception:
        connection.rollback()
        raise
    return updated


def backfill_partition_grid_status(connection: Any, *, commit: bool = True) -> int:
    """Create per-band grid state rows for existing scene partition runs."""
    updated = 0
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT partition_run_id,scene_id,dataset_id,status,grid_config,output_version,attempt_count,error_message "
            "FROM partition_run_scenes ORDER BY created_at"
        )
        rows = cursor.fetchall()
        for partition_run_id, scene_id, dataset_id, status, raw_config, output_version, attempt_count, error_message in rows:
            config = raw_config if isinstance(raw_config, dict) else json.loads(raw_config or "{}")
            grid_type = str(config.get("grid_type") or "")
            raw_grid_level = config.get("requested_grid_level")
            if (not grid_type or raw_grid_level is None) and output_version:
                cursor.execute(
                    "SELECT grid_type,requested_grid_level FROM partition_output_versions "
                    "WHERE dataset_id=%s AND output_version=%s",
                    (dataset_id, output_version),
                )
                output_row = cursor.fetchone()
                if output_row is not None:
                    grid_type = grid_type or str(output_row[0] or "")
                    raw_grid_level = raw_grid_level if raw_grid_level is not None else output_row[1]
            if grid_type not in {"geohash", "mgrs", "isea4h"} or raw_grid_level is None:
                raise RuntimeError(
                    f"partition run scene has no supported grid identity: {partition_run_id}/{scene_id}"
                )
            grid_level = int(raw_grid_level)
            band_unit_ids = [str(value) for value in config.get("band_unit_ids") or [] if value]
            if not band_unit_ids:
                cursor.execute("SELECT band_unit_id FROM scene_bands WHERE scene_id=%s AND band_unit_id IS NOT NULL", (scene_id,))
                band_unit_ids = [str(row[0]) for row in cursor.fetchall()]
            quality_status = "pending"
            ingest_status = "pending"
            if output_version:
                cursor.execute(
                    "SELECT status FROM partition_quality_runs WHERE dataset_id=%s AND output_version=%s "
                    "ORDER BY completed_at DESC NULLS LAST,created_at DESC LIMIT 1",
                    (dataset_id, output_version),
                )
                quality_row = cursor.fetchone()
                if quality_row is not None:
                    quality_status = str(quality_row[0])
            for band_unit_id in band_unit_ids:
                if output_version:
                    cursor.execute(
                        "SELECT status FROM ingest_run_scenes WHERE scene_id=%s AND output_version=%s "
                        "AND band_unit_ids ? %s ORDER BY updated_at DESC LIMIT 1",
                        (scene_id, output_version, band_unit_id),
                    )
                    ingest_row = cursor.fetchone()
                    ingest_status = str(ingest_row[0]) if ingest_row is not None else "pending"
                cursor.execute(
                    """MERGE INTO partition_data_unit_grid_status target USING (
                      SELECT %s::text dataset_id,%s::text scene_id,%s::text band_unit_id,%s::text grid_type,%s::int grid_level
                    ) source ON (target.band_unit_id=source.band_unit_id AND target.grid_type=source.grid_type)
                    WHEN NOT MATCHED THEN INSERT (dataset_id,scene_id,band_unit_id,grid_type,grid_level,partition_run_id,
                      partition_status,quality_status,ingest_status,output_version,attempt_no,error_message)
                    VALUES (source.dataset_id,source.scene_id,source.band_unit_id,source.grid_type,source.grid_level,
                      %s,%s,%s,%s,%s,%s,%s)""",
                    (
                        dataset_id, scene_id, band_unit_id, grid_type, grid_level, partition_run_id, status, quality_status,
                        ingest_status, output_version, int(attempt_count or 0), error_message,
                    ),
                )
                updated += max(0, cursor.rowcount)
    if commit:
        connection.commit()
    return updated
