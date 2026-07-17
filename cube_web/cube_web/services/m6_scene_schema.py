"""Additive M6 scene-domain schema and legacy backfill.

The M2 ``partition_*`` and loader ``ard_*`` tables remain intact.  This module
creates the normalized M6 domain beside them and copies legacy facts in one
transaction.  A schema-version row is written only after all preservation and
reference checks pass.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable

M6_SCENE_SCHEMA_VERSION = "2026-07-16-m6-scene-domain-v1"

M6_TABLES = {
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
    "migration_lineage",
    "m6_scene_schema_version",
}

LEGACY_SOURCE_TABLES = {
    "partition_batches",
    "partition_assets",
    "partition_job_attempts",
    "partition_datasets",
    "partition_dataset_assets",
    "partition_dataset_bands",
    "partition_output_versions",
    "ard_partition_batches",
    "ard_partition_assets",
    "ard_partition_observations",
    "rs_ingest_job",
    "rs_raw_scene_asset",
    "rs_cube_cell_fact",
    "rs_entity_tile_asset",
    "rs_product_asset",
    "rs_product_cell_fact",
    "rs_carbon_observation_fact",
}


@dataclass(frozen=True)
class MigrationReport:
    schema_version: str
    existing_legacy_tables: tuple[str, ...]
    datasets: int
    scenes: int
    scene_assets: int
    load_batches: int
    load_batch_scenes: int
    partition_runs: int
    ingest_runs: int
    migration_lineage: int


def schema_statements() -> tuple[str, ...]:
    """Return ordered PostgreSQL/OpenGauss-compatible additive DDL."""
    return (
        """CREATE TABLE IF NOT EXISTS datasets (
          dataset_id TEXT PRIMARY KEY,
          dataset_code TEXT NOT NULL UNIQUE,
          dataset_title TEXT NOT NULL,
          data_type TEXT NOT NULL CHECK (data_type IN ('optical','radar','product','carbon','entity','unknown')),
          product_type TEXT,
          status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('draft','active','archived')),
          assignment_confidence NUMERIC(4,3) NOT NULL DEFAULT 1 CHECK (assignment_confidence BETWEEN 0 AND 1),
          assignment_issue TEXT,
          auto_ingest_allowed BOOLEAN NOT NULL DEFAULT TRUE,
          current_output_version TEXT,
          attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
          legacy_partition_dataset_id TEXT UNIQUE,
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
          status TEXT NOT NULL DEFAULT 'loaded' CHECK (status IN ('discovered','loaded','partitioning','partitioned','quality_pending','quality_passed','quality_failed','ingesting','available','failed','archived')),
          attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE (dataset_id, scene_key)
        )""",
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
          partition_run_id TEXT REFERENCES partition_runs(partition_run_id),
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
          partition_run_id TEXT REFERENCES partition_runs(partition_run_id),
          output_version TEXT,
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
          action TEXT NOT NULL CHECK (action IN ('backfill','assign','reassign')),
          reason TEXT NOT NULL,
          changed_by TEXT NOT NULL,
          changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          attributes JSONB NOT NULL DEFAULT '{}'::jsonb
        )""",
        """CREATE TABLE IF NOT EXISTS m6_scene_schema_version (
          singleton BOOLEAN PRIMARY KEY CHECK (singleton),
          schema_version TEXT NOT NULL,
          installed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          migration_report JSONB NOT NULL DEFAULT '{}'::jsonb
        )""",
        """CREATE TABLE IF NOT EXISTS migration_lineage (
          source_table TEXT NOT NULL,
          source_key TEXT NOT NULL,
          entity_type TEXT NOT NULL CHECK (entity_type IN ('dataset','scene','asset','cube_cell','entity_tile','partition_run','ingest_run')),
          entity_id TEXT NOT NULL,
          dataset_id TEXT REFERENCES datasets(dataset_id),
          scene_id TEXT REFERENCES scenes(scene_id),
          ingest_run_id TEXT REFERENCES ingest_runs(ingest_run_id),
          confidence NUMERIC(4,3) NOT NULL CHECK (confidence BETWEEN 0 AND 1),
          assignment_issue TEXT,
          provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (source_table, source_key, entity_type)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_scenes_dataset_time ON scenes(dataset_id, acquisition_time)",
        "CREATE INDEX IF NOT EXISTS idx_scenes_status ON scenes(status, updated_at)",
        "CREATE INDEX IF NOT EXISTS idx_scene_assets_source_uri ON scene_assets(source_uri)",
        "CREATE INDEX IF NOT EXISTS idx_load_batch_scenes_scene ON load_batch_scenes(scene_id, load_batch_id)",
        "CREATE INDEX IF NOT EXISTS idx_partition_run_scenes_dataset_status ON partition_run_scenes(dataset_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_ingest_run_scenes_status ON ingest_run_scenes(status, updated_at)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_ingest_run_scenes_idempotency ON ingest_run_scenes(idempotency_key)",
        "CREATE INDEX IF NOT EXISTS idx_scene_dataset_audit_scene ON scene_dataset_audit(scene_id, changed_at)",
        "CREATE INDEX IF NOT EXISTS idx_migration_lineage_entity ON migration_lineage(entity_type, entity_id)",
    )


def _hash64(expression: str) -> str:
    return f"(md5({expression}) || md5('m6:' || {expression}))"


def _scene_id(namespace_expr: str, scene_expr: str) -> str:
    """Build a canonical scene id from provider namespace and stable key."""
    identity_expression = f"({namespace_expr} || ':' || {scene_expr})"
    return f"('scene-' || {_hash64(identity_expression)})"


def _identity_key(namespace_expr: str, scene_expr: str) -> str:
    return _hash64(f"({namespace_expr} || ':' || {scene_expr})")


def _rs_dataset_id(dataset_expr: str) -> str:
    """Reuse an M6 Dataset identity before synthesizing a legacy RS one."""
    return (
        f"COALESCE((SELECT d.dataset_id FROM datasets d WHERE d.dataset_id={dataset_expr}),"
        f"'rs-dataset-' || md5({dataset_expr}))"
    )


def _rs_scene_id(scene_expr: str, fallback_expr: str) -> str:
    """Reuse a managed M6 Scene written verbatim into an RS catalog row."""
    return f"COALESCE((SELECT s.scene_id FROM scenes s WHERE s.scene_id={scene_expr}),{fallback_expr})"


def _rs_identity_key(scene_expr: str, fallback_expr: str) -> str:
    return f"COALESCE((SELECT s.identity_key FROM scenes s WHERE s.scene_id={scene_expr}),{fallback_expr})"


def backfill_statements(existing_tables: Iterable[str]) -> tuple[str, ...]:
    """Build only the backfills whose complete legacy sources are present."""
    tables = set(existing_tables)
    statements: list[str] = []

    if "partition_batches" in tables:
        statements.append("""MERGE INTO load_batches target USING (
          SELECT batch_id AS load_batch_id, batch_name,
                 source_system, CASE WHEN status IN ('pending','running','succeeded','failed','cancelled','manual_required','archived') THEN status ELSE 'unknown' END AS status,
                 json_build_object('legacy_table','partition_batches','data_type',data_type)::jsonb AS attributes,
                 NULL::timestamptz AS loaded_at, created_at, updated_at
          FROM partition_batches
        ) source ON (target.load_batch_id = source.load_batch_id)
        WHEN NOT MATCHED THEN INSERT (load_batch_id,batch_name,source_system,status,attributes,loaded_at,created_at,updated_at)
        VALUES (source.load_batch_id,source.batch_name,source.source_system,source.status,source.attributes,source.loaded_at,source.created_at,source.updated_at)""")

    if "ard_partition_batches" in tables:
        statements.append("""MERGE INTO load_batches target USING (
          SELECT batch_id AS load_batch_id, COALESCE(batch_name,batch_id) AS batch_name,
                 source_system, CASE WHEN status IN ('pending','running','succeeded','failed','cancelled','manual_required','archived') THEN status ELSE 'unknown' END AS status,
                 json_build_object('legacy_table','ard_partition_batches','legacy_id',id,'data_type',data_type,'raw_meta_uri',raw_meta_uri)::jsonb AS attributes,
                 loaded_at, COALESCE(loaded_at,now()) AS created_at, COALESCE(updated_at,loaded_at,now()) AS updated_at
          FROM ard_partition_batches
        ) source ON (target.load_batch_id = source.load_batch_id)
        WHEN NOT MATCHED THEN INSERT (load_batch_id,batch_name,source_system,status,attributes,loaded_at,created_at,updated_at)
        VALUES (source.load_batch_id,source.batch_name,source.source_system,source.status,source.attributes,source.loaded_at,source.created_at,source.updated_at)""")

    if "partition_datasets" in tables:
        statements.append("""MERGE INTO datasets target USING (
          SELECT dataset_id, dataset_code, dataset_title,
                 CASE WHEN data_type IN ('optical','radar','product','carbon','entity') THEN data_type ELSE 'unknown' END AS data_type,
                 product_type, CASE WHEN partition_status = 'cancelled' THEN 'archived' ELSE 'active' END AS status,
                 current_output_version, attributes, dataset_id AS legacy_partition_dataset_id, created_at, updated_at
          FROM partition_datasets
        ) source ON (target.dataset_id = source.dataset_id)
        WHEN MATCHED THEN UPDATE SET legacy_partition_dataset_id=COALESCE(target.legacy_partition_dataset_id,source.legacy_partition_dataset_id),
          updated_at=CASE WHEN target.updated_at>source.updated_at THEN target.updated_at ELSE source.updated_at END
        WHEN NOT MATCHED THEN INSERT (dataset_id,dataset_code,dataset_title,data_type,product_type,status,current_output_version,attributes,legacy_partition_dataset_id,created_at,updated_at)
        VALUES (source.dataset_id,source.dataset_code,source.dataset_title,source.data_type,source.product_type,source.status,source.current_output_version,source.attributes,source.legacy_partition_dataset_id,source.created_at,source.updated_at)""")

    # Unassigned legacy assets are grouped by product semantics, never by load
    # batch.  The inferred Dataset is marked ineligible for automatic ingest.
    if {"partition_batches", "partition_assets", "partition_datasets", "partition_dataset_assets"} <= tables:
        group_key = "(LOWER(COALESCE(b.data_type,'unknown')) || ':' || LOWER(COALESCE(NULLIF(a.asset_payload->>'sensor',''),'unknown')) || ':' || LOWER(COALESCE(NULLIF(a.asset_payload->>'product_family',''),'unknown')) || ':' || LOWER(COALESCE(NULLIF(a.asset_payload->>'product_type',''),NULLIF(a.asset_payload->>'product_name',''),'unknown')))"
        statements.append("""MERGE INTO datasets target USING (
          SELECT 'legacy-dataset-' || md5(%(group_key)s) AS dataset_id,
                 'legacy-dataset-' || md5(%(group_key)s) AS dataset_code,
                 'Inferred ' || MIN(b.data_type) || ' dataset' AS dataset_title,
                 CASE WHEN b.data_type IN ('optical','radar','product','carbon','entity') THEN b.data_type ELSE 'unknown' END AS data_type,
                 NULL::text AS product_type,'draft' AS status,0.250::numeric AS assignment_confidence,
                 'Dataset inferred from legacy product metadata'::text AS assignment_issue,FALSE AS auto_ingest_allowed,
                 NULL::text AS current_output_version,
                 json_build_object('migration_inferred',true,'assignment_ambiguous',true,'auto_ingest_allowed',false,'group_key',%(group_key)s)::jsonb AS attributes,
                 MIN(a.created_at) AS created_at,MAX(a.updated_at) AS updated_at
          FROM partition_assets a JOIN partition_batches b ON b.batch_id=a.batch_id
          WHERE NOT EXISTS (SELECT 1 FROM partition_dataset_assets da WHERE da.source_asset_id=a.asset_id)
          GROUP BY %(group_key)s,b.data_type
        ) source ON (target.dataset_id = source.dataset_id)
        WHEN NOT MATCHED THEN INSERT (dataset_id,dataset_code,dataset_title,data_type,product_type,status,assignment_confidence,assignment_issue,auto_ingest_allowed,current_output_version,attributes,created_at,updated_at)
        VALUES (source.dataset_id,source.dataset_code,source.dataset_title,source.data_type,source.product_type,source.status,source.assignment_confidence,source.assignment_issue,source.auto_ingest_allowed,source.current_output_version,source.attributes,source.created_at,source.updated_at)""".replace("%(group_key)s", group_key))

    if "partition_dataset_assets" in tables and "partition_datasets" in tables:
        dataset_expr = "a.dataset_id"
        scene_expr = (
            "COALESCE((SELECT MIN(NULLIF(pa.scene_id,'')) FROM partition_assets pa WHERE pa.asset_id=a.source_asset_id),a.source_asset_id)"
            if "partition_assets" in tables else "a.source_asset_id"
        )
        provider_expr = "COALESCE((SELECT pb.source_system FROM partition_datasets pd JOIN partition_batches pb ON pb.batch_id=pd.batch_id WHERE pd.dataset_id=a.dataset_id),'loader')" if "partition_batches" in tables else "'loader'"
        scene_id = _scene_id(provider_expr, scene_expr)
        identity = _identity_key(provider_expr, scene_expr)
        statements.extend((
            f"""MERGE INTO scenes target USING (
              SELECT {_scene_id('x.provider_namespace','x.scene_key')} AS scene_id,x.dataset_id,x.scene_key,
                     {_identity_key('x.provider_namespace', 'x.scene_key')} AS identity_key,x.source_asset_id,x.source_uri,
                     x.checksum,x.acquisition_time,x.bbox,x.crs,'partitioned' AS status,
                     json_build_object('legacy_table','partition_dataset_assets','asset_count',x.asset_count)::jsonb AS attributes,
                     x.created_at,x.created_at AS updated_at FROM (
                SELECT MIN(a.dataset_id) AS dataset_id,{provider_expr} AS provider_namespace,{scene_expr} AS scene_key,MIN(a.source_asset_id) AS source_asset_id,
                       MIN(a.source_uri) AS source_uri,MIN(a.checksum) AS checksum,MIN(a.time_start) AS acquisition_time,
                       MIN(a.bbox::text)::jsonb AS bbox,MIN(a.crs) AS crs,COUNT(*) AS asset_count,MIN(a.created_at) AS created_at
                FROM partition_dataset_assets a GROUP BY {provider_expr},{scene_expr}
              ) x
            ) source ON (target.identity_key = source.identity_key)
            WHEN NOT MATCHED THEN INSERT (scene_id,dataset_id,scene_key,identity_key,source_asset_id,source_uri,checksum,acquisition_time,bbox,crs,status,attributes,created_at,updated_at)
            VALUES (source.scene_id,source.dataset_id,source.scene_key,source.identity_key,source.source_asset_id,source.source_uri,source.checksum,source.acquisition_time,source.bbox,source.crs,source.status,source.attributes,source.created_at,source.updated_at)""",
            f"""MERGE INTO scene_dataset_audit target USING (
              SELECT 'audit-' || md5({scene_id}) AS audit_id,{scene_id} AS scene_id,MIN(a.dataset_id) AS dataset_id,
                     COUNT(DISTINCT a.dataset_id) AS candidate_dataset_count
              FROM partition_dataset_assets a GROUP BY {provider_expr},{scene_expr}
            ) source ON (target.audit_id = source.audit_id)
            WHEN NOT MATCHED THEN INSERT (audit_id,scene_id,previous_dataset_id,dataset_id,action,reason,changed_by,attributes)
            VALUES (source.audit_id,source.scene_id,NULL,source.dataset_id,'backfill',
              CASE WHEN source.candidate_dataset_count>1 THEN 'M6 ambiguous legacy assignment; deterministic dataset selected' ELSE 'M6 legacy dataset asset backfill' END,
              'm6-migration',json_build_object('candidate_dataset_count',source.candidate_dataset_count,'assignment_confidence',CASE WHEN source.candidate_dataset_count>1 THEN 0.500 ELSE 1.000 END)::jsonb)""",
            f"""MERGE INTO scene_assets target USING (
              SELECT {scene_id} AS scene_id,a.source_asset_id AS asset_id,MIN(a.source_uri) AS source_uri,MIN(a.cog_uri) AS cog_uri,
                     'data' AS asset_role,MIN(a.source_kind) AS source_kind,MIN(a.source_format) AS source_format,MIN(a.checksum) AS checksum,
                     MIN(a.time_start) AS acquisition_time,MIN(a.bbox::text)::jsonb AS bbox,MIN(a.crs) AS crs,
                     MIN(a.attributes::text)::jsonb AS attributes,MIN(a.created_at) AS created_at,MAX(a.created_at) AS updated_at
              FROM partition_dataset_assets a GROUP BY {provider_expr},{scene_expr},a.source_asset_id
            ) source ON (target.scene_id=source.scene_id AND target.asset_id=source.asset_id)
            WHEN NOT MATCHED THEN INSERT (scene_id,asset_id,source_uri,cog_uri,asset_role,source_kind,source_format,checksum,acquisition_time,bbox,crs,attributes,created_at,updated_at)
            VALUES (source.scene_id,source.asset_id,source.source_uri,source.cog_uri,source.asset_role,source.source_kind,source.source_format,source.checksum,source.acquisition_time,source.bbox,source.crs,source.attributes,source.created_at,source.updated_at)""",
        ))
        if "partition_dataset_bands" in tables:
            statements.append(f"""MERGE INTO scene_bands target USING (
              SELECT {scene_id} AS scene_id,b.source_asset_id AS asset_id,b.band_code,MIN(b.band_name) AS band_name,
                     MIN(b.band_type) AS band_type,MIN(b.unit) AS unit,MIN(b.display_order) AS display_order,
                     MIN(b.attributes::text)::jsonb AS attributes,MIN(b.created_at) AS created_at
              FROM partition_dataset_bands b JOIN partition_dataset_assets a
                ON a.dataset_id=b.dataset_id AND a.source_asset_id=b.source_asset_id
              GROUP BY {provider_expr},{scene_expr},b.source_asset_id,b.band_code
            ) source ON (target.scene_id=source.scene_id AND target.asset_id=source.asset_id AND target.band_code=source.band_code)
            WHEN NOT MATCHED THEN INSERT (scene_id,asset_id,band_code,band_name,band_type,unit,display_order,attributes,created_at)
            VALUES (source.scene_id,source.asset_id,source.band_code,source.band_name,source.band_type,source.unit,source.display_order,source.attributes,source.created_at)""")
        statements.append(f"""UPDATE datasets SET status='draft',assignment_confidence=0.500,
          assignment_issue='Legacy scenes reference multiple datasets',auto_ingest_allowed=FALSE,updated_at=now()
          WHERE dataset_id IN (
            SELECT DISTINCT a.dataset_id FROM partition_dataset_assets a
            JOIN (SELECT {provider_expr} AS provider_namespace,{scene_expr} AS scene_key FROM partition_dataset_assets a
                  GROUP BY {provider_expr},{scene_expr} HAVING COUNT(DISTINCT a.dataset_id)>1) conflicts
              ON conflicts.provider_namespace={provider_expr} AND conflicts.scene_key={scene_expr}
          )""")

    if {"partition_batches", "partition_assets", "partition_datasets"} <= tables:
        asset_mapping = (
            "(SELECT MIN(da.dataset_id) FROM partition_dataset_assets da WHERE da.source_asset_id = a.asset_id), "
            if "partition_dataset_assets" in tables else ""
        )
        provisional_group = "(LOWER(COALESCE((SELECT pb.data_type FROM partition_batches pb WHERE pb.batch_id=a.batch_id),'unknown')) || ':' || LOWER(COALESCE(NULLIF(a.asset_payload->>'sensor',''),'unknown')) || ':' || LOWER(COALESCE(NULLIF(a.asset_payload->>'product_family',''),'unknown')) || ':' || LOWER(COALESCE(NULLIF(a.asset_payload->>'product_type',''),NULLIF(a.asset_payload->>'product_name',''),'unknown')))"
        mapped_dataset = f"COALESCE({asset_mapping}'legacy-dataset-' || md5({provisional_group}))"
        source_scene_key = "COALESCE(NULLIF(a.scene_id,''),a.asset_id)"
        source_provider = "COALESCE((SELECT pb.source_system FROM partition_batches pb WHERE pb.batch_id=a.batch_id),'loader')"
        source_scene_id = _scene_id(source_provider, source_scene_key)
        source_identity = _identity_key(source_provider, source_scene_key)
        statements.extend((
            f"""MERGE INTO scenes target USING (
              SELECT {_scene_id('a.provider_namespace','a.scene_key')} AS scene_id,a.dataset_id,a.scene_key,
                     {_identity_key('a.provider_namespace', 'a.scene_key')} AS identity_key,
                     a.source_asset_id,a.source_uri,NULL::char(64) AS checksum,NULL::timestamptz AS acquisition_time,
                     NULL::jsonb AS bbox,NULL::text AS crs,a.status,
                     json_build_object('legacy_table','partition_assets','asset_count',a.asset_count)::jsonb AS attributes,
                     a.created_at,a.updated_at FROM (
                SELECT MIN({mapped_dataset}) AS dataset_id,{source_provider} AS provider_namespace,{source_scene_key} AS scene_key,
                       MIN(a.asset_id) AS source_asset_id,MIN(a.source_uri) AS source_uri,COUNT(*) AS asset_count,
                       CASE WHEN MAX(CASE WHEN a.status='failed' THEN 1 ELSE 0 END)=1 THEN 'failed'
                            WHEN MAX(CASE WHEN a.partitioned_at IS NOT NULL THEN 1 ELSE 0 END)=1 THEN 'partitioned' ELSE 'loaded' END AS status,
                       MIN(a.created_at) AS created_at,MAX(a.updated_at) AS updated_at
                FROM partition_assets a GROUP BY {source_provider},{source_scene_key}
              ) a
            ) source ON (target.identity_key = source.identity_key)
            WHEN NOT MATCHED THEN INSERT (scene_id,dataset_id,scene_key,identity_key,source_asset_id,source_uri,checksum,acquisition_time,bbox,crs,status,attributes,created_at,updated_at)
            VALUES (source.scene_id,source.dataset_id,source.scene_key,source.identity_key,source.source_asset_id,source.source_uri,source.checksum,source.acquisition_time,source.bbox,source.crs,source.status,source.attributes,source.created_at,source.updated_at)""",
            f"""MERGE INTO load_batch_scenes target USING (
              SELECT a.batch_id AS load_batch_id, s.scene_id, MIN(a.asset_id) AS source_asset_id, MIN(a.source_uri) AS source_uri,
                     NULL::char(64) AS checksum,
                     CASE WHEN MAX(CASE WHEN a.status='failed' THEN 1 ELSE 0 END)=1 THEN 'failed'
                          WHEN MAX(CASE WHEN a.status='running' THEN 1 ELSE 0 END)=1 THEN 'running'
                          WHEN MAX(CASE WHEN a.status='cancelled' THEN 1 ELSE 0 END)=1 THEN 'cancelled'
                          WHEN MIN(CASE WHEN a.status IN ('succeeded','completed') THEN 1 ELSE 0 END)=1 THEN 'succeeded'
                          WHEN MAX(CASE WHEN a.status='pending' THEN 1 ELSE 0 END)=1 THEN 'pending' ELSE 'unknown' END AS load_status,
                     MAX(a.last_error) AS error_message,json_build_object('legacy_table','partition_assets','asset_count',COUNT(*))::jsonb AS attributes,
                     MIN(a.created_at) AS created_at,MAX(a.updated_at) AS updated_at
              FROM partition_assets a JOIN scenes s ON s.identity_key = {source_identity}
              GROUP BY a.batch_id,s.scene_id
            ) source ON (target.load_batch_id = source.load_batch_id AND target.scene_id = source.scene_id)
            WHEN NOT MATCHED THEN INSERT (load_batch_id,scene_id,source_asset_id,source_uri,checksum,load_status,error_message,attributes,created_at,updated_at)
            VALUES (source.load_batch_id,source.scene_id,source.source_asset_id,source.source_uri,source.checksum,source.load_status,source.error_message,source.attributes,source.created_at,source.updated_at)""",
            f"""MERGE INTO scene_assets target USING (
              SELECT s.scene_id,a.asset_id,a.source_uri,NULL::text AS cog_uri,'data' AS asset_role,
                     'raw' AS source_kind,COALESCE(NULLIF(a.asset_payload->>'file_format',''),'unknown') AS source_format,
                     NULL::char(64) AS checksum,NULL::timestamptz AS acquisition_time,NULL::jsonb AS bbox,NULL::text AS crs,
                     a.asset_payload AS attributes,a.created_at,a.updated_at
              FROM partition_assets a JOIN scenes s ON s.identity_key={source_identity}
            ) source ON (target.scene_id=source.scene_id AND target.asset_id=source.asset_id)
            WHEN NOT MATCHED THEN INSERT (scene_id,asset_id,source_uri,cog_uri,asset_role,source_kind,source_format,checksum,acquisition_time,bbox,crs,attributes,created_at,updated_at)
            VALUES (source.scene_id,source.asset_id,source.source_uri,source.cog_uri,source.asset_role,source.source_kind,source.source_format,source.checksum,source.acquisition_time,source.bbox,source.crs,source.attributes,source.created_at,source.updated_at)""",
        ))

    if {"ard_partition_batches", "ard_partition_assets"} <= tables:
        group_key = "(LOWER(COALESCE(b.data_type,'unknown')) || ':' || LOWER(COALESCE(NULLIF(a.sensor,''),'unknown')) || ':' || LOWER(COALESCE(NULLIF(a.product_family,''),'unknown')) || ':' || LOWER(COALESCE(NULLIF(a.product_name,''),'unknown')))"
        exact_filter = (
            "WHERE NOT EXISTS (SELECT 1 FROM partition_dataset_assets da WHERE da.source_asset_id=a.asset_id)"
            if "partition_dataset_assets" in tables else ""
        )
        statements.append(f"""MERGE INTO datasets target USING (
          SELECT 'legacy-dataset-' || md5({group_key}) AS dataset_id,'legacy-dataset-' || md5({group_key}) AS dataset_code,
                 'Inferred ' || MIN(b.data_type) || ' dataset' AS dataset_title,
                 CASE WHEN b.data_type IN ('optical','radar','product','carbon','entity') THEN b.data_type ELSE 'unknown' END AS data_type,
                 NULL::text AS product_type,'draft' AS status,0.250::numeric AS assignment_confidence,
                 'Dataset inferred from ARD product metadata'::text AS assignment_issue,FALSE AS auto_ingest_allowed,NULL::text AS current_output_version,
                 json_build_object('migration_inferred',true,'assignment_ambiguous',true,'auto_ingest_allowed',false,'group_key',{group_key})::jsonb AS attributes,
                 MIN(COALESCE(b.loaded_at,now())) AS created_at,MAX(COALESCE(b.updated_at,b.loaded_at,now())) AS updated_at
          FROM ard_partition_assets a JOIN ard_partition_batches b ON b.id=a.batch_id {exact_filter}
          GROUP BY {group_key},b.data_type
        ) source ON (target.dataset_id=source.dataset_id)
        WHEN NOT MATCHED THEN INSERT (dataset_id,dataset_code,dataset_title,data_type,product_type,status,assignment_confidence,assignment_issue,auto_ingest_allowed,current_output_version,attributes,created_at,updated_at)
        VALUES (source.dataset_id,source.dataset_code,source.dataset_title,source.data_type,source.product_type,source.status,source.assignment_confidence,source.assignment_issue,source.auto_ingest_allowed,source.current_output_version,source.attributes,source.created_at,source.updated_at)""")

    if {"ard_partition_batches", "ard_partition_observations"} <= tables:
        group_key = "(LOWER(COALESCE(b.data_type,'carbon')) || ':' || LOWER(COALESCE(NULLIF(o.sensor,''),'unknown')) || ':' || LOWER(COALESCE(NULLIF(o.product_family,''),'unknown')) || ':' || LOWER(COALESCE(NULLIF(o.product_type,''),'unknown')))"
        exact_filter = (
            "WHERE NOT EXISTS (SELECT 1 FROM partition_dataset_assets da WHERE da.source_asset_id=o.observation_id)"
            if "partition_dataset_assets" in tables else ""
        )
        statements.append(f"""MERGE INTO datasets target USING (
          SELECT 'legacy-dataset-' || md5({group_key}) AS dataset_id,'legacy-dataset-' || md5({group_key}) AS dataset_code,
                 'Inferred ' || MIN(b.data_type) || ' dataset' AS dataset_title,
                 CASE WHEN b.data_type IN ('optical','radar','product','carbon','entity') THEN b.data_type ELSE 'unknown' END AS data_type,
                 MIN(o.product_type) AS product_type,'draft' AS status,0.250::numeric AS assignment_confidence,
                 'Dataset inferred from ARD observation metadata'::text AS assignment_issue,FALSE AS auto_ingest_allowed,NULL::text AS current_output_version,
                 json_build_object('migration_inferred',true,'assignment_ambiguous',true,'auto_ingest_allowed',false,'group_key',{group_key})::jsonb AS attributes,
                 MIN(COALESCE(b.loaded_at,now())) AS created_at,MAX(COALESCE(b.updated_at,b.loaded_at,now())) AS updated_at
          FROM ard_partition_observations o JOIN ard_partition_batches b ON b.id=o.batch_id {exact_filter}
          GROUP BY {group_key},b.data_type
        ) source ON (target.dataset_id=source.dataset_id)
        WHEN NOT MATCHED THEN INSERT (dataset_id,dataset_code,dataset_title,data_type,product_type,status,assignment_confidence,assignment_issue,auto_ingest_allowed,current_output_version,attributes,created_at,updated_at)
        VALUES (source.dataset_id,source.dataset_code,source.dataset_title,source.data_type,source.product_type,source.status,source.assignment_confidence,source.assignment_issue,source.auto_ingest_allowed,source.current_output_version,source.attributes,source.created_at,source.updated_at)""")

    if {"ard_partition_batches", "ard_partition_assets"} <= tables:
        exact_dataset = (
            "(SELECT MIN(da.dataset_id) FROM partition_dataset_assets da WHERE da.source_asset_id=a.asset_id),"
            if "partition_dataset_assets" in tables else ""
        )
        group_key = "(LOWER(COALESCE(b.data_type,'unknown')) || ':' || LOWER(COALESCE(NULLIF(a.sensor,''),'unknown')) || ':' || LOWER(COALESCE(NULLIF(a.product_family,''),'unknown')) || ':' || LOWER(COALESCE(NULLIF(a.product_name,''),'unknown')))"
        ard_dataset = f"COALESCE({exact_dataset}'legacy-dataset-' || md5({group_key}))"
        ard_scene_key = "COALESCE(NULLIF(a.scene_id,''),a.asset_id)"
        ard_provider = "COALESCE(b.source_system,'loader')"
        ard_scene_id = _scene_id(ard_provider, ard_scene_key)
        ard_identity = _identity_key(ard_provider, ard_scene_key)
        statements.extend((
            f"""MERGE INTO scenes target USING (
              SELECT {_scene_id('x.provider_namespace','x.scene_key')} AS scene_id,x.dataset_id,x.scene_key,
                     {_identity_key('x.provider_namespace', 'x.scene_key')} AS identity_key,
                     x.source_asset_id,x.source_uri,NULL::char(64) AS checksum,x.acquisition_time,x.bbox,NULL::text AS crs,
                     x.scene_status AS status,json_build_object('legacy_table','ard_partition_assets','asset_count',x.asset_count,'migration_inferred',true,'auto_ingest_allowed',false)::jsonb AS attributes,
                     x.created_at,x.updated_at FROM (
                SELECT MIN({ard_dataset}) AS dataset_id,{ard_provider} AS provider_namespace,{ard_scene_key} AS scene_key,MIN(a.asset_id) AS source_asset_id,
                       MIN(a.source_uri) AS source_uri,MIN(a.acq_time) AS acquisition_time,MIN(a.bbox::text)::jsonb AS bbox,
                       COUNT(*) AS asset_count,CASE WHEN MAX(CASE WHEN b.status='succeeded' THEN 1 ELSE 0 END)=1 THEN 'loaded'
                         WHEN MAX(CASE WHEN b.status='failed' THEN 1 ELSE 0 END)=1 THEN 'failed' ELSE 'discovered' END AS scene_status,
                       MIN(COALESCE(b.loaded_at,now())) AS created_at,MAX(COALESCE(b.updated_at,b.loaded_at,now())) AS updated_at
                FROM ard_partition_assets a JOIN ard_partition_batches b ON b.id=a.batch_id
                GROUP BY {ard_provider},{ard_scene_key}
              ) x
            ) source ON (target.identity_key=source.identity_key)
            WHEN NOT MATCHED THEN INSERT (scene_id,dataset_id,scene_key,identity_key,source_asset_id,source_uri,checksum,acquisition_time,bbox,crs,status,attributes,created_at,updated_at)
            VALUES (source.scene_id,source.dataset_id,source.scene_key,source.identity_key,source.source_asset_id,source.source_uri,source.checksum,source.acquisition_time,source.bbox,source.crs,source.status,source.attributes,source.created_at,source.updated_at)""",
            f"""MERGE INTO load_batch_scenes target USING (
              SELECT b.batch_id AS load_batch_id,s.scene_id,MIN(a.asset_id) AS source_asset_id,MIN(a.source_uri) AS source_uri,
                     NULL::char(64) AS checksum,
                     CASE WHEN b.status IN ('pending','running','failed','cancelled') THEN b.status WHEN b.status='succeeded' THEN 'succeeded' ELSE 'unknown' END AS load_status,
                     NULL::text AS error_message,
                     json_build_object('legacy_table','ard_partition_assets','asset_count',COUNT(*),'migration_inferred',true,'auto_ingest_allowed',false)::jsonb AS attributes,
                     COALESCE(b.loaded_at,now()) AS created_at,COALESCE(b.updated_at,b.loaded_at,now()) AS updated_at
              FROM ard_partition_assets a JOIN ard_partition_batches b ON b.id=a.batch_id
              JOIN scenes s ON s.identity_key={ard_identity}
              GROUP BY b.batch_id,s.scene_id,b.status,b.loaded_at,b.updated_at
            ) source ON (target.load_batch_id=source.load_batch_id AND target.scene_id=source.scene_id)
            WHEN NOT MATCHED THEN INSERT (load_batch_id,scene_id,source_asset_id,source_uri,checksum,load_status,error_message,attributes,created_at,updated_at)
            VALUES (source.load_batch_id,source.scene_id,source.source_asset_id,source.source_uri,source.checksum,source.load_status,source.error_message,source.attributes,source.created_at,source.updated_at)""",
            f"""MERGE INTO scene_assets target USING (
              SELECT s.scene_id,a.asset_id,a.source_uri,NULL::text AS cog_uri,'data' AS asset_role,'raw' AS source_kind,
                     COALESCE(NULLIF(a.file_format,''),'unknown') AS source_format,NULL::char(64) AS checksum,
                     a.acq_time AS acquisition_time,a.bbox::jsonb AS bbox,NULL::text AS crs,
                     json_build_object('sensor',a.sensor,'product_family',a.product_family,'bands',a.bands,'band',a.band,'polarization',a.polarization)::jsonb AS attributes,
                     COALESCE(b.loaded_at,now()) AS created_at,COALESCE(b.updated_at,b.loaded_at,now()) AS updated_at
              FROM ard_partition_assets a JOIN ard_partition_batches b ON b.id=a.batch_id
              JOIN scenes s ON s.identity_key={ard_identity}
            ) source ON (target.scene_id=source.scene_id AND target.asset_id=source.asset_id)
            WHEN NOT MATCHED THEN INSERT (scene_id,asset_id,source_uri,cog_uri,asset_role,source_kind,source_format,checksum,acquisition_time,bbox,crs,attributes,created_at,updated_at)
            VALUES (source.scene_id,source.asset_id,source.source_uri,source.cog_uri,source.asset_role,source.source_kind,source.source_format,source.checksum,source.acquisition_time,source.bbox,source.crs,source.attributes,source.created_at,source.updated_at)""",
        ))

    rs_sources = {"rs_ingest_job", "rs_raw_scene_asset", "rs_entity_tile_asset", "rs_product_asset", "rs_product_cell_fact", "rs_carbon_observation_fact"} & tables
    if rs_sources:
        unions: list[str] = []
        if "rs_ingest_job" in tables:
            unions.append("SELECT COALESCE(NULLIF(params_json->>'dataset',''),'__unassigned__') AS dataset_name,params_json->>'sensor' AS sensor,created_at,updated_at FROM rs_ingest_job")
        if "rs_raw_scene_asset" in tables:
            unions.append("SELECT dataset AS dataset_name,sensor,ingest_time AS created_at,ingest_time AS updated_at FROM rs_raw_scene_asset")
        if "rs_entity_tile_asset" in tables:
            unions.append("SELECT dataset AS dataset_name,sensor,ingest_time AS created_at,ingest_time AS updated_at FROM rs_entity_tile_asset")
        if "rs_product_asset" in tables:
            unions.append("SELECT dataset AS dataset_name,product_name AS sensor,ingest_time AS created_at,ingest_time AS updated_at FROM rs_product_asset")
        if "rs_product_cell_fact" in tables:
            unions.append("SELECT dataset AS dataset_name,product_name AS sensor,ingest_time AS created_at,ingest_time AS updated_at FROM rs_product_cell_fact")
        if "rs_carbon_observation_fact" in tables:
            unions.append("SELECT 'carbon:' || satellite || ':' || product_type AS dataset_name,satellite AS sensor,ingest_time AS created_at,ingest_time AS updated_at FROM rs_carbon_observation_fact")
        rs_dataset_id = _rs_dataset_id("dataset_name")
        statements.append(f"""MERGE INTO datasets target USING (
          SELECT {rs_dataset_id} AS dataset_id,'rs-' || md5(dataset_name) AS dataset_code,
                 CASE WHEN dataset_name='__unassigned__' THEN 'Unassigned RS ingest Dataset' ELSE dataset_name END AS dataset_title,
                 CASE WHEN LOWER(COALESCE(MIN(sensor),'') || ':' || dataset_name) LIKE '%optical%' THEN 'optical'
                      WHEN LOWER(COALESCE(MIN(sensor),'') || ':' || dataset_name) LIKE '%radar%' OR LOWER(COALESCE(MIN(sensor),'') || ':' || dataset_name) LIKE '%sar%' THEN 'radar'
                      WHEN LOWER(COALESCE(MIN(sensor),'') || ':' || dataset_name) LIKE '%carbon%' OR LOWER(COALESCE(MIN(sensor),'') || ':' || dataset_name) LIKE '%co2%' THEN 'carbon'
                      WHEN LOWER(COALESCE(MIN(sensor),'') || ':' || dataset_name) LIKE '%entity%' THEN 'entity'
                      WHEN LOWER(COALESCE(MIN(sensor),'') || ':' || dataset_name) LIKE '%product%' THEN 'product' ELSE 'unknown' END AS data_type,
                 MIN(sensor) AS product_type,
                 CASE WHEN dataset_name='__unassigned__' OR NOT (LOWER(COALESCE(MIN(sensor),'') || ':' || dataset_name) LIKE ANY(ARRAY['%optical%','%radar%','%sar%','%carbon%','%co2%','%entity%','%product%'])) THEN 'draft' ELSE 'active' END AS status,
                 CASE WHEN dataset_name='__unassigned__' THEN 0.100 WHEN NOT (LOWER(COALESCE(MIN(sensor),'') || ':' || dataset_name) LIKE ANY(ARRAY['%optical%','%radar%','%sar%','%carbon%','%co2%','%entity%','%product%'])) THEN 0.500 ELSE 0.900 END::numeric AS assignment_confidence,
                 CASE WHEN dataset_name='__unassigned__' THEN 'rs_ingest_job params_json has no dataset'
                      WHEN NOT (LOWER(COALESCE(MIN(sensor),'') || ':' || dataset_name) LIKE ANY(ARRAY['%optical%','%radar%','%sar%','%carbon%','%co2%','%entity%','%product%'])) THEN 'RS data type cannot be inferred from dataset and sensor' ELSE NULL::text END AS assignment_issue,
                 CASE WHEN dataset_name='__unassigned__' OR NOT (LOWER(COALESCE(MIN(sensor),'') || ':' || dataset_name) LIKE ANY(ARRAY['%optical%','%radar%','%sar%','%carbon%','%co2%','%entity%','%product%'])) THEN FALSE ELSE TRUE END AS auto_ingest_allowed,
                 json_build_object('legacy_table','rs_*','legacy_dataset',dataset_name)::jsonb AS attributes,
                 MIN(created_at) AS created_at,MAX(updated_at) AS updated_at
          FROM ({' UNION ALL '.join(unions)}) legacy_datasets GROUP BY dataset_name
        ) source ON (target.dataset_id=source.dataset_id)
        WHEN NOT MATCHED THEN INSERT (dataset_id,dataset_code,dataset_title,data_type,product_type,status,assignment_confidence,assignment_issue,auto_ingest_allowed,attributes,created_at,updated_at)
        VALUES (source.dataset_id,source.dataset_code,source.dataset_title,source.data_type,source.product_type,source.status,source.assignment_confidence,source.assignment_issue,source.auto_ingest_allowed,source.attributes,source.created_at,source.updated_at)""")

    if "rs_ingest_job" in tables:
        ingest_dataset_id = _rs_dataset_id("COALESCE(NULLIF(params_json->>'dataset',''),'__unassigned__')")
        statements.extend((
            f"""MERGE INTO ingest_runs target USING (
              SELECT job_id AS ingest_run_id,{ingest_dataset_id} AS dataset_id,
                     CASE WHEN status='succeeded' THEN 'completed' WHEN status IN ('pending','queued','running','failed','cancelled') THEN status ELSE 'failed' END AS status,
                     'legacy-rs' AS requested_by,error_msg AS error_message,
                     json_build_object(
                       'legacy_table','rs_ingest_job',
                       'params_json',params_json::text,
                       'stats_json',stats_json::text,
                       'output_snapshot',output_snapshot
                     )::jsonb AS attributes,
                     created_at,started_at,CASE WHEN status='succeeded' THEN COALESCE(finished_at,updated_at) ELSE finished_at END AS completed_at
              FROM rs_ingest_job
            ) source ON (target.ingest_run_id=source.ingest_run_id)
            WHEN NOT MATCHED THEN INSERT (ingest_run_id,dataset_id,status,requested_by,error_message,attributes,created_at,started_at,completed_at)
            VALUES (source.ingest_run_id,source.dataset_id,source.status,source.requested_by,source.error_message,source.attributes,source.created_at,source.started_at,source.completed_at)""",
            f"""MERGE INTO migration_lineage target USING (
              SELECT 'rs_ingest_job' AS source_table,job_id AS source_key,'ingest_run' AS entity_type,job_id AS entity_id,
                     {ingest_dataset_id} AS dataset_id,NULL::text AS scene_id,job_id AS ingest_run_id,
                     CASE WHEN COALESCE(params_json->>'dataset','')='' THEN 0.100 ELSE 1.000 END::numeric AS confidence,
                     CASE WHEN COALESCE(params_json->>'dataset','')='' THEN 'rs_ingest_job params_json has no dataset' ELSE NULL::text END AS assignment_issue,
                     json_build_object('status',status,'output_snapshot',output_snapshot)::jsonb AS provenance
              FROM rs_ingest_job
            ) source ON (target.source_table=source.source_table AND target.source_key=source.source_key AND target.entity_type=source.entity_type)
            WHEN NOT MATCHED THEN INSERT (source_table,source_key,entity_type,entity_id,dataset_id,scene_id,ingest_run_id,confidence,assignment_issue,provenance)
            VALUES (source.source_table,source.source_key,source.entity_type,source.entity_id,source.dataset_id,source.scene_id,source.ingest_run_id,source.confidence,source.assignment_issue,source.provenance)""",
        ))

    if "rs_raw_scene_asset" in tables:
        synthetic_rs_scene_id = _scene_id("'rs'", "r.scene_id")
        synthetic_rs_identity = _identity_key("'rs'", "r.scene_id")
        rs_scene_id = _rs_scene_id("r.scene_id", synthetic_rs_scene_id)
        rs_identity = _rs_identity_key("r.scene_id", synthetic_rs_identity)
        rs_dataset_id = f"COALESCE((SELECT s.dataset_id FROM scenes s WHERE s.scene_id=r.scene_id),{_rs_dataset_id('r.dataset')})"
        statements.extend((
            f"""MERGE INTO scenes target USING (
              SELECT {rs_scene_id} AS scene_id,MIN({rs_dataset_id}) AS dataset_id,r.scene_id AS scene_key,
                     {rs_identity} AS identity_key,MIN('rs-raw-' || r.id::text) AS source_asset_id,MIN(r.raw_cog_uri) AS source_uri,
                     MIN(r.acq_time) AS acquisition_time,'available' AS status,
                     json_build_object('legacy_table','rs_raw_scene_asset','asset_count',COUNT(*),'candidate_dataset_count',COUNT(DISTINCT r.dataset),
                       'assignment_issue',CASE WHEN COUNT(DISTINCT r.dataset)>1 THEN 'RS scene references multiple datasets' ELSE NULL END)::jsonb AS attributes,
                     MIN(r.ingest_time) AS created_at,MAX(r.ingest_time) AS updated_at
              FROM rs_raw_scene_asset r GROUP BY r.scene_id
            ) source ON (target.identity_key=source.identity_key)
            WHEN NOT MATCHED THEN INSERT (scene_id,dataset_id,scene_key,identity_key,source_asset_id,source_uri,acquisition_time,status,attributes,created_at,updated_at)
            VALUES (source.scene_id,source.dataset_id,source.scene_key,source.identity_key,source.source_asset_id,source.source_uri,source.acquisition_time,source.status,source.attributes,source.created_at,source.updated_at)""",
            f"""MERGE INTO scene_assets target USING (
              SELECT {rs_scene_id} AS scene_id,'rs-raw-' || r.id::text AS asset_id,r.raw_cog_uri AS source_uri,r.raw_cog_uri AS cog_uri,
                     'data' AS asset_role,'cog' AS source_kind,'cog' AS source_format,r.acq_time AS acquisition_time,
                     json_build_object('legacy_table','rs_raw_scene_asset','band',r.band,'version',r.version,'run_id',r.run_id)::jsonb AS attributes,
                     r.ingest_time AS created_at,r.ingest_time AS updated_at FROM rs_raw_scene_asset r
            ) source ON (target.scene_id=source.scene_id AND target.asset_id=source.asset_id)
            WHEN NOT MATCHED THEN INSERT (scene_id,asset_id,source_uri,cog_uri,asset_role,source_kind,source_format,acquisition_time,attributes,created_at,updated_at)
            VALUES (source.scene_id,source.asset_id,source.source_uri,source.cog_uri,source.asset_role,source.source_kind,source.source_format,source.acquisition_time,source.attributes,source.created_at,source.updated_at)""",
            f"""MERGE INTO ingest_run_scenes target USING (
              SELECT ranked.ingest_run_id,ranked.scene_id,ranked.output_version,'completed' AS status,
                     ranked.idempotency_key,ranked.provenance,ranked.created_at,ranked.updated_at
              FROM (
                SELECT current_output.*,
                       ROW_NUMBER() OVER (PARTITION BY current_output.idempotency_key ORDER BY current_output.updated_at DESC,current_output.ingest_run_id) AS owner_rank
                FROM (
                  SELECT versioned.* FROM (
                    SELECT grouped.*,
                           ROW_NUMBER() OVER (PARTITION BY grouped.ingest_run_id,grouped.scene_id ORDER BY grouped.updated_at DESC,grouped.output_version DESC,grouped.dataset_id) AS current_rank
                    FROM (
                      SELECT i.ingest_run_id,{rs_scene_id} AS scene_id,{rs_dataset_id} AS dataset_id,r.version AS output_version,
                             COALESCE((SELECT existing.idempotency_key FROM ingest_run_scenes existing
                                       WHERE existing.ingest_run_id=i.ingest_run_id AND existing.scene_id={rs_scene_id}),
                                      md5({rs_dataset_id} || ':' || {rs_scene_id} || ':' || r.version)) AS idempotency_key,
                             json_build_object('legacy_table','rs_raw_scene_asset','asset_count',COUNT(*),'current_selection','latest_ingest_time')::jsonb AS provenance,
                             MIN(r.ingest_time) AS created_at,MAX(r.ingest_time) AS updated_at
                      FROM rs_raw_scene_asset r JOIN ingest_runs i ON i.ingest_run_id=r.run_id
                      GROUP BY i.ingest_run_id,r.dataset,r.scene_id,r.version
                    ) grouped
                  ) versioned WHERE versioned.current_rank=1
                ) current_output
              ) ranked WHERE ranked.owner_rank=1
            ) source ON (target.idempotency_key=source.idempotency_key)
            WHEN MATCHED THEN UPDATE SET ingest_run_id=source.ingest_run_id,scene_id=source.scene_id,
              output_version=source.output_version,status=source.status,provenance=source.provenance,updated_at=source.updated_at
            WHEN NOT MATCHED THEN INSERT (ingest_run_id,scene_id,output_version,status,idempotency_key,provenance,created_at,updated_at)
            VALUES (source.ingest_run_id,source.scene_id,source.output_version,source.status,source.idempotency_key,source.provenance,source.created_at,source.updated_at)""",
            f"""MERGE INTO migration_lineage target USING (
              SELECT 'rs_raw_scene_asset' AS source_table,r.id::text AS source_key,'asset' AS entity_type,'rs-raw-' || r.id::text AS entity_id,
                     {rs_dataset_id} AS dataset_id,{rs_scene_id} AS scene_id,i.ingest_run_id,1.000::numeric AS confidence,
                     NULL::text AS assignment_issue,json_build_object('run_id',r.run_id,'version',r.version,'band',r.band)::jsonb AS provenance
              FROM rs_raw_scene_asset r LEFT JOIN ingest_runs i ON i.ingest_run_id=r.run_id
            ) source ON (target.source_table=source.source_table AND target.source_key=source.source_key AND target.entity_type=source.entity_type)
            WHEN NOT MATCHED THEN INSERT (source_table,source_key,entity_type,entity_id,dataset_id,scene_id,ingest_run_id,confidence,assignment_issue,provenance)
            VALUES (source.source_table,source.source_key,source.entity_type,source.entity_id,source.dataset_id,source.scene_id,source.ingest_run_id,source.confidence,source.assignment_issue,source.provenance)""",
        ))

    if "rs_entity_tile_asset" in tables:
        entity_scene_id = _rs_scene_id("e.scene_id", _scene_id("'rs'", "e.scene_id"))
        entity_identity = _rs_identity_key("e.scene_id", _identity_key("'rs'", "e.scene_id"))
        entity_dataset_id = f"COALESCE((SELECT s.dataset_id FROM scenes s WHERE s.scene_id=e.scene_id),{_rs_dataset_id('e.dataset')})"
        statements.extend((
            f"""MERGE INTO scenes target USING (
              SELECT {entity_scene_id} AS scene_id,MIN({entity_dataset_id}) AS dataset_id,e.scene_id AS scene_key,{entity_identity} AS identity_key,
                     MIN('rs-entity-' || e.id::text) AS source_asset_id,MIN(e.source_asset_path) AS source_uri,MIN(e.acq_time) AS acquisition_time,
                     'available' AS status,json_build_object('legacy_table','rs_entity_tile_asset','tile_count',COUNT(*),'candidate_dataset_count',COUNT(DISTINCT e.dataset),
                       'assignment_issue',CASE WHEN COUNT(DISTINCT e.dataset)>1 THEN 'RS scene references multiple datasets' ELSE NULL END)::jsonb AS attributes,
                     MIN(e.ingest_time) AS created_at,MAX(e.ingest_time) AS updated_at
              FROM rs_entity_tile_asset e GROUP BY e.scene_id
            ) source ON (target.identity_key=source.identity_key)
            WHEN NOT MATCHED THEN INSERT (scene_id,dataset_id,scene_key,identity_key,source_asset_id,source_uri,acquisition_time,status,attributes,created_at,updated_at)
            VALUES (source.scene_id,source.dataset_id,source.scene_key,source.identity_key,source.source_asset_id,source.source_uri,source.acquisition_time,source.status,source.attributes,source.created_at,source.updated_at)""",
            f"""MERGE INTO migration_lineage target USING (
              SELECT 'rs_entity_tile_asset' AS source_table,e.id::text AS source_key,'entity_tile' AS entity_type,'rs-entity-' || e.id::text AS entity_id,
                     {entity_dataset_id} AS dataset_id,{entity_scene_id} AS scene_id,i.ingest_run_id,1.000::numeric AS confidence,
                     NULL::text AS assignment_issue,json_build_object('tile_uri',e.tile_uri,'tile_version',e.tile_version,'run_id',e.run_id,
                       'grid_type',e.grid_type,'grid_level',e.grid_level,'space_code',e.space_code,'space_code_prefix',e.space_code_prefix,'st_code',e.st_code)::jsonb AS provenance
              FROM rs_entity_tile_asset e LEFT JOIN ingest_runs i ON i.ingest_run_id=e.run_id
            ) source ON (target.source_table=source.source_table AND target.source_key=source.source_key AND target.entity_type=source.entity_type)
            WHEN NOT MATCHED THEN INSERT (source_table,source_key,entity_type,entity_id,dataset_id,scene_id,ingest_run_id,confidence,assignment_issue,provenance)
            VALUES (source.source_table,source.source_key,source.entity_type,source.entity_id,source.dataset_id,source.scene_id,source.ingest_run_id,source.confidence,source.assignment_issue,source.provenance)""",
        ))

    rs_assignment_sources: list[str] = []
    if "rs_raw_scene_asset" in tables:
        rs_assignment_sources.append("SELECT scene_id,dataset FROM rs_raw_scene_asset")
    if "rs_entity_tile_asset" in tables:
        rs_assignment_sources.append("SELECT scene_id,dataset FROM rs_entity_tile_asset")
    if rs_assignment_sources:
        scene_id = _rs_scene_id("x.scene_id", _scene_id("'rs'", "x.scene_id"))
        dataset_id = f"COALESCE((SELECT s.dataset_id FROM scenes s WHERE s.scene_id=x.scene_id),{_rs_dataset_id('MIN(x.dataset)')})"
        statements.append(f"""MERGE INTO scene_dataset_audit target USING (
          SELECT 'audit-rs-' || md5(x.scene_id) AS audit_id,{scene_id} AS scene_id,
                 {dataset_id} AS dataset_id,COUNT(DISTINCT x.dataset) AS candidate_dataset_count
          FROM ({' UNION ALL '.join(rs_assignment_sources)}) x GROUP BY x.scene_id
        ) source ON (target.audit_id=source.audit_id)
        WHEN MATCHED THEN UPDATE SET dataset_id=source.dataset_id,
          reason=CASE WHEN source.candidate_dataset_count>1 THEN 'RS scene references multiple datasets; deterministic dataset selected' ELSE 'RS scene dataset backfill' END,
          attributes=json_build_object('candidate_dataset_count',source.candidate_dataset_count,'assignment_confidence',CASE WHEN source.candidate_dataset_count>1 THEN 0.500 ELSE 1.000 END)::jsonb,
          changed_at=now()
        WHEN NOT MATCHED THEN INSERT (audit_id,scene_id,previous_dataset_id,dataset_id,action,reason,changed_by,attributes)
        VALUES (source.audit_id,source.scene_id,NULL,source.dataset_id,'backfill',
          CASE WHEN source.candidate_dataset_count>1 THEN 'RS scene references multiple datasets; deterministic dataset selected' ELSE 'RS scene dataset backfill' END,
          'm6-migration',json_build_object('candidate_dataset_count',source.candidate_dataset_count,'assignment_confidence',CASE WHEN source.candidate_dataset_count>1 THEN 0.500 ELSE 1.000 END)::jsonb)""")
        statements.append(f"""UPDATE datasets SET status='draft',assignment_confidence=0.500,
          assignment_issue='RS scenes reference multiple datasets',auto_ingest_allowed=FALSE,updated_at=now()
          WHERE dataset_id IN (
            SELECT DISTINCT {_rs_dataset_id('x.dataset')} FROM ({' UNION ALL '.join(rs_assignment_sources)}) x
            JOIN (SELECT scene_id FROM ({' UNION ALL '.join(rs_assignment_sources)}) grouped GROUP BY scene_id HAVING COUNT(DISTINCT dataset)>1) conflicts
              ON conflicts.scene_id=x.scene_id
          )""")

    if "rs_cube_cell_fact" in tables:
        winner_scene = "(c.provenance_json->>'winner_scene_id')"
        cube_scene_id = _rs_scene_id(winner_scene, _scene_id("'rs'", winner_scene))
        statements.append(f"""MERGE INTO migration_lineage target USING (
          SELECT 'rs_cube_cell_fact' AS source_table,c.id::text AS source_key,'cube_cell' AS entity_type,'rs-cube-cell-' || c.id::text AS entity_id,
                 s.dataset_id,s.scene_id,i.ingest_run_id,1.000::numeric AS confidence,NULL::text AS assignment_issue,
                 json_build_object('run_id',c.run_id,'cube_version',c.cube_version,'grid_type',c.grid_type,'grid_level',c.grid_level,
                   'space_code',c.space_code,'time_bucket',c.time_bucket,'band',c.band,'st_code',c.st_code,
                   'value_ref_uri',c.value_ref_uri,'provenance',c.provenance_json::json)::jsonb AS provenance
          FROM rs_cube_cell_fact c LEFT JOIN scenes s ON s.scene_id={cube_scene_id}
          LEFT JOIN ingest_runs i ON i.ingest_run_id=c.run_id
        ) source ON (target.source_table=source.source_table AND target.source_key=source.source_key AND target.entity_type=source.entity_type)
        WHEN NOT MATCHED THEN INSERT (source_table,source_key,entity_type,entity_id,dataset_id,scene_id,ingest_run_id,confidence,assignment_issue,provenance)
        VALUES (source.source_table,source.source_key,source.entity_type,source.entity_id,source.dataset_id,source.scene_id,source.ingest_run_id,source.confidence,source.assignment_issue,source.provenance)""")

    if "rs_product_asset" in tables:
        product_provider = "('rs-product:' || COALESCE(p.product_name,'unknown'))"
        product_scene_id = _rs_scene_id("p.scene_id", _scene_id(product_provider, "p.scene_id"))
        product_identity = _rs_identity_key("p.scene_id", _identity_key(product_provider, "p.scene_id"))
        product_dataset_id = f"COALESCE((SELECT s.dataset_id FROM scenes s WHERE s.scene_id=p.scene_id),{_rs_dataset_id('p.dataset')})"
        statements.extend((
            f"""MERGE INTO scenes target USING (
              SELECT {product_scene_id} AS scene_id,MIN({product_dataset_id}) AS dataset_id,p.scene_id AS scene_key,
                     {product_identity} AS identity_key,MIN('rs-product-' || p.id::text) AS source_asset_id,MIN(p.cog_uri) AS source_uri,
                     MIN(p.acq_time) AS acquisition_time,'available' AS status,
                     json_build_object('legacy_table','rs_product_asset','asset_count',COUNT(*),'product_name',MIN(p.product_name),
                       'candidate_dataset_count',COUNT(DISTINCT p.dataset),'assignment_issue',CASE WHEN COUNT(DISTINCT p.dataset)>1 THEN 'Product scene references multiple datasets' ELSE NULL END)::jsonb AS attributes,
                     MIN(p.ingest_time) AS created_at,MAX(p.ingest_time) AS updated_at
              FROM rs_product_asset p GROUP BY {product_provider},p.scene_id
            ) source ON (target.identity_key=source.identity_key)
            WHEN NOT MATCHED THEN INSERT (scene_id,dataset_id,scene_key,identity_key,source_asset_id,source_uri,acquisition_time,status,attributes,created_at,updated_at)
            VALUES (source.scene_id,source.dataset_id,source.scene_key,source.identity_key,source.source_asset_id,source.source_uri,source.acquisition_time,source.status,source.attributes,source.created_at,source.updated_at)""",
            f"""MERGE INTO migration_lineage target USING (
              SELECT 'rs_product_asset' AS source_table,p.id::text AS source_key,'asset' AS entity_type,'rs-product-' || p.id::text AS entity_id,
                     {product_dataset_id} AS dataset_id,{product_scene_id} AS scene_id,i.ingest_run_id,1.000::numeric AS confidence,NULL::text AS assignment_issue,
                     json_build_object('cog_uri',p.cog_uri,'version',p.version,'run_id',p.run_id,'product_name',p.product_name)::jsonb AS provenance
              FROM rs_product_asset p LEFT JOIN ingest_runs i ON i.ingest_run_id=p.run_id
            ) source ON (target.source_table=source.source_table AND target.source_key=source.source_key AND target.entity_type=source.entity_type)
            WHEN NOT MATCHED THEN INSERT (source_table,source_key,entity_type,entity_id,dataset_id,scene_id,ingest_run_id,confidence,assignment_issue,provenance)
            VALUES (source.source_table,source.source_key,source.entity_type,source.entity_id,source.dataset_id,source.scene_id,source.ingest_run_id,source.confidence,source.assignment_issue,source.provenance)""",
        ))
        statements.append(f"""UPDATE datasets SET status='draft',assignment_confidence=0.500,
          assignment_issue='Product scenes reference multiple datasets',auto_ingest_allowed=FALSE,updated_at=now()
          WHERE dataset_id IN (SELECT DISTINCT {_rs_dataset_id('p.dataset')} FROM rs_product_asset p
            JOIN (SELECT {product_provider} AS provider_namespace,scene_id FROM rs_product_asset p
                  GROUP BY {product_provider},scene_id HAVING COUNT(DISTINCT dataset)>1) conflicts
              ON conflicts.provider_namespace={product_provider} AND conflicts.scene_id=p.scene_id)""")

    if "rs_product_cell_fact" in tables:
        product_fact_dataset_id = _rs_dataset_id("p.dataset")
        statements.append(f"""MERGE INTO migration_lineage target USING (
          SELECT 'rs_product_cell_fact' AS source_table,p.id::text AS source_key,'cube_cell' AS entity_type,'rs-product-cell-' || p.id::text AS entity_id,
                 {product_fact_dataset_id} AS dataset_id,NULL::text AS scene_id,i.ingest_run_id,1.000::numeric AS confidence,NULL::text AS assignment_issue,
                 json_build_object('product_name',p.product_name,'product_band',p.product_band,'cube_version',p.cube_version,'run_id',p.run_id,
                   'grid_type',p.grid_type,'grid_level',p.grid_level,'space_code',p.space_code,'time_bucket',p.time_bucket,'st_code',p.st_code,'value_ref_uri',p.value_ref_uri)::jsonb AS provenance
          FROM rs_product_cell_fact p LEFT JOIN ingest_runs i ON i.ingest_run_id=p.run_id
        ) source ON (target.source_table=source.source_table AND target.source_key=source.source_key AND target.entity_type=source.entity_type)
        WHEN NOT MATCHED THEN INSERT (source_table,source_key,entity_type,entity_id,dataset_id,scene_id,ingest_run_id,confidence,assignment_issue,provenance)
        VALUES (source.source_table,source.source_key,source.entity_type,source.entity_id,source.dataset_id,source.scene_id,source.ingest_run_id,source.confidence,source.assignment_issue,source.provenance)""")

    if "rs_carbon_observation_fact" in tables:
        carbon_dataset = "('rs-dataset-' || md5('carbon:' || c.satellite || ':' || c.product_type))"
        carbon_provider = "('rs-carbon:' || c.satellite || ':' || c.product_type)"
        carbon_scene_id = _scene_id(carbon_provider, "c.observation_id")
        carbon_identity = _identity_key(carbon_provider, "c.observation_id")
        statements.extend((
            f"""MERGE INTO scenes target USING (
              SELECT {carbon_scene_id} AS scene_id,MIN({carbon_dataset}) AS dataset_id,c.observation_id AS scene_key,{carbon_identity} AS identity_key,
                     MIN('rs-carbon-' || c.id::text) AS source_asset_id,MIN(c.source_uri) AS source_uri,MIN(c.acq_time) AS acquisition_time,
                     'available' AS status,json_build_object('legacy_table','rs_carbon_observation_fact','fact_count',COUNT(*))::jsonb AS attributes,
                     MIN(c.ingest_time) AS created_at,MAX(c.ingest_time) AS updated_at FROM rs_carbon_observation_fact c
              GROUP BY {carbon_provider},c.observation_id
            ) source ON (target.identity_key=source.identity_key)
            WHEN NOT MATCHED THEN INSERT (scene_id,dataset_id,scene_key,identity_key,source_asset_id,source_uri,acquisition_time,status,attributes,created_at,updated_at)
            VALUES (source.scene_id,source.dataset_id,source.scene_key,source.identity_key,source.source_asset_id,source.source_uri,source.acquisition_time,source.status,source.attributes,source.created_at,source.updated_at)""",
            f"""MERGE INTO migration_lineage target USING (
              SELECT 'rs_carbon_observation_fact' AS source_table,c.id::text AS source_key,'asset' AS entity_type,'rs-carbon-' || c.id::text AS entity_id,
                     {carbon_dataset} AS dataset_id,{carbon_scene_id} AS scene_id,i.ingest_run_id,1.000::numeric AS confidence,NULL::text AS assignment_issue,
                     json_build_object('observation_id',c.observation_id,'satellite',c.satellite,'product_type',c.product_type,'cube_version',c.cube_version,
                       'run_id',c.run_id,'grid_type',c.grid_type,'grid_level',c.grid_level,'space_code',c.space_code,'st_code',c.st_code,'source_uri',c.source_uri)::jsonb AS provenance
              FROM rs_carbon_observation_fact c LEFT JOIN ingest_runs i ON i.ingest_run_id=c.run_id
            ) source ON (target.source_table=source.source_table AND target.source_key=source.source_key AND target.entity_type=source.entity_type)
            WHEN NOT MATCHED THEN INSERT (source_table,source_key,entity_type,entity_id,dataset_id,scene_id,ingest_run_id,confidence,assignment_issue,provenance)
            VALUES (source.source_table,source.source_key,source.entity_type,source.entity_id,source.dataset_id,source.scene_id,source.ingest_run_id,source.confidence,source.assignment_issue,source.provenance)""",
        ))

    if {"ard_partition_batches", "ard_partition_observations"} <= tables:
        exact_dataset = (
            "(SELECT MIN(da.dataset_id) FROM partition_dataset_assets da WHERE da.source_asset_id=o.observation_id),"
            if "partition_dataset_assets" in tables else ""
        )
        group_key = "(LOWER(COALESCE(b.data_type,'carbon')) || ':' || LOWER(COALESCE(NULLIF(o.sensor,''),'unknown')) || ':' || LOWER(COALESCE(NULLIF(o.product_family,''),'unknown')) || ':' || LOWER(COALESCE(NULLIF(o.product_type,''),'unknown')))"
        obs_dataset = f"COALESCE({exact_dataset}'legacy-dataset-' || md5({group_key}))"
        obs_scene_key = "o.observation_id"
        obs_provider = "COALESCE(b.source_system,'loader')"
        obs_scene_id = _scene_id(obs_provider, obs_scene_key)
        obs_identity = _identity_key(obs_provider, obs_scene_key)
        statements.extend((
            f"""MERGE INTO scenes target USING (
              SELECT {obs_scene_id} AS scene_id,{obs_dataset} AS dataset_id,o.observation_id AS scene_key,
                     {obs_identity} AS identity_key,o.observation_id AS source_asset_id,o.source_uri,
                     NULL::char(64) AS checksum,o.acq_time AS acquisition_time,
                     CASE WHEN o.lon IS NULL OR o.lat IS NULL THEN NULL::jsonb ELSE json_build_array(o.lon,o.lat,o.lon,o.lat)::jsonb END AS bbox,
                     'EPSG:4326'::text AS crs,CASE WHEN b.status='succeeded' THEN 'loaded' WHEN b.status='failed' THEN 'failed' ELSE 'discovered' END AS status,
                     json_build_object('legacy_table','ard_partition_observations','sensor',o.sensor,'product_family',o.product_family,'product_type',o.product_type,'source_index',o.source_index,'migration_inferred',true,'auto_ingest_allowed',false)::jsonb AS attributes,
                     COALESCE(b.loaded_at,now()) AS created_at,COALESCE(b.updated_at,b.loaded_at,now()) AS updated_at
              FROM ard_partition_observations o JOIN ard_partition_batches b ON b.id=o.batch_id
            ) source ON (target.identity_key=source.identity_key)
            WHEN NOT MATCHED THEN INSERT (scene_id,dataset_id,scene_key,identity_key,source_asset_id,source_uri,checksum,acquisition_time,bbox,crs,status,attributes,created_at,updated_at)
            VALUES (source.scene_id,source.dataset_id,source.scene_key,source.identity_key,source.source_asset_id,source.source_uri,source.checksum,source.acquisition_time,source.bbox,source.crs,source.status,source.attributes,source.created_at,source.updated_at)""",
            f"""MERGE INTO load_batch_scenes target USING (
              SELECT b.batch_id AS load_batch_id,s.scene_id,o.observation_id AS source_asset_id,o.source_uri,
                     NULL::char(64) AS checksum,
                     CASE WHEN b.status IN ('pending','running','failed','cancelled') THEN b.status WHEN b.status='succeeded' THEN 'succeeded' ELSE 'unknown' END AS load_status,
                     NULL::text AS error_message,
                     json_build_object('legacy_table','ard_partition_observations','migration_inferred',true,'auto_ingest_allowed',false)::jsonb AS attributes,
                     COALESCE(b.loaded_at,now()) AS created_at,COALESCE(b.updated_at,b.loaded_at,now()) AS updated_at
              FROM ard_partition_observations o JOIN ard_partition_batches b ON b.id=o.batch_id
              JOIN scenes s ON s.identity_key={obs_identity}
            ) source ON (target.load_batch_id=source.load_batch_id AND target.scene_id=source.scene_id)
            WHEN NOT MATCHED THEN INSERT (load_batch_id,scene_id,source_asset_id,source_uri,checksum,load_status,error_message,attributes,created_at,updated_at)
            VALUES (source.load_batch_id,source.scene_id,source.source_asset_id,source.source_uri,source.checksum,source.load_status,source.error_message,source.attributes,source.created_at,source.updated_at)""",
            f"""MERGE INTO scene_assets target USING (
              SELECT s.scene_id,o.observation_id AS asset_id,o.source_uri,NULL::text AS cog_uri,'data' AS asset_role,
                     'observation' AS source_kind,'observation' AS source_format,NULL::char(64) AS checksum,o.acq_time AS acquisition_time,
                     CASE WHEN o.lon IS NULL OR o.lat IS NULL THEN NULL::jsonb ELSE json_build_array(o.lon,o.lat,o.lon,o.lat)::jsonb END AS bbox,
                     'EPSG:4326'::text AS crs,
                     json_build_object('sensor',o.sensor,'product_family',o.product_family,'product_type',o.product_type,'source_index',o.source_index,'xco2',o.xco2,'quality_flag',o.quality_flag)::jsonb AS attributes,
                     COALESCE(b.loaded_at,now()) AS created_at,COALESCE(b.updated_at,b.loaded_at,now()) AS updated_at
              FROM ard_partition_observations o JOIN ard_partition_batches b ON b.id=o.batch_id
              JOIN scenes s ON s.identity_key={obs_identity}
            ) source ON (target.scene_id=source.scene_id AND target.asset_id=source.asset_id)
            WHEN NOT MATCHED THEN INSERT (scene_id,asset_id,source_uri,cog_uri,asset_role,source_kind,source_format,checksum,acquisition_time,bbox,crs,attributes,created_at,updated_at)
            VALUES (source.scene_id,source.asset_id,source.source_uri,source.cog_uri,source.asset_role,source.source_kind,source.source_format,source.checksum,source.acquisition_time,source.bbox,source.crs,source.attributes,source.created_at,source.updated_at)""",
        ))

    if "partition_job_attempts" in tables:
        statements.extend((
            """MERGE INTO partition_runs target USING (
              SELECT 'partition-run-legacy-' || md5(j.task_id) AS partition_run_id,
                     CASE WHEN j.status IN ('succeeded','completed') THEN 'completed' WHEN j.status='failed' THEN 'failed'
                          WHEN j.status='cancelled' THEN 'cancelled' WHEN j.status IN ('pending','queued') THEN j.status ELSE 'running' END AS status,
                     json_build_array(j.batch_id)::jsonb AS source_load_batch_ids,j.requested_by,j.error_message,
                     json_build_object('legacy_table','partition_job_attempts','task_id',j.task_id,'operation',j.operation,'attempt_no',j.attempt_no,'payload',j.payload::json)::jsonb AS attributes,
                     j.created_at,j.started_at,j.finished_at AS completed_at FROM partition_job_attempts j
            ) source ON (target.partition_run_id=source.partition_run_id)
            WHEN NOT MATCHED THEN INSERT (partition_run_id,status,source_load_batch_ids,requested_by,error_message,attributes,created_at,started_at,completed_at)
            VALUES (source.partition_run_id,source.status,source.source_load_batch_ids,source.requested_by,source.error_message,source.attributes,source.created_at,source.started_at,source.completed_at)""",
            """MERGE INTO migration_lineage target USING (
              SELECT 'partition_job_attempts' AS source_table,j.task_id AS source_key,'partition_run' AS entity_type,
                     'partition-run-legacy-' || md5(j.task_id) AS entity_id,NULL::text AS dataset_id,NULL::text AS scene_id,NULL::text AS ingest_run_id,
                     CASE WHEN COALESCE(array_length(j.asset_ids,1),0)>0 THEN 1.000 ELSE 0.500 END::numeric AS confidence,
                     CASE WHEN COALESCE(array_length(j.asset_ids,1),0)>0 THEN NULL::text ELSE 'Legacy partition attempt has no asset-level scene scope' END AS assignment_issue,
                     json_build_object('batch_id',j.batch_id,'asset_ids',array_to_json(j.asset_ids),'status',j.status)::jsonb AS provenance
              FROM partition_job_attempts j
            ) source ON (target.source_table=source.source_table AND target.source_key=source.source_key AND target.entity_type=source.entity_type)
            WHEN NOT MATCHED THEN INSERT (source_table,source_key,entity_type,entity_id,dataset_id,scene_id,ingest_run_id,confidence,assignment_issue,provenance)
            VALUES (source.source_table,source.source_key,source.entity_type,source.entity_id,source.dataset_id,source.scene_id,source.ingest_run_id,source.confidence,source.assignment_issue,source.provenance)""",
        ))

    if {"partition_output_versions", "partition_datasets"} <= tables:
        statements.append(
            """MERGE INTO partition_runs target USING (
              SELECT 'partition-run-legacy-' || md5(o.task_id) AS partition_run_id,
                     CASE WHEN MAX(CASE WHEN o.status='failed' THEN 1 ELSE 0 END)=1 AND MAX(CASE WHEN o.status='completed' THEN 1 ELSE 0 END)=1 THEN 'partial_failure'
                          WHEN MAX(CASE WHEN o.status='failed' THEN 1 ELSE 0 END)=1 THEN 'failed'
                          WHEN MIN(CASE WHEN o.status='completed' THEN 1 ELSE 0 END)=1 THEN 'completed' ELSE 'running' END AS status,
                     json_build_array(MIN(d.batch_id))::jsonb AS source_load_batch_ids,
                     MAX(o.error_message) AS error_message, json_build_object('legacy_task_id',o.task_id)::jsonb AS attributes,
                     MIN(o.created_at) AS created_at, MIN(o.created_at) AS started_at,
                     MAX(COALESCE(o.completed_at,o.failed_at)) AS completed_at
              FROM partition_output_versions o JOIN partition_datasets d ON d.dataset_id = o.dataset_id
              GROUP BY o.task_id
            ) source ON (target.partition_run_id = source.partition_run_id)
            WHEN NOT MATCHED THEN INSERT (partition_run_id,status,source_load_batch_ids,error_message,attributes,created_at,started_at,completed_at)
            VALUES (source.partition_run_id,source.status,source.source_load_batch_ids,source.error_message,source.attributes,source.created_at,source.started_at,source.completed_at)"""
        )
        if "partition_job_attempts" in tables:
            statements.append("""MERGE INTO partition_run_scenes target USING (
              SELECT DISTINCT 'partition-run-legacy-' || md5(o.task_id) AS partition_run_id,s.scene_id,s.dataset_id,
                     d.batch_id AS source_load_batch_id,
                     CASE WHEN o.status='failed' THEN 'failed' WHEN o.status='completed' THEN 'completed' ELSE 'running' END AS status,
                     json_build_object('grid_type',o.grid_type,'grid_level',o.requested_grid_level,'grid_level_name',o.requested_grid_level_name,'partition_method',o.partition_method)::jsonb AS grid_config,
                     o.output_version,md5(o.task_id || ':' || s.scene_id) AS idempotency_key,
                     o.error_message,o.created_at,COALESCE(o.completed_at,o.failed_at,o.created_at) AS updated_at
              FROM (
                SELECT ranked.* FROM (
                  SELECT source.*,ROW_NUMBER() OVER (PARTITION BY source.task_id,source.dataset_id ORDER BY COALESCE(source.completed_at,source.failed_at,source.created_at) DESC,source.output_version DESC) AS current_rank
                  FROM partition_output_versions source
                ) ranked WHERE ranked.current_rank=1
              ) o JOIN partition_datasets d ON d.dataset_id=o.dataset_id
              JOIN partition_job_attempts j ON j.task_id=o.task_id
              JOIN scene_assets sa ON sa.asset_id=ANY(j.asset_ids)
              JOIN scenes s ON s.scene_id=sa.scene_id AND s.dataset_id=o.dataset_id
            ) source ON (target.partition_run_id = source.partition_run_id AND target.scene_id = source.scene_id)
            WHEN MATCHED THEN UPDATE SET dataset_id=source.dataset_id,source_load_batch_id=source.source_load_batch_id,
              status=source.status,grid_config=source.grid_config,output_version=source.output_version,
              idempotency_key=source.idempotency_key,error_message=source.error_message,updated_at=source.updated_at
            WHEN NOT MATCHED THEN INSERT (partition_run_id,scene_id,dataset_id,source_load_batch_id,status,grid_config,output_version,idempotency_key,error_message,created_at,updated_at)
            VALUES (source.partition_run_id,source.scene_id,source.dataset_id,source.source_load_batch_id,source.status,source.grid_config,source.output_version,source.idempotency_key,source.error_message,source.created_at,source.updated_at)""")
        attempt_join = "LEFT JOIN partition_job_attempts j ON j.task_id=o.task_id" if "partition_job_attempts" in tables else ""
        confidence = "CASE WHEN COALESCE(array_length(j.asset_ids,1),0)>0 THEN 1.000 ELSE 0.250 END" if "partition_job_attempts" in tables else "0.250"
        issue = "CASE WHEN COALESCE(array_length(j.asset_ids,1),0)>0 THEN NULL::text ELSE 'Legacy output has no asset-level scene scope'::text END" if "partition_job_attempts" in tables else "'Legacy output has no asset-level scene scope'::text"
        statements.append(f"""MERGE INTO migration_lineage target USING (
          SELECT 'partition_output_versions' AS source_table,o.dataset_id || ':' || o.output_version AS source_key,
                 'partition_run' AS entity_type,'partition-run-legacy-' || md5(o.task_id) AS entity_id,
                 o.dataset_id,NULL::text AS scene_id,NULL::text AS ingest_run_id,{confidence}::numeric AS confidence,
                 {issue} AS assignment_issue,
                 json_build_object('task_id',o.task_id,'output_version',o.output_version,'status',o.status)::jsonb AS provenance
          FROM partition_output_versions o {attempt_join}
        ) source ON (target.source_table=source.source_table AND target.source_key=source.source_key AND target.entity_type=source.entity_type)
        WHEN NOT MATCHED THEN INSERT (source_table,source_key,entity_type,entity_id,dataset_id,scene_id,ingest_run_id,confidence,assignment_issue,provenance)
        VALUES (source.source_table,source.source_key,source.entity_type,source.entity_id,source.dataset_id,source.scene_id,source.ingest_run_id,source.confidence,source.assignment_issue,source.provenance)""")

    if "partition_batches" in tables:
        statements.append("""MERGE INTO ingest_runs target USING (
          SELECT 'ingest-run-legacy-' || md5(COALESCE(NULLIF(b.ingest_job_id,''),b.batch_id) || ':' || s.dataset_id) AS ingest_run_id,
                 s.dataset_id,
                 CASE WHEN b.ingest_status = 'ingested' THEN 'completed' WHEN b.ingest_status = 'failed' THEN 'failed' WHEN b.ingest_status IN ('running','queued') THEN b.ingest_status ELSE 'pending' END AS status,
                 b.ingest_error AS error_message,
                 json_build_object('legacy_ingest_job_id',b.ingest_job_id,'source_load_batch_id',b.batch_id)::jsonb AS attributes,
                 b.created_at, b.ingested_at AS completed_at
          FROM partition_batches b JOIN load_batch_scenes lbs ON lbs.load_batch_id = b.batch_id
          JOIN scenes s ON s.scene_id = lbs.scene_id
          JOIN datasets ds ON ds.dataset_id=s.dataset_id
          WHERE ds.auto_ingest_allowed=TRUE AND (b.ingest_job_id IS NOT NULL OR b.ingested_at IS NOT NULL OR b.ingest_status IN ('ingested','failed','running','queued'))
          GROUP BY b.batch_id,b.ingest_job_id,b.ingest_status,b.ingest_error,b.created_at,b.ingested_at,s.dataset_id
        ) source ON (target.ingest_run_id = source.ingest_run_id)
        WHEN NOT MATCHED THEN INSERT (ingest_run_id,dataset_id,status,error_message,attributes,created_at,completed_at)
        VALUES (source.ingest_run_id,source.dataset_id,source.status,source.error_message,source.attributes,source.created_at,source.completed_at)""")

        statements.append("""MERGE INTO ingest_run_scenes target USING (
          SELECT MIN('ingest-run-legacy-' || md5(COALESCE(NULLIF(b.ingest_job_id,''),b.batch_id) || ':' || s.dataset_id)) AS ingest_run_id,
                 s.scene_id,NULL::text AS partition_run_id,ds.current_output_version AS output_version,
                 CASE WHEN MAX(CASE WHEN b.ingest_status='ingested' THEN 1 ELSE 0 END)=1 THEN 'completed'
                      WHEN MAX(CASE WHEN b.ingest_status='failed' THEN 1 ELSE 0 END)=1 THEN 'failed'
                      WHEN MAX(CASE WHEN b.ingest_status='running' THEN 1 ELSE 0 END)=1 THEN 'running'
                      WHEN MAX(CASE WHEN b.ingest_status='queued' THEN 1 ELSE 0 END)=1 THEN 'queued' ELSE 'pending' END AS status,
                 md5(s.dataset_id || ':' || s.scene_id || ':' || COALESCE(ds.current_output_version,'legacy-unknown')) AS idempotency_key,
                 MAX(b.ingest_error) AS error_message,
                 json_build_object('source_load_batch_count',COUNT(DISTINCT b.batch_id),'migration_source','partition_batches')::jsonb AS provenance,
                 MIN(b.created_at) AS created_at,MAX(COALESCE(b.ingested_at,b.updated_at,b.created_at)) AS updated_at
          FROM partition_batches b JOIN load_batch_scenes lbs ON lbs.load_batch_id=b.batch_id
          JOIN scenes s ON s.scene_id=lbs.scene_id
          JOIN datasets ds ON ds.dataset_id=s.dataset_id
          WHERE ds.auto_ingest_allowed=TRUE AND (b.ingest_job_id IS NOT NULL OR b.ingested_at IS NOT NULL OR b.ingest_status IN ('ingested','failed','running','queued'))
          GROUP BY s.dataset_id,s.scene_id,ds.current_output_version
        ) source ON (target.idempotency_key=source.idempotency_key)
        WHEN MATCHED THEN UPDATE SET ingest_run_id=source.ingest_run_id,scene_id=source.scene_id,
          output_version=source.output_version,status=source.status,
          error_message=source.error_message,provenance=source.provenance,updated_at=source.updated_at
        WHEN NOT MATCHED THEN INSERT (ingest_run_id,scene_id,partition_run_id,output_version,status,idempotency_key,error_message,provenance,created_at,updated_at)
        VALUES (source.ingest_run_id,source.scene_id,source.partition_run_id,source.output_version,source.status,source.idempotency_key,source.error_message,source.provenance,source.created_at,source.updated_at)""")

    statements.append("""MERGE INTO scene_dataset_audit target USING (
      SELECT 'audit-' || md5(scene_id) AS audit_id,scene_id,dataset_id FROM scenes
    ) source ON (target.audit_id=source.audit_id)
    WHEN NOT MATCHED THEN INSERT (audit_id,scene_id,previous_dataset_id,dataset_id,action,reason,changed_by,attributes)
    VALUES (source.audit_id,source.scene_id,NULL,source.dataset_id,'backfill','M6 legacy scene assignment','m6-migration','{}'::jsonb)""")

    return tuple(statements)


def _detect_legacy_tables(cursor: Any) -> set[str]:
    cursor.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = current_schema() AND table_name = ANY(%s::text[])",
        (sorted(LEGACY_SOURCE_TABLES),),
    )
    return {str(row[0]) for row in cursor.fetchall()}


def _scalar(cursor: Any, query: str) -> int:
    cursor.execute(query)
    row = cursor.fetchone()
    return 0 if row is None else int(row[0])


def _verify(cursor: Any, legacy_tables: set[str]) -> dict[str, int]:
    failures: list[str] = []
    if "partition_datasets" in legacy_tables:
        missing = _scalar(cursor, "SELECT COUNT(*) FROM partition_datasets p WHERE NOT EXISTS (SELECT 1 FROM datasets d WHERE d.legacy_partition_dataset_id = p.dataset_id)")
        if missing:
            failures.append(f"partition_datasets not preserved: {missing}")
    if "partition_batches" in legacy_tables:
        missing = _scalar(cursor, "SELECT COUNT(*) FROM partition_batches b WHERE NOT EXISTS (SELECT 1 FROM load_batches l WHERE l.load_batch_id=b.batch_id)")
        if missing:
            failures.append(f"partition_batches not preserved: {missing}")
    if "ard_partition_batches" in legacy_tables:
        missing = _scalar(cursor, "SELECT COUNT(*) FROM ard_partition_batches b WHERE NOT EXISTS (SELECT 1 FROM load_batches l WHERE l.load_batch_id=b.batch_id)")
        if missing:
            failures.append(f"ard_partition_batches not preserved: {missing}")
    if "partition_assets" in legacy_tables:
        missing = _scalar(cursor, "SELECT COUNT(*) FROM partition_assets a WHERE NOT EXISTS (SELECT 1 FROM scene_assets sa WHERE sa.asset_id = a.asset_id AND sa.source_uri = a.source_uri)")
        if missing:
            failures.append(f"partition_assets not preserved as scene assets: {missing}")
    if "partition_dataset_assets" in legacy_tables:
        missing = _scalar(cursor, "SELECT COUNT(*) FROM partition_dataset_assets a WHERE NOT EXISTS (SELECT 1 FROM scene_assets sa WHERE sa.asset_id = a.source_asset_id AND sa.source_uri = a.source_uri)")
        if missing:
            failures.append(f"partition_dataset_assets not preserved: {missing}")
    if "ard_partition_assets" in legacy_tables:
        missing = _scalar(cursor, "SELECT COUNT(*) FROM ard_partition_assets a WHERE NOT EXISTS (SELECT 1 FROM scene_assets sa WHERE sa.asset_id = a.asset_id AND sa.source_uri = a.source_uri)")
        if missing:
            failures.append(f"ard_partition_assets not preserved: {missing}")
    if "ard_partition_observations" in legacy_tables:
        missing = _scalar(cursor, "SELECT COUNT(*) FROM ard_partition_observations o WHERE NOT EXISTS (SELECT 1 FROM scene_assets sa WHERE sa.asset_id = o.observation_id AND sa.source_uri = o.source_uri)")
        if missing:
            failures.append(f"ard_partition_observations not preserved: {missing}")
    if "partition_dataset_bands" in legacy_tables:
        missing = _scalar(cursor, "SELECT COUNT(*) FROM partition_dataset_bands b WHERE NOT EXISTS (SELECT 1 FROM scene_bands sb WHERE sb.asset_id=b.source_asset_id AND sb.band_code=b.band_code)")
        if missing:
            failures.append(f"partition_dataset_bands not preserved: {missing}")
    if "partition_job_attempts" in legacy_tables:
        missing = _scalar(cursor, "SELECT COUNT(*) FROM partition_job_attempts j WHERE NOT EXISTS (SELECT 1 FROM migration_lineage l WHERE l.source_table='partition_job_attempts' AND l.source_key=j.task_id AND l.entity_type='partition_run')")
        if missing:
            failures.append(f"partition_job_attempts not preserved: {missing}")
    if "partition_output_versions" in legacy_tables:
        missing = _scalar(cursor, "SELECT COUNT(*) FROM partition_output_versions o WHERE NOT EXISTS (SELECT 1 FROM migration_lineage l WHERE l.source_table='partition_output_versions' AND l.source_key=o.dataset_id || ':' || o.output_version AND l.entity_type='partition_run')")
        if missing:
            failures.append(f"partition_output_versions not preserved: {missing}")
    rs_lineage = {
        "rs_ingest_job": ("job_id", "ingest_run"),
        "rs_raw_scene_asset": ("id::text", "asset"),
        "rs_cube_cell_fact": ("id::text", "cube_cell"),
        "rs_entity_tile_asset": ("id::text", "entity_tile"),
        "rs_product_asset": ("id::text", "asset"),
        "rs_product_cell_fact": ("id::text", "cube_cell"),
        "rs_carbon_observation_fact": ("id::text", "asset"),
    }
    for table, (key_expression, entity_type) in rs_lineage.items():
        if table not in legacy_tables:
            continue
        missing = _scalar(cursor, f"SELECT COUNT(*) FROM {table} source WHERE NOT EXISTS (SELECT 1 FROM migration_lineage l WHERE l.source_table='{table}' AND l.source_key=source.{key_expression} AND l.entity_type='{entity_type}')")
        if missing:
            failures.append(f"{table} rows without migration lineage: {missing}")

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
        raise RuntimeError("M6 migration validation failed: " + "; ".join(failures))

    return {
        "datasets": _scalar(cursor, "SELECT COUNT(*) FROM datasets"),
        "scenes": _scalar(cursor, "SELECT COUNT(*) FROM scenes"),
        "scene_assets": _scalar(cursor, "SELECT COUNT(*) FROM scene_assets"),
        "load_batches": _scalar(cursor, "SELECT COUNT(*) FROM load_batches"),
        "load_batch_scenes": _scalar(cursor, "SELECT COUNT(*) FROM load_batch_scenes"),
        "partition_runs": _scalar(cursor, "SELECT COUNT(*) FROM partition_runs"),
        "ingest_runs": _scalar(cursor, "SELECT COUNT(*) FROM ingest_runs"),
        "migration_lineage": _scalar(cursor, "SELECT COUNT(*) FROM migration_lineage"),
    }


def apply_m6_scene_schema(connection: Any) -> MigrationReport:
    """Apply, backfill, verify, and commit M6 atomically; roll back on failure."""
    try:
        with connection.cursor() as cursor:
            legacy_tables = _detect_legacy_tables(cursor)
            for statement in schema_statements():
                cursor.execute(statement)
            for statement in backfill_statements(legacy_tables):
                cursor.execute(statement)
            counts = _verify(cursor, legacy_tables)
            cursor.execute(
                """MERGE INTO m6_scene_schema_version target
                USING (SELECT TRUE AS singleton, %s::text AS schema_version, %s::jsonb AS migration_report) source
                ON (target.singleton = source.singleton)
                WHEN MATCHED THEN UPDATE SET schema_version=source.schema_version, installed_at=now(), migration_report=source.migration_report
                WHEN NOT MATCHED THEN INSERT (singleton,schema_version,migration_report) VALUES (source.singleton,source.schema_version,source.migration_report)""",
                (M6_SCENE_SCHEMA_VERSION, json.dumps(counts, sort_keys=True)),
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return MigrationReport(
        schema_version=M6_SCENE_SCHEMA_VERSION,
        existing_legacy_tables=tuple(sorted(legacy_tables)),
        **counts,
    )
