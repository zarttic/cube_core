"""Canonical M2 partition-domain DDL and catalog inspection helpers.

The legacy scheduling tables are intentionally retained.  This module owns only
the versioned dataset/result domain added by M2 and can therefore be applied to
an existing loader database without destructive migration behaviour.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

PARTITION_DOMAIN_SCHEMA_VERSION = "2026-07-16-m2-mixed-carbon-v1"
NEW_DOMAIN_TABLES = {
    "partition_datasets", "partition_dataset_assets", "partition_dataset_bands",
    "partition_output_versions", "partition_tiles", "partition_indexes", "partition_grid_cells",
    "partition_quality_runs", "partition_quality_results", "partition_quality_errors",
    "partition_quality_warn_approvals", "partition_publications", "partition_domain_outbox",
    "partition_domain_schema_version",
}
NEW_DOMAIN_OBJECTS = NEW_DOMAIN_TABLES | {
    "idx_partition_quality_claim", "idx_partition_quality_errors_page", "idx_partition_quality_errors_filter",
    "uq_partition_publication_live_snapshot", "idx_partition_publication_claim",
    "idx_partition_publication_dataset_latest", "idx_partition_domain_outbox_claim",
}
LEGACY_ALLOWLIST = {"quality_reports", "partition_batches", "partition_assets", "partition_job_attempts"}


@dataclass(frozen=True)
class CatalogObject:
    kind: str
    schema: str
    table: str | None
    name: str


@dataclass(frozen=True)
class CatalogConstraint:
    table: str
    name: str
    definition: str


@dataclass(frozen=True)
class PartitionObjectInventory:
    objects: set[tuple[str, str, str | None, str]]
    indexes: set[tuple[str, tuple[str, ...], str | None]]
    constraints: tuple[CatalogConstraint, ...]


def schema_statements() -> tuple[str, ...]:
    """Return ordered, executable, additive DDL for the M2 domain."""
    return (
        """CREATE TABLE IF NOT EXISTS partition_datasets (
          dataset_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL REFERENCES partition_batches(batch_id),
          dataset_code TEXT NOT NULL UNIQUE, dataset_title TEXT NOT NULL,
          data_type TEXT NOT NULL CHECK (data_type IN ('optical','radar','product','carbon')),
          product_type TEXT, attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
          grid_type TEXT NOT NULL CHECK (grid_type IN ('geohash','mgrs','isea4h')),
          requested_grid_level INT NOT NULL, requested_grid_level_name TEXT NOT NULL,
          partition_method TEXT NOT NULL CHECK (partition_method IN ('logical','entity')),
          cover_mode TEXT NOT NULL CHECK (cover_mode IN ('intersect','contain','minimal')),
          partition_status TEXT NOT NULL CHECK (partition_status IN ('pending','queued','running','completed','failed','cancelled')),
          current_output_version TEXT, current_quality_run_id UUID,
          quality_status TEXT NOT NULL DEFAULT 'pending' CHECK (quality_status IN ('pending','running','pass','warn','fail','error','cancelled')),
          quality_sequence BIGINT NOT NULL DEFAULT 0 CHECK (quality_sequence >= 0),
          quality_error_count BIGINT NOT NULL DEFAULT 0 CHECK (quality_error_count >= 0),
          quality_warning_count BIGINT NOT NULL DEFAULT 0 CHECK (quality_warning_count >= 0),
          partition_completed_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CHECK ((partition_status = 'completed' AND partition_completed_at IS NOT NULL) OR
                 (partition_status <> 'completed' AND partition_completed_at IS NULL))
        )""",
        """CREATE TABLE IF NOT EXISTS partition_dataset_assets (
          dataset_id TEXT NOT NULL REFERENCES partition_datasets(dataset_id) ON DELETE CASCADE,
          source_asset_id TEXT NOT NULL, cog_uri TEXT CHECK (cog_uri LIKE 's3://%'),
          source_uri TEXT NOT NULL CHECK (source_uri LIKE 's3://%'),
          source_kind TEXT NOT NULL DEFAULT 'cog' CHECK (source_kind IN ('cog','raw')),
          source_format TEXT NOT NULL DEFAULT 'cog' CHECK (source_format IN ('cog','netcdf','hdf5')),
          checksum CHAR(64) CHECK (checksum ~ '^[0-9a-f]{64}$'), bbox JSONB, crs TEXT,
          time_start TIMESTAMPTZ, time_end TIMESTAMPTZ, attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (dataset_id, source_asset_id),
          CHECK (time_end IS NULL OR time_start IS NULL OR time_end >= time_start),
          CHECK ((source_kind = 'cog' AND source_format = 'cog' AND cog_uri IS NOT NULL) OR
                 (source_kind = 'raw' AND source_format IN ('netcdf','hdf5')))
        )""",
        """CREATE TABLE IF NOT EXISTS partition_dataset_bands (
          dataset_id TEXT NOT NULL, source_asset_id TEXT NOT NULL, band_code TEXT NOT NULL,
          band_name TEXT, band_type TEXT CHECK (band_type IN ('spectral','polarization','variable')),
          unit TEXT, display_order INT NOT NULL DEFAULT 0 CHECK (display_order >= 0),
          attributes JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (dataset_id, source_asset_id, band_code),
          FOREIGN KEY (dataset_id, source_asset_id) REFERENCES partition_dataset_assets(dataset_id, source_asset_id) ON DELETE CASCADE
        )""",
        # Existing M2 databases stored every source in a non-null ``cog_uri``.
        # Carbon observations retain their raw NetCDF/HDF source URI instead.
        """ALTER TABLE partition_dataset_assets ADD COLUMN IF NOT EXISTS source_uri TEXT""",
        """ALTER TABLE partition_dataset_assets ADD COLUMN IF NOT EXISTS source_kind TEXT NOT NULL DEFAULT 'cog'""",
        """ALTER TABLE partition_dataset_assets ADD COLUMN IF NOT EXISTS source_format TEXT NOT NULL DEFAULT 'cog'""",
        """UPDATE partition_dataset_assets SET source_uri = cog_uri WHERE source_uri IS NULL AND cog_uri IS NOT NULL""",
        """ALTER TABLE partition_dataset_assets ALTER COLUMN source_uri SET NOT NULL""",
        """ALTER TABLE partition_dataset_assets ALTER COLUMN cog_uri DROP NOT NULL""",
        """DO $$ BEGIN
          ALTER TABLE partition_dataset_assets ADD CONSTRAINT partition_dataset_assets_source_uri_s3
          CHECK (source_uri LIKE 's3://%');
        EXCEPTION WHEN duplicate_object THEN NULL; END $$""",
        """DO $$ BEGIN
          ALTER TABLE partition_dataset_assets ADD CONSTRAINT partition_dataset_assets_source_format_check
          CHECK (source_format IN ('cog','netcdf','hdf5'));
        EXCEPTION WHEN duplicate_object THEN NULL; END $$""",
        """DO $$ BEGIN
          ALTER TABLE partition_dataset_assets ADD CONSTRAINT partition_dataset_assets_source_kind_check
          CHECK (source_kind IN ('cog','raw'));
        EXCEPTION WHEN duplicate_object THEN NULL; END $$""",
        """DO $$ BEGIN
          ALTER TABLE partition_dataset_assets ADD CONSTRAINT partition_dataset_assets_source_contract_check
          CHECK ((source_kind = 'cog' AND source_format = 'cog' AND cog_uri IS NOT NULL) OR
                 (source_kind = 'raw' AND source_format IN ('netcdf','hdf5')));
        EXCEPTION WHEN duplicate_object THEN NULL; END $$""",
        """CREATE TABLE IF NOT EXISTS partition_output_versions (
          dataset_id TEXT NOT NULL, output_version TEXT NOT NULL UNIQUE,
          task_id TEXT NOT NULL REFERENCES partition_job_attempts(task_id),
          grid_type TEXT NOT NULL CHECK (grid_type IN ('geohash','mgrs','isea4h')),
          requested_grid_level INT NOT NULL, requested_grid_level_name TEXT NOT NULL,
          partition_method TEXT NOT NULL CHECK (partition_method IN ('logical','entity')),
          status TEXT NOT NULL CHECK (status IN ('staging','completed','failed','superseded')),
          object_prefix TEXT NOT NULL, tile_count BIGINT NOT NULL DEFAULT 0 CHECK (tile_count >= 0),
          index_count BIGINT NOT NULL DEFAULT 0 CHECK (index_count >= 0),
          grid_cell_count BIGINT NOT NULL DEFAULT 0 CHECK (grid_cell_count >= 0),
          counts JSONB NOT NULL DEFAULT '{}'::jsonb, error_code TEXT, error_message TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(), completed_at TIMESTAMPTZ, failed_at TIMESTAMPTZ,
          PRIMARY KEY (dataset_id, output_version),
          CHECK ((status = 'completed' AND completed_at IS NOT NULL AND failed_at IS NULL AND error_code IS NULL) OR
                 (status = 'failed' AND failed_at IS NOT NULL AND length(coalesce(error_code, '')) > 0) OR
                 (status = 'staging' AND completed_at IS NULL AND failed_at IS NULL) OR
                 (status = 'superseded' AND completed_at IS NOT NULL AND failed_at IS NULL AND error_code IS NULL))
        )""",
        """CREATE TABLE IF NOT EXISTS partition_grid_cells (
          output_id TEXT PRIMARY KEY, dataset_id TEXT NOT NULL, output_version TEXT NOT NULL,
          grid_type TEXT NOT NULL, grid_level INT NOT NULL, grid_level_name TEXT NOT NULL,
          space_code TEXT NOT NULL, topology_code TEXT,
          normalized_topology_code TEXT GENERATED ALWAYS AS (coalesce(topology_code, '')) STORED,
          bbox JSONB, geometry JSONB, tile_count BIGINT NOT NULL DEFAULT 0 CHECK (tile_count >= 0),
          index_count BIGINT NOT NULL DEFAULT 0 CHECK (index_count >= 0), created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          FOREIGN KEY (dataset_id, output_version) REFERENCES partition_output_versions(dataset_id, output_version) ON DELETE CASCADE,
          UNIQUE (dataset_id, output_version, grid_type, grid_level, normalized_topology_code, space_code)
        )""",
        """CREATE TABLE IF NOT EXISTS partition_tiles (
          output_id TEXT PRIMARY KEY, dataset_id TEXT NOT NULL, output_version TEXT NOT NULL,
          source_asset_id TEXT NOT NULL, band_code TEXT NOT NULL, grid_type TEXT NOT NULL, grid_level INT NOT NULL,
          grid_level_name TEXT NOT NULL, space_code TEXT NOT NULL, topology_code TEXT, time_bucket TEXT NOT NULL,
          tile_uri TEXT NOT NULL CHECK (tile_uri LIKE 's3://%'), tile_kind TEXT NOT NULL CHECK (tile_kind IN ('logical_reference','entity_file')),
          bbox JSONB, width BIGINT CHECK (width > 0), height BIGINT CHECK (height > 0), byte_size BIGINT CHECK (byte_size >= 0),
          checksum CHAR(64) CHECK (checksum ~ '^[0-9a-f]{64}$'), status TEXT NOT NULL CHECK (status IN ('ready','failed')),
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          FOREIGN KEY (dataset_id, output_version) REFERENCES partition_output_versions(dataset_id, output_version) ON DELETE CASCADE,
          FOREIGN KEY (dataset_id, source_asset_id, band_code) REFERENCES partition_dataset_bands(dataset_id, source_asset_id, band_code),
          UNIQUE (dataset_id, output_version, source_asset_id, band_code, grid_type, grid_level, space_code, time_bucket, tile_kind)
        )""",
        """CREATE TABLE IF NOT EXISTS partition_indexes (
          output_id TEXT PRIMARY KEY, dataset_id TEXT NOT NULL, output_version TEXT NOT NULL, tile_output_id TEXT,
          source_asset_id TEXT NOT NULL, band_code TEXT NOT NULL, acquisition_time TIMESTAMPTZ, time_bucket TEXT NOT NULL,
          grid_type TEXT NOT NULL, grid_level INT NOT NULL, grid_level_name TEXT NOT NULL, topology_code TEXT,
          space_code TEXT NOT NULL, st_code TEXT NOT NULL, window_col_off BIGINT, window_row_off BIGINT,
          window_width BIGINT, window_height BIGINT, value_ref_uri TEXT NOT NULL CHECK (value_ref_uri LIKE 's3://%'),
          attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          FOREIGN KEY (dataset_id, output_version) REFERENCES partition_output_versions(dataset_id, output_version) ON DELETE CASCADE,
          FOREIGN KEY (tile_output_id) REFERENCES partition_tiles(output_id),
          FOREIGN KEY (dataset_id, source_asset_id, band_code) REFERENCES partition_dataset_bands(dataset_id, source_asset_id, band_code),
          CHECK ((window_col_off IS NULL AND window_row_off IS NULL AND window_width IS NULL AND window_height IS NULL) OR
                 (window_col_off >= 0 AND window_row_off >= 0 AND window_width > 0 AND window_height > 0)),
          UNIQUE (dataset_id, output_version, source_asset_id, band_code, grid_type, grid_level, space_code, time_bucket, st_code)
        )""",
        """ALTER TABLE partition_indexes ADD COLUMN IF NOT EXISTS attributes JSONB NOT NULL DEFAULT '{}'::jsonb""",
        """CREATE TABLE IF NOT EXISTS partition_quality_runs (
          quality_run_id UUID PRIMARY KEY, dataset_id TEXT NOT NULL, output_version TEXT NOT NULL,
          quality_sequence BIGINT NOT NULL CHECK (quality_sequence > 0), trigger TEXT NOT NULL CHECK (trigger IN ('automatic','manual')),
          trigger_event_id UUID NULL UNIQUE, requested_by TEXT NOT NULL, rule_set_version TEXT NOT NULL,
          rule_snapshot JSONB NOT NULL, status TEXT NOT NULL CHECK (status IN ('pending','running','pass','warn','fail','error','cancelled')),
          error_count BIGINT NOT NULL DEFAULT 0 CHECK (error_count >= 0), warning_count BIGINT NOT NULL DEFAULT 0 CHECK (warning_count >= 0),
          result_complete BOOLEAN NOT NULL DEFAULT FALSE, attempt_count INT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
          available_at TIMESTAMPTZ NOT NULL DEFAULT now(), claimed_at TIMESTAMPTZ, claimed_by TEXT, last_error TEXT,
          started_at TIMESTAMPTZ, completed_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE (dataset_id, output_version, quality_sequence), UNIQUE (dataset_id, output_version, quality_run_id),
          FOREIGN KEY (dataset_id, output_version) REFERENCES partition_output_versions(dataset_id, output_version),
          CHECK ((claimed_at IS NULL) = (claimed_by IS NULL))
        )""",
        """CREATE TABLE IF NOT EXISTS partition_quality_results (
          quality_run_id UUID NOT NULL REFERENCES partition_quality_runs(quality_run_id) ON DELETE CASCADE,
          dataset_id TEXT NOT NULL, output_version TEXT NOT NULL, rule_code TEXT NOT NULL,
          status TEXT NOT NULL CHECK (status IN ('pass','warn','fail','error')), finding_count BIGINT NOT NULL DEFAULT 0 CHECK (finding_count >= 0),
          error_count BIGINT NOT NULL DEFAULT 0 CHECK (error_count >= 0), warning_count BIGINT NOT NULL DEFAULT 0 CHECK (warning_count >= 0),
          metrics JSONB NOT NULL DEFAULT '{}'::jsonb, execution_error TEXT, started_at TIMESTAMPTZ NOT NULL, completed_at TIMESTAMPTZ NOT NULL,
          PRIMARY KEY (quality_run_id, rule_code),
          FOREIGN KEY (dataset_id, output_version, quality_run_id) REFERENCES partition_quality_runs(dataset_id, output_version, quality_run_id)
        )""",
        """CREATE TABLE IF NOT EXISTS partition_quality_errors (
          quality_error_id UUID PRIMARY KEY, quality_run_id UUID NOT NULL, dataset_id TEXT NOT NULL, output_version TEXT NOT NULL,
          rule_code TEXT NOT NULL, source_asset_id TEXT, tile_id TEXT, index_id TEXT, output_id TEXT,
          row_number BIGINT CHECK (row_number IS NULL OR row_number >= 0), field_name TEXT, error_code TEXT NOT NULL,
          message TEXT NOT NULL, context JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          FOREIGN KEY (dataset_id, output_version, quality_run_id) REFERENCES partition_quality_runs(dataset_id, output_version, quality_run_id)
        )""",
        """CREATE TABLE IF NOT EXISTS partition_quality_warn_approvals (
          approval_id UUID PRIMARY KEY, dataset_id TEXT NOT NULL, output_version TEXT NOT NULL, quality_run_id UUID NOT NULL UNIQUE,
          rule_set_version TEXT NOT NULL, approved_by TEXT NOT NULL, approved_at TIMESTAMPTZ NOT NULL,
          reason TEXT NOT NULL CHECK (length(btrim(reason)) BETWEEN 1 AND 2000),
          FOREIGN KEY (dataset_id, output_version, quality_run_id) REFERENCES partition_quality_runs(dataset_id, output_version, quality_run_id)
        )""",
        """CREATE TABLE IF NOT EXISTS partition_publications (
          publication_id UUID PRIMARY KEY, dataset_id TEXT NOT NULL, output_version TEXT NOT NULL, quality_run_id UUID NOT NULL,
          status TEXT NOT NULL CHECK (status IN ('publishing','active','withdrawing','failed','withdrawn')),
          desired_action TEXT NOT NULL CHECK (desired_action IN ('activate','withdraw')), service_version_id TEXT,
          requested_by TEXT NOT NULL, requested_at TIMESTAMPTZ NOT NULL, activated_at TIMESTAMPTZ, failure TEXT,
          withdrawn_by TEXT, withdrawn_at TIMESTAMPTZ, withdrawal_reason TEXT, attempt_count INT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
          available_at TIMESTAMPTZ NOT NULL DEFAULT now(), claimed_at TIMESTAMPTZ, claimed_by TEXT, last_error TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          FOREIGN KEY (dataset_id, output_version, quality_run_id) REFERENCES partition_quality_runs(dataset_id, output_version, quality_run_id),
          CHECK ((claimed_at IS NULL) = (claimed_by IS NULL)),
          CHECK ((status = 'publishing' AND desired_action = 'activate') OR
                 (status = 'active' AND desired_action = 'activate' AND service_version_id IS NOT NULL AND activated_at IS NOT NULL) OR
                 (status = 'withdrawing' AND desired_action = 'withdraw' AND service_version_id IS NOT NULL AND withdrawn_by IS NOT NULL AND withdrawal_reason IS NOT NULL AND length(btrim(withdrawal_reason)) BETWEEN 1 AND 2000) OR
                 (status = 'withdrawn' AND desired_action = 'withdraw' AND service_version_id IS NOT NULL AND withdrawn_by IS NOT NULL AND withdrawn_at IS NOT NULL AND withdrawal_reason IS NOT NULL AND length(btrim(withdrawal_reason)) BETWEEN 1 AND 2000) OR
                 (status = 'failed' AND failure IS NOT NULL))
        )""",
        """CREATE TABLE IF NOT EXISTS partition_domain_outbox (
          event_id UUID PRIMARY KEY, dataset_id TEXT NOT NULL, output_version TEXT NOT NULL,
          event_type TEXT NOT NULL CHECK (event_type = 'output-version.completed'), payload JSONB NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','processing','delivered')),
          attempt_count INT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0), available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          claimed_at TIMESTAMPTZ, claimed_by TEXT, last_error TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), delivered_at TIMESTAMPTZ,
          FOREIGN KEY (dataset_id, output_version) REFERENCES partition_output_versions(dataset_id, output_version),
          UNIQUE (dataset_id, output_version, event_type), CHECK ((claimed_at IS NULL) = (claimed_by IS NULL)),
          CHECK ((status <> 'delivered') OR delivered_at IS NOT NULL)
        )""",
        """CREATE TABLE IF NOT EXISTS partition_domain_schema_version (
          singleton BOOLEAN PRIMARY KEY CHECK (singleton), schema_version TEXT NOT NULL, installed_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )""",
        "CREATE INDEX IF NOT EXISTS idx_partition_quality_claim ON partition_quality_runs(status, available_at, claimed_at, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_partition_quality_errors_page ON partition_quality_errors(quality_run_id, created_at, quality_error_id)",
        "CREATE INDEX IF NOT EXISTS idx_partition_quality_errors_filter ON partition_quality_errors(quality_run_id, rule_code, error_code, source_asset_id, output_id, field_name)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_partition_publication_live_snapshot ON partition_publications(dataset_id, output_version, quality_run_id) WHERE status IN ('publishing','active','withdrawing')",
        "CREATE INDEX IF NOT EXISTS idx_partition_publication_claim ON partition_publications(status, available_at, claimed_at, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_partition_publication_dataset_latest ON partition_publications(dataset_id, requested_at DESC, publication_id DESC)",
        "CREATE INDEX IF NOT EXISTS idx_partition_domain_outbox_claim ON partition_domain_outbox(status, available_at, claimed_at, created_at)",
        """DO $$ BEGIN
          ALTER TABLE partition_datasets ADD CONSTRAINT partition_datasets_current_output_fkey
          FOREIGN KEY (dataset_id, current_output_version) REFERENCES partition_output_versions(dataset_id, output_version)
          DEFERRABLE INITIALLY DEFERRED;
        EXCEPTION WHEN duplicate_object THEN NULL; END $$""",
        """DO $$ BEGIN
          ALTER TABLE partition_datasets ADD CONSTRAINT partition_datasets_current_quality_run_fkey
          FOREIGN KEY (dataset_id, current_output_version, current_quality_run_id) REFERENCES partition_quality_runs(dataset_id, output_version, quality_run_id)
          DEFERRABLE INITIALLY DEFERRED;
        EXCEPTION WHEN duplicate_object THEN NULL; END $$""",
        """MERGE INTO partition_domain_schema_version target
           USING (SELECT TRUE AS singleton, '2026-07-16-m2-mixed-carbon-v1' AS schema_version) source
           ON (target.singleton = source.singleton)
           WHEN MATCHED THEN UPDATE SET schema_version = source.schema_version, installed_at = now()
           WHEN NOT MATCHED THEN INSERT (singleton, schema_version) VALUES (source.singleton, source.schema_version)""",
    )


def apply_schema(connection: Any) -> None:
    """Apply the forward DDL once."""
    with connection.cursor() as cursor:
        for statement in schema_statements():
            cursor.execute(statement)
    connection.commit()


def assert_schema_version(connection: Any) -> None:
    row = connection.execute("SELECT schema_version FROM partition_domain_schema_version WHERE singleton = TRUE").fetchone()
    actual = None if row is None else str(row[0])
    if actual != PARTITION_DOMAIN_SCHEMA_VERSION:
        raise RuntimeError(
            f"partition domain schema version {actual!r} does not match {PARTITION_DOMAIN_SCHEMA_VERSION!r}; "
            "run cube_web/scripts/reset_partition_domain.py with the required development guards"
        )


def inventory_partition_objects(connection: Any) -> PartitionObjectInventory:
    """Read public partition/quality objects with parameterized catalog queries."""
    object_rows = _fetchall(connection, """
        SELECT c.relkind, n.nspname, t.relname, c.relname
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_depend d ON d.objid = c.oid AND d.deptype IN ('a', 'i')
        LEFT JOIN pg_class t ON t.oid = d.refobjid
        WHERE n.nspname = current_schema() AND (c.relname LIKE 'partition_%' OR c.relname LIKE 'quality_%')
    """)
    objects = {(str(row[0]), str(row[1]), None if row[2] is None else str(row[2]), str(row[3])) for row in object_rows}
    index_rows = _fetchall(connection, """
        SELECT i.relname, pg_get_indexdef(i.oid), pg_get_expr(ix.indpred, ix.indrelid)
        FROM pg_index ix JOIN pg_class i ON i.oid = ix.indexrelid JOIN pg_class t ON t.oid = ix.indrelid
        WHERE t.relname LIKE 'partition_%' OR t.relname LIKE 'quality_%'
    """)
    indexes = {
        (str(row[0]), (str(row[1]),), None if row[2] is None else str(row[2]).lower())
        for row in index_rows
    }
    constraint_rows = _fetchall(connection, """
        SELECT t.relname, c.conname, pg_get_constraintdef(c.oid)
        FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid
        WHERE t.relname LIKE 'partition_%' OR t.relname LIKE 'quality_%'
    """)
    constraints = tuple(CatalogConstraint(str(row[0]), str(row[1]), str(row[2]).lower()) for row in constraint_rows)
    return PartitionObjectInventory(objects=objects, indexes=indexes, constraints=constraints)


def _fetchall(connection: Any, query: str) -> list[Any]:
    if hasattr(connection, "execute"):
        return list(connection.execute(query).fetchall())
    with connection.cursor() as cursor:
        cursor.execute(query)
        return list(cursor.fetchall())
