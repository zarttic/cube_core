from __future__ import annotations

import pytest

from cube_web.services.scene_domain_schema import (
    SCENE_DOMAIN_SCHEMA_VERSION,
    SCENE_DOMAIN_TABLES,
    apply_scene_domain_schema,
    schema_statements,
)


def test_schema_is_a_fresh_production_install() -> None:
    sql = "\n".join(schema_statements()).lower()

    assert SCENE_DOMAIN_SCHEMA_VERSION == "2026-07-19-scene-domain-v7"
    assert SCENE_DOMAIN_TABLES == {
        "datasets",
        "scenes",
        "scene_assets",
        "scene_bands",
        "load_batches",
        "load_batch_scenes",
        "partition_runs",
        "partition_drafts",
        "partition_run_scenes",
        "partition_data_unit_grid_status",
        "ingest_runs",
        "ingest_run_scenes",
        "scene_dataset_audit",
        "scene_domain_schema_version",
    }
    assert "migration_lineage" not in sql
    assert "legacy_partition_dataset_id" not in sql
    assert "entity','unknown" not in sql
    assert "partition_run_id text not null" in sql
    assert "output_version text not null" in sql
    assert "'backfill'" not in sql
    assert "resolution_native double precision" in sql
    assert "resolution_unit text" in sql
    assert "resolution_m double precision" in sql
    assert "suggested_grid_type text" in sql
    assert "suggested_grid_type in ('geohash','mgrs','isea4h')" in sql
    assert "band_unit_id text" in sql


def test_partition_data_unit_grid_status_enforces_band_grid_identity_and_lifecycle_states() -> None:
    table_sql = next(
        " ".join(statement.lower().split())
        for statement in schema_statements()
        if "create table if not exists partition_data_unit_grid_status" in statement.lower()
    )

    assert "dataset_id text not null references datasets(dataset_id)" in table_sql
    assert "scene_id text not null references scenes(scene_id)" in table_sql
    assert "primary key (band_unit_id, grid_type)" in table_sql
    assert "grid_type text not null check (grid_type in ('geohash','mgrs','isea4h'))" in table_sql
    assert "grid_level int not null check (grid_level >= 0)" in table_sql
    assert "attempt_no int not null default 0 check (attempt_no >= 0)" in table_sql
    assert "partition_status text not null default 'pending' check" in table_sql
    assert "quality_status text not null default 'pending' check" in table_sql
    assert "ingest_status text not null default 'pending' check" in table_sql


class _Cursor:
    def __init__(self, fail_query: str | None = None) -> None:
        self.executed: list[tuple[str, object]] = []
        self.last_sql = ""
        self.fail_query = fail_query

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, sql, params=None):
        self.last_sql = " ".join(str(sql).split())
        self.executed.append((self.last_sql, params))
        if self.fail_query and self.fail_query in self.last_sql:
            raise RuntimeError("install failed")

    def fetchone(self):
        return (0,)


class _Connection:
    def __init__(self, fail_query: str | None = None) -> None:
        self.cursor_value = _Cursor(fail_query)
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cursor_value

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_apply_schema_commits_after_validation_and_is_repeatable() -> None:
    connection = _Connection()

    first = apply_scene_domain_schema(connection)
    second = apply_scene_domain_schema(connection)

    assert first == second
    assert first.schema_version == SCENE_DOMAIN_SCHEMA_VERSION
    assert connection.commits == 2
    assert connection.rollbacks == 0
    version_writes = [sql for sql, _ in connection.cursor_value.executed if "MERGE INTO scene_domain_schema_version" in sql]
    assert len(version_writes) == 2


def test_apply_schema_rolls_back_before_version_marker() -> None:
    connection = _Connection("CREATE TABLE IF NOT EXISTS scenes")

    with pytest.raises(RuntimeError, match="install failed"):
        apply_scene_domain_schema(connection)

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert not any("MERGE INTO scene_domain_schema_version" in sql for sql, _ in connection.cursor_value.executed)
