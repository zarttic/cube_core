from __future__ import annotations

import re
from typing import Any

import pytest

from cube_web.services.m6_scene_schema import (
    LEGACY_SOURCE_TABLES,
    M6_SCENE_SCHEMA_VERSION,
    M6_TABLES,
    apply_m6_scene_schema,
    backfill_statements,
    schema_statements,
)


def test_m6_schema_models_scene_level_relationships_and_idempotency() -> None:
    sql = "\n".join(schema_statements()).lower()
    assert M6_SCENE_SCHEMA_VERSION == "2026-07-16-m6-scene-domain-v1"
    for table in M6_TABLES:
        assert f"create table if not exists {table}" in sql
    assert "unique (dataset_id, scene_key)" in sql
    assert "identity_key text not null unique" in sql
    assert "primary key (load_batch_id, scene_id)" in sql
    assert "primary key (partition_run_id, scene_id)" in sql
    assert "primary key (ingest_run_id, scene_id)" in sql
    assert "unique (partition_run_id, idempotency_key)" in sql
    assert "create unique index if not exists uq_ingest_run_scenes_idempotency on ingest_run_scenes(idempotency_key)" in sql
    assert "source_load_batch_ids jsonb" in sql
    assert "scene_dataset_audit" in sql


def test_m6_migration_is_strictly_additive() -> None:
    sql = "\n".join((*schema_statements(), *backfill_statements(LEGACY_SOURCE_TABLES)))
    assert not re.search(r"\b(?:DROP|TRUNCATE)\b", sql, re.IGNORECASE)
    assert not re.search(r"\bDELETE\s+FROM\b", sql, re.IGNORECASE)
    assert "ALTER TABLE partition_" not in sql.upper()
    assert "MERGE INTO datasets" in sql
    assert "MERGE INTO scenes" in sql
    assert "MERGE INTO load_batch_scenes" in sql
    assert "jsonb_build_object" not in sql.lower()
    assert "params_json::text" in sql.lower()
    assert "provenance_json::json" in sql.lower()


def test_backfill_covers_partition_and_ard_sources_and_splits_run_ids() -> None:
    sql = "\n".join(backfill_statements(LEGACY_SOURCE_TABLES)).lower()
    for table in (
        "partition_batches",
        "partition_assets",
        "partition_datasets",
        "partition_dataset_assets",
        "partition_output_versions",
        "ard_partition_batches",
        "ard_partition_assets",
        "ard_partition_observations",
    ):
        assert table in sql
    assert "partition-run-legacy-" in sql
    assert "ingest-run-legacy-" in sql
    assert "|| ':' || s.dataset_id" in sql
    assert "migration_inferred" in sql
    assert "auto_ingest_allowed',false" in sql
    assert "legacy-load-" not in sql
    for table in ("rs_ingest_job", "rs_raw_scene_asset", "rs_cube_cell_fact", "rs_entity_tile_asset"):
        assert table in sql
    assert "merge into migration_lineage" in sql
    assert "when status='succeeded' then 'completed'" in sql
    assert "'auto_ingest_allowed',false" in sql
    assert "'legacy-dataset-' || md5" in sql
    assert "'legacy-load-'" not in sql
    assert "provider_namespace || ':'" in sql
    assert "source_system,'loader'" in sql
    assert "md5(('rs' || ':' ||" in sql
    assert "join scenes s on s.dataset_id = o.dataset_id" not in sql
    assert "join partition_job_attempts j on j.task_id=o.task_id" in sql
    assert "join scene_assets sa on sa.asset_id=any(j.asset_ids)" in sql


def test_rs_backfill_has_complete_fact_lineage_and_no_batch_status_substitution() -> None:
    sql = "\n".join(backfill_statements(LEGACY_SOURCE_TABLES)).lower()
    expected = {
        "rs_ingest_job": "'ingest_run' as entity_type",
        "rs_raw_scene_asset": "'asset' as entity_type",
        "rs_cube_cell_fact": "'cube_cell' as entity_type",
        "rs_entity_tile_asset": "'entity_tile' as entity_type",
    }
    for table, marker in expected.items():
        assert f"'{table}' as source_table" in sql
        assert marker in sql
    rs_ingest = next(item.lower() for item in backfill_statements(LEGACY_SOURCE_TABLES) if "merge into ingest_runs" in item.lower() and "rs_ingest_job" in item.lower())
    assert "from rs_ingest_job" in rs_ingest
    assert "partition_batches" not in rs_ingest
    assert "coalesce(nullif(params_json->>'dataset',''),'__unassigned__')" in rs_ingest
    assert "where coalesce(params_json->>'dataset','')<>''" not in sql
    assert "like '%optical%' then 'optical'" in sql
    assert "else 'unknown' end as data_type" in sql
    assert "group by i.ingest_run_id,r.dataset,r.scene_id" in sql
    assert "on (target.idempotency_key=source.idempotency_key)" in sql
    assert "when matched then update set ingest_run_id=source.ingest_run_id,scene_id=source.scene_id" in sql
    assert "md5(i.ingest_run_id || ':' || r.scene_id)" not in sql
    assert "md5(coalesce(nullif(b.ingest_job_id,''),b.batch_id) || ':' || s.scene_id)" not in sql
    assert "partition by grouped.ingest_run_id,grouped.scene_id" in sql
    assert "where versioned.current_rank=1" in sql
    assert "group by i.ingest_run_id,r.dataset,r.scene_id,r.version" in sql
    assert "from partition_dataset_assets a group by" in sql
    assert "a.source_asset_id" in sql
    for table in ("rs_product_asset", "rs_product_cell_fact", "rs_carbon_observation_fact"):
        assert f"'{table}' as source_table" in sql
    assert "'tile_version',e.tile_version" in sql
    assert "'grid_type',c.grid_type" in sql


def test_managed_ingest_rs_rows_reuse_m6_identity_when_migration_is_rerun() -> None:
    tables = {
        "rs_ingest_job",
        "rs_raw_scene_asset",
        "rs_cube_cell_fact",
        "rs_entity_tile_asset",
        "rs_product_asset",
        "rs_product_cell_fact",
    }
    first = backfill_statements(tables)
    assert first == backfill_statements(tables)

    sql = "\n".join(first).lower()
    assert "select d.dataset_id from datasets d where d.dataset_id=dataset_name" in sql
    assert "select d.dataset_id from datasets d where d.dataset_id=coalesce(nullif(params_json->>'dataset',''),'__unassigned__')" in sql

    raw_scene = next(item.lower() for item in first if "merge into scenes" in item.lower() and "from rs_raw_scene_asset" in item.lower())
    assert "select s.scene_id from scenes s where s.scene_id=r.scene_id" in raw_scene
    assert "select s.identity_key from scenes s where s.scene_id=r.scene_id" in raw_scene
    assert "select s.dataset_id from scenes s where s.scene_id=r.scene_id" in raw_scene

    ingest_scenes = next(item.lower() for item in first if "merge into ingest_run_scenes" in item.lower())
    assert "select existing.idempotency_key from ingest_run_scenes existing" in ingest_scenes
    assert "existing.ingest_run_id=i.ingest_run_id and existing.scene_id=" in ingest_scenes

    cube_lineage = next(item.lower() for item in first if "'rs_cube_cell_fact' as source_table" in item.lower())
    assert "select s.scene_id from scenes s where s.scene_id=(c.provenance_json->>'winner_scene_id')" in cube_lineage
    assert "s.scene_id=coalesce(" in cube_lineage

    for source_table, alias in (("rs_entity_tile_asset", "e"), ("rs_product_asset", "p")):
        lineage = next(item.lower() for item in first if f"'{source_table}' as source_table" in item.lower())
        assert f"select s.scene_id from scenes s where s.scene_id={alias}.scene_id" in lineage
        assert f"select s.dataset_id from scenes s where s.scene_id={alias}.scene_id" in lineage
    product_fact = next(item.lower() for item in first if "'rs_product_cell_fact' as source_table" in item.lower())
    assert "select d.dataset_id from datasets d where d.dataset_id=p.dataset" in product_fact


def test_backfill_only_references_available_legacy_families() -> None:
    sql = "\n".join(backfill_statements({"ard_partition_batches", "ard_partition_observations"})).lower()
    assert "ard_partition_batches" in sql
    assert "ard_partition_observations" in sql
    assert "from partition_batches" not in sql
    assert "from partition_datasets" not in sql
    assert "from partition_assets" not in sql


class FakeCursor:
    def __init__(self, connection: "FakeConnection") -> None:
        self.connection = connection
        self._rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        self.connection.executed.append((sql, params))
        normalized = " ".join(sql.lower().split())
        if "from information_schema.tables" in normalized:
            self._rows = [(name,) for name in sorted(self.connection.legacy_tables)]
            return
        if self.connection.fail_preservation and "partition_datasets p where not exists" in normalized:
            self._rows = [(1,)]
            return
        counts = {
            "select count(*) from datasets": 3,
            "select count(*) from scenes": 8,
            "select count(*) from scene_assets": 12,
            "select count(*) from load_batches": 2,
            "select count(*) from load_batch_scenes": 8,
            "select count(*) from partition_runs": 1,
            "select count(*) from ingest_runs": 3,
            "select count(*) from migration_lineage": 17073,
        }
        self._rows = [(counts.get(normalized, 0),)] if normalized.startswith("select count(*)") else []

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None


class FakeConnection:
    def __init__(self, *, fail_preservation: bool = False) -> None:
        self.legacy_tables = set(LEGACY_SOURCE_TABLES)
        self.fail_preservation = fail_preservation
        self.executed: list[tuple[str, Any]] = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def test_apply_m6_schema_commits_only_after_validation_and_is_repeatable() -> None:
    connection = FakeConnection()
    first = apply_m6_scene_schema(connection)
    second = apply_m6_scene_schema(connection)
    assert first == second
    assert first.scenes == 8
    assert first.scene_assets == 12
    assert first.ingest_runs == 3
    assert first.migration_lineage == 17073
    assert connection.commits == 2
    assert connection.rollbacks == 0
    version_writes = [sql for sql, _ in connection.executed if "MERGE INTO m6_scene_schema_version" in sql]
    assert len(version_writes) == 2


def test_apply_m6_schema_fails_closed_before_version_marker() -> None:
    connection = FakeConnection(fail_preservation=True)
    with pytest.raises(RuntimeError, match="partition_datasets not preserved"):
        apply_m6_scene_schema(connection)
    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert not any("MERGE INTO m6_scene_schema_version" in sql for sql, _ in connection.executed)
