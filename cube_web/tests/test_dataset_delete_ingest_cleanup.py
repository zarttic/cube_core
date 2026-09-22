from __future__ import annotations

from typing import Any

import pytest
from cube_split.ingest.dataset_cleanup import make_ingest_job_id

from cube_web.services.dataset_management import (
    DatasetManagementConflict,
    DatasetManagementService,
    OpenGaussDatasetManagementRepository,
)

DATASET_ID = "dataset-a"
OUTPUT_VERSION = "ov-1"
BAND_UNIT_ID = "band-1"
PARTITION_TILE = f"s3://cube/partition/{DATASET_ID}/versions/{OUTPUT_VERSION}/tiles/1.tif"
RAW_ASSET_COPY = f"s3://cube/cube/raw/dataset={DATASET_ID}/sensor=s1/version=v1/a.tif"
SOURCE_OBJECT = "s3://cube/cube/source/optical/输入.tif"

_RS_TABLES = (
    "rs_raw_scene_asset",
    "rs_entity_tile_asset",
    "rs_product_asset",
    "rs_product_cell_fact",
    "rs_cube_cell_fact",
    "rs_carbon_observation_fact",
    "rs_ingest_job",
)


class _Cursor:
    def __init__(self, conn: "_Connection") -> None:
        self._conn = conn
        self._rows: list[dict[str, Any]] = []
        self.rowcount = 0

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, statement: str, params: tuple[Any, ...] = ()) -> None:
        normalized = " ".join(statement.split())
        self._conn.executed.append((normalized, params))
        self._rows = self._conn.results_for(normalized)
        self.rowcount = 1 if normalized.upper().startswith("DELETE") else 0

    def fetchall(self) -> list[dict[str, Any]]:
        return list(self._rows)

    def fetchone(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None


class _Connection:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.closed = False

    def cursor(self, **_kwargs: object) -> _Cursor:
        return _Cursor(self)

    def close(self) -> None:
        self.closed = True

    def results_for(self, statement: str) -> list[dict[str, Any]]:
        if "FROM datasets WHERE dataset_id=%s FOR UPDATE" in statement:
            return [{"dataset_id": DATASET_ID}]
        if "FROM scenes WHERE dataset_id=%s FOR UPDATE" in statement:
            return [{"scene_id": "scene-1"}]
        if "FROM tile_uri AS object_uri" in statement or "AS object_uri FROM partition_tiles" in statement:
            return [{"object_uri": PARTITION_TILE}]
        if "jsonb_array_elements_text(scene.band_unit_ids)" in statement:
            return [{"output_version": OUTPUT_VERSION, "band_unit_id": BAND_UNIT_ID}]
        if "FROM partition_output_versions WHERE dataset_id=%s" in statement:
            return [{"output_version": OUTPUT_VERSION}, {"output_version": "ov-2"}]
        if "FROM pg_class" in statement:
            return [{"relname": table} for table in _RS_TABLES]
        if "raw_cog_uri AS object_uri FROM rs_raw_scene_asset" in statement:
            return [{"object_uri": RAW_ASSET_COPY}, {"object_uri": SOURCE_OBJECT}]
        if statement.startswith("SELECT EXISTS"):
            return [{"active": False}]
        return []

    def deletes(self) -> list[tuple[str, tuple[Any, ...]]]:
        return [entry for entry in self.executed if entry[0].upper().startswith("DELETE")]


def _repository() -> tuple[OpenGaussDatasetManagementRepository, _Connection]:
    connection = _Connection()
    repository = OpenGaussDatasetManagementRepository(None, connection_factory=lambda: connection)
    return repository, connection


def test_delete_dataset_removes_every_rs_row_owned_by_the_dataset() -> None:
    repository, connection = _repository()

    result = repository.delete_dataset(DATASET_ID, actor="admin")

    deletes = connection.deletes()
    assert deletes, "the deletion must delete something"
    # Every delete is one bounded batch: the ctid subquery keeps a large dataset
    # from turning into one long transaction (2026-09-19 incident).
    for statement, _params in deletes:
        assert " WHERE ctid IN (SELECT ctid FROM " in statement
        assert " LIMIT 20000)" in statement

    for table in ("rs_raw_scene_asset", "rs_entity_tile_asset", "rs_product_asset", "rs_product_cell_fact"):
        statement = next(item for item, _ in deletes if f"DELETE FROM {table} WHERE ctid" in item)
        assert "WHERE dataset = %s" in statement
        assert dict(deletes)[statement] == (DATASET_ID,)

    expected_run_id = make_ingest_job_id(DATASET_ID, OUTPUT_VERSION, BAND_UNIT_ID)
    statements = dict(deletes)
    cube_statement = next(statement for statement in statements if "rs_cube_cell_fact" in statement)
    assert "WHERE run_id = ANY(%s::text[]) OR cube_version = ANY(%s::text[])" in cube_statement
    assert statements[cube_statement] == ([expected_run_id], [OUTPUT_VERSION, "ov-2"])
    assert statements[next(s for s in statements if "rs_carbon_observation_fact" in s)] == ([expected_run_id],)
    assert statements[next(s for s in statements if "rs_ingest_job" in s)] == ([expected_run_id],)

    assert any("DELETE FROM partition_assets WHERE ctid" in item and "scene_id = ANY(%s::text[])" in item for item, _ in deletes)
    assert result["deleted_ingest_rows"] == 7
    assert result["deleted_ingest_rows_by_table"]["rs_raw_scene_asset"] == 1
    # Ingest copies are handed to object cleanup; loader-owned sources never are.
    assert result["object_uris"] == sorted({PARTITION_TILE, RAW_ASSET_COPY})
    assert result["batch_size"] == 20000


def test_active_publication_blocks_dataset_deletion() -> None:
    connection = _Connection()
    original = connection.results_for

    def results_for(statement: str) -> list[dict[str, Any]]:
        if "FROM partition_publications" in statement:
            return [{"publication_id": "pub-1"}]
        return original(statement)

    connection.results_for = results_for  # type: ignore[method-assign]
    repository = OpenGaussDatasetManagementRepository(None, connection_factory=lambda: connection)

    with pytest.raises(DatasetManagementConflict, match="未撤回的发布"):
        repository.delete_dataset(DATASET_ID, actor="admin")

    assert connection.deletes() == [], "守卫拒绝后不得写入任何删除"


def test_delete_dataset_without_ingest_history_only_deletes_dataset_scoped_rows() -> None:
    connection = _Connection()
    original = connection.results_for

    def results_for(statement: str) -> list[dict[str, Any]]:
        if "jsonb_array_elements_text(scene.band_unit_ids)" in statement:
            return []
        if "FROM partition_output_versions WHERE dataset_id=%s" in statement:
            return []
        if "FROM pg_class" in statement:
            return [{"relname": "rs_cube_cell_fact"}]
        return original(statement)

    connection.results_for = results_for  # type: ignore[method-assign]
    repository = OpenGaussDatasetManagementRepository(None, connection_factory=lambda: connection)

    result = repository.delete_dataset(DATASET_ID, actor="admin")

    deleted_tables = [statement.split()[2] for statement, _ in connection.deletes()]
    assert "rs_cube_cell_fact" not in deleted_tables
    assert result["deleted_ingest_rows"] == 0
    assert result["object_uris"] == [PARTITION_TILE]


class _FakeRepository:
    def __init__(self, result: dict[str, Any]) -> None:
        self.result = result
        self.deleted: list[str] = []

    def get_dataset(self, dataset_id: str, *, viewer_role: str | None = None) -> dict[str, Any]:
        return {"dataset_id": dataset_id}

    def delete_dataset(self, dataset_id: str, *, actor: str) -> dict[str, Any]:
        self.deleted.append(dataset_id)
        return dict(self.result)


def test_service_hands_ingest_objects_to_the_object_store_cleanup() -> None:
    repository = _FakeRepository({"dataset_id": DATASET_ID, "object_uris": [RAW_ASSET_COPY]})
    cleaned: list[tuple[str, ...]] = []

    def cleanup(object_uris: tuple[str, ...]) -> dict[str, Any]:
        cleaned.append(tuple(object_uris))
        return {"status": "completed", "object_count": len(object_uris)}

    service = DatasetManagementService(repository, grid_object_cleanup=cleanup)  # type: ignore[arg-type]
    result = service.delete_dataset(DATASET_ID, actor="admin")

    assert cleaned == [(RAW_ASSET_COPY,)]
    assert result["object_cleanup"] == {"status": "completed", "object_count": 1}
    assert "object_uris" not in result
