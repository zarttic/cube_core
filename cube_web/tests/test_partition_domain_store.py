from __future__ import annotations

from types import SimpleNamespace

import pytest

from cube_web.services.partition_domain_schema import PARTITION_DOMAIN_SCHEMA_VERSION
from cube_web.services.partition_domain_store import (
    InMemoryPartitionDomainStore,
    OpenGaussPartitionDomainStore,
    PartitionDomainStore,
)


def _request() -> SimpleNamespace:
    asset = SimpleNamespace(
        source_asset_id="asset-a",
        cog_uri="s3://cube/source/a.tif",
        checksum="a" * 64,
        bbox=(0.0, 0.0, 1.0, 1.0),
        crs="EPSG:4326",
        time_start="2026-07-01T00:00:00Z",
        time_end="2026-07-01T01:00:00Z",
        attributes={},
    )
    band = SimpleNamespace(
        source_asset_id="asset-a",
        band_code="B04",
        band_name="red",
        band_type="spectral",
        unit=None,
        display_order=0,
        attributes={},
    )
    dataset = SimpleNamespace(
        dataset_id="dataset-a",
        dataset_code="dataset-a",
        dataset_title="Dataset A",
        data_type="optical",
        product_type="L2A",
        assets=(asset,),
        bands=(band,),
        attributes={},
    )
    return SimpleNamespace(
        batch_id="batch-a",
        grid_type="geohash",
        requested_grid_level=7,
        partition_method="logical",
        cover_mode="intersect",
        datasets=(dataset,),
    )


def _result(version: str) -> SimpleNamespace:
    row = {
        "output_id": "output-a",
        "source_asset_id": "asset-a",
        "band_code": "B04",
        "grid_level": 4,
        "space_code": "u4pr",
        "topology_code": "geohash-topo-v1:u4pr",
    }
    return SimpleNamespace(
        dataset_id="dataset-a",
        task_id="task-a",
        output_version=version,
        grid_type="geohash",
        requested_grid_level=7,
        partition_method="logical",
        object_prefix=f"partition/dataset-a/versions/{version}",
        tiles=(dict(row, tile_uri="s3://cube/tile.tif", tile_kind="logical_reference"),),
        indexes=(dict(row, st_code="u4pr"),),
        grid_cells=(dict(row),),
    )


def test_complete_output_is_idempotent_and_resets_quality() -> None:
    store = InMemoryPartitionDomainStore()
    request = _request()
    version = store.start_output(request, request.datasets[0], "task-a")
    store.seed_quality_state("dataset-a", current_run="run-a", sequence=8, errors=4, warnings=2)
    first = store.complete_output(_result(version))
    second = store.complete_output(_result(version))
    dataset = store.get_dataset("dataset-a")
    assert first["status"] == second["status"] == "completed"
    assert dataset["current_output_version"] == version
    assert dataset["quality_status"] == "pending"
    assert dataset["current_quality_run_id"] is None
    assert dataset["quality_sequence"] == 8
    assert dataset["quality_error_count"] == 0
    assert len(store.claim_outbox("worker", limit=10)) == 1


def test_detail_failure_rolls_back_pointer_and_outbox() -> None:
    store = InMemoryPartitionDomainStore()
    request = _request()
    old_version = store.start_output(request, request.datasets[0], "old-task")
    old = _result(old_version)
    old.task_id = "old-task"
    store.complete_output(old)
    new_version = store.start_output(request, request.datasets[0], "task-a")
    store.fail_on_output_id = "output-a"
    with pytest.raises(RuntimeError, match="injected detail failure"):
        store.complete_output(_result(new_version))
    assert store.resolve_output_version("dataset-a") == old_version
    assert all(row["output_version"] != new_version for row in store.outbox_rows())


def test_outbox_claim_ack_retry_lifecycle() -> None:
    store = InMemoryPartitionDomainStore()
    request = _request()
    version = store.start_output(request, request.datasets[0], "task-a")
    store.complete_output(_result(version))
    event = store.claim_outbox("worker", limit=1)[0]
    store.retry_outbox(event["event_id"], "temporary", available_at="2026-07-15T00:00:00Z")
    event = store.claim_outbox("worker", limit=1)[0]
    store.acknowledge_outbox(event["event_id"])
    assert store.outbox_rows()[0]["status"] == "delivered"


def test_protocol_methods_and_schema_fail_closed() -> None:
    names = (
        "get_dataset",
        "list_datasets",
        "count_datasets",
        "get_output_version",
        "list_assets",
        "count_assets",
        "list_bands",
        "count_bands",
        "list_tiles",
        "count_tiles",
        "list_indexes",
        "count_indexes",
        "list_grid_cells",
        "count_grid_cells",
        "list_publications",
        "count_publications",
        "output_has_publication_reference",
        "get_output_cleanup_state",
    )
    for name in names:
        assert hasattr(PartitionDomainStore, name)
        assert hasattr(InMemoryPartitionDomainStore, name)
        assert hasattr(OpenGaussPartitionDomainStore, name)
    store = InMemoryPartitionDomainStore()
    store.schema_version = "old"
    with pytest.raises(RuntimeError, match="schema version"):
        store.ensure_schema()


class _RecordingCursor:
    description = [("schema_version",)]

    def __init__(self, connection: "_RecordingConnection") -> None:
        self.connection = connection
        self.rows = [(PARTITION_DOMAIN_SCHEMA_VERSION,)]

    def execute(self, sql: str, params: tuple[object, ...] = ()) -> None:
        self.connection.statements.append((sql, params))

    def fetchall(self) -> list[tuple[str]]:
        return self.rows


class _RecordingConnection:
    def __init__(self) -> None:
        self.statements: list[tuple[str, tuple[object, ...]]] = []

    def __enter__(self) -> "_RecordingConnection":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def cursor(self) -> _RecordingCursor:
        return _RecordingCursor(self)

    def commit(self) -> None:
        self.statements.append(("COMMIT", ()))


def test_opengauss_mutation_uses_schema_guard_and_transaction_order() -> None:
    connection = _RecordingConnection()
    store = OpenGaussPartitionDomainStore(connection_factory=lambda: connection)
    request = _request()
    version = store.start_output(request, request.datasets[0], "task-a")
    statements = [sql for sql, _params in connection.statements]
    assert "SELECT schema_version FROM partition_domain_schema_version WHERE singleton = TRUE" in statements[0]
    assert any("MERGE INTO partition_output_versions" in sql for sql in statements)
    assert version


def test_opengauss_complete_accepts_strict_attempt_for_reused_dataset(monkeypatch) -> None:
    connection = _RecordingConnection()
    store = OpenGaussPartitionDomainStore(connection_factory=lambda: connection)
    monkeypatch.setattr(store, "_assert_live_schema", lambda _connection: None)
    monkeypatch.setattr(store, "_execute", lambda *_args, **_kwargs: None)

    def fetchall(_connection, sql, _params=()):
        if "FROM partition_datasets" in sql:
            return [{"dataset_id": "dataset-a", "batch_id": "first-run"}]
        if "FROM partition_job_attempts" in sql:
            return [
                {
                    "task_id": "second-task",
                    "batch_id": "second-run",
                    "payload": {
                        "strict_partition_request": True,
                        "datasets": [{"dataset_id": "dataset-a"}],
                    },
                }
            ]
        return []

    monkeypatch.setattr(store, "_fetchall", fetchall)
    result = _result("version-2")
    result.task_id = "second-task"

    with pytest.raises(ValueError, match="output version has not been started"):
        store.complete_output(result)


def test_opengauss_complete_persists_index_attributes_as_jsonb(monkeypatch) -> None:
    connection = _RecordingConnection()
    store = OpenGaussPartitionDomainStore(connection_factory=lambda: connection)
    monkeypatch.setattr(store, "_assert_live_schema", lambda _connection: None)
    inserted: list[dict[str, object]] = []

    def fetchall(_connection, sql, _params=()):
        if "FROM partition_datasets" in sql:
            return [{"dataset_id": "dataset-a", "batch_id": "batch-a"}]
        if "FROM partition_job_attempts" in sql:
            return [{"task_id": "task-a", "batch_id": "batch-a", "payload": {}}]
        if "FROM partition_output_versions" in sql:
            return [{"task_id": "task-a", "status": "staging"}]
        return []

    def merge_insert(_connection, **kwargs):
        inserted.append(kwargs)

    monkeypatch.setattr(store, "_fetchall", fetchall)
    monkeypatch.setattr(store, "_merge_insert", merge_insert)
    result = _result("version-a")
    attributes = {"satellite": "OCO2", "observation_id": "obs-1", "xco2": 410.25}
    result.indexes[0]["attributes"] = attributes

    store.complete_output(result)

    index_insert = next(item for item in inserted if item["table"] == "partition_indexes")
    index_values = dict(zip(index_insert["columns"], index_insert["values"]))
    assert index_values["attributes"] == '{"satellite": "OCO2", "observation_id": "obs-1", "xco2": 410.25}'


def test_opengauss_reads_are_parameterized_and_validate_bounds_before_sql() -> None:
    connection = _RecordingConnection()
    store = OpenGaussPartitionDomainStore(connection_factory=lambda: connection)
    store.seed_dataset("dataset-a")
    store.list_datasets(
        keyword="dataset",
        data_type=None,
        product_type=None,
        batch_id=None,
        grid_type=None,
        partition_status=None,
        quality_status=None,
        publish_status="unpublished",
        time_start=None,
        time_end=None,
        limit=2,
        offset=0,
        sort_by="updated_at",
        sort_order="asc",
    )
    assert any("ILIKE %s" in sql and "LIMIT %s OFFSET %s" in sql for sql, _params in connection.statements)
    before = len(connection.statements)
    with pytest.raises(ValueError):
        store.list_tiles("dataset-a", limit=0, offset=0, sort_by="created_at", sort_order="asc")
    assert len(connection.statements) == before


class _EmptyReadCursor(_RecordingCursor):
    def fetchall(self) -> list[tuple[str]]:
        if self.connection.statements[-1][0].startswith("SELECT schema_version"):
            return super().fetchall()
        self.description = []
        return []


class _EmptyReadConnection(_RecordingConnection):
    def cursor(self) -> _EmptyReadCursor:
        return _EmptyReadCursor(self)


def test_opengauss_empty_reads_do_not_fall_back_to_inherited_memory() -> None:
    connection = _EmptyReadConnection()
    store = OpenGaussPartitionDomainStore(connection_factory=lambda: connection)
    store.seed_dataset("dataset-a")
    assert store.get_dataset("dataset-a") is None
    assert (
        store.list_datasets(
            keyword=None,
            data_type=None,
            product_type=None,
            batch_id=None,
            grid_type=None,
            partition_status=None,
            quality_status=None,
            publish_status=None,
            time_start=None,
            time_end=None,
            limit=10,
            offset=0,
            sort_by="updated_at",
            sort_order="asc",
        )
        == []
    )


class _CleanupCursor(_RecordingCursor):
    def fetchall(self) -> list[tuple[object, ...]]:
        sql = self.connection.statements[-1][0]
        if sql.startswith("SELECT schema_version"):
            self.description = [("schema_version",)]
            return [(PARTITION_DOMAIN_SCHEMA_VERSION,)]
        if "FROM partition_output_versions" in sql:
            self.description = [
                ("dataset_id",),
                ("output_version",),
                ("status",),
                ("object_prefix",),
                ("completed_at",),
                ("failed_at",),
                ("is_current",),
            ]
            return [("dataset-a", "version-a", "completed", "partition/dataset-a/versions/version-a/", None, None, True)]
        self.description = [("tile_uri",), ("checksum",), ("byte_size",)]
        return [("s3://cube/partition/dataset-a/versions/version-a/tile.tif", "a" * 64, 12)]


class _CleanupConnection(_RecordingConnection):
    def cursor(self) -> _CleanupCursor:
        return _CleanupCursor(self)


def test_cleanup_state_contains_tile_manifest_shape() -> None:
    connection = _CleanupConnection()
    store = OpenGaussPartitionDomainStore(connection_factory=lambda: connection)
    state = store.get_output_cleanup_state("dataset-a", "version-a")
    assert state["manifest"] == [
        {
            "object_key": "partition/dataset-a/versions/version-a/tile.tif",
            "tile_uri": "s3://cube/partition/dataset-a/versions/version-a/tile.tif",
            "checksum": "a" * 64,
            "byte_size": 12,
        }
    ]
