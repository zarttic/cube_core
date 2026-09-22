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


def test_chunk_descriptors_are_registered_without_materializing_rows() -> None:
    store = InMemoryPartitionDomainStore()
    request = _request()
    version = store.start_output(request, request.datasets[0], "task-a")
    result = _result(version)
    result.chunks = ({
        "chunk_id": "chunk-a", "object_uri": f"s3://cube/partition/dataset-a/versions/{version}/logical-chunks/chunk-a.jsonl.gz",
        "checksum": "a" * 64, "byte_size": 123, "grid_cell_count": 1, "tile_count": 1, "index_count": 1,
    },)

    store.record_output_chunks(result)

    assert store.chunks[("dataset-a", version, "chunk-a")]["byte_size"] == 123


def test_selection_execution_id_uses_parent_attempt_for_output_ownership() -> None:
    store = InMemoryPartitionDomainStore()
    request = _request()
    execution_task_id = "task-a:load-a:dataset-a"
    version = store.start_output(request, request.datasets[0], execution_task_id)
    result = _result(version)
    result.task_id = execution_task_id

    committed = store.complete_output(result)

    assert committed["status"] == "completed"
    assert store.outputs[("dataset-a", version)]["task_id"] == "task-a"


def test_repartition_rebinds_dataset_to_latest_runtime_batch() -> None:
    store = InMemoryPartitionDomainStore()
    first_request = _request()
    first_version = store.start_output(first_request, first_request.datasets[0], "first-task")
    first_result = _result(first_version)
    first_result.task_id = "first-task"
    store.complete_output(first_result)

    second_request = _request()
    second_request.batch_id = "batch-b"
    second_request.grid_type = "isea4h"
    second_request.requested_grid_level = 6
    store.start_output(second_request, second_request.datasets[0], "second-task")

    dataset = store.get_dataset("dataset-a")
    assert dataset["batch_id"] == "batch-b"
    assert dataset["grid_type"] == "isea4h"
    assert dataset["requested_grid_level"] == 6
    assert dataset["partition_status"] == "running"
    assert dataset["partition_completed_at"] is None


def test_different_grid_outputs_remain_independently_available() -> None:
    store = InMemoryPartitionDomainStore()
    first_request = _request()
    first_version = store.start_output(first_request, first_request.datasets[0], "geohash-task")
    first_result = _result(first_version)
    first_result.task_id = "geohash-task"
    store.complete_output(first_result)

    second_request = _request()
    second_request.batch_id = "batch-mgrs"
    second_request.grid_type = "mgrs"
    second_request.requested_grid_level = 1
    second_version = store.start_output(second_request, second_request.datasets[0], "mgrs-task")
    second_result = _result(second_version)
    second_result.task_id = "mgrs-task"
    second_result.grid_type = "mgrs"
    second_result.requested_grid_level = 1
    for rows in (second_result.tiles, second_result.indexes, second_result.grid_cells):
        rows[0]["output_id"] = "output-mgrs"
    store.complete_output(second_result)

    assert store.outputs[("dataset-a", first_version)]["status"] == "completed"
    assert store.outputs[("dataset-a", second_version)]["status"] == "completed"


def test_stale_failed_output_does_not_regress_newer_staging_output() -> None:
    store = InMemoryPartitionDomainStore()
    request = _request()
    first_version = store.start_output(request, request.datasets[0], "first-task")
    first_result = _result(first_version)
    first_result.task_id = "first-task"
    store.complete_output(first_result)

    stale_version = store.start_output(request, request.datasets[0], "stale-task")
    newer_version = store.start_output(request, request.datasets[0], "newer-task")

    store.fail_output("dataset-a", stale_version, error_code="partition_execution_failed", error_message="stale failure")

    assert store.outputs[("dataset-a", stale_version)]["status"] == "failed"
    assert store.outputs[("dataset-a", newer_version)]["status"] == "staging"
    assert store.get_dataset("dataset-a")["partition_status"] == "running"

    store.fail_output("dataset-a", newer_version, error_code="partition_execution_failed", error_message="latest failure")
    assert store.get_dataset("dataset-a")["partition_status"] == "failed"


def test_stale_failed_output_does_not_regress_newer_completed_output() -> None:
    store = InMemoryPartitionDomainStore()
    request = _request()
    old_version = store.start_output(request, request.datasets[0], "old-task")
    old_result = _result(old_version)
    old_result.task_id = "old-task"
    store.complete_output(old_result)

    stale_version = store.start_output(request, request.datasets[0], "stale-task")
    newer_version = store.start_output(request, request.datasets[0], "newer-task")
    newer_result = _result(newer_version)
    newer_result.task_id = "newer-task"
    for noun in ("tiles", "indexes", "grid_cells"):
        getattr(newer_result, noun)[0]["output_id"] = f"{noun}-newer"
    store.complete_output(newer_result)

    store.fail_output("dataset-a", stale_version, error_code="partition_execution_failed", error_message="stale failure")

    assert store.outputs[("dataset-a", newer_version)]["status"] == "completed"
    assert store.get_dataset("dataset-a")["current_output_version"] == newer_version
    assert store.get_dataset("dataset-a")["partition_status"] == "completed"


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
    rebind = next(
        (sql, params)
        for sql, params in connection.statements
        if "UPDATE partition_datasets SET" in sql and "batch_id = %s" in sql
    )
    assert rebind[1] == (
        "batch-a", "geohash", 7, "7", "logical", "intersect",
        "dataset-a", "dataset-a", version,
    )
    output_merge_index = next(
        index for index, (sql, _params) in enumerate(connection.statements)
        if "MERGE INTO partition_output_versions" in sql
    )
    rebind_index = connection.statements.index(rebind)
    assert output_merge_index < rebind_index
    assert "status = 'staging'" in rebind[0]
    assert "partition_completed_at = NULL" in rebind[0]
    assert version


def test_opengauss_fail_output_protects_newer_staging_output() -> None:
    connection = _RecordingConnection()
    store = OpenGaussPartitionDomainStore(connection_factory=lambda: connection)

    store.fail_output("dataset-a", "version-a", error_code="partition_execution_failed", error_message="failed")

    updates = [(sql, params) for sql, params in connection.statements if sql.lstrip().startswith("UPDATE partition_")]
    assert len(updates) == 2
    dataset_update_sql, dataset_update_params = updates[1]
    assert "NOT EXISTS" in dataset_update_sql
    assert dataset_update_params == (
        "dataset-a", "version-a",
        "dataset-a", "version-a",
        "dataset-a", "version-a",
        "dataset-a", "version-a",
    )


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
            return [{
                "task_id": "task-a",
                "batch_id": "batch-a",
                "payload": {"strict_partition_request": True, "datasets": [{"dataset_id": "dataset-a"}]},
            }]
        if "FROM partition_output_versions" in sql:
            return [{"task_id": "task-a", "status": "staging"}]
        return []

    def merge_insert(_connection, **kwargs):
        inserted.append(kwargs)

    def copy_insert_many(_connection, **kwargs):
        for values in kwargs["rows"]:
            inserted.append({**kwargs, "values": values})

    monkeypatch.setattr(store, "_fetchall", fetchall)
    monkeypatch.setattr(store, "_merge_insert", merge_insert)
    monkeypatch.setattr(store, "_copy_insert_many", copy_insert_many)
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


def test_tile_detail_query_hides_internal_publication_status() -> None:
    store = OpenGaussPartitionDomainStore(connection_factory=lambda: _RecordingConnection())

    sql, _params = store._detail_query(
        "tiles", "dataset-a", "version-a", limit=1, offset=0, sort_by="created_at", sort_order="asc"
    )

    assert "SELECT *" not in sql
    assert "publication_status" not in sql
    assert "tile_uri" in sql


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


def _chunk_result(version: str, *, inserted: dict[str, int] | None, manifest: dict[str, int] | None = None) -> SimpleNamespace:
    """A logical result carrying Ray chunk descriptors."""
    result = _result(version)
    counts = manifest or {"grid_cells": 1, "tiles": 1, "indexes": 1}
    descriptor = {
        "chunk_id": "chunk-a",
        "object_uri": "s3://cube/partition/dataset-a/versions/%s/logical-chunks/chunk-a.jsonl.gz" % version,
        "checksum": "a" * 64,
        "byte_size": 100,
        "grid_cell_count": counts["grid_cells"],
        "tile_count": counts["tiles"],
        "index_count": counts["indexes"],
    }
    if inserted is not None:
        descriptor["inserted_counts"] = dict(inserted)
    result.chunks = (descriptor,)
    return result


def test_direct_write_counts_detect_legacy_and_direct_chunks() -> None:
    from cube_web.services.partition_domain_store import _direct_write_counts

    assert _direct_write_counts(()) is None
    legacy = _chunk_result("version-a", inserted=None).chunks
    assert _direct_write_counts(legacy) is None
    direct = _chunk_result("version-a", inserted={"grid_cells": 3, "tiles": 4, "indexes": 5}).chunks
    assert _direct_write_counts(direct) == {"grid_cells": 3, "tiles": 4, "indexes": 5}


def test_direct_write_counts_reject_empty_or_partial_mappings() -> None:
    """Only a full three-table mapping counts as a direct write."""
    from cube_web.services.partition_domain_store import _direct_write_counts

    assert _direct_write_counts(_chunk_result("version-a", inserted={}).chunks) is None
    partial = _chunk_result("version-a", inserted={"tiles": 1, "indexes": 1}).chunks
    assert _direct_write_counts(partial) is None


def test_promote_logical_staging_skips_direct_written_chunks(monkeypatch) -> None:
    connection = _RecordingConnection()
    store = OpenGaussPartitionDomainStore(connection_factory=lambda: connection)
    result = _chunk_result("version-a", inserted={"grid_cells": 1, "tiles": 1, "indexes": 1})

    store.promote_logical_staging(result)

    assert connection.statements == []


@pytest.mark.parametrize("inserted", [None, {}], ids=["legacy-missing-key", "staging-empty-map"])
def test_promote_logical_staging_still_merges_legacy_staging_rows(monkeypatch, inserted) -> None:
    connection = _RecordingConnection()
    store = OpenGaussPartitionDomainStore(connection_factory=lambda: connection)
    monkeypatch.setattr(store, "_assert_live_schema", lambda _connection: None)

    def fetchall(_connection, sql, _params=()):
        if "GROUP BY kind" in sql:
            return [
                {"kind": "grid_cells", "count": 1},
                {"kind": "tiles", "count": 1},
                {"kind": "indexes", "count": 1},
            ]
        return []

    monkeypatch.setattr(store, "_fetchall", fetchall)

    store.promote_logical_staging(_chunk_result("version-a", inserted=inserted))

    statements = [sql for sql, _params in connection.statements]
    assert any("INSERT INTO partition_grid_cells" in sql for sql in statements)
    assert any("INSERT INTO partition_tiles" in sql for sql in statements)
    assert any("INSERT INTO partition_indexes" in sql for sql in statements)


def test_result_rows_skip_minio_when_workers_wrote_directly(monkeypatch) -> None:
    store = OpenGaussPartitionDomainStore(connection_factory=lambda: _RecordingConnection())
    calls: list[str] = []
    monkeypatch.setattr(store, "_fetchall", lambda *_args, **_kwargs: calls.append("fetchall") or [])
    monkeypatch.setattr(
        store, "_iter_persisted_chunk_rows",
        lambda *_args, **_kwargs: calls.append("minio") or iter(()),
    )

    rows = list(store._result_rows(_RecordingConnection(), _chunk_result("version-a", inserted={"grid_cells": 1, "tiles": 1, "indexes": 1}), "tiles"))

    assert rows == []
    assert calls == []


def test_result_rows_read_minio_chunks_for_legacy_staging(monkeypatch) -> None:
    store = OpenGaussPartitionDomainStore(connection_factory=lambda: _RecordingConnection())
    calls: list[str] = []
    monkeypatch.setattr(store, "_fetchall", lambda *_args, **_kwargs: [{"count": 0}])
    monkeypatch.setattr(
        store, "_iter_persisted_chunk_rows",
        lambda *_args, **_kwargs: calls.append("minio") or iter([{"output_id": "row-a"}]),
    )

    rows = list(store._result_rows(_RecordingConnection(), _chunk_result("version-a", inserted=None), "tiles"))

    assert rows == [{"output_id": "row-a"}]
    assert calls == ["minio"]


def test_complete_output_fails_when_direct_rows_are_missing(monkeypatch) -> None:
    connection = _RecordingConnection()
    store = OpenGaussPartitionDomainStore(connection_factory=lambda: connection)
    monkeypatch.setattr(store, "_assert_live_schema", lambda _connection: None)
    monkeypatch.setattr(store, "_execute", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(store, "_merge_insert", lambda *_args, **_kwargs: None)

    def fetchall(_connection, sql, _params=()):
        if "FROM partition_datasets" in sql:
            return [{"dataset_id": "dataset-a", "batch_id": "batch-a"}]
        if "FROM partition_job_attempts" in sql:
            return [{
                "task_id": "task-a",
                "batch_id": "batch-a",
                "payload": {"strict_partition_request": True, "datasets": [{"dataset_id": "dataset-a"}]},
            }]
        if "FROM partition_output_versions" in sql:
            return [{"task_id": "task-a", "status": "staging"}]
        if "AS grid_cells" in sql:
            return [{"tiles": 4, "indexes": 4, "grid_cells": 0}]
        return []

    monkeypatch.setattr(store, "_fetchall", fetchall)
    result = _chunk_result("version-a", inserted={"grid_cells": 2, "tiles": 4, "indexes": 4})

    with pytest.raises(RuntimeError, match="logical chunk rows are missing"):
        store.complete_output(result)


def test_complete_output_accepts_direct_rows_matching_counts(monkeypatch) -> None:
    connection = _RecordingConnection()
    store = OpenGaussPartitionDomainStore(connection_factory=lambda: connection)
    monkeypatch.setattr(store, "_assert_live_schema", lambda _connection: None)
    monkeypatch.setattr(store, "_execute", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(store, "_merge_insert", lambda *_args, **_kwargs: None)
    committed: list = []

    def fetchall(_connection, sql, _params=()):
        if "FROM partition_datasets" in sql:
            return [{"dataset_id": "dataset-a", "batch_id": "batch-a"}]
        if "FROM partition_job_attempts" in sql:
            return [{
                "task_id": "task-a",
                "batch_id": "batch-a",
                "payload": {"strict_partition_request": True, "datasets": [{"dataset_id": "dataset-a"}]},
            }]
        if "FROM partition_output_versions" in sql:
            return [{"task_id": "task-a", "status": "staging"}]
        if "AS grid_cells" in sql:
            return [{"tiles": 4, "indexes": 4, "grid_cells": 2}]
        return []

    monkeypatch.setattr(store, "_fetchall", fetchall)
    monkeypatch.setattr(store, "_recover_ambiguous_commit", lambda *_args, **_kwargs: committed.append("recover") or None)
    result = _chunk_result("version-a", inserted={"grid_cells": 2, "tiles": 4, "indexes": 4})

    store.complete_output(result)

    assert committed == []
