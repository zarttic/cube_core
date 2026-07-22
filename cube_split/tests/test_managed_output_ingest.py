from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from cube_split.ingest import managed_output_ingest as managed


class _Conn:
    pass


def _snapshot(data_type="optical", method="logical", geometry=None, attributes=None):
    geometry = geometry or {
        "type": "Polygon",
        "coordinates": [[[116.0, 39.0], [116.1, 39.0], [116.1, 39.1], [116.0, 39.1], [116.0, 39.0]]],
    }
    return {
        "dataset": {"dataset_id": "dataset-a", "dataset_title": "Dataset A", "data_type": data_type, "product_type": "xco2", "attributes": {}},
        "output": {"partition_method": method},
        "scene_assets": [{"scene_id": "scene-a", "asset_id": "asset-a", "source_uri": "s3://cube/source.tif", "acquisition_time": "2026-01-02T00:00:00Z"}],
        "indexes": [{
            "source_asset_id": "asset-a", "band_code": "b1", "acquisition_time": "2026-01-02T00:00:00Z",
            "cog_uri": "s3://cube/source.tif", "source_uri": None, "grid_type": "isea4h" if method == "entity" else "geohash",
            "grid_level": 1 if method == "entity" else 5, "space_code": "cell-a", "topology_code": None,
            "time_bucket": "20260102", "st_code": "st-a", "value_ref_uri": "s3://cube/tile.tif" if method == "entity" else "s3://cube/source.tif#window=0,0,8,8",
            "cell_bbox": [116.0, 39.0, 116.1, 39.1], "cell_geometry": geometry, "width": 8, "height": 8,
            "attributes": attributes or {},
        }],
    }


@pytest.fixture
def capture(monkeypatch):
    values = {}
    monkeypatch.setattr(managed, "ensure_tables_postgres", lambda _conn: None)
    monkeypatch.setattr(managed, "ensure_product_tables_postgres", lambda _conn: None)
    monkeypatch.setattr(managed, "ensure_carbon_tables_postgres", lambda _conn: None)
    monkeypatch.setattr(managed, "_ensure_entity_tables_postgres", lambda _conn: None)
    monkeypatch.setattr(managed, "upsert_raw_assets_postgres", lambda _conn, rows: values.setdefault("raw", rows))
    monkeypatch.setattr(managed, "upsert_cube_facts_postgres", lambda _conn, rows: values.setdefault("cube", rows))
    monkeypatch.setattr(managed, "upsert_product_assets_postgres", lambda _conn, rows: values.setdefault("product_assets", rows))
    monkeypatch.setattr(managed, "upsert_product_facts_postgres", lambda _conn, rows: values.setdefault("product_facts", rows))
    monkeypatch.setattr(managed, "upsert_carbon_facts_postgres", lambda _conn, rows: values.setdefault("carbon", rows))
    monkeypatch.setattr(managed, "_upsert_entity_tiles_postgres", lambda _conn, rows, *_args: values.setdefault("entity", rows))
    return values


def test_raster_logical_writes_actual_managed_cell_geometry(capture):
    snapshot = _snapshot()
    result = managed._ingest_raster(_Conn(), snapshot, "output-v1", "job-a", entity=False)
    assert result.target_tables == ("rs_raw_scene_asset", "rs_cube_cell_fact")
    assert json.loads(capture["cube"][0].cell_geom_geojson) == snapshot["indexes"][0]["cell_geometry"]
    assert capture["cube"][0].cube_version == "output-v1"


def test_managed_ingest_verifies_accessible_cross_bucket_source(monkeypatch):
    calls = []

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        def stat_object(self, bucket, key):
            calls.append((bucket, key))
            return SimpleNamespace(metadata={})

    import minio

    monkeypatch.setattr(minio, "Minio", Client)
    monkeypatch.setattr(
        managed.runtime_config,
        "minio_settings",
        lambda: SimpleNamespace(endpoint="minio:9000", access_key="key", secret_key="secret", secure=False, bucket="cube"),
    )
    snapshot = _snapshot()
    snapshot["indexes"][0].update({
        "cog_uri": "s3://user-1/cog/source.tif",
        "value_ref_uri": "s3://user-1/cog/source.tif#window=0,0,8,8",
    })

    managed._verify_minio_objects(snapshot)

    assert calls == [("user-1", "cog/source.tif")]


def test_managed_ingest_rejects_cross_bucket_entity_tile(monkeypatch):
    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        def stat_object(self, _bucket, _key):
            return SimpleNamespace(metadata={})

    import minio

    monkeypatch.setattr(minio, "Minio", Client)
    monkeypatch.setattr(
        managed.runtime_config,
        "minio_settings",
        lambda: SimpleNamespace(endpoint="minio:9000", access_key="key", secret_key="secret", secure=False, bucket="cube"),
    )
    snapshot = _snapshot(method="entity")
    snapshot["indexes"][0]["value_ref_uri"] = "s3://user-1/cog/entity-tile.tif"

    with pytest.raises(RuntimeError, match="outside configured MinIO bucket"):
        managed._verify_minio_objects(snapshot)


def test_raster_entity_writes_cube_geometry_and_entity_catalog(capture):
    geometry = {
        "type": "Polygon",
        "coordinates": [[[116.0, 39.0], [116.1, 39.0], [116.15, 39.05], [116.1, 39.1], [116.0, 39.1], [115.95, 39.05], [116.0, 39.0]]],
    }
    result = managed._ingest_raster(_Conn(), _snapshot("radar", "entity", geometry), "output-v1", "job-a", entity=True)
    assert "rs_entity_tile_asset" in result.target_tables
    assert json.loads(capture["cube"][0].cell_geom_geojson) == geometry
    assert capture["entity"][0]["entity_tile_uri"] == "s3://cube/tile.tif"


def test_managed_entity_failure_rolls_back_all_target_rows(monkeypatch):
    class TransactionConnection:
        def __init__(self):
            self.pending = []
            self.committed = []
            self.commit_count = 0
            self.rollback_count = 0

        def cursor(self, *args, **kwargs):
            _ = args, kwargs
            return self

        def __enter__(self):
            return self

        def __exit__(self, *_exc_info):
            return False

        def execute(self, *_args, **_kwargs):
            pass

        def commit(self):
            self.committed.extend(self.pending)
            self.pending.clear()
            self.commit_count += 1

        def rollback(self):
            self.pending.clear()
            self.rollback_count += 1

    conn = TransactionConnection()
    geometry = {
        "type": "Polygon",
        "coordinates": [[
            [116.0, 39.0], [116.1, 39.0], [116.15, 39.05], [116.1, 39.1],
            [116.0, 39.1], [115.95, 39.05], [116.0, 39.0],
        ]],
    }
    snapshot = _snapshot("radar", "entity", geometry)
    monkeypatch.setattr(managed, "_load_snapshot", lambda *_args, **_kwargs: snapshot)
    monkeypatch.setattr(managed, "_verify_minio_objects", lambda _snapshot: None)
    monkeypatch.setattr(managed, "ensure_tables_postgres", lambda _conn: None)
    monkeypatch.setattr(managed, "upsert_raw_assets_postgres", lambda target, _rows: target.pending.append("raw"))
    monkeypatch.setattr(managed, "upsert_cube_facts_postgres", lambda target, _rows: target.pending.append("cube"))
    monkeypatch.setattr(
        managed,
        "_upsert_job_status_postgres",
        lambda target, _job_id, status, *_args, **_kwargs: target.pending.append(f"job:{status}"),
    )

    def fail_entity_write(target, *_args):
        target.pending.append("entity")
        raise RuntimeError("entity write failed")

    monkeypatch.setattr(managed, "_upsert_entity_tiles_postgres", fail_entity_write)

    with pytest.raises(RuntimeError, match="entity write failed"):
        managed.ingest_managed_output(
            conn,
            dataset_id="dataset-a",
            output_dataset_id="output-a",
            output_version="output-v1",
            ingest_job_id="job-a",
            scene_ids=("scene-a",),
        )

    assert conn.rollback_count == 1
    assert conn.commit_count == 1
    assert conn.committed == ["job:failed"]


def test_product_logical_uses_output_version(capture):
    result = managed._ingest_product(_Conn(), _snapshot("product"), "output-v2", "job-p")
    assert result.row_counts == {"rs_product_asset": 1, "rs_product_cell_fact": 1}
    assert capture["product_assets"][0].version == "output-v2"
    assert capture["product_facts"][0].cube_version == "output-v2"


def test_carbon_requires_and_writes_lossless_observation_attributes(capture):
    attrs = {
        "satellite": "OCO2", "product_type": "xco2", "observation_id": "obs-1", "xco2": 411.2,
        "quality_flag": "0", "center_lon": 116.1, "center_lat": 39.9, "footprint_geojson": {"type": "Point", "coordinates": [116.1, 39.9]},
        "source_index": 7, "metadata_json": {"orbit": "a"},
    }
    result = managed._ingest_carbon(_Conn(), _snapshot("carbon", attributes=attrs), "output-v3", "job-c")
    assert result.row_counts["rs_carbon_observation_fact"] == 1
    assert capture["carbon"][0].observation_id == "obs-1"
    with pytest.raises(RuntimeError, match="lacks observation attributes"):
        managed._ingest_carbon(_Conn(), _snapshot("carbon"), "output-v3", "job-c")


def test_managed_target_verification_casts_opengauss_text_keys() -> None:
    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, sql, _params):
            self.sql = sql

        def fetchone(self):
            return (1,)

    class Connection:
        def __init__(self):
            self.cursor_value = Cursor()

        def cursor(self):
            return self.cursor_value

    connection = Connection()
    managed._verify_targets(
        connection,
        "job-a",
        "version-a",
        managed.ManagedIngestResult(("rs_carbon_observation_fact",), {"rs_carbon_observation_fact": 1}),
    )

    assert "CAST(run_id AS VARCHAR(128))=%s::varchar(128)" in connection.cursor_value.sql
    assert "CAST(cube_version AS VARCHAR(128))=%s::varchar(128)" in connection.cursor_value.sql
