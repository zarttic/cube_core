from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from grid_core.sdk import CubeEncoderSDK, GridAddress
from shapely.geometry import shape

import cube_split.ingest.ray_ingest_job as ray_ingest_job
from cube_split.ingest.ray_ingest_job import (
    CubeFactRecord,
    RawAssetRecord,
    build_cube_fact_records,
    build_raw_asset_records,
    cell_geometry_geojson,
    ensure_tables,
    ensure_tables_postgres,
    load_rows,
    materialize_cog_assets,
    run_ingest,
    upload_assets_to_minio,
    upsert_cube_facts_postgres,
    upsert_raw_assets_postgres,
)


def _sample_row(
    scene_id: str,
    acq_time: str,
    space_code: str = "wx4dy",
    band: str = "b04",
    asset_path: str | None = None,
) -> dict:
    return {
        "scene_id": scene_id,
        "band": band,
        "asset_path": asset_path or f"/tmp/{scene_id}_{band}.TIF",
        "acq_time": acq_time,
        "grid_type": "geohash",
        "grid_level": 5,
        "space_code": space_code,
        "space_code_prefix": space_code[:3],
        "st_code": f"gh:5:{space_code}:20260421",
        "time_bucket": "20260421",
        "cover_mode": "intersect",
        "cell_min_lon": 116.1,
        "cell_min_lat": 39.8,
        "cell_max_lon": 116.2,
        "cell_max_lat": 39.9,
        "window_col_off": 0,
        "window_row_off": 0,
        "window_width": 256,
        "window_height": 256,
        "intersect_min_lon": 116.1,
        "intersect_min_lat": 39.8,
        "intersect_max_lon": 116.2,
        "intersect_max_lat": 39.9,
        "sample_mean_band1": 1.0,
    }


def test_build_raw_asset_records_deduplicates_by_scene_band():
    rows = [
        _sample_row("S1", "2026-04-21T00:00:00Z"),
        _sample_row("S1", "2026-04-21T01:00:00Z"),
        _sample_row("S2", "2026-04-21T02:00:00Z"),
    ]
    records = build_raw_asset_records(
        rows=rows,
        dataset="landsat8",
        sensor="L8",
        asset_version="v1",
        run_id="job-1",
        asset_uri_map={row["asset_path"]: row["asset_path"] for row in rows},
    )
    assert len(records) == 2
    by_scene = {row.scene_id: row for row in records}
    assert by_scene["S1"].acq_time == "2026-04-21T01:00:00Z"


def test_build_cube_fact_records_resolves_conflict_with_latest_scene():
    rows = [
        _sample_row("S_OLD", "2026-04-21T00:00:00Z", space_code="wx4dy"),
        _sample_row("S_NEW", "2026-04-21T06:00:00Z", space_code="wx4dy"),
    ]
    facts = build_cube_fact_records(
        rows=rows,
        cube_version="v1",
        run_id="job-1",
        quality_rule="latest_wins",
        asset_uri_map={row["asset_path"]: row["asset_path"] for row in rows},
    )
    assert len(facts) == 1
    fact = facts[0]
    provenance = json.loads(fact.provenance_json)
    assert provenance["winner_scene_id"] == "S_NEW"
    assert sorted(provenance["candidate_scene_ids"]) == ["S_NEW", "S_OLD"]
    assert fact.source_scene_count == 2


def test_build_cube_fact_records_respects_quality_rule():
    rows = [
        {**_sample_row("S_OLD", "2026-04-21T00:00:00Z", space_code="wx4dy"), "sample_mean_band1": 99.0},
        {**_sample_row("S_NEW", "2026-04-21T06:00:00Z", space_code="wx4dy"), "sample_mean_band1": 1.0},
    ]

    best = build_cube_fact_records(
        rows=rows,
        cube_version="v1",
        run_id="job-1",
        quality_rule="best_quality_wins",
        asset_uri_map={row["asset_path"]: row["asset_path"] for row in rows},
    )[0]
    latest = build_cube_fact_records(
        rows=rows,
        cube_version="v1",
        run_id="job-1",
        quality_rule="latest_wins",
        asset_uri_map={row["asset_path"]: row["asset_path"] for row in rows},
    )[0]

    assert json.loads(best.provenance_json)["winner_scene_id"] == "S_OLD"
    assert json.loads(latest.provenance_json)["winner_scene_id"] == "S_NEW"


def test_cell_geometry_uses_sdk_boundaries_for_all_production_grids():
    sdk = CubeEncoderSDK()
    cases = (("geohash", 5, 5), ("mgrs", 3, 5), ("isea4h", 2, 7))

    for grid_type, grid_level, expected_points in cases:
        cell = sdk.locate(grid_type=grid_type, requested_grid_level=grid_level, point=[116.3, 39.9])
        geometry = json.loads(
            cell_geometry_geojson(
                grid_type=grid_type,
                grid_level=cell.grid_level,
                space_code=cell.space_code,
                topology_code=cell.topology_code,
                sdk=sdk,
            )
        )
        ring = geometry["coordinates"][0]

        assert geometry["type"] == "Polygon"
        assert len(ring) == expected_points
        assert ring[0] == ring[-1]
        assert all(-180 <= lon <= 180 and -90 <= lat <= 90 for lon, lat in ring)
        assert len({tuple(point) for point in ring[:-1]}) == expected_points - 1


def test_mgrs_cell_geometry_reduces_latitude_band_clipped_boundary_to_four_corners():
    geometry = json.loads(
        cell_geometry_geojson(
            grid_type="mgrs",
            grid_level=2,
            space_code="50SMK1428",
        )
    )

    ring = geometry["coordinates"][0]
    assert len(ring) == 5
    assert ring[0] == ring[-1]
    assert len({tuple(point) for point in ring[:-1]}) == 4


def test_mgrs_cell_geometry_preserves_non_quadrilateral_boundary_cells():
    sdk = CubeEncoderSDK()
    cases = (("01CDM42", 1), ("02VLH1210", 2))
    for space_code, grid_level in cases:
        address = GridAddress(grid_type="mgrs", grid_level=grid_level, space_code=space_code)
        expected = sdk.code_to_geometry(address=address)
        actual = json.loads(
            cell_geometry_geojson(
                grid_type="mgrs",
                grid_level=grid_level,
                space_code=space_code,
                geometry=expected,
            )
        )

        ring = actual["coordinates"][0]
        assert len(ring) >= 4
        assert ring[0] == ring[-1]
        expected_shape = shape(expected)
        relative_error = shape(actual).symmetric_difference(expected_shape).area / expected_shape.area
        assert relative_error <= 1e-6


def test_isea4h_cell_geometry_is_actual_hexagon_not_bbox():
    sdk = CubeEncoderSDK()
    cell = sdk.locate(grid_type="isea4h", requested_grid_level=2, point=[116.3, 39.9])
    geometry = json.loads(
        cell_geometry_geojson(
            grid_type="isea4h",
            grid_level=cell.grid_level,
            space_code=cell.space_code,
            sdk=sdk,
        )
    )
    ring = geometry["coordinates"][0]
    xs = {point[0] for point in ring[:-1]}
    ys = {point[1] for point in ring[:-1]}

    assert len(ring) == 7
    assert len(xs) > 2
    assert len(ys) > 2


def test_cube_fact_preserves_structured_partition_cell_geometry(monkeypatch):
    geometry = {
        "type": "Polygon",
        "coordinates": [[[10.0, 20.0], [11.0, 20.0], [11.0, 21.0], [10.0, 21.0], [10.0, 20.0]]],
    }
    row = {**_sample_row("S1", "2026-04-21T00:00:00Z"), "cell_geom": geometry}

    def fail_recompute(*args, **kwargs):
        raise AssertionError("structured partition geometry should not be recomputed")

    monkeypatch.setattr(CubeEncoderSDK, "code_to_geometry", fail_recompute)
    fact = build_cube_fact_records(
        rows=[row],
        cube_version="v1",
        run_id="job-1",
        quality_rule="latest_wins",
        asset_uri_map={row["asset_path"]: row["asset_path"]},
    )[0]

    assert json.loads(fact.cell_geom_geojson) == geometry


def test_upload_assets_to_minio_reuploads_when_identity_changes(monkeypatch, tmp_path: Path):
    source = tmp_path / "S1_b04.TIF"
    source.write_bytes(b"data")
    source.with_name(f"{source.name}.identity").write_text("local=size:4|mtime_ns:1\nremote=etag:stale", encoding="utf-8")
    uploaded: list[str] = []

    class FakeStat:
        size = 4
        etag = "remote-new"
        last_modified = None

    class FakeMinio:
        def __init__(self, *args, **kwargs):
            pass

        def bucket_exists(self, _bucket):
            return True

        def make_bucket(self, _bucket):
            raise AssertionError("bucket already exists")

        def stat_object(self, _bucket, _key):
            return FakeStat()

        def fput_object(self, _bucket, key, _path):
            uploaded.append(key)

    monkeypatch.setattr("minio.Minio", FakeMinio)

    mapping = upload_assets_to_minio(
        rows=[_sample_row("S1", "2026-04-21T00:00:00Z", asset_path=str(source))],
        dataset="landsat8",
        sensor="L8",
        asset_version="v1",
        endpoint="127.0.0.1:9000",
        access_key="access",
        secret_key="secret",
        bucket="cube",
        prefix="cube/raw",
        secure=False,
        workers=1,
    )

    assert uploaded
    assert mapping[str(source)].startswith("s3://cube/cube/raw/")
    identity_text = source.with_name(f"{source.name}.identity").read_text(encoding="utf-8")
    assert "local=size:4|mtime_ns:" in identity_text
    assert "remote=etag:remote-new" in identity_text


def test_run_ingest_creates_and_upserts_tables(tmp_path: Path):
    run_dir = tmp_path / "run_001"
    run_dir.mkdir(parents=True)
    rows_path = run_dir / "index_rows.jsonl"
    source_dir = tmp_path / "source_assets"
    source_dir.mkdir(parents=True)

    s1_path = source_dir / "S1_b04.TIF"
    s2_path = source_dir / "S2_b04.TIF"
    s1_path.write_bytes(b"fake-cog-s1")
    s2_path.write_bytes(b"fake-cog-s2")

    rows = [
        _sample_row("S1", "2026-04-21T00:00:00Z", space_code="wx4dy", band="b04", asset_path=str(s1_path)),
        _sample_row("S2", "2026-04-21T05:00:00Z", space_code="wx4dy", band="b04", asset_path=str(s2_path)),
        _sample_row("S1", "2026-04-21T00:00:00Z", space_code="wx4dz", band="b04", asset_path=str(s1_path)),
    ]
    with rows_path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    db_path = tmp_path / "ingest.db"

    args = SimpleNamespace(
        run_dir=str(run_dir),
        db_path=str(db_path),
        job_id="job-001",
        dataset="landsat8",
        sensor="L8",
        asset_version="v1",
        cube_version="v1",
        quality_rule="best_quality_wins",
        metadata_backend="sqlite",
        asset_storage_backend="local",
        cog_output_root=str(tmp_path / "cog_store"),
        cog_materialize_mode="copy",
        postgres_dsn="",
        minio_endpoint="",
        minio_access_key="",
        minio_secret_key="",
        minio_bucket="",
        minio_prefix="cube/raw",
        minio_secure=False,
        minio_upload_workers=2,
    )

    stats = run_ingest(args)
    assert stats["input_rows"] == 3
    assert stats["materialized_cog_assets"] == 2
    assert stats["raw_asset_rows"] == 2
    assert stats["cube_fact_rows"] == 2

    conn = sqlite3.connect(str(db_path))
    try:
        raw_count = conn.execute("SELECT COUNT(*) FROM rs_raw_scene_asset").fetchone()[0]
        cube_count = conn.execute("SELECT COUNT(*) FROM rs_cube_cell_fact").fetchone()[0]
        job = conn.execute("SELECT status FROM rs_ingest_job WHERE job_id = ?", ("job-001",)).fetchone()
        cog_uri = conn.execute("SELECT raw_cog_uri FROM rs_raw_scene_asset ORDER BY raw_cog_uri LIMIT 1").fetchone()[0]
        value_ref, cell_geom = conn.execute(
            "SELECT value_ref_uri, cell_geom FROM rs_cube_cell_fact ORDER BY value_ref_uri LIMIT 1"
        ).fetchone()
    finally:
        conn.close()

    assert raw_count == 2
    assert cube_count == 2
    assert job[0] == "succeeded"
    assert "/cog_store/" in cog_uri
    assert "#window=" in value_ref
    assert "/cog_store/" in value_ref
    geometry = json.loads(cell_geom)
    assert geometry["type"] == "Polygon"
    assert len(geometry["coordinates"][0]) == 5
    assert geometry["coordinates"][0][0] == geometry["coordinates"][0][-1]


def test_run_ingest_reports_probe_metrics_per_cube_fact(monkeypatch, tmp_path: Path):
    run_dir = tmp_path / "run_probe"
    run_dir.mkdir(parents=True)
    rows_path = run_dir / "index_rows.jsonl"
    source_dir = tmp_path / "source_assets"
    source_dir.mkdir(parents=True)
    b04_path = source_dir / "S1_b04.TIF"
    b08_path = source_dir / "S1_b08.TIF"
    b04_path.write_bytes(b"fake-cog-b04")
    b08_path.write_bytes(b"fake-cog-b08")
    rows = [
        _sample_row("S1", "2026-04-21T00:00:00Z", space_code="wx4dy", band="b04", asset_path=str(b04_path)),
        _sample_row("S1", "2026-04-21T00:00:00Z", space_code="wx4dy", band="b08", asset_path=str(b08_path)),
    ]
    with rows_path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    captured: list[ray_ingest_job.TileProbeMetric] = []

    def fake_report_tile_metrics(metrics):
        captured.extend(list(metrics))

    monkeypatch.setattr(ray_ingest_job, "report_tile_metrics", fake_report_tile_metrics)

    run_ingest(
        SimpleNamespace(
            run_dir=str(run_dir),
            db_path=str(tmp_path / "ingest_probe.db"),
            job_id="job-probe",
            dataset="landsat8",
            sensor="L8",
            asset_version="v1",
            cube_version="v1",
            quality_rule="best_quality_wins",
            metadata_backend="sqlite",
            asset_storage_backend="local",
            cog_output_root=str(tmp_path / "cog_store"),
            cog_materialize_mode="copy",
            postgres_dsn="",
            minio_endpoint="",
            minio_access_key="",
            minio_secret_key="",
            minio_bucket="",
            minio_prefix="cube/raw",
            minio_secure=False,
            minio_upload_workers=1,
        )
    )

    assert len(captured) == 2
    assert {metric.task_name for metric in captured} == {"cube.partition.logical.ingest.optical"}
    assert {metric.method_name for metric in captured} == {"merge.rs_cube_cell_fact"}
    assert {metric.attributes["cube.band"] for metric in captured} == {"b04", "b08"}
    assert {metric.attributes["cube.space_code"] for metric in captured} == {"wx4dy"}
    assert {metric.attributes["cube.target_table"] for metric in captured} == {"rs_cube_cell_fact"}


def test_postgres_upserts_batch_merge_rows() -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple]] = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            self.calls.append((sql, tuple(params)))

    class FakeConn:
        def __init__(self) -> None:
            self.cursor_obj = FakeCursor()

        def cursor(self):
            return self.cursor_obj

    raw_records = [
        RawAssetRecord("dataset", "sensor", f"scene-{idx}", "b04", "2026-04-21T00:00:00Z", f"s3://cube/a{idx}.tif", "v1", "job")
        for idx in range(3)
    ]
    cube_records = [
        CubeFactRecord(
            "geohash",
            5,
            f"wx4d{'y' if idx < 2 else 'z'}",
            "20260421",
            "b04",
            f"gh:5:wx4d{'y' if idx < 2 else 'z'}:20260421",
            116.1,
            39.8,
            116.2,
            39.9,
            '{"type":"Polygon","coordinates":[[[116.1,39.8],[116.2,39.8],[116.2,39.9],[116.1,39.9],[116.1,39.8]]]}',
            f"s3://cube/a{idx}.tif#window=0,0,256,256",
            1,
            '{"winner_scene_id":"scene"}',
            "best_quality_wins",
            "v1",
            "job",
        )
        for idx in range(3)
    ]

    raw_conn = FakeConn()
    cube_conn = FakeConn()

    upsert_raw_assets_postgres(raw_conn, raw_records, batch_size=2)
    upsert_cube_facts_postgres(cube_conn, cube_records, batch_size=2)

    assert len(raw_conn.cursor_obj.calls) == 2
    assert len(cube_conn.cursor_obj.calls) == 2
    assert "VALUES" in raw_conn.cursor_obj.calls[0][0]
    assert "VALUES" in cube_conn.cursor_obj.calls[0][0]
    assert len(raw_conn.cursor_obj.calls[0][1]) == 16
    assert len(raw_conn.cursor_obj.calls[1][1]) == 8
    assert len(cube_conn.cursor_obj.calls[0][1]) == 34
    assert len(cube_conn.cursor_obj.calls[1][1]) == 17
    assert "ST_SetSRID(ST_GeomFromGeoJSON(source.cell_geom_geojson), 4326)" in cube_conn.cursor_obj.calls[0][0]


def test_load_rows_rejects_empty_file(tmp_path: Path):
    run_dir = tmp_path / "run_empty"
    run_dir.mkdir()
    rows_path = run_dir / "index_rows.jsonl"
    rows_path.write_text("", encoding="utf-8")
    try:
        load_rows(rows_path)
    except RuntimeError as exc:
        assert "No rows found" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError for empty rows file")


def test_ensure_tables_is_idempotent(tmp_path: Path):
    db_path = tmp_path / "ingest.db"
    conn = sqlite3.connect(str(db_path))
    try:
        ensure_tables(conn)
        ensure_tables(conn)
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'rs_%'"
            ).fetchall()
        }
    finally:
        conn.close()
    assert {"rs_raw_scene_asset", "rs_cube_cell_fact", "rs_ingest_job"}.issubset(tables)


def test_ensure_tables_adds_cell_geom_to_existing_sqlite_table(tmp_path: Path):
    conn = sqlite3.connect(str(tmp_path / "legacy.db"))
    try:
        conn.execute("CREATE TABLE rs_cube_cell_fact (id INTEGER PRIMARY KEY)")
        ensure_tables(conn)
        columns = {row[1]: row[2] for row in conn.execute("PRAGMA table_info(rs_cube_cell_fact)").fetchall()}
    finally:
        conn.close()

    assert columns["cell_geom"] == "TEXT"


def test_ensure_tables_postgres_uses_additive_polygon_migration():
    class FakeCursor:
        def __init__(self):
            self.sql: list[str] = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql):
            self.sql.append(sql)

    class FakeConn:
        def __init__(self):
            self.cursor_obj = FakeCursor()
            self.commits = 0

        def cursor(self):
            return self.cursor_obj

        def commit(self):
            self.commits += 1

    conn = FakeConn()
    ensure_tables_postgres(conn)
    statements = "\n".join(conn.cursor_obj.sql)

    assert "ADD COLUMN IF NOT EXISTS cell_geom geometry(Polygon, 4326)" in statements
    assert conn.commits == 1


def test_materialize_cog_assets_copies_to_standard_layout(tmp_path: Path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    source = source_dir / "A.TIF"
    source.write_bytes(b"abc")
    rows = [
        _sample_row(
            scene_id="SCENE_A",
            acq_time="2026-04-21T00:00:00Z",
            band="b04",
            asset_path=str(source),
        )
    ]
    mapping = materialize_cog_assets(
        rows=rows,
        dataset="landsat8",
        sensor="L8",
        asset_version="v1",
        cog_output_root=tmp_path / "cog_store",
        materialize_mode="copy",
    )
    assert str(source) in mapping
    target = Path(mapping[str(source)])
    assert target.exists()
    assert target.read_bytes() == b"abc"
    assert "dataset=landsat8" in str(target)
    assert "scene_id=SCENE_A" in str(target)
