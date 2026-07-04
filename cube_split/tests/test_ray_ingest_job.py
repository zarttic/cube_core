from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import cube_split.ingest.ray_ingest_job as ray_ingest_job
from cube_split.ingest.ray_ingest_job import (
    CubeFactRecord,
    RawAssetRecord,
    build_cube_fact_records,
    build_raw_asset_records,
    ensure_tables,
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
    space_code: str = "35f04",
    band: str = "b04",
    asset_path: str | None = None,
) -> dict:
    return {
        "scene_id": scene_id,
        "band": band,
        "asset_path": asset_path or f"/tmp/{scene_id}_{band}.TIF",
        "acq_time": acq_time,
        "grid_type": "s2",
        "grid_level": 7,
        "space_code": space_code,
        "space_code_prefix": space_code[:3],
        "st_code": f"s2:7:{space_code}:20260421",
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
        _sample_row("S_OLD", "2026-04-21T00:00:00Z", space_code="35f04"),
        _sample_row("S_NEW", "2026-04-21T06:00:00Z", space_code="35f04"),
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
        {**_sample_row("S_OLD", "2026-04-21T00:00:00Z", space_code="35f04"), "sample_mean_band1": 99.0},
        {**_sample_row("S_NEW", "2026-04-21T06:00:00Z", space_code="35f04"), "sample_mean_band1": 1.0},
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
        _sample_row("S1", "2026-04-21T00:00:00Z", space_code="35f04", band="b04", asset_path=str(s1_path)),
        _sample_row("S2", "2026-04-21T05:00:00Z", space_code="35f04", band="b04", asset_path=str(s2_path)),
        _sample_row("S1", "2026-04-21T00:00:00Z", space_code="35f05", band="b04", asset_path=str(s1_path)),
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
        value_ref = conn.execute("SELECT value_ref_uri FROM rs_cube_cell_fact ORDER BY value_ref_uri LIMIT 1").fetchone()[0]
    finally:
        conn.close()

    assert raw_count == 2
    assert cube_count == 2
    assert job[0] == "succeeded"
    assert "/cog_store/" in cog_uri
    assert "#window=" in value_ref
    assert "/cog_store/" in value_ref


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
        _sample_row("S1", "2026-04-21T00:00:00Z", space_code="35f04", band="b04", asset_path=str(b04_path)),
        _sample_row("S1", "2026-04-21T00:00:00Z", space_code="35f04", band="b08", asset_path=str(b08_path)),
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
    assert {metric.attributes["cube.space_code"] for metric in captured} == {"35f04"}
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
            "s2",
            7,
            f"35f0{idx}",
            "20260421",
            "b04",
            f"s2:7:35f0{idx}:20260421",
            116.1,
            39.8,
            116.2,
            39.9,
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
    assert len(cube_conn.cursor_obj.calls[0][1]) == 32
    assert len(cube_conn.cursor_obj.calls[1][1]) == 16


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
