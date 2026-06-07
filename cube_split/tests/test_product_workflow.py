from __future__ import annotations

import json
import sqlite3
from argparse import Namespace
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_origin

from cube_split.ingest.product_ingest_job import run_product_ingest
from cube_split.jobs.product_partition_job import _prepare_product_task_rows, run_product_partition
from cube_split.partition.product_products import parse_product_asset
from cube_split.quality.product_quality import run_quality_check


def _write_product_tif(path: Path, *, crs: str = "EPSG:4326") -> None:
    data = np.arange(100, dtype=np.float32).reshape(1, 10, 10)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=10,
        height=10,
        count=1,
        dtype=data.dtype,
        crs=crs,
        transform=from_origin(100.0, 25.0, 0.01, 0.01),
        nodata=-9999.0,
    ) as ds:
        ds.write(data)


def _product_row(asset_path: Path, year: int = 1980) -> dict:
    return {
        "scene_id": f"dianzhong_ecological_security_{year}",
        "band": "product_value",
        "asset_path": str(asset_path),
        "acq_time": f"{year}-01-01T00:00:00Z",
        "grid_type": "geohash",
        "grid_level": 5,
        "space_code": "wm6n0",
        "space_code_prefix": "wm6",
        "st_code": f"gh:5:wm6n0:{year}",
        "time_bucket": str(year),
        "cover_mode": "intersect",
        "cell_min_lon": 100.0,
        "cell_min_lat": 24.9,
        "cell_max_lon": 100.1,
        "cell_max_lat": 25.0,
        "window_col_off": 0,
        "window_row_off": 0,
        "window_width": 8,
        "window_height": 8,
        "intersect_min_lon": 100.0,
        "intersect_min_lat": 24.9,
        "intersect_max_lon": 100.1,
        "intersect_max_lat": 25.0,
        "sample_mean_band1": 10.0,
    }


def test_parse_product_asset_extracts_year_and_product_identity(tmp_path: Path):
    tif = tmp_path / "1980-2020年滇中地区30米生态安全评价数据集（第一版）_2010年.tif"
    tif.write_bytes(b"fake")

    record = parse_product_asset(tif)

    assert record.scene_id == "dianzhong_ecological_security_2010"
    assert record.band == "product_value"
    assert record.product_family == "product"
    assert record.sensor == "data_product"
    assert record.acq_time.isoformat().startswith("2010-01-01T00:00:00")


def test_prepare_product_task_rows_keeps_year_bucket_and_day_st_code_input():
    rows = _prepare_product_task_rows(
        [
            {
                "space_code": "wm6n0",
                "acq_time": "2010-01-01T00:00:00Z",
            }
        ],
        partition_prefix_len=3,
    )

    assert rows[0]["space_code_prefix"] == "wm6"
    assert rows[0]["time_bucket"] == "2010"
    assert rows[0]["st_time_granularity"] == "day"


def test_product_partition_disables_cog_predictor_for_64bit_product_assets(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    cog_dir = tmp_path / "cog"
    input_dir.mkdir()
    tif = input_dir / "1980-2020年滇中地区30米生态安全评价数据集（第一版）_1980年.tif"
    _write_product_tif(tif)
    captured = {}

    def fake_convert_assets_to_cog(assets, **kwargs):
        captured["predictor"] = kwargs["predictor"]
        return assets

    def fake_build_grid_tasks_driver(**kwargs):
        return [
            {
                "scene_id": "dianzhong_ecological_security_1980",
                "band": "product_value",
                "asset_path": str(tif),
                "acq_time": "1980-01-01T00:00:00Z",
                "grid_type": "geohash",
                "grid_level": 5,
                "space_code": "wm6n0",
                "st_code": "gh:5:wm6n0:19800101",
                "cell_min_lon": 100.0,
                "cell_min_lat": 24.9,
                "cell_max_lon": 100.1,
                "cell_max_lat": 25.0,
                "window_col_off": 0,
                "window_row_off": 0,
                "window_width": 8,
                "window_height": 8,
            }
        ]

    def fake_process_local_task_group(group, time_granularity, include_sample_mean=False):
        return group

    monkeypatch.setattr("cube_split.jobs.product_partition_job.convert_assets_to_cog", fake_convert_assets_to_cog)
    monkeypatch.setattr("cube_split.jobs.product_partition_job.build_grid_tasks_driver", fake_build_grid_tasks_driver)
    monkeypatch.setattr("cube_split.jobs.product_partition_job._process_local_task_group", fake_process_local_task_group)

    result = run_product_partition(
        Namespace(
            input_dir=str(input_dir),
            output_dir=str(output_dir),
            cog_input_dir=str(cog_dir),
            target_crs="EPSG:4326",
            grid_type="geohash",
            grid_level=5,
            cover_mode="intersect",
            max_cells_per_asset=20000,
            partition_prefix_len=3,
            cog_overwrite=True,
            cog_workers=1,
            partition_workers=1,
            partition_backend="thread",
            ray_address="",
            ray_parallelism=0,
            chunk_size=0,
            sample_mean=False,
        )
    )

    assert result["status"] == "completed"
    assert captured["predictor"] == 0


def test_product_partition_dispatches_ray_backend(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    cog_dir = tmp_path / "cog"
    input_dir.mkdir()
    tif = input_dir / "1980-2020年滇中地区30米生态安全评价数据集（第一版）_1980年.tif"
    _write_product_tif(tif)
    captured = {}

    def fake_convert_assets_to_cog(assets, **kwargs):
        return assets

    def fake_build_grid_tasks_driver(**kwargs):
        return [_product_row(tif, 1980)]

    def fake_partition_groups_ray(
        task_chunks,
        parallelism,
        ray_address,
        include_sample_mean,
        assets_by_path,
        cog_input_dir,
        cog_overwrite,
        cog_options,
        target_crs,
        source_options,
        cog_upload_options,
    ):
        captured["task_chunk_count"] = len(task_chunks)
        captured["parallelism"] = parallelism
        captured["ray_address"] = ray_address
        captured["include_sample_mean"] = include_sample_mean
        captured["assets_by_path"] = assets_by_path
        captured["cog_input_dir"] = cog_input_dir
        captured["cog_overwrite"] = cog_overwrite
        captured["cog_options"] = cog_options
        captured["target_crs"] = target_crs
        captured["source_options"] = source_options
        captured["cog_upload_options"] = cog_upload_options
        return [_product_row(tif, 1980)], 0.25

    monkeypatch.setattr("cube_split.jobs.product_partition_job.convert_assets_to_cog", fake_convert_assets_to_cog)
    monkeypatch.setattr("cube_split.jobs.product_partition_job.build_grid_tasks_driver", fake_build_grid_tasks_driver)
    monkeypatch.setattr("cube_split.jobs.product_partition_job._partition_groups_ray", fake_partition_groups_ray)

    result = run_product_partition(
        Namespace(
            input_dir=str(input_dir),
            manifest_path="",
            output_dir=str(output_dir),
            cog_input_dir=str(cog_dir),
            target_crs="EPSG:4326",
            grid_type="geohash",
            grid_level=5,
            cover_mode="intersect",
            max_cells_per_asset=20000,
            partition_prefix_len=3,
            cog_overwrite=True,
            cog_workers=1,
            partition_workers=1,
            partition_backend="ray",
            ray_address="10.3.100.182:6379",
            ray_parallelism=4,
            chunk_size=1,
            sample_mean=True,
            metadata_backend="none",
            asset_storage_backend="local",
            minio_endpoint="10.3.100.179:9000",
            minio_access_key="access",
            minio_secret_key="secret",
            minio_bucket="cube",
            minio_prefix="cube/product",
            minio_secure=False,
            dataset="dianzhong_ecological_security",
            asset_version="v1",
        )
    )

    assert result["execution_engine"] == "ray"
    assert result["partition_backend_used"] == "ray"
    assert result["ray_parallelism"] == 1
    assert result["ray_address"] == "10.3.100.182:6379"
    assert result["ray_init_elapsed_sec"] == 0.25
    assert captured["parallelism"] == 1
    assert captured["include_sample_mean"] is True
    assert str(tif.resolve()) in captured["assets_by_path"]
    assert captured["cog_input_dir"] == str(cog_dir)
    assert captured["cog_overwrite"] is True
    assert "PREDICTOR" not in captured["cog_options"]
    assert captured["target_crs"] == "EPSG:4326"
    assert captured["source_options"]["endpoint"] == "10.3.100.179:9000"
    assert captured["source_options"]["access_key"] == "access"
    assert captured["cog_upload_options"]["bucket"] == "cube"
    assert captured["cog_upload_options"]["prefix"] == "cube/product"
    assert captured["cog_upload_options"]["dataset"] == "dianzhong_ecological_security"


def test_product_partition_runs_ingest_after_rows_are_written(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    cog_dir = tmp_path / "cog"
    input_dir.mkdir()
    tif = input_dir / "1980-2020年滇中地区30米生态安全评价数据集（第一版）_1980年.tif"
    _write_product_tif(tif)
    captured = {}

    def fake_convert_assets_to_cog(assets, **kwargs):
        return assets

    def fake_build_grid_tasks_driver(**kwargs):
        return [_product_row(tif, 1980)]

    def fake_process_local_task_group(group, time_granularity, include_sample_mean=False):
        return group

    def fake_run_product_ingest(args):
        captured["run_dir"] = args.run_dir
        captured["job_id"] = args.job_id
        captured["metadata_backend"] = args.metadata_backend
        captured["asset_storage_backend"] = args.asset_storage_backend
        captured["minio_bucket"] = args.minio_bucket
        assert (Path(args.run_dir) / "index_rows.jsonl").exists()
        return {
            "input_rows": 1,
            "product_asset_rows": 1,
            "product_fact_rows": 1,
            "metadata_backend": args.metadata_backend,
            "asset_storage_backend": args.asset_storage_backend,
        }

    monkeypatch.setattr("cube_split.jobs.product_partition_job.convert_assets_to_cog", fake_convert_assets_to_cog)
    monkeypatch.setattr("cube_split.jobs.product_partition_job.build_grid_tasks_driver", fake_build_grid_tasks_driver)
    monkeypatch.setattr("cube_split.jobs.product_partition_job._process_local_task_group", fake_process_local_task_group)
    monkeypatch.setattr("cube_split.ingest.product_ingest_job.run_product_ingest", fake_run_product_ingest)

    result = run_product_partition(
        Namespace(
            input_dir=str(input_dir),
            manifest_path="",
            output_dir=str(output_dir),
            cog_input_dir=str(cog_dir),
            target_crs="EPSG:4326",
            grid_type="geohash",
            grid_level=5,
            cover_mode="intersect",
            max_cells_per_asset=20000,
            partition_prefix_len=3,
            cog_overwrite=True,
            cog_workers=1,
            partition_workers=1,
            partition_backend="thread",
            ray_address="",
            ray_parallelism=0,
            chunk_size=0,
            sample_mean=False,
            job_id="",
            dataset="dianzhong_ecological_security",
            product_name="滇中地区30米生态安全评价数据集",
            asset_version="v1",
            cube_version="product_v1",
            metadata_backend="postgres",
            postgres_dsn="postgresql://postgres:postgres@127.0.0.1:5432/cube",
            db_path="",
            asset_storage_backend="minio",
            minio_endpoint="10.3.100.179:9000",
            minio_access_key="access",
            minio_secret_key="secret",
            minio_bucket="cube",
            minio_prefix="cube/product",
            minio_secure=False,
            minio_upload_workers=2,
            cog_output_root=str(tmp_path / "product_cog_store"),
            cog_materialize_mode="copy",
        )
    )

    assert result["ingest_enabled"] is True
    assert result["ingest_stats"]["input_rows"] == 1
    assert captured["job_id"] == Path(captured["run_dir"]).name
    assert captured["asset_storage_backend"] == "minio"
    assert captured["minio_bucket"] == "cube"


def test_product_quality_passes_complete_years(tmp_path: Path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    rows = []
    for year in (1980, 1990, 2000, 2010, 2020):
        asset = tmp_path / f"product_{year}_cog.tif"
        _write_product_tif(asset)
        rows.append(_product_row(asset, year=year))
    (run_dir / "index_rows.jsonl").write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )

    report = run_quality_check(Namespace(run_dir=str(run_dir), target_crs="EPSG:4326"))

    assert report["status"] == "PASS"
    assert report["summary"]["index_rows"] == 5
    assert report["summary"]["product_years"] == [1980, 1990, 2000, 2010, 2020]
    report_path = run_dir / "quality_report.json"
    assert report["report_path"] == str(report_path.resolve())
    stored_report = json.loads(report_path.read_text(encoding="utf-8"))
    assert stored_report["data_type"] == "product"
    assert stored_report["summary"]["product_years"] == [1980, 1990, 2000, 2010, 2020]


def test_product_quality_passes_selected_single_year_by_default(tmp_path: Path):
    asset = tmp_path / "product_1980_cog.tif"
    _write_product_tif(asset)
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "index_rows.jsonl").write_text(json.dumps(_product_row(asset, 1980)) + "\n", encoding="utf-8")

    report = run_quality_check(Namespace(run_dir=str(run_dir), target_crs="EPSG:4326"))

    assert report["status"] == "PASS"
    year_check = next(check for check in report["checks"] if check["name"] == "product_years")
    assert year_check["status"] == "PASS"
    assert year_check["metrics"]["present_years"] == [1980]


def test_product_quality_warns_missing_explicit_expected_year(tmp_path: Path):
    asset = tmp_path / "product_1980_cog.tif"
    _write_product_tif(asset)
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "index_rows.jsonl").write_text(json.dumps(_product_row(asset, 1980)) + "\n", encoding="utf-8")

    report = run_quality_check(Namespace(run_dir=str(run_dir), target_crs="EPSG:4326", expected_years="1980,1990,2000,2010,2020"))

    assert report["status"] == "WARN"
    year_check = next(check for check in report["checks"] if check["name"] == "product_years")
    assert year_check["metrics"]["missing_years"] == [1990, 2000, 2010, 2020]


def test_run_product_ingest_creates_product_tables(tmp_path: Path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    asset = tmp_path / "product_1980_cog.tif"
    _write_product_tif(asset)
    (run_dir / "index_rows.jsonl").write_text(json.dumps(_product_row(asset, 1980)) + "\n", encoding="utf-8")
    db_path = tmp_path / "product_ingest.db"

    stats = run_product_ingest(
        Namespace(
            run_dir=str(run_dir),
            job_id="product-job-1",
            dataset="dianzhong_ecological_security",
            product_name="滇中地区30米生态安全评价数据集",
            asset_version="v1",
            cube_version="product_v1",
            metadata_backend="sqlite",
            db_path=str(db_path),
            asset_storage_backend="local",
            cog_output_root=str(tmp_path / "product_cog_store"),
            cog_materialize_mode="copy",
            postgres_dsn="",
            minio_endpoint="",
            minio_access_key="",
            minio_secret_key="",
            minio_bucket="",
            minio_prefix="cube/product",
            minio_secure=False,
            minio_upload_workers=1,
        )
    )

    assert stats["input_rows"] == 1
    assert stats["product_asset_rows"] == 1
    assert stats["product_fact_rows"] == 1

    conn = sqlite3.connect(str(db_path))
    try:
        assets = conn.execute("SELECT dataset, product_name, product_year FROM rs_product_asset").fetchall()
        facts = conn.execute("SELECT product_year, product_band, value_ref_uri FROM rs_product_cell_fact").fetchall()
    finally:
        conn.close()

    assert assets == [("dianzhong_ecological_security", "滇中地区30米生态安全评价数据集", 1980)]
    assert facts[0][0] == 1980
    assert facts[0][1] == "product_value"
    assert "#window=" in facts[0][2]
