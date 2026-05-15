import json
import sqlite3
from argparse import Namespace
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_origin

from cube_split.ingest.product_ingest_job import run_product_ingest
from cube_split.jobs.product_partition_job import _prepare_product_task_rows
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
        "st_code": f"gh:5:wm6n0:{year}:v1",
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


def test_product_quality_warns_missing_expected_year(tmp_path: Path):
    asset = tmp_path / "product_1980_cog.tif"
    _write_product_tif(asset)
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "index_rows.jsonl").write_text(json.dumps(_product_row(asset, 1980)) + "\n", encoding="utf-8")

    report = run_quality_check(Namespace(run_dir=str(run_dir), target_crs="EPSG:4326"))

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
