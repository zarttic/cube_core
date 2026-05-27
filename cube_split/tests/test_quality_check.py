import json
from argparse import Namespace
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_origin

from cube_split.quality.optical_quality import run_quality_check
from cube_split.quality.carbon_quality import run_quality_check as run_carbon_quality_check


def _write_tif(path: Path, *, crs: str = "EPSG:4326") -> None:
    data = np.arange(100, dtype=np.int16).reshape(1, 10, 10)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=10,
        height=10,
        count=1,
        dtype=data.dtype,
        crs=crs,
        transform=from_origin(116.0, 40.0, 0.01, 0.01),
    ) as ds:
        ds.write(data)


def test_quality_check_passes_valid_partition_rows(tmp_path: Path):
    asset = tmp_path / "asset_cog.tif"
    _write_tif(asset)
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    row = {
        "scene_id": "scene-1",
        "band": "sr_band2",
        "asset_path": str(asset),
        "acq_time": "2020-07-01T00:00:00Z",
        "grid_type": "geohash",
        "grid_level": 5,
        "space_code": "wx4g0",
        "st_code": "gh:5:wx4g0:20200701:v1",
        "time_bucket": "20200701",
        "cell_min_lon": 116.0,
        "cell_min_lat": 39.9,
        "cell_max_lon": 116.1,
        "cell_max_lat": 40.0,
        "window_col_off": 1,
        "window_row_off": 2,
        "window_width": 4,
        "window_height": 5,
    }
    (run_dir / "index_rows.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")

    report = run_quality_check(Namespace(run_dir=str(run_dir), target_crs="EPSG:4326"))

    assert report["status"] == "PASS"
    assert report["summary"]["index_rows"] == 1
    assert report["summary"]["asset_count"] == 1
    assert report["summary"]["failed_checks"] == 0


def test_quality_check_allows_entity_tiles_for_same_scene_band(tmp_path: Path):
    asset_a = tmp_path / "tile_a.tif"
    asset_b = tmp_path / "tile_b.tif"
    _write_tif(asset_a)
    _write_tif(asset_b, crs="EPSG:3857")
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    base_row = {
        "partition_type": "entity",
        "scene_id": "scene-1",
        "band": "sr_band2",
        "acq_time": "2020-07-01T00:00:00Z",
        "grid_type": "isea4h",
        "grid_level": 5,
        "time_bucket": "20200701",
        "cell_min_lon": 116.0,
        "cell_min_lat": 39.9,
        "cell_max_lon": 116.1,
        "cell_max_lat": 40.0,
        "window_col_off": 0,
        "window_row_off": 0,
        "window_width": 10,
        "window_height": 10,
    }
    rows = [
        {
            **base_row,
            "asset_path": str(asset_a),
            "space_code": "85283473fffffff",
            "st_code": "hx:5:85283473fffffff:20200701:v1",
        },
        {
            **base_row,
            "asset_path": str(asset_b),
            "space_code": "85283477fffffff",
            "st_code": "hx:5:85283477fffffff:20200701:v1",
        },
    ]
    (run_dir / "index_rows.jsonl").write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )

    report = run_quality_check(Namespace(run_dir=str(run_dir), target_crs="EPSG:4326"))

    duplicate_check = next(check for check in report["checks"] if check["name"] == "logical_duplicates")
    assert duplicate_check["status"] == "PASS"
    assert report["summary"]["index_rows"] == 2


def test_quality_check_fails_invalid_window_and_crs(tmp_path: Path):
    asset = tmp_path / "asset_cog.tif"
    _write_tif(asset, crs="EPSG:3857")
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    row = {
        "scene_id": "scene-1",
        "band": "sr_band2",
        "asset_path": str(asset),
        "acq_time": "2020-07-01T00:00:00Z",
        "grid_type": "geohash",
        "grid_level": 5,
        "space_code": "wx4g0",
        "st_code": "gh:5:wx4g0:20200701:v1",
        "time_bucket": "20200701",
        "cell_min_lon": 116.0,
        "cell_min_lat": 39.9,
        "cell_max_lon": 116.1,
        "cell_max_lat": 40.0,
        "window_col_off": 8,
        "window_row_off": 8,
        "window_width": 4,
        "window_height": 5,
    }
    (run_dir / "index_rows.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")

    report = run_quality_check(Namespace(run_dir=str(run_dir), target_crs="EPSG:4326"))

    assert report["status"] == "FAIL"
    failed_names = {check["name"] for check in report["checks"] if check["status"] == "FAIL"}
    assert "cog_crs" in failed_names
    assert "window_bounds" in failed_names


def test_carbon_quality_check_passes_valid_observation_rows(tmp_path: Path):
    run_dir = tmp_path / "carbon_run"
    run_dir.mkdir()
    row = {
        "data_type": "carbon_satellite",
        "satellite": "OCO2",
        "product_type": "xco2",
        "observation_id": "2020123100010671",
        "acq_time": "2020-12-31T00:01:06.700Z",
        "time_bucket": "20201231",
        "grid_type": "isea4h",
        "grid_level": 5,
        "space_code": "R5-12345",
        "st_code": "isea4h:5:R5-12345:20201231:v1",
        "xco2": 417.384,
        "quality_flag": "1",
        "center_lon": -167.413,
        "center_lat": 41.1686,
        "source_uri": "s3://cube/carbon/raw/oco2.nc4",
    }
    (run_dir / "carbon_observation_rows.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")

    report = run_carbon_quality_check(Namespace(run_dir=str(run_dir), target_crs="EPSG:4326"))

    assert report["status"] == "PASS"
    assert report["data_type"] == "carbon"
    assert report["summary"]["observation_rows"] == 1
    assert report["summary"]["quality_counts"] == {"1": 1}
    assert report["summary"]["avg_xco2"] == 417.384


def test_carbon_quality_check_fails_invalid_schema_coordinates_and_xco2(tmp_path: Path):
    run_dir = tmp_path / "carbon_run"
    run_dir.mkdir()
    row = {
        "data_type": "carbon_satellite",
        "satellite": "OCO2",
        "product_type": "xco2",
        "observation_id": "bad-observation",
        "acq_time": "2020-12-31T00:01:06.700Z",
        "time_bucket": "20201230",
        "grid_type": "isea4h",
        "grid_level": 5,
        "space_code": "R5-12345",
        "st_code": "isea4h:5:R5-12345:20201231:v1",
        "xco2": 999.0,
        "quality_flag": "7",
        "center_lon": 181.0,
        "center_lat": 91.0,
    }
    (run_dir / "carbon_observation_rows.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")

    report = run_carbon_quality_check(Namespace(run_dir=str(run_dir), target_crs="EPSG:4326"))

    assert report["status"] == "FAIL"
    failed_names = {check["name"] for check in report["checks"] if check["status"] == "FAIL"}
    warn_names = {check["name"] for check in report["checks"] if check["status"] == "WARN"}
    assert {"time_bucket", "carbon_coordinates", "xco2_range"} <= failed_names
    assert "carbon_quality_flag" in warn_names
