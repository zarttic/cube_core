import json
from argparse import Namespace
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_origin

from cube_split.quality.optical_quality import run_quality_check


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
