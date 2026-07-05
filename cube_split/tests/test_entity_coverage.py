from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import rasterio
from rasterio.transform import from_origin

import cube_split.jobs.entity_partition_job as entity_partition_job


def _write_tif(path: Path) -> None:
    transform = from_origin(116.0, 40.0, 0.001, 0.001)
    data = np.ones((1, 8, 8), dtype=np.uint8)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=8,
        height=8,
        count=1,
        dtype=data.dtype,
        crs="EPSG:4326",
        transform=transform,
    ) as ds:
        ds.write(data)


def _entity_args(tmp_path: Path, **overrides) -> SimpleNamespace:
    values = {
        "input_dir": str(tmp_path / "input"),
        "manifest_path": "",
        "product_family": "auto",
        "data_type": "optical",
        "output_dir": str(tmp_path / "output"),
        "cog_input_dir": str(tmp_path / "cog"),
        "cog_overwrite": True,
        "cog_workers": 1,
        "cog_compress": "LZW",
        "cog_predictor": 2,
        "cog_level": 0,
        "cog_num_threads": "ALL_CPUS",
        "target_crs": "EPSG:4326",
        "grid_type": "isea4h",
        "grid_level": 2,
        "entity_clip_mode": "exact",
        "target_pixels_per_hex_edge": 768,
        "cover_mode": "intersect",
        "time_granularity": "day",
        "max_cells_per_asset": 7,
        "partition_prefix_len": 3,
        "partition_backend": "thread",
        "ray_address": "",
        "ray_parallelism": 0,
        "chunk_size": 0,
        "asset_storage_backend": "local",
        "metadata_backend": "none",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_entity_partition_passes_max_cells_per_asset_to_grid_builder(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    _write_tif(input_dir / "scene.tif")
    captured: dict[str, object] = {}

    def fake_build_grid_tasks_driver(**kwargs):
        captured["max_cells_per_asset"] = kwargs["max_cells_per_asset"]
        asset = kwargs["assets"][0]
        return [
            {
                "scene_id": asset.scene_id,
                "band": asset.band,
                "asset_path": asset.path,
                "acq_time": asset.acq_time,
                "grid_type": kwargs["grid_type"],
                "grid_level": kwargs["grid_level"],
                "space_code": "82754ffffffffff",
                "cover_mode": kwargs["cover_mode"],
                "cell_min_lon": 115.0,
                "cell_min_lat": 39.0,
                "cell_max_lon": 117.0,
                "cell_max_lat": 41.0,
            }
        ]

    def fake_write_entity_tile_chunks_thread(task_chunks, run_dir, time_granularity, partition_prefix_len, workers, **kwargs):
        task = task_chunks[0][0][0]
        return [
            {
                "partition_type": "entity",
                "data_type": kwargs["data_type"],
                "scene_id": task["scene_id"],
                "band": task["band"],
                "asset_path": str(run_dir / "tile.tif"),
                "source_asset_path": task["asset_path"],
                "output_path": str(run_dir / "tile.tif"),
                "acq_time": task["acq_time"],
                "grid_type": task["grid_type"],
                "grid_level": task["grid_level"],
                "space_code": task["space_code"],
                "space_code_prefix": task["space_code"][:partition_prefix_len],
                "st_code": f"hx:{task['grid_level']}:{task['space_code']}:20260530",
                "time_bucket": "20260530",
                "cover_mode": task["cover_mode"],
                "cell_min_lon": task["cell_min_lon"],
                "cell_min_lat": task["cell_min_lat"],
                "cell_max_lon": task["cell_max_lon"],
                "cell_max_lat": task["cell_max_lat"],
                "window_col_off": 0,
                "window_row_off": 0,
                "window_width": 1,
                "window_height": 1,
                "nodata": 0,
                "valid_pixel_ratio": 1.0,
            }
        ]

    monkeypatch.setattr(entity_partition_job, "build_grid_tasks_driver", fake_build_grid_tasks_driver)
    monkeypatch.setattr(entity_partition_job, "_write_entity_tile_chunks_thread", fake_write_entity_tile_chunks_thread)

    report = entity_partition_job.run_entity_partition(_entity_args(tmp_path, max_cells_per_asset=7))

    rows = [json.loads(line) for line in Path(report["rows_path"]).read_text(encoding="utf-8").splitlines()]
    assert captured["max_cells_per_asset"] == 7
    assert report["grid_task_count"] == 1
    assert report["entity_tile_count"] == 1
    assert rows[0]["partition_type"] == "entity"


def test_entity_partition_rejects_negative_max_cells_per_asset(tmp_path: Path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    _write_tif(input_dir / "scene.tif")

    try:
        entity_partition_job.run_entity_partition(_entity_args(tmp_path, max_cells_per_asset=-1))
    except ValueError as exc:
        assert "max_cells_per_asset" in str(exc)
    else:
        raise AssertionError("negative max_cells_per_asset should fail")
