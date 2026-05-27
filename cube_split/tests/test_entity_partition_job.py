import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import rasterio
from rasterio.transform import from_origin

from cube_split.jobs.entity_partition_job import run_entity_partition


def _write_tif(path: Path) -> None:
    transform = from_origin(116.0, 40.0, 0.001, 0.001)
    data = np.ones((1, 32, 32), dtype=np.uint8)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=32,
        height=32,
        count=1,
        dtype=data.dtype,
        crs="EPSG:4326",
        transform=transform,
    ) as ds:
        ds.write(data)


def test_entity_partition_writes_one_hex_file_per_band(tmp_path: Path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    source = input_dir / "demo_scene_blue.tif"
    _write_tif(source)

    report = run_entity_partition(
        SimpleNamespace(
            input_dir=str(input_dir),
            manifest_path="",
            product_family="auto",
            output_dir=str(tmp_path / "output"),
            cog_input_dir=str(tmp_path / "cog"),
            cog_overwrite=True,
            cog_workers=1,
            cog_compress="LZW",
            cog_predictor=2,
            cog_level=0,
            cog_num_threads="ALL_CPUS",
            target_crs="EPSG:4326",
            grid_level=0,
            target_pixels_per_hex_edge=768,
            cover_mode="intersect",
            time_granularity="day",
            max_cells_per_asset=20000,
            partition_prefix_len=3,
            partition_backend="thread",
        )
    )

    run_dir = Path(report["run_dir"])
    entity_rows_path = run_dir / "entity_index_rows.jsonl"
    rows = [json.loads(line) for line in entity_rows_path.read_text(encoding="utf-8").splitlines()]

    assert report["partition_type"] == "entity"
    assert report["grid_type"] == "isea4h"
    assert report["requested_grid_level"] is None
    assert report["grid_level"] == report["inferred_grid_level"]
    assert report["entity_tile_count"] == len(rows) >= 1
    assert (run_dir / "index_rows.jsonl").exists()

    row = rows[0]
    tile_path = Path(row["output_path"])
    assert row["partition_type"] == "entity"
    assert row["asset_path"] == str(tile_path)
    assert row["source_asset_path"] == str(source.resolve())
    assert row["space_code"]
    assert row["st_code"].startswith("hx:")
    assert tile_path.exists()
    with rasterio.open(tile_path) as ds:
        assert ds.count == 1
        assert ds.nodata == 0
        assert ds.width > 0
        assert ds.height > 0
