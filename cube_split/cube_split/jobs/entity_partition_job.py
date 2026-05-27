#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import h3
import numpy as np
import rasterio
import rasterio.mask
from pyproj import Geod
from rasterio.warp import transform_geom

from grid_core.sdk import CubeEncoderSDK
from cube_split.jobs.ray_partition_core import (
    AssetRecord,
    _dataset_bounds_wgs84,
    build_grid_tasks_driver,
    build_manifest,
)


DEFAULT_TARGET_PIXELS_PER_HEX_EDGE = 768


def infer_isea4h_level_for_assets(
    assets: list[AssetRecord],
    target_pixels_per_hex_edge: int = DEFAULT_TARGET_PIXELS_PER_HEX_EDGE,
) -> int:
    if target_pixels_per_hex_edge <= 0:
        raise ValueError("target_pixels_per_hex_edge must be greater than 0")
    pixel_size_m = max(_asset_pixel_size_m(Path(asset.path)) for asset in assets)
    target_edge_m = pixel_size_m * target_pixels_per_hex_edge
    selected = 1
    for level in range(1, 13):
        if h3.average_hexagon_edge_length(level, unit="m") >= target_edge_m:
            selected = level
    return selected


def _asset_pixel_size_m(path: Path) -> float:
    geod = Geod(ellps="WGS84")
    with rasterio.open(path) as ds:
        res_x = abs(float(ds.transform.a))
        res_y = abs(float(ds.transform.e))
        if ds.crs and ds.crs.is_projected:
            return max(res_x, res_y)

        min_lon, min_lat, max_lon, max_lat = _dataset_bounds_wgs84(ds)
        center_lon = (min_lon + max_lon) / 2.0
        center_lat = (min_lat + max_lat) / 2.0
        _, _, x_m = geod.inv(center_lon, center_lat, center_lon + res_x, center_lat)
        _, _, y_m = geod.inv(center_lon, center_lat, center_lon, center_lat + res_y)
        pixel_size = max(abs(x_m), abs(y_m))
        if not math.isfinite(pixel_size) or pixel_size <= 0:
            raise ValueError(f"Cannot infer pixel size for asset: {path}")
        return pixel_size


def _time_bucket(acq_time: str, granularity: str) -> str:
    time_format = {
        "year": "%Y",
        "month": "%Y%m",
        "day": "%Y%m%d",
        "hour": "%Y%m%d%H",
        "minute": "%Y%m%d%H%M",
    }[granularity]
    return datetime.fromisoformat(acq_time.replace("Z", "+00:00")).strftime(time_format)


def _ensure_center_cell_tasks(
    assets: list[AssetRecord],
    tasks: list[dict[str, Any]],
    grid_level: int,
    cover_mode: str,
) -> list[dict[str, Any]]:
    task_keys = {(row["scene_id"], row["band"], row["asset_path"]) for row in tasks}
    missing_assets = [
        asset
        for asset in assets
        if (asset.scene_id, asset.band, asset.path) not in task_keys
    ]
    if not missing_assets:
        return tasks

    sdk = CubeEncoderSDK()
    out = list(tasks)
    for asset in missing_assets:
        if asset.bbox is not None:
            min_lon, min_lat, max_lon, max_lat = map(float, asset.bbox)
        else:
            with rasterio.open(asset.path) as ds:
                min_lon, min_lat, max_lon, max_lat = _dataset_bounds_wgs84(ds)
        center = [(min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0]
        cell = sdk.locate(grid_type="isea4h", level=grid_level, point=center)
        out.append(
            {
                "scene_id": asset.scene_id,
                "band": asset.band,
                "asset_path": asset.path,
                "acq_time": asset.acq_time,
                "grid_type": "isea4h",
                "grid_level": grid_level,
                "space_code": cell.space_code,
                "cell_min_lon": float(cell.bbox[0]),
                "cell_min_lat": float(cell.bbox[1]),
                "cell_max_lon": float(cell.bbox[2]),
                "cell_max_lat": float(cell.bbox[3]),
                "cover_mode": cover_mode,
            }
        )
    return out


def _safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in value)


def _hex_geometry_for_dataset(sdk: CubeEncoderSDK, ds: rasterio.DatasetReader, space_code: str) -> dict[str, Any]:
    geom = sdk.code_to_geometry(grid_type="isea4h", code=space_code)
    if ds.crs and ds.crs.to_string().upper() != "EPSG:4326":
        return transform_geom("EPSG:4326", ds.crs, geom)
    return geom


def _nodata_value(ds: rasterio.DatasetReader, band_index: int) -> float | int:
    nodata = ds.nodatavals[band_index - 1] if ds.nodatavals else ds.nodata
    if nodata is not None:
        return nodata
    dtype = np.dtype(ds.dtypes[band_index - 1])
    if np.issubdtype(dtype, np.floating):
        return -9999.0
    return 0


def _write_entity_tiles(
    tasks: list[dict[str, Any]],
    run_dir: Path,
    time_granularity: str,
    partition_prefix_len: int,
) -> list[dict[str, Any]]:
    sdk = CubeEncoderSDK()
    st_cache: dict[str, str] = {}
    rows: list[dict[str, Any]] = []
    task_groups: dict[str, list[dict[str, Any]]] = {}
    for task in tasks:
        task_groups.setdefault(str(task["asset_path"]), []).append(task)

    for asset_path, asset_tasks in sorted(task_groups.items()):
        with rasterio.open(asset_path) as ds:
            for task in sorted(asset_tasks, key=lambda row: str(row["space_code"])):
                geom = _hex_geometry_for_dataset(sdk, ds, str(task["space_code"]))
                for band_index in range(1, ds.count + 1):
                    band_name = str(task["band"]) if ds.count == 1 else f"{task['band']}_band{band_index}"
                    nodata = _nodata_value(ds, band_index)
                    try:
                        data, out_transform = rasterio.mask.mask(
                            ds,
                            [geom],
                            crop=True,
                            filled=False,
                            indexes=band_index,
                        )
                    except ValueError:
                        continue
                    if data.size == 0:
                        continue

                    valid_mask = ~np.ma.getmaskarray(data)
                    valid_pixels = int(valid_mask.sum())
                    total_pixels = int(data.size)
                    if valid_pixels == 0 or total_pixels == 0:
                        continue

                    filled = np.ma.filled(data, nodata)
                    tile_dir = (
                        run_dir
                        / "entity_tiles"
                        / "optical"
                        / _safe_name(str(task["scene_id"]))
                        / "isea4h"
                        / f"L{int(task['grid_level'])}"
                        / _safe_name(str(task["space_code"]))
                    )
                    tile_dir.mkdir(parents=True, exist_ok=True)
                    tile_path = tile_dir / f"{_safe_name(band_name)}.tif"
                    profile = ds.profile.copy()
                    profile.update(
                        driver="GTiff",
                        height=int(filled.shape[-2]),
                        width=int(filled.shape[-1]),
                        count=1,
                        transform=out_transform,
                        nodata=nodata,
                    )
                    with rasterio.open(tile_path, "w", **profile) as out_ds:
                        out_ds.write(filled, 1)

                    acq_time = str(task["acq_time"])
                    st_key = "|".join(
                        [
                            "isea4h",
                            str(int(task["grid_level"])),
                            str(task["space_code"]),
                            acq_time,
                            time_granularity,
                        ]
                    )
                    if st_key not in st_cache:
                        st_cache[st_key] = sdk.generate_st_code(
                            grid_type="isea4h",
                            level=int(task["grid_level"]),
                            space_code=str(task["space_code"]),
                            timestamp=datetime.fromisoformat(acq_time.replace("Z", "+00:00")),
                            time_granularity=time_granularity,
                            version="v1",
                        ).st_code

                    rows.append(
                        {
                            "partition_type": "entity",
                            "scene_id": task["scene_id"],
                            "band": band_name,
                            "asset_path": str(tile_path.resolve()),
                            "source_asset_path": asset_path,
                            "output_path": str(tile_path.resolve()),
                            "acq_time": acq_time,
                            "grid_type": "isea4h",
                            "grid_level": int(task["grid_level"]),
                            "space_code": task["space_code"],
                            "space_code_prefix": str(task["space_code"])[: max(1, int(partition_prefix_len))],
                            "st_code": st_cache[st_key],
                            "time_bucket": _time_bucket(acq_time, time_granularity),
                            "cover_mode": task["cover_mode"],
                            "cell_min_lon": float(task["cell_min_lon"]),
                            "cell_min_lat": float(task["cell_min_lat"]),
                            "cell_max_lon": float(task["cell_max_lon"]),
                            "cell_max_lat": float(task["cell_max_lat"]),
                            "window_col_off": 0,
                            "window_row_off": 0,
                            "window_width": int(filled.shape[-1]),
                            "window_height": int(filled.shape[-2]),
                            "nodata": nodata,
                            "valid_pixel_ratio": round(valid_pixels / total_pixels, 6),
                        }
                    )
    return rows


def run_entity_partition(args: argparse.Namespace) -> dict[str, Any]:
    total_start = time.perf_counter()
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    source_assets = build_manifest(
        input_dir,
        product_family=args.product_family,
        manifest_path=(Path(args.manifest_path) if args.manifest_path else None),
    )
    if not source_assets:
        raise RuntimeError(f"No .TIF assets found under: {input_dir}")

    assets = source_assets

    requested_level = int(getattr(args, "grid_level", 0) or 0)
    target_pixels = int(getattr(args, "target_pixels_per_hex_edge", DEFAULT_TARGET_PIXELS_PER_HEX_EDGE) or DEFAULT_TARGET_PIXELS_PER_HEX_EDGE)
    inferred_level = infer_isea4h_level_for_assets(assets, target_pixels)
    grid_level = requested_level if requested_level > 0 else inferred_level

    grid_tasks = build_grid_tasks_driver(
        assets=assets,
        grid_type="isea4h",
        grid_level=grid_level,
        cover_mode=args.cover_mode,
        max_cells_per_asset=args.max_cells_per_asset,
    )
    grid_tasks = _ensure_center_cell_tasks(assets, grid_tasks, grid_level, args.cover_mode)
    if len(grid_tasks) > int(args.max_cells_per_asset) * len(assets):
        raise RuntimeError("Cover cells exceed max limit for entity partition")

    output_dir.mkdir(parents=True, exist_ok=True)
    run_dir = output_dir / time.strftime("run_%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)
    rows = _write_entity_tiles(
        grid_tasks,
        run_dir=run_dir,
        time_granularity=args.time_granularity,
        partition_prefix_len=args.partition_prefix_len,
    )

    entity_rows_path = run_dir / "entity_index_rows.jsonl"
    index_rows_path = run_dir / "index_rows.jsonl"
    for path in (entity_rows_path, index_rows_path):
        with path.open("w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    rows_by_band: dict[str, int] = {}
    for row in rows:
        rows_by_band[row["band"]] = rows_by_band.get(row["band"], 0) + 1

    report = {
        "run_dir": str(run_dir.resolve()),
        "input_dir": str(input_dir.resolve()),
        "cog_input_dir": "",
        "rows_path": str(entity_rows_path.resolve()),
        "index_rows_path": str(index_rows_path.resolve()),
        "source_asset_count": len(source_assets),
        "asset_count": len(assets),
        "product_family": args.product_family,
        "partition_type": "entity",
        "grid_task_count": len(grid_tasks),
        "grid_type": "isea4h",
        "grid_level": grid_level,
        "inferred_grid_level": inferred_level,
        "requested_grid_level": requested_level or None,
        "target_pixels_per_hex_edge": target_pixels,
        "cover_mode": args.cover_mode,
        "execution_engine": "local",
        "partition_backend_requested": getattr(args, "partition_backend", "local"),
        "partition_backend_used": "local",
        "time_granularity": args.time_granularity,
        "partition_prefix_len": max(1, int(args.partition_prefix_len)),
        "total_index_rows": len(rows),
        "entity_tile_count": len(rows),
        "distinct_space_codes": len({row["space_code"] for row in rows}),
        "distinct_st_codes": len({row["st_code"] for row in rows}),
        "rows_by_band": rows_by_band,
        "ray_parallelism": 0,
        "ray_address": "",
        "cog_elapsed_sec": 0.0,
        "partition_elapsed_sec": round(time.perf_counter() - total_start, 3),
        "total_elapsed_sec": round(time.perf_counter() - total_start, 3),
    }
    (run_dir / "job_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local entity partition job for ISEA4H optical raster tiles")
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--manifest-path", default="")
    parser.add_argument("--product-family", default="auto")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--cog-input-dir", required=True)
    parser.add_argument("--cog-overwrite", action="store_true")
    parser.add_argument("--cog-workers", type=int, default=0)
    parser.add_argument("--cog-compress", default="LZW")
    parser.add_argument("--cog-predictor", type=int, default=2)
    parser.add_argument("--cog-level", type=int, default=0)
    parser.add_argument("--cog-num-threads", default="ALL_CPUS")
    parser.add_argument("--target-crs", default="EPSG:4326")
    parser.add_argument("--grid-level", type=int, default=0)
    parser.add_argument("--target-pixels-per-hex-edge", type=int, default=DEFAULT_TARGET_PIXELS_PER_HEX_EDGE)
    parser.add_argument("--cover-mode", default="intersect", choices=["intersect", "contain", "minimal"])
    parser.add_argument("--time-granularity", default="day", choices=["year", "month", "day", "hour", "minute"])
    parser.add_argument("--max-cells-per-asset", type=int, default=20000)
    parser.add_argument("--partition-prefix-len", type=int, default=3)
    return parser.parse_args()


def main() -> None:
    report = run_entity_partition(parse_args())
    print("=== Entity partition job completed ===")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
