#!/usr/bin/env python3
from __future__ import annotations

import os
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterator, Optional

import rasterio
from pyproj import Transformer
from rasterio.shutil import copy as rio_copy
from rasterio.windows import Window

from grid_core.sdk import CubeEncoderSDK


@dataclass
class AssetRecord:
    scene_id: str
    band: str
    path: str
    acq_time: str


def _parse_scene_id(path: Path) -> str:
    stem = path.stem
    sentinel = re.match(r"^(T\d{2}[A-Z]{3})_(\d{8}T\d{6})_(B\d{2,3}A?)_(\d+m)$", stem, re.IGNORECASE)
    if sentinel:
        tile, acq_time, _, _ = sentinel.groups()
        return f"S2_{tile.upper()}_{acq_time.upper()}"
    parts = stem.split("_")
    if len(parts) < 7:
        return stem
    return "_".join(parts[:7])


def _parse_acq_time(path: Path) -> datetime:
    sentinel = re.search(r"_(\d{8}T\d{6})_", path.stem, re.IGNORECASE)
    if sentinel:
        return datetime.strptime(sentinel.group(1), "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
    m = re.search(r"_(\d{8})_", path.stem)
    if not m:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    return datetime.strptime(m.group(1), "%Y%m%d").replace(tzinfo=timezone.utc)


def _parse_band(path: Path, scene_id: str) -> str:
    name = path.stem
    sentinel = re.match(r"^T\d{2}[A-Z]{3}_\d{8}T\d{6}_(B\d{2,3}A?)_(\d+m)$", name, re.IGNORECASE)
    if sentinel:
        band, resolution = sentinel.groups()
        return f"{band}_{resolution}".lower()
    if name.startswith(scene_id + "_"):
        suffix = name[len(scene_id) + 1 :]
        return suffix.lower()
    return name.lower()


def build_manifest(input_dir: Path) -> list[AssetRecord]:
    records: list[AssetRecord] = []
    tif_paths = sorted(path for path in input_dir.iterdir() if path.is_file() and path.suffix.lower() == ".tif")
    for tif in tif_paths:
        scene_id = _parse_scene_id(tif)
        acq_time = _parse_acq_time(tif)
        band = _parse_band(tif, scene_id)
        records.append(
            AssetRecord(
                scene_id=scene_id,
                band=band,
                path=str(tif.resolve()),
                acq_time=acq_time.isoformat().replace("+00:00", "Z"),
            )
        )
    return records


def convert_assets_to_cog(
    assets: list[AssetRecord],
    cog_input_dir: Path,
    overwrite: bool = False,
    workers: int = 0,
    compress: str = "LZW",
    predictor: int = 2,
    level: int | None = None,
    overviews: str = "NONE",
    num_threads: str = "ALL_CPUS",
) -> list[AssetRecord]:
    if not assets:
        return []

    cog_input_dir.mkdir(parents=True, exist_ok=True)
    if workers < 0:
        raise ValueError("workers must be >= 0")
    worker_count = workers or min(len(assets), (os.cpu_count() or 1))
    worker_count = max(1, worker_count)

    creation_options: dict[str, str] = {
        "COMPRESS": str(compress).upper(),
        "OVERVIEWS": overviews,
    }
    if predictor > 0:
        creation_options["PREDICTOR"] = str(predictor)
    if level is not None and level > 0:
        creation_options["LEVEL"] = str(level)
    if num_threads:
        creation_options["NUM_THREADS"] = str(num_threads)

    def convert_one(asset: AssetRecord) -> AssetRecord:
        src = Path(asset.path)
        dst = cog_input_dir / f"{src.stem}_cog.tif"
        if overwrite and dst.exists():
            dst.unlink()
        if not dst.exists():
            with rasterio.open(src) as ds:
                rio_copy(
                    ds,
                    str(dst),
                    driver="COG",
                    **creation_options,
                )
        return AssetRecord(
            scene_id=asset.scene_id,
            band=asset.band,
            path=str(dst.resolve()),
            acq_time=asset.acq_time,
        )

    if worker_count == 1:
        return [convert_one(asset) for asset in assets]

    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        return list(pool.map(convert_one, assets))


def _dataset_bounds_wgs84(ds: rasterio.DatasetReader) -> tuple[float, float, float, float]:
    b = ds.bounds
    if ds.crs and str(ds.crs).upper() != "EPSG:4326":
        transformer = Transformer.from_crs(ds.crs, "EPSG:4326", always_xy=True)
        xs = [b.left, b.left, b.right, b.right]
        ys = [b.bottom, b.top, b.bottom, b.top]
        lons, lats = transformer.transform(xs, ys)
        return min(lons), min(lats), max(lons), max(lats)
    return b.left, b.bottom, b.right, b.top


def _bbox_intersects(a: list[float] | tuple[float, float, float, float], b: list[float] | tuple[float, float, float, float]) -> bool:
    return float(a[0]) < float(b[2]) and float(a[2]) > float(b[0]) and float(a[1]) < float(b[3]) and float(a[3]) > float(b[1])


def _bbox_intersection(
    a: list[float] | tuple[float, float, float, float],
    b: list[float] | tuple[float, float, float, float],
) -> tuple[float, float, float, float] | None:
    min_lon = max(float(a[0]), float(b[0]))
    min_lat = max(float(a[1]), float(b[1]))
    max_lon = min(float(a[2]), float(b[2]))
    max_lat = min(float(a[3]), float(b[3]))
    if min_lon >= max_lon or min_lat >= max_lat:
        return None
    return min_lon, min_lat, max_lon, max_lat


def _group_tasks_for_local_processing(tasks: list[dict]) -> list[list[dict]]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for task in tasks:
        grouped[task["asset_path"]].append(task)
    return [
        sorted(rows, key=lambda row: row["space_code"])
        for _, rows in sorted(grouped.items(), key=lambda item: item[0])
    ]


def _prepare_task_rows_for_partitioning(
    tasks: list[dict],
    partition_prefix_len: int,
    time_granularity: str,
) -> list[dict]:
    prefix_len = max(1, int(partition_prefix_len))
    time_format = {
        "year": "%Y",
        "month": "%Y%m",
        "day": "%Y%m%d",
        "hour": "%Y%m%d%H",
        "minute": "%Y%m%d%H%M",
    }[time_granularity]

    task_rows: list[dict] = []
    for task in tasks:
        row = dict(task)
        row["space_code_prefix"] = row["space_code"][:prefix_len]
        row["time_bucket"] = datetime.fromisoformat(row["acq_time"].replace("Z", "+00:00")).strftime(time_format)
        task_rows.append(row)
    return task_rows


def _cover_codes(
    sdk: CubeEncoderSDK,
    grid_type: str,
    grid_level: int,
    cover_mode: str,
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
    bbox_cache: dict[str, list[float]],
) -> list[tuple[str, int, list[float]]]:
    def resolve_bbox(code: str) -> list[float]:
        if code not in bbox_cache:
            bbox_cache[code] = sdk.code_to_bbox(grid_type=grid_type, code=code)
        return bbox_cache[code]

    if grid_type == "geohash" and grid_level >= 7:
        coarse_level = max(1, grid_level - 2)
        coarse_cells = sdk.cover_compact(
            grid_type=grid_type,
            level=coarse_level,
            cover_mode=cover_mode,
            bbox=[min_lon, min_lat, max_lon, max_lat],
            crs="EPSG:4326",
        )
        target_bbox = [min_lon, min_lat, max_lon, max_lat]
        seen: set[str] = set()
        refined: list[tuple[str, int, list[float]]] = []
        for coarse in coarse_cells:
            child_codes = sdk.children(grid_type=grid_type, code=coarse.space_code, target_level=grid_level)
            for code in child_codes:
                if code in seen:
                    continue
                cb = resolve_bbox(code)
                if not _bbox_intersects(cb, target_bbox):
                    continue
                seen.add(code)
                refined.append((code, grid_level, cb))
        return refined

    cells = sdk.cover_compact(
        grid_type=grid_type,
        level=grid_level,
        cover_mode=cover_mode,
        bbox=[min_lon, min_lat, max_lon, max_lat],
        crs="EPSG:4326",
    )
    return [(cell.space_code, int(cell.level), cell.bbox) for cell in cells]


def build_grid_tasks_driver(
    assets: list[AssetRecord],
    grid_type: str,
    grid_level: int,
    cover_mode: str,
    max_cells_per_asset: int,
) -> list[dict]:
    sdk = CubeEncoderSDK()
    bbox_cache: dict[str, list[float]] = {}
    scene_cover_cache: dict[tuple[str, float, float, float, float], list[tuple[str, int, list[float]]]] = {}
    tasks: list[dict] = []

    for asset in assets:
        with rasterio.open(asset.path) as ds:
            min_lon, min_lat, max_lon, max_lat = _dataset_bounds_wgs84(ds)

        scene_cover_key = (asset.scene_id, float(min_lon), float(min_lat), float(max_lon), float(max_lat))
        cells = scene_cover_cache.get(scene_cover_key)
        if cells is None:
            cells = _cover_codes(
                sdk=sdk,
                grid_type=grid_type,
                grid_level=grid_level,
                cover_mode=cover_mode,
                min_lon=min_lon,
                min_lat=min_lat,
                max_lon=max_lon,
                max_lat=max_lat,
                bbox_cache=bbox_cache,
            )
            scene_cover_cache[scene_cover_key] = cells

        if len(cells) > max_cells_per_asset:
            raise RuntimeError(
                "Cover cells exceed max limit for asset %s: %d > %d"
                % (asset.path, len(cells), max_cells_per_asset)
            )

        for space_code, level, cb in cells:
            tasks.append(
                {
                    "scene_id": asset.scene_id,
                    "band": asset.band,
                    "asset_path": asset.path,
                    "acq_time": asset.acq_time,
                    "grid_type": grid_type,
                    "grid_level": int(level),
                    "space_code": space_code,
                    "cell_min_lon": float(cb[0]),
                    "cell_min_lat": float(cb[1]),
                    "cell_max_lon": float(cb[2]),
                    "cell_max_lat": float(cb[3]),
                    "cover_mode": cover_mode,
                }
            )
    return tasks


def _wgs84_to_dataset_bounds(
    ds: rasterio.DatasetReader,
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
) -> tuple[float, float, float, float]:
    if ds.crs and str(ds.crs).upper() != "EPSG:4326":
        transformer = Transformer.from_crs("EPSG:4326", ds.crs, always_xy=True)
        xs = [min_lon, min_lon, max_lon, max_lon]
        ys = [min_lat, max_lat, min_lat, max_lat]
        px, py = transformer.transform(xs, ys)
        return min(px), min(py), max(px), max(py)
    return min_lon, min_lat, max_lon, max_lat


def _safe_window_from_bounds(ds: rasterio.DatasetReader, left: float, bottom: float, right: float, top: float) -> Optional[Window]:
    if not (left < right and bottom < top):
        return None

    win = rasterio.windows.from_bounds(left=left, bottom=bottom, right=right, top=top, transform=ds.transform)
    rounded = win.round_offsets().round_lengths()

    col_off = max(0, int(rounded.col_off))
    row_off = max(0, int(rounded.row_off))
    width = int(min(ds.width - col_off, max(0, int(rounded.width))))
    height = int(min(ds.height - row_off, max(0, int(rounded.height))))

    if width <= 0 or height <= 0:
        return None
    return Window(col_off=col_off, row_off=row_off, width=width, height=height)


def process_partition(rows: Iterator[Any], time_granularity: str, include_sample_mean: bool = True) -> Iterator[dict]:
    sdk = CubeEncoderSDK()
    st_cache: dict[str, str] = {}

    open_path: str | None = None
    open_ds: rasterio.DatasetReader | None = None
    ds_bounds_wgs84: tuple[float, float, float, float] | None = None

    try:
        for row in rows:
            if open_path != row.asset_path:
                if open_ds is not None:
                    open_ds.close()
                open_ds = rasterio.open(row.asset_path)
                open_path = row.asset_path
                ds_bounds_wgs84 = _dataset_bounds_wgs84(open_ds)

            assert open_ds is not None and ds_bounds_wgs84 is not None

            acq_dt = datetime.fromisoformat(row.acq_time.replace("Z", "+00:00"))
            st_key = "%s|%d|%s|%s|%s" % (
                row.grid_type,
                row.grid_level,
                row.space_code,
                row.acq_time,
                time_granularity,
            )
            if st_key in st_cache:
                st_code = st_cache[st_key]
            else:
                st_code = sdk.generate_st_code(
                    grid_type=row.grid_type,
                    level=row.grid_level,
                    space_code=row.space_code,
                    timestamp=acq_dt,
                    time_granularity=time_granularity,
                    version="v1",
                ).st_code
                st_cache[st_key] = st_code

            inter = _bbox_intersection(
                ds_bounds_wgs84,
                (row.cell_min_lon, row.cell_min_lat, row.cell_max_lon, row.cell_max_lat),
            )
            if inter is None:
                continue
            min_lon, min_lat, max_lon, max_lat = inter

            left, bottom, right, top = _wgs84_to_dataset_bounds(open_ds, min_lon, min_lat, max_lon, max_lat)
            win = _safe_window_from_bounds(open_ds, left, bottom, right, top)
            if win is None:
                continue

            sample_mean = None
            if include_sample_mean:
                band1 = open_ds.read(1, window=win, masked=True)
                if band1.size > 0 and band1.count() > 0:
                    sample_mean = float(band1.mean())

            yield {
                "scene_id": row.scene_id,
                "band": row.band,
                "asset_path": row.asset_path,
                "acq_time": row.acq_time,
                "grid_type": row.grid_type,
                "grid_level": int(row.grid_level),
                "space_code": row.space_code,
                "space_code_prefix": row.space_code_prefix,
                "st_code": st_code,
                "time_bucket": row.time_bucket,
                "cover_mode": row.cover_mode,
                "cell_min_lon": float(row.cell_min_lon),
                "cell_min_lat": float(row.cell_min_lat),
                "cell_max_lon": float(row.cell_max_lon),
                "cell_max_lat": float(row.cell_max_lat),
                "window_col_off": int(win.col_off),
                "window_row_off": int(win.row_off),
                "window_width": int(win.width),
                "window_height": int(win.height),
                "intersect_min_lon": float(min_lon),
                "intersect_min_lat": float(min_lat),
                "intersect_max_lon": float(max_lon),
                "intersect_max_lat": float(max_lat),
                "sample_mean_band1": sample_mean,
            }
    finally:
        if open_ds is not None:
            open_ds.close()


def _process_local_task_group(rows: list[dict], time_granularity: str, include_sample_mean: bool = True) -> list[dict]:
    if not rows:
        return []
    task_rows = [SimpleNamespace(**row) for row in rows]
    return list(process_partition(iter(task_rows), time_granularity, include_sample_mean=include_sample_mean))
