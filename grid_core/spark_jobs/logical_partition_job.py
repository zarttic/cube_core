#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Iterator, Optional, Tuple

import rasterio
from pyproj import Transformer
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from rasterio.windows import Window

from grid_core.sdk import CubeEncoderSDK


@dataclass
class AssetRecord:
    scene_id: str
    band: str
    path: str
    acq_time: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark logical partition job for COG assets (grid-driven)")
    parser.add_argument("--input-dir", default="data/landsat8", help="Input directory containing COG .TIF files")
    parser.add_argument("--output-dir", default="data/spark_output/logical_partition", help="Output directory")
    parser.add_argument("--grid-type", default="geohash", choices=["geohash", "mgrs", "isea4h"], help="Grid type")
    parser.add_argument("--grid-level", type=int, default=5, help="Grid level")
    parser.add_argument("--cover-mode", default="intersect", choices=["intersect", "contain", "minimal"], help="Cover mode")
    parser.add_argument("--time-granularity", default="day", choices=["year", "month", "day", "hour", "minute"], help="ST time code granularity")
    parser.add_argument("--repartition", type=int, default=0, help="Force shuffle partitions (0 means auto)")
    parser.add_argument(
        "--partition-prefix-len",
        type=int,
        default=3,
        help="Prefix length of space_code used in Spark repartition key",
    )
    parser.add_argument("--max-cells-per-asset", type=int, default=20000, help="Safety limit for cover cells per asset")
    parser.add_argument(
        "--execution-engine",
        default="spark",
        choices=["spark", "local", "auto"],
        help="Execution engine for partition processing: spark, local, or auto",
    )
    parser.add_argument(
        "--cover-execution",
        default="spark",
        choices=["spark", "driver"],
        help="How to execute cover task generation: spark (parallel) or driver (serial)",
    )
    parser.add_argument(
        "--optimize-small-runs",
        action="store_true",
        help="Reduce Spark fixed overhead for small inputs by using lighter execution defaults",
    )
    parser.add_argument(
        "--timing-mode",
        action="store_true",
        help="Measure partition runtime without optional counts and post-write verification actions",
    )
    parser.add_argument(
        "--skip-verify",
        action="store_true",
        help="Skip parquet read-back verification and summary aggregations",
    )
    parser.add_argument(
        "--skip-task-count",
        action="store_true",
        help="Skip counting cover tasks before partition processing",
    )
    return parser.parse_args()


def _time_bucket_format(time_granularity: str) -> str:
    if time_granularity == "year":
        return "yyyy"
    if time_granularity == "month":
        return "yyyyMM"
    if time_granularity == "day":
        return "yyyyMMdd"
    if time_granularity == "hour":
        return "yyyyMMddHH"
    if time_granularity == "minute":
        return "yyyyMMddHHmm"
    raise ValueError(f"Unsupported time granularity: {time_granularity}")


def _parse_scene_id(path: Path) -> str:
    stem = path.stem
    parts = stem.split("_")
    if len(parts) < 7:
        return stem
    return "_".join(parts[:7])


def _parse_acq_time(path: Path) -> datetime:
    m = re.search(r"_(\d{8})_", path.stem)
    if not m:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    return datetime.strptime(m.group(1), "%Y%m%d").replace(tzinfo=timezone.utc)


def _parse_band(path: Path, scene_id: str) -> str:
    name = path.stem
    if name.startswith(scene_id + "_"):
        suffix = name[len(scene_id) + 1 :]
        return suffix.lower()
    return name.lower()


def build_manifest(input_dir: Path) -> list[AssetRecord]:
    records: list[AssetRecord] = []
    for tif in sorted(input_dir.glob("*.TIF")):
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


def _resolve_cover_execution(asset_count: int, cover_execution: str, optimize_small_runs: bool) -> str:
    if optimize_small_runs and asset_count <= 16 and cover_execution == "spark":
        return "driver"
    return cover_execution


def _resolve_execution_engine(execution_engine: str, optimize_small_runs: bool, asset_count: int) -> str:
    if execution_engine in {"spark", "local"}:
        return execution_engine
    if optimize_small_runs and asset_count <= 16:
        return "local"
    return "spark"


def _resolve_shuffle_partitions(asset_count: int, requested_repartition: int, optimize_small_runs: bool) -> int:
    if requested_repartition > 0:
        return requested_repartition
    if optimize_small_runs and asset_count <= 16:
        return 2
    return 8


def _should_skip_sort_within_partitions(asset_count: int, optimize_small_runs: bool) -> bool:
    return optimize_small_runs and asset_count <= 16


def _should_skip_verify(skip_verify: bool, timing_mode: bool) -> bool:
    return skip_verify or timing_mode


def _should_skip_grid_task_count(skip_task_count: bool, timing_mode: bool) -> bool:
    return skip_task_count or timing_mode


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


def _process_local_task_group(rows: list[dict], time_granularity: str) -> list[dict]:
    if not rows:
        return []
    task_rows = [SimpleNamespace(**row) for row in rows]
    return list(process_partition(iter(task_rows), time_granularity))


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

    # For high geohash levels, direct cover can be very expensive in current engine.
    # Use coarse cover + children refinement to keep grid-driven semantics but improve runtime.
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


def build_grid_tasks_partition(
    rows: Iterator[Row],
    grid_type: str,
    grid_level: int,
    cover_mode: str,
    max_cells_per_asset: int,
) -> Iterator[dict]:
    sdk = CubeEncoderSDK()
    bbox_cache: dict[str, list[float]] = {}
    scene_cover_cache: dict[tuple[str, float, float, float, float], list[tuple[str, int, list[float]]]] = {}

    for row in rows:
        with rasterio.open(row.asset_path) as ds:
            min_lon, min_lat, max_lon, max_lat = _dataset_bounds_wgs84(ds)

        scene_cover_key = (row.scene_id, float(min_lon), float(min_lat), float(max_lon), float(max_lat))
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
                % (row.asset_path, len(cells), max_cells_per_asset)
            )

        for space_code, level, cb in cells:
            yield {
                "scene_id": row.scene_id,
                "band": row.band,
                "asset_path": row.asset_path,
                "acq_time": row.acq_time,
                "grid_type": grid_type,
                "grid_level": int(level),
                "space_code": space_code,
                "cell_min_lon": float(cb[0]),
                "cell_min_lat": float(cb[1]),
                "cell_max_lon": float(cb[2]),
                "cell_max_lat": float(cb[3]),
                "cover_mode": cover_mode,
            }


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


def process_partition(rows: Iterator[Row], time_granularity: str) -> Iterator[dict]:
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

            # On-demand read: read only intersected grid window.
            band1 = open_ds.read(1, window=win, masked=True)
            sample_mean = None
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


def main() -> None:
    for key in ("SPARK_HOME", "SPARK_CONF_DIR", "HADOOP_CONF_DIR", "YARN_CONF_DIR"):
        os.environ.pop(key, None)

    args = parse_args()
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)

    if not input_dir.exists():
        raise FileNotFoundError("Input directory not found: %s" % input_dir)

    assets = build_manifest(input_dir)
    if not assets:
        raise RuntimeError("No .TIF assets found under: %s" % input_dir)
    asset_count = len(assets)
    effective_execution_engine = _resolve_execution_engine(
        execution_engine=args.execution_engine,
        optimize_small_runs=args.optimize_small_runs,
        asset_count=asset_count,
    )
    effective_cover_execution = _resolve_cover_execution(
        asset_count=asset_count,
        cover_execution=args.cover_execution,
        optimize_small_runs=args.optimize_small_runs,
    )
    effective_shuffle_partitions = _resolve_shuffle_partitions(
        asset_count=asset_count,
        requested_repartition=args.repartition,
        optimize_small_runs=args.optimize_small_runs,
    )
    skip_verify = _should_skip_verify(args.skip_verify, args.timing_mode)
    skip_task_count = _should_skip_grid_task_count(args.skip_task_count, args.timing_mode)
    skip_sort = _should_skip_sort_within_partitions(asset_count=asset_count, optimize_small_runs=args.optimize_small_runs)

    output_dir.mkdir(parents=True, exist_ok=True)
    run_dir = output_dir / datetime.now().strftime("run_%Y%m%d_%H%M%S")
    index_dir = run_dir / "index_parquet"
    index_dir_uri = index_dir.resolve().as_uri()
    local_index_path = run_dir / "index_rows.jsonl"
    report_path = run_dir / "job_report.json"

    task_schema = T.StructType(
        [
            T.StructField("scene_id", T.StringType(), False),
            T.StructField("band", T.StringType(), False),
            T.StructField("asset_path", T.StringType(), False),
            T.StructField("acq_time", T.StringType(), False),
            T.StructField("grid_type", T.StringType(), False),
            T.StructField("grid_level", T.IntegerType(), False),
            T.StructField("space_code", T.StringType(), False),
            T.StructField("cell_min_lon", T.DoubleType(), False),
            T.StructField("cell_min_lat", T.DoubleType(), False),
            T.StructField("cell_max_lon", T.DoubleType(), False),
            T.StructField("cell_max_lat", T.DoubleType(), False),
            T.StructField("cover_mode", T.StringType(), False),
        ]
    )

    partition_start = time.perf_counter()
    schema = T.StructType(
        [
            T.StructField("scene_id", T.StringType(), False),
            T.StructField("band", T.StringType(), False),
            T.StructField("asset_path", T.StringType(), False),
            T.StructField("acq_time", T.StringType(), False),
            T.StructField("grid_type", T.StringType(), False),
            T.StructField("grid_level", T.IntegerType(), False),
            T.StructField("space_code", T.StringType(), False),
            T.StructField("space_code_prefix", T.StringType(), False),
            T.StructField("st_code", T.StringType(), False),
            T.StructField("time_bucket", T.StringType(), False),
            T.StructField("cover_mode", T.StringType(), False),
            T.StructField("cell_min_lon", T.DoubleType(), False),
            T.StructField("cell_min_lat", T.DoubleType(), False),
            T.StructField("cell_max_lon", T.DoubleType(), False),
            T.StructField("cell_max_lat", T.DoubleType(), False),
            T.StructField("window_col_off", T.IntegerType(), False),
            T.StructField("window_row_off", T.IntegerType(), False),
            T.StructField("window_width", T.IntegerType(), False),
            T.StructField("window_height", T.IntegerType(), False),
            T.StructField("intersect_min_lon", T.DoubleType(), False),
            T.StructField("intersect_min_lat", T.DoubleType(), False),
            T.StructField("intersect_max_lon", T.DoubleType(), False),
            T.StructField("intersect_max_lat", T.DoubleType(), False),
            T.StructField("sample_mean_band1", T.DoubleType(), True),
        ]
    )
    prefix_len = max(1, int(args.partition_prefix_len))
    grid_task_count: int | None = None

    if effective_execution_engine == "local":
        grid_tasks = build_grid_tasks_driver(
            assets=assets,
            grid_type=args.grid_type,
            grid_level=args.grid_level,
            cover_mode=args.cover_mode,
            max_cells_per_asset=args.max_cells_per_asset,
        )
        if not skip_task_count:
            grid_task_count = len(grid_tasks)
            if grid_task_count == 0:
                raise RuntimeError("No grid tasks produced by cover()")
        task_rows = _prepare_task_rows_for_partitioning(
            grid_tasks,
            partition_prefix_len=prefix_len,
            time_granularity=args.time_granularity,
        )
        task_groups = _group_tasks_for_local_processing(task_rows)
        max_workers = min(max(1, os.cpu_count() or 1), max(1, len(task_groups)))
        out_rows: list[dict] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for part_rows in executor.map(lambda group: _process_local_task_group(group, args.time_granularity), task_groups):
                out_rows.extend(part_rows)

        run_dir.mkdir(parents=True, exist_ok=True)
        with local_index_path.open("w", encoding="utf-8") as fh:
            for row in out_rows:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")

        if skip_verify:
            total_rows = -1
            distinct_cells = -1
            distinct_st = -1
            by_band = {}
        else:
            total_rows = len(out_rows)
            distinct_cells = len({row["space_code"] for row in out_rows})
            distinct_st = len({row["st_code"] for row in out_rows})
            by_band: dict[str, int] = {}
            for row in out_rows:
                by_band[row["band"]] = by_band.get(row["band"], 0) + 1
    else:
        spark = (
            SparkSession.builder.appName("cube-encoder-logical-partition")
            .master("local[*]")
            .config("spark.hadoop.fs.defaultFS", "file:///")
            .config("spark.sql.warehouse.dir", str((Path(".tmp") / "spark-warehouse").resolve()))
            .config("spark.eventLog.enabled", "false")
            .config("spark.eventLog.dir", str((Path(".tmp") / "spark-events").resolve().as_uri()))
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.shuffle.partitions", str(effective_shuffle_partitions))
            .getOrCreate()
        )

        spark.conf.set("spark.sql.shuffle.partitions", str(effective_shuffle_partitions))

        if effective_cover_execution == "spark":
            assets_df = spark.createDataFrame(
                Row(scene_id=a.scene_id, band=a.band, asset_path=a.path, acq_time=a.acq_time) for a in assets
            ).repartition("asset_path")
            task_df = spark.createDataFrame(
                assets_df.rdd.mapPartitions(
                    lambda rows: build_grid_tasks_partition(
                        rows=rows,
                        grid_type=args.grid_type,
                        grid_level=args.grid_level,
                        cover_mode=args.cover_mode,
                        max_cells_per_asset=args.max_cells_per_asset,
                    )
                ),
                schema=task_schema,
            ).cache()
        else:
            grid_tasks = build_grid_tasks_driver(
                assets=assets,
                grid_type=args.grid_type,
                grid_level=args.grid_level,
                cover_mode=args.cover_mode,
                max_cells_per_asset=args.max_cells_per_asset,
            )
            task_df = spark.createDataFrame(Row(**r) for r in grid_tasks).cache()

        if not skip_task_count:
            grid_task_count = task_df.count()
            if grid_task_count == 0:
                raise RuntimeError("No grid tasks produced by cover()")

        ts_col = F.to_timestamp(F.col("acq_time"))
        task_df = (
            task_df.withColumn("space_code_prefix", F.substring(F.col("space_code"), 1, prefix_len))
            .withColumn("time_bucket", F.date_format(ts_col, _time_bucket_format(args.time_granularity)))
        )
        task_df = task_df.repartition(effective_shuffle_partitions, "space_code_prefix", "time_bucket")
        if not skip_sort:
            task_df = task_df.sortWithinPartitions("asset_path", "space_code")

        out_df = spark.createDataFrame(
            task_df.rdd.mapPartitions(lambda rows: process_partition(rows, args.time_granularity)),
            schema=schema,
        )

        out_df.write.mode("overwrite").partitionBy("band", "time_bucket").parquet(index_dir_uri)
        if skip_verify:
            total_rows = -1
            distinct_cells = -1
            distinct_st = -1
            by_band = {}
        else:
            verify_df = spark.read.parquet(index_dir_uri)
            total_rows = verify_df.count()
            approx = verify_df.agg(
                F.approx_count_distinct("space_code").alias("distinct_space_codes"),
                F.approx_count_distinct("st_code").alias("distinct_st_codes"),
            ).first()
            distinct_cells = int(approx["distinct_space_codes"])
            distinct_st = int(approx["distinct_st_codes"])

            by_band = {
                row["band"]: int(row["cnt"])
                for row in verify_df.groupBy("band").agg(F.count(F.lit(1)).alias("cnt")).collect()
            }
        spark.stop()

    report = {
        "run_dir": str(run_dir.resolve()),
        "input_dir": str(input_dir.resolve()),
        "asset_count": asset_count,
        "grid_task_count": None if grid_task_count is None else int(grid_task_count),
        "grid_type": args.grid_type,
        "grid_level": args.grid_level,
        "cover_mode": args.cover_mode,
        "execution_engine": effective_execution_engine,
        "cover_execution": effective_cover_execution,
        "time_granularity": args.time_granularity,
        "partition_prefix_len": prefix_len,
        "spark_repartition_key": ["space_code_prefix", "time_bucket"],
        "total_index_rows": int(total_rows),
        "distinct_space_codes": int(distinct_cells),
        "distinct_st_codes": int(distinct_st),
        "rows_by_band": by_band,
        "optimize_small_runs": args.optimize_small_runs,
        "timing_mode": args.timing_mode,
        "skip_verify": skip_verify,
        "skip_task_count": skip_task_count,
        "shuffle_partitions": effective_shuffle_partitions,
        "skip_sort_within_partitions": skip_sort,
        "partition_elapsed_sec": round(time.perf_counter() - partition_start, 3),
    }

    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=== Logical partition job completed (grid-driven) ===")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
