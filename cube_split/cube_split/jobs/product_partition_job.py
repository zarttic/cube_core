#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from cube_split.jobs.ray_partition_core import (
    _group_tasks_for_local_processing,
    _prepare_task_rows_for_partitioning,
    _process_local_task_group,
    build_grid_tasks_driver,
    build_manifest,
    convert_assets_to_cog,
)


def _prepare_product_task_rows(tasks: list[dict], partition_prefix_len: int) -> list[dict]:
    rows = _prepare_task_rows_for_partitioning(
        tasks,
        partition_prefix_len=partition_prefix_len,
        time_granularity="day",
    )
    for row in rows:
        row["time_bucket"] = row["acq_time"][:4]
        row["st_time_granularity"] = "day"
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Partition raster product TIF data into cube index rows")
    parser.add_argument("--input-dir", default="data/product", help="Input directory containing product TIF files")
    parser.add_argument("--output-dir", default="data/ray_output/product", help="Output directory")
    parser.add_argument("--cog-input-dir", default="data/cog/product_epsg4326", help="Directory for standardized product COGs")
    parser.add_argument("--target-crs", default="EPSG:4326", help="Target CRS for standardized COG assets")
    parser.add_argument("--grid-type", default="geohash", choices=["geohash", "mgrs", "isea4h"], help="Grid type")
    parser.add_argument("--grid-level", type=int, default=5, help="Grid level")
    parser.add_argument("--cover-mode", default="intersect", choices=["intersect", "contain", "minimal"], help="Cover mode")
    parser.add_argument("--max-cells-per-asset", type=int, default=20000, help="Safety limit for cover cells per asset")
    parser.add_argument("--partition-prefix-len", type=int, default=3, help="Prefix length used in row grouping")
    parser.add_argument("--cog-overwrite", action="store_true", help="Force reconvert source TIF files to COG")
    parser.add_argument("--cog-workers", type=int, default=0, help="Parallel workers for COG conversion")
    parser.add_argument("--partition-workers", type=int, default=0, help="Parallel workers for partition stage")
    parser.add_argument("--sample-mean", action="store_true", help="Compute per-window sample mean")
    return parser.parse_args()


def run_product_partition(args: argparse.Namespace) -> dict:
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    total_start = time.perf_counter()
    source_assets = build_manifest(input_dir, data_type="product")
    if not source_assets:
        raise RuntimeError(f"No product TIF assets found under: {input_dir}")

    cog_start = time.perf_counter()
    assets = convert_assets_to_cog(
        source_assets,
        cog_input_dir=Path(args.cog_input_dir),
        overwrite=bool(args.cog_overwrite),
        workers=int(args.cog_workers),
        compress="LZW",
        predictor=2,
        overviews="NONE",
        num_threads="ALL_CPUS",
        target_crs=args.target_crs,
    )
    cog_elapsed = time.perf_counter() - cog_start

    grid_tasks = build_grid_tasks_driver(
        assets=assets,
        grid_type=args.grid_type,
        grid_level=int(args.grid_level),
        cover_mode=args.cover_mode,
        max_cells_per_asset=int(args.max_cells_per_asset),
    )
    task_rows = _prepare_product_task_rows(grid_tasks, partition_prefix_len=int(args.partition_prefix_len))
    grouped_tasks = _group_tasks_for_local_processing(task_rows)

    output_dir.mkdir(parents=True, exist_ok=True)
    run_dir = output_dir / time.strftime("run_%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=False)
    rows_path = run_dir / "index_rows.jsonl"
    report_path = run_dir / "job_report.json"

    worker_count = int(args.partition_workers) or min(len(grouped_tasks), 8)
    worker_count = max(1, worker_count)
    start = time.perf_counter()
    out_rows: list[dict] = []
    if worker_count == 1:
        for group in grouped_tasks:
            out_rows.extend(_process_local_task_group(group, "day", include_sample_mean=bool(args.sample_mean)))
    else:
        with ThreadPoolExecutor(max_workers=worker_count) as pool:
            futures = [
                pool.submit(_process_local_task_group, group, "day", bool(args.sample_mean))
                for group in grouped_tasks
            ]
            for future in futures:
                out_rows.extend(future.result())
    partition_elapsed = time.perf_counter() - start

    with rows_path.open("w", encoding="utf-8") as fh:
        for row in out_rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    summary = {
        "status": "completed",
        "data_type": "product",
        "input_dir": str(input_dir.resolve()),
        "run_dir": str(run_dir.resolve()),
        "rows_path": str(rows_path.resolve()),
        "source_asset_count": len(source_assets),
        "asset_count": len(assets),
        "grid_task_count": len(grid_tasks),
        "rows": len(out_rows),
        "grid_type": args.grid_type,
        "grid_level": int(args.grid_level),
        "target_crs": args.target_crs,
        "cog_elapsed_sec": round(cog_elapsed, 3),
        "partition_elapsed_sec": round(partition_elapsed, 3),
        "total_elapsed_sec": round(time.perf_counter() - total_start, 3),
    }
    report_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    print(json.dumps(run_product_partition(parse_args()), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
