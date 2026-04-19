#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any

from grid_core.spark_jobs.logical_partition_job import (
    build_manifest,
    build_grid_tasks_driver,
    _group_tasks_for_local_processing,
    _prepare_task_rows_for_partitioning,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ray logical partition job for COG assets")
    parser.add_argument("--input-dir", default="data/landsat8", help="Input directory containing COG .TIF files")
    parser.add_argument("--output-dir", default="data/ray_output/logical_partition", help="Output directory")
    parser.add_argument("--grid-type", default="geohash", choices=["geohash", "mgrs", "isea4h"], help="Grid type")
    parser.add_argument("--grid-level", type=int, default=5, help="Grid level")
    parser.add_argument("--cover-mode", default="intersect", choices=["intersect", "contain", "minimal"], help="Cover mode")
    parser.add_argument("--time-granularity", default="day", choices=["year", "month", "day", "hour", "minute"], help="ST time code granularity")
    parser.add_argument("--max-cells-per-asset", type=int, default=20000, help="Safety limit for cover cells per asset")
    parser.add_argument(
        "--ray-parallelism",
        type=int,
        default=0,
        help="Max number of Ray actors to use (0 means auto)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=0,
        help="Number of asset groups per Ray task chunk (0 means auto)",
    )
    parser.add_argument(
        "--partition-prefix-len",
        type=int,
        default=3,
        help="Prefix length of space_code used in grouping metadata",
    )
    parser.add_argument(
        "--timing-mode",
        action="store_true",
        help="Measure partition runtime without optional counts and summary aggregation",
    )
    parser.add_argument("--skip-verify", action="store_true", help="Skip summary aggregations for faster timing runs")
    return parser.parse_args()


def _chunk_tasks_for_ray(tasks: list[Any], chunk_size: int) -> list[list[Any]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    return [tasks[idx : idx + chunk_size] for idx in range(0, len(tasks), chunk_size)]


def _resolve_ray_parallelism(task_group_count: int, requested_parallelism: int) -> int:
    if task_group_count <= 0:
        raise ValueError("task_group_count must be > 0")
    if requested_parallelism == 0:
        requested_parallelism = os.cpu_count() or 1
    if requested_parallelism < 0:
        raise ValueError("requested_parallelism must be >= 0")
    return max(1, min(task_group_count, requested_parallelism))


def _resolve_ray_chunk_size(task_group_count: int, parallelism: int, requested_chunk_size: int) -> int:
    if task_group_count <= 0:
        raise ValueError("task_group_count must be > 0")
    if parallelism <= 0:
        raise ValueError("parallelism must be > 0")
    if requested_chunk_size == 0:
        return max(1, task_group_count // max(1, parallelism * 2))
    if requested_chunk_size < 0:
        raise ValueError("requested_chunk_size must be >= 0")
    return requested_chunk_size


def _load_ray():
    try:
        import ray  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Ray is not installed. Install `ray` before running the Ray logical partition job."
        ) from exc
    return ray


def main() -> None:
    args = parse_args()
    for key in ("SPARK_HOME", "SPARK_CONF_DIR", "HADOOP_CONF_DIR", "YARN_CONF_DIR"):
        os.environ.pop(key, None)

    ray = _load_ray()
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    assets = build_manifest(input_dir)
    if not assets:
        raise RuntimeError(f"No .TIF assets found under: {input_dir}")

    grid_tasks = build_grid_tasks_driver(
        assets=assets,
        grid_type=args.grid_type,
        grid_level=args.grid_level,
        cover_mode=args.cover_mode,
        max_cells_per_asset=args.max_cells_per_asset,
    )
    task_rows = _prepare_task_rows_for_partitioning(
        grid_tasks,
        partition_prefix_len=args.partition_prefix_len,
        time_granularity=args.time_granularity,
    )
    grouped_tasks = _group_tasks_for_local_processing(task_rows)
    parallelism = _resolve_ray_parallelism(len(grouped_tasks), args.ray_parallelism)
    chunk_size = _resolve_ray_chunk_size(len(grouped_tasks), parallelism, args.chunk_size)
    task_chunks = _chunk_tasks_for_ray(grouped_tasks, chunk_size)
    skip_verify = args.skip_verify or args.timing_mode

    output_dir.mkdir(parents=True, exist_ok=True)
    run_dir = output_dir / time.strftime("run_%Y%m%d_%H%M%S")
    rows_path = run_dir / "index_rows.jsonl"
    report_path = run_dir / "job_report.json"

    ray.init(ignore_reinit_error=True, include_dashboard=False, logging_level="ERROR")

    @ray.remote
    class AssetTaskProcessor:
        def process_groups(self, task_groups: list[list[dict]], time_granularity: str) -> list[dict]:
            from grid_core.spark_jobs.logical_partition_job import _process_local_task_group

            out_rows: list[dict] = []
            for group in task_groups:
                out_rows.extend(_process_local_task_group(group, time_granularity))
            return out_rows

    start = time.perf_counter()
    actors = [AssetTaskProcessor.remote() for _ in range(parallelism)]
    futures = [
        actors[idx % parallelism].process_groups.remote(chunk, args.time_granularity)
        for idx, chunk in enumerate(task_chunks)
    ]
    out_rows: list[dict] = []
    for rows in ray.get(futures):
        out_rows.extend(rows)
    elapsed = time.perf_counter() - start

    run_dir.mkdir(parents=True, exist_ok=True)
    with rows_path.open("w", encoding="utf-8") as fh:
        for row in out_rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    if skip_verify:
        total_rows = -1
        distinct_space_codes = -1
        distinct_st_codes = -1
        rows_by_band: dict[str, int] = {}
    else:
        total_rows = len(out_rows)
        distinct_space_codes = len({row["space_code"] for row in out_rows})
        distinct_st_codes = len({row["st_code"] for row in out_rows})
        rows_by_band: dict[str, int] = {}
        for row in out_rows:
            rows_by_band[row["band"]] = rows_by_band.get(row["band"], 0) + 1

    report = {
        "run_dir": str(run_dir.resolve()),
        "input_dir": str(input_dir.resolve()),
        "asset_count": len(assets),
        "grid_task_count": len(grid_tasks),
        "grid_type": args.grid_type,
        "grid_level": args.grid_level,
        "cover_mode": args.cover_mode,
        "execution_engine": "ray",
        "time_granularity": args.time_granularity,
        "partition_prefix_len": max(1, int(args.partition_prefix_len)),
        "total_index_rows": int(total_rows),
        "distinct_space_codes": int(distinct_space_codes),
        "distinct_st_codes": int(distinct_st_codes),
        "rows_by_band": rows_by_band,
        "ray_parallelism": parallelism,
        "chunk_size": chunk_size,
        "timing_mode": args.timing_mode,
        "skip_verify": skip_verify,
        "partition_elapsed_sec": round(elapsed, 3),
    }
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print("=== Ray logical partition job completed ===")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    ray.shutdown()


if __name__ == "__main__":
    main()
