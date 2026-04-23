#!/usr/bin/env python3
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
import time
from pathlib import Path
from typing import Any

from cube_split.jobs.ray_partition_core import (
    build_manifest,
    build_grid_tasks_driver,
    convert_assets_to_cog,
    _group_tasks_for_local_processing,
    _prepare_task_rows_for_partitioning,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ray logical partition job for COG assets")
    parser.add_argument("--input-dir", default="data/landsat8", help="Input directory containing COG .TIF files")
    parser.add_argument("--output-dir", default="data/ray_output/logical_partition", help="Output directory")
    parser.add_argument(
        "--cog-input-dir",
        default="data/cog/partition_input",
        help="Directory for converted COG inputs used by partitioning",
    )
    parser.add_argument("--cog-overwrite", action="store_true", help="Force reconvert source TIF files to COG")
    parser.add_argument("--cog-workers", type=int, default=0, help="Parallel workers for TIF->COG conversion (0 means auto)")
    parser.add_argument("--cog-compress", default="LZW", choices=["LZW", "DEFLATE", "ZSTD"], help="COG compression type")
    parser.add_argument("--cog-predictor", type=int, default=2, help="COG predictor (0 disables predictor option)")
    parser.add_argument("--cog-level", type=int, default=0, help="COG compression level (0 means driver default)")
    parser.add_argument(
        "--cog-num-threads",
        default="ALL_CPUS",
        help="COG encoder NUM_THREADS option (empty string disables)",
    )
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
        "--ray-address",
        default="",
        help="Ray address to connect (e.g. auto or ray://host:10001). Empty means start local runtime.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=0,
        help="Number of asset groups per Ray task chunk (0 means auto)",
    )
    parser.add_argument(
        "--partition-backend",
        default="auto",
        choices=["auto", "ray", "thread"],
        help="Parallel backend for partition stage (auto selects thread unless --ray-address is set).",
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
    parser.add_argument(
        "--sample-mean",
        action="store_true",
        help="Read per-window pixel values to compute sample_mean_band1 (disabled by default in timing-mode)",
    )
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
    total_start = time.perf_counter()
    for key in ("SPARK_HOME", "SPARK_CONF_DIR", "HADOOP_CONF_DIR", "YARN_CONF_DIR"):
        os.environ.pop(key, None)

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    source_assets = build_manifest(input_dir)
    if not source_assets:
        raise RuntimeError(f"No .TIF assets found under: {input_dir}")
    cog_start = time.perf_counter()
    assets = convert_assets_to_cog(
        source_assets,
        cog_input_dir=Path(args.cog_input_dir),
        overwrite=args.cog_overwrite,
        workers=args.cog_workers,
        compress=args.cog_compress,
        predictor=args.cog_predictor,
        level=(args.cog_level if args.cog_level > 0 else None),
        overviews="NONE",
        num_threads=(args.cog_num_threads or ""),
    )
    cog_elapsed = time.perf_counter() - cog_start

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

    backend_requested = args.partition_backend
    if backend_requested == "auto":
        backend = "ray" if args.ray_address else "thread"
    else:
        backend = backend_requested

    ray_init_elapsed = 0.0
    include_sample_mean = args.sample_mean and (not args.timing_mode)
    start = time.perf_counter()
    out_rows: list[dict] = []
    if backend == "ray":
        ray = _load_ray()
        ray_init_start = time.perf_counter()
        if args.ray_address:
            try:
                ray.init(address=args.ray_address, ignore_reinit_error=True, include_dashboard=False, logging_level="ERROR")
            except Exception:
                if args.ray_address != "auto":
                    raise
                ray.init(ignore_reinit_error=True, include_dashboard=False, logging_level="ERROR")
        else:
            ray.init(ignore_reinit_error=True, include_dashboard=False, logging_level="ERROR")
        ray_init_elapsed = time.perf_counter() - ray_init_start

        @ray.remote
        class AssetTaskProcessor:
            def process_groups(self, task_groups: list[list[dict]], time_granularity: str, include_sample_mean: bool) -> list[dict]:
                from cube_split.jobs.ray_partition_core import _process_local_task_group

                rows_out: list[dict] = []
                for group in task_groups:
                    rows_out.extend(_process_local_task_group(group, time_granularity, include_sample_mean=include_sample_mean))
                return rows_out

        actors = [AssetTaskProcessor.remote() for _ in range(parallelism)]
        futures = [
            actors[idx % parallelism].process_groups.remote(chunk, args.time_granularity, include_sample_mean)
            for idx, chunk in enumerate(task_chunks)
        ]
        for rows in ray.get(futures):
            out_rows.extend(rows)
    else:
        from cube_split.jobs.ray_partition_core import _process_local_task_group

        def process_chunk(chunk: list[list[dict]]) -> list[dict]:
            rows_out: list[dict] = []
            for group in chunk:
                rows_out.extend(_process_local_task_group(group, args.time_granularity, include_sample_mean=include_sample_mean))
            return rows_out

        with ThreadPoolExecutor(max_workers=parallelism) as pool:
            for rows in pool.map(process_chunk, task_chunks):
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
        "cog_input_dir": str(Path(args.cog_input_dir).resolve()),
        "source_asset_count": len(source_assets),
        "asset_count": len(assets),
        "grid_task_count": len(grid_tasks),
        "grid_type": args.grid_type,
        "grid_level": args.grid_level,
        "cover_mode": args.cover_mode,
        "execution_engine": backend,
        "partition_backend_requested": backend_requested,
        "partition_backend_used": backend,
        "time_granularity": args.time_granularity,
        "partition_prefix_len": max(1, int(args.partition_prefix_len)),
        "total_index_rows": int(total_rows),
        "distinct_space_codes": int(distinct_space_codes),
        "distinct_st_codes": int(distinct_st_codes),
        "rows_by_band": rows_by_band,
        "ray_parallelism": parallelism,
        "ray_address": args.ray_address,
        "ray_init_elapsed_sec": round(ray_init_elapsed, 3),
        "chunk_size": chunk_size,
        "timing_mode": args.timing_mode,
        "skip_verify": skip_verify,
        "sample_mean_enabled": include_sample_mean,
        "cog_overwrite": args.cog_overwrite,
        "cog_workers": args.cog_workers,
        "cog_compress": args.cog_compress,
        "cog_predictor": args.cog_predictor,
        "cog_level": args.cog_level,
        "cog_num_threads": args.cog_num_threads,
        "cog_elapsed_sec": round(cog_elapsed, 3),
        "partition_elapsed_sec": round(elapsed, 3),
        "total_elapsed_sec": round(time.perf_counter() - total_start, 3),
    }
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print("=== Ray logical partition job completed ===")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if backend == "ray":
        ray.shutdown()


if __name__ == "__main__":
    main()
