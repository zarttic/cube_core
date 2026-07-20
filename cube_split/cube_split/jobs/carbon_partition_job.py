#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from cube_split import runtime_config
from cube_split.jobs.ray_partition_core import create_unique_run_dir
from cube_split.partition.carbon import CarbonPartitionConfig, CarbonSatellitePartitionService
from cube_split.partition.carbon_products import normalize_carbon_product_type


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Partition carbon satellite observations into cube observation rows")
    parser.add_argument("--input-dir", default="data/carbon", help="Input directory containing carbon observation files")
    parser.add_argument("--output-dir", default="data/ray_output/carbon", help="Output directory")
    parser.add_argument("--grid-type", default="isea4h", choices=["geohash", "mgrs", "isea4h"], help="Grid type")
    parser.add_argument("--grid-level", type=int, default=5, help="Grid level")
    parser.add_argument("--time-granularity", default="day", choices=["month", "day", "hour", "minute", "second"])
    parser.add_argument("--product-type", default="xco2", help="Carbon product type")
    parser.add_argument("--max-observations", type=int, default=0, help="Maximum observations to process; 0 means all")
    parser.add_argument("--partition-chunk-size", type=int, default=0, help="Observations per partition chunk; 0 sizes chunks automatically")
    parser.add_argument("--partition-workers", type=int, default=0, help="Parallel workers for partition stage; 0 means auto")
    parser.add_argument(
        "--partition-backend",
        default="ray",
        choices=["auto", "ray", "process", "thread"],
        help="Partition backend.",
    )
    parser.add_argument("--ray-address", default=runtime_config.ray_address(), help="Ray address, e.g. auto or ray://host:10001")
    parser.add_argument("--ray-parallelism", type=int, default=0, help="Ray worker count; 0 uses --partition-workers/auto")
    return parser.parse_args()


def _resolve_backend(requested_backend: str, ray_address: str) -> str:
    if requested_backend == "auto":
        return "ray" if ray_address else "process"
    return requested_backend


def _resolve_worker_count(partition_workers: int, ray_parallelism: int, backend: str) -> int:
    if backend == "ray" and ray_parallelism > 0:
        return ray_parallelism
    if partition_workers > 0:
        return partition_workers
    return 4 if backend == "ray" else 1


def run_carbon_partition(args: argparse.Namespace) -> dict:
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    total_start = time.perf_counter()
    run_dir = create_unique_run_dir(output_dir)
    report_path = run_dir / "job_report.json"

    backend = _resolve_backend(args.partition_backend, args.ray_address)
    worker_count = _resolve_worker_count(int(args.partition_workers), int(args.ray_parallelism), backend)
    product_type = normalize_carbon_product_type(args.product_type)
    config = CarbonPartitionConfig(
        grid_type=args.grid_type,
        grid_level=int(args.grid_level),
        time_granularity=args.time_granularity,
        product_type=product_type,
        max_observations=(None if int(args.max_observations) <= 0 else int(args.max_observations)),
        selected_source_indexes=getattr(args, "selected_source_indexes", None),
        source_uris=getattr(args, "source_uris", None),
        partition_chunk_size=int(args.partition_chunk_size),
        partition_backend=backend,
        ray_address=args.ray_address,
        cancellation_check=getattr(args, "cancellation_check", None),
    )
    partition_start = time.perf_counter()
    result = CarbonSatellitePartitionService().run(
        input_dir=input_dir,
        output_dir=run_dir,
        config=config,
        workers=worker_count,
    )
    partition_elapsed = time.perf_counter() - partition_start

    summary = {
        "status": "completed",
        "data_type": result.data_type,
        "input_dir": str(input_dir.resolve()),
        "run_dir": str(run_dir.resolve()),
        "rows_path": str(result.rows_path.resolve()),
        "rows": result.total_rows,
        "grid_type": config.grid_type,
        "grid_level": config.grid_level,
        "time_granularity": config.time_granularity,
        "product_type": product_type,
        "max_observations": config.max_observations,
        "partition_chunk_size": config.partition_chunk_size,
        "execution_engine": backend,
        "partition_backend_requested": args.partition_backend,
        "partition_backend_used": backend,
        "partition_backend": backend,
        "partition_workers": worker_count,
        "ray_address": args.ray_address,
        "ray_parallelism": worker_count if backend == "ray" else 0,
        "partition_elapsed_sec": round(partition_elapsed, 3),
        "total_elapsed_sec": round(time.perf_counter() - total_start, 3),
    }
    report_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    print(json.dumps(run_carbon_partition(parse_args()), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
