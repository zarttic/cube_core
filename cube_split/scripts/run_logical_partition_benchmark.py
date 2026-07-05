#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from argparse import Namespace
from pathlib import Path
from typing import Any

from cube_split import runtime_config
from cube_split.jobs.ray_logical_partition_job import run_logical_partition


SHANDONG_CORNERS_CUT = [
    [114.757377, 38.503521],
    [122.774914, 38.503521],
    [122.774914, 33.857041],
    [114.757377, 33.857041],
]

DEFAULT_SHANDONG_ASSETS = [
    {
        "data_type": "optical",
        "source_uri": "s3://cube/cube/source/optocal/Shandong_mosaic_2020Q3_sr_band2_cut/Shandong_mosaic_2020Q3_sr_band2_cut.tif",
        "scene_id": "Shandong_mosaic_2020Q3",
        "band": "sr_band2",
        "acq_time": "2020-07-01T00:00:00Z",
        "sensor": "optical_mosaic",
        "product_family": "other",
        "resolution": 30,
        "corners": SHANDONG_CORNERS_CUT,
    },
    {
        "data_type": "optical",
        "source_uri": "s3://cube/cube/source/optocal/Shandong_mosaic_2020Q3_sr_band3_cut/Shandong_mosaic_2020Q3_sr_band3_cut.tif",
        "scene_id": "Shandong_mosaic_2020Q3",
        "band": "sr_band3",
        "acq_time": "2020-07-01T00:00:00Z",
        "sensor": "optical_mosaic",
        "product_family": "other",
        "resolution": 30,
        "corners": SHANDONG_CORNERS_CUT,
    },
]

REPORT_FIELDS = [
    "source_asset_count",
    "grid_task_count",
    "total_index_rows",
    "partition_elapsed_sec",
    "total_elapsed_sec",
    "ray_init_elapsed_sec",
    "worker_source_resolve_elapsed_sec",
    "worker_cog_write_elapsed_sec",
    "worker_cog_upload_elapsed_sec",
    "worker_partition_rows_elapsed_sec",
    "worker_cog_write_count",
    "worker_cog_cache_hit_count",
    "worker_cog_upload_count",
    "run_dir",
    "minio_prefix",
]

SENSITIVE_FLAGS = {
    "--minio-access-key",
    "--minio-secret-key",
    "--postgres-dsn",
}


def parse_args() -> argparse.Namespace:
    minio = runtime_config.minio_settings()
    run_id = time.strftime("logical_bench_%Y%m%d_%H%M%S")
    parser = argparse.ArgumentParser(description="Run small reproducible optical logical partition benchmarks.")
    parser.add_argument("--work-dir", default="/tmp/cube_logical_partition_benchmark", help="Local benchmark work root")
    parser.add_argument("--run-id", default=run_id, help="Unique run id used in local output and MinIO prefixes")
    parser.add_argument(
        "--manifest-path",
        default="",
        help="Optional manifest .json/.jsonl. Defaults to Shandong 2020Q3 band2/band3 MinIO sources.",
    )
    parser.add_argument("--grid-types", default="s2,tile_matrix", help="Comma-separated grid types: s2,tile_matrix")
    parser.add_argument("--s2-level", type=int, default=5, help="S2 level for the benchmark")
    parser.add_argument("--tile-matrix-level", type=int, default=5, help="tile_matrix level for the benchmark")
    parser.add_argument("--ray-parallelism-values", default="2", help="Comma-separated Ray actor counts")
    parser.add_argument("--chunk-sizes", default="1", help="Comma-separated chunk sizes")
    parser.add_argument("--repeat", type=int, default=1, help="Repeat each case this many times")
    parser.add_argument("--partition-backend", default="ray", choices=["ray", "thread", "auto"], help="Partition backend")
    parser.add_argument("--ray-address", default=runtime_config.ray_address(), help="Ray address, e.g. 10.3.100.182:6379")
    parser.add_argument("--max-cells-per-asset", type=int, default=500, help="Safety cap per asset")
    parser.add_argument("--cover-mode", default="intersect", choices=["intersect", "contain", "minimal"], help="Cover mode")
    parser.add_argument("--time-granularity", default="day", choices=["year", "month", "day", "hour", "minute"])
    parser.add_argument("--target-crs", default="EPSG:4326", help="COG target CRS")
    parser.add_argument("--cog-compress", default="LZW", choices=["NONE", "LZW", "DEFLATE", "ZSTD"])
    parser.add_argument("--cog-predictor", type=int, default=2)
    parser.add_argument("--cog-level", type=int, default=0)
    parser.add_argument("--cog-num-threads", default="ALL_CPUS")
    parser.add_argument("--cog-overwrite", action="store_true", help="Force worker-side COG rewrite")
    parser.add_argument("--timing-mode", action="store_true", help="Skip summary counts for faster timing")
    parser.add_argument("--sample-mean", action="store_true", help="Compute sample_mean_band1")
    parser.add_argument("--source-cache-dir", default="/tmp/cube_split_source_cache", help="Worker source cache dir")
    parser.add_argument("--minio-endpoint", default=minio.endpoint)
    parser.add_argument("--minio-access-key", default=minio.access_key)
    parser.add_argument("--minio-secret-key", default=minio.secret_key)
    parser.add_argument("--minio-bucket", default=minio.bucket or "cube")
    parser.add_argument("--minio-prefix-base", default="cube/benchmark/logical", help="MinIO output prefix base")
    parser.add_argument("--minio-secure", action=argparse.BooleanOptionalAction, default=minio.secure)
    return parser.parse_args()


def _csv(value: str) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def _csv_ints(value: str) -> list[int]:
    values = [int(item) for item in _csv(value)]
    if not values:
        raise ValueError("expected at least one integer")
    if any(item <= 0 for item in values):
        raise ValueError("integer values must be > 0")
    return values


def _grid_cases(args: argparse.Namespace) -> list[tuple[str, int]]:
    levels = {"s2": int(args.s2_level), "tile_matrix": int(args.tile_matrix_level)}
    cases: list[tuple[str, int]] = []
    for grid_type in _csv(args.grid_types):
        if grid_type not in levels:
            raise ValueError("grid-types only supports s2 and tile_matrix for logical benchmark")
        cases.append((grid_type, levels[grid_type]))
    if not cases:
        raise ValueError("grid-types must include at least one grid")
    return cases


def _write_default_manifest(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for asset in DEFAULT_SHANDONG_ASSETS:
            fh.write(json.dumps(asset, ensure_ascii=False) + "\n")
    return path


def _redacted_argv(argv: list[str]) -> str:
    redacted: list[str] = []
    skip_next = False
    for item in argv:
        if skip_next:
            redacted.append("***")
            skip_next = False
            continue
        key = item.split("=", 1)[0]
        if key in SENSITIVE_FLAGS:
            if "=" in item:
                redacted.append(f"{key}=***")
            else:
                redacted.append(item)
                skip_next = True
            continue
        redacted.append(item)
    return " ".join(redacted)


def _validate_remote_settings(args: argparse.Namespace, manifest_path: Path) -> None:
    text = manifest_path.read_text(encoding="utf-8")
    if "s3://" not in text:
        return
    if not args.minio_endpoint or not args.minio_access_key or not args.minio_secret_key:
        raise RuntimeError("MinIO endpoint/access-key/secret-key are required for s3:// benchmark assets")


def _job_args(
    args: argparse.Namespace,
    *,
    case_id: str,
    manifest_path: Path,
    input_dir: Path,
    output_dir: Path,
    cog_input_dir: Path,
    grid_type: str,
    grid_level: int,
    ray_parallelism: int,
    chunk_size: int,
) -> argparse.Namespace:
    minio_prefix = f"{str(args.minio_prefix_base).strip('/')}/{args.run_id}/{case_id}"
    return Namespace(
        input_dir=str(input_dir),
        manifest_path=str(manifest_path),
        product_family="auto",
        output_dir=str(output_dir),
        cog_input_dir=str(cog_input_dir),
        cog_overwrite=bool(args.cog_overwrite),
        cog_workers=0,
        cog_compress=args.cog_compress,
        cog_predictor=args.cog_predictor,
        cog_level=args.cog_level,
        cog_num_threads=args.cog_num_threads,
        target_crs=args.target_crs,
        grid_type=grid_type,
        grid_level=grid_level,
        cover_mode=args.cover_mode,
        time_granularity=args.time_granularity,
        max_cells_per_asset=args.max_cells_per_asset,
        ray_parallelism=ray_parallelism,
        ray_address=args.ray_address,
        chunk_size=chunk_size,
        partition_backend=args.partition_backend,
        partition_prefix_len=3,
        timing_mode=bool(args.timing_mode),
        skip_verify=False,
        sample_mean=bool(args.sample_mean),
        job_id="",
        data_type="optical",
        dataset="shandong_optical_logical_bench",
        sensor="optical_mosaic",
        asset_version="v1",
        cube_version="v1",
        quality_rule="best_quality_wins",
        metadata_backend="none",
        postgres_dsn="",
        db_path="",
        asset_storage_backend="minio",
        minio_endpoint=args.minio_endpoint,
        minio_access_key=args.minio_access_key,
        minio_secret_key=args.minio_secret_key,
        minio_bucket=args.minio_bucket,
        minio_prefix=minio_prefix,
        minio_secure=bool(args.minio_secure),
        minio_upload_workers=4,
        cog_output_root=str(output_dir / "local_cog"),
        cog_materialize_mode="copy",
    )


def _compact_report(report: dict[str, Any]) -> dict[str, Any]:
    return {key: report.get(key) for key in REPORT_FIELDS if key in report}


def _write_markdown(summary: dict[str, Any], path: Path) -> None:
    lines = [
        "# Logical Partition Benchmark Summary",
        "",
        f"- run_id: `{summary['run_id']}`",
        f"- command: `{summary['command']}`",
        f"- manifest: `{summary['manifest_path']}`",
        f"- source_cache_dir: `{summary['source_cache_dir']}`",
        "",
        "| case | grid | level | parallelism | chunk | assets | grid tasks | rows | partition s | total s | worker source s | worker cog write s | worker cog upload s | worker rows s | cog writes/uploads | MinIO prefix |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for case in summary["cases"]:
        report = case["report"]
        lines.append(
            "| {case_id} | {grid_type} | {grid_level} | {ray_parallelism} | {chunk_size} | "
            "{source_asset_count} | {grid_task_count} | {total_index_rows} | {partition_elapsed_sec} | "
            "{total_elapsed_sec} | {worker_source_resolve_elapsed_sec} | {worker_cog_write_elapsed_sec} | "
            "{worker_cog_upload_elapsed_sec} | {worker_partition_rows_elapsed_sec} | "
            "{worker_cog_write_count}/{worker_cog_upload_count} | `{minio_prefix}` |".format(
                case_id=case["case_id"],
                grid_type=case["grid_type"],
                grid_level=case["grid_level"],
                ray_parallelism=case["ray_parallelism"],
                chunk_size=case["chunk_size"],
                source_asset_count=report.get("source_asset_count", ""),
                grid_task_count=report.get("grid_task_count", ""),
                total_index_rows=report.get("total_index_rows", ""),
                partition_elapsed_sec=report.get("partition_elapsed_sec", ""),
                total_elapsed_sec=report.get("total_elapsed_sec", ""),
                worker_source_resolve_elapsed_sec=report.get("worker_source_resolve_elapsed_sec", ""),
                worker_cog_write_elapsed_sec=report.get("worker_cog_write_elapsed_sec", ""),
                worker_cog_upload_elapsed_sec=report.get("worker_cog_upload_elapsed_sec", ""),
                worker_partition_rows_elapsed_sec=report.get("worker_partition_rows_elapsed_sec", ""),
                worker_cog_write_count=report.get("worker_cog_write_count", ""),
                worker_cog_upload_count=report.get("worker_cog_upload_count", ""),
                minio_prefix=report.get("minio_prefix", ""),
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    if args.repeat <= 0:
        raise ValueError("repeat must be > 0")

    work_dir = Path(args.work_dir) / args.run_id
    input_dir = work_dir / "input"
    manifest_path = Path(args.manifest_path) if args.manifest_path else work_dir / "manifest" / "shandong_2020q3_2band.jsonl"
    if not args.manifest_path:
        _write_default_manifest(manifest_path)
    input_dir.mkdir(parents=True, exist_ok=True)
    _validate_remote_settings(args, manifest_path)
    if args.source_cache_dir:
        os.environ["CUBE_SOURCE_CACHE_DIR"] = args.source_cache_dir

    summary: dict[str, Any] = {
        "run_id": args.run_id,
        "command": _redacted_argv(sys.argv),
        "manifest_path": str(manifest_path.resolve()),
        "source_cache_dir": str(args.source_cache_dir),
        "cases": [],
    }
    parallelism_values = _csv_ints(args.ray_parallelism_values)
    chunk_sizes = _csv_ints(args.chunk_sizes)

    for repeat_idx in range(1, args.repeat + 1):
        for grid_type, grid_level in _grid_cases(args):
            for ray_parallelism in parallelism_values:
                for chunk_size in chunk_sizes:
                    case_id = f"{grid_type}_l{grid_level}_p{ray_parallelism}_c{chunk_size}_r{repeat_idx}"
                    print(f"=== logical benchmark {case_id} ===", flush=True)
                    job_args = _job_args(
                        args,
                        case_id=case_id,
                        manifest_path=manifest_path,
                        input_dir=input_dir,
                        output_dir=work_dir / "output" / case_id,
                        cog_input_dir=work_dir / "cog" / case_id,
                        grid_type=grid_type,
                        grid_level=grid_level,
                        ray_parallelism=ray_parallelism,
                        chunk_size=chunk_size,
                    )
                    report = run_logical_partition(job_args)
                    summary["cases"].append(
                        {
                            "case_id": case_id,
                            "grid_type": grid_type,
                            "grid_level": grid_level,
                            "ray_parallelism": ray_parallelism,
                            "chunk_size": chunk_size,
                            "report": _compact_report(report),
                            "report_path": str(Path(report["run_dir"]) / "job_report.json"),
                        }
                    )

    summary_path = work_dir / "summary.json"
    markdown_path = work_dir / "summary.md"
    summary["summary_path"] = str(summary_path)
    summary["markdown_path"] = str(markdown_path)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_markdown(summary, markdown_path)
    print(json.dumps({"summary_path": str(summary_path), "markdown_path": str(markdown_path)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
