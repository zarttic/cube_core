#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from cube_split.jobs.ray_partition_core import (
    _group_tasks_for_local_processing,
    _prepare_task_rows_for_partitioning,
    _process_local_task_group,
    asset_record_to_dict,
    build_grid_tasks_driver,
    build_manifest,
    cog_creation_options,
    convert_assets_to_cog,
)
from cube_split.ingest.ray_ingest_job import (
    DEFAULT_MINIO_ACCESS_KEY,
    DEFAULT_MINIO_BUCKET,
    DEFAULT_MINIO_ENDPOINT,
    DEFAULT_MINIO_SECRET_KEY,
    DEFAULT_POSTGRES_DSN,
)
from cube_split.jobs.ray_logical_partition_job import (
    DEFAULT_RAY_ADDRESS,
    _chunk_tasks_for_ray,
    _load_ray,
    _prepend_sys_paths,
    _ray_actor_options_from_env,
    _ray_project_roots,
    _ray_runtime_env_from_env,
    _resolve_ray_chunk_size,
    _resolve_ray_parallelism,
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
    parser.add_argument("--manifest-path", default="", help="Optional selected product asset manifest")
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
    parser.add_argument("--partition-backend", default="ray", choices=["auto", "ray", "thread"], help="Partition backend")
    parser.add_argument("--ray-address", default=DEFAULT_RAY_ADDRESS, help="Ray address, e.g. auto or ray://host:10001")
    parser.add_argument("--ray-parallelism", type=int, default=0, help="Ray worker count; 0 means auto")
    parser.add_argument("--chunk-size", type=int, default=0, help="Number of grouped tasks per Ray chunk; 0 means auto")
    parser.add_argument("--sample-mean", action="store_true", help="Compute per-window sample mean")
    parser.add_argument("--job-id", default="", help="Ingest job id; defaults to run directory name when ingest is enabled")
    parser.add_argument("--dataset", default="dianzhong_ecological_security", help="Dataset name")
    parser.add_argument("--product-name", default="滇中地区30米生态安全评价数据集", help="Product name")
    parser.add_argument("--asset-version", default="v1", help="Asset version")
    parser.add_argument("--cube-version", default="product_v1", help="Product cube version")
    parser.add_argument("--metadata-backend", default="postgres", choices=["none", "sqlite", "postgres"], help="Metadata backend")
    parser.add_argument("--postgres-dsn", default=DEFAULT_POSTGRES_DSN, help="PostgreSQL DSN when metadata-backend=postgres")
    parser.add_argument("--db-path", default="data/ingest/product_ingest.db", help="SQLite DB path")
    parser.add_argument("--asset-storage-backend", default="minio", choices=["local", "minio"], help="Asset storage backend")
    parser.add_argument("--minio-endpoint", default=DEFAULT_MINIO_ENDPOINT, help="MinIO endpoint host:port")
    parser.add_argument("--minio-access-key", default=DEFAULT_MINIO_ACCESS_KEY, help="MinIO access key")
    parser.add_argument("--minio-secret-key", default=DEFAULT_MINIO_SECRET_KEY, help="MinIO secret key")
    parser.add_argument("--minio-bucket", default=DEFAULT_MINIO_BUCKET, help="MinIO bucket name")
    parser.add_argument("--minio-prefix", default="cube/product", help="Object key prefix")
    parser.add_argument("--minio-secure", action="store_true", help="Use TLS for MinIO")
    parser.add_argument("--minio-upload-workers", type=int, default=8, help="Parallel upload workers")
    parser.add_argument("--cog-output-root", default="data/cog/product_raw", help="Local asset materialization root")
    parser.add_argument("--cog-materialize-mode", default="copy", choices=["copy", "hardlink", "symlink"], help="Local materialization mode")
    return parser.parse_args()


def _resolve_backend(requested_backend: str, ray_address: str) -> str:
    if requested_backend == "auto":
        return "ray" if ray_address else "thread"
    return requested_backend


def _process_group_chunk(chunk: list[list[dict]], include_sample_mean: bool) -> list[dict]:
    rows: list[dict] = []
    for group in chunk:
        rows.extend(_process_local_task_group(group, "day", include_sample_mean=include_sample_mean))
    return rows


def _partition_groups_thread(grouped_tasks: list[list[dict]], workers: int, include_sample_mean: bool) -> list[dict]:
    worker_count = max(1, workers)
    if worker_count == 1:
        return _process_group_chunk(grouped_tasks, include_sample_mean)

    rows: list[dict] = []
    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        futures = [
            pool.submit(_process_local_task_group, group, "day", include_sample_mean)
            for group in grouped_tasks
        ]
        for future in futures:
            rows.extend(future.result())
    return rows


def _partition_groups_ray(
    task_chunks: list[list[list[dict]]],
    parallelism: int,
    ray_address: str,
    include_sample_mean: bool,
    assets_by_path: dict[str, dict],
    cog_input_dir: str,
    cog_overwrite: bool,
    cog_options: dict[str, str],
    target_crs: str,
    source_options: dict,
    cog_upload_options: dict,
) -> tuple[list[dict], float]:
    ray = _load_ray()
    runtime_env = _ray_runtime_env_from_env()
    ray_init_start = time.perf_counter()
    if ray_address:
        try:
            ray.init(
                address=ray_address,
                ignore_reinit_error=True,
                include_dashboard=False,
                logging_level="ERROR",
                runtime_env=runtime_env,
            )
        except Exception:
            if ray_address != "auto":
                raise
            ray.init(ignore_reinit_error=True, include_dashboard=False, logging_level="ERROR", runtime_env=runtime_env)
    else:
        ray.init(ignore_reinit_error=True, include_dashboard=False, logging_level="ERROR", runtime_env=runtime_env)
    ray_init_elapsed = time.perf_counter() - ray_init_start

    @ray.remote
    class ProductTaskProcessor:
        def process_groups(
            self,
            task_groups: list[list[dict]],
            include_sample_mean_value: bool,
            assets_by_path_value: dict[str, dict],
            cog_input_dir_value: str,
            cog_overwrite_value: bool,
            cog_options_value: dict[str, str],
            target_crs_value: str,
            source_options_value: dict,
            cog_upload_options_value: dict,
        ) -> list[dict]:
            import os
            from pathlib import Path

            _prepend_sys_paths(
                [
                    os.path.abspath(os.path.join(project_root, rel_path))
                    for project_root in _ray_project_roots()
                    for rel_path in ("", "cube_encoder", "cube_split", "cube_web")
                ]
            )

            from cube_split.jobs.product_partition_job import _process_group_chunk
            from cube_split.jobs.ray_partition_core import asset_record_from_dict, convert_asset_to_cog, upload_cog_to_minio

            env_options = dict(cog_upload_options_value or source_options_value or {})
            if env_options.get("endpoint"):
                os.environ["CUBE_WEB_MINIO_ENDPOINT"] = str(env_options["endpoint"])
            if env_options.get("access_key"):
                os.environ["CUBE_WEB_MINIO_ACCESS_KEY"] = str(env_options["access_key"])
            if env_options.get("secret_key"):
                os.environ["CUBE_WEB_MINIO_SECRET_KEY"] = str(env_options["secret_key"])

            prepared_groups: list[list[dict]] = []
            cog_uri_by_source: dict[str, str] = {}
            worker_cog_root = Path(cog_input_dir_value) / f"ray_worker_{os.getpid()}"
            for group in task_groups:
                if not group:
                    continue
                source_path = str(group[0]["asset_path"])
                cog_uri = cog_uri_by_source.get(source_path)
                if cog_uri is None:
                    asset = asset_record_from_dict(assets_by_path_value[source_path])
                    converted = convert_asset_to_cog(
                        asset,
                        cog_input_dir=worker_cog_root,
                        overwrite=cog_overwrite_value,
                        creation_options=cog_options_value,
                        target_crs=target_crs_value or None,
                        source_options=source_options_value,
                    )
                    cog_uri = upload_cog_to_minio(converted, Path(converted.path), cog_upload_options_value)
                    cog_uri_by_source[source_path] = cog_uri
                prepared_groups.append([{**row, "asset_path": cog_uri} for row in group])

            return _process_group_chunk(prepared_groups, include_sample_mean_value)

    actor_cls = ProductTaskProcessor.options(**_ray_actor_options_from_env())
    actors = [actor_cls.remote() for _ in range(parallelism)]
    futures = [
        actors[idx % parallelism].process_groups.remote(
            chunk,
            include_sample_mean,
            assets_by_path,
            cog_input_dir,
            cog_overwrite,
            cog_options,
            target_crs,
            source_options,
            cog_upload_options,
        )
        for idx, chunk in enumerate(task_chunks)
    ]
    rows: list[dict] = []
    try:
        for chunk_rows in ray.get(futures):
            rows.extend(chunk_rows)
    finally:
        ray.shutdown()
    return rows, ray_init_elapsed


def _should_run_ingest(args: argparse.Namespace) -> bool:
    explicit = getattr(args, "ingest_enabled", None)
    if explicit is not None:
        return bool(explicit)
    metadata_backend = str(getattr(args, "metadata_backend", "none") or "none")
    return metadata_backend in {"sqlite", "postgres"}


def _run_product_partition_ingest(args: argparse.Namespace, run_dir: Path) -> dict[str, Any]:
    metadata_backend = str(getattr(args, "metadata_backend", "none") or "none")
    if metadata_backend not in {"sqlite", "postgres"}:
        raise ValueError("metadata_backend must be sqlite or postgres when ingest is enabled")

    from cube_split.ingest.product_ingest_job import run_product_ingest

    ingest_args = argparse.Namespace(**vars(args))
    ingest_args.run_dir = str(run_dir)
    ingest_args.job_id = str(getattr(args, "job_id", "") or "").strip() or run_dir.name
    return run_product_ingest(ingest_args)


def run_product_partition(args: argparse.Namespace) -> dict:
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    total_start = time.perf_counter()
    backend_requested = str(getattr(args, "partition_backend", "thread") or "thread")
    ray_address = str(getattr(args, "ray_address", "") or "")
    backend = _resolve_backend(backend_requested, ray_address)
    if backend not in {"ray", "thread"}:
        raise ValueError("partition_backend must be one of: auto, ray, thread")

    manifest_path_raw = str(getattr(args, "manifest_path", "") or "").strip()
    manifest_path = Path(manifest_path_raw).expanduser() if manifest_path_raw else None
    source_assets = build_manifest(
        input_dir,
        data_type="product",
        manifest_path=manifest_path,
    )
    if not source_assets:
        raise RuntimeError(f"No product TIF assets found under: {input_dir}")

    cog_start = time.perf_counter()
    if backend == "ray":
        assets = source_assets
    else:
        assets = convert_assets_to_cog(
            source_assets,
            cog_input_dir=Path(args.cog_input_dir),
            overwrite=bool(args.cog_overwrite),
            workers=int(args.cog_workers),
            compress="LZW",
            predictor=0,
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

    if grouped_tasks:
        if backend == "ray":
            parallelism = _resolve_ray_parallelism(len(grouped_tasks), int(getattr(args, "ray_parallelism", 0) or 0))
        else:
            parallelism = int(getattr(args, "partition_workers", 0) or 0) or min(len(grouped_tasks), 8)
        chunk_size = _resolve_ray_chunk_size(len(grouped_tasks), parallelism, int(getattr(args, "chunk_size", 0) or 0))
        task_chunks = _chunk_tasks_for_ray(grouped_tasks, chunk_size)
    else:
        parallelism = 1
        chunk_size = 0
        task_chunks = []

    worker_count = max(1, parallelism)
    ray_init_elapsed = 0.0
    start = time.perf_counter()
    if not grouped_tasks:
        out_rows = []
    elif backend == "ray":
        assets_by_path = {asset.path: asset_record_to_dict(asset) for asset in assets}
        out_rows, ray_init_elapsed = _partition_groups_ray(
            task_chunks=task_chunks,
            parallelism=worker_count,
            ray_address=ray_address,
            include_sample_mean=bool(args.sample_mean),
            assets_by_path=assets_by_path,
            cog_input_dir=str(args.cog_input_dir),
            cog_overwrite=bool(args.cog_overwrite),
            cog_options=cog_creation_options("LZW", predictor=0, overviews="NONE", num_threads="ALL_CPUS"),
            target_crs=str(args.target_crs or ""),
            source_options={
                "endpoint": str(getattr(args, "minio_endpoint", "")),
                "access_key": str(getattr(args, "minio_access_key", "")),
                "secret_key": str(getattr(args, "minio_secret_key", "")),
                "secure": bool(getattr(args, "minio_secure", False)),
            },
            cog_upload_options={
                "endpoint": str(getattr(args, "minio_endpoint", "")),
                "access_key": str(getattr(args, "minio_access_key", "")),
                "secret_key": str(getattr(args, "minio_secret_key", "")),
                "secure": bool(getattr(args, "minio_secure", False)),
                "bucket": str(getattr(args, "minio_bucket", "")),
                "prefix": str(getattr(args, "minio_prefix", "cube/product")),
                "dataset": str(getattr(args, "dataset", "dianzhong_ecological_security")),
                "sensor": "data_product",
                "asset_version": str(getattr(args, "asset_version", "v1")),
            },
        )
    else:
        out_rows = _partition_groups_thread(grouped_tasks, worker_count, bool(args.sample_mean))
    partition_elapsed = time.perf_counter() - start

    with rows_path.open("w", encoding="utf-8") as fh:
        for row in out_rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    ingest_enabled = _should_run_ingest(args)
    ingest_stats: dict[str, Any] | None = None
    ingest_elapsed = 0.0
    if ingest_enabled:
        ingest_start = time.perf_counter()
        ingest_stats = _run_product_partition_ingest(args, run_dir)
        ingest_elapsed = time.perf_counter() - ingest_start

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
        "execution_engine": backend,
        "partition_backend_requested": backend_requested,
        "partition_backend_used": backend,
        "partition_workers": worker_count if backend == "thread" else 0,
        "ray_parallelism": worker_count if backend == "ray" else 0,
        "ray_address": ray_address if backend == "ray" else "",
        "ray_init_elapsed_sec": round(ray_init_elapsed, 3),
        "chunk_size": chunk_size,
        "ingest_enabled": ingest_enabled,
        "ingest_stats": ingest_stats,
        "metadata_backend": str(getattr(args, "metadata_backend", "none") or "none"),
        "asset_storage_backend": str(getattr(args, "asset_storage_backend", "local") or "local"),
        "dataset": str(getattr(args, "dataset", "")),
        "asset_version": str(getattr(args, "asset_version", "")),
        "cube_version": str(getattr(args, "cube_version", "")),
        "minio_bucket": str(getattr(args, "minio_bucket", "")) if str(getattr(args, "asset_storage_backend", "local") or "local") == "minio" else "",
        "minio_prefix": str(getattr(args, "minio_prefix", "")) if str(getattr(args, "asset_storage_backend", "local") or "local") == "minio" else "",
        "cog_elapsed_sec": round(cog_elapsed, 3),
        "partition_elapsed_sec": round(partition_elapsed, 3),
        "ingest_elapsed_sec": round(ingest_elapsed, 3),
        "total_elapsed_sec": round(time.perf_counter() - total_start, 3),
    }
    report_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    print(json.dumps(run_product_partition(parse_args()), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
