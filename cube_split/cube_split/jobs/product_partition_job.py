#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor
from inspect import signature
from pathlib import Path
from typing import Any

from cube_split import runtime_config
from cube_split.jobs.cancellation import PartitionCancelledError, cancel_ray_refs, check_cancelled, shutdown_ray_if_needed
from cube_split.jobs.ray_logical_partition_job import (
    _chunk_task_groups_by_actor,
    _chunk_tasks_for_ray,
    _load_ray,
    _prepend_sys_paths,
    _ray_actor_options_from_env,
    _ray_project_roots,
    _ray_runtime_env_from_env,
    _resolve_ray_actor_parallelism,
    _resolve_ray_chunk_size,
)
from cube_split.jobs.ray_partition_core import (
    _group_tasks_for_local_processing,
    _prepare_task_rows_for_partitioning,
    _process_local_task_group,
    asset_record_to_dict,
    build_grid_tasks_driver,
    build_manifest,
    cog_creation_options,
    convert_assets_to_cog,
    create_unique_run_dir,
    upload_source_assets_to_minio,
)
from cube_split.partition.product_products import parse_product_asset


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
    minio = runtime_config.minio_settings()
    parser.add_argument("--input-dir", default="data/product", help="Input directory containing product TIF files")
    parser.add_argument("--manifest-path", default="", help="Optional selected product asset manifest")
    parser.add_argument("--output-dir", default="data/ray_output/product", help="Output directory")
    parser.add_argument("--cog-input-dir", default="data/cog/product", help="Directory for standardized product COGs")
    parser.add_argument("--target-crs", default="", help="Optional target CRS for standardized COG assets. Empty keeps source CRS.")
    parser.add_argument("--grid-type", default="geohash", choices=["geohash", "mgrs"], help="Grid type")
    parser.add_argument("--grid-level", type=int, default=5, help="Grid level")
    parser.add_argument("--cover-mode", default="intersect", choices=["intersect", "contain", "minimal"], help="Cover mode")
    parser.add_argument("--max-cells-per-asset", type=int, default=0, help="Safety limit for cover cells per asset (0 disables)")
    parser.add_argument("--partition-prefix-len", type=int, default=3, help="Prefix length used in row grouping")
    parser.add_argument("--cog-overwrite", action="store_true", help="Force reconvert source TIF files to COG")
    parser.add_argument("--cog-workers", type=int, default=0, help="Parallel workers for COG conversion")
    parser.add_argument("--partition-workers", type=int, default=0, help="Parallel workers for partition stage")
    parser.add_argument("--partition-backend", default="ray", choices=["auto", "ray", "thread"], help="Partition backend")
    parser.add_argument("--ray-address", default=runtime_config.ray_address(), help="Ray address, e.g. auto or ray://host:10001")
    parser.add_argument("--ray-parallelism", type=int, default=0, help="Ray worker count; 0 means auto")
    parser.add_argument("--chunk-size", type=int, default=0, help="Number of grouped tasks per Ray chunk; 0 means auto")
    parser.add_argument("--sample-mean", action="store_true", help="Compute per-window sample mean")
    parser.add_argument("--job-id", default="", help="Ingest job id; defaults to run directory name when ingest is enabled")
    parser.add_argument("--dataset", default="dianzhong_ecological_security", help="Dataset name")
    parser.add_argument("--product-name", default="滇中地区30米生态安全评价数据集", help="Product name")
    parser.add_argument("--asset-version", default="v1", help="Asset version")
    parser.add_argument("--cube-version", default="product_v1", help="Product cube version")
    parser.add_argument("--metadata-backend", default="postgres", choices=["none", "sqlite", "postgres"], help="Metadata backend")
    parser.add_argument("--postgres-dsn", default=runtime_config.postgres_dsn(), help="PostgreSQL DSN when metadata-backend=postgres")
    parser.add_argument("--db-path", default="data/ingest/product_ingest.db", help="SQLite DB path")
    parser.add_argument("--asset-storage-backend", default="minio", choices=["local", "minio"], help="Asset storage backend")
    parser.add_argument("--minio-endpoint", default=minio.endpoint, help="MinIO endpoint host:port")
    parser.add_argument("--minio-access-key", default=minio.access_key, help="MinIO access key")
    parser.add_argument("--minio-secret-key", default=minio.secret_key, help="MinIO secret key")
    parser.add_argument("--minio-bucket", default=minio.bucket, help="MinIO bucket name")
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
    rows = [row for group in chunk for row in group]
    return _process_local_task_group(rows, "day", include_sample_mean=include_sample_mean)


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
    cancellation_check: Any | None = None,
) -> tuple[list[dict], float, dict[str, float]]:
    chunk_size = max((len(chunk) for chunk in task_chunks), default=1)
    task_groups = [group for chunk in task_chunks for group in chunk]
    task_chunks_by_actor = _chunk_task_groups_by_actor(task_groups, parallelism, chunk_size)

    ray = _load_ray()
    runtime_env = _ray_runtime_env_from_env()
    ray_init_start = time.perf_counter()
    ray_already_initialized = bool(getattr(ray, "is_initialized", lambda: False)())
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
        def __init__(self) -> None:
            self._local_cog_by_source: dict[str, str] = {}
            self._converted_asset_by_source: dict[str, dict[str, Any]] = {}
            self._remote_cog_by_local_path: dict[str, str] = {}

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
        ) -> dict[str, Any]:
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
            from cube_split.jobs.ray_partition_core import (
                _prepare_actor_cog_groups,
                _upload_actor_cogs,
            )

            env_options = dict(cog_upload_options_value or source_options_value or {})
            if env_options.get("endpoint"):
                os.environ["CUBE_WEB_MINIO_ENDPOINT"] = str(env_options["endpoint"])
            if env_options.get("access_key"):
                os.environ["CUBE_WEB_MINIO_ACCESS_KEY"] = str(env_options["access_key"])
            if env_options.get("secret_key"):
                os.environ["CUBE_WEB_MINIO_SECRET_KEY"] = str(env_options["secret_key"])

            stats: dict[str, float] = {
                "source_resolve_elapsed_sec": 0.0,
                "cog_write_elapsed_sec": 0.0,
                "cog_upload_elapsed_sec": 0.0,
                "partition_rows_elapsed_sec": 0.0,
                "cog_write_count": 0.0,
                "cog_cache_hit_count": 0.0,
                "cog_upload_count": 0.0,
            }
            worker_cog_root = Path(cog_input_dir_value) / f"ray_worker_{os.getpid()}"
            prepared_groups, used_local_cog_paths = _prepare_actor_cog_groups(
                task_groups,
                assets_by_path=assets_by_path_value,
                local_cog_by_source=self._local_cog_by_source,
                converted_asset_by_source=self._converted_asset_by_source,
                cog_input_dir=worker_cog_root,
                cog_overwrite=cog_overwrite_value,
                cog_options=cog_options_value,
                target_crs=target_crs_value,
                source_options=source_options_value,
                timing=stats,
            )
            partition_rows_start = time.perf_counter()
            rows = _process_group_chunk(prepared_groups, include_sample_mean_value)
            stats["partition_rows_elapsed_sec"] += time.perf_counter() - partition_rows_start
            _upload_actor_cogs(
                local_cog_by_source=self._local_cog_by_source,
                converted_asset_by_source=self._converted_asset_by_source,
                remote_cog_by_local_path=self._remote_cog_by_local_path,
                used_local_cog_paths=used_local_cog_paths,
                cog_upload_options=cog_upload_options_value,
                timing=stats,
            )
            for row in rows:
                row["asset_path"] = self._remote_cog_by_local_path.get(str(row["asset_path"]), row["asset_path"])
            return {"rows": rows, "stats": stats}

    actor_cls = ProductTaskProcessor.options(**_ray_actor_options_from_env())
    actors = [actor_cls.remote() for _ in range(parallelism)]
    rows: list[dict] = []
    ray_worker_stats: dict[str, float] = {}

    def submit_chunk(actor_idx: int, chunk_idx: int):
        return actors[actor_idx].process_groups.remote(
            task_chunks_by_actor[actor_idx][chunk_idx],
            include_sample_mean,
            assets_by_path,
            cog_input_dir,
            cog_overwrite,
            cog_options,
            target_crs,
            source_options,
            cog_upload_options,
        )

    try:
        pending = []
        pending_actor_by_ref: dict[Any, int] = {}
        next_chunk_by_actor = [0 for _ in range(parallelism)]
        for actor_idx, actor_chunks in enumerate(task_chunks_by_actor):
            if not actor_chunks:
                continue
            if cancellation_check is not None and cancellation_check():
                raise PartitionCancelledError("Partition task cancelled")
            ref = submit_chunk(actor_idx, 0)
            pending.append(ref)
            pending_actor_by_ref[ref] = actor_idx
            next_chunk_by_actor[actor_idx] = 1
        while pending:
            if cancellation_check is not None and cancellation_check():
                raise PartitionCancelledError("Partition task cancelled")
            ready, pending = ray.wait(pending, num_returns=1, timeout=1.0)
            if not ready:
                continue
            for ready_ref in ready:
                actor_idx = pending_actor_by_ref.pop(ready_ref)
                result = ray.get(ready_ref)
                if isinstance(result, dict) and "rows" in result:
                    rows.extend(result["rows"])
                    for key, value in dict(result.get("stats") or {}).items():
                        ray_worker_stats[key] = ray_worker_stats.get(key, 0.0) + float(value)
                else:
                    rows.extend(result)
                if next_chunk_by_actor[actor_idx] < len(task_chunks_by_actor[actor_idx]):
                    if cancellation_check is not None and cancellation_check():
                        raise PartitionCancelledError("Partition task cancelled")
                    ref = submit_chunk(actor_idx, next_chunk_by_actor[actor_idx])
                    pending.append(ref)
                    pending_actor_by_ref[ref] = actor_idx
                    next_chunk_by_actor[actor_idx] += 1
    except PartitionCancelledError:
        cancel_ray_refs(ray, pending)
        raise
    finally:
        shutdown_ray_if_needed(ray, ray_already_initialized)
    return rows, ray_init_elapsed, ray_worker_stats


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
    if str(getattr(args, "partition_backend", "thread") or "thread") == "ray":
        ingest_args.asset_storage_backend = "minio"
    return run_product_ingest(ingest_args)


def _resolve_product_name(args: argparse.Namespace, source_assets: list[Any]) -> str:
    explicit = str(getattr(args, "product_name", "") or "").strip()
    if explicit:
        return explicit
    if not source_assets:
        raise ValueError("product_name is required when no source assets are available")
    return parse_product_asset(Path(str(source_assets[0].path))).product_name


def run_product_partition(args: argparse.Namespace) -> dict:
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")
    grid_type = str(getattr(args, "grid_type", "geohash") or "").strip().lower()
    if grid_type not in {"geohash", "mgrs"}:
        raise ValueError("grid_type must be one of: geohash, mgrs")
    args.grid_type = grid_type
    check_cancelled(args)

    total_start = time.perf_counter()
    backend_requested = str(getattr(args, "partition_backend", "thread") or "thread")
    ray_address = str(getattr(args, "ray_address", "") or "")
    backend = _resolve_backend(backend_requested, ray_address)
    if backend not in {"ray", "thread"}:
        raise ValueError("partition_backend must be one of: auto, ray, thread")
    manifest_path_raw = str(getattr(args, "manifest_path", "") or "").strip()
    manifest_path = Path(manifest_path_raw).expanduser() if manifest_path_raw else None
    source_uploader = None
    if backend == "ray":

        def source_uploader(assets: list[Any]) -> list[Any]:
            return upload_source_assets_to_minio(
                assets,
                prefix="cube/source",
                options={
                    "endpoint": str(getattr(args, "minio_endpoint", "")),
                    "access_key": str(getattr(args, "minio_access_key", "")),
                    "secret_key": str(getattr(args, "minio_secret_key", "")),
                    "secure": bool(getattr(args, "minio_secure", False)),
                    "bucket": str(getattr(args, "minio_bucket", "")),
                    "dataset": str(getattr(args, "dataset", "dianzhong_ecological_security")),
                    "sensor": str(getattr(args, "sensor", "data_product")),
                    "asset_version": str(getattr(args, "asset_version", "v1")),
                },
            )
    source_assets = build_manifest(
        input_dir,
        data_type="product",
        manifest_path=manifest_path,
        **({"source_uploader": source_uploader} if source_uploader is not None else {}),
    )
    if not source_assets:
        raise RuntimeError(f"No product TIF assets found under: {input_dir}")
    args.product_name = _resolve_product_name(args, source_assets)
    check_cancelled(args)

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
            cancellation_check=getattr(args, "cancellation_check", None),
        )
    cog_elapsed = time.perf_counter() - cog_start
    check_cancelled(args)

    grid_tasks = build_grid_tasks_driver(
        assets=assets,
        grid_type=args.grid_type,
        grid_level=int(args.grid_level),
        cover_mode=args.cover_mode,
        max_cells_per_asset=int(args.max_cells_per_asset),
    )
    task_rows = _prepare_product_task_rows(grid_tasks, partition_prefix_len=int(args.partition_prefix_len))
    grouped_tasks = _group_tasks_for_local_processing(task_rows)
    check_cancelled(args)

    run_dir = create_unique_run_dir(output_dir)
    rows_path = run_dir / "index_rows.jsonl"
    report_path = run_dir / "job_report.json"

    requested_asset_storage_backend = str(getattr(args, "asset_storage_backend", "local") or "local")
    if requested_asset_storage_backend not in {"local", "minio"}:
        raise ValueError("asset_storage_backend must be one of: local, minio")
    asset_storage_backend = "minio" if backend == "ray" else requested_asset_storage_backend

    if grouped_tasks:
        if backend == "ray":
            parallelism = _resolve_ray_actor_parallelism(grouped_tasks, int(getattr(args, "ray_parallelism", 0) or 0))
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
    ray_worker_stats: dict[str, float] = {}
    start = time.perf_counter()
    if not grouped_tasks:
        out_rows = []
    elif backend == "ray":
        assets_by_path = {asset.path: asset_record_to_dict(asset) for asset in assets}
        partition_kwargs = {
            "task_chunks": task_chunks,
            "parallelism": worker_count,
            "ray_address": ray_address,
            "include_sample_mean": bool(args.sample_mean),
            "assets_by_path": assets_by_path,
            "cog_input_dir": str(args.cog_input_dir),
            "cog_overwrite": bool(args.cog_overwrite),
            "cog_options": cog_creation_options("LZW", predictor=2, overviews="NONE", num_threads="ALL_CPUS"),
            "target_crs": str(args.target_crs or ""),
            "source_options": {
                "endpoint": str(getattr(args, "minio_endpoint", "")),
                "access_key": str(getattr(args, "minio_access_key", "")),
                "secret_key": str(getattr(args, "minio_secret_key", "")),
                "secure": bool(getattr(args, "minio_secure", False)),
            },
            "cog_upload_options": {
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
        }
        if "cancellation_check" in signature(_partition_groups_ray).parameters:
            partition_kwargs["cancellation_check"] = getattr(args, "cancellation_check", None)
        partition_result = _partition_groups_ray(**partition_kwargs)
        out_rows = partition_result[0]
        ray_init_elapsed = partition_result[1]
        if len(partition_result) > 2:
            ray_worker_stats = dict(partition_result[2] or {})
    else:
        out_rows = _partition_groups_thread(grouped_tasks, worker_count, bool(args.sample_mean))
    partition_elapsed = time.perf_counter() - start
    check_cancelled(args)

    with rows_path.open("w", encoding="utf-8") as fh:
        for row in out_rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    ingest_enabled = _should_run_ingest(args)
    ingest_stats: dict[str, Any] | None = None
    ingest_elapsed = 0.0
    if ingest_enabled:
        check_cancelled(args)
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
        "asset_storage_backend": asset_storage_backend,
        "dataset": str(getattr(args, "dataset", "")),
        "asset_version": str(getattr(args, "asset_version", "")),
        "cube_version": str(getattr(args, "cube_version", "")),
        "minio_bucket": str(getattr(args, "minio_bucket", "")) if asset_storage_backend == "minio" else "",
        "minio_prefix": str(getattr(args, "minio_prefix", "")) if asset_storage_backend == "minio" else "",
        "cog_elapsed_sec": round(cog_elapsed, 3),
        "worker_source_resolve_elapsed_sec": round(ray_worker_stats.get("source_resolve_elapsed_sec", 0.0), 3),
        "worker_cog_write_elapsed_sec": round(ray_worker_stats.get("cog_write_elapsed_sec", 0.0), 3),
        "worker_cog_upload_elapsed_sec": round(ray_worker_stats.get("cog_upload_elapsed_sec", 0.0), 3),
        "worker_partition_rows_elapsed_sec": round(ray_worker_stats.get("partition_rows_elapsed_sec", 0.0), 3),
        "worker_cog_write_count": int(ray_worker_stats.get("cog_write_count", 0.0)),
        "worker_cog_cache_hit_count": int(ray_worker_stats.get("cog_cache_hit_count", 0.0)),
        "worker_cog_upload_count": int(ray_worker_stats.get("cog_upload_count", 0.0)),
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
