#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from cube_split import runtime_config
from cube_split.jobs.cancellation import PartitionCancelledError, cancel_ray_refs, check_cancelled, shutdown_ray_if_needed
from cube_split.jobs.ray_partition_core import (
    _group_tasks_for_local_processing,
    _prepare_task_rows_for_partitioning,
    asset_record_to_dict,
    build_grid_tasks_driver,
    build_manifest,
    cog_creation_options,
    convert_assets_to_cog,
    create_unique_run_dir,
    upload_source_assets_to_minio,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ray logical partition job for COG assets")
    parser.add_argument("--input-dir", default="data/landsat8", help="Input directory containing COG .TIF files")
    parser.add_argument(
        "--manifest-path",
        default="",
        help="Optional manifest file (.jsonl/.json) from ingest system; when set, input assets come from manifest rows.",
    )
    parser.add_argument(
        "--product-family",
        default="auto",
        help="Optical product family for filename parsing: auto, landsat, or sentinel2",
    )
    parser.add_argument("--output-dir", default="data/ray_output/logical_partition", help="Output directory")
    parser.add_argument(
        "--cog-input-dir",
        default="data/cog/partition_input",
        help="Directory for converted COG inputs used by partitioning",
    )
    parser.add_argument("--cog-overwrite", action="store_true", help="Force reconvert source TIF files to COG")
    parser.add_argument("--cog-workers", type=int, default=0, help="Parallel workers for TIF->COG conversion (0 means auto)")
    parser.add_argument("--cog-compress", default="LZW", choices=["NONE", "LZW", "DEFLATE", "ZSTD"], help="COG compression type")
    parser.add_argument("--cog-predictor", type=int, default=2, help="COG predictor (0 disables predictor option)")
    parser.add_argument("--cog-level", type=int, default=0, help="COG compression level (0 means driver default)")
    parser.add_argument(
        "--cog-num-threads",
        default="ALL_CPUS",
        help="COG encoder NUM_THREADS option (empty string disables)",
    )
    parser.add_argument(
        "--target-crs",
        default="",
        help="Optional target CRS for standardized COG assets, e.g. EPSG:4326. Empty keeps source CRS.",
    )
    parser.add_argument("--grid-type", default="s2", choices=["s2", "mgrs", "tile_matrix", "isea4h"], help="Grid type")
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
        default=runtime_config.ray_address(),
        help="Ray address to connect (e.g. auto or ray://host:10001).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=0,
        help="Number of asset groups per Ray task chunk (0 means auto)",
    )
    parser.add_argument(
        "--partition-backend",
        default="ray",
        choices=["auto", "ray", "thread"],
        help="Parallel backend for partition stage.",
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
    parser.add_argument("--job-id", default="", help="Ingest job id; defaults to run directory name when ingest is enabled")
    parser.add_argument("--data-type", default="optical", choices=["optical", "radar"], help="Input data type")
    parser.add_argument("--dataset", default="demo_optical", help="Dataset name used for metadata and object keys")
    parser.add_argument("--sensor", default="optical_mosaic", help="Sensor name used for metadata and object keys")
    parser.add_argument("--asset-version", default="v1", help="Raw asset version used for ingest")
    parser.add_argument("--cube-version", default="v1", help="Cube fact version used for ingest")
    parser.add_argument(
        "--quality-rule",
        default="best_quality_wins",
        choices=["best_quality_wins", "latest_wins"],
        help="Conflict resolution rule used during metadata ingest",
    )
    parser.add_argument(
        "--metadata-backend",
        default="postgres",
        choices=["none", "sqlite", "postgres"],
        help="Metadata backend. Set to postgres for end-to-end ingest after partitioning.",
    )
    parser.add_argument("--postgres-dsn", default=runtime_config.postgres_dsn(), help="PostgreSQL DSN when metadata-backend=postgres")
    parser.add_argument("--db-path", default="data/ingest/ingest.db", help="SQLite DB path when metadata-backend=sqlite")
    parser.add_argument(
        "--asset-storage-backend",
        default="minio",
        choices=["local", "minio"],
        help="Asset storage backend used during ingest",
    )
    parser.add_argument("--minio-endpoint", default=runtime_config.minio_settings().endpoint, help="MinIO endpoint host:port")
    parser.add_argument("--minio-access-key", default=runtime_config.minio_settings().access_key, help="MinIO access key")
    parser.add_argument("--minio-secret-key", default=runtime_config.minio_settings().secret_key, help="MinIO secret key")
    parser.add_argument("--minio-bucket", default=runtime_config.minio_settings().bucket, help="MinIO bucket name")
    parser.add_argument("--minio-prefix", default="cube/raw", help="MinIO object key prefix")
    parser.add_argument("--minio-secure", action="store_true", help="Use TLS for MinIO")
    parser.add_argument("--minio-upload-workers", type=int, default=8, help="Parallel upload workers for MinIO")
    parser.add_argument("--cog-output-root", default="data/cog/raw", help="Local asset materialization root")
    parser.add_argument(
        "--cog-materialize-mode",
        default="copy",
        choices=["copy", "hardlink", "symlink"],
        help="Local asset materialization mode when asset-storage-backend=local",
    )
    return parser.parse_args()


def _chunk_tasks_for_ray(tasks: list[Any], chunk_size: int) -> list[list[Any]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    return [tasks[idx : idx + chunk_size] for idx in range(0, len(tasks), chunk_size)]


def _resolve_ray_actor_parallelism(task_groups: list[list[dict]], requested_parallelism: int) -> int:
    parallelism = _resolve_ray_parallelism(len(task_groups), requested_parallelism)
    asset_count = len({str(group[0]["asset_path"]) for group in task_groups if group})
    return min(parallelism, max(1, asset_count))


def _chunk_task_groups_by_actor(
    task_groups: list[list[dict]],
    parallelism: int,
    chunk_size: int,
) -> list[list[list[list[dict]]]]:
    if parallelism <= 0:
        raise ValueError("parallelism must be > 0")
    buckets: list[list[list[dict]]] = [[] for _ in range(parallelism)]
    actor_by_asset: dict[str, int] = {}
    for group in task_groups:
        if not group:
            continue
        asset_path = str(group[0]["asset_path"])
        actor_idx = actor_by_asset.get(asset_path)
        if actor_idx is None:
            actor_idx = len(actor_by_asset) % parallelism
            actor_by_asset[asset_path] = actor_idx
        buckets[actor_idx].append(group)
    return [_chunk_tasks_for_ray(bucket, chunk_size) if bucket else [] for bucket in buckets]


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


def _ray_runtime_env_from_env() -> dict[str, Any] | None:
    raw = os.environ.get("RAY_RUNTIME_ENV_JSON", "").strip()
    if raw:
        loaded = json.loads(raw)
        if not isinstance(loaded, dict):
            raise ValueError("RAY_RUNTIME_ENV_JSON must decode to an object")
        return loaded

    project_root = Path(__file__).resolve().parents[3]
    env_vars = {
        "CUBE_PROJECT_ROOT": ".",
        "PYTHONPATH": ".:./cube_encoder:./cube_split:./cube_web",
    }
    source_cache_dir = os.environ.get("CUBE_SOURCE_CACHE_DIR", "").strip()
    if source_cache_dir:
        env_vars["CUBE_SOURCE_CACHE_DIR"] = source_cache_dir

    return {
        "working_dir": str(project_root),
        "excludes": [
            ".git/**",
            ".agents/**",
            ".cache/**",
            ".codegraph/**",
            ".codex/**",
            ".mypy_cache/**",
            ".ruff_cache/**",
            ".tmp/**",
            ".venv/**",
            "**/__pycache__/**",
            "**/.pytest_cache/**",
            "cube_split/*.gz",
            "cube_split/*.nc4",
            "cube_split/data/**",
            "cube_split/data_tmp/**",
            "cube_split/test_output/**",
            "cube_split/results/**",
            "cube_web/frontend/node_modules/**",
            "cube_web/frontend/dist/**",
            "cube_web/frontend/test-results/**",
            "cube_web/frontend/playwright-report/**",
        ],
        "env_vars": env_vars,
    }


def _ray_project_roots() -> list[str]:
    roots = [os.environ.get("CUBE_PROJECT_ROOT", ""), os.getcwd(), "/tmp/cube_project_ray_code"]
    return [root for root in roots if root]


def _prepend_sys_paths(paths: list[str]) -> None:
    import sys

    for path in reversed(paths):
        if os.path.isdir(path) and path not in sys.path:
            sys.path.insert(0, path)


def _ensure_ray_worker_project_paths() -> None:
    package_paths = [
        os.path.abspath(os.path.join(project_root, rel_path))
        for project_root in _ray_project_roots()
        for rel_path in ("", "cube_encoder", "cube_split", "cube_web")
    ]
    _prepend_sys_paths(package_paths)

    search_roots = [
        os.environ.get("RAY_RUNTIME_ENV_CREATE_WORKING_DIR", ""),
        "/tmp/ray/session_latest/runtime_resources/working_dir_files",
    ]
    for search_root in search_roots:
        if not search_root or not os.path.isdir(search_root):
            continue
        for dirpath, _, filenames in os.walk(search_root):
            if "entity_partition_job.py" not in filenames and "ray_partition_core.py" not in filenames:
                continue
            package_parent = os.path.abspath(os.path.join(dirpath, "..", ".."))
            _prepend_sys_paths([package_parent])


def _ray_actor_options_from_env() -> dict[str, Any]:
    node_resource = os.environ.get("RAY_ACTOR_NODE_RESOURCE", "").strip()
    if not node_resource:
        return {}
    return {"resources": {node_resource: 0.001}}


def _should_run_ingest(args: argparse.Namespace) -> bool:
    explicit = getattr(args, "ingest_enabled", None)
    if explicit is not None:
        return bool(explicit)
    metadata_backend = str(getattr(args, "metadata_backend", "none") or "none")
    return metadata_backend in {"sqlite", "postgres"}


def _run_partition_ingest(args: argparse.Namespace, run_dir: Path) -> dict[str, Any]:
    metadata_backend = str(getattr(args, "metadata_backend", "none") or "none")
    if metadata_backend not in {"sqlite", "postgres"}:
        raise ValueError("metadata_backend must be sqlite or postgres when ingest is enabled")

    from cube_split.ingest.ray_ingest_job import run_ingest

    ingest_args = argparse.Namespace(**vars(args))
    ingest_args.run_dir = str(run_dir)
    ingest_args.job_id = str(getattr(args, "job_id", "") or "").strip() or run_dir.name
    return run_ingest(ingest_args)


def run_logical_partition(args: argparse.Namespace) -> dict[str, Any]:
    total_start = time.perf_counter()
    check_cancelled(args)
    for key in ("SPARK_HOME", "SPARK_CONF_DIR", "HADOOP_CONF_DIR", "YARN_CONF_DIR"):
        os.environ.pop(key, None)

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")
    backend_requested = args.partition_backend
    if backend_requested == "auto":
        backend = "ray" if args.ray_address else "thread"
    else:
        backend = backend_requested

    source_uploader = None
    if backend == "ray":
        source_uploader = lambda assets: upload_source_assets_to_minio(
            assets,
            prefix="cube/source",
            options={
                "endpoint": str(getattr(args, "minio_endpoint", "")),
                "access_key": str(getattr(args, "minio_access_key", "")),
                "secret_key": str(getattr(args, "minio_secret_key", "")),
                "secure": bool(getattr(args, "minio_secure", False)),
                "bucket": str(getattr(args, "minio_bucket", "")),
                "dataset": str(getattr(args, "dataset", "demo")),
                "sensor": str(getattr(args, "sensor", "unknown")),
                "asset_version": str(getattr(args, "asset_version", "v1")),
            },
        )

    source_assets = build_manifest(
        input_dir,
        product_family=args.product_family,
        data_type=str(getattr(args, "data_type", "optical") or "optical"),
        manifest_path=(Path(args.manifest_path) if args.manifest_path else None),
        **({"source_uploader": source_uploader} if source_uploader is not None else {}),
    )
    if not source_assets:
        data_type = str(getattr(args, "data_type", "optical") or "optical")
        suffix_hint = ".dat/.TIF" if data_type == "radar" else ".TIF"
        raise RuntimeError(f"No {suffix_hint} assets found under: {input_dir}")
    check_cancelled(args)

    cog_start = time.perf_counter()
    if backend == "ray":
        assets = source_assets
    else:
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
            target_crs=(args.target_crs or None),
            cancellation_check=(getattr(args, "cancellation_check", None)),
        )
    cog_elapsed = time.perf_counter() - cog_start
    check_cancelled(args)

    grid_tasks = build_grid_tasks_driver(
        assets=assets,
        grid_type=args.grid_type,
        grid_level=args.grid_level,
        cover_mode=args.cover_mode,
        max_cells_per_asset=args.max_cells_per_asset,
    )
    if not grid_tasks:
        raise RuntimeError(
            "No grid tasks produced for input assets; check grid_type/grid_level/cover_mode or source extent"
        )
    task_rows = _prepare_task_rows_for_partitioning(
        grid_tasks,
        partition_prefix_len=args.partition_prefix_len,
        time_granularity=args.time_granularity,
    )
    grouped_tasks = _group_tasks_for_local_processing(task_rows, split_by_space_prefix=(backend == "ray"))
    if not grouped_tasks:
        raise RuntimeError("No partition task groups produced after task preparation")
    if backend == "ray":
        parallelism = _resolve_ray_actor_parallelism(grouped_tasks, args.ray_parallelism)
    else:
        parallelism = _resolve_ray_parallelism(len(grouped_tasks), args.ray_parallelism)
    chunk_size = _resolve_ray_chunk_size(len(grouped_tasks), parallelism, args.chunk_size)
    task_chunks = _chunk_tasks_for_ray(grouped_tasks, chunk_size)
    task_chunks_by_actor = _chunk_task_groups_by_actor(grouped_tasks, parallelism, chunk_size) if backend == "ray" else []
    skip_verify = args.skip_verify or args.timing_mode
    check_cancelled(args)

    run_dir = create_unique_run_dir(output_dir)
    rows_path = run_dir / "index_rows.jsonl"
    report_path = run_dir / "job_report.json"

    ray_init_elapsed = 0.0
    include_sample_mean = args.sample_mean and (not args.timing_mode)
    start = time.perf_counter()
    out_rows: list[dict] = []
    ray = None
    ray_already_initialized = False
    ray_worker_stats: dict[str, float] = {}
    try:
        if backend == "ray":
            ray = _load_ray()
            runtime_env = _ray_runtime_env_from_env()
            ray_init_start = time.perf_counter()
            ray_already_initialized = bool(getattr(ray, "is_initialized", lambda: False)())
            if args.ray_address:
                try:
                    ray.init(
                        address=args.ray_address,
                        ignore_reinit_error=True,
                        include_dashboard=False,
                        logging_level="ERROR",
                        runtime_env=runtime_env,
                    )
                except Exception:
                    if args.ray_address != "auto":
                        raise
                    ray.init(
                        ignore_reinit_error=True,
                        include_dashboard=False,
                        logging_level="ERROR",
                        runtime_env=runtime_env,
                    )
            else:
                ray.init(
                    ignore_reinit_error=True,
                    include_dashboard=False,
                    logging_level="ERROR",
                    runtime_env=runtime_env,
                )
            ray_init_elapsed = time.perf_counter() - ray_init_start

            @ray.remote
            class AssetTaskProcessor:
                def __init__(self) -> None:
                    self._local_cog_by_source: dict[str, str] = {}
                    self._converted_asset_by_source: dict[str, dict[str, Any]] = {}
                    self._remote_cog_by_local_path: dict[str, str] = {}

                def process_groups(
                    self,
                    task_groups: list[list[dict]],
                    time_granularity: str,
                    include_sample_mean: bool,
                    assets_by_path_value: dict[str, dict],
                    cog_input_dir_value: str,
                    cog_overwrite_value: bool,
                    cog_options_value: dict[str, str],
                    target_crs_value: str,
                    source_options_value: dict[str, Any],
                    cog_upload_options_value: dict[str, Any],
                ) -> dict[str, Any]:
                    import os
                    import time
                    from pathlib import Path

                    _prepend_sys_paths(
                        [
                            os.path.abspath(os.path.join(project_root, rel_path))
                            for project_root in _ray_project_roots()
                            for rel_path in ("", "cube_encoder", "cube_split", "cube_web")
                        ]
                    )

                    from cube_split.jobs.ray_partition_core import (
                        _process_local_task_group,
                        asset_record_from_dict,
                        asset_record_to_dict,
                        convert_asset_to_cog,
                        upload_cog_to_minio,
                    )

                    env_options = dict(cog_upload_options_value or source_options_value or {})
                    if env_options.get("endpoint"):
                        os.environ["CUBE_WEB_MINIO_ENDPOINT"] = str(env_options["endpoint"])
                    if env_options.get("access_key"):
                        os.environ["CUBE_WEB_MINIO_ACCESS_KEY"] = str(env_options["access_key"])
                    if env_options.get("secret_key"):
                        os.environ["CUBE_WEB_MINIO_SECRET_KEY"] = str(env_options["secret_key"])

                    prepared_groups: list[list[dict]] = []
                    used_local_cog_paths: set[str] = set()
                    stats: dict[str, float] = {
                        "source_resolve_elapsed_sec": 0.0,
                        "cog_write_elapsed_sec": 0.0,
                        "cog_upload_elapsed_sec": 0.0,
                        "partition_rows_elapsed_sec": 0.0,
                        "cog_write_count": 0.0,
                        "cog_cache_hit_count": 0.0,
                        "cog_upload_count": 0.0,
                    }
                    worker_cog_root = Path(cog_input_dir_value or "/tmp/cube_logical_cog") / f"ray_worker_{os.getpid()}"
                    for group in task_groups:
                        if not group:
                            continue
                        source_path = str(group[0]["asset_path"])
                        local_cog_path = self._local_cog_by_source.get(source_path)
                        if local_cog_path is None:
                            asset = asset_record_from_dict(assets_by_path_value[source_path])
                            convert_timing: dict[str, float] = {}
                            converted = convert_asset_to_cog(
                                asset,
                                cog_input_dir=worker_cog_root,
                                overwrite=cog_overwrite_value,
                                creation_options=cog_options_value,
                                target_crs=target_crs_value or None,
                                source_options=source_options_value,
                                timing=convert_timing,
                            )
                            for key, value in convert_timing.items():
                                stats[key] = stats.get(key, 0.0) + float(value)
                            local_cog_path = str(converted.path)
                            self._local_cog_by_source[source_path] = local_cog_path
                            self._converted_asset_by_source[source_path] = asset_record_to_dict(converted)
                        used_local_cog_paths.add(local_cog_path)
                        prepared_groups.append([{**row, "asset_path": local_cog_path} for row in group])

                    prepared_rows = [row for group in prepared_groups for row in group]
                    partition_rows_start = time.perf_counter()
                    rows_out = _process_local_task_group(
                        prepared_rows,
                        time_granularity,
                        include_sample_mean=include_sample_mean,
                    )
                    stats["partition_rows_elapsed_sec"] += time.perf_counter() - partition_rows_start
                    for source_path, local_cog_path in self._local_cog_by_source.items():
                        if local_cog_path not in used_local_cog_paths or local_cog_path in self._remote_cog_by_local_path:
                            continue
                        converted = asset_record_from_dict(self._converted_asset_by_source[source_path])
                        upload_start = time.perf_counter()
                        self._remote_cog_by_local_path[local_cog_path] = upload_cog_to_minio(
                            converted,
                            Path(local_cog_path),
                            cog_upload_options_value,
                        )
                        stats["cog_upload_elapsed_sec"] += time.perf_counter() - upload_start
                        stats["cog_upload_count"] += 1.0
                    for row in rows_out:
                        row["asset_path"] = self._remote_cog_by_local_path.get(str(row["asset_path"]), row["asset_path"])
                    return {"rows": rows_out, "stats": stats}

            actor_cls = AssetTaskProcessor.options(**_ray_actor_options_from_env())
            actors = [actor_cls.remote() for _ in range(parallelism)]
            assets_by_path = {asset.path: asset_record_to_dict(asset) for asset in assets}
            minio_options = {
                "endpoint": str(getattr(args, "minio_endpoint", "")),
                "access_key": str(getattr(args, "minio_access_key", "")),
                "secret_key": str(getattr(args, "minio_secret_key", "")),
                "secure": bool(getattr(args, "minio_secure", False)),
            }

            def submit_chunk(actor_idx: int, chunk_idx: int):
                return actors[actor_idx].process_groups.remote(
                    task_chunks_by_actor[actor_idx][chunk_idx],
                    args.time_granularity,
                    include_sample_mean,
                    assets_by_path,
                    str(args.cog_input_dir),
                    bool(args.cog_overwrite),
                    cog_creation_options(
                        compress=str(args.cog_compress or "LZW"),
                        predictor=int(args.cog_predictor or 0),
                        level=(int(args.cog_level or 0) or None),
                        overviews="NONE",
                        num_threads=str(args.cog_num_threads or ""),
                    ),
                    str(args.target_crs or ""),
                    minio_options,
                    {
                        **minio_options,
                        "bucket": str(getattr(args, "minio_bucket", "")),
                        "prefix": str(getattr(args, "minio_prefix", "cube/raw")),
                        "dataset": str(getattr(args, "dataset", "demo_optical")),
                        "sensor": str(getattr(args, "sensor", "optical_mosaic")),
                        "asset_version": str(getattr(args, "asset_version", "v1")),
                    },
                )

            pending = []
            pending_actor_by_ref: dict[Any, int] = {}
            try:
                next_chunk_by_actor = [0 for _ in range(parallelism)]
                for actor_idx, actor_chunks in enumerate(task_chunks_by_actor):
                    if not actor_chunks:
                        continue
                    check_cancelled(args)
                    ref = submit_chunk(actor_idx, 0)
                    pending.append(ref)
                    pending_actor_by_ref[ref] = actor_idx
                    next_chunk_by_actor[actor_idx] = 1
                while pending:
                    check_cancelled(args)
                    ready, pending = ray.wait(pending, num_returns=1, timeout=1.0)
                    if not ready:
                        continue
                    for ready_ref in ready:
                        actor_idx = pending_actor_by_ref.pop(ready_ref)
                        result = ray.get(ready_ref)
                        if isinstance(result, dict) and "rows" in result:
                            out_rows.extend(result["rows"])
                            for key, value in dict(result.get("stats") or {}).items():
                                ray_worker_stats[key] = ray_worker_stats.get(key, 0.0) + float(value)
                        else:
                            out_rows.extend(result)
                        if next_chunk_by_actor[actor_idx] < len(task_chunks_by_actor[actor_idx]):
                            check_cancelled(args)
                            ref = submit_chunk(actor_idx, next_chunk_by_actor[actor_idx])
                            pending.append(ref)
                            pending_actor_by_ref[ref] = actor_idx
                            next_chunk_by_actor[actor_idx] += 1
            except PartitionCancelledError:
                cancel_ray_refs(ray, pending)
                raise
            except Exception:
                cancel_ray_refs(ray, pending)
                raise
        else:
            from cube_split.jobs.ray_partition_core import _process_local_task_group

            def process_chunk(chunk: list[list[dict]]) -> list[dict]:
                rows = [row for group in chunk for row in group]
                return _process_local_task_group(rows, args.time_granularity, include_sample_mean=include_sample_mean)

            with ThreadPoolExecutor(max_workers=parallelism) as pool:
                for rows in pool.map(process_chunk, task_chunks):
                    check_cancelled(args)
                    out_rows.extend(rows)
    finally:
        if backend == "ray" and ray is not None:
            shutdown_ray_if_needed(ray, ray_already_initialized)
    elapsed = time.perf_counter() - start

    check_cancelled(args)
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

    ingest_enabled = _should_run_ingest(args)
    ingest_stats: dict[str, Any] | None = None
    ingest_elapsed = 0.0
    if ingest_enabled:
        check_cancelled(args)
        ingest_start = time.perf_counter()
        ingest_stats = _run_partition_ingest(args, run_dir)
        ingest_elapsed = time.perf_counter() - ingest_start

    report = {
        "run_dir": str(run_dir.resolve()),
        "input_dir": str(input_dir.resolve()),
        "cog_input_dir": str(Path(args.cog_input_dir).resolve()),
        "source_asset_count": len(source_assets),
        "asset_count": len(assets),
        "data_type": str(getattr(args, "data_type", "optical") or "optical"),
        "product_family": args.product_family,
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
        "target_crs": args.target_crs,
        "ingest_enabled": ingest_enabled,
        "ingest_stats": ingest_stats,
        "metadata_backend": str(getattr(args, "metadata_backend", "none") or "none"),
        "asset_storage_backend": str(getattr(args, "asset_storage_backend", "local") or "local"),
        "dataset": str(getattr(args, "dataset", "")),
        "sensor": str(getattr(args, "sensor", "")),
        "asset_version": str(getattr(args, "asset_version", "")),
        "cube_version": str(getattr(args, "cube_version", "")),
        "minio_bucket": str(getattr(args, "minio_bucket", "")) if str(getattr(args, "asset_storage_backend", "local") or "local") == "minio" else "",
        "minio_prefix": str(getattr(args, "minio_prefix", "")) if str(getattr(args, "asset_storage_backend", "local") or "local") == "minio" else "",
        "cog_elapsed_sec": round(cog_elapsed, 3),
        "worker_source_resolve_elapsed_sec": round(ray_worker_stats.get("source_resolve_elapsed_sec", 0.0), 3),
        "worker_cog_write_elapsed_sec": round(ray_worker_stats.get("cog_write_elapsed_sec", 0.0), 3),
        "worker_cog_upload_elapsed_sec": round(ray_worker_stats.get("cog_upload_elapsed_sec", 0.0), 3),
        "worker_partition_rows_elapsed_sec": round(ray_worker_stats.get("partition_rows_elapsed_sec", 0.0), 3),
        "worker_cog_write_count": int(ray_worker_stats.get("cog_write_count", 0.0)),
        "worker_cog_cache_hit_count": int(ray_worker_stats.get("cog_cache_hit_count", 0.0)),
        "worker_cog_upload_count": int(ray_worker_stats.get("cog_upload_count", 0.0)),
        "partition_elapsed_sec": round(elapsed, 3),
        "ingest_elapsed_sec": round(ingest_elapsed, 3),
        "total_elapsed_sec": round(time.perf_counter() - total_start, 3),
    }
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def main() -> None:
    report = run_logical_partition(parse_args())
    print("=== Ray logical partition job completed ===")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
