#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import h3
import numpy as np
import rasterio
import rasterio.mask
from grid_core.sdk import CubeEncoderSDK
from pyproj import Geod
from rasterio.warp import transform_geom

from cube_split import runtime_config
from cube_split.jobs.cancellation import PartitionCancelledError, cancel_ray_refs, check_cancelled
from cube_split.jobs.ray_logical_partition_job import (
    _chunk_tasks_for_ray,
    _load_ray,
    _prepend_sys_paths,
    _ray_actor_options_from_env,
    _ray_project_roots,
    _ray_runtime_env_from_env,
    _resolve_ray_chunk_size,
    _resolve_ray_parallelism,
)
from cube_split.jobs.ray_partition_core import (
    AssetRecord,
    _dataset_bounds_wgs84,
    asset_record_to_dict,
    build_grid_tasks_driver,
    build_manifest,
    cog_creation_options,
    resolve_asset_source_path,
)

DEFAULT_TARGET_PIXELS_PER_HEX_EDGE = 768
ENTITY_DATA_TYPES = {"optical", "product", "radar"}


def _normalize_data_type(value: Any) -> str:
    data_type = str(value or "optical").strip().lower()
    if data_type not in ENTITY_DATA_TYPES:
        raise ValueError("data_type must be one of: optical, product, radar")
    return data_type


def infer_isea4h_level_for_assets(
    assets: list[AssetRecord],
    target_pixels_per_hex_edge: int = DEFAULT_TARGET_PIXELS_PER_HEX_EDGE,
) -> int:
    if target_pixels_per_hex_edge <= 0:
        raise ValueError("target_pixels_per_hex_edge must be greater than 0")
    resolution = max(
        float(asset.resolution) if asset.resolution is not None and float(asset.resolution) > 0 else _asset_pixel_size_m(asset.path)
        for asset in assets
    )
    target_edge_m = resolution * target_pixels_per_hex_edge
    selected = 1
    for level in range(1, 13):
        if h3.average_hexagon_edge_length(level, unit="m") >= target_edge_m:
            selected = level
    return selected


def _asset_pixel_size_m(path: str | Path) -> float:
    geod = Geod(ellps="WGS84")
    with rasterio.open(resolve_asset_source_path(str(path))) as ds:
        res_x = abs(float(ds.transform.a))
        res_y = abs(float(ds.transform.e))
        if ds.crs and ds.crs.is_projected:
            return max(res_x, res_y)

        min_lon, min_lat, max_lon, max_lat = _dataset_bounds_wgs84(ds)
        center_lon = (min_lon + max_lon) / 2.0
        center_lat = (min_lat + max_lat) / 2.0
        _, _, x_m = geod.inv(center_lon, center_lat, center_lon + res_x, center_lat)
        _, _, y_m = geod.inv(center_lon, center_lat, center_lon, center_lat + res_y)
        pixel_size = max(abs(x_m), abs(y_m))
        if not math.isfinite(pixel_size) or pixel_size <= 0:
            raise ValueError(f"Cannot infer pixel size for asset: {path}")
        return pixel_size


def _time_bucket(acq_time: str, granularity: str) -> str:
    time_format = {
        "year": "%Y",
        "month": "%Y%m",
        "day": "%Y%m%d",
        "hour": "%Y%m%d%H",
        "minute": "%Y%m%d%H%M",
    }[granularity]
    return datetime.fromisoformat(acq_time.replace("Z", "+00:00")).strftime(time_format)


def _st_time_granularity(granularity: str) -> str:
    # Product outputs use annual buckets, while the grid-core ST encoder supports
    # month/day/hour/minute/second. Match product logical partitioning by encoding
    # the concrete acquisition day and storing the annual bucket separately.
    return "day" if granularity == "year" else granularity


def _ensure_center_cell_tasks(
    assets: list[AssetRecord],
    tasks: list[dict[str, Any]],
    grid_level: int,
    cover_mode: str,
) -> list[dict[str, Any]]:
    task_keys = {(row["scene_id"], row["band"], row["asset_path"]) for row in tasks}
    missing_assets = [
        asset
        for asset in assets
        if (asset.scene_id, asset.band, asset.path) not in task_keys
    ]
    if not missing_assets:
        return tasks

    sdk = CubeEncoderSDK()
    out = list(tasks)
    for asset in missing_assets:
        if asset.bbox is not None:
            min_lon, min_lat, max_lon, max_lat = map(float, asset.bbox)
        else:
            with rasterio.open(resolve_asset_source_path(asset.path)) as ds:
                min_lon, min_lat, max_lon, max_lat = _dataset_bounds_wgs84(ds)
        center = [(min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0]
        cell = sdk.locate(grid_type="isea4h", level=grid_level, point=center)
        out.append(
            {
                "scene_id": asset.scene_id,
                "band": asset.band,
                "asset_path": asset.path,
                "acq_time": asset.acq_time,
                "grid_type": "isea4h",
                "grid_level": grid_level,
                "space_code": cell.space_code,
                "cell_min_lon": float(cell.bbox[0]),
                "cell_min_lat": float(cell.bbox[1]),
                "cell_max_lon": float(cell.bbox[2]),
                "cell_max_lat": float(cell.bbox[3]),
                "cover_mode": cover_mode,
            }
        )
    return out


def _safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in value)


def _hex_geometry_for_dataset(sdk: CubeEncoderSDK, ds: rasterio.DatasetReader, space_code: str) -> dict[str, Any]:
    geom = sdk.code_to_geometry(grid_type="isea4h", code=space_code)
    if ds.crs and ds.crs.to_string().upper() != "EPSG:4326":
        return transform_geom("EPSG:4326", ds.crs, geom)
    return geom


def _nodata_value(ds: rasterio.DatasetReader, band_index: int) -> float | int:
    nodata = ds.nodatavals[band_index - 1] if ds.nodatavals else ds.nodata
    if nodata is not None:
        return nodata
    dtype = np.dtype(ds.dtypes[band_index - 1])
    if np.issubdtype(dtype, np.floating):
        return -9999.0
    return 0


def _write_entity_tiles(
    tasks: list[dict[str, Any]],
    run_dir: Path,
    time_granularity: str,
    partition_prefix_len: int,
    data_type: str = "optical",
) -> list[dict[str, Any]]:
    data_type = _normalize_data_type(data_type)
    sdk = CubeEncoderSDK()
    st_cache: dict[str, str] = {}
    rows: list[dict[str, Any]] = []
    task_groups: dict[str, list[dict[str, Any]]] = {}
    for task in tasks:
        task_groups.setdefault(str(task["asset_path"]), []).append(task)

    for asset_path, asset_tasks in sorted(task_groups.items()):
        local_asset_path = resolve_asset_source_path(asset_path)
        with rasterio.open(local_asset_path) as ds:
            for task in sorted(asset_tasks, key=lambda row: str(row["space_code"])):
                geom = _hex_geometry_for_dataset(sdk, ds, str(task["space_code"]))
                for band_index in range(1, ds.count + 1):
                    band_name = str(task["band"]) if ds.count == 1 else f"{task['band']}_band{band_index}"
                    nodata = _nodata_value(ds, band_index)
                    try:
                        data, out_transform = rasterio.mask.mask(
                            ds,
                            [geom],
                            crop=True,
                            filled=False,
                            indexes=band_index,
                        )
                    except ValueError:
                        continue
                    if data.size == 0:
                        continue

                    valid_mask = ~np.ma.getmaskarray(data)
                    valid_pixels = int(valid_mask.sum())
                    total_pixels = int(data.size)
                    if valid_pixels == 0 or total_pixels == 0:
                        continue

                    filled = np.ma.filled(data, nodata)
                    tile_dir = (
                        run_dir
                        / "entity_tiles"
                        / _safe_name(data_type)
                        / _safe_name(str(task["scene_id"]))
                        / "isea4h"
                        / f"L{int(task['grid_level'])}"
                        / _safe_name(str(task["space_code"]))
                    )
                    tile_dir.mkdir(parents=True, exist_ok=True)
                    tile_path = tile_dir / f"{_safe_name(band_name)}.tif"
                    profile = ds.profile.copy()
                    profile.update(
                        driver="GTiff",
                        height=int(filled.shape[-2]),
                        width=int(filled.shape[-1]),
                        count=1,
                        transform=out_transform,
                        nodata=nodata,
                    )
                    with rasterio.open(tile_path, "w", **profile) as out_ds:
                        out_ds.write(filled, 1)

                    acq_time = str(task["acq_time"])
                    st_time_granularity = _st_time_granularity(time_granularity)
                    st_key = "|".join(
                        [
                            "isea4h",
                            str(int(task["grid_level"])),
                            str(task["space_code"]),
                            acq_time,
                            st_time_granularity,
                        ]
                    )
                    if st_key not in st_cache:
                        st_cache[st_key] = sdk.generate_st_code(
                            grid_type="isea4h",
                            level=int(task["grid_level"]),
                            space_code=str(task["space_code"]),
                            timestamp=datetime.fromisoformat(acq_time.replace("Z", "+00:00")),
                            time_granularity=st_time_granularity,
                            version="v1",
                        ).st_code

                    rows.append(
                        {
                            "partition_type": "entity",
                            "data_type": data_type,
                            "scene_id": task["scene_id"],
                            "band": band_name,
                            "asset_path": str(tile_path.resolve()),
                            "source_asset_path": asset_path,
                            "output_path": str(tile_path.resolve()),
                            "acq_time": acq_time,
                            "grid_type": "isea4h",
                            "grid_level": int(task["grid_level"]),
                            "space_code": task["space_code"],
                            "space_code_prefix": str(task["space_code"])[: max(1, int(partition_prefix_len))],
                            "st_code": st_cache[st_key],
                            "time_bucket": _time_bucket(acq_time, time_granularity),
                            "st_time_granularity": st_time_granularity,
                            "cover_mode": task["cover_mode"],
                            "cell_min_lon": float(task["cell_min_lon"]),
                            "cell_min_lat": float(task["cell_min_lat"]),
                            "cell_max_lon": float(task["cell_max_lon"]),
                            "cell_max_lat": float(task["cell_max_lat"]),
                            "window_col_off": 0,
                            "window_row_off": 0,
                            "window_width": int(filled.shape[-1]),
                            "window_height": int(filled.shape[-2]),
                            "nodata": nodata,
                            "valid_pixel_ratio": round(valid_pixels / total_pixels, 6),
                        }
                    )
    return rows


def _group_tasks_for_parallel_processing(tasks: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    return [[task] for task in sorted(tasks, key=lambda row: (str(row["asset_path"]), str(row["space_code"])))]


def _flatten_task_groups(task_groups: list[list[dict[str, Any]]]) -> list[dict[str, Any]]:
    return [task for group in task_groups for task in group]


def _resolve_backend(requested_backend: str, ray_address: str) -> str:
    if requested_backend == "auto":
        return "ray" if ray_address else "thread"
    if requested_backend in {"local", "thread", "process"}:
        return "thread"
    return requested_backend


def _write_entity_tile_chunks_thread(
    task_chunks: list[list[list[dict[str, Any]]]],
    run_dir: Path,
    time_granularity: str,
    partition_prefix_len: int,
    workers: int,
    data_type: str = "optical",
) -> list[dict[str, Any]]:
    def process_chunk(chunk: list[list[dict[str, Any]]]) -> list[dict[str, Any]]:
        return _write_entity_tiles(
            _flatten_task_groups(chunk),
            run_dir,
            time_granularity,
            partition_prefix_len,
            data_type=data_type,
        )

    rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        for chunk_rows in pool.map(process_chunk, task_chunks):
            rows.extend(chunk_rows)
    return rows


def _write_entity_tile_chunks_ray(
    task_chunks: list[list[list[dict[str, Any]]]],
    run_dir: Path,
    time_granularity: str,
    partition_prefix_len: int,
    parallelism: int,
    ray_address: str,
    data_type: str = "optical",
    assets_by_path: dict[str, dict[str, Any]] | None = None,
    cog_input_dir: str = "",
    cog_overwrite: bool = False,
    cog_options: dict[str, str] | None = None,
    target_crs: str = "",
    source_options: dict[str, Any] | None = None,
    cog_upload_options: dict[str, Any] | None = None,
    tile_upload_options: dict[str, Any] | None = None,
    cancellation_check: Any | None = None,
) -> tuple[list[dict[str, Any]], float]:
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
    class EntityTileProcessor:
        def process_groups(
            self,
            task_groups: list[list[dict[str, Any]]],
            run_dir_text: str,
            time_granularity_text: str,
            prefix_len: int,
            data_type_text: str,
            assets_by_path_value: dict[str, dict[str, Any]] | None,
            cog_input_dir_value: str,
            cog_overwrite_value: bool,
            cog_options_value: dict[str, str] | None,
            target_crs_value: str,
            source_options_value: dict[str, Any] | None,
            cog_upload_options_value: dict[str, Any] | None,
            tile_upload_options_value: dict[str, Any] | None,
        ) -> list[dict[str, Any]]:
            import os
            import sys
            from pathlib import Path

            entity_module_path = None
            project_roots = _ray_project_roots()
            for project_root in project_roots:
                candidate = os.path.abspath(os.path.join(project_root, "cube_split", "cube_split", "jobs", "entity_partition_job.py"))
                if entity_module_path is None and os.path.exists(candidate):
                    entity_module_path = candidate
            _prepend_sys_paths(
                [
                    os.path.abspath(os.path.join(project_root, rel_path))
                    for project_root in project_roots
                    for rel_path in ("", "cube_encoder", "cube_split", "cube_web")
                ]
            )

            for search_root in (
                os.environ.get("RAY_RUNTIME_ENV_CREATE_WORKING_DIR", ""),
                "/tmp/ray/session_latest/runtime_resources/working_dir_files",
            ):
                if not search_root or not os.path.isdir(search_root):
                    continue
                for dirpath, _, filenames in os.walk(search_root):
                    if "entity_partition_job.py" not in filenames:
                        continue
                    if entity_module_path is None:
                        entity_module_path = os.path.abspath(os.path.join(dirpath, "entity_partition_job.py"))
                    outer_cube_split = os.path.abspath(os.path.join(dirpath, "..", ".."))
                    project_root = os.path.abspath(os.path.join(dirpath, "..", "..", "..", ".."))
                    _prepend_sys_paths(
                        [
                            project_root,
                            os.path.join(project_root, "cube_encoder"),
                            outer_cube_split,
                            os.path.join(project_root, "cube_web"),
                        ]
                    )
                    break
                if entity_module_path:
                    break

            try:
                from cube_split.jobs.entity_partition_job import _rows_with_asset_uris as rows_with_asset_uris
                from cube_split.jobs.entity_partition_job import _write_entity_tiles as writer
            except ModuleNotFoundError:
                if not entity_module_path:
                    raise
                import importlib.util

                spec = importlib.util.spec_from_file_location("_ray_entity_partition_job", entity_module_path)
                if spec is None or spec.loader is None:
                    raise
                module = importlib.util.module_from_spec(spec)
                sys.modules["_ray_entity_partition_job"] = module
                spec.loader.exec_module(module)
                writer = module._write_entity_tiles
                rows_with_asset_uris = module._rows_with_asset_uris

            flat_tasks = [task for group in task_groups for task in group]
            if assets_by_path_value and cog_options_value and cog_upload_options_value:
                from cube_split.jobs.ray_partition_core import asset_record_from_dict, convert_asset_to_cog, upload_cog_to_minio

                env_options = dict(cog_upload_options_value or source_options_value or {})
                if env_options.get("endpoint"):
                    os.environ["CUBE_WEB_MINIO_ENDPOINT"] = str(env_options["endpoint"])
                if env_options.get("access_key"):
                    os.environ["CUBE_WEB_MINIO_ACCESS_KEY"] = str(env_options["access_key"])
                if env_options.get("secret_key"):
                    os.environ["CUBE_WEB_MINIO_SECRET_KEY"] = str(env_options["secret_key"])

                prepared: list[dict[str, Any]] = []
                cog_uri_by_source: dict[str, str] = {}
                worker_cog_root = Path(cog_input_dir_value or "/tmp/cube_entity_cog") / f"ray_worker_{os.getpid()}"
                for task in flat_tasks:
                    source_path = str(task["asset_path"])
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
                    prepared.append({**task, "asset_path": cog_uri})
                flat_tasks = prepared

            rows = writer(
                flat_tasks,
                run_dir=Path(run_dir_text),
                time_granularity=time_granularity_text,
                partition_prefix_len=prefix_len,
                data_type=data_type_text,
            )
            if tile_upload_options_value:
                import argparse

                from cube_split.jobs.entity_partition_job import _upload_entity_tiles_to_minio

                asset_uri_map = _upload_entity_tiles_to_minio(rows, argparse.Namespace(**tile_upload_options_value))
                rows = rows_with_asset_uris(rows, asset_uri_map)
            return rows

    actor_cls = EntityTileProcessor.options(**_ray_actor_options_from_env())
    actors = [actor_cls.remote() for _ in range(parallelism)]
    def submit_chunk(idx: int):
        return actors[idx % parallelism].process_groups.remote(
            task_chunks[idx],
            str(run_dir),
            time_granularity,
            partition_prefix_len,
            data_type,
            assets_by_path,
            cog_input_dir,
            cog_overwrite,
            cog_options,
            target_crs,
            source_options,
            cog_upload_options,
            tile_upload_options,
        )

    rows: list[dict[str, Any]] = []
    next_idx = 0
    pending = []
    while next_idx < len(task_chunks) and len(pending) < parallelism:
        if cancellation_check is not None and cancellation_check():
            raise PartitionCancelledError("Partition task cancelled")
        pending.append(submit_chunk(next_idx))
        next_idx += 1
    try:
        while pending:
            if cancellation_check is not None and cancellation_check():
                raise PartitionCancelledError("Partition task cancelled")
            ready, pending = ray.wait(pending, num_returns=1, timeout=1.0)
            if not ready:
                continue
            chunk_rows = ray.get(ready[0])
            rows.extend(chunk_rows)
            if next_idx < len(task_chunks):
                if cancellation_check is not None and cancellation_check():
                    raise PartitionCancelledError("Partition task cancelled")
                pending.append(submit_chunk(next_idx))
                next_idx += 1
    except PartitionCancelledError:
        cancel_ray_refs(ray, pending)
        raise
    finally:
        for actor in actors:
            ray.kill(actor, no_restart=True)
        ray.shutdown()
    return rows, ray_init_elapsed


def _upload_entity_tiles_to_minio(rows: list[dict[str, Any]], args: argparse.Namespace) -> dict[str, str]:
    from cube_split.ingest.ray_ingest_job import upload_assets_to_minio

    return upload_assets_to_minio(
        rows=rows,
        dataset=str(getattr(args, "dataset", "demo_optical")),
        sensor=str(getattr(args, "sensor", "optical_mosaic")),
        asset_version=str(getattr(args, "asset_version", getattr(args, "tile_version", "v1"))),
        endpoint=str(getattr(args, "minio_endpoint", "")),
        access_key=str(getattr(args, "minio_access_key", "")),
        secret_key=str(getattr(args, "minio_secret_key", "")),
        bucket=str(getattr(args, "minio_bucket", "")),
        prefix=str(getattr(args, "minio_prefix", "cube/entity")),
        secure=bool(getattr(args, "minio_secure", False)),
        workers=max(1, int(getattr(args, "minio_upload_workers", 8) or 8)),
    )


def _rows_with_asset_uris(rows: list[dict[str, Any]], asset_uri_map: dict[str, str]) -> list[dict[str, Any]]:
    updated: list[dict[str, Any]] = []
    for row in rows:
        next_row = dict(row)
        local_path = str(row["asset_path"])
        tile_uri = asset_uri_map.get(local_path, local_path)
        next_row["local_asset_path"] = local_path
        next_row["entity_tile_uri"] = tile_uri
        next_row["asset_path"] = tile_uri
        next_row["output_path"] = tile_uri
        updated.append(next_row)
    return updated


def _ensure_entity_tables_postgres(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rs_entity_tile_asset (
              id BIGSERIAL PRIMARY KEY,
              dataset TEXT NOT NULL,
              sensor TEXT NOT NULL,
              scene_id TEXT NOT NULL,
              band TEXT NOT NULL,
              acq_time TIMESTAMPTZ NOT NULL,
              grid_type TEXT NOT NULL,
              grid_level INTEGER NOT NULL,
              space_code TEXT NOT NULL,
              space_code_prefix TEXT NOT NULL,
              st_code TEXT NOT NULL,
              time_bucket TEXT NOT NULL,
              tile_uri TEXT NOT NULL,
              local_tile_path TEXT,
              source_asset_path TEXT NOT NULL,
              tile_version TEXT NOT NULL,
              run_id TEXT NOT NULL,
              cover_mode TEXT NOT NULL,
              cell_min_lon DOUBLE PRECISION NOT NULL,
              cell_min_lat DOUBLE PRECISION NOT NULL,
              cell_max_lon DOUBLE PRECISION NOT NULL,
              cell_max_lat DOUBLE PRECISION NOT NULL,
              window_width INTEGER NOT NULL,
              window_height INTEGER NOT NULL,
              nodata DOUBLE PRECISION,
              valid_pixel_ratio DOUBLE PRECISION NOT NULL,
              metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
              ingest_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              UNIQUE (dataset, scene_id, band, grid_type, grid_level, space_code, time_bucket, tile_version)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rs_ingest_job (
              job_id TEXT PRIMARY KEY,
              status TEXT NOT NULL,
              params_json JSONB NOT NULL,
              stats_json JSONB NOT NULL,
              error_msg TEXT,
              retry_count INTEGER NOT NULL DEFAULT 0,
              started_at TIMESTAMPTZ,
              finished_at TIMESTAMPTZ,
              output_snapshot TEXT,
              created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
    conn.commit()


def _upsert_entity_tiles_postgres(
    conn: Any,
    rows: list[dict[str, Any]],
    dataset: str,
    sensor: str,
    tile_version: str,
    run_id: str,
) -> None:
    from cube_split.ingest.ray_ingest_job import _parse_timestamp

    sql = """
        MERGE INTO rs_entity_tile_asset target
        USING (
          SELECT
            %s::text AS dataset,
            %s::text AS sensor,
            %s::text AS scene_id,
            %s::text AS band,
            %s::timestamptz AS acq_time,
            %s::text AS grid_type,
            %s::int AS grid_level,
            %s::text AS space_code,
            %s::text AS space_code_prefix,
            %s::text AS st_code,
            %s::text AS time_bucket,
            %s::text AS tile_uri,
            %s::text AS local_tile_path,
            %s::text AS source_asset_path,
            %s::text AS tile_version,
            %s::text AS run_id,
            %s::text AS cover_mode,
            %s::double precision AS cell_min_lon,
            %s::double precision AS cell_min_lat,
            %s::double precision AS cell_max_lon,
            %s::double precision AS cell_max_lat,
            %s::int AS window_width,
            %s::int AS window_height,
            %s::double precision AS nodata,
            %s::double precision AS valid_pixel_ratio,
            %s::jsonb AS metadata_json
        ) source
        ON (
          target.dataset = source.dataset
          AND target.scene_id = source.scene_id
          AND target.band = source.band
          AND target.grid_type = source.grid_type
          AND target.grid_level = source.grid_level
          AND target.space_code = source.space_code
          AND target.time_bucket = source.time_bucket
          AND target.tile_version = source.tile_version
        )
        WHEN MATCHED THEN UPDATE SET
          sensor = source.sensor,
          acq_time = source.acq_time,
          space_code_prefix = source.space_code_prefix,
          st_code = source.st_code,
          tile_uri = source.tile_uri,
          local_tile_path = source.local_tile_path,
          source_asset_path = source.source_asset_path,
          run_id = source.run_id,
          cover_mode = source.cover_mode,
          cell_min_lon = source.cell_min_lon,
          cell_min_lat = source.cell_min_lat,
          cell_max_lon = source.cell_max_lon,
          cell_max_lat = source.cell_max_lat,
          window_width = source.window_width,
          window_height = source.window_height,
          nodata = source.nodata,
          valid_pixel_ratio = source.valid_pixel_ratio,
          metadata_json = source.metadata_json,
          ingest_time = NOW()
        WHEN NOT MATCHED THEN INSERT (
          dataset, sensor, scene_id, band, acq_time, grid_type, grid_level, space_code,
          space_code_prefix, st_code, time_bucket, tile_uri, local_tile_path, source_asset_path,
          tile_version, run_id, cover_mode, cell_min_lon, cell_min_lat, cell_max_lon, cell_max_lat,
          window_width, window_height, nodata, valid_pixel_ratio, metadata_json
        ) VALUES (
          source.dataset, source.sensor, source.scene_id, source.band, source.acq_time, source.grid_type, source.grid_level, source.space_code,
          source.space_code_prefix, source.st_code, source.time_bucket, source.tile_uri, source.local_tile_path, source.source_asset_path,
          source.tile_version, source.run_id, source.cover_mode, source.cell_min_lon, source.cell_min_lat, source.cell_max_lon, source.cell_max_lat,
          source.window_width, source.window_height, source.nodata, source.valid_pixel_ratio, source.metadata_json
        )
    """
    values = []
    for row in rows:
        metadata = {
            "partition_type": row.get("partition_type"),
            "data_type": row.get("data_type"),
            "source_asset_path": row.get("source_asset_path"),
            "window_col_off": row.get("window_col_off"),
            "window_row_off": row.get("window_row_off"),
        }
        values.append(
            (
                dataset,
                sensor,
                row["scene_id"],
                row["band"],
                _parse_timestamp(row["acq_time"]),
                row["grid_type"],
                int(row["grid_level"]),
                row["space_code"],
                row["space_code_prefix"],
                row["st_code"],
                row["time_bucket"],
                row.get("entity_tile_uri") or row["asset_path"],
                row.get("local_asset_path"),
                row["source_asset_path"],
                tile_version,
                run_id,
                row["cover_mode"],
                float(row["cell_min_lon"]),
                float(row["cell_min_lat"]),
                float(row["cell_max_lon"]),
                float(row["cell_max_lat"]),
                int(row["window_width"]),
                int(row["window_height"]),
                None if row.get("nodata") is None else float(row["nodata"]),
                float(row["valid_pixel_ratio"]),
                json.dumps(metadata, ensure_ascii=False),
            )
        )
    with conn.cursor() as cur:
        cur.executemany(sql, values)

def _write_entity_metadata_postgres(rows: list[dict[str, Any]], args: argparse.Namespace, run_dir: Path) -> dict[str, Any]:
    from cube_split.ingest.ray_ingest_job import _upsert_job_status_postgres

    try:
        import psycopg
    except ModuleNotFoundError as exc:
        raise RuntimeError("Postgres backend requires `psycopg` package") from exc

    dsn = str(getattr(args, "postgres_dsn", ""))
    if not dsn:
        raise ValueError("postgres_dsn is required when metadata_backend=postgres")

    dataset = str(getattr(args, "dataset", "demo_optical"))
    sensor = str(getattr(args, "sensor", "optical_mosaic"))
    tile_version = str(getattr(args, "asset_version", getattr(args, "tile_version", "v1")))
    run_id = str(getattr(args, "job_id", "")) or run_dir.name
    started_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    params = {
        "run_dir": str(run_dir.resolve()),
        "dataset": dataset,
        "sensor": sensor,
        "tile_version": tile_version,
        "metadata_backend": "postgres",
        "asset_storage_backend": str(getattr(args, "asset_storage_backend", "local")),
    }
    stats = {
        "entity_tile_rows": len(rows),
        "dataset": dataset,
        "sensor": sensor,
        "tile_version": tile_version,
        "run_id": run_id,
    }

    try:
        conn_ctx = psycopg.connect(dsn, client_encoding="UTF8")
    except TypeError:
        conn_ctx = psycopg.connect(dsn)

    with conn_ctx as conn:
        try:
            _ensure_entity_tables_postgres(conn)
            _upsert_job_status_postgres(conn, run_id, "running", params, started_at=started_at)
            _upsert_entity_tiles_postgres(conn, rows, dataset, sensor, tile_version, run_id)
            finished_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            _upsert_job_status_postgres(
                conn,
                run_id,
                "succeeded",
                params,
                stats_json=stats,
                output_snapshot=f"entity_tiles={len(rows)},run_id={run_id}",
                started_at=started_at,
                finished_at=finished_at,
            )
            conn.commit()
        except Exception as exc:
            finished_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            conn.rollback()
            _upsert_job_status_postgres(
                conn,
                run_id,
                "failed",
                params,
                error_msg=str(exc),
                started_at=started_at,
                finished_at=finished_at,
            )
            conn.commit()
            raise
    return stats


def run_entity_partition(args: argparse.Namespace) -> dict[str, Any]:
    total_start = time.perf_counter()
    check_cancelled(args)
    for key in ("SPARK_HOME", "SPARK_CONF_DIR", "HADOOP_CONF_DIR", "YARN_CONF_DIR"):
        os.environ.pop(key, None)

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")
    data_type = _normalize_data_type(getattr(args, "data_type", "optical"))

    source_assets = build_manifest(
        input_dir,
        product_family=args.product_family,
        data_type=data_type,
        manifest_path=(Path(args.manifest_path) if args.manifest_path else None),
    )
    if not source_assets:
        suffix_hint = ".dat/.TIF" if data_type == "radar" else ".TIF"
        raise RuntimeError(f"No {suffix_hint} assets found under: {input_dir}")
    check_cancelled(args)

    assets = source_assets

    requested_level = int(getattr(args, "grid_level", 0) or 0)
    target_pixels = int(getattr(args, "target_pixels_per_hex_edge", DEFAULT_TARGET_PIXELS_PER_HEX_EDGE) or DEFAULT_TARGET_PIXELS_PER_HEX_EDGE)
    inferred_level = requested_level if requested_level > 0 else infer_isea4h_level_for_assets(assets, target_pixels)
    grid_level = requested_level if requested_level > 0 else inferred_level

    grid_tasks = build_grid_tasks_driver(
        assets=assets,
        grid_type="isea4h",
        grid_level=grid_level,
        cover_mode=args.cover_mode,
        max_cells_per_asset=0,
    )
    grid_tasks = _ensure_center_cell_tasks(assets, grid_tasks, grid_level, args.cover_mode)
    check_cancelled(args)

    output_dir.mkdir(parents=True, exist_ok=True)
    run_dir = output_dir / time.strftime("run_%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    backend_requested = str(getattr(args, "partition_backend", "thread") or "thread")
    ray_address = str(getattr(args, "ray_address", "") or "")
    backend = _resolve_backend(backend_requested, ray_address)
    if backend not in {"ray", "thread"}:
        raise ValueError("partition_backend must be one of: auto, ray, thread, local, process")

    grouped_tasks = _group_tasks_for_parallel_processing(grid_tasks)
    parallelism = _resolve_ray_parallelism(
        len(grouped_tasks),
        int(getattr(args, "ray_parallelism", 0) or 0),
    )
    chunk_size = _resolve_ray_chunk_size(
        len(grouped_tasks),
        parallelism,
        int(getattr(args, "chunk_size", 0) or 0),
    )
    task_chunks = _chunk_tasks_for_ray(grouped_tasks, chunk_size)

    ray_init_elapsed = 0.0
    partition_start = time.perf_counter()
    if backend == "ray":
        assets_by_path = {asset.path: asset_record_to_dict(asset) for asset in assets}
        minio_options = {
            "endpoint": str(getattr(args, "minio_endpoint", "")),
            "access_key": str(getattr(args, "minio_access_key", "")),
            "secret_key": str(getattr(args, "minio_secret_key", "")),
            "secure": bool(getattr(args, "minio_secure", False)),
        }
        rows, ray_init_elapsed = _write_entity_tile_chunks_ray(
            task_chunks=task_chunks,
            run_dir=run_dir,
            time_granularity=args.time_granularity,
            partition_prefix_len=args.partition_prefix_len,
            parallelism=parallelism,
            ray_address=ray_address,
            data_type=data_type,
            assets_by_path=assets_by_path,
            cog_input_dir=str(getattr(args, "cog_input_dir", "") or "/tmp/cube_entity_cog"),
            cog_overwrite=bool(getattr(args, "cog_overwrite", False)),
            cog_options=cog_creation_options(
                compress=str(getattr(args, "cog_compress", "LZW") or "LZW"),
                predictor=int(getattr(args, "cog_predictor", 2) or 0),
                level=(int(getattr(args, "cog_level", 0) or 0) or None),
                overviews="NONE",
                num_threads=str(getattr(args, "cog_num_threads", "ALL_CPUS") or ""),
            ),
            target_crs=str(getattr(args, "target_crs", "") or ""),
            source_options=minio_options,
            cog_upload_options={
                **minio_options,
                "bucket": str(getattr(args, "minio_bucket", "")),
                "prefix": "cube/entity_cog",
                "dataset": str(getattr(args, "dataset", "demo_optical")),
                "sensor": str(getattr(args, "sensor", "optical_mosaic")),
                "asset_version": str(getattr(args, "asset_version", "v1")),
            },
            tile_upload_options=(
                {
                    "dataset": str(getattr(args, "dataset", "demo_optical")),
                    "sensor": str(getattr(args, "sensor", "optical_mosaic")),
                    "asset_version": str(getattr(args, "asset_version", "v1")),
                    "minio_endpoint": str(getattr(args, "minio_endpoint", "")),
                    "minio_access_key": str(getattr(args, "minio_access_key", "")),
                    "minio_secret_key": str(getattr(args, "minio_secret_key", "")),
                    "minio_bucket": str(getattr(args, "minio_bucket", "")),
                    "minio_prefix": str(getattr(args, "minio_prefix", "cube/entity")),
                    "minio_secure": bool(getattr(args, "minio_secure", False)),
                    "minio_upload_workers": int(getattr(args, "minio_upload_workers", 8) or 8),
                }
                if str(getattr(args, "asset_storage_backend", "local") or "local") == "minio"
                else None
            ),
            cancellation_check=getattr(args, "cancellation_check", None),
        )
    else:
        rows = _write_entity_tile_chunks_thread(
            task_chunks=task_chunks,
            run_dir=run_dir,
            time_granularity=args.time_granularity,
            partition_prefix_len=args.partition_prefix_len,
            workers=parallelism,
            data_type=data_type,
        )
    partition_elapsed = time.perf_counter() - partition_start
    check_cancelled(args)

    upload_elapsed = 0.0
    metadata_elapsed = 0.0
    uploaded_tile_count = 0
    metadata_rows = 0
    asset_storage_backend = str(getattr(args, "asset_storage_backend", "local") or "local")
    metadata_backend = str(getattr(args, "metadata_backend", "none") or "none")
    explicit_ingest = getattr(args, "ingest_enabled", True)
    ingest_enabled = True if explicit_ingest is None else bool(explicit_ingest)
    if not ingest_enabled:
        asset_storage_backend = "local"
        metadata_backend = "none"
    asset_uri_map: dict[str, str] = {}
    if asset_storage_backend == "minio":
        check_cancelled(args)
        upload_start = time.perf_counter()
        asset_uri_map = _upload_entity_tiles_to_minio(rows, args)
        upload_elapsed = time.perf_counter() - upload_start
        uploaded_tile_count = len(asset_uri_map)
        rows = _rows_with_asset_uris(rows, asset_uri_map)
    elif asset_storage_backend != "local":
        raise ValueError("asset_storage_backend must be one of: local, minio")

    if metadata_backend == "postgres":
        check_cancelled(args)
        metadata_start = time.perf_counter()
        metadata_stats = _write_entity_metadata_postgres(rows, args, run_dir)
        metadata_elapsed = time.perf_counter() - metadata_start
        metadata_rows = int(metadata_stats["entity_tile_rows"])
    elif metadata_backend not in {"none", "local"}:
        raise ValueError("metadata_backend must be one of: none, local, postgres")

    entity_rows_path = run_dir / "entity_index_rows.jsonl"
    index_rows_path = run_dir / "index_rows.jsonl"
    for path in (entity_rows_path, index_rows_path):
        with path.open("w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    rows_by_band: dict[str, int] = {}
    for row in rows:
        rows_by_band[row["band"]] = rows_by_band.get(row["band"], 0) + 1

    report = {
        "status": "completed",
        "run_dir": str(run_dir.resolve()),
        "input_dir": str(input_dir.resolve()),
        "cog_input_dir": "",
        "rows_path": str(entity_rows_path.resolve()),
        "index_rows_path": str(index_rows_path.resolve()),
        "source_asset_count": len(source_assets),
        "asset_count": len(assets),
        "product_family": args.product_family,
        "data_type": data_type,
        "partition_type": "entity",
        "grid_task_count": len(grid_tasks),
        "grid_type": "isea4h",
        "grid_level": grid_level,
        "inferred_grid_level": inferred_level,
        "requested_grid_level": requested_level or None,
        "target_pixels_per_hex_edge": target_pixels,
        "cover_mode": args.cover_mode,
        "execution_engine": backend,
        "partition_backend_requested": backend_requested,
        "partition_backend_used": backend,
        "time_granularity": args.time_granularity,
        "partition_prefix_len": max(1, int(args.partition_prefix_len)),
        "rows": len(rows),
        "total_index_rows": len(rows),
        "entity_tile_count": len(rows),
        "distinct_space_codes": len({row["space_code"] for row in rows}),
        "distinct_st_codes": len({row["st_code"] for row in rows}),
        "rows_by_band": rows_by_band,
        "ray_parallelism": parallelism if backend == "ray" else 0,
        "ray_address": ray_address if backend == "ray" else "",
        "ray_init_elapsed_sec": round(ray_init_elapsed, 3),
        "chunk_size": chunk_size,
        "task_group_count": len(grouped_tasks),
        "ingest_enabled": ingest_enabled,
        "asset_storage_backend": asset_storage_backend,
        "metadata_backend": metadata_backend,
        "uploaded_tile_count": uploaded_tile_count,
        "metadata_rows": metadata_rows,
        "dataset": str(getattr(args, "dataset", "demo_optical")),
        "sensor": str(getattr(args, "sensor", "optical_mosaic")),
        "asset_version": str(getattr(args, "asset_version", getattr(args, "tile_version", "v1"))),
        "minio_bucket": str(getattr(args, "minio_bucket", "")) if asset_storage_backend == "minio" else "",
        "minio_prefix": str(getattr(args, "minio_prefix", "")) if asset_storage_backend == "minio" else "",
        "cog_elapsed_sec": 0.0,
        "partition_elapsed_sec": round(partition_elapsed, 3),
        "upload_elapsed_sec": round(upload_elapsed, 3),
        "metadata_elapsed_sec": round(metadata_elapsed, 3),
        "total_elapsed_sec": round(time.perf_counter() - total_start, 3),
    }
    (run_dir / "job_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local entity partition job for ISEA4H raster tiles")
    minio = runtime_config.minio_settings()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--manifest-path", default="")
    parser.add_argument("--product-family", default="auto")
    parser.add_argument("--data-type", default="optical", choices=sorted(ENTITY_DATA_TYPES))
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--cog-input-dir", required=True)
    parser.add_argument("--cog-overwrite", action="store_true")
    parser.add_argument("--cog-workers", type=int, default=0)
    parser.add_argument("--cog-compress", default="LZW")
    parser.add_argument("--cog-predictor", type=int, default=2)
    parser.add_argument("--cog-level", type=int, default=0)
    parser.add_argument("--cog-num-threads", default="ALL_CPUS")
    parser.add_argument("--target-crs", default="EPSG:4326")
    parser.add_argument("--grid-level", type=int, default=0)
    parser.add_argument("--target-pixels-per-hex-edge", type=int, default=DEFAULT_TARGET_PIXELS_PER_HEX_EDGE)
    parser.add_argument("--cover-mode", default="intersect", choices=["intersect", "contain", "minimal"])
    parser.add_argument("--time-granularity", default="day", choices=["year", "month", "day", "hour", "minute"])
    parser.add_argument("--max-cells-per-asset", type=int, default=20000)
    parser.add_argument("--partition-prefix-len", type=int, default=3)
    parser.add_argument("--ray-parallelism", type=int, default=0)
    parser.add_argument("--ray-address", default=runtime_config.ray_address())
    parser.add_argument("--chunk-size", type=int, default=0)
    parser.add_argument("--partition-backend", default="ray", choices=["auto", "ray", "thread", "local", "process"])
    parser.add_argument("--job-id", default="")
    parser.add_argument("--dataset", default="demo_optical")
    parser.add_argument("--sensor", default="optical_mosaic")
    parser.add_argument("--asset-version", default="v1")
    parser.add_argument("--metadata-backend", default="postgres", choices=["none", "local", "postgres"])
    parser.add_argument("--postgres-dsn", default=runtime_config.postgres_dsn())
    parser.add_argument("--asset-storage-backend", default="minio", choices=["local", "minio"])
    parser.add_argument("--minio-endpoint", default=minio.endpoint)
    parser.add_argument("--minio-access-key", default=minio.access_key)
    parser.add_argument("--minio-secret-key", default=minio.secret_key)
    parser.add_argument("--minio-bucket", default=minio.bucket)
    parser.add_argument("--minio-prefix", default="cube/entity")
    parser.add_argument("--minio-secure", action="store_true")
    parser.add_argument("--minio-upload-workers", type=int, default=8)
    return parser.parse_args()


def main() -> None:
    report = run_entity_partition(parse_args())
    print("=== Entity partition job completed ===")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
