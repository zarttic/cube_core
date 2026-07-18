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

import numpy as np
import rasterio
import rasterio.mask
from grid_core.app.models.grid_address import GridAddress
from grid_core.app.models.request import validate_requested_grid_level
from grid_core.sdk import CubeEncoderSDK
from pyproj import Transformer
from rasterio.warp import transform_geom

from cube_split import runtime_config
from cube_split.jobs.cancellation import PartitionCancelledError, cancel_ray_refs, check_cancelled, shutdown_ray_if_needed
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
    _bbox_intersection,
    _dataset_bounds_wgs84,
    _local_file_identity,
    _object_identity,
    _read_identity_sidecar,
    _safe_window_from_bounds,
    _wgs84_to_dataset_bounds,
    _write_identity_sidecar,
    build_grid_tasks_driver,
    build_manifest,
    create_unique_run_dir,
    resolve_asset_source_path,
)
from cube_split.tile_probe import TileProbeMetric, report_tile_metrics

DEFAULT_ENTITY_TASKS_PER_GROUP = 64
DEFAULT_ENTITY_MINIO_UPLOAD_WORKERS = 16
ENTITY_DATA_TYPES = {"optical", "product", "radar"}
_ENTITY_MODULE_SEARCH_ROOTS = (
    os.environ.get("RAY_RUNTIME_ENV_CREATE_WORKING_DIR", ""),
    "/tmp/ray/session_latest/runtime_resources/working_dir_files",
)


def _normalize_data_type(value: Any) -> str:
    data_type = str(value or "optical").strip().lower()
    if data_type not in ENTITY_DATA_TYPES:
        raise ValueError("data_type must be one of: optical, product, radar")
    return data_type


def _normalize_grid_type(value: Any) -> str:
    grid_type = str(value or "isea4h").strip().lower()
    if grid_type != "isea4h":
        raise ValueError("grid_type must be isea4h for entity partitioning")
    return grid_type


def _normalize_entity_clip_mode(value: Any) -> str:
    mode = str(value or "exact").strip().lower()
    if mode not in {"bbox", "exact"}:
        raise ValueError("entity_clip_mode must be one of: bbox, exact")
    return mode


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


def _resolve_entity_module_path() -> str | None:
    entity_module_path = None
    project_roots = _ray_project_roots()
    for project_root in project_roots:
        candidate = os.path.abspath(os.path.join(project_root, "cube_split", "cube_split", "jobs", "entity_partition_job.py"))
        if entity_module_path is None and os.path.exists(candidate):
            entity_module_path = candidate
    for search_root in _ENTITY_MODULE_SEARCH_ROOTS:
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
            return entity_module_path
    return entity_module_path


def _ensure_center_cell_tasks(
    assets: list[AssetRecord],
    tasks: list[dict[str, Any]],
    grid_type: str,
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
        cell = sdk.locate(grid_type=grid_type, requested_grid_level=grid_level, point=center)
        out.append(
            {
                "scene_id": asset.scene_id,
                "band": asset.band,
                "asset_path": asset.path,
                "acq_time": asset.acq_time,
                "grid_type": grid_type,
                "grid_level": grid_level,
                "space_code": cell.space_code,
                "topology_code": cell.topology_code,
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


def _cell_geometry_for_dataset(
    sdk: CubeEncoderSDK,
    ds: rasterio.DatasetReader,
    *,
    address: GridAddress,
) -> dict[str, Any]:
    geom = sdk.code_to_geometry(address=address)
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


def _add_timing(timing: dict[str, float] | None, key: str, value: float) -> None:
    if timing is not None:
        timing[key] = timing.get(key, 0.0) + float(value)


def _minio_upload_workers(args: argparse.Namespace) -> int:
    value = getattr(args, "minio_upload_workers", DEFAULT_ENTITY_MINIO_UPLOAD_WORKERS)
    return max(1, int(value or DEFAULT_ENTITY_MINIO_UPLOAD_WORKERS))


def _make_minio_client(args: argparse.Namespace, *, http_pool_size: int) -> Any:
    try:
        from minio import Minio
    except ModuleNotFoundError as exc:
        raise RuntimeError("MinIO backend requires `minio` package") from exc

    import certifi
    import urllib3
    from urllib3.util import Timeout
    from urllib3.util.retry import Retry

    timeout = 300
    return Minio(
        str(getattr(args, "minio_endpoint", "")),
        access_key=str(getattr(args, "minio_access_key", "")),
        secret_key=str(getattr(args, "minio_secret_key", "")),
        secure=bool(getattr(args, "minio_secure", False)),
        http_client=urllib3.PoolManager(
            timeout=Timeout(connect=timeout, read=timeout),
            maxsize=max(10, int(http_pool_size)),
            cert_reqs="CERT_REQUIRED",
            ca_certs=os.environ.get("SSL_CERT_FILE") or certifi.where(),
            retries=Retry(total=5, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504]),
        ),
    )


def _write_entity_tiles(
    tasks: list[dict[str, Any]],
    run_dir: Path,
    time_granularity: str,
    partition_prefix_len: int,
    data_type: str = "optical",
    source_options: dict[str, Any] | None = None,
    timing: dict[str, float] | None = None,
    clip_mode: str = "exact",
    tile_upload_options: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    data_type = _normalize_data_type(data_type)
    clip_mode = _normalize_entity_clip_mode(clip_mode)
    sdk = CubeEncoderSDK()
    st_cache: dict[str, str] = {}
    geometry_cache: dict[tuple[str, str, str], dict[str, Any]] = {}
    rows: list[dict[str, Any]] = []
    upload_pool = None
    upload_futures = []
    upload_start_time: float | None = None
    upload_args = argparse.Namespace(**tile_upload_options) if tile_upload_options else None
    upload_client = None
    upload_bucket = ""
    upload_fast = True
    if upload_args is not None:
        upload_bucket = str(getattr(upload_args, "minio_bucket", ""))
        upload_workers = _minio_upload_workers(upload_args)
        upload_client = _make_minio_client(upload_args, http_pool_size=upload_workers)
        if not upload_client.bucket_exists(upload_bucket):
            upload_client.make_bucket(upload_bucket)
        upload_fast = bool(getattr(upload_args, "minio_fast_upload", True))
        upload_pool = ThreadPoolExecutor(max_workers=upload_workers)
    task_groups: dict[str, list[dict[str, Any]]] = {}
    try:
        for task in tasks:
            task_groups.setdefault(str(task["asset_path"]), []).append(task)

        for asset_path, asset_tasks in sorted(task_groups.items()):
            resolve_start = time.perf_counter()
            local_asset_path = resolve_asset_source_path(asset_path, source_options)
            _add_timing(timing, "entity_source_resolve_elapsed_sec", time.perf_counter() - resolve_start)
            open_start = time.perf_counter()
            with rasterio.open(local_asset_path) as ds:
                _add_timing(timing, "entity_dataset_open_elapsed_sec", time.perf_counter() - open_start)
                base_profile = ds.profile.copy()
                for key in ("compress", "predictor", "zlevel", "level", "blockxsize", "blockysize"):
                    base_profile.pop(key, None)
                crs_key = ds.crs.to_string() if ds.crs else ""
                exact_clip_cache: dict[tuple[str, str], tuple[Any, Any, np.ndarray]] = {}
                ds_bounds_wgs84: tuple[float, float, float, float] | None = None
                ds_wgs84_to_native: Transformer | None = None
                if clip_mode == "bbox":
                    bounds_start = time.perf_counter()
                    ds_bounds_wgs84 = _dataset_bounds_wgs84(ds)
                    ds_wgs84_to_native = (
                        Transformer.from_crs("EPSG:4326", ds.crs, always_xy=True)
                        if ds.crs and str(ds.crs).upper() != "EPSG:4326"
                        else None
                    )
                    _add_timing(timing, "entity_dataset_bounds_elapsed_sec", time.perf_counter() - bounds_start)
                for task in sorted(asset_tasks, key=lambda row: str(row["space_code"])):
                    geom = None
                    exact_clip = None
                    if clip_mode == "exact":
                        geom_key = (crs_key, str(task["grid_type"]), str(task["space_code"]))
                        geom = geometry_cache.get(geom_key)
                        if geom is None:
                            geom_start = time.perf_counter()
                            geom = _cell_geometry_for_dataset(
                                sdk,
                                ds,
                                address=GridAddress(
                                    grid_type=geom_key[1],
                                    grid_level=int(task["grid_level"]),
                                    space_code=geom_key[2],
                                    topology_code=task.get("topology_code"),
                                ),
                            )
                            geometry_cache[geom_key] = geom
                            _add_timing(timing, "entity_geometry_elapsed_sec", time.perf_counter() - geom_start)
                        else:
                            _add_timing(timing, "entity_geometry_cache_hit_count", 1.0)
                        clip_key = (str(task["grid_type"]), str(task["space_code"]))
                        exact_clip = exact_clip_cache.get(clip_key)
                        if exact_clip is None:
                            mask_start = time.perf_counter()
                            try:
                                shape_mask, out_transform, win = rasterio.mask.raster_geometry_mask(
                                    ds,
                                    [geom],
                                    crop=True,
                                )
                            except ValueError:
                                _add_timing(timing, "entity_tile_mask_elapsed_sec", time.perf_counter() - mask_start)
                                _add_timing(timing, "entity_tile_empty_count", 1.0)
                                continue
                            _add_timing(timing, "entity_tile_mask_elapsed_sec", time.perf_counter() - mask_start)
                            if shape_mask.size == 0 or bool(shape_mask.all()):
                                _add_timing(timing, "entity_tile_empty_count", 1.0)
                                continue
                            exact_clip = (win, out_transform, shape_mask)
                            exact_clip_cache[clip_key] = exact_clip
                        else:
                            _add_timing(timing, "entity_tile_mask_cache_hit_count", 1.0)
                    else:
                        assert ds_bounds_wgs84 is not None
                        inter = _bbox_intersection(
                            ds_bounds_wgs84,
                            (
                                float(task["cell_min_lon"]),
                                float(task["cell_min_lat"]),
                                float(task["cell_max_lon"]),
                                float(task["cell_max_lat"]),
                            ),
                        )
                        if inter is None:
                            continue
                        left, bottom, right, top = _wgs84_to_dataset_bounds(ds, *inter, transformer=ds_wgs84_to_native)
                        win = _safe_window_from_bounds(ds, left, bottom, right, top)
                        if win is None:
                            continue
                    for band_index in range(1, ds.count + 1):
                        band_name = str(task["band"]) if ds.count == 1 else f"{task['band']}_band{band_index}"
                        nodata = _nodata_value(ds, band_index)
                        read_start = time.perf_counter()
                        if clip_mode == "exact":
                            assert exact_clip is not None
                            win, out_transform, shape_mask = exact_clip
                            data = ds.read(band_index, window=win, masked=True)
                            _add_timing(timing, "entity_tile_read_elapsed_sec", time.perf_counter() - read_start)
                            apply_mask_start = time.perf_counter()
                            data = np.ma.array(
                                data,
                                mask=np.logical_or(np.ma.getmaskarray(data), shape_mask),
                                copy=False,
                            )
                            _add_timing(timing, "entity_tile_mask_elapsed_sec", time.perf_counter() - apply_mask_start)
                        else:
                            data = ds.read(band_index, window=win, masked=True)
                            out_transform = ds.window_transform(win)
                            _add_timing(timing, "entity_tile_read_elapsed_sec", time.perf_counter() - read_start)
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
                            / _safe_name(str(task["grid_type"]))
                            / f"L{int(task['grid_level'])}"
                            / _safe_name(str(task["space_code"]))
                        )
                        tile_dir.mkdir(parents=True, exist_ok=True)
                        tile_path = tile_dir / f"{_safe_name(band_name)}.tif"
                        profile = base_profile.copy()
                        profile.update(
                            driver="GTiff",
                            height=int(filled.shape[-2]),
                            width=int(filled.shape[-1]),
                            count=1,
                            transform=out_transform,
                            nodata=nodata,
                            compress="ZSTD",
                            predictor=2,
                            zstd_level=1,
                            tiled=True,
                            blockxsize=512,
                            blockysize=512,
                            num_threads="2",
                        )
                        write_start = time.perf_counter()
                        with rasterio.open(tile_path, "w", **profile) as out_ds:
                            out_ds.write(filled, 1)
                        _add_timing(timing, "entity_tile_write_elapsed_sec", time.perf_counter() - write_start)
                        _add_timing(timing, "entity_tile_count", 1.0)

                        acq_time = str(task["acq_time"])
                        st_time_granularity = _st_time_granularity(time_granularity)
                        st_key = "|".join(
                            [
                                str(task["grid_type"]),
                                str(int(task["grid_level"])),
                                str(task["space_code"]),
                                acq_time,
                                st_time_granularity,
                            ]
                        )
                        if st_key not in st_cache:
                            st_cache[st_key] = sdk.generate_st_code(
                                address=GridAddress(
                                    grid_type=str(task["grid_type"]),
                                    grid_level=int(task["grid_level"]),
                                    space_code=str(task["space_code"]),
                                    topology_code=task.get("topology_code"),
                                ),
                                timestamp=datetime.fromisoformat(acq_time.replace("Z", "+00:00")),
                                time_granularity=st_time_granularity,
                            ).st_code

                        row = {
                            "partition_type": "entity",
                            "data_type": data_type,
                            "scene_id": task["scene_id"],
                            "band": band_name,
                            "asset_path": str(tile_path.resolve()),
                            "source_asset_path": str(task.get("source_asset_path") or asset_path),
                            "output_path": str(tile_path.resolve()),
                            "acq_time": acq_time,
                            "grid_type": str(task["grid_type"]),
                            "partition_method": "entity",
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
                        rows.append(row)
                        if upload_pool is not None and upload_client is not None and upload_args is not None:
                            if upload_start_time is None:
                                upload_start_time = time.perf_counter()
                            upload_futures.append(
                                upload_pool.submit(
                                    _upload_entity_tile_file,
                                    upload_client,
                                    upload_bucket,
                                    str(tile_path.resolve()),
                                    row,
                                    upload_args,
                                    upload_fast,
                                )
                            )
        if upload_pool is not None:
            asset_uri_map = dict(future.result() for future in upload_futures)
            if upload_start_time is not None:
                _add_timing(timing, "entity_tile_upload_elapsed_sec", time.perf_counter() - upload_start_time)
            _add_timing(timing, "entity_tile_upload_count", float(len(asset_uri_map)))
            rows = _rows_with_asset_uris(rows, asset_uri_map, keep_local_asset_path=False)
    finally:
        if upload_pool is not None:
            upload_pool.shutdown(wait=True)
    return rows


def _prepare_entity_source_tasks(
    tasks: list[dict[str, Any]],
    source_options: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    local_by_source = {
        asset_path: resolve_asset_source_path(asset_path, source_options)
        for asset_path in sorted({str(task["asset_path"]) for task in tasks})
    }
    prepared: list[dict[str, Any]] = []
    for task in tasks:
        source_asset_path = str(task.get("source_asset_path") or task["asset_path"])
        local_asset_path = local_by_source[str(task["asset_path"])]
        prepared.append({**task, "asset_path": local_asset_path, "source_asset_path": source_asset_path})
    return prepared


def _group_tasks_for_parallel_processing(
    tasks: list[dict[str, Any]],
    partition_prefix_len: int,
    max_tasks_per_group: int = DEFAULT_ENTITY_TASKS_PER_GROUP,
) -> list[list[dict[str, Any]]]:
    prefix_len = max(1, int(partition_prefix_len))
    group_limit = max(1, int(max_tasks_per_group))
    buckets: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for task in tasks:
        asset_path = str(task["asset_path"])
        space_code = str(task["space_code"])
        buckets.setdefault((asset_path, space_code[:prefix_len]), []).append(task)

    groups: list[list[dict[str, Any]]] = []
    for key in sorted(buckets):
        bucket = sorted(buckets[key], key=lambda row: str(row["space_code"]))
        for idx in range(0, len(bucket), group_limit):
            groups.append(bucket[idx : idx + group_limit])
    return groups


def _flatten_task_groups(task_groups: list[list[dict[str, Any]]]) -> list[dict[str, Any]]:
    return [task for group in task_groups for task in group]


def _assign_entity_task_groups_to_actors(
    task_chunks: list[list[list[dict[str, Any]]]],
    parallelism: int,
) -> list[list[list[dict[str, Any]]]]:
    all_task_groups = [group for chunk in task_chunks for group in chunk]
    actor_count = max(1, min(int(parallelism), len(all_task_groups)))
    task_groups_by_actor: list[list[list[dict[str, Any]]]] = [[] for _ in range(actor_count)]
    groups_by_asset: dict[str, list[list[dict[str, Any]]]] = {}
    for group in all_task_groups:
        asset_path = str(group[0]["asset_path"])
        groups_by_asset.setdefault(asset_path, []).append(group)

    asset_items = sorted(groups_by_asset.items())
    if len(asset_items) <= actor_count:
        slots_by_asset = {asset_path: 1 for asset_path, _ in asset_items}
        remaining_slots = actor_count - len(asset_items)
        total_group_count = max(1, len(all_task_groups))
        fractions: list[tuple[float, int, str]] = []
        for asset_path, groups in asset_items:
            ideal_slots = actor_count * (len(groups) / total_group_count)
            whole_slots = max(1, min(len(groups), math.floor(ideal_slots)))
            slots_by_asset[asset_path] = whole_slots
            remaining_slots -= whole_slots - 1
            fractions.append((ideal_slots - math.floor(ideal_slots), len(groups), asset_path))
        while remaining_slots > 0:
            allocated = False
            for _, _, asset_path in sorted(fractions, reverse=True):
                if slots_by_asset[asset_path] >= len(groups_by_asset[asset_path]):
                    continue
                slots_by_asset[asset_path] += 1
                remaining_slots -= 1
                allocated = True
                if remaining_slots == 0:
                    break
            if not allocated:
                break

        actor_idx = 0
        for asset_path, groups in asset_items:
            slot_count = slots_by_asset[asset_path]
            actor_indices = list(range(actor_idx, actor_idx + slot_count))
            actor_idx += slot_count
            for group_idx, group in enumerate(groups):
                task_groups_by_actor[actor_indices[group_idx % slot_count]].append(group)
    else:
        actor_loads = [0] * actor_count
        for _, groups in sorted(asset_items, key=lambda item: len(item[1]), reverse=True):
            actor_idx = min(range(actor_count), key=lambda idx: actor_loads[idx])
            task_groups_by_actor[actor_idx].extend(groups)
            actor_loads[actor_idx] += len(groups)

    return [groups for groups in task_groups_by_actor if groups]


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
    clip_mode: str = "exact",
) -> list[dict[str, Any]]:
    def process_chunk(chunk: list[list[dict[str, Any]]]) -> list[dict[str, Any]]:
        tasks = _flatten_task_groups(chunk)
        return _write_entity_tiles(
            tasks,
            run_dir,
            time_granularity,
            partition_prefix_len,
            data_type=data_type,
            clip_mode=clip_mode,
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
    source_options: dict[str, Any] | None = None,
    tile_upload_options: dict[str, Any] | None = None,
    cancellation_check: Any | None = None,
    clip_mode: str = "exact",
) -> tuple[list[dict[str, Any]], float, float, float, float, float, dict[str, float]]:
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
    class EntityTileProcessor:
        def __init__(self) -> None:
            self._source_path_by_uri: dict[str, str] = {}

        def prepare_sources(
            self,
            source_uris: list[str],
            source_options_value: dict[str, Any] | None,
        ) -> dict[str, Any]:
            import os

            env_options = dict(source_options_value or {})
            if env_options.get("endpoint"):
                os.environ["CUBE_WEB_MINIO_ENDPOINT"] = str(env_options["endpoint"])
            if env_options.get("access_key"):
                os.environ["CUBE_WEB_MINIO_ACCESS_KEY"] = str(env_options["access_key"])
            if env_options.get("secret_key"):
                os.environ["CUBE_WEB_MINIO_SECRET_KEY"] = str(env_options["secret_key"])

            from cube_split.jobs.ray_partition_core import resolve_asset_source_path as resolve_source

            started = time.perf_counter()
            for source_uri in sorted(set(source_uris)):
                self._source_path_by_uri[str(source_uri)] = resolve_source(str(source_uri), source_options_value)
            return {"source_prepare_elapsed_sec": time.perf_counter() - started}

        def process_groups(
            self,
            task_groups: list[list[dict[str, Any]]],
            run_dir_text: str,
            time_granularity_text: str,
            prefix_len: int,
            data_type_text: str,
            source_options_value: dict[str, Any] | None,
            tile_upload_options_value: dict[str, Any] | None,
            clip_mode_text: str,
        ) -> dict[str, Any]:
            import os
            import sys
            from pathlib import Path

            entity_module_path = _resolve_entity_module_path()
            _prepend_sys_paths(
                [os.path.abspath(os.path.join(project_root, rel_path)) for project_root in _ray_project_roots() for rel_path in ("", "cube_encoder", "cube_split", "cube_web")]
            )

            try:
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

            flat_tasks = [task for group in task_groups for task in group]
            env_options = dict(source_options_value or {})
            if env_options.get("endpoint"):
                os.environ["CUBE_WEB_MINIO_ENDPOINT"] = str(env_options["endpoint"])
            if env_options.get("access_key"):
                os.environ["CUBE_WEB_MINIO_ACCESS_KEY"] = str(env_options["access_key"])
            if env_options.get("secret_key"):
                os.environ["CUBE_WEB_MINIO_SECRET_KEY"] = str(env_options["secret_key"])

            prepared_tasks = []
            for task in flat_tasks:
                source_asset_path = str(task.get("source_asset_path") or task["asset_path"])
                local_asset_path = self._source_path_by_uri.get(str(task["asset_path"]))
                if local_asset_path is None:
                    local_asset_path = _prepare_entity_source_tasks([task], source_options_value)[0]["asset_path"]
                    self._source_path_by_uri[str(task["asset_path"])] = local_asset_path
                prepared_tasks.append({**task, "asset_path": local_asset_path, "source_asset_path": source_asset_path})
            partition_start = time.perf_counter()
            writer_timing: dict[str, float] = {}
            writer_start = time.perf_counter()
            rows = writer(
                prepared_tasks,
                run_dir=Path(run_dir_text),
                time_granularity=time_granularity_text,
                partition_prefix_len=prefix_len,
                data_type=data_type_text,
                source_options=None,
                timing=writer_timing,
                clip_mode=clip_mode_text,
                tile_upload_options=tile_upload_options_value,
            )
            writer_timing["entity_writer_wall_elapsed_sec"] = time.perf_counter() - writer_start
            return {
                "rows": rows,
                "partition_elapsed_sec": time.perf_counter() - partition_start,
                "stats": writer_timing,
            }

    task_groups_by_actor = _assign_entity_task_groups_to_actors(task_chunks, parallelism)
    actor_count = len(task_groups_by_actor)
    actor_cls = EntityTileProcessor.options(**_ray_actor_options_from_env())
    actors = [actor_cls.remote() for _ in range(actor_count)]

    def submit_actor(actor_idx: int):
        return actors[actor_idx].process_groups.remote(
            task_groups_by_actor[actor_idx],
            str(run_dir),
            time_granularity,
            partition_prefix_len,
            data_type,
            source_options,
            tile_upload_options,
            clip_mode,
        )

    rows: list[dict[str, Any]] = []
    source_prepare_elapsed = 0.0
    source_prepare_worker_elapsed = 0.0
    partition_wall_start = time.perf_counter()
    worker_partition_elapsed = 0.0
    worker_stats: dict[str, float] = {}
    pending = []
    pending_actor_by_ref: dict[Any, int] = {}
    try:
        if cancellation_check is not None and cancellation_check():
            raise PartitionCancelledError("Partition task cancelled")
        source_prepare_start = time.perf_counter()
        source_uris_by_actor: dict[int, set[str]] = {idx: set() for idx in range(actor_count)}
        for actor_idx, task_groups in enumerate(task_groups_by_actor):
            for group in task_groups:
                for task in group:
                    source_uris_by_actor[actor_idx].add(str(task["asset_path"]))
        source_prepare_refs = [
            actor.prepare_sources.remote(sorted(source_uris_by_actor[idx]), source_options) for idx, actor in enumerate(actors)
        ]
        try:
            for prepare_result in ray.get(source_prepare_refs):
                source_prepare_worker_elapsed += float(prepare_result.get("source_prepare_elapsed_sec") or 0.0)
        except Exception:
            cancel_ray_refs(ray, source_prepare_refs)
            raise
        source_prepare_elapsed = time.perf_counter() - source_prepare_start
        partition_wall_start = time.perf_counter()
        for actor_idx, task_groups in enumerate(task_groups_by_actor):
            if not task_groups:
                continue
            if cancellation_check is not None and cancellation_check():
                raise PartitionCancelledError("Partition task cancelled")
            ref = submit_actor(actor_idx)
            pending.append(ref)
            pending_actor_by_ref[ref] = actor_idx
        while pending:
            if cancellation_check is not None and cancellation_check():
                raise PartitionCancelledError("Partition task cancelled")
            ready, pending = ray.wait(pending, num_returns=1, timeout=1.0)
            if not ready:
                continue
            for ready_ref in ready:
                actor_idx = pending_actor_by_ref.pop(ready_ref)
                chunk_result = ray.get(ready_ref)
                chunk_rows = list(chunk_result.get("rows", []))
                rows.extend(chunk_rows)
                worker_partition_elapsed += float(chunk_result.get("partition_elapsed_sec") or 0.0)
                for key, value in dict(chunk_result.get("stats") or {}).items():
                    worker_stats[key] = worker_stats.get(key, 0.0) + float(value or 0.0)
    except PartitionCancelledError:
        cancel_ray_refs(ray, pending)
        raise
    except Exception:
        cancel_ray_refs(ray, pending)
        raise
    finally:
        for actor in actors:
            ray.kill(actor, no_restart=True)
        shutdown_ray_if_needed(ray, ray_already_initialized)
    return (
        rows,
        ray_init_elapsed,
        source_prepare_elapsed,
        time.perf_counter() - partition_wall_start,
        source_prepare_worker_elapsed,
        worker_partition_elapsed,
        worker_stats,
    )


def _upload_entity_tiles_to_minio(rows: list[dict[str, Any]], args: argparse.Namespace) -> dict[str, str]:
    endpoint = str(getattr(args, "minio_endpoint", ""))
    access_key = str(getattr(args, "minio_access_key", ""))
    secret_key = str(getattr(args, "minio_secret_key", ""))
    bucket = str(getattr(args, "minio_bucket", ""))
    if not endpoint or not access_key or not secret_key or not bucket:
        raise ValueError("minio endpoint/access-key/secret-key/bucket are required for entity tile upload")

    workers = _minio_upload_workers(args)
    client = _make_minio_client(args, http_pool_size=workers)
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    unique_tiles: dict[str, dict[str, Any]] = {}
    for row in rows:
        unique_tiles.setdefault(str(row["asset_path"]), row)

    fast_upload = bool(getattr(args, "minio_fast_upload", True))

    def upload_one(item: tuple[str, dict[str, Any]]) -> tuple[str, str]:
        source_uri, row = item
        return _upload_entity_tile_file(client, bucket, source_uri, row, args, fast_upload)

    if workers == 1:
        return dict(upload_one(item) for item in unique_tiles.items())
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return dict(pool.map(upload_one, unique_tiles.items()))


def _entity_tile_object_key(row: dict[str, Any], source_path: Path, args: argparse.Namespace) -> str:
    prefix = str(getattr(args, "minio_prefix", "cube/entity") or "cube/entity").strip("/")
    dataset = _safe_name(str(getattr(args, "dataset", "optical_default") or "optical_default"))
    sensor = _safe_name(str(getattr(args, "sensor", "optical_mosaic") or "optical_mosaic"))
    version = _safe_name(str(getattr(args, "asset_version", getattr(args, "tile_version", "v1")) or "v1"))
    scene_id = _safe_name(str(row.get("scene_id") or "unknown_scene"))
    band = _safe_name(str(row.get("band") or source_path.stem))
    grid_level = int(row.get("grid_level") or 0)
    space_code = _safe_name(str(row.get("space_code") or "unknown_cell"))
    date_path = datetime.fromisoformat(str(row["acq_time"]).replace("Z", "+00:00")).strftime("%Y/%m/%d")
    return (
        f"{prefix}/dataset={dataset}/sensor={sensor}/acq_date={date_path}/"
        f"scene_id={scene_id}/grid={_safe_name(str(row.get('grid_type') or 'isea4h'))}/L{grid_level}/space_code={space_code}/"
        f"band={band}/version={version}/{source_path.name}"
    )


def _rows_with_asset_uris(
    rows: list[dict[str, Any]],
    asset_uri_map: dict[str, str],
    *,
    keep_local_asset_path: bool = True,
) -> list[dict[str, Any]]:
    updated: list[dict[str, Any]] = []
    for row in rows:
        next_row = dict(row)
        local_path = str(row["asset_path"])
        tile_uri = asset_uri_map.get(local_path, local_path)
        if keep_local_asset_path:
            next_row["local_asset_path"] = local_path
        else:
            next_row.pop("local_asset_path", None)
        next_row["entity_tile_uri"] = tile_uri
        next_row["asset_path"] = tile_uri
        next_row["output_path"] = tile_uri
        updated.append(next_row)
    return updated


def _entity_tile_upload_options(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "dataset": str(getattr(args, "dataset", "optical_default")),
        "sensor": str(getattr(args, "sensor", "optical_mosaic")),
        "asset_version": str(getattr(args, "asset_version", "v1")),
        "minio_endpoint": str(getattr(args, "minio_endpoint", "")),
        "minio_access_key": str(getattr(args, "minio_access_key", "")),
        "minio_secret_key": str(getattr(args, "minio_secret_key", "")),
        "minio_bucket": str(getattr(args, "minio_bucket", "")),
        "minio_prefix": str(getattr(args, "minio_prefix", "cube/entity")),
        "minio_secure": bool(getattr(args, "minio_secure", False)),
        "minio_upload_workers": _minio_upload_workers(args),
        "minio_fast_upload": bool(getattr(args, "minio_fast_upload", True)),
    }


def _validate_entity_tile_upload_options(options: dict[str, Any]) -> None:
    if not all(str(options.get(key) or "") for key in ("minio_endpoint", "minio_access_key", "minio_secret_key", "minio_bucket")):
        raise ValueError("Ray entity partition requires minio endpoint/access-key/secret-key/bucket for shared tile output")


def _count_remote_entity_tiles(rows: list[dict[str, Any]]) -> int:
    tile_paths = {str(row.get("asset_path") or "") for row in rows}
    if any(not path.startswith("s3://") for path in tile_paths):
        raise RuntimeError("Ray entity partition requires worker tile upload to MinIO; local worker paths are not shared")
    return len(tile_paths)


def _upload_entity_tile_file(
    client: Any,
    bucket: str,
    source_uri: str,
    row: dict[str, Any],
    args: argparse.Namespace,
    fast_upload: bool,
) -> tuple[str, str]:
    if source_uri.startswith("s3://"):
        return source_uri, source_uri
    source_path = Path(source_uri)
    if not source_path.exists():
        raise FileNotFoundError(f"Entity tile file not found: {source_path}")
    key = _entity_tile_object_key(row, source_path, args)
    if fast_upload:
        client.fput_object(bucket, key, str(source_path))
        return source_uri, f"s3://{bucket}/{key}"
    local_identity = _local_file_identity(source_path)
    identity_state = _read_identity_sidecar(source_path)
    try:
        stat = client.stat_object(bucket, key)
        remote_identity = _object_identity(stat)
        if (
            stat.size == source_path.stat().st_size
            and identity_state.get("local") == local_identity
            and identity_state.get("remote") == remote_identity
        ):
            return source_uri, f"s3://{bucket}/{key}"
    except Exception as exc:
        if exc.__class__.__name__ != "S3Error" or getattr(exc, "code", "") not in {"NoSuchKey", "NoSuchObject"}:
            raise
    client.fput_object(bucket, key, str(source_path))
    stat = client.stat_object(bucket, key)
    _write_identity_sidecar(source_path, local=local_identity, remote=_object_identity(stat))
    return source_uri, f"s3://{bucket}/{key}"


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


def _report_entity_tile_metrics(rows: list[dict[str, Any]], *, dataset: str, sensor: str, tile_version: str, run_id: str) -> None:
    report_tile_metrics(
        TileProbeMetric(
            task_name=f"cube.partition.entity.ingest.{row.get('data_type') or 'optical'}",
            tile_type="ingest",
            method_name="merge.rs_entity_tile_asset",
            attributes={
                "cube.stage": "ingest",
                "cube.target_table": "rs_entity_tile_asset",
                "cube.data_type": row.get("data_type") or "optical",
                "cube.dataset": dataset,
                "cube.sensor": sensor,
                "cube.scene_id": row["scene_id"],
                "cube.band": row["band"],
                "cube.grid_type": row["grid_type"],
                "cube.grid_level": int(row["grid_level"]),
                "cube.space_code": row["space_code"],
                "cube.time_bucket": row["time_bucket"],
                "cube.st_code": row["st_code"],
                "cube.tile_version": tile_version,
                "cube.cover_mode": row["cover_mode"],
                "cube.run_id": run_id,
            },
        )
        for row in rows
    )

def _write_entity_metadata_postgres(rows: list[dict[str, Any]], args: argparse.Namespace, run_dir: Path) -> dict[str, Any]:
    from cube_split.ingest.ray_ingest_job import _upsert_job_status_postgres

    try:
        import psycopg
    except ModuleNotFoundError as exc:
        raise RuntimeError("Postgres backend requires `psycopg` package") from exc

    dsn = str(getattr(args, "postgres_dsn", ""))
    if not dsn:
        raise ValueError("postgres_dsn is required when metadata_backend=postgres")

    dataset = str(getattr(args, "dataset", "optical_default"))
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
            _report_entity_tile_metrics(rows, dataset=dataset, sensor=sensor, tile_version=tile_version, run_id=run_id)
        except Exception as exc:
            finished_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            conn.rollback()
            _ensure_entity_tables_postgres(conn)
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
    check_cancelled(args)
    for key in ("SPARK_HOME", "SPARK_CONF_DIR", "HADOOP_CONF_DIR", "YARN_CONF_DIR"):
        os.environ.pop(key, None)

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")
    data_type = _normalize_data_type(getattr(args, "data_type", "optical"))
    grid_type = _normalize_grid_type(getattr(args, "grid_type", "isea4h"))
    entity_clip_mode = _normalize_entity_clip_mode(getattr(args, "entity_clip_mode", "exact"))

    backend_requested = str(getattr(args, "partition_backend", "thread") or "thread")
    ray_address = str(getattr(args, "ray_address", "") or "")
    backend = _resolve_backend(backend_requested, ray_address)
    if backend == "ray":
        _validate_entity_tile_upload_options(_entity_tile_upload_options(args))

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

    grid_level = int(getattr(args, "grid_level", 6) or 6)
    validate_requested_grid_level(grid_type, grid_level)
    max_cells_per_asset = int(getattr(args, "max_cells_per_asset", 0) or 0)
    if max_cells_per_asset < 0:
        raise ValueError("max_cells_per_asset must be greater than or equal to 0")

    grid_tasks = build_grid_tasks_driver(
        assets=assets,
        grid_type=grid_type,
        grid_level=grid_level,
        cover_mode=args.cover_mode,
        max_cells_per_asset=max_cells_per_asset,
    )
    grid_tasks = _ensure_center_cell_tasks(assets, grid_tasks, grid_type, grid_level, args.cover_mode)
    if not grid_tasks:
        raise RuntimeError(
            "No grid tasks produced for input assets; check grid_type/grid_level/cover_mode or source extent"
        )
    check_cancelled(args)

    run_dir = create_unique_run_dir(output_dir)

    if backend not in {"ray", "thread"}:
        raise ValueError("partition_backend must be one of: auto, ray, thread, local, process")

    requested_ray_parallelism = int(getattr(args, "ray_parallelism", 0) or 0)
    initial_parallelism = _resolve_ray_parallelism(len(grid_tasks), requested_ray_parallelism)
    max_tasks_per_group = max(1, math.ceil(len(grid_tasks) / max(1, initial_parallelism)))
    max_tasks_per_group = min(DEFAULT_ENTITY_TASKS_PER_GROUP, max_tasks_per_group)
    grouped_tasks = _group_tasks_for_parallel_processing(
        grid_tasks,
        int(args.partition_prefix_len),
        max_tasks_per_group=max_tasks_per_group,
    )
    if not grouped_tasks:
        raise RuntimeError("No partition task groups produced after task preparation")
    parallelism = _resolve_ray_parallelism(len(grouped_tasks), requested_ray_parallelism)
    parallelism = max(1, min(len(grouped_tasks), parallelism))
    chunk_size = _resolve_ray_chunk_size(
        len(grouped_tasks),
        parallelism,
        int(getattr(args, "chunk_size", 0) or 0),
    )
    task_chunks = _chunk_tasks_for_ray(grouped_tasks, chunk_size)

    requested_asset_storage_backend = str(getattr(args, "asset_storage_backend", "local") or "local")
    if requested_asset_storage_backend not in {"local", "minio"}:
        raise ValueError("asset_storage_backend must be one of: local, minio")
    asset_storage_backend = "minio" if backend == "ray" else requested_asset_storage_backend
    if backend == "ray" and requested_asset_storage_backend == "local":
        # Ray workers may run on different nodes; worker-local tile paths are not valid driver outputs.
        asset_storage_backend = "minio"

    if backend == "ray":
        minio_options = {
            "endpoint": str(getattr(args, "minio_endpoint", "")),
            "access_key": str(getattr(args, "minio_access_key", "")),
            "secret_key": str(getattr(args, "minio_secret_key", "")),
            "secure": bool(getattr(args, "minio_secure", False)),
            "bucket": str(getattr(args, "minio_bucket", "")),
        }
        tile_upload_options = _entity_tile_upload_options(args)
        _validate_entity_tile_upload_options(tile_upload_options)
        ray_result = _write_entity_tile_chunks_ray(
            task_chunks=task_chunks,
            run_dir=run_dir,
            time_granularity=args.time_granularity,
            partition_prefix_len=args.partition_prefix_len,
            parallelism=parallelism,
            ray_address=ray_address,
            data_type=data_type,
            source_options=minio_options,
            tile_upload_options=tile_upload_options,
            cancellation_check=getattr(args, "cancellation_check", None),
            clip_mode=entity_clip_mode,
        )
        (
            rows,
            *_timing,
        ) = ray_result
    else:
        rows = _write_entity_tile_chunks_thread(
            task_chunks=task_chunks,
            run_dir=run_dir,
            time_granularity=args.time_granularity,
            partition_prefix_len=args.partition_prefix_len,
            workers=parallelism,
            data_type=data_type,
            clip_mode=entity_clip_mode,
        )
    check_cancelled(args)

    uploaded_tile_count = 0
    metadata_rows = 0
    metadata_backend = str(getattr(args, "metadata_backend", "none") or "none")
    explicit_ingest = getattr(args, "ingest_enabled", True)
    ingest_enabled = True if explicit_ingest is None else bool(explicit_ingest)
    if not ingest_enabled:
        if backend != "ray":
            asset_storage_backend = "local"
        metadata_backend = "none"
    asset_uri_map: dict[str, str] = {}
    if backend == "ray":
        uploaded_tile_count = _count_remote_entity_tiles(rows)
    elif asset_storage_backend == "minio":
        check_cancelled(args)
        asset_uri_map = _upload_entity_tiles_to_minio(rows, args)
        uploaded_tile_count = len(asset_uri_map)
        rows = _rows_with_asset_uris(rows, asset_uri_map)

    if metadata_backend == "postgres":
        check_cancelled(args)
        metadata_args = argparse.Namespace(
            **{
                **vars(args),
                "asset_storage_backend": asset_storage_backend,
                "metadata_backend": metadata_backend,
            }
        )
        metadata_stats = _write_entity_metadata_postgres(rows, metadata_args, run_dir)
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
        "rows_path": str(entity_rows_path.resolve()),
        "index_rows_path": str(index_rows_path.resolve()),
        "source_asset_count": len(source_assets),
        "asset_count": len(assets),
        "product_family": args.product_family,
        "data_type": data_type,
        "partition_type": "entity",
        "partition_method": "entity",
        "entity_clip_mode": entity_clip_mode,
        "grid_task_count": len(grid_tasks),
        "grid_type": grid_type,
        "grid_level": grid_level,
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
        "chunk_size": chunk_size,
        "task_group_count": len(grouped_tasks),
        "ingest_enabled": ingest_enabled,
        "asset_storage_backend": asset_storage_backend,
        "metadata_backend": metadata_backend,
        "uploaded_tile_count": uploaded_tile_count,
        "metadata_rows": metadata_rows,
        "dataset": str(getattr(args, "dataset", "optical_default")),
        "sensor": str(getattr(args, "sensor", "optical_mosaic")),
        "asset_version": str(getattr(args, "asset_version", getattr(args, "tile_version", "v1"))),
        "minio_bucket": str(getattr(args, "minio_bucket", "")) if asset_storage_backend == "minio" else "",
        "minio_prefix": str(getattr(args, "minio_prefix", "")) if asset_storage_backend == "minio" else "",
    }
    (run_dir / "job_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local entity partition job for raster tiles")
    minio = runtime_config.minio_settings()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--manifest-path", default="")
    parser.add_argument("--product-family", default="auto")
    parser.add_argument("--data-type", default="optical", choices=sorted(ENTITY_DATA_TYPES))
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--grid-type", default="isea4h", choices=["isea4h"])
    parser.add_argument("--grid-level", type=int, default=6)
    parser.add_argument("--entity-clip-mode", default="exact", choices=["bbox", "exact"])
    parser.add_argument("--cover-mode", default="intersect", choices=["intersect", "contain", "minimal"])
    parser.add_argument("--time-granularity", default="day", choices=["year", "month", "day", "hour", "minute"])
    parser.add_argument("--max-cells-per-asset", type=int, default=20000)
    parser.add_argument("--partition-prefix-len", type=int, default=3)
    parser.add_argument("--ray-parallelism", type=int, default=0)
    parser.add_argument("--ray-address", default=runtime_config.ray_address())
    parser.add_argument("--chunk-size", type=int, default=0)
    parser.add_argument("--partition-backend", default="ray", choices=["auto", "ray", "thread", "local", "process"])
    parser.add_argument("--job-id", default="")
    parser.add_argument("--dataset", default="optical_default")
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
    parser.add_argument("--minio-upload-workers", type=int, default=DEFAULT_ENTITY_MINIO_UPLOAD_WORKERS)
    parser.set_defaults(minio_fast_upload=True)
    parser.add_argument(
        "--minio-safe-upload",
        dest="minio_fast_upload",
        action="store_false",
        help="Check remote tile identity before upload",
    )
    return parser.parse_args()


def main() -> None:
    report = run_entity_partition(parse_args())
    print("=== Entity partition job completed ===")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
