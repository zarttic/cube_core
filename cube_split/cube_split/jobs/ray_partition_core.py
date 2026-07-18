#!/usr/bin/env python3
from __future__ import annotations

import errno
import hashlib
import json
import os
import shutil
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterator, Optional
from urllib.parse import unquote, urlparse
from uuid import uuid4

import rasterio
from grid_core.sdk import CubeEncoderSDK, GridAddress
from pyproj import Transformer
from rasterio.windows import Window

from cube_split.partition.optical_products import parse_optical_asset
from cube_split.partition.product_products import parse_product_asset
from cube_split.partition.radar_products import RADAR_ASSET_EXTENSIONS, parse_radar_asset
from cube_split.runtime_config import (
    bool_option,
    minio_service_env,
    minio_settings,
)

SUPPORTED_GRID_TYPES = frozenset({"geohash", "mgrs", "isea4h"})


@dataclass
class AssetRecord:
    scene_id: str
    band: str
    path: str
    acq_time: str
    product_family: str = "unknown"
    sensor: str = "unknown"
    bbox: list[float] | None = None
    corners: list[list[float]] | None = None
    resolution: float | None = None


def _is_s3_uri(value: str) -> bool:
    return str(value or "").strip().lower().startswith("s3://")


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme.lower() != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise ValueError(f"Invalid s3 URI: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def _bool_option(value: Any, default: bool = False) -> bool:
    return bool_option(value, default)


def _minio_service_env() -> dict[str, str]:
    return minio_service_env()


def _minio_options(options: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = minio_settings(options)
    return {
        "endpoint": cfg.endpoint,
        "access_key": cfg.access_key,
        "secret_key": cfg.secret_key,
        "secure": cfg.secure,
    }


def _minio_client(options: dict[str, Any] | None = None):
    try:
        from minio import Minio
    except ModuleNotFoundError as exc:
        raise RuntimeError("MinIO source assets require `minio` package") from exc

    cfg = _minio_options(options)
    if not cfg["endpoint"] or not cfg["access_key"] or not cfg["secret_key"]:
        raise ValueError("MinIO endpoint/access-key/secret-key are required")
    return Minio(
        endpoint=cfg["endpoint"],
        access_key=cfg["access_key"],
        secret_key=cfg["secret_key"],
        secure=bool(cfg["secure"]),
    )


def cache_source_cog(cog_uri: str, cache_dir: Path, minio_client: Any, bucket: str) -> Path:
    """Cache one loader-owned COG locally without altering its content."""
    parsed = urlparse(cog_uri)
    if parsed.scheme != "s3" or parsed.netloc != bucket or not parsed.path.lstrip("/"):
        raise ValueError(f"invalid source COG URI for bucket {bucket}: {cog_uri}")
    key = unquote(parsed.path).lstrip("/")
    target = cache_dir / hashlib.sha256(cog_uri.encode("utf-8")).hexdigest() / Path(key).name
    if target.exists():
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(f"{target.suffix}.part")
    try:
        minio_client.fget_object(bucket, key, str(temporary))
    except OSError as exc:
        if exc.errno != errno.ENOSPC:
            raise
        # Worker-local loader cache is disposable; reclaim a stale cache once.
        temporary.unlink(missing_ok=True)
        shutil.rmtree(cache_dir.parent, ignore_errors=True)
        target.parent.mkdir(parents=True, exist_ok=True)
        minio_client.fget_object(bucket, key, str(temporary))
    temporary.replace(target)
    return target


def _object_identity(stat: Any) -> str:
    etag = str(getattr(stat, "etag", "") or "").strip().strip('"')
    if etag:
        return f"etag:{etag}"
    last_modified = getattr(stat, "last_modified", None)
    if last_modified is not None:
        return f"mtime:{last_modified}"
    return f"size:{getattr(stat, 'size', '')}"


def _identity_sidecar_path(target: Path) -> Path:
    return target.with_name(f"{target.name}.identity")


def _read_identity_sidecar(target: Path) -> dict[str, str]:
    sidecar = _identity_sidecar_path(target)
    if not sidecar.exists():
        return {}
    text = sidecar.read_text(encoding="utf-8").strip()
    if not text:
        return {}
    if "=" not in text:
        return {"remote": text}
    values: dict[str, str] = {}
    for line in text.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key and value:
            values[key] = value
    return values


def _write_identity_sidecar(target: Path, **identity: str) -> None:
    lines = [f"{key}={value}" for key, value in sorted(identity.items()) if value]
    _identity_sidecar_path(target).write_text("\n".join(lines), encoding="utf-8")


def _local_file_identity(path: Path) -> str:
    stat = path.stat()
    return f"size:{stat.st_size}|mtime_ns:{stat.st_mtime_ns}"


def create_unique_run_dir(output_dir: Path, *, prefix: str = "run") -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    base = time.strftime(f"{prefix}_%Y%m%d_%H%M%S")
    for suffix in ("", *(f"_{idx:02d}" for idx in range(1, 100))):
        run_dir = output_dir / f"{base}{suffix}"
        try:
            run_dir.mkdir(parents=False, exist_ok=False)
            return run_dir
        except FileExistsError:
            continue
    fallback = output_dir / f"{base}_{uuid4().hex[:8]}"
    fallback.mkdir(parents=False, exist_ok=False)
    return fallback


def resolve_asset_source_path(source_uri: str, options: dict[str, Any] | None = None) -> str:
    text = str(source_uri or "").strip()
    if not text:
        raise ValueError("source_uri is required")
    if not _is_s3_uri(text):
        return text
    cache_root = Path(
        str(
            (options or {}).get("source_cache_dir")
            or os.environ.get("CUBE_SOURCE_CACHE_DIR")
            or "/tmp/cube_split_source_cache"
        )
    )
    bucket = str(minio_settings(options).bucket)
    if not bucket:
        raise ValueError("MinIO bucket is required for loader COG cache")
    return str(cache_source_cog(text, cache_root, _minio_client(options), bucket).resolve())


def _load_manifest_records(manifest_path: Path, default_data_type: str) -> list[AssetRecord]:
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest file not found: {manifest_path}")
    suffix = manifest_path.suffix.lower()
    if suffix == ".jsonl":
        rows = [json.loads(line) for line in manifest_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    elif suffix == ".json":
        loaded = json.loads(manifest_path.read_text(encoding="utf-8"))
        if isinstance(loaded, list):
            rows = loaded
        elif isinstance(loaded, dict):
            rows = loaded.get("assets")
            if not isinstance(rows, list):
                raise ValueError("manifest.json object must contain an `assets` array")
        else:
            raise ValueError("manifest.json must contain a JSON array or object")
    else:
        raise ValueError("manifest file must be .jsonl or .json")

    records: list[AssetRecord] = []
    base_dir = manifest_path.parent
    for idx, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise ValueError(f"Invalid manifest row #{idx}: expected object")
        source_uri = str(row.get("source_uri") or "").strip()
        scene_id = str(row.get("scene_id") or "").strip()
        acq_time = str(row.get("acq_time") or "").strip()
        sensor = str(row.get("sensor") or "").strip()
        product_family = str(row.get("product_family") or row.get("product_type") or "").strip()
        resolution = _manifest_resolution(row)
        bands_raw = row.get("bands")
        bands: list[str] = []
        if isinstance(bands_raw, list):
            bands = [str(item).strip().lower() for item in bands_raw if str(item).strip()]
        if not bands:
            fallback_band = str(row.get("band") or row.get("variable") or row.get("polarization") or "").strip().lower()
            if fallback_band:
                bands = [fallback_band]
        corners = row.get("corners")
        if not source_uri or not scene_id or not acq_time or not bands or not sensor or not product_family or resolution is None:
            raise ValueError(
                f"Invalid manifest row #{idx}: required fields are source_uri, scene_id, acq_time, sensor, product_family, resolution, and one of bands/band/variable/polarization"
            )
        if not isinstance(corners, list) or len(corners) != 4:
            raise ValueError(f"Invalid manifest row #{idx}: `corners` must be a list of 4 [lon, lat] points")
        parsed_corners: list[list[float]] = []
        for point in corners:
            if not isinstance(point, (list, tuple)) or len(point) != 2:
                raise ValueError(f"Invalid manifest row #{idx}: each corner must be [lon, lat]")
            lon = float(point[0])
            lat = float(point[1])
            if not (-180.0 <= lon <= 180.0 and -90.0 <= lat <= 90.0):
                raise ValueError(f"Invalid manifest row #{idx}: corner coordinate out of range")
            parsed_corners.append([lon, lat])
        lons = [p[0] for p in parsed_corners]
        lats = [p[1] for p in parsed_corners]
        bbox = [min(lons), min(lats), max(lons), max(lats)]
        datetime.fromisoformat(acq_time.replace("Z", "+00:00"))
        if _is_s3_uri(source_uri):
            path_text = source_uri
        else:
            path = Path(source_uri)
            if not path.is_absolute():
                path = (base_dir / path).resolve()
            path_text = str(path)
        data_type = str(row.get("data_type") or default_data_type).strip().lower()
        if data_type not in {"optical", "product", "radar"}:
            raise ValueError(f"Invalid manifest row #{idx}: unsupported data_type={data_type!r}")
        for band in bands:
            records.append(
                AssetRecord(
                    scene_id=scene_id,
                    band=band,
                    path=path_text,
                    acq_time=acq_time,
                    product_family=product_family.lower(),
                    sensor=sensor.lower(),
                    bbox=bbox,
                    corners=parsed_corners,
                    resolution=resolution,
                )
            )
    return records


def build_manifest(
    input_dir: Path,
    product_family: str = "auto",
    data_type: str = "optical",
    manifest_path: Path | None = None,
) -> list[AssetRecord]:
    if manifest_path is not None:
        return _load_manifest_records(manifest_path, default_data_type=data_type)
    records: list[AssetRecord] = []
    asset_suffixes = RADAR_ASSET_EXTENSIONS if data_type == "radar" else {".tif", ".tiff"}
    asset_paths = sorted(path for path in input_dir.rglob("*") if path.is_file() and path.suffix.lower() in asset_suffixes)
    for asset_path in asset_paths:
        if data_type == "product":
            metadata = parse_product_asset(asset_path)
        elif data_type == "radar":
            metadata = parse_radar_asset(asset_path)
        else:
            metadata = parse_optical_asset(asset_path, product_family=product_family)
        records.append(
            AssetRecord(
                scene_id=metadata.scene_id,
                band=metadata.band,
                path=str(asset_path.resolve()),
                acq_time=metadata.acq_time.isoformat().replace("+00:00", "Z"),
                product_family=metadata.product_family,
                sensor=metadata.sensor,
            )
        )
    return records


def _manifest_resolution(row: dict[str, Any]) -> float | None:
    value = row.get("resolution")
    if value is None or value == "":
        return None
    try:
        resolution = float(str(value).lower().replace("m", "").strip())
    except ValueError:
        return None
    if resolution > 0:
        return resolution
    return None


def _prepare_actor_source_groups(
    task_groups: list[list[dict]],
    *,
    cache_dir: Path,
    source_options: dict[str, Any],
) -> list[list[dict]]:
    bucket = str(source_options.get("bucket") or "")
    if not bucket:
        raise ValueError("MinIO bucket is required for loader COG cache")
    client = _minio_client(source_options)
    local_paths: dict[str, str] = {}
    prepared_groups: list[list[dict]] = []
    for group in task_groups:
        if not group:
            continue
        source_uri = str(group[0]["asset_path"])
        local_path = local_paths.get(source_uri)
        if local_path is None:
            local_path = str(cache_source_cog(source_uri, cache_dir, client, bucket))
            local_paths[source_uri] = local_path
        prepared_groups.append([{**row, "asset_path": local_path, "source_asset_path": source_uri} for row in group])
    return prepared_groups


def _dataset_bounds_wgs84(ds: rasterio.DatasetReader) -> tuple[float, float, float, float]:
    b = ds.bounds
    if ds.crs and str(ds.crs).upper() != "EPSG:4326":
        transformer = Transformer.from_crs(ds.crs, "EPSG:4326", always_xy=True)
        xs = [b.left, b.left, b.right, b.right]
        ys = [b.bottom, b.top, b.bottom, b.top]
        lons, lats = transformer.transform(xs, ys)
        return min(lons), min(lats), max(lons), max(lats)
    return b.left, b.bottom, b.right, b.top


def _bbox_intersection(
    a: list[float] | tuple[float, float, float, float],
    b: list[float] | tuple[float, float, float, float],
) -> tuple[float, float, float, float] | None:
    min_lon = max(float(a[0]), float(b[0]))
    min_lat = max(float(a[1]), float(b[1]))
    max_lon = min(float(a[2]), float(b[2]))
    max_lat = min(float(a[3]), float(b[3]))
    if min_lon >= max_lon or min_lat >= max_lat:
        return None
    return min_lon, min_lat, max_lon, max_lat


def _group_tasks_for_local_processing(
    tasks: list[dict],
    split_by_space_prefix: bool = False,
) -> list[list[dict]]:
    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for task in tasks:
        asset_path = str(task["asset_path"])
        prefix = str(task.get("space_code_prefix") or "") if split_by_space_prefix else ""
        grouped[(asset_path, prefix)].append(task)
    return [
        sorted(rows, key=lambda row: row["space_code"])
        for _, rows in sorted(grouped.items(), key=lambda item: item[0])
    ]


def _prepare_task_rows_for_partitioning(
    tasks: list[dict],
    partition_prefix_len: int,
    time_granularity: str,
) -> list[dict]:
    prefix_len = max(1, int(partition_prefix_len))
    time_format = {
        "year": "%Y",
        "month": "%Y%m",
        "day": "%Y%m%d",
        "hour": "%Y%m%d%H",
        "minute": "%Y%m%d%H%M",
        "second": "%Y%m%d%H%M%S",
    }[time_granularity]

    task_rows: list[dict] = []
    for task in tasks:
        row = dict(task)
        row["space_code_prefix"] = row["space_code"][:prefix_len]
        row["time_bucket"] = datetime.fromisoformat(row["acq_time"].replace("Z", "+00:00")).strftime(time_format)
        task_rows.append(row)
    return task_rows


def _st_time_granularity(time_granularity: str) -> str:
    # Product rows retain annual business buckets, while the frozen SDK has no
    # yearly ST code. Encode the acquisition day in the ST address instead.
    return "day" if time_granularity == "year" else time_granularity


def _cover_codes(
    sdk: CubeEncoderSDK,
    grid_type: str,
    grid_level: int,
    cover_mode: str,
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
    bbox_cache: dict[str, list[float]],
) -> list[tuple[GridAddress, list[float]]]:
    cells = sdk.cover_compact(
        grid_type=grid_type,
        requested_grid_level=grid_level,
        cover_mode=cover_mode,
        bbox=[min_lon, min_lat, max_lon, max_lat],
        crs="EPSG:4326",
    )
    covered: list[tuple[GridAddress, list[float]]] = []
    for cell in cells:
        cache_key = cell.topology_code or f"{cell.grid_type}:{cell.grid_level}:{cell.space_code}"
        bbox = bbox_cache.get(cache_key)
        if bbox is None:
            bbox = sdk.code_to_bbox(address=cell)
            bbox_cache[cache_key] = bbox
        covered.append((cell, bbox))
    return covered


def build_grid_tasks_driver(
    assets: list[AssetRecord],
    grid_type: str,
    grid_level: int,
    cover_mode: str,
    max_cells_per_asset: int,
) -> list[dict]:
    grid_type = str(grid_type or "").strip().lower()
    if grid_type not in SUPPORTED_GRID_TYPES:
        raise ValueError(f"Unsupported production grid_type: {grid_type}")

    sdk = CubeEncoderSDK()
    bbox_cache: dict[str, list[float]] = {}
    geometry_cache: dict[str, dict[str, object]] = {}
    scene_cover_cache: dict[tuple[str, float, float, float, float], list[tuple[GridAddress, list[float]]]] = {}
    tasks: list[dict] = []

    for asset in assets:
        if asset.bbox is not None:
            min_lon, min_lat, max_lon, max_lat = map(float, asset.bbox)
        else:
            with rasterio.open(resolve_asset_source_path(asset.path)) as ds:
                min_lon, min_lat, max_lon, max_lat = _dataset_bounds_wgs84(ds)

        scene_cover_key = (asset.scene_id, float(min_lon), float(min_lat), float(max_lon), float(max_lat))
        cells = scene_cover_cache.get(scene_cover_key)
        if cells is None:
            cells = _cover_codes(
                sdk=sdk,
                grid_type=grid_type,
                grid_level=grid_level,
                cover_mode=cover_mode,
                min_lon=min_lon,
                min_lat=min_lat,
                max_lon=max_lon,
                max_lat=max_lat,
                bbox_cache=bbox_cache,
            )
            scene_cover_cache[scene_cover_key] = cells

        if max_cells_per_asset > 0 and len(cells) > max_cells_per_asset:
            raise RuntimeError(
                "Cover cells exceed max limit for asset %s: %d > %d"
                % (asset.path, len(cells), max_cells_per_asset)
            )

        for cell, cb in cells:
            cache_key = cell.topology_code or f"{cell.grid_type}:{cell.grid_level}:{cell.space_code}"
            cell_geom = geometry_cache.get(cache_key)
            if cell_geom is None:
                cell_geom = sdk.code_to_geometry(address=cell)
                geometry_cache[cache_key] = cell_geom
            tasks.append(
                {
                    "scene_id": asset.scene_id,
                    "band": asset.band,
                    "asset_path": asset.path,
                    "acq_time": asset.acq_time,
                    "grid_type": grid_type,
                    "grid_level": int(cell.grid_level),
                    "space_code": cell.space_code,
                    "topology_code": cell.topology_code,
                    "cell_min_lon": float(cb[0]),
                    "cell_min_lat": float(cb[1]),
                    "cell_max_lon": float(cb[2]),
                    "cell_max_lat": float(cb[3]),
                    "cell_geom": cell_geom,
                    "cover_mode": cover_mode,
                    "resolution": asset.resolution,
                }
            )
    return tasks


def _wgs84_to_dataset_bounds(
    ds: rasterio.DatasetReader,
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
    transformer: Transformer | None = None,
) -> tuple[float, float, float, float]:
    if transformer is not None:
        xs = [min_lon, min_lon, max_lon, max_lon]
        ys = [min_lat, max_lat, min_lat, max_lat]
        px, py = transformer.transform(xs, ys)
        return min(px), min(py), max(px), max(py)
    if ds.crs and str(ds.crs).upper() != "EPSG:4326":
        return _wgs84_to_dataset_bounds(
            ds,
            min_lon,
            min_lat,
            max_lon,
            max_lat,
            transformer=Transformer.from_crs("EPSG:4326", ds.crs, always_xy=True),
        )
    return min_lon, min_lat, max_lon, max_lat


def _safe_window_from_bounds(ds: rasterio.DatasetReader, left: float, bottom: float, right: float, top: float) -> Optional[Window]:
    if not (left < right and bottom < top):
        return None

    win = rasterio.windows.from_bounds(left=left, bottom=bottom, right=right, top=top, transform=ds.transform)
    rounded = win.round_offsets().round_lengths()

    col_off = max(0, int(rounded.col_off))
    row_off = max(0, int(rounded.row_off))
    width = int(min(ds.width - col_off, max(0, int(rounded.width))))
    height = int(min(ds.height - row_off, max(0, int(rounded.height))))

    if width <= 0 or height <= 0:
        return None
    return Window(col_off=col_off, row_off=row_off, width=width, height=height)


def process_partition(rows: Iterator[Any], time_granularity: str, include_sample_mean: bool = True) -> Iterator[dict]:
    sdk = CubeEncoderSDK()
    st_cache: dict[str, str] = {}

    open_path: str | None = None
    open_ds: rasterio.DatasetReader | None = None
    ds_bounds_wgs84: tuple[float, float, float, float] | None = None
    ds_wgs84_to_native: Transformer | None = None

    try:
        for row in rows:
            if open_path != row.asset_path:
                if open_ds is not None:
                    open_ds.close()
                local_asset_path = resolve_asset_source_path(row.asset_path)
                open_ds = rasterio.open(local_asset_path)
                open_path = row.asset_path
                ds_bounds_wgs84 = None
                ds_wgs84_to_native = None

            assert open_ds is not None

            acq_dt = datetime.fromisoformat(row.acq_time.replace("Z", "+00:00"))
            st_key = "%s|%d|%s|%s|%s|%s" % (
                row.grid_type,
                row.grid_level,
                row.space_code,
                getattr(row, "topology_code", None),
                row.acq_time,
                time_granularity,
            )
            if st_key in st_cache:
                st_code = st_cache[st_key]
            else:
                address = GridAddress(
                    grid_type=row.grid_type,
                    grid_level=int(row.grid_level),
                    space_code=row.space_code,
                    topology_code=getattr(row, "topology_code", None),
                )
                st_code = sdk.generate_st_code(
                    address=address,
                    timestamp=acq_dt,
                    time_granularity=_st_time_granularity(time_granularity),
                ).st_code
                st_cache[st_key] = st_code

            if ds_bounds_wgs84 is None:
                ds_bounds_wgs84 = _dataset_bounds_wgs84(open_ds)
                ds_wgs84_to_native = (
                    Transformer.from_crs("EPSG:4326", open_ds.crs, always_xy=True)
                    if open_ds.crs and str(open_ds.crs).upper() != "EPSG:4326"
                    else None
                )

            inter = _bbox_intersection(
                ds_bounds_wgs84,
                (row.cell_min_lon, row.cell_min_lat, row.cell_max_lon, row.cell_max_lat),
            )
            if inter is None:
                continue
            min_lon, min_lat, max_lon, max_lat = inter

            left, bottom, right, top = _wgs84_to_dataset_bounds(
                open_ds,
                min_lon,
                min_lat,
                max_lon,
                max_lat,
                transformer=ds_wgs84_to_native,
            )
            win = _safe_window_from_bounds(open_ds, left, bottom, right, top)
            if win is None:
                continue

            sample_mean = None
            if include_sample_mean:
                band1 = open_ds.read(1, window=win, masked=True)
                if band1.size > 0 and band1.count() > 0:
                    sample_mean = float(band1.mean())

            yield {
                "scene_id": row.scene_id,
                "band": row.band,
                "asset_path": row.asset_path,
                "acq_time": row.acq_time,
                "grid_type": row.grid_type,
                "grid_level": int(row.grid_level),
                "space_code": row.space_code,
                "topology_code": getattr(row, "topology_code", None),
                "space_code_prefix": row.space_code_prefix,
                "st_code": st_code,
                "time_bucket": row.time_bucket,
                "cover_mode": row.cover_mode,
                "cell_min_lon": float(row.cell_min_lon),
                "cell_min_lat": float(row.cell_min_lat),
                "cell_max_lon": float(row.cell_max_lon),
                "cell_max_lat": float(row.cell_max_lat),
                "cell_geom": getattr(row, "cell_geom", None),
                "window_col_off": int(win.col_off),
                "window_row_off": int(win.row_off),
                "window_width": int(win.width),
                "window_height": int(win.height),
                "intersect_min_lon": float(min_lon),
                "intersect_min_lat": float(min_lat),
                "intersect_max_lon": float(max_lon),
                "intersect_max_lat": float(max_lat),
                "sample_mean_band1": sample_mean,
            }
    finally:
        if open_ds is not None:
            open_ds.close()


def _process_local_task_group(rows: list[dict], time_granularity: str, include_sample_mean: bool = True) -> list[dict]:
    if not rows:
        return []
    source_paths = {
        str(row["asset_path"]): str(row["source_asset_path"])
        for row in rows
        if row.get("source_asset_path")
    }
    task_rows = [SimpleNamespace(**row) for row in rows]
    results = list(process_partition(iter(task_rows), time_granularity, include_sample_mean=include_sample_mean))
    for result in results:
        source_path = source_paths.get(str(result.get("asset_path") or ""))
        if source_path:
            result["asset_path"] = source_path
            result["source_asset_path"] = source_path
    return results
