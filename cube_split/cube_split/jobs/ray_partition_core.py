#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
from uuid import uuid4
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterator, Optional
from urllib.parse import urlparse

import rasterio
from grid_core.sdk import CubeEncoderSDK
from pyproj import Transformer
from rasterio.enums import Resampling
from rasterio.shutil import copy as rio_copy
from rasterio.vrt import WarpedVRT
from rasterio.windows import Window

from cube_split.partition.optical_products import parse_optical_asset
from cube_split.partition.product_products import parse_product_asset
from cube_split.partition.radar_products import RADAR_ASSET_EXTENSIONS, parse_radar_asset
from cube_split.runtime_config import (
    bool_option,
    minio_service_env,
    minio_settings,
)

PLANE_GRID_TYPE = "plane_grid"
PLANE_GRID_BASE_CHUNK_LEVEL = 13


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


def _cache_path_for_uri(uri: str, cache_root: Path, suffix: str | None = None) -> Path:
    _, key = _parse_s3_uri(uri)
    digest = hashlib.sha1(uri.encode("utf-8")).hexdigest()[:16]
    name = Path(key).name
    if suffix is not None:
        name = f"{Path(name).stem}{suffix}"
    return cache_root / digest / name


def _fallback_cache_root(cache_root: Path) -> Path:
    uid = getattr(os, "getuid", lambda: 0)()
    return Path(tempfile.gettempdir()) / f"{cache_root.name}_u{uid}"


def _cache_target_for_uri(uri: str, cache_root: Path) -> Path:
    target = _cache_path_for_uri(uri, cache_root)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        fallback = _cache_path_for_uri(uri, _fallback_cache_root(cache_root))
        fallback.parent.mkdir(parents=True, exist_ok=True)
        return fallback
    if os.access(target.parent, os.W_OK | os.X_OK):
        return target
    fallback = _cache_path_for_uri(uri, _fallback_cache_root(cache_root))
    fallback.parent.mkdir(parents=True, exist_ok=True)
    return fallback


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


def _download_s3_object(uri: str, cache_root: Path, options: dict[str, Any] | None = None) -> Path:
    from minio.error import S3Error

    bucket, key = _parse_s3_uri(uri)
    client = _minio_client(options)
    target = _cache_target_for_uri(uri, cache_root)
    stat = client.stat_object(bucket, key)
    identity = _object_identity(stat)
    identity_state = _read_identity_sidecar(target)
    if not target.exists() or target.stat().st_size != stat.size or identity_state.get("remote") != identity:
        handle = tempfile.NamedTemporaryFile(prefix=f".{target.name}.", suffix=".part", dir=target.parent, delete=False)
        tmp = Path(handle.name)
        handle.close()
        try:
            client.fget_object(bucket, key, str(tmp))
            tmp.replace(target)
            _write_identity_sidecar(target, remote=identity)
        finally:
            if tmp.exists():
                tmp.unlink()

    sidecar_keys = [f"{key}.aux.xml", f"{key}.ovr"]
    if key.lower().endswith((".tif", ".tiff")):
        sidecar_keys.append(f"{key.rsplit('.', 1)[0]}.tfw")
    if key.lower().endswith(".dat"):
        sidecar_keys.append(f"{key.rsplit('.', 1)[0]}.hdr")
    for sidecar_key in sidecar_keys:
        sidecar_target = target.parent / Path(sidecar_key).name
        try:
            sidecar_stat = client.stat_object(bucket, sidecar_key)
        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchObject"}:
                continue
            raise
        sidecar_identity = _object_identity(sidecar_stat)
        sidecar_identity_state = _read_identity_sidecar(sidecar_target)
        if (
            not sidecar_target.exists()
            or sidecar_target.stat().st_size != sidecar_stat.size
            or sidecar_identity_state.get("remote") != sidecar_identity
        ):
            handle = tempfile.NamedTemporaryFile(
                prefix=f".{sidecar_target.name}.",
                suffix=".part",
                dir=sidecar_target.parent,
                delete=False,
            )
            tmp = Path(handle.name)
            handle.close()
            try:
                client.fget_object(bucket, sidecar_key, str(tmp))
                tmp.replace(sidecar_target)
                _write_identity_sidecar(sidecar_target, remote=sidecar_identity)
            finally:
                if tmp.exists():
                    tmp.unlink()
    return target


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
    return str(_download_s3_object(text, cache_root, options).resolve())


def _upload_file_to_minio(path: Path, key: str, options: dict[str, Any]) -> str:
    from minio.error import S3Error

    bucket = minio_settings(options).bucket
    if not bucket:
        raise ValueError("MinIO bucket is required")
    client = _minio_client(options)
    local_identity = _local_file_identity(path)
    identity_state = _read_identity_sidecar(path)
    try:
        stat = client.stat_object(bucket, key)
        remote_identity = _object_identity(stat)
        if (
            stat.size == path.stat().st_size
            and identity_state.get("local") == local_identity
            and identity_state.get("remote") == remote_identity
        ):
            return f"s3://{bucket}/{key}"
    except S3Error as exc:
        if exc.code not in {"NoSuchKey", "NoSuchObject"}:
            raise
    client.fput_object(bucket, key, str(path))
    stat = client.stat_object(bucket, key)
    _write_identity_sidecar(path, local=local_identity, remote=_object_identity(stat))
    return f"s3://{bucket}/{key}"


def upload_cog_to_minio(asset: AssetRecord, cog_path: Path, options: dict[str, Any] | None = None) -> str:
    opts = dict(options or {})
    prefix = str(opts.get("prefix") or opts.get("minio_prefix") or "cube/cog").strip("/")
    dataset = str(opts.get("dataset") or "demo")
    sensor = str(opts.get("sensor") or asset.sensor or "unknown")
    version = str(opts.get("asset_version") or opts.get("version") or "v1")
    date_path = datetime.fromisoformat(asset.acq_time.replace("Z", "+00:00")).strftime("%Y/%m/%d")
    key = (
        f"{prefix}/dataset={dataset}/sensor={sensor}/acq_date={date_path}/"
        f"scene_id={asset.scene_id}/version={version}/{cog_path.name}"
    )
    return _upload_file_to_minio(cog_path, key, opts)


def upload_source_assets_to_minio(
    assets: list[AssetRecord],
    *,
    prefix: str,
    options: dict[str, Any] | None = None,
) -> list[AssetRecord]:
    from minio.error import S3Error

    if not assets:
        return []

    opts = dict(options or {})
    source_prefix = str(prefix or opts.get("prefix") or opts.get("minio_prefix") or "cube/source").strip("/")
    dataset = str(opts.get("dataset") or "demo")
    sensor = str(opts.get("sensor") or "unknown")
    version = str(opts.get("asset_version") or opts.get("version") or "v1")
    bucket = str(minio_settings(opts).bucket)
    if not bucket:
        raise ValueError("MinIO bucket is required")
    client = _minio_client(opts)
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    uploaded: list[AssetRecord] = []
    for asset in assets:
        if _is_s3_uri(asset.path):
            uploaded.append(asset)
            continue
        source_path = Path(asset.path)
        if not source_path.exists():
            raise FileNotFoundError(f"Asset file not found: {source_path}")
        local_identity = _local_file_identity(source_path)
        identity_state = _read_identity_sidecar(source_path)
        date_path = datetime.fromisoformat(asset.acq_time.replace("Z", "+00:00")).strftime("%Y/%m/%d")
        key = (
            f"{source_prefix}/dataset={dataset}/sensor={sensor}/acq_date={date_path}/"
            f"scene_id={asset.scene_id}/version={version}/{source_path.name}"
        )
        try:
            stat = client.stat_object(bucket, key)
            remote_identity = _object_identity(stat)
            if (
                stat.size == source_path.stat().st_size
                and identity_state.get("local") == local_identity
                and identity_state.get("remote") == remote_identity
            ):
                uploaded.append(replace(asset, path=f"s3://{bucket}/{key}"))
                continue
        except S3Error as exc:
            if exc.code not in {"NoSuchKey", "NoSuchObject"}:
                raise
        client.fput_object(bucket, key, str(source_path))
        stat = client.stat_object(bucket, key)
        _write_identity_sidecar(source_path, local=local_identity, remote=_object_identity(stat))
        uploaded.append(replace(asset, path=f"s3://{bucket}/{key}"))
    return uploaded


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
    source_uploader: Any | None = None,
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
    if source_uploader is not None:
        records = list(source_uploader(records))
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


def convert_assets_to_cog(
    assets: list[AssetRecord],
    cog_input_dir: Path,
    overwrite: bool = False,
    workers: int = 0,
    compress: str = "LZW",
    predictor: int = 2,
    level: int | None = None,
    overviews: str = "NONE",
    num_threads: str = "ALL_CPUS",
    target_crs: str | None = None,
    source_options: dict[str, Any] | None = None,
    cancellation_check: Any | None = None,
) -> list[AssetRecord]:
    if not assets:
        return []

    cog_input_dir.mkdir(parents=True, exist_ok=True)
    if workers < 0:
        raise ValueError("workers must be >= 0")
    worker_count = workers or min(len(assets), (os.cpu_count() or 1))
    worker_count = max(1, worker_count)

    creation_options = cog_creation_options(
        compress=compress,
        predictor=predictor,
        level=level,
        overviews=overviews,
        num_threads=num_threads,
    )

    def convert_one(asset: AssetRecord) -> AssetRecord:
        if cancellation_check is not None and cancellation_check():
            from cube_split.jobs.cancellation import PartitionCancelledError

            raise PartitionCancelledError("Partition task cancelled")
        return convert_asset_to_cog(
            asset,
            cog_input_dir=cog_input_dir,
            overwrite=overwrite,
            creation_options=creation_options,
            target_crs=target_crs,
            source_options=source_options,
        )

    if worker_count == 1:
        converted = []
        for asset in assets:
            if cancellation_check is not None and cancellation_check():
                from cube_split.jobs.cancellation import PartitionCancelledError

                raise PartitionCancelledError("Partition task cancelled")
            converted.append(convert_one(asset))
        return converted

    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        converted = []
        for item in pool.map(convert_one, assets):
            if cancellation_check is not None and cancellation_check():
                from cube_split.jobs.cancellation import PartitionCancelledError

                raise PartitionCancelledError("Partition task cancelled")
            converted.append(item)
        return converted


def cog_creation_options(
    compress: str = "LZW",
    predictor: int = 2,
    level: int | None = None,
    overviews: str = "NONE",
    num_threads: str = "ALL_CPUS",
) -> dict[str, str]:
    compress_value = str(compress or "NONE").upper()
    options: dict[str, str] = {
        "COMPRESS": compress_value,
        "OVERVIEWS": overviews,
    }
    if compress_value != "NONE" and predictor > 0:
        options["PREDICTOR"] = str(predictor)
    if compress_value != "NONE" and level is not None and level > 0:
        options["LEVEL"] = str(level)
    if num_threads:
        options["NUM_THREADS"] = str(num_threads)
    return options


def asset_record_to_dict(asset: AssetRecord) -> dict[str, Any]:
    return {
        "scene_id": asset.scene_id,
        "band": asset.band,
        "path": asset.path,
        "acq_time": asset.acq_time,
        "product_family": asset.product_family,
        "sensor": asset.sensor,
        "bbox": asset.bbox,
        "corners": asset.corners,
        "resolution": asset.resolution,
    }


def asset_record_from_dict(row: dict[str, Any]) -> AssetRecord:
    return AssetRecord(
        scene_id=str(row["scene_id"]),
        band=str(row["band"]),
        path=str(row["path"]),
        acq_time=str(row["acq_time"]),
        product_family=str(row.get("product_family") or "unknown"),
        sensor=str(row.get("sensor") or "unknown"),
        bbox=row.get("bbox"),
        corners=row.get("corners"),
        resolution=(float(row["resolution"]) if row.get("resolution") is not None else None),
    )


def convert_asset_to_cog(
    asset: AssetRecord,
    cog_input_dir: Path,
    overwrite: bool,
    creation_options: dict[str, str],
    target_crs: str | None = None,
    source_options: dict[str, Any] | None = None,
    timing: dict[str, float] | None = None,
) -> AssetRecord:
    cog_input_dir.mkdir(parents=True, exist_ok=True)
    resolve_start = time.perf_counter()
    local_source = Path(resolve_asset_source_path(asset.path, source_options))
    if timing is not None:
        timing["source_resolve_elapsed_sec"] = timing.get("source_resolve_elapsed_sec", 0.0) + (
            time.perf_counter() - resolve_start
        )
    if _is_s3_uri(asset.path):
        source_stem = Path(_parse_s3_uri(asset.path)[1]).stem
        digest = hashlib.sha1(asset.path.encode("utf-8")).hexdigest()[:10]
        dst = cog_input_dir / f"{source_stem}_{digest}_cog.tif"
    else:
        dst = cog_input_dir / f"{local_source.stem}_cog.tif"
    if overwrite and dst.exists():
        dst.unlink()
    if not dst.exists():
        write_start = time.perf_counter()
        with rasterio.open(local_source) as ds:
            if target_crs and (ds.crs is None or ds.crs.to_string().upper() != target_crs.upper()):
                if ds.crs is None:
                    raise ValueError(f"Cannot reproject asset without CRS: {asset.path}")
                with WarpedVRT(ds, crs=target_crs, resampling=Resampling.nearest) as vrt:
                    rio_copy(vrt, str(dst), driver="COG", **creation_options)
            else:
                rio_copy(ds, str(dst), driver="COG", **creation_options)
        if timing is not None:
            timing["cog_write_elapsed_sec"] = timing.get("cog_write_elapsed_sec", 0.0) + (
                time.perf_counter() - write_start
            )
            timing["cog_write_count"] = timing.get("cog_write_count", 0.0) + 1.0
    elif timing is not None:
        timing["cog_cache_hit_count"] = timing.get("cog_cache_hit_count", 0.0) + 1.0
    return AssetRecord(
        scene_id=asset.scene_id,
        band=asset.band,
        path=str(dst.resolve()),
        acq_time=asset.acq_time,
        product_family=asset.product_family,
        sensor=asset.sensor,
        bbox=asset.bbox,
        corners=asset.corners,
        resolution=asset.resolution,
    )


def _prepare_actor_cog_groups(
    task_groups: list[list[dict]],
    *,
    assets_by_path: dict[str, dict],
    local_cog_by_source: dict[str, str],
    converted_asset_by_source: dict[str, dict[str, Any]],
    cog_input_dir: Path,
    cog_overwrite: bool,
    cog_options: dict[str, str],
    target_crs: str,
    source_options: dict[str, Any],
    timing: dict[str, float] | None = None,
) -> tuple[list[list[dict]], set[str]]:
    prepared_groups: list[list[dict]] = []
    used_local_cog_paths: set[str] = set()
    for group in task_groups:
        if not group:
            continue
        source_path = str(group[0]["asset_path"])
        local_cog_path = local_cog_by_source.get(source_path)
        if local_cog_path is None:
            asset = asset_record_from_dict(assets_by_path[source_path])
            converted = convert_asset_to_cog(
                asset,
                cog_input_dir=cog_input_dir,
                overwrite=cog_overwrite,
                creation_options=cog_options,
                target_crs=target_crs or None,
                source_options=source_options,
                timing=timing,
            )
            local_cog_path = str(converted.path)
            local_cog_by_source[source_path] = local_cog_path
            converted_asset_by_source[source_path] = asset_record_to_dict(converted)
        elif timing is not None:
            timing["cog_cache_hit_count"] = timing.get("cog_cache_hit_count", 0.0) + 1.0
        used_local_cog_paths.add(local_cog_path)
        prepared_groups.append([{**row, "asset_path": local_cog_path} for row in group])
    return prepared_groups, used_local_cog_paths


def _upload_actor_cogs(
    *,
    local_cog_by_source: dict[str, str],
    converted_asset_by_source: dict[str, dict[str, Any]],
    remote_cog_by_local_path: dict[str, str],
    used_local_cog_paths: set[str],
    cog_upload_options: dict[str, Any],
    timing: dict[str, float] | None = None,
) -> None:
    for source_path, local_cog_path in local_cog_by_source.items():
        if local_cog_path not in used_local_cog_paths or local_cog_path in remote_cog_by_local_path:
            continue
        converted = asset_record_from_dict(converted_asset_by_source[source_path])
        upload_start = time.perf_counter()
        remote_cog_by_local_path[local_cog_path] = upload_cog_to_minio(
            converted,
            Path(local_cog_path),
            cog_upload_options,
        )
        if timing is not None:
            timing["cog_upload_elapsed_sec"] = timing.get("cog_upload_elapsed_sec", 0.0) + (
                time.perf_counter() - upload_start
            )
            timing["cog_upload_count"] = timing.get("cog_upload_count", 0.0) + 1.0


def _dataset_bounds_wgs84(ds: rasterio.DatasetReader) -> tuple[float, float, float, float]:
    b = ds.bounds
    if ds.crs and str(ds.crs).upper() != "EPSG:4326":
        transformer = Transformer.from_crs(ds.crs, "EPSG:4326", always_xy=True)
        xs = [b.left, b.left, b.right, b.right]
        ys = [b.bottom, b.top, b.bottom, b.top]
        lons, lats = transformer.transform(xs, ys)
        return min(lons), min(lats), max(lons), max(lats)
    return b.left, b.bottom, b.right, b.top


def _bbox_intersects(a: list[float] | tuple[float, float, float, float], b: list[float] | tuple[float, float, float, float]) -> bool:
    return float(a[0]) < float(b[2]) and float(a[2]) > float(b[0]) and float(a[1]) < float(b[3]) and float(a[3]) > float(b[1])


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
    }[time_granularity]

    task_rows: list[dict] = []
    for task in tasks:
        row = dict(task)
        row["space_code_prefix"] = row["space_code"][:prefix_len]
        row["time_bucket"] = datetime.fromisoformat(row["acq_time"].replace("Z", "+00:00")).strftime(time_format)
        task_rows.append(row)
    return task_rows


def _is_plane_grid(grid_type: str) -> bool:
    return str(grid_type or "").strip().lower() == PLANE_GRID_TYPE


def _plane_grid_chunk_pixels(grid_level: int) -> int:
    level = int(grid_level)
    if level < 1:
        raise ValueError("plane_grid level must be >= 1")
    return 2 ** max(0, PLANE_GRID_BASE_CHUNK_LEVEL - level)


def _plane_grid_crs_token(ds: rasterio.DatasetReader) -> str:
    if ds.crs is None:
        raise ValueError("plane_grid requires source assets with a CRS")
    authority = ds.crs.to_authority()
    if authority and authority[0] and authority[1]:
        return f"{authority[0].lower()}{authority[1]}"
    crs_text = ds.crs.to_string()
    digest = hashlib.sha1(crs_text.encode("utf-8")).hexdigest()[:12]
    return f"crs{digest}"


def _safe_window_from_offsets(
    ds: rasterio.DatasetReader,
    col_off: int,
    row_off: int,
    width: int,
    height: int,
) -> Optional[Window]:
    col_off = max(0, int(col_off))
    row_off = max(0, int(row_off))
    width = int(min(ds.width - col_off, max(0, int(width))))
    height = int(min(ds.height - row_off, max(0, int(height))))
    if width <= 0 or height <= 0:
        return None
    return Window(col_off=col_off, row_off=row_off, width=width, height=height)


def _plane_grid_cells(ds: rasterio.DatasetReader, grid_level: int) -> list[dict[str, Any]]:
    chunk_pixels = _plane_grid_chunk_pixels(grid_level)
    crs_token = _plane_grid_crs_token(ds)
    crs_text = ds.crs.to_string() if ds.crs is not None else ""
    cells: list[dict[str, Any]] = []

    for row_index, row_off in enumerate(range(0, ds.height, chunk_pixels)):
        height = min(chunk_pixels, ds.height - row_off)
        for col_index, col_off in enumerate(range(0, ds.width, chunk_pixels)):
            width = min(chunk_pixels, ds.width - col_off)
            win = Window(col_off=col_off, row_off=row_off, width=width, height=height)
            left, bottom, right, top = rasterio.windows.bounds(win, ds.transform)
            min_x, max_x = sorted((float(left), float(right)))
            min_y, max_y = sorted((float(bottom), float(top)))
            space_code = f"{crs_token}/{int(grid_level)}/{col_index}/{row_index}"
            cells.append(
                {
                    "grid_level": int(grid_level),
                    "space_code": space_code,
                    "cell_crs": crs_text,
                    "cell_crs_token": crs_token,
                    "plane_grid_chunk_pixels": int(chunk_pixels),
                    "plane_grid_col": int(col_index),
                    "plane_grid_row": int(row_index),
                    "cell_min_x": min_x,
                    "cell_min_y": min_y,
                    "cell_max_x": max_x,
                    "cell_max_y": max_y,
                    "cell_min_lon": min_x,
                    "cell_min_lat": min_y,
                    "cell_max_lon": max_x,
                    "cell_max_lat": max_y,
                    "window_col_off": int(col_off),
                    "window_row_off": int(row_off),
                    "window_width": int(width),
                    "window_height": int(height),
                }
            )
    return cells


def _build_plane_grid_tasks_for_asset(asset: AssetRecord, grid_level: int, cover_mode: str) -> list[dict]:
    with rasterio.open(resolve_asset_source_path(asset.path)) as ds:
        cells = _plane_grid_cells(ds, grid_level)

    return [
        {
            "scene_id": asset.scene_id,
            "band": asset.band,
            "asset_path": asset.path,
            "acq_time": asset.acq_time,
            "grid_type": PLANE_GRID_TYPE,
            "grid_level": int(cell["grid_level"]),
            "space_code": cell["space_code"],
            "cell_min_lon": cell["cell_min_lon"],
            "cell_min_lat": cell["cell_min_lat"],
            "cell_max_lon": cell["cell_max_lon"],
            "cell_max_lat": cell["cell_max_lat"],
            "cell_min_x": cell["cell_min_x"],
            "cell_min_y": cell["cell_min_y"],
            "cell_max_x": cell["cell_max_x"],
            "cell_max_y": cell["cell_max_y"],
            "cell_crs": cell["cell_crs"],
            "cell_crs_token": cell["cell_crs_token"],
            "plane_grid_chunk_pixels": cell["plane_grid_chunk_pixels"],
            "plane_grid_col": cell["plane_grid_col"],
            "plane_grid_row": cell["plane_grid_row"],
            "window_col_off": cell["window_col_off"],
            "window_row_off": cell["window_row_off"],
            "window_width": cell["window_width"],
            "window_height": cell["window_height"],
            "cover_mode": cover_mode,
            "resolution": asset.resolution,
        }
        for cell in cells
    ]


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
) -> list[tuple[str, int, list[float]]]:
    def resolve_bbox(code: str) -> list[float]:
        if code not in bbox_cache:
            bbox_cache[code] = sdk.code_to_bbox(grid_type=grid_type, code=code)
        return bbox_cache[code]

    if grid_type == "s2" and grid_level >= 7:
        coarse_level = max(1, grid_level - 2)
        coarse_cells = sdk.cover_compact(
            grid_type=grid_type,
            level=coarse_level,
            cover_mode=cover_mode,
            bbox=[min_lon, min_lat, max_lon, max_lat],
            crs="EPSG:4326",
        )
        target_bbox = [min_lon, min_lat, max_lon, max_lat]
        seen: set[str] = set()
        refined: list[tuple[str, int, list[float]]] = []
        for coarse in coarse_cells:
            child_codes = sdk.children(grid_type=grid_type, code=coarse.space_code, target_level=grid_level)
            for code in child_codes:
                if code in seen:
                    continue
                cb = resolve_bbox(code)
                if not _bbox_intersects(cb, target_bbox):
                    continue
                seen.add(code)
                refined.append((code, grid_level, cb))
        return refined

    cells = sdk.cover_compact(
        grid_type=grid_type,
        level=grid_level,
        cover_mode=cover_mode,
        bbox=[min_lon, min_lat, max_lon, max_lat],
        crs="EPSG:4326",
    )
    return [(cell.space_code, int(cell.level), cell.bbox) for cell in cells]


def build_grid_tasks_driver(
    assets: list[AssetRecord],
    grid_type: str,
    grid_level: int,
    cover_mode: str,
    max_cells_per_asset: int,
) -> list[dict]:
    grid_type = str(grid_type or "").strip().lower()
    if _is_plane_grid(grid_type):
        tasks: list[dict] = []
        for asset in assets:
            asset_tasks = _build_plane_grid_tasks_for_asset(asset, grid_level, cover_mode)
            if max_cells_per_asset > 0 and len(asset_tasks) > max_cells_per_asset:
                raise RuntimeError(
                    "Cover cells exceed max limit for asset %s: %d > %d"
                    % (asset.path, len(asset_tasks), max_cells_per_asset)
                )
            tasks.extend(asset_tasks)
        return tasks

    sdk = CubeEncoderSDK()
    bbox_cache: dict[str, list[float]] = {}
    scene_cover_cache: dict[tuple[str, float, float, float, float], list[tuple[str, int, list[float]]]] = {}
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

        for space_code, level, cb in cells:
            tasks.append(
                {
                    "scene_id": asset.scene_id,
                    "band": asset.band,
                    "asset_path": asset.path,
                    "acq_time": asset.acq_time,
                    "grid_type": grid_type,
                    "grid_level": int(level),
                    "space_code": space_code,
                    "cell_min_lon": float(cb[0]),
                    "cell_min_lat": float(cb[1]),
                    "cell_max_lon": float(cb[2]),
                    "cell_max_lat": float(cb[3]),
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


def _window_bounds_xy(ds: rasterio.DatasetReader, win: Window) -> tuple[float, float, float, float]:
    left, bottom, right, top = rasterio.windows.bounds(win, ds.transform)
    min_x, max_x = sorted((float(left), float(right)))
    min_y, max_y = sorted((float(bottom), float(top)))
    return min_x, min_y, max_x, max_y


def _plane_grid_window_from_row(ds: rasterio.DatasetReader, row: Any) -> Optional[Window]:
    col_off = getattr(row, "window_col_off", None)
    row_off = getattr(row, "window_row_off", None)
    width = getattr(row, "window_width", None)
    height = getattr(row, "window_height", None)
    if None not in (col_off, row_off, width, height):
        return _safe_window_from_offsets(ds, int(col_off), int(row_off), int(width), int(height))

    min_x = float(getattr(row, "cell_min_x", getattr(row, "cell_min_lon")))
    min_y = float(getattr(row, "cell_min_y", getattr(row, "cell_min_lat")))
    max_x = float(getattr(row, "cell_max_x", getattr(row, "cell_max_lon")))
    max_y = float(getattr(row, "cell_max_y", getattr(row, "cell_max_lat")))
    return _safe_window_from_bounds(ds, min_x, min_y, max_x, max_y)


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
            st_key = "%s|%d|%s|%s|%s" % (
                row.grid_type,
                row.grid_level,
                row.space_code,
                row.acq_time,
                time_granularity,
            )
            if st_key in st_cache:
                st_code = st_cache[st_key]
            else:
                st_code = sdk.generate_st_code(
                    grid_type=row.grid_type,
                    level=row.grid_level,
                    space_code=row.space_code,
                    timestamp=acq_dt,
                    time_granularity=time_granularity,
                ).st_code
                st_cache[st_key] = st_code

            if _is_plane_grid(row.grid_type):
                win = _plane_grid_window_from_row(open_ds, row)
                if win is None:
                    continue
                min_x, min_y, max_x, max_y = _window_bounds_xy(open_ds, win)

                sample_mean = None
                if include_sample_mean:
                    band1 = open_ds.read(1, window=win, masked=True)
                    if band1.size > 0 and band1.count() > 0:
                        sample_mean = float(band1.mean())

                cell_crs = str(getattr(row, "cell_crs", "") or (open_ds.crs.to_string() if open_ds.crs else ""))
                yield {
                    "scene_id": row.scene_id,
                    "band": row.band,
                    "asset_path": row.asset_path,
                    "acq_time": row.acq_time,
                    "grid_type": row.grid_type,
                    "grid_level": int(row.grid_level),
                    "space_code": row.space_code,
                    "space_code_prefix": getattr(row, "space_code_prefix", str(row.space_code)[:3]),
                    "st_code": st_code,
                    "time_bucket": row.time_bucket,
                    "cover_mode": row.cover_mode,
                    "cell_min_lon": float(getattr(row, "cell_min_lon", min_x)),
                    "cell_min_lat": float(getattr(row, "cell_min_lat", min_y)),
                    "cell_max_lon": float(getattr(row, "cell_max_lon", max_x)),
                    "cell_max_lat": float(getattr(row, "cell_max_lat", max_y)),
                    "cell_min_x": float(getattr(row, "cell_min_x", min_x)),
                    "cell_min_y": float(getattr(row, "cell_min_y", min_y)),
                    "cell_max_x": float(getattr(row, "cell_max_x", max_x)),
                    "cell_max_y": float(getattr(row, "cell_max_y", max_y)),
                    "cell_crs": cell_crs,
                    "cell_crs_token": str(getattr(row, "cell_crs_token", "") or ""),
                    "plane_grid_chunk_pixels": int(getattr(row, "plane_grid_chunk_pixels", 0) or 0),
                    "plane_grid_col": int(getattr(row, "plane_grid_col", 0) or 0),
                    "plane_grid_row": int(getattr(row, "plane_grid_row", 0) or 0),
                    "window_col_off": int(win.col_off),
                    "window_row_off": int(win.row_off),
                    "window_width": int(win.width),
                    "window_height": int(win.height),
                    "intersect_min_lon": float(min_x),
                    "intersect_min_lat": float(min_y),
                    "intersect_max_lon": float(max_x),
                    "intersect_max_lat": float(max_y),
                    "intersect_min_x": float(min_x),
                    "intersect_min_y": float(min_y),
                    "intersect_max_x": float(max_x),
                    "intersect_max_y": float(max_y),
                    "sample_mean_band1": sample_mean,
                }
                continue

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
                "space_code_prefix": row.space_code_prefix,
                "st_code": st_code,
                "time_bucket": row.time_bucket,
                "cover_mode": row.cover_mode,
                "cell_min_lon": float(row.cell_min_lon),
                "cell_min_lat": float(row.cell_min_lat),
                "cell_max_lon": float(row.cell_max_lon),
                "cell_max_lat": float(row.cell_max_lat),
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
    task_rows = [SimpleNamespace(**row) for row in rows]
    return list(process_partition(iter(task_rows), time_granularity, include_sample_mean=include_sample_mean))
