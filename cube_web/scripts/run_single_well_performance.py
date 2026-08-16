#!/usr/bin/env python3
"""Measure the single-well production partition chain and write a replayable report.

The input manifest is intentionally focused: one dataset, scene, data asset and
band for each of optical, radar, product and carbon.  The script imports that
manifest through the public schema endpoint, then submits ten independent
partition runs through the public scene endpoint and the configured Ray Jobs
executor.  It never prints runtime credentials.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from urllib.parse import quote, unquote, urlparse
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parents[2]
for package_root in (REPO_ROOT / "cube_encoder", REPO_ROOT / "cube_split", REPO_ROOT / "cube_web"):
    if str(package_root) not in sys.path:
        sys.path.insert(0, str(package_root))

from cube_split import runtime_config  # noqa: E402

DATA_TYPES = ("optical", "radar", "product", "carbon")
TERMINAL_TASK_STATES = {"succeeded", "completed", "failed", "cancelled", "partial_failure", "manual_required"}
GRID_ORDER = ("geohash", "mgrs", "isea4h")
STANDARD_OPTICAL_BANDS = 4


@dataclass(frozen=True)
class CaseSpec:
    case_id: str
    data_type: str
    grid_type: str
    partition_method: str


CASE_MATRIX = (
    CaseSpec("optical_geohash", "optical", "geohash", "logical"),
    CaseSpec("optical_mgrs", "optical", "mgrs", "logical"),
    CaseSpec("optical_isea4h", "optical", "isea4h", "entity"),
    CaseSpec("radar_geohash", "radar", "geohash", "logical"),
    CaseSpec("radar_mgrs", "radar", "mgrs", "logical"),
    CaseSpec("radar_isea4h", "radar", "isea4h", "entity"),
    CaseSpec("product_geohash", "product", "geohash", "logical"),
    CaseSpec("product_mgrs", "product", "mgrs", "logical"),
    CaseSpec("product_isea4h", "product", "isea4h", "entity"),
    CaseSpec("carbon_isea4h", "carbon", "isea4h", "entity"),
)

DATA_TYPE_LABELS = {
    "optical": "光学",
    "radar": "雷达",
    "product": "产品信息",
    "carbon": "碳卫星",
}


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, set):
        return sorted(json_safe(item) for item in value)
    return value


def epoch(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def elapsed_between(start: Any, end: Any) -> float | None:
    first, last = epoch(start), epoch(end)
    if first is None or last is None:
        return None
    return round(max(0.0, last - first), 6)


def load_json(path: str | Path) -> dict[str, Any]:
    manifest_path = Path(path)
    if not manifest_path.is_file():
        raise FileNotFoundError(f"single-well manifest not found: {manifest_path}")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("single-well manifest must be a JSON object")
    return payload


def validate_manifest(
    manifest: dict[str, Any], *, expected_optical_bands: int = STANDARD_OPTICAL_BANDS
) -> dict[str, Any]:
    """Validate one complete scene per data type for the formal benchmark."""
    well_id = str(manifest.get("well_id") or "").strip()
    if not well_id:
        raise ValueError("manifest.well_id is required")
    if str(manifest.get("scene_mode") or "").lower() != "full":
        raise ValueError("formal performance manifest must declare scene_mode=full")
    datasets = manifest.get("datasets")
    if not isinstance(datasets, list) or len(datasets) != len(DATA_TYPES):
        raise ValueError("manifest.datasets must contain exactly four data types")
    by_type: dict[str, dict[str, Any]] = {}
    input_profile: dict[str, Any] = {}
    dataset_ids: set[str] = set()
    for dataset in datasets:
        if not isinstance(dataset, dict):
            raise ValueError("datasets entries must be objects")
        data_type = str(dataset.get("data_type") or "").strip().lower()
        dataset_id = str(dataset.get("dataset_id") or "").strip()
        if data_type not in DATA_TYPES:
            raise ValueError(f"unsupported data_type: {data_type}")
        if data_type in by_type:
            raise ValueError(f"duplicate data_type: {data_type}")
        if not dataset_id or dataset_id in dataset_ids:
            raise ValueError(f"dataset_id must be unique and non-empty: {dataset_id}")
        dataset_ids.add(dataset_id)
        scenes = dataset.get("scenes")
        if not isinstance(scenes, list) or len(scenes) != 1:
            raise ValueError(f"{data_type} must contain exactly one scene for single-well testing")
        scene = scenes[0]
        assets = scene.get("assets") if isinstance(scene, dict) else None
        if not isinstance(assets, list) or not assets:
            raise ValueError(f"{data_type} scene must contain at least one data asset")
        band_count = 0
        radar_band_codes: list[str] = []
        asset_profiles: list[dict[str, Any]] = []
        for asset in assets:
            if not isinstance(asset, dict):
                raise ValueError(f"{data_type} assets must be objects")
            bands = asset.get("bands")
            if not isinstance(bands, list) or not bands:
                raise ValueError(f"{data_type} asset must contain at least one band")
            source_uri = str(asset.get("source_uri") or asset.get("cog_uri") or "").strip()
            parsed = urlparse(source_uri)
            if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
                raise ValueError(f"{data_type} source must use s3://bucket/key")
            checksum = str(asset.get("checksum") or "").lower()
            if len(checksum) != 64 or any(char not in "0123456789abcdef" for char in checksum):
                raise ValueError(f"{data_type} asset checksum must be a SHA-256 hex digest")
            if data_type == "carbon":
                if asset.get("cog_uri") or str(asset.get("source_kind") or "raw") != "raw":
                    raise ValueError("carbon asset must be raw and must not declare cog_uri")
                if str(asset.get("source_format") or "") not in {"netcdf", "hdf5", "sif"}:
                    raise ValueError("carbon source_format must be netcdf, hdf5 or sif")
            else:
                if not asset.get("bbox") or not asset.get("crs"):
                    raise ValueError(f"{data_type} COG asset requires bbox and crs")
                if str(asset.get("source_kind") or "cog") != "cog":
                    raise ValueError(f"{data_type} asset must use source_kind=cog")
            for band in bands:
                if not isinstance(band, dict):
                    raise ValueError(f"{data_type} bands must be objects")
                if data_type == "carbon":
                    continue
                source_band_index = band.get("source_band_index")
                if source_band_index is None:
                    source_band_index = (band.get("attributes") or {}).get("source_band_index")
                valid_string_index = (
                    isinstance(source_band_index, str)
                    and source_band_index.isdigit()
                    and int(source_band_index) >= 1
                )
                valid_integer_index = (
                    isinstance(source_band_index, int)
                    and not isinstance(source_band_index, bool)
                    and source_band_index >= 1
                )
                if not valid_string_index and not valid_integer_index:
                    raise ValueError(f"{data_type} band requires a positive source_band_index")
                if data_type == "radar":
                    radar_band_codes.append(str(band.get("band_code") or "").strip().upper())
            attributes = asset.get("attributes") if isinstance(asset.get("attributes"), dict) else {}
            asset_profiles.append({
                "asset_id": asset.get("asset_id"),
                "width": attributes.get("width"),
                "height": attributes.get("height"),
                "raster_band_count": attributes.get("count"),
                "band_count": len(bands),
                "source_uri": source_uri,
            })
            band_count += len(bands)
        expected_counts = manifest.get("expected_band_counts")
        if data_type == "optical":
            if expected_optical_bands < 1:
                raise ValueError("expected optical band count must be positive")
            if not isinstance(expected_counts, dict) or int(expected_counts.get("optical", -1)) != expected_optical_bands:
                raise ValueError(
                    f"formal optical benchmark must declare expected_band_counts.optical={expected_optical_bands}"
                )
            if band_count != expected_optical_bands:
                raise ValueError(f"optical requires {expected_optical_bands} bands, observed {band_count}")
        elif isinstance(expected_counts, dict) and data_type in expected_counts:
            expected = int(expected_counts[data_type])
            if band_count != expected:
                raise ValueError(f"{data_type} requires {expected} bands, observed {band_count}")
        if data_type == "radar" and sorted(radar_band_codes) != ["VH", "VV"]:
            raise ValueError("formal radar benchmark must contain exactly one VV asset and one VH asset")
        input_profile[data_type] = {
            "scene_count": 1,
            "asset_count": len(assets),
            "band_count": band_count,
            "assets": asset_profiles,
        }
        by_type[data_type] = dataset
    if set(by_type) != set(DATA_TYPES):
        raise ValueError("manifest must contain optical, radar, product and carbon datasets")

    raw_levels = manifest.get("grid_levels")
    if not isinstance(raw_levels, dict):
        raise ValueError("manifest.grid_levels must define geohash, mgrs and isea4h")
    levels: dict[str, int] = {}
    from grid_core.app.core.enums import GridType
    from grid_core.app.models.request import validate_requested_grid_level

    for grid_type in GRID_ORDER:
        try:
            level = int(raw_levels[grid_type])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"grid_levels.{grid_type} must be an integer") from exc
        validate_requested_grid_level(GridType(grid_type), level)
        levels[grid_type] = level
    try:
        max_cells_per_asset = int(manifest.get("max_cells_per_asset", 0))
    except (TypeError, ValueError) as exc:
        raise ValueError("max_cells_per_asset must be a non-negative integer") from exc
    if max_cells_per_asset < 0:
        raise ValueError("max_cells_per_asset must be non-negative")
    try:
        carbon_max_observations = int(manifest.get("carbon_max_observations"))
    except (TypeError, ValueError) as exc:
        raise ValueError("carbon_max_observations must be a positive integer") from exc
    if carbon_max_observations < 1:
        raise ValueError("carbon_max_observations must be a positive integer")
    return {
        "well_id": well_id,
        "datasets_by_type": by_type,
        "grid_levels": levels,
        "max_cells_per_asset": max_cells_per_asset,
        "carbon_max_observations": carbon_max_observations,
        "input_profile": input_profile,
    }


def namespace_manifest(manifest: dict[str, Any], prefix: str | None = None) -> tuple[dict[str, Any], str]:
    token = prefix or f"single-well-perf-{uuid4().hex[:12]}"
    result = json.loads(json.dumps(manifest, ensure_ascii=False))
    result["load_batch_id"] = f"{token}-load"
    result["batch_name"] = f"Single-well partition performance {token}"
    result["source_system"] = result.get("source_system") or "single-well-performance"
    for dataset in result["datasets"]:
        dataset_id = str(dataset["dataset_id"])
        dataset["dataset_id"] = f"{token}-{dataset_id}"
        dataset["dataset_code"] = f"{token}-{dataset.get('dataset_code') or dataset_id}"
        for scene in dataset["scenes"]:
            scene_id = str(scene.get("scene_id") or scene.get("scene_key") or uuid4().hex[:8])
            namespaced_scene_id = f"{token}-{scene_id}"
            scene["scene_id"] = namespaced_scene_id
            scene["canonical_scene_id"] = namespaced_scene_id
            scene["scene_key"] = f"{token}-{scene.get('scene_key') or scene_id}"
            scene["identity_key"] = f"{token}:{scene['scene_key']}"
            for asset in scene.get("assets", []):
                asset_id = str(asset.get("asset_id") or asset.get("source_asset_id") or uuid4().hex[:8])
                asset["asset_id"] = f"{token}-{asset_id}"
                for band in asset.get("bands", []):
                    band_code = str(band.get("band_code") or "band")
                    band["band_code"] = f"{token}_{band_code}"
    return result, token


def import_payload(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": manifest.get("schema_version", "single-well-performance-v1"),
        "load_batch_id": manifest["load_batch_id"],
        "batch_name": manifest["batch_name"],
        "source_system": manifest["source_system"],
        "loaded_at": manifest.get("loaded_at"),
        "datasets": manifest["datasets"],
    }


@dataclass
class HttpClient:
    base_url: str
    token: str | None = None
    timeout: float = 60.0

    def request_timed(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        started_tick = time.perf_counter()
        started_epoch = time.time()
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        data = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            self.base_url.rstrip("/") + path,
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")
            raise RuntimeError(f"HTTP {exc.code} {method} {path}: {detail[:500]}") from exc
        finished_epoch = time.time()
        timing = {
            "started_at": datetime.fromtimestamp(started_epoch, UTC).isoformat().replace("+00:00", "Z"),
            "finished_at": datetime.fromtimestamp(finished_epoch, UTC).isoformat().replace("+00:00", "Z"),
            "started_epoch": started_epoch,
            "finished_epoch": finished_epoch,
            "elapsed_sec": round(time.perf_counter() - started_tick, 6),
        }
        value = json.loads(raw or b"{}")
        if not isinstance(value, dict):
            raise RuntimeError(f"HTTP {method} {path} returned a non-object JSON response")
        return value, timing


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _positive_int(value: Any, label: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"{label} must be a positive integer") from exc
    if result < 1:
        raise RuntimeError(f"{label} must be a positive integer")
    return result


def verify_sources(manifest: dict[str, Any], *, skip_checksum: bool = False) -> dict[str, Any]:
    import rasterio
    from minio import Minio

    settings = runtime_config.minio_settings()
    if not all((settings.endpoint, settings.access_key, settings.secret_key, settings.bucket)):
        raise RuntimeError("MinIO runtime configuration is incomplete")
    client = Minio(settings.endpoint, access_key=settings.access_key, secret_key=settings.secret_key, secure=settings.secure)
    records: list[dict[str, Any]] = []
    started_tick = time.perf_counter()
    with TemporaryDirectory(prefix="single-well-source-verify-") as temp_dir:
        temp_root = Path(temp_dir)
        for dataset in manifest["datasets"]:
            for scene in dataset["scenes"]:
                for asset in scene["assets"]:
                    data_type = str(dataset["data_type"])
                    uri = str(asset.get("source_uri") or asset.get("cog_uri") or "")
                    parsed = urlparse(uri)
                    bucket = parsed.netloc
                    key = unquote(parsed.path.lstrip("/"))
                    stat_started = time.perf_counter()
                    stat = client.stat_object(bucket, key)
                    stat_elapsed = round(time.perf_counter() - stat_started, 6)
                    size_bytes = int(getattr(stat, "size", 0) or 0)
                    if size_bytes <= 0:
                        raise RuntimeError(f"source object is empty: {uri}")
                    checksum_elapsed = 0.0
                    observed_checksum = None
                    actual_raster: dict[str, Any] = {}
                    if data_type == "carbon":
                        if not skip_checksum:
                            digest = hashlib.sha256()
                            checksum_started = time.perf_counter()
                            response = client.get_object(bucket, key)
                            try:
                                for chunk in response.stream(1024 * 1024):
                                    digest.update(chunk)
                            finally:
                                response.close()
                                response.release_conn()
                            checksum_elapsed = round(time.perf_counter() - checksum_started, 6)
                            observed_checksum = digest.hexdigest()
                    else:
                        local_path = temp_root / f"{len(records):04d}.tif"
                        download_started = time.perf_counter()
                        client.fget_object(bucket, key, str(local_path))
                        download_elapsed = round(time.perf_counter() - download_started, 6)
                        if not skip_checksum:
                            checksum_started = time.perf_counter()
                            observed_checksum = _sha256_file(local_path)
                            checksum_elapsed = round(time.perf_counter() - checksum_started, 6)
                        with rasterio.open(local_path) as raster:
                            actual_raster = {
                                "width": raster.width,
                                "height": raster.height,
                                "count": raster.count,
                                "crs": raster.crs.to_string() if raster.crs else None,
                            }
                        attributes = asset.get("attributes") if isinstance(asset.get("attributes"), dict) else {}
                        declared_width = _positive_int(attributes.get("width"), f"{data_type} asset width")
                        declared_height = _positive_int(attributes.get("height"), f"{data_type} asset height")
                        declared_count = _positive_int(attributes.get("count"), f"{data_type} asset band count")
                        if (declared_width, declared_height, declared_count) != (
                            actual_raster["width"], actual_raster["height"], actual_raster["count"]
                        ):
                            raise RuntimeError(
                                f"{data_type} raster metadata mismatch for {uri}: "
                                f"manifest={declared_width}x{declared_height}/{declared_count}, "
                                f"actual={actual_raster['width']}x{actual_raster['height']}/{actual_raster['count']}"
                            )
                        if str(attributes.get("preparation") or "") != "full":
                            raise RuntimeError(f"{data_type} asset is not marked as a full-scene preparation: {uri}")
                        source_window = attributes.get("source_window")
                        if not isinstance(source_window, dict):
                            raise RuntimeError(f"{data_type} full-scene asset must declare source_window: {uri}")
                        if any(int(source_window.get(name, -1)) != expected for name, expected in (
                            ("col_off", 0),
                            ("row_off", 0),
                            ("width", actual_raster["width"]),
                            ("height", actual_raster["height"]),
                        )):
                            raise RuntimeError(f"{data_type} asset source_window is not the complete scene: {uri}")
                        source_indexes = []
                        for band in asset.get("bands") or []:
                            source_index = band.get("source_band_index")
                            if source_index is None:
                                source_index = (band.get("attributes") or {}).get("source_band_index")
                            source_indexes.append(int(source_index))
                        if sorted(source_indexes) != list(range(1, actual_raster["count"] + 1)):
                            raise RuntimeError(f"{data_type} bands do not cover the actual COG bands: {uri}")
                    if observed_checksum is not None and observed_checksum != str(asset["checksum"]).lower():
                        raise RuntimeError(f"source SHA-256 mismatch: {uri}")
                    records.append({
                        "data_type": data_type,
                        "source_asset_id": asset.get("asset_id"),
                        "source_uri": uri,
                        "size_bytes": size_bytes,
                        "etag": str(getattr(stat, "etag", "") or "").strip('"'),
                        "stat_elapsed_sec": stat_elapsed,
                        "download_elapsed_sec": download_elapsed if data_type != "carbon" else 0.0,
                        "checksum_elapsed_sec": checksum_elapsed,
                        "checksum_verified": observed_checksum is not None,
                        "verified_width": actual_raster.get("width"),
                        "verified_height": actual_raster.get("height"),
                        "verified_count": actual_raster.get("count"),
                    })
    return {
        "elapsed_sec": round(time.perf_counter() - started_tick, 6),
        "checksum_skipped": skip_checksum,
        "objects": records,
    }


def build_case_payload(
    manifest: dict[str, Any],
    validated: dict[str, Any],
    spec: CaseSpec,
    namespace: str,
) -> dict[str, Any]:
    dataset = validated["datasets_by_type"][spec.data_type]
    scene = dataset["scenes"][0]
    partition: dict[str, Any] = {
        "grid_type": spec.grid_type,
        "requested_grid_level": validated["grid_levels"][spec.grid_type],
        "partition_method": spec.partition_method,
        "cover_mode": "intersect",
        "time_granularity": "day",
        "max_cells_per_asset": validated["max_cells_per_asset"],
    }
    if spec.data_type == "carbon":
        partition["max_observations"] = validated["carbon_max_observations"]
    return {
        "partition_run_id": f"{namespace}-{spec.case_id}",
        "source_batch_ids": [manifest["load_batch_id"]],
        "datasets": [{
            "dataset_id": dataset["dataset_id"],
            "scene_ids": [scene["scene_id"]],
            "partition": partition,
        }],
    }


def query_attempt(dsn: str, task_id: str) -> dict[str, Any] | None:
    import psycopg

    with psycopg.connect(dsn, client_encoding="UTF8") as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM partition_job_attempts WHERE task_id = %s",
                (task_id,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            values = dict(zip((column.name for column in cursor.description), row, strict=True))
    return json_safe(values)


def wait_for_attempt(dsn: str, task_id: str, timeout_seconds: float = 30.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while True:
        attempt = query_attempt(dsn, task_id)
        if attempt is not None:
            return attempt
        if time.monotonic() >= deadline:
            raise TimeoutError(f"attempt row was not visible: {task_id}")
        time.sleep(0.5)


def wait_for_task(
    client: HttpClient,
    task_id: str,
    *,
    poll_seconds: float,
    timeout_seconds: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    started_tick = time.perf_counter()
    started_epoch = time.time()
    polls: list[dict[str, Any]] = []
    deadline = time.monotonic() + timeout_seconds
    while True:
        task, request_timing = client.request_timed("GET", f"/v1/partition/tasks/{task_id}")
        status = str(task.get("status") or "").lower()
        polls.append({"status": status, "at": request_timing["finished_at"], "request_elapsed_sec": request_timing["elapsed_sec"]})
        if status in TERMINAL_TASK_STATES:
            finished_epoch = time.time()
            return task, {
                "started_at": datetime.fromtimestamp(started_epoch, UTC).isoformat().replace("+00:00", "Z"),
                "finished_at": datetime.fromtimestamp(finished_epoch, UTC).isoformat().replace("+00:00", "Z"),
                "elapsed_sec": round(time.perf_counter() - started_tick, 6),
                "poll_count": len(polls),
                "polls": polls,
            }
        if time.monotonic() >= deadline:
            raise TimeoutError(f"partition task did not finish: {task_id}")
        time.sleep(poll_seconds)


def ray_job_report(
    address: str,
    job_id: str | None,
    *,
    terminal_timeout_seconds: float = 30.0,
    poll_seconds: float = 0.5,
) -> dict[str, Any]:
    if not job_id:
        return {"available": False, "reason": "missing_ray_job_id"}
    lookup_started = time.perf_counter()
    deadline = time.monotonic() + terminal_timeout_seconds
    poll_count = 0
    try:
        base = _ray_job_api_base(address)
        while True:
            poll_count += 1
            request = urllib.request.Request(
                f"{base}/api/jobs/{quote(job_id, safe='')}",
                headers={"Accept": "application/json"},
            )
            with urllib.request.urlopen(request, timeout=10) as response:
                info = json.loads(response.read() or b"{}")
            if not isinstance(info, dict):
                raise RuntimeError("Ray Jobs API returned a non-object job payload")
            status = str(info.get("status") or "").upper()
            if info.get("end_time") is not None or status in {"SUCCEEDED", "FAILED", "STOPPED"}:
                break
            if time.monotonic() >= deadline:
                break
            time.sleep(poll_seconds)
    except Exception as exc:
        return {
            "available": False,
            "job_id": job_id,
            "error": str(exc)[:500],
            "lookup_elapsed_sec": round(time.perf_counter() - lookup_started, 6),
            "poll_count": poll_count,
        }
    start_time = info.get("start_time")
    end_time = info.get("end_time")
    start_epoch = float(start_time) / (1000 if start_time and start_time > 100_000_000_000 else 1) if start_time else None
    end_epoch = float(end_time) / (1000 if end_time and end_time > 100_000_000_000 else 1) if end_time else None
    return {
        "available": True,
        "job_id": job_id,
        "status": str(info.get("status") or ""),
        "start_at": datetime.fromtimestamp(start_epoch, UTC).isoformat().replace("+00:00", "Z") if start_epoch else None,
        "end_at": datetime.fromtimestamp(end_epoch, UTC).isoformat().replace("+00:00", "Z") if end_epoch else None,
        "elapsed_sec": round(max(0.0, end_epoch - start_epoch), 6) if start_epoch and end_epoch else None,
        "driver_node_id": info.get("driver_node_id"),
        "driver_exit_code": info.get("driver_exit_code"),
        "error_type": info.get("error_type"),
        "lookup_elapsed_sec": round(time.perf_counter() - lookup_started, 6),
        "poll_count": poll_count,
    }


def _ray_job_api_base(address: str) -> str:
    value = str(address or "").strip().rstrip("/")
    if not value:
        raise ValueError("Ray Jobs address is required for the cold-start gate")
    if value.startswith(("http://", "https://")):
        return value
    if value.startswith("ray://"):
        raise ValueError("Ray Jobs address must expose the HTTP dashboard API")
    return f"http://{value}"


def ray_jobs_snapshot(address: str) -> dict[str, Any]:
    """Read the Jobs API without requiring a local Ray GCS connection."""
    started = time.perf_counter()
    base = _ray_job_api_base(address)
    request = urllib.request.Request(f"{base}/api/jobs/", headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = json.loads(response.read() or b"[]")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise RuntimeError(f"Ray Jobs API HTTP {exc.code}: {detail[:300]}") from exc
    if not isinstance(payload, list):
        raise RuntimeError("Ray Jobs API returned a non-list payload")
    status_counts: dict[str, int] = {}
    active_ids: list[str] = []
    active_partition_ids: list[str] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "UNKNOWN").upper()
        status_counts[status] = status_counts.get(status, 0) + 1
        submission_id = str(item.get("submission_id") or item.get("job_id") or "")
        metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
        is_partition_job = metadata.get("cube_job_kind") == "partition" or submission_id.startswith("partition-")
        if status in {"RUNNING", "PENDING", "SUBMITTED"}:
            if submission_id:
                active_ids.append(submission_id)
                if is_partition_job:
                    active_partition_ids.append(submission_id)
    return {
        "source": "ray_jobs_api",
        "address": base,
        "observed_at": utc_now_iso(),
        "job_count": len(payload),
        "status_counts": status_counts,
        "active_job_count": len(active_ids),
        "active_submission_ids": active_ids,
        "active_partition_job_count": len(active_partition_ids),
        "active_partition_submission_ids": active_partition_ids,
        "request_elapsed_sec": round(time.perf_counter() - started, 6),
    }


def wait_for_cold_start(
    job_address: str,
    *,
    timeout_seconds: float,
    poll_seconds: float,
) -> dict[str, Any]:
    """Verify no partition Ray Job is active before submitting the next one.

    The configured cluster keeps Ray worker nodes alive, so a zero-worker gate
    would reject a healthy long-running KubeRay cluster.  The production
    executor creates one new Ray Job per partition task; a quiescent partition
    Job set plus a new submission is the cold-start boundary measured here.
    """
    started_tick = time.perf_counter()
    started_at = utc_now_iso()
    deadline = time.monotonic() + timeout_seconds
    observations: list[dict[str, Any]] = []
    while True:
        snapshot = ray_jobs_snapshot(job_address)
        active_partition = list(snapshot.get("active_partition_submission_ids") or [])
        observations.append(snapshot)
        if not active_partition:
            return {
                "enabled": True,
                "mode": "partition_job_quiescence_then_new_submission",
                "started_at": started_at,
                "observed_at": utc_now_iso(),
                "elapsed_sec": round(time.perf_counter() - started_tick, 6),
                "observations": observations,
            }
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Ray cold-start gate found active partition jobs: {active_partition}")
        time.sleep(poll_seconds)


def timing_records(value: Any, path: str = "") -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if isinstance(value, dict):
        if isinstance(value.get("scope"), str) and isinstance(value.get("phases"), dict):
            record = value
            for phase_name, phase in record["phases"].items():
                if not isinstance(phase, dict):
                    continue
                records.append({
                    "path": path,
                    "scope": record["scope"],
                    "started_at": record.get("started_at"),
                    "finished_at": record.get("finished_at"),
                    "elapsed_sec": record.get("elapsed_sec"),
                    "phase": phase_name,
                    "phase_elapsed_sec": phase.get("elapsed_sec"),
                    "phase_count": phase.get("count", 0),
                })
        for key, item in value.items():
            if key not in {"attributes", "phases"}:
                records.extend(timing_records(item, f"{path}.{key}" if path else str(key)))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            records.extend(timing_records(item, f"{path}[{index}]"))
    return records


def build_summary(case: dict[str, Any], threshold_scope: str) -> dict[str, Any]:
    attempt = case.get("attempt") or {}
    submit_timing = case.get("submission_timing") or {}
    final_task = case.get("final_task") or {}
    core_elapsed = elapsed_between(attempt.get("started_at"), attempt.get("finished_at"))
    end_to_end = elapsed_between(submit_timing.get("started_at"), attempt.get("finished_at"))
    queue_elapsed = elapsed_between(attempt.get("created_at"), attempt.get("started_at"))
    ray_info = case.get("ray_job") or {}
    ray_start_to_attempt = elapsed_between(ray_info.get("start_at"), attempt.get("started_at"))
    submit_to_ray = elapsed_between(submit_timing.get("started_at"), ray_info.get("start_at"))
    result = attempt.get("runner_result") or final_task.get("result") or {}
    records = timing_records(result if isinstance(result, dict) else None)
    driver_records = [row for row in records if str(row["scope"]).endswith("_driver")]
    worker_records = [row for row in records if str(row["scope"]).endswith("_worker")]
    workflow_records = [row for row in records if row["scope"] == "workflow_dataset"]
    job_records = [row for row in records if row["scope"] == "ray_job_driver"]
    driver_elapsed = max((float(row["elapsed_sec"] or 0) for row in driver_records), default=None)
    worker_elapsed_max = max((float(row["elapsed_sec"] or 0) for row in worker_records), default=None)
    worker_scopes: dict[tuple[str, str], float] = {}
    for row in worker_records:
        worker_scopes[(str(row["path"]), str(row["scope"]))] = float(row["elapsed_sec"] or 0)
    worker_elapsed_sum = sum(worker_scopes.values()) if worker_scopes else None
    workflow_elapsed = max((float(row["elapsed_sec"] or 0) for row in workflow_records), default=None)
    job_driver_elapsed = max((float(row["elapsed_sec"] or 0) for row in job_records), default=None)
    ray_measurement_ok = (
        bool(ray_info.get("available"))
        and str(ray_info.get("status") or "").upper() == "SUCCEEDED"
        and ray_info.get("elapsed_sec") is not None
    )
    gate = case.get("cold_start_gate") if isinstance(case.get("cold_start_gate"), dict) else {}
    observations = gate.get("observations") if isinstance(gate.get("observations"), list) else []
    active_partition_count = max(
        (int(item.get("active_partition_job_count") or 0) for item in observations if isinstance(item, dict)),
        default=0,
    )
    cold_start_gate_valid = bool(gate.get("enabled", bool(gate.get("mode")))) and bool(observations) and active_partition_count == 0
    core_pass = core_elapsed is not None and core_elapsed < 10.0
    e2e_pass = end_to_end is not None and end_to_end < 10.0
    if threshold_scope == "partition":
        passed = core_pass
    elif threshold_scope == "end_to_end":
        passed = e2e_pass
    else:
        passed = core_pass and e2e_pass
    final_status = str(final_task.get("status") or "").lower()
    if final_status not in {"completed", "succeeded"} or str(attempt.get("status") or "").lower() != "succeeded":
        passed = False
    if not ray_measurement_ok:
        passed = False
    if not cold_start_gate_valid:
        passed = False
    return {
        "case_id": case["case_id"],
        "data_type": case["data_type"],
        "grid_type": case["grid_type"],
        "grid_level": case["grid_level"],
        "partition_method": case["partition_method"],
        "task_id": case.get("task_id"),
        "ray_job_id": (case.get("ray_job") or {}).get("job_id"),
        "ray_job_status": ray_info.get("status"),
        "ray_job_available": bool(ray_info.get("available")),
        "cold_start_gate_valid": cold_start_gate_valid,
        "final_status": final_status,
        "attempt_status": attempt.get("status"),
        "partition_execution_sec": core_elapsed,
        "end_to_end_sec": end_to_end,
        "queue_wait_sec": queue_elapsed,
        "api_submit_sec": submit_timing.get("elapsed_sec"),
        "submit_to_ray_start_sec": submit_to_ray,
        "ray_start_to_attempt_start_sec": ray_start_to_attempt,
        "ray_job_sec": ray_info.get("elapsed_sec"),
        "ray_job_lookup_sec": ray_info.get("lookup_elapsed_sec"),
        "ray_driver_internal_sec": driver_elapsed,
        "worker_internal_max_sec": worker_elapsed_max,
        "worker_internal_sum_sec": worker_elapsed_sum,
        "workflow_internal_sec": workflow_elapsed,
        "job_driver_internal_sec": job_driver_elapsed,
        "threshold_scope": threshold_scope,
        "partition_under_10_sec": core_pass,
        "end_to_end_under_10_sec": e2e_pass,
        "passed": passed,
        "error": case.get("error"),
    }


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_report(
    path: Path,
    *,
    run_metadata: dict[str, Any],
    summaries: list[dict[str, Any]],
    phases: list[dict[str, Any]],
    fatal_error: str | None,
) -> None:
    lines = [
        "# 单井剖分性能测试报告",
        "",
        f"- 测试时间：`{run_metadata.get('started_at')}` 至 `{run_metadata.get('finished_at')}`",
        f"- 单井标识：`{run_metadata.get('well_id')}`",
        f"- 单井口径：{run_metadata.get('well_definition', '每类数据一个 scene/asset/band；各 asset 保留独立 bbox。')}",
        f"- 执行链路：`{run_metadata.get('partition_executor')}` + Ray Jobs",
        f"- 验收口径：`{run_metadata.get('threshold_scope')}`，阈值 `< 10 秒`",
        "- `partition_execution_sec`：OpenGauss attempt 的 `started_at` 到 `finished_at`，代表任务执行区间。",
        "- `end_to_end_sec`：客户端提交请求开始到 OpenGauss attempt 完成，包含 API、排队、Ray Job 冷启动和执行。",
        "- `ray_job_sec`：Ray Jobs API 的 `start_time` 到 `end_time`；任务完成后等待 Jobs API 进入终态再读取。",
        "",
        "## 输入景规格",
        "",
        "| 数据类型 | scene 数 | asset 数 | band 数 | 影像尺寸/波段数 |",
        "|---|---:|---:|---:|---|",
    ]
    input_profile = run_metadata.get("input_profile") or {}
    for data_type in DATA_TYPES:
        profile = input_profile.get(data_type) or {}
        asset_descriptions = []
        for asset in profile.get("assets") or []:
            width = asset.get("width") or "?"
            height = asset.get("height") or "?"
            raster_band_count = asset.get("raster_band_count") or "?"
            asset_descriptions.append(f"{width}x{height}/{raster_band_count} band")
        lines.append(
            f"| {DATA_TYPE_LABELS.get(data_type, data_type)} | {profile.get('scene_count', '-')} | "
            f"{profile.get('asset_count', '-')} | {profile.get('band_count', '-')} | "
            f"{'; '.join(asset_descriptions) or '-'} |"
        )
    lines.extend([
        "",
        "- 正式口径要求 `scene_mode=full`；不使用 64×64 等裁剪窗口作为验收输入。",
        "",
        "## 场景汇总",
        "",
        "| 场景 | 数据类型 | 格网 | 层级 | 执行区间(s) | 端到端(s) | 队列(s) | Ray Job(s) | 结果 | 10秒验收 |",
        "|---|---|---|---:|---:|---:|---:|---:|---|---|",
    ])
    for row in summaries:
        status = "通过" if row.get("passed") else "未通过"
        lines.append(
            f"| `{row['case_id']}` | {DATA_TYPE_LABELS.get(row['data_type'], row['data_type'])} | "
            f"`{row['grid_type']}` | {row['grid_level']} | {fmt(row.get('partition_execution_sec'))} | "
            f"{fmt(row.get('end_to_end_sec'))} | {fmt(row.get('queue_wait_sec'))} | "
            f"{fmt(row.get('ray_job_sec'))} | {row.get('final_status')} | {status} |"
        )
    if fatal_error:
        lines.extend(["", "## 执行错误", "", f"`{fatal_error}`"])
    lines.extend(["", "## 分阶段明细", ""])
    by_case: dict[str, list[dict[str, Any]]] = {}
    for row in phases:
        by_case.setdefault(str(row["case_id"]), []).append(row)
    for summary in summaries:
        case_id = summary["case_id"]
        lines.extend([
            f"### `{case_id}`",
            "",
            "| Scope | Phase | 次数 | 阶段耗时(s) | Scope总耗时(s) |",
            "|---|---|---:|---:|---:|",
        ])
        for row in by_case.get(case_id, []):
            lines.append(
                f"| `{row['scope']}` | `{row['phase']}` | {row['phase_count']} | "
                f"{fmt(row.get('phase_elapsed_sec'))} | {fmt(row.get('elapsed_sec'))} |"
            )
        if not by_case.get(case_id):
            lines.append("| - | 无内部计时记录 | - | - | - |")
        lines.append("")
    passed_count = sum(bool(row.get("passed")) for row in summaries)
    failed_cases = [str(row["case_id"]) for row in summaries if not row.get("passed")]
    lines.extend([
        "## 结论",
        "",
        f"- 共完成 {len(summaries)} 个场景，任务状态均为 completed/succeeded；按当前 both 口径通过 {passed_count} 个。",
        "- `both` 要求 OpenGauss attempt 执行区间和端到端耗时都小于 10 秒。",
    ])
    if failed_cases:
        lines.append(f"- 未通过场景：{', '.join(f'`{case_id}`' for case_id in failed_cases)}。")
    lines.extend([
        "- 详细阶段计时以 `phase_timings.csv` 为准；同一 Worker 的总时长按 scope 去重，避免按 phase 重复累加。",
        "",
    ])
    lines.extend([
        "## 原始产物",
        "",
        "- `raw_cases.json`：每个场景的请求、任务、OpenGauss attempt、Ray Job 和原始计时树。",
        "- `summary.csv`：每个场景的一行验收汇总。",
        "- `phase_timings.csv`：所有 scope/phase 的逐项耗时。",
        "- `namespaced_manifest.json`：本次导入使用的命名空间化输入，不包含运行凭据。",
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def fmt(value: Any) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):.3f}"
    except (TypeError, ValueError):
        return str(value)


def run_case(
    client: HttpClient,
    dsn: str,
    ray_job_address: str,
    manifest: dict[str, Any],
    validated: dict[str, Any],
    namespace: str,
    spec: CaseSpec,
    *,
    cold_start_gate: bool,
    cold_start_timeout: float,
    cold_start_poll: float,
    poll_seconds: float,
    task_timeout: float,
) -> dict[str, Any]:
    case_started_at = utc_now_iso()
    case: dict[str, Any] = {
        "case_id": spec.case_id,
        "data_type": spec.data_type,
        "grid_type": spec.grid_type,
        "grid_level": validated["grid_levels"][spec.grid_type],
        "partition_method": spec.partition_method,
        "case_started_at": case_started_at,
    }
    try:
        if cold_start_gate:
            case["cold_start_gate"] = wait_for_cold_start(
                ray_job_address,
                timeout_seconds=cold_start_timeout,
                poll_seconds=cold_start_poll,
            )
        else:
            case["cold_start_gate"] = {"enabled": False, "observed_at": utc_now_iso(), "observations": []}
        payload = build_case_payload(manifest, validated, spec, namespace)
        case["request_payload"] = payload
        response, submission_timing = client.request_timed("POST", "/v1/partition/runs", payload)
        case["submission_response"] = response
        case["submission_timing"] = submission_timing
        task_id = str(response.get("task_id") or "")
        if not task_id:
            raise RuntimeError(f"{spec.case_id} submission did not return task_id")
        case["task_id"] = task_id
        final_task, polling_timing = wait_for_task(
            client,
            task_id,
            poll_seconds=poll_seconds,
            timeout_seconds=task_timeout,
        )
        case["final_task"] = final_task
        case["polling_timing"] = polling_timing
        attempt = wait_for_attempt(dsn, task_id)
        case["attempt"] = attempt
        case["ray_job"] = ray_job_report(ray_job_address, str(attempt.get("ray_job_id") or "") or None)
    except Exception as exc:
        case["error"] = str(exc)[:1000]
    case["case_finished_at"] = utc_now_iso()
    return case


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--base-url", default=os.getenv("CUBE_WEB_PERF_BASE_URL", "http://127.0.0.1:50039"))
    parser.add_argument("--token", default=os.getenv("CUBE_WEB_PERF_TOKEN") or os.getenv("CUBE_WEB_ACCEPTANCE_TOKEN"))
    parser.add_argument("--output-dir", default=str(REPO_ROOT / ".tmp" / "single_well_performance"))
    parser.add_argument("--prefix", default=None)
    parser.add_argument("--ray-job-address", default=None)
    parser.add_argument("--ray-address", default=None)
    parser.add_argument("--ray-head-ip", default=None)
    parser.add_argument("--allow-warm-start", action="store_true")
    parser.add_argument("--skip-source-checksum", action="store_true")
    parser.add_argument("--expected-optical-bands", type=int, default=STANDARD_OPTICAL_BANDS)
    parser.add_argument(
        "--case-ids",
        default=None,
        help="Comma-separated case ids to run; defaults to the complete CASE_MATRIX.",
    )
    parser.add_argument("--threshold-scope", choices=("partition", "end_to_end", "both"), default="both")
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--cold-start-timeout", type=float, default=300.0)
    parser.add_argument("--task-timeout", type=float, default=1800.0)
    args = parser.parse_args(argv)

    if args.case_ids:
        requested_case_ids = tuple(item.strip() for item in args.case_ids.split(",") if item.strip())
        known_case_ids = {spec.case_id for spec in CASE_MATRIX}
        unknown_case_ids = sorted(set(requested_case_ids) - known_case_ids)
        if unknown_case_ids:
            parser.error(f"unknown case ids: {', '.join(unknown_case_ids)}")
        case_specs = tuple(spec for spec in CASE_MATRIX if spec.case_id in requested_case_ids)
    else:
        case_specs = CASE_MATRIX

    run_id = f"{time.strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"
    run_dir = Path(args.output_dir) / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    run_started_at = utc_now_iso()
    fatal_error: str | None = None
    cases: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    phases: list[dict[str, Any]] = []
    manifest: dict[str, Any] = {}
    namespaced: dict[str, Any] = {}
    namespace = ""
    validated: dict[str, Any] = {}
    source_report: dict[str, Any] | None = None
    imported: dict[str, Any] | None = None
    runtime_ray_address = args.ray_address or runtime_config.env_text("CUBE_WEB_RAY_ADDRESS")
    runtime_job_address = args.ray_job_address or runtime_config.env_text("CUBE_WEB_RAY_JOB_ADDRESS")
    head_ip = args.ray_head_ip
    if not head_ip and ":" in runtime_ray_address and not runtime_ray_address.startswith("ray://"):
        head_ip = runtime_ray_address.rsplit(":", 1)[0]
    executor = runtime_config.env_text("CUBE_WEB_PARTITION_EXECUTOR", "local").lower()
    try:
        if executor != "ray_job":
            raise RuntimeError("CUBE_WEB_PARTITION_EXECUTOR must be ray_job for this performance test")
        if not runtime_ray_address or not runtime_job_address:
            raise RuntimeError("CUBE_WEB_RAY_ADDRESS and CUBE_WEB_RAY_JOB_ADDRESS are required")
        manifest = load_json(args.manifest)
        validated = validate_manifest(manifest, expected_optical_bands=args.expected_optical_bands)
        namespaced, namespace = namespace_manifest(manifest, args.prefix)
        (run_dir / "namespaced_manifest.json").write_text(
            json.dumps(json_safe(namespaced), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        if args.skip_source_checksum:
            raise RuntimeError("formal performance runs require source checksum verification")
        source_report = verify_sources(namespaced, skip_checksum=args.skip_source_checksum)
        client = HttpClient(args.base_url, args.token)
        imported, import_timing = client.request_timed("POST", "/v1/partition/schemas/import", import_payload(namespaced))
        import_report = {"response": imported, "timing": import_timing}
        dsn = runtime_config.require_postgres_dsn()
        for spec in case_specs:
            try:
                case = run_case(
                    client,
                    dsn,
                    runtime_job_address,
                    namespaced,
                    {**validated, "datasets_by_type": {data_type: next(item for item in namespaced["datasets"] if item["data_type"] == data_type) for data_type in DATA_TYPES}},
                    namespace,
                    spec,
                    cold_start_gate=not args.allow_warm_start,
                    cold_start_timeout=args.cold_start_timeout,
                    cold_start_poll=args.poll_seconds,
                    poll_seconds=args.poll_seconds,
                    task_timeout=args.task_timeout,
                )
            except Exception as exc:
                case = {
                    "case_id": spec.case_id,
                    "data_type": spec.data_type,
                    "grid_type": spec.grid_type,
                    "grid_level": validated["grid_levels"][spec.grid_type],
                    "partition_method": spec.partition_method,
                    "error": str(exc)[:1000],
                    "case_started_at": utc_now_iso(),
                }
            cases.append(json_safe(case))
            summary = build_summary(case, args.threshold_scope) if "attempt" in case else {
                "case_id": spec.case_id,
                "data_type": spec.data_type,
                "grid_type": spec.grid_type,
                "grid_level": validated["grid_levels"][spec.grid_type],
                "partition_method": spec.partition_method,
                "task_id": case.get("task_id"),
                "ray_job_id": None,
                "ray_job_status": None,
                "ray_job_available": False,
                "cold_start_gate_valid": False,
                "final_status": "error",
                "attempt_status": None,
                "partition_execution_sec": None,
                "end_to_end_sec": None,
                "queue_wait_sec": None,
                "api_submit_sec": None,
                "submit_to_ray_start_sec": None,
                "ray_start_to_attempt_start_sec": None,
                "ray_job_sec": None,
                "ray_job_lookup_sec": None,
                "ray_driver_internal_sec": None,
                "worker_internal_max_sec": None,
                "worker_internal_sum_sec": None,
                "workflow_internal_sec": None,
                "job_driver_internal_sec": None,
                "threshold_scope": args.threshold_scope,
                "partition_under_10_sec": False,
                "end_to_end_under_10_sec": False,
                "passed": False,
                "error": case.get("error"),
            }
            cases[-1]["summary"] = summary
            summaries.append(summary)
            result = case.get("attempt", {}).get("runner_result") if isinstance(case.get("attempt"), dict) else None
            if not isinstance(result, dict):
                result = case.get("final_task", {}).get("result") if isinstance(case.get("final_task"), dict) else None
            for row in timing_records(result if isinstance(result, dict) else None):
                phases.append({"case_id": spec.case_id, **row})
    except Exception as exc:
        fatal_error = str(exc)[:1000]
    finished_at = utc_now_iso()
    run_metadata = {
        "run_id": run_id,
        "started_at": run_started_at,
        "finished_at": finished_at,
        "well_id": validated.get("well_id") or manifest.get("well_id"),
        "well_definition": (
            f"每类数据一个完整 scene；光学为 {int((manifest.get('expected_band_counts') or {}).get('optical', args.expected_optical_bands))} 波段，"
            "雷达可含 VV/VH 多 asset，产品和碳卫星保留其完整数据单元；各 asset 保留独立 bbox，未宣称跨数据集地理坐标一致。"
        ),
        "source_manifest": str(Path(args.manifest).resolve()),
        "partition_executor": executor,
        "base_url": args.base_url,
        "ray_address": runtime_ray_address,
        "ray_job_address": runtime_job_address,
        "ray_head_ip": head_ip,
        "cold_start_gate": not args.allow_warm_start,
        "threshold_scope": args.threshold_scope,
        "case_ids": [spec.case_id for spec in case_specs],
        "grid_levels": validated.get("grid_levels"),
        "scene_mode": manifest.get("scene_mode"),
        "expected_band_counts": manifest.get("expected_band_counts"),
        "input_profile": validated.get("input_profile"),
        "source_verification": source_report,
        "import": locals().get("import_report"),
        "fatal_error": fatal_error,
        "case_count": len(cases),
    }
    (run_dir / "run_metadata.json").write_text(json.dumps(json_safe(run_metadata), ensure_ascii=False, indent=2), encoding="utf-8")
    (run_dir / "raw_cases.json").write_text(json.dumps(json_safe(cases), ensure_ascii=False, indent=2), encoding="utf-8")
    summary_fields = list(summaries[0].keys()) if summaries else ["case_id", "passed", "error"]
    write_csv(run_dir / "summary.csv", summaries, summary_fields)
    phase_fields = ["case_id", "path", "scope", "phase", "phase_count", "phase_elapsed_sec", "elapsed_sec", "started_at", "finished_at"]
    write_csv(run_dir / "phase_timings.csv", phases, phase_fields)
    write_report(
        run_dir / "TEST_REPORT.md",
        run_metadata=run_metadata,
        summaries=summaries,
        phases=phases,
        fatal_error=fatal_error,
    )
    passed = bool(summaries) and not fatal_error and len(summaries) == len(case_specs) and all(row.get("passed") for row in summaries)
    print(json.dumps({
        "status": "passed" if passed else "failed",
        "run_dir": str(run_dir),
        "report": str(run_dir / "TEST_REPORT.md"),
        "summary": str(run_dir / "summary.csv"),
        "phase_timings": str(run_dir / "phase_timings.csv"),
        "case_count": len(cases),
        "passed_case_count": sum(bool(row.get("passed")) for row in summaries),
        "fatal_error": fatal_error,
    }, ensure_ascii=False))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
