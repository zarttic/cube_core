from __future__ import annotations

import json
import os
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

from cube_split import runtime_config

from cube_web.services import quality_checks
from cube_web.services.config_store import optical_ingest_defaults, optical_partition_defaults
from cube_web.services.partition_defaults import default_grid_level_for_grid_type, default_grid_level_from_assets
from cube_web.services.quality_report_store import get_quality_report_store
from cube_web.services.quality_service import quality_args, repo_root

DEFAULT_ENTITY_GRID_LEVEL = 4
DEFAULT_ENTITY_TEST_GRID_LEVEL = 4
PARTITION_GRID_TYPES = {"geohash", "mgrs", "tile_matrix", "isea4h"}


def _demo_run_dir(name: str) -> Path:
    run_dir = Path("/tmp") / "cube_web_partition_demo" / name / f"{time.strftime('run_%Y%m%d_%H%M%S')}_{time.perf_counter_ns()}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _demo_task_metadata(execution_engine: str) -> dict[str, str | None]:
    return {
        "demo_task_id": f"demo-{uuid4().hex[:12]}",
        "execution_engine": execution_engine,
        "ray_task_id": None,
    }


def _optical_demo_input_dir() -> Path:
    return repo_root() / "cube_split" / "data" / "optocal"


def _product_demo_input_dir() -> Path:
    return repo_root() / "cube_split" / "data" / "product"


def _radar_demo_input_dir() -> Path:
    return repo_root() / "cube_split" / "data" / "2018-2020年6月-8月江苏扬州10米Sentinel-1影像数据-01"


def _is_s3_uri(value: str) -> bool:
    return str(value or "").strip().lower().startswith("s3://")


def _parse_s3_source_name(source_uri: str) -> str:
    parsed = urlparse(source_uri)
    if parsed.scheme.lower() != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise ValueError(f"Invalid s3 source_uri: {source_uri}")
    return Path(parsed.path).name


def _minio_access_key(payload: dict | None = None) -> str:
    return runtime_config.minio_settings(payload).access_key


def _minio_secret_key(payload: dict | None = None) -> str:
    return runtime_config.minio_settings(payload).secret_key


def _resolve_optical_demo_source(source_uri: str, input_dir: Path) -> Path:
    if _is_s3_uri(source_uri):
        name = _parse_s3_source_name(source_uri)
        if Path(name).suffix.lower() not in {".tif", ".tiff"}:
            raise FileNotFoundError(f"Optical demo asset is not a TIF: {source_uri}")
        return Path(source_uri)
    source_path = Path(str(source_uri or "").strip())
    if not source_path:
        raise ValueError("selected_assets[].source_uri is required")
    if source_path.is_absolute():
        resolved = source_path.resolve()
    else:
        resolved = (input_dir / source_path).resolve()
    input_root = input_dir.resolve()
    if input_root != resolved and input_root not in resolved.parents:
        raise ValueError(f"Optical demo asset is outside input_dir: {source_uri}")
    if not resolved.exists() or resolved.suffix.lower() not in {".tif", ".tiff"}:
        raise FileNotFoundError(f"Optical demo asset not found: {resolved}")
    return resolved


def _resolve_product_demo_source(source_uri: str, input_dir: Path) -> Path:
    if _is_s3_uri(source_uri):
        name = _parse_s3_source_name(source_uri)
        if Path(name).suffix.lower() not in {".tif", ".tiff"}:
            raise FileNotFoundError(f"Product demo asset is not a TIF: {source_uri}")
        return Path(source_uri)
    source_path = Path(str(source_uri or "").strip())
    if not source_path:
        raise ValueError("selected_assets[].source_uri is required")
    if source_path.is_absolute():
        resolved = source_path.resolve()
    else:
        resolved = (input_dir / source_path).resolve()
    input_root = input_dir.resolve()
    if input_root != resolved and input_root not in resolved.parents:
        raise ValueError(f"Product demo asset is outside input_dir: {source_uri}")
    if not resolved.exists() or resolved.suffix.lower() not in {".tif", ".tiff"}:
        raise FileNotFoundError(f"Product demo asset not found: {resolved}")
    return resolved


def _resolve_radar_demo_source(source_uri: str, input_dir: Path) -> Path:
    if _is_s3_uri(source_uri):
        name = _parse_s3_source_name(source_uri)
        if Path(name).suffix.lower() not in {".dat", ".tif", ".tiff"}:
            raise FileNotFoundError(f"Radar demo asset is not a supported raster: {source_uri}")
        return Path(source_uri)
    source_path = Path(str(source_uri or "").strip())
    if not source_path:
        raise ValueError("selected_assets[].source_uri is required")
    if source_path.is_absolute():
        resolved = source_path.resolve()
    else:
        resolved = (input_dir / source_path).resolve()
    input_root = input_dir.resolve()
    if input_root != resolved and input_root not in resolved.parents:
        raise ValueError(f"Radar demo asset is outside input_dir: {source_uri}")
    if not resolved.exists() or resolved.suffix.lower() not in {".dat", ".tif", ".tiff"}:
        raise FileNotFoundError(f"Radar demo asset not found: {resolved}")
    return resolved


def _selected_optical_manifest_assets(payload: dict, input_dir: Path) -> list[dict]:
    selected_assets = payload.get("selected_assets") or []
    if not selected_assets:
        return []
    if not isinstance(selected_assets, list):
        raise ValueError("selected_assets must be an array")

    manifest_assets: list[dict] = []
    for idx, asset in enumerate(selected_assets, start=1):
        if not isinstance(asset, dict):
            raise ValueError(f"selected_assets[{idx}] must be an object")
        _validate_selected_raster_asset(asset, idx=idx, data_type="optical")
        source = _resolve_optical_demo_source(str(asset.get("source_uri") or ""), input_dir)
        band = _selected_asset_band(asset)
        manifest_assets.append(
            {
                "data_type": "optical",
                "source_uri": str(asset.get("source_uri") or source),
                "scene_id": str(asset["scene_id"]),
                "acq_time": str(asset["acq_time"]),
                "bands": asset.get("bands") or [band],
                "corners": asset.get("corners"),
                "resolution": asset.get("resolution"),
                "sensor": str(asset["sensor"]),
                "product_family": str(asset["product_family"]),
            }
        )
    return manifest_assets


def _selected_radar_input_dir(payload: dict, input_dir: Path, root: Path) -> Path:
    selected_assets = payload.get("selected_assets") or []
    if not selected_assets:
        return input_dir
    if not isinstance(selected_assets, list):
        raise ValueError("selected_assets must be an array")

    selected_input_dir = root / "input"
    selected_input_dir.mkdir(parents=True, exist_ok=True)
    for idx, asset in enumerate(selected_assets, start=1):
        if not isinstance(asset, dict):
            raise ValueError(f"selected_assets[{idx}] must be an object")
        if _is_s3_uri(str(asset.get("source_uri") or "")):
            continue
        source = _resolve_radar_demo_source(str(asset.get("source_uri") or ""), input_dir)
        target = selected_input_dir / source.name
        if not target.exists():
            target.symlink_to(source)
        hdr = source.with_suffix(".hdr")
        if hdr.exists():
            hdr_target = selected_input_dir / hdr.name
            if not hdr_target.exists():
                hdr_target.symlink_to(hdr)
    return selected_input_dir


def _selected_radar_manifest_assets(payload: dict, input_dir: Path, run_input_dir: Path) -> list[dict]:
    selected_assets = payload.get("selected_assets") or []
    if not selected_assets:
        return []
    if not isinstance(selected_assets, list):
        raise ValueError("selected_assets must be an array")

    manifest_assets: list[dict] = []
    for idx, asset in enumerate(selected_assets, start=1):
        if not isinstance(asset, dict):
            raise ValueError(f"selected_assets[{idx}] must be an object")
        _validate_selected_raster_asset(asset, idx=idx, data_type="radar")
        source = _resolve_radar_demo_source(str(asset.get("source_uri") or ""), input_dir)
        target = run_input_dir / source.name
        source_uri = str(asset.get("source_uri") or source)
        band = _selected_asset_band(asset)
        manifest_assets.append(
            {
                "data_type": "radar",
                "source_uri": source_uri if _is_s3_uri(source_uri) else str(target),
                "scene_id": str(asset["scene_id"]),
                "acq_time": str(asset["acq_time"]),
                "bands": asset.get("bands") or [band],
                "band": band,
                "polarization": str(asset.get("polarization") or band).lower(),
                "bbox": asset.get("bbox"),
                "corners": asset.get("corners"),
                "resolution": asset.get("resolution"),
                "sensor": str(asset["sensor"]),
                "product_family": str(asset["product_family"]),
            }
        )
    return manifest_assets


def _selected_product_input_dir(payload: dict, input_dir: Path, root: Path) -> Path:
    selected_assets = payload.get("selected_assets") or []
    if not selected_assets:
        return input_dir
    if not isinstance(selected_assets, list):
        raise ValueError("selected_assets must be an array")

    selected_input_dir = root / "input"
    selected_input_dir.mkdir(parents=True, exist_ok=True)
    for idx, asset in enumerate(selected_assets, start=1):
        if not isinstance(asset, dict):
            raise ValueError(f"selected_assets[{idx}] must be an object")
        if _is_s3_uri(str(asset.get("source_uri") or "")):
            continue
        source = _resolve_product_demo_source(str(asset.get("source_uri") or ""), input_dir)
        target = selected_input_dir / source.name
        if not target.exists():
            target.symlink_to(source)
    return selected_input_dir


def _selected_product_manifest_assets(payload: dict, input_dir: Path, run_input_dir: Path) -> list[dict]:
    selected_assets = payload.get("selected_assets") or []
    if not selected_assets:
        return []
    if not isinstance(selected_assets, list):
        raise ValueError("selected_assets must be an array")

    manifest_assets: list[dict] = []
    for idx, asset in enumerate(selected_assets, start=1):
        if not isinstance(asset, dict):
            raise ValueError(f"selected_assets[{idx}] must be an object")
        _validate_selected_raster_asset(asset, idx=idx, data_type="product")
        source = _resolve_product_demo_source(str(asset.get("source_uri") or ""), input_dir)
        target = run_input_dir / source.name
        product_year = asset.get("product_year")
        band = _selected_asset_band(asset)
        manifest_assets.append(
            {
                "data_type": "product",
                "source_uri": str(asset.get("source_uri") if _is_s3_uri(str(asset.get("source_uri") or "")) else target),
                "scene_id": str(asset["scene_id"]),
                "product_name": str(asset.get("product_name") or source.stem),
                "product_year": product_year,
                "acq_time": str(asset["acq_time"]),
                "band": band,
                "bbox": asset.get("bbox"),
                "corners": asset.get("corners"),
                "resolution": asset.get("resolution"),
                "sensor": str(asset["sensor"]),
                "product_family": str(asset["product_family"]),
            }
        )
    return manifest_assets


def _validate_selected_raster_asset(asset: dict, *, idx: int, data_type: str) -> None:
    prefix = f"selected_assets[{idx}]"
    for field in ("source_uri", "scene_id", "acq_time", "sensor", "product_family"):
        if not str(asset.get(field) or "").strip():
            raise ValueError(f"{prefix}.{field} is required for schema-first {data_type} partition")
    _selected_asset_band(asset)
    if not isinstance(asset.get("corners"), list) or len(asset["corners"]) != 4:
        raise ValueError(f"{prefix}.corners must contain 4 [lon, lat] points")
    for point in asset["corners"]:
        if not isinstance(point, (list, tuple)) or len(point) != 2:
            raise ValueError(f"{prefix}.corners must contain 4 [lon, lat] points")
        lon = _float_payload_value(point[0], f"{prefix}.corners")
        lat = _float_payload_value(point[1], f"{prefix}.corners")
        if not (-180.0 <= lon <= 180.0 and -90.0 <= lat <= 90.0):
            raise ValueError(f"{prefix}.corners coordinate out of range")
    if _float_payload_value(asset.get("resolution"), f"{prefix}.resolution") <= 0:
        raise ValueError(f"{prefix}.resolution must be greater than 0")


def _selected_asset_band(asset: dict) -> str:
    bands = asset.get("bands")
    if isinstance(bands, list):
        for item in bands:
            text = str(item).strip().lower()
            if text:
                return text
    elif bands is not None and str(bands).strip():
        return str(bands).strip().lower()
    for field in ("band", "polarization", "variable"):
        text = str(asset.get(field) or "").strip().lower()
        if text:
            return text
    raise ValueError("selected asset requires bands, band, polarization, or variable")


def _float_payload_value(value: Any, label: str) -> float:
    if isinstance(value, str):
        text = value.strip().lower()
        if text.endswith("m"):
            value = text[:-1].strip()
    try:
        return float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{label} must be numeric") from None


def _int_payload_value(payload: dict, key: str, default: int) -> int:
    value = payload.get(key, default)
    if value is None or value == "":
        value = default
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{key} must be an integer") from None


def _partition_grid_type(payload: dict) -> str:
    grid_type = str(payload.get("grid_type") or "geohash").lower()
    if grid_type not in PARTITION_GRID_TYPES:
        raise ValueError("grid_type must be one of: geohash, mgrs, tile_matrix, isea4h")
    return grid_type


def _payload_with_defaults(payload: dict | None, defaults: dict) -> dict:
    result = dict(defaults)
    for key, value in (payload or {}).items():
        if value is not None and value != "":
            result[key] = value
    return result


def _cancellation_check_from_payload(payload: dict | None) -> Any | None:
    payload = payload or {}
    return payload.get("_cancellation_check") or payload.get("cancellation_check")


def _env_text(name: str, default: str = "") -> str:
    return runtime_config.env_text(name, default)


def _ray_address() -> str:
    return runtime_config.require_ray_address()


def _postgres_dsn() -> str:
    return runtime_config.require_postgres_dsn()


def _minio_settings(payload: dict | None = None, ingest_payload: dict | None = None) -> runtime_config.MinioSettings:
    return runtime_config.minio_settings({**(ingest_payload or {}), **(payload or {})})


def _warn_checks_from_result(result: dict) -> list[dict]:
    report = result.get("quality_report") if isinstance(result, dict) else None
    if not isinstance(report, dict):
        return []
    checks = report.get("checks") or []
    if not isinstance(checks, list):
        return []
    return [check for check in checks if isinstance(check, dict) and check.get("status") == "WARN"]


def _warning_asset_paths(checks: list[dict]) -> set[str]:
    paths: set[str] = set()
    for check in checks:
        metrics = check.get("metrics") or {}
        if not isinstance(metrics, dict):
            continue
        for item in metrics.get("zero_assets") or []:
            if isinstance(item, dict) and item.get("path"):
                paths.add(str(item["path"]))
        for item in metrics.get("duplicates") or []:
            if isinstance(item, dict):
                paths.update(str(path) for path in item.get("asset_paths") or [] if path)
    return paths


def _asset_matches_warning_path(asset: dict, warning_path: str) -> bool:
    source_uri = str(asset.get("source_uri") or "")
    if not source_uri:
        return False
    warning = Path(warning_path)
    source = Path(source_uri)
    if source_uri == warning_path or source.name == warning.name:
        return True
    if warning.suffix.lower() == source.suffix.lower() and warning.stem == f"{source.stem}_cog":
        return True
    if warning.suffix.lower() == source.suffix.lower() and warning.stem.startswith(f"{source.stem}_") and warning.stem.endswith("_cog"):
        return True
    warning_parts = warning.parts
    source_parts = source.parts
    return len(source_parts) <= len(warning_parts) and tuple(warning_parts[-len(source_parts) :]) == tuple(source_parts)


def _retry_payload_for_warning_assets(payload: dict, warning_paths: set[str]) -> tuple[dict, int]:
    selected_assets = payload.get("selected_assets") or []
    if not warning_paths or not isinstance(selected_assets, list) or not selected_assets:
        return dict(payload), 0
    retry_assets = [
        asset
        for asset in selected_assets
        if isinstance(asset, dict) and any(_asset_matches_warning_path(asset, warning_path) for warning_path in warning_paths)
    ]
    if not retry_assets:
        return dict(payload), 0
    retry_payload = dict(payload)
    retry_payload["selected_assets"] = retry_assets
    return retry_payload, len(retry_assets)


def _run_optical_partition_retry(payload: dict | None = None) -> dict:
    payload = payload or {}
    request = payload.get("request") or {}
    if not isinstance(request, dict):
        raise ValueError("request must be an object")
    last_result = payload.get("last_result") or {}
    if not isinstance(last_result, dict):
        raise ValueError("last_result must be an object")

    request_payload = request.get("payload") or {}
    if not isinstance(request_payload, dict):
        raise ValueError("request.payload must be an object")
    warn_checks = _warn_checks_from_result(last_result)
    warning_paths = _warning_asset_paths(warn_checks)
    retry_payload, retried_asset_count = _retry_payload_for_warning_assets(request_payload, warning_paths)
    result = _run_optical_partition_from_payload(retry_payload, mode="partition_retry")
    result["retry"] = {
        "strategy": "warning_assets" if retried_asset_count else "full_request",
        "warning_check_names": [str(check.get("name")) for check in warn_checks],
        "warning_asset_count": len(warning_paths),
        "retried_asset_count": retried_asset_count,
    }
    return result


def _run_entity_partition_from_payload(payload: dict | None = None, mode: str = "partition_demo") -> dict:
    from cube_split.jobs.entity_partition_job import DEFAULT_TARGET_PIXELS_PER_HEX_EDGE, run_entity_partition

    raw_payload = payload or {}
    payload = _payload_with_defaults(payload, optical_partition_defaults())
    ingest_payload = _payload_with_defaults(raw_payload, optical_ingest_defaults())
    cancellation_check = _cancellation_check_from_payload(raw_payload)
    input_dir = Path(str(payload.get("input_dir") or _optical_demo_input_dir())).expanduser().resolve()
    if not input_dir.exists():
        raise FileNotFoundError(f"Entity demo input_dir not found: {input_dir}")

    root = _demo_run_dir("entity")
    output_root = root / "output"
    manifest_path = Path(str(payload.get("manifest_path") or "")).expanduser()
    manifest_assets = _selected_optical_manifest_assets(payload, input_dir)
    if manifest_assets:
        manifest_path = root / "selected_assets_manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "batch_id": payload.get("batch_id") or "frontend-entity-demo",
                    "batch_name": payload.get("batch_name") or "frontend entity demo",
                    "data_type": "optical",
                    "assets": manifest_assets,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    elif not str(manifest_path):
        default_manifest = input_dir / "manifest.json"
        manifest_path = default_manifest if default_manifest.exists() else Path("")

    default_grid_level = DEFAULT_ENTITY_TEST_GRID_LEVEL if mode == "partition_test_no_ingest" else DEFAULT_ENTITY_GRID_LEVEL
    default_grid_level = default_grid_level_from_assets(
        raw_payload.get("selected_assets") if isinstance(raw_payload.get("selected_assets"), list) else [],
        grid_type="isea4h",
        fallback=default_grid_level,
    )
    grid_level = _int_payload_value(raw_payload, "grid_level", default_grid_level)
    if grid_level < 0:
        raise ValueError("grid_level must be greater than or equal to 0")
    grid_level_mode = str(raw_payload.get("grid_level_mode") or "manual").lower()
    if grid_level_mode not in {"auto", "manual"}:
        raise ValueError("grid_level_mode must be one of: auto, manual")
    entity_grid_level = 0 if grid_level_mode == "auto" else grid_level

    args = SimpleNamespace(
        input_dir=str(input_dir),
        manifest_path=(str(manifest_path.resolve()) if str(manifest_path) else ""),
        product_family=str(payload.get("product_family") or "auto"),
        output_dir=str(output_root),
        cog_input_dir=str(root / "cog"),
        cog_overwrite=True,
        cog_workers=_int_payload_value(payload, "cog_workers", 2),
        cog_compress=str(payload.get("cog_compress") or "LZW"),
        cog_predictor=_int_payload_value(payload, "cog_predictor", 2),
        cog_level=_int_payload_value(payload, "cog_level", 0),
        cog_num_threads=str(payload.get("cog_num_threads") or "ALL_CPUS"),
        target_crs=str(payload.get("target_crs") or "EPSG:4326"),
        grid_level=entity_grid_level,
        target_pixels_per_hex_edge=_int_payload_value(payload, "target_pixels_per_hex_edge", DEFAULT_TARGET_PIXELS_PER_HEX_EDGE),
        cover_mode=str(payload.get("cover_mode") or "intersect"),
        time_granularity=str(payload.get("time_granularity") or "day"),
        max_cells_per_asset=_int_payload_value(payload, "max_cells_per_asset", 20000),
        partition_prefix_len=_int_payload_value(payload, "partition_prefix_len", 3),
        ray_parallelism=_int_payload_value(payload, "ray_parallelism", 0),
        ray_address=str(payload.get("ray_address") or _ray_address()),
        chunk_size=_int_payload_value(payload, "chunk_size", 0),
        partition_backend=str(payload.get("partition_backend") or "ray"),
        job_id=str(payload.get("job_id") or payload.get("batch_id") or ""),
        dataset=str(ingest_payload.get("dataset") or "demo_optical"),
        sensor=str(ingest_payload.get("sensor") or "optical_mosaic"),
        asset_version=str(ingest_payload.get("asset_version") or "v1"),
        cube_version=str(ingest_payload.get("cube_version") or "v1"),
        metadata_backend=str(ingest_payload.get("metadata_backend") or "none"),
        postgres_dsn=str(payload.get("postgres_dsn") or _postgres_dsn()),
        asset_storage_backend=str(ingest_payload.get("asset_storage_backend") or "local"),
        minio_endpoint=_minio_settings(payload, ingest_payload).endpoint,
        minio_access_key=_minio_access_key(payload),
        minio_secret_key=_minio_secret_key(payload),
        minio_bucket=_minio_settings(payload, ingest_payload).bucket,
        minio_prefix=str(payload.get("minio_prefix") or ingest_payload.get("minio_prefix") or "cube/entity"),
        minio_secure=bool(payload.get("minio_secure", ingest_payload.get("minio_secure", False))),
        minio_upload_workers=_int_payload_value(ingest_payload, "minio_upload_workers", 8),
        ingest_enabled=(False if mode == "partition_test_no_ingest" else None),
        cancellation_check=cancellation_check,
    )
    report = run_entity_partition(args)
    run_dir = Path(report["run_dir"])
    rows_path = Path(str(report.get("rows_path") or run_dir / "entity_index_rows.jsonl"))
    response = {
        "status": "completed",
        "mode": mode,
        "data_type": "entity",
        **_demo_task_metadata(str(report.get("execution_engine") or args.partition_backend)),
        "demo_source": str(input_dir),
        "batch_id": payload.get("batch_id") or "",
        "batch_name": payload.get("batch_name") or "",
        "run_dir": str(run_dir),
        "rows_path": str(rows_path),
        "output_path": str(rows_path),
        "rows": int(report.get("total_index_rows", 0)),
        "workers": report.get("ray_parallelism", 0),
        "ingest_enabled": bool(report.get("ingest_enabled", False)),
        **report,
    }
    response["data_type"] = "entity"
    response["ingest_enabled"] = mode != "partition_test_no_ingest" and bool(report.get("ingest_enabled", False))
    if quality_checks.run_optical_quality_check is not None:
        quality_report = quality_checks.run_optical_quality_check(quality_args(str(run_dir), {"target_crs": args.target_crs}))
        quality_report = get_quality_report_store().upsert_report("optical", run_dir, quality_report)
        response["quality_status"] = quality_report.get("status")
        response["quality_report"] = quality_report
        response["quality_report_id"] = quality_report.get("report_id")
    return response


def _run_entity_partition_demo(payload: dict | None = None) -> dict:
    return _run_entity_partition_from_payload(payload, mode="partition_demo")


def _run_entity_partition_test(payload: dict | None = None) -> dict:
    return _run_entity_partition_from_payload(payload, mode="partition_test_no_ingest")


def _run_entity_partition_retry(payload: dict | None = None) -> dict:
    request = (payload or {}).get("request") or {}
    request_payload = request.get("payload") if isinstance(request, dict) else {}
    if not isinstance(request_payload, dict):
        request_payload = {}
    result = _run_entity_partition_from_payload(request_payload, mode="partition_retry")
    result["retry"] = {
        "strategy": "full_request",
        "warning_check_names": [],
        "warning_asset_count": 0,
        "retried_asset_count": 0,
    }
    return result


def _carbon_selected_source_indexes(payload: dict | None) -> tuple[int, ...] | None:
    selected_observations = (payload or {}).get("selected_observations") or []
    if not isinstance(selected_observations, list):
        return None
    indexes: list[int] = []
    for item in selected_observations:
        if not isinstance(item, dict):
            continue
        source_index = item.get("source_index")
        if source_index is None:
            continue
        try:
            indexes.append(int(source_index))
        except (TypeError, ValueError):
            continue
    if not indexes:
        return None
    return tuple(sorted(set(indexes)))


def _run_carbon_partition_demo(mode: str = "partition_demo", payload: dict | None = None) -> dict:
    from cube_split.jobs.carbon_partition_job import run_carbon_partition

    sample = repo_root() / "cube_split" / "oco2_LtCO2_201231_B11014Ar_220729012824s(1).nc4"
    if not sample.exists():
        raise RuntimeError(f"Carbon demo data not found: {sample}")

    root = _demo_run_dir("carbon")
    input_dir = root / "input"
    output_dir = root / "output"
    input_dir.mkdir(parents=True)
    (input_dir / sample.name).symlink_to(sample)
    workers = 4
    selected_source_indexes = _carbon_selected_source_indexes(payload)
    cancellation_check = _cancellation_check_from_payload(payload)
    args = SimpleNamespace(
        input_dir=str(input_dir),
        output_dir=str(output_dir),
        grid_type="isea4h",
        grid_level=5,
        time_granularity="day",
        product_type="xco2",
        max_observations=1000,
        partition_chunk_size=250,
        partition_workers=workers,
        partition_backend=str(os.environ.get("CUBE_WEB_CARBON_PARTITION_BACKEND", "ray")),
        ray_address=_ray_address(),
        ray_parallelism=workers,
        selected_source_indexes=selected_source_indexes,
        cancellation_check=cancellation_check,
    )
    start = time.perf_counter()
    result = run_carbon_partition(args)
    elapsed = time.perf_counter() - start
    rows_path = Path(result["rows_path"])
    space_codes: set[str] = set()
    quality_counts: dict[str, int] = {}
    with rows_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            row = json.loads(line)
            space_codes.add(row["space_code"])
            quality = str(row.get("quality_flag"))
            quality_counts[quality] = quality_counts.get(quality, 0) + 1
    response = {
        "status": "completed",
        "mode": mode,
        "data_type": "carbon_satellite",
        **_demo_task_metadata(str(result["execution_engine"])),
        "demo_source": sample.name,
        "run_dir": result["run_dir"],
        "rows": result["rows"],
        "distinct_space_codes": len(space_codes),
        "quality_counts": quality_counts,
        "elapsed_sec": round(elapsed, 3),
        "rows_per_sec": round(int(result["rows"]) / elapsed, 1) if elapsed > 0 else 0,
        "grid_type": result["grid_type"],
        "grid_level": result["grid_level"],
        "workers": workers,
        "batch_id": (payload or {}).get("batch_id") or "",
        "batch_name": (payload or {}).get("batch_name") or "",
        "selected_observation_count": len(selected_source_indexes or ()),
        "partition_backend": result["partition_backend_used"],
        "execution_engine": result["execution_engine"],
        "ray_address": result["ray_address"],
        "ingest_enabled": mode != "partition_test_no_ingest",
        "output_path": str(rows_path),
    }
    if quality_checks.run_carbon_quality_check is not None:
        quality_report = quality_checks.run_carbon_quality_check(quality_args(str(result["run_dir"]), {"target_crs": "EPSG:4326"}))
        quality_report = get_quality_report_store().upsert_report("carbon", result["run_dir"], quality_report)
        response["quality_status"] = quality_report.get("status")
        response["quality_report"] = quality_report
        response["quality_report_id"] = quality_report.get("report_id")
    return response


def _run_carbon_partition_test(payload: dict | None = None) -> dict:
    return _run_carbon_partition_demo(mode="partition_test_no_ingest", payload=payload)


def _run_carbon_partition_retry(payload: dict | None = None) -> dict:
    request = (payload or {}).get("request") or {}
    request_payload = request.get("payload") if isinstance(request, dict) else {}
    if not isinstance(request_payload, dict):
        request_payload = {}
    result = _run_carbon_partition_demo(payload=request_payload)
    result["mode"] = "partition_retry"
    result["retry"] = {
        "strategy": "full_request",
        "warning_check_names": [],
        "warning_asset_count": 0,
        "retried_asset_count": 0,
    }
    return result


def _run_product_partition_demo(payload: dict | None = None, mode: str = "partition_demo") -> dict:
    from cube_split.jobs.entity_partition_job import DEFAULT_TARGET_PIXELS_PER_HEX_EDGE, run_entity_partition
    from cube_split.jobs.product_partition_job import run_product_partition

    payload = payload or {}
    cancellation_check = _cancellation_check_from_payload(payload)
    root = _demo_run_dir("product")
    input_dir = Path(str(payload.get("input_dir") or _product_demo_input_dir())).expanduser().resolve()
    if not input_dir.exists():
        raise FileNotFoundError(f"Product demo input_dir not found: {input_dir}")
    run_input_dir = _selected_product_input_dir(payload, input_dir, root)
    manifest_path = Path(str(payload.get("manifest_path") or "")).expanduser()
    manifest_assets = _selected_product_manifest_assets(payload, input_dir, run_input_dir)
    if manifest_assets:
        manifest_path = root / "selected_assets_manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "batch_id": payload.get("batch_id") or "frontend-product-demo",
                    "batch_name": payload.get("batch_name") or "frontend product demo",
                    "data_type": "product",
                    "assets": manifest_assets,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    grid_type = _partition_grid_type(payload)
    grid_level_default = default_grid_level_from_assets(
        payload.get("selected_assets") if isinstance(payload.get("selected_assets"), list) else [],
        grid_type=grid_type,
        fallback=default_grid_level_for_grid_type(grid_type),
    )
    grid_level = _int_payload_value(payload, "grid_level", grid_level_default)
    if grid_level <= 0:
        raise ValueError("grid_level must be greater than 0")
    grid_level_mode = str(payload.get("grid_level_mode") or ("manual" if grid_type == "isea4h" else "auto")).lower()
    if grid_level_mode not in {"auto", "manual"}:
        raise ValueError("grid_level_mode must be one of: auto, manual")
    entity_grid_level = grid_level if grid_level_mode == "manual" else 0

    args = SimpleNamespace(
        input_dir=str(run_input_dir),
        manifest_path=(str(manifest_path.resolve()) if str(manifest_path) else ""),
        product_family=str(payload.get("product_family") or "product"),
        data_type="product",
        output_dir=str(root / "output"),
        cog_input_dir=str(root / "cog"),
        cog_overwrite=True,
        cog_workers=_int_payload_value(payload, "cog_workers", 2),
        cog_compress=str(payload.get("cog_compress") or "LZW"),
        cog_predictor=_int_payload_value(payload, "cog_predictor", 2),
        cog_level=_int_payload_value(payload, "cog_level", 0),
        cog_num_threads=str(payload.get("cog_num_threads") or "ALL_CPUS"),
        target_crs=str(payload.get("target_crs") or "EPSG:4326"),
        grid_type=grid_type,
        grid_level=entity_grid_level if grid_type == "isea4h" else grid_level,
        target_pixels_per_hex_edge=_int_payload_value(payload, "target_pixels_per_hex_edge", DEFAULT_TARGET_PIXELS_PER_HEX_EDGE),
        cover_mode=str(payload.get("cover_mode") or "intersect"),
        time_granularity=str(payload.get("time_granularity") or "year"),
        max_cells_per_asset=_int_payload_value(payload, "max_cells_per_asset", 20000),
        partition_prefix_len=_int_payload_value(payload, "partition_prefix_len", 3),
        partition_workers=_int_payload_value(payload, "partition_workers", 0),
        partition_backend=str(payload.get("partition_backend") or "ray"),
        ray_address=str(payload.get("ray_address") or _ray_address()),
        ray_parallelism=_int_payload_value(payload, "ray_parallelism", 0),
        chunk_size=_int_payload_value(payload, "chunk_size", 0),
        sample_mean=bool(payload.get("sample_mean", False)),
        job_id=str(payload.get("job_id") or payload.get("batch_id") or ""),
        dataset=str(payload.get("dataset") or "dianzhong_ecological_security"),
        product_name=str(payload.get("product_name") or "滇中地区30米生态安全评价数据集"),
        asset_version=str(payload.get("asset_version") or "v1"),
        cube_version=str(payload.get("cube_version") or "product_v1"),
        metadata_backend=str(payload.get("metadata_backend") or "postgres"),
        postgres_dsn=str(payload.get("postgres_dsn") or _postgres_dsn()),
        db_path=str(payload.get("db_path") or ""),
        asset_storage_backend=str(payload.get("asset_storage_backend") or "minio"),
        minio_endpoint=_minio_settings(payload).endpoint,
        minio_access_key=_minio_access_key(payload),
        minio_secret_key=_minio_secret_key(payload),
        minio_bucket=_minio_settings(payload).bucket,
        minio_prefix=str(payload.get("minio_prefix") or "cube/product"),
        minio_secure=bool(payload.get("minio_secure", False)),
        minio_upload_workers=_int_payload_value(payload, "minio_upload_workers", 8),
        cog_output_root=str(payload.get("cog_output_root") or root / "product_cog_store"),
        cog_materialize_mode=str(payload.get("cog_materialize_mode") or "copy"),
        ingest_enabled=(False if mode == "partition_test_no_ingest" else None),
        cancellation_check=cancellation_check,
    )
    result = run_entity_partition(args) if grid_type == "isea4h" else run_product_partition(args)
    result["mode"] = mode
    result["output_path"] = result.get("rows_path")
    result["workers"] = result.get("ray_parallelism") or args.partition_workers
    result["execution_engine"] = result.get("execution_engine") or result.get("partition_backend_used") or args.partition_backend
    result["batch_id"] = payload.get("batch_id") or ""
    result["batch_name"] = payload.get("batch_name") or ""
    result["selected_asset_count"] = len(payload.get("selected_assets") or [])
    result["ingest_enabled"] = mode != "partition_test_no_ingest" and bool(result.get("ingest_enabled", False))
    if quality_checks.run_product_quality_check is not None:
        quality_report = quality_checks.run_product_quality_check(quality_args(str(result["run_dir"]), {"target_crs": args.target_crs}))
        quality_report = get_quality_report_store().upsert_report("product", result["run_dir"], quality_report)
        result["quality_status"] = quality_report.get("status")
        result["quality_report"] = quality_report
        result["quality_report_id"] = quality_report.get("report_id")
    return result


def _run_product_partition_test(payload: dict | None = None) -> dict:
    return _run_product_partition_demo(payload, mode="partition_test_no_ingest")


def _run_product_partition_retry(payload: dict | None = None) -> dict:
    request = (payload or {}).get("request") or {}
    request_payload = request.get("payload") if isinstance(request, dict) else {}
    if not isinstance(request_payload, dict):
        request_payload = {}
    result = _run_product_partition_demo(request_payload, mode="partition_retry")
    result["retry"] = {
        "strategy": "full_request",
        "warning_check_names": [],
        "warning_asset_count": 0,
        "retried_asset_count": 0,
    }
    return result


def _run_radar_partition_demo(payload: dict | None = None, mode: str = "partition_demo") -> dict:
    from cube_split.jobs.entity_partition_job import DEFAULT_TARGET_PIXELS_PER_HEX_EDGE, run_entity_partition
    from cube_split.jobs.ray_logical_partition_job import run_logical_partition

    raw_payload = payload or {}
    payload = _payload_with_defaults(payload, optical_partition_defaults())
    if "partition_backend" not in raw_payload:
        payload["partition_backend"] = "thread"
    if "product_family" not in raw_payload:
        payload["product_family"] = "sentinel1"
    cancellation_check = _cancellation_check_from_payload(raw_payload)
    root = _demo_run_dir("radar")
    input_dir = Path(str(payload.get("input_dir") or _radar_demo_input_dir())).expanduser().resolve()
    if not input_dir.exists():
        raise FileNotFoundError(f"Radar demo input_dir not found: {input_dir}")
    run_input_dir = _selected_radar_input_dir(payload, input_dir, root)
    manifest_path = Path(str(payload.get("manifest_path") or "")).expanduser()
    manifest_assets = _selected_radar_manifest_assets(payload, input_dir, run_input_dir)
    if manifest_assets:
        manifest_path = root / "selected_assets_manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "batch_id": payload.get("batch_id") or "frontend-radar-demo",
                    "batch_name": payload.get("batch_name") or "frontend radar demo",
                    "data_type": "radar",
                    "assets": manifest_assets,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    elif not str(manifest_path):
        default_manifest = input_dir / "manifest.json"
        manifest_path = default_manifest if default_manifest.exists() else Path("")

    grid_type = _partition_grid_type(payload)
    grid_level_default = default_grid_level_from_assets(
        raw_payload.get("selected_assets") if isinstance(raw_payload.get("selected_assets"), list) else [],
        grid_type=grid_type,
        fallback=default_grid_level_for_grid_type(grid_type),
    )
    grid_level = _int_payload_value(raw_payload, "grid_level", grid_level_default)
    if grid_level <= 0:
        raise ValueError("grid_level must be greater than 0")
    grid_level_mode = str(raw_payload.get("grid_level_mode") or ("manual" if grid_type == "isea4h" else "auto")).lower()
    if grid_level_mode not in {"auto", "manual"}:
        raise ValueError("grid_level_mode must be one of: auto, manual")
    entity_grid_level = grid_level if grid_level_mode == "manual" else 0
    partition_backend = str(payload.get("partition_backend") or "thread")
    ray_address = str(payload.get("ray_address") or (_ray_address() if partition_backend in {"auto", "ray"} else ""))
    metadata_backend = str(payload.get("metadata_backend") or "none")
    postgres_dsn = str(payload.get("postgres_dsn") or (_postgres_dsn() if metadata_backend == "postgres" else ""))

    args = SimpleNamespace(
        input_dir=str(run_input_dir),
        manifest_path=(str(manifest_path.resolve()) if str(manifest_path) else ""),
        product_family=str(payload.get("product_family") or "sentinel1"),
        data_type="radar",
        output_dir=str(root / "output"),
        cog_input_dir=str(root / "cog"),
        cog_overwrite=True,
        cog_workers=_int_payload_value(payload, "cog_workers", 2),
        cog_compress=str(payload.get("cog_compress") or "LZW"),
        cog_predictor=_int_payload_value(payload, "cog_predictor", 2),
        cog_level=_int_payload_value(payload, "cog_level", 0),
        cog_num_threads=str(payload.get("cog_num_threads") or "ALL_CPUS"),
        target_crs=str(payload.get("target_crs") or "EPSG:4326"),
        grid_type=grid_type,
        grid_level=entity_grid_level if grid_type == "isea4h" else grid_level,
        target_pixels_per_hex_edge=_int_payload_value(payload, "target_pixels_per_hex_edge", DEFAULT_TARGET_PIXELS_PER_HEX_EDGE),
        cover_mode=str(payload.get("cover_mode") or "intersect"),
        time_granularity=str(payload.get("time_granularity") or "day"),
        max_cells_per_asset=_int_payload_value(payload, "max_cells_per_asset", 20000),
        ray_parallelism=_int_payload_value(payload, "ray_parallelism", 0),
        ray_address=ray_address,
        chunk_size=_int_payload_value(payload, "chunk_size", 0),
        partition_backend=partition_backend,
        partition_prefix_len=_int_payload_value(payload, "partition_prefix_len", 3),
        timing_mode=False,
        skip_verify=False,
        sample_mean=bool(payload.get("sample_mean", False)),
        job_id=str(payload.get("job_id") or payload.get("batch_id") or ""),
        dataset=str(payload.get("dataset") or "jiangsu_yangzhou_sentinel1"),
        sensor=str(payload.get("sensor") or "sentinel1_sar"),
        asset_version=str(payload.get("asset_version") or "v1"),
        cube_version=str(payload.get("cube_version") or "radar_v1"),
        quality_rule=str(payload.get("quality_rule") or "best_quality_wins"),
        metadata_backend=metadata_backend,
        postgres_dsn=postgres_dsn,
        db_path=str(payload.get("db_path") or ""),
        asset_storage_backend=str(payload.get("asset_storage_backend") or "local"),
        minio_endpoint=_minio_settings(payload).endpoint,
        minio_access_key=_minio_access_key(payload),
        minio_secret_key=_minio_secret_key(payload),
        minio_bucket=_minio_settings(payload).bucket,
        minio_prefix=str(payload.get("minio_prefix") or "cube/radar"),
        minio_secure=bool(payload.get("minio_secure", False)),
        minio_upload_workers=_int_payload_value(payload, "minio_upload_workers", 8),
        cog_output_root=str(payload.get("cog_output_root") or root / "radar_cog_store"),
        cog_materialize_mode=str(payload.get("cog_materialize_mode") or "copy"),
        ingest_enabled=False if mode == "partition_test_no_ingest" else None,
        cancellation_check=cancellation_check,
    )
    report = run_entity_partition(args) if grid_type == "isea4h" else run_logical_partition(args)
    run_dir = Path(report["run_dir"])
    rows_path = Path(str(report.get("rows_path") or run_dir / "index_rows.jsonl"))
    response = {
        "status": "completed",
        "mode": mode,
        "data_type": "radar",
        **_demo_task_metadata(str(report.get("execution_engine") or args.partition_backend)),
        "demo_source": str(input_dir),
        "batch_id": payload.get("batch_id") or "",
        "batch_name": payload.get("batch_name") or "",
        "run_dir": str(run_dir),
        "rows_path": str(rows_path),
        "output_path": str(rows_path),
        "rows": int(report.get("total_index_rows", 0)),
        "workers": report.get("ray_parallelism", 0),
        "selected_asset_count": len(raw_payload.get("selected_assets") or []),
        "ingest_enabled": bool(report.get("ingest_enabled", False)),
        **report,
    }
    response["data_type"] = "radar"
    response["ingest_enabled"] = mode != "partition_test_no_ingest" and bool(report.get("ingest_enabled", False))
    return response


def _run_radar_partition_test(payload: dict | None = None) -> dict:
    return _run_radar_partition_demo(payload, mode="partition_test_no_ingest")


def _run_radar_partition_retry(payload: dict | None = None) -> dict:
    request = (payload or {}).get("request") or {}
    request_payload = request.get("payload") if isinstance(request, dict) else {}
    if not isinstance(request_payload, dict):
        request_payload = {}
    result = _run_radar_partition_demo(request_payload, mode="partition_retry")
    result["retry"] = {
        "strategy": "full_request",
        "warning_check_names": [],
        "warning_asset_count": 0,
        "retried_asset_count": 0,
    }
    return result


def _run_optical_partition_from_payload(payload: dict | None = None, mode: str = "partition_demo") -> dict:
    from cube_split.jobs.entity_partition_job import DEFAULT_TARGET_PIXELS_PER_HEX_EDGE, run_entity_partition
    from cube_split.jobs.ray_logical_partition_job import run_logical_partition

    raw_payload = payload or {}
    payload = _payload_with_defaults(payload, optical_partition_defaults())
    ingest_payload = _payload_with_defaults(raw_payload, optical_ingest_defaults())
    cancellation_check = _cancellation_check_from_payload(raw_payload)
    input_dir = Path(str(payload.get("input_dir") or _optical_demo_input_dir())).expanduser().resolve()
    if not input_dir.exists():
        raise FileNotFoundError(f"Optical demo input_dir not found: {input_dir}")

    root = _demo_run_dir("optical")
    output_root = root / "output"
    manifest_path = Path(str(payload.get("manifest_path") or "")).expanduser()
    manifest_assets = _selected_optical_manifest_assets(payload, input_dir)
    if manifest_assets:
        manifest_path = root / "selected_assets_manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "batch_id": payload.get("batch_id") or "frontend-optical-demo",
                    "batch_name": payload.get("batch_name") or "frontend optical demo",
                    "data_type": "optical",
                    "assets": manifest_assets,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    elif not str(manifest_path):
        default_manifest = input_dir / "manifest.json"
        manifest_path = default_manifest if default_manifest.exists() else Path("")

    grid_type = _partition_grid_type(payload)
    grid_level_default = default_grid_level_for_grid_type(grid_type)
    if mode == "partition_test_no_ingest" and grid_type == "isea4h":
        grid_level_default = DEFAULT_ENTITY_TEST_GRID_LEVEL
    grid_level_default = default_grid_level_from_assets(
        raw_payload.get("selected_assets") if isinstance(raw_payload.get("selected_assets"), list) else [],
        grid_type=grid_type,
        fallback=grid_level_default,
    )
    grid_level = _int_payload_value(raw_payload, "grid_level", grid_level_default)
    if grid_level <= 0:
        raise ValueError("grid_level must be greater than 0")
    grid_level_mode = str(raw_payload.get("grid_level_mode") or ("manual" if grid_type == "isea4h" else "auto")).lower()
    if grid_level_mode not in {"auto", "manual"}:
        raise ValueError("grid_level_mode must be one of: auto, manual")
    entity_grid_level = grid_level if grid_level_mode == "manual" else 0

    args = SimpleNamespace(
        input_dir=str(input_dir),
        manifest_path=(str(manifest_path.resolve()) if str(manifest_path) else ""),
        product_family=str(payload.get("product_family") or "auto"),
        output_dir=str(output_root),
        cog_input_dir=str(root / "cog"),
        cog_overwrite=True,
        cog_workers=_int_payload_value(payload, "cog_workers", 2),
        cog_compress=str(payload.get("cog_compress") or "LZW"),
        cog_predictor=_int_payload_value(payload, "cog_predictor", 2),
        cog_level=_int_payload_value(payload, "cog_level", 0),
        cog_num_threads=str(payload.get("cog_num_threads") or "ALL_CPUS"),
        target_crs=str(payload.get("target_crs") or "EPSG:4326"),
        grid_type=grid_type,
        grid_level=entity_grid_level if grid_type == "isea4h" else grid_level,
        target_pixels_per_hex_edge=_int_payload_value(payload, "target_pixels_per_hex_edge", DEFAULT_TARGET_PIXELS_PER_HEX_EDGE),
        cover_mode=str(payload.get("cover_mode") or "intersect"),
        time_granularity=str(payload.get("time_granularity") or "day"),
        max_cells_per_asset=_int_payload_value(payload, "max_cells_per_asset", 20000),
        ray_parallelism=_int_payload_value(payload, "ray_parallelism", 0),
        ray_address=str(payload.get("ray_address") or _ray_address()),
        chunk_size=_int_payload_value(payload, "chunk_size", 0),
        partition_backend=str(payload.get("partition_backend") or "ray"),
        partition_prefix_len=_int_payload_value(payload, "partition_prefix_len", 3),
        timing_mode=False,
        skip_verify=False,
        sample_mean=bool(payload.get("sample_mean", False)),
        job_id=str(payload.get("job_id") or payload.get("batch_id") or ""),
        dataset=str(ingest_payload.get("dataset") or "demo_optical"),
        sensor=str(ingest_payload.get("sensor") or "optical_mosaic"),
        asset_version=str(ingest_payload.get("asset_version") or "v1"),
        cube_version=str(ingest_payload.get("cube_version") or "v1"),
        quality_rule=str(ingest_payload.get("quality_rule") or "best_quality_wins"),
        metadata_backend=str(ingest_payload.get("metadata_backend") or "none"),
        postgres_dsn=str(payload.get("postgres_dsn") or _postgres_dsn()),
        db_path=str(payload.get("db_path") or ""),
        asset_storage_backend=str(ingest_payload.get("asset_storage_backend") or "local"),
        minio_endpoint=_minio_settings(payload, ingest_payload).endpoint,
        minio_access_key=_minio_access_key(payload),
        minio_secret_key=_minio_secret_key(payload),
        minio_bucket=_minio_settings(payload, ingest_payload).bucket,
        minio_prefix=str(payload.get("minio_prefix") or ingest_payload.get("minio_prefix") or "cube/entity"),
        minio_secure=bool(payload.get("minio_secure", ingest_payload.get("minio_secure", False))),
        minio_upload_workers=_int_payload_value(ingest_payload, "minio_upload_workers", 8),
        cog_output_root=str(payload.get("cog_output_root") or root / "optical_cog_store"),
        cog_materialize_mode=str(payload.get("cog_materialize_mode") or "copy"),
        ingest_enabled=(False if mode == "partition_test_no_ingest" else None),
        cancellation_check=cancellation_check,
    )
    report = run_entity_partition(args) if grid_type == "isea4h" else run_logical_partition(args)
    run_dir = Path(report["run_dir"])
    rows_path = Path(str(report.get("rows_path") or run_dir / "index_rows.jsonl"))

    response = {
        "status": "completed",
        "mode": mode,
        "data_type": "optical",
        **_demo_task_metadata(str(report.get("execution_engine") or args.partition_backend)),
        "demo_source": str(input_dir),
        "batch_id": payload.get("batch_id") or "",
        "batch_name": payload.get("batch_name") or "",
        "run_dir": str(run_dir),
        "rows_path": str(rows_path),
        "output_path": str(rows_path),
        "rows": int(report.get("total_index_rows", 0)),
        "workers": report.get("ray_parallelism", 0),
        "ingest_enabled": bool(report.get("ingest_enabled", False)),
        **report,
    }
    response["ingest_enabled"] = mode != "partition_test_no_ingest" and bool(report.get("ingest_enabled", False))
    if quality_checks.run_optical_quality_check is not None:
        quality_report = quality_checks.run_optical_quality_check(quality_args(str(run_dir), {"target_crs": args.target_crs}))
        quality_report = get_quality_report_store().upsert_report("optical", run_dir, quality_report)
        response["quality_status"] = quality_report.get("status")
        response["quality_report"] = quality_report
        response["quality_report_id"] = quality_report.get("report_id")
    return response


def _run_optical_partition_demo(payload: dict | None = None) -> dict:
    return _run_optical_partition_from_payload(payload, mode="partition_demo")


def _run_optical_partition_test(payload: dict | None = None) -> dict:
    return _run_optical_partition_from_payload(payload, mode="partition_test_no_ingest")
