from __future__ import annotations

import json
import os
import time
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from cube_web.services import quality_checks
from cube_web.services.config_store import optical_partition_defaults
from cube_web.services.quality_report_store import get_quality_report_store
from cube_web.services.quality_service import quality_args, repo_root


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


def _resolve_optical_demo_source(source_uri: str, input_dir: Path) -> Path:
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
        source = _resolve_optical_demo_source(str(asset.get("source_uri") or ""), input_dir)
        manifest_assets.append(
            {
                "data_type": "optical",
                "source_uri": str(source),
                "scene_id": str(asset.get("scene_id") or source.stem),
                "acq_time": str(asset.get("acq_time") or "1970-01-01T00:00:00Z"),
                "bands": asset.get("bands") or ([asset["band"]] if asset.get("band") else [source.stem]),
                "corners": asset.get("corners"),
                "sensor": str(asset.get("sensor") or "optical_mosaic"),
                "product_family": str(asset.get("product_family") or "other"),
            }
        )
    return manifest_assets


def _int_payload_value(payload: dict, key: str, default: int) -> int:
    value = payload.get(key, default)
    if value is None or value == "":
        value = default
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{key} must be an integer") from None


def _payload_with_defaults(payload: dict | None, defaults: dict) -> dict:
    result = dict(defaults)
    for key, value in (payload or {}).items():
        if value is not None and value != "":
            result[key] = value
    return result


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
    if warning.stem == f"{source.stem}_cog" and warning.suffix.lower() == source.suffix.lower():
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


def _run_carbon_partition_demo() -> dict:
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
        ray_address=str(os.environ.get("CUBE_WEB_RAY_ADDRESS", "")),
        ray_parallelism=workers,
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
    return {
        "status": "completed",
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
        "partition_backend": result["partition_backend_used"],
        "execution_engine": result["execution_engine"],
        "ray_address": result["ray_address"],
        "output_path": str(rows_path),
    }


def _run_carbon_partition_retry(payload: dict | None = None) -> dict:
    result = _run_carbon_partition_demo()
    result["mode"] = "partition_retry"
    result["retry"] = {
        "strategy": "full_request",
        "warning_check_names": [],
        "warning_asset_count": 0,
        "retried_asset_count": 0,
    }
    return result


def _run_product_partition_demo(payload: dict | None = None, mode: str = "partition_demo") -> dict:
    from cube_split.jobs.product_partition_job import run_product_partition

    payload = payload or {}
    root = _demo_run_dir("product")
    input_dir = Path(str(payload.get("input_dir") or (repo_root() / "cube_split" / "data" / "product"))).expanduser().resolve()
    if not input_dir.exists():
        raise FileNotFoundError(f"Product demo input_dir not found: {input_dir}")

    grid_type = str(payload.get("grid_type") or "geohash").lower()
    if grid_type not in {"geohash", "mgrs", "isea4h"}:
        raise ValueError("grid_type must be one of: geohash, mgrs, isea4h")
    grid_level = _int_payload_value(payload, "grid_level", 5)
    if grid_level <= 0:
        raise ValueError("grid_level must be greater than 0")

    args = SimpleNamespace(
        input_dir=str(input_dir),
        output_dir=str(root / "output"),
        cog_input_dir=str(root / "cog"),
        target_crs=str(payload.get("target_crs") or "EPSG:4326"),
        grid_type=grid_type,
        grid_level=grid_level,
        cover_mode=str(payload.get("cover_mode") or "intersect"),
        max_cells_per_asset=_int_payload_value(payload, "max_cells_per_asset", 20000),
        partition_prefix_len=_int_payload_value(payload, "partition_prefix_len", 3),
        cog_overwrite=True,
        cog_workers=_int_payload_value(payload, "cog_workers", 2),
        partition_workers=_int_payload_value(payload, "partition_workers", 0),
        sample_mean=bool(payload.get("sample_mean", False)),
    )
    result = run_product_partition(args)
    result["mode"] = mode
    result["output_path"] = result.get("rows_path")
    result["workers"] = args.partition_workers
    result["execution_engine"] = "thread"
    if quality_checks.run_product_quality_check is not None:
        quality_report = quality_checks.run_product_quality_check(quality_args(str(result["run_dir"]), {"target_crs": args.target_crs}))
        quality_report = get_quality_report_store().upsert_report("product", result["run_dir"], quality_report)
        result["quality_status"] = quality_report.get("status")
        result["quality_report"] = quality_report
        result["quality_report_id"] = quality_report.get("report_id")
    return result


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


def _run_optical_partition_from_payload(payload: dict | None = None, mode: str = "partition_demo") -> dict:
    from cube_split.jobs.ray_logical_partition_job import run_logical_partition

    payload = _payload_with_defaults(payload, optical_partition_defaults())
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

    grid_type = str(payload.get("grid_type") or "geohash").lower()
    if grid_type not in {"geohash", "mgrs", "isea4h"}:
        raise ValueError("grid_type must be one of: geohash, mgrs, isea4h")
    grid_level = _int_payload_value(payload, "grid_level", 5)
    if grid_level <= 0:
        raise ValueError("grid_level must be greater than 0")

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
        grid_level=grid_level,
        cover_mode=str(payload.get("cover_mode") or "intersect"),
        time_granularity=str(payload.get("time_granularity") or "day"),
        max_cells_per_asset=_int_payload_value(payload, "max_cells_per_asset", 20000),
        ray_parallelism=_int_payload_value(payload, "ray_parallelism", 0),
        ray_address=str(payload.get("ray_address") or os.environ.get("CUBE_WEB_RAY_ADDRESS", "")),
        chunk_size=_int_payload_value(payload, "chunk_size", 0),
        partition_backend=str(payload.get("partition_backend") or "ray"),
        partition_prefix_len=_int_payload_value(payload, "partition_prefix_len", 3),
        timing_mode=False,
        skip_verify=False,
        sample_mean=bool(payload.get("sample_mean", False)),
    )
    report = run_logical_partition(args)
    run_dir = Path(report["run_dir"])
    rows_path = run_dir / "index_rows.jsonl"

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
        "workers": report.get("ray_parallelism"),
        "ingest_enabled": mode != "partition_test_no_ingest",
        **report,
    }
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
