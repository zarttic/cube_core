#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
from minio import Minio
from minio.error import S3Error
from rasterio.transform import from_origin

from cube_split import runtime_config
from cube_web.services import quality_checks
from cube_web.services import partition_runners


DATA_TYPES = ("optical", "radar", "product")
GRID_TYPES = ("geohash", "tile_matrix", "isea4h")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run optical, radar, and product partition flows through geohash, tile_matrix, and isea4h."
    )
    parser.add_argument("--work-dir", default="/tmp/cube_partition_flow_smoke", help="Local smoke input/output root")
    parser.add_argument("--summary-path", default="", help="Output summary JSON path")
    parser.add_argument("--run-id", default="", help="Unique run id; defaults to current timestamp")
    parser.add_argument("--mode", default="demo", choices=["demo", "test"], help="demo enables ingest; test skips ingest")
    parser.add_argument("--ray-parallelism", type=int, default=2, help="Small Ray parallelism for smoke runs")
    parser.add_argument("--chunk-size", type=int, default=1, help="Ray chunk size")
    parser.add_argument("--max-cells-per-asset", type=int, default=50, help="Safety limit for logical cover cells")
    parser.add_argument("--keep-quality", action="store_true", help="Run web quality checks after partition")
    return parser.parse_args()


def _corners(min_lon: float, min_lat: float, max_lon: float, max_lat: float) -> list[list[float]]:
    return [[min_lon, max_lat], [max_lon, max_lat], [max_lon, min_lat], [min_lon, min_lat]]


def _write_tif(path: Path, *, min_lon: float, max_lat: float, pixel_size: float, value: int) -> list[list[float]]:
    width = 96
    height = 96
    data = (np.arange(width * height, dtype=np.uint16).reshape(height, width) + value).astype(np.uint16)
    transform = from_origin(min_lon, max_lat, pixel_size, pixel_size)
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype=data.dtype,
        crs="EPSG:4326",
        transform=transform,
        nodata=0,
        tiled=True,
        compress="deflate",
    ) as ds:
        ds.write(data, 1)
    max_lon = min_lon + width * pixel_size
    min_lat = max_lat - height * pixel_size
    return _corners(min_lon, min_lat, max_lon, max_lat)


def _minio_client(settings: runtime_config.MinioSettings) -> Minio:
    if not settings.endpoint or not settings.access_key or not settings.secret_key:
        raise RuntimeError("MinIO endpoint/access_key/secret_key are required")
    return Minio(
        settings.endpoint,
        access_key=settings.access_key,
        secret_key=settings.secret_key,
        secure=settings.secure,
    )


def _ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def _upload(client: Minio, bucket: str, path: Path, key: str) -> str:
    try:
        stat = client.stat_object(bucket, key)
        if stat.size == path.stat().st_size:
            return f"s3://{bucket}/{key}"
    except S3Error as exc:
        if exc.code not in {"NoSuchKey", "NoSuchObject"}:
            raise
    client.fput_object(bucket, key, str(path))
    return f"s3://{bucket}/{key}"


def _prepare_assets(work_dir: Path, prefix: str, client: Minio, bucket: str) -> dict[str, list[dict[str, Any]]]:
    samples = {
        "optical": {
            "path": work_dir / "optical" / "Shandong_mosaic_2026Q1_sr_band1.tif",
            "scene_id": "smoke_optical_2026q1",
            "band": "sr_band1",
            "sensor": "optical_mosaic",
            "product_family": "other",
            "origin": (116.0, 40.0, 0.001),
            "value": 100,
        },
        "radar": {
            "path": work_dir / "radar" / "20260101_VV.tif",
            "scene_id": "S1_SMOKE_20260101",
            "band": "vv",
            "sensor": "sentinel1_sar",
            "product_family": "sentinel1",
            "origin": (119.35, 32.45, 0.001),
            "value": 200,
        },
        "product": {
            "path": work_dir / "product" / "smoke_product_2026.tif",
            "scene_id": "smoke_product_2026",
            "band": "product_value",
            "sensor": "data_product",
            "product_family": "product",
            "product_name": "smoke_product",
            "product_year": 2026,
            "origin": (100.6, 25.2, 0.001),
            "value": 300,
        },
    }
    selected: dict[str, list[dict[str, Any]]] = {}
    for data_type, spec in samples.items():
        min_lon, max_lat, pixel_size = spec["origin"]
        corners = _write_tif(
            spec["path"],
            min_lon=float(min_lon),
            max_lat=float(max_lat),
            pixel_size=float(pixel_size),
            value=int(spec["value"]),
        )
        key = f"{prefix}/sources/{data_type}/{spec['path'].name}"
        uri = _upload(client, bucket, spec["path"], key)
        asset = {
            "source_uri": uri,
            "scene_id": spec["scene_id"],
            "acq_time": "2026-01-01T00:00:00Z",
            "bands": [spec["band"]],
            "band": spec["band"],
            "corners": corners,
            "resolution": 30,
            "sensor": spec["sensor"],
            "product_family": spec["product_family"],
        }
        if data_type == "product":
            asset["product_name"] = spec["product_name"]
            asset["product_year"] = spec["product_year"]
        selected[data_type] = [asset]
    return selected


def _runner(data_type: str, mode: str):
    if data_type == "optical":
        return partition_runners._run_optical_partition_test if mode == "test" else partition_runners._run_optical_partition_demo
    if data_type == "radar":
        return partition_runners._run_radar_partition_test if mode == "test" else partition_runners._run_radar_partition_demo
    if data_type == "product":
        return partition_runners._run_product_partition_test if mode == "test" else partition_runners._run_product_partition_demo
    raise ValueError(f"Unsupported data_type: {data_type}")


def _grid_level(grid_type: str) -> int:
    if grid_type == "geohash":
        return 4
    if grid_type == "tile_matrix":
        return 6
    if grid_type == "isea4h":
        return 1
    raise ValueError(f"Unsupported grid_type: {grid_type}")


def _first_jsonl_row(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    rows_path = Path(path)
    if not rows_path.exists():
        return {}
    for line in rows_path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            return json.loads(line)
    return {}


def _row_count(result: dict[str, Any]) -> int:
    for key in ("total_index_rows", "rows", "entity_tile_count"):
        value = result.get(key)
        if isinstance(value, int):
            return value
    return 0


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _validate_result(
    label: str,
    grid_type: str,
    mode: str,
    result: dict[str, Any],
    *,
    keep_quality: bool = False,
) -> dict[str, Any]:
    rows = _row_count(result)
    data_type = label.split(":", 1)[0]
    first_row = _first_jsonl_row(str(result.get("rows_path") or result.get("output_path") or ""))
    asset_path = str(first_row.get("asset_path") or "")
    source_asset_path = str(first_row.get("source_asset_path") or "")
    ingest_enabled = bool(result.get("ingest_enabled"))
    execution_engine = str(result.get("execution_engine") or result.get("partition_backend_used") or "")
    metadata_backend = str(result.get("metadata_backend") or "")
    asset_storage_backend = str(result.get("asset_storage_backend") or "")

    _require(rows > 0, f"{label}: expected rows > 0")
    _require(execution_engine == "ray", f"{label}: expected execution_engine=ray, got {execution_engine!r}")
    _require(str(result.get("grid_type")) == grid_type, f"{label}: unexpected grid_type={result.get('grid_type')!r}")
    if mode == "demo":
        _require(ingest_enabled, f"{label}: demo mode should enable ingest")
        _require(metadata_backend == "postgres", f"{label}: expected postgres metadata backend")
        _require(asset_storage_backend == "minio", f"{label}: expected minio asset storage")
    if grid_type == "isea4h":
        _require(asset_path.startswith("s3://"), f"{label}: entity tile asset_path should be s3:// after ingest")
        _require(source_asset_path.startswith("s3://"), f"{label}: entity source_asset_path should be s3://")
        _require(int(result.get("uploaded_tile_count") or 0) > 0 or mode == "test", f"{label}: expected uploaded entity tiles")
        _require(int(result.get("metadata_rows") or 0) > 0 or mode == "test", f"{label}: expected entity metadata rows")
    else:
        _require(asset_path.startswith("s3://"), f"{label}: logical asset_path should be s3://")
        if mode == "demo":
            ingest_stats = result.get("ingest_stats") or {}
            _require(bool(ingest_stats), f"{label}: expected ingest_stats")
    if keep_quality and data_type in {"optical", "product"}:
        _require(str(result.get("quality_status") or ""), f"{label}: expected quality_status")
        _require(str(result.get("quality_report_id") or ""), f"{label}: expected quality_report_id")

    return {
        "label": label,
        "status": "ok",
        "rows": rows,
        "run_dir": result.get("run_dir"),
        "rows_path": result.get("rows_path") or result.get("output_path"),
        "execution_engine": execution_engine,
        "ingest_enabled": ingest_enabled,
        "metadata_backend": metadata_backend,
        "asset_storage_backend": asset_storage_backend,
        "grid_type": result.get("grid_type"),
        "grid_level": result.get("grid_level"),
        "first_space_code": first_row.get("space_code"),
        "asset_path_scheme": "s3" if asset_path.startswith("s3://") else asset_path,
        "source_asset_path_scheme": "s3" if source_asset_path.startswith("s3://") else source_asset_path,
        "uploaded_tile_count": result.get("uploaded_tile_count"),
        "metadata_rows": result.get("metadata_rows"),
        "ingest_stats": result.get("ingest_stats"),
        "total_elapsed_sec": result.get("total_elapsed_sec"),
        "quality_status": result.get("quality_status"),
        "quality_report_id": result.get("quality_report_id"),
    }


def main() -> None:
    args = parse_args()
    run_id = args.run_id or time.strftime("%Y%m%d%H%M%S")
    work_dir = Path(args.work_dir) / run_id
    summary_path = Path(args.summary_path) if args.summary_path else work_dir / "smoke_summary.json"
    minio = runtime_config.minio_settings()
    client = _minio_client(minio)
    _ensure_bucket(client, minio.bucket)
    prefix = f"cube/smoke/all_partition_flows/{run_id}".strip("/")
    selected_assets = _prepare_assets(work_dir, prefix, client, minio.bucket)

    if not args.keep_quality:
        quality_checks.run_optical_quality_check = None
        quality_checks.run_product_quality_check = None

    base_payload = {
        "input_dir": str(work_dir),
        "partition_backend": "ray",
        "ray_address": runtime_config.require_ray_address(),
        "ray_parallelism": args.ray_parallelism,
        "chunk_size": args.chunk_size,
        "metadata_backend": "postgres",
        "postgres_dsn": runtime_config.require_postgres_dsn(),
        "asset_storage_backend": "minio",
        "minio_endpoint": minio.endpoint,
        "minio_bucket": minio.bucket,
        "minio_prefix": f"{prefix}/outputs",
        "minio_secure": minio.secure,
        "minio_upload_workers": 2,
        "asset_version": f"asset-{run_id}",
        "cube_version": f"cube-{run_id}",
        "cover_mode": "intersect",
        "target_crs": "EPSG:4326",
        "time_granularity": "day",
        "max_cells_per_asset": args.max_cells_per_asset,
        "cog_workers": 1,
        "cog_overwrite": True,
        "partition_prefix_len": 2,
    }

    results: list[dict[str, Any]] = []
    for grid_type in GRID_TYPES:
        for data_type in DATA_TYPES:
            label = f"{data_type}:{grid_type}"
            payload = dict(base_payload)
            payload.update(
                {
                    "batch_id": f"smoke-{run_id}-{data_type}-{grid_type}",
                    "job_id": f"smoke-{run_id}-{data_type}-{grid_type}",
                    "dataset": f"smoke_{run_id}_{data_type}",
                    "sensor": selected_assets[data_type][0]["sensor"],
                    "grid_type": grid_type,
                    "grid_level": _grid_level(grid_type),
                    "grid_level_mode": "manual",
                    "selected_assets": selected_assets[data_type],
                }
            )
            if data_type == "product":
                payload["product_name"] = "smoke_product"
                payload["time_granularity"] = "year"
            start = time.perf_counter()
            result = _runner(data_type, args.mode)(payload)
            item = _validate_result(label, grid_type, args.mode, result, keep_quality=args.keep_quality)
            item["elapsed_sec"] = round(time.perf_counter() - start, 3)
            results.append(item)
            print(json.dumps(item, ensure_ascii=False))

    summary = {
        "run_id": run_id,
        "mode": args.mode,
        "prefix": prefix,
        "work_dir": str(work_dir),
        "summary_path": str(summary_path),
        "results": results,
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
