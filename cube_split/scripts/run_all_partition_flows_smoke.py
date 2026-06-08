#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Callable, Dict

import numpy as np
import rasterio
from cube_web.services import partition_runners
from minio import Minio
from minio.error import S3Error
from rasterio.transform import from_origin

from cube_split import runtime_config
from cube_split.partition.carbon import load_oco2_lite_observations

PartitionRunner = Callable[[Dict[str, Any]], Dict[str, Any]]


class AcceptanceCase:
    __slots__ = ("case_id", "label", "data_type", "grid_type", "grid_level", "require_quality", "aoi_readback")

    def __init__(
        self,
        case_id: str,
        label: str,
        data_type: str,
        grid_type: str,
        grid_level: int,
        *,
        require_quality: bool = False,
        aoi_readback: bool = False,
    ) -> None:
        self.case_id = case_id
        self.label = label
        self.data_type = data_type
        self.grid_type = grid_type
        self.grid_level = grid_level
        self.require_quality = require_quality
        self.aoi_readback = aoi_readback


ACCEPTANCE_CASES = (
    AcceptanceCase(
        "optical_s2",
        "small optical s2 partition",
        "optical",
        "s2",
        4,
        require_quality=True,
        aoi_readback=True,
    ),
    AcceptanceCase(
        "optical_mgrs",
        "small optical MGRS partition",
        "optical",
        "mgrs",
        2,
    ),
    AcceptanceCase(
        "optical_isea4h_level1",
        "optical ISEA4H level 1 entity partition",
        "optical",
        "isea4h",
        1,
    ),
    AcceptanceCase(
        "radar_s2",
        "small radar s2 partition",
        "radar",
        "s2",
        4,
    ),
    AcceptanceCase(
        "product_s2",
        "product s2 partition",
        "product",
        "s2",
        4,
        require_quality=True,
    ),
    AcceptanceCase(
        "carbon_satellite",
        "carbon satellite partition",
        "carbon",
        "isea4h",
        5,
        require_quality=True,
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run optical, radar, and product partition flows through s2, tile_matrix, and isea4h."
    )
    parser.add_argument("--work-dir", default="/tmp/cube_partition_flow_smoke", help="Local smoke input/output root")
    parser.add_argument("--summary-path", default="", help="Output summary JSON path")
    parser.add_argument("--run-id", default="", help="Unique run id; defaults to current timestamp")
    parser.add_argument("--mode", default="demo", choices=["demo", "test"], help="demo enables ingest; test skips ingest")
    parser.add_argument("--ray-parallelism", type=int, default=2, help="Small Ray parallelism for smoke runs")
    parser.add_argument("--chunk-size", type=int, default=1, help="Ray chunk size")
    parser.add_argument("--max-cells-per-asset", type=int, default=50, help="Safety limit for logical cover cells")
    parser.add_argument(
        "--keep-quality",
        action="store_true",
        help="Compatibility flag; quality checks are part of the fixed smoke acceptance.",
    )
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


def _prepare_carbon_input(work_dir: Path, prefix: str, client: Minio, bucket: str) -> tuple[Path, str]:
    sample = Path(__file__).resolve().parents[1] / "oco2_LtCO2_201231_B11014Ar_220729012824s(1).nc4"
    if not sample.exists():
        raise RuntimeError(f"Carbon source sample not found: {sample}")
    key = f"{prefix}/sources/carbon/oco2_LtCO2_201231_B11014Ar_220729012824s.nc4"
    source_uri = _upload(client, bucket, sample, key)

    observations = load_oco2_lite_observations(sample, max_observations=1)
    if not observations:
        raise RuntimeError(f"No carbon observations loaded from: {sample}")
    observation = observations[0]
    input_dir = work_dir / "carbon"
    input_dir.mkdir(parents=True, exist_ok=True)
    rows_path = input_dir / "carbon_observations.jsonl"
    rows_path.write_text(
        json.dumps(
            {
                "satellite": observation.satellite,
                "observation_id": observation.observation_id,
                "acq_time": observation.acq_time,
                "lon": observation.lon,
                "lat": observation.lat,
                "xco2": observation.xco2,
                "quality_flag": observation.quality_flag,
                "footprint": observation.footprint,
                "source_uri": source_uri,
                "source_index": 0,
                "metadata": {"source_format": "oco2_lite_nc4", "source_sample": sample.name},
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    return input_dir, source_uri


def _runner(data_type: str, mode: str) -> PartitionRunner:
    if data_type == "optical":
        return partition_runners._run_optical_partition_test if mode == "test" else partition_runners._run_optical_partition_demo
    if data_type == "radar":
        return partition_runners._run_radar_partition_test if mode == "test" else partition_runners._run_radar_partition_demo
    if data_type == "product":
        return partition_runners._run_product_partition_test if mode == "test" else partition_runners._run_product_partition_demo
    if data_type == "carbon":
        if mode == "test":
            return partition_runners._run_carbon_partition_test
        return lambda payload: partition_runners._run_carbon_partition_demo(payload=payload)
    raise ValueError(f"Unsupported data_type: {data_type}")


def _grid_level(grid_type: str) -> int:
    if grid_type == "s2":
        return 4
    if grid_type == "mgrs":
        return 2
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


def _pass_check(name: str, **details: Any) -> dict[str, Any]:
    return {"name": name, "status": "pass", **details}


def _validate_result(
    label: str,
    grid_type: str,
    mode: str,
    result: dict[str, Any],
    *,
    keep_quality: bool = False,
    data_type: str | None = None,
    require_quality: bool = False,
) -> dict[str, Any]:
    rows = _row_count(result)
    data_type = data_type or label.split(":", 1)[0]
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
    if mode == "demo" and data_type != "carbon":
        _require(ingest_enabled, f"{label}: demo mode should enable ingest")
        _require(metadata_backend == "postgres", f"{label}: expected postgres metadata backend")
        _require(asset_storage_backend == "minio", f"{label}: expected minio asset storage")
    if data_type == "carbon":
        _require(int(result.get("distinct_space_codes") or 0) > 0, f"{label}: expected carbon distinct_space_codes")
    elif grid_type == "isea4h":
        _require(asset_path.startswith("s3://"), f"{label}: entity tile asset_path should be s3:// after ingest")
        _require(source_asset_path.startswith("s3://"), f"{label}: entity source_asset_path should be s3://")
        _require(int(result.get("uploaded_tile_count") or 0) > 0 or mode == "test", f"{label}: expected uploaded entity tiles")
        _require(int(result.get("metadata_rows") or 0) > 0 or mode == "test", f"{label}: expected entity metadata rows")
    else:
        _require(asset_path.startswith("s3://"), f"{label}: logical asset_path should be s3://")
        if mode == "demo":
            ingest_stats = result.get("ingest_stats") or {}
            _require(bool(ingest_stats), f"{label}: expected ingest_stats")
    if keep_quality and require_quality:
        _require(str(result.get("quality_status") or ""), f"{label}: expected quality_status")
        _require(str(result.get("quality_report_id") or ""), f"{label}: expected quality_report_id")

    return {
        "label": label,
        "status": "pass",
        "checks": [
            _pass_check("rows", rows=rows),
            _pass_check("execution_engine", execution_engine=execution_engine),
            _pass_check("grid", grid_type=result.get("grid_type"), grid_level=result.get("grid_level")),
        ],
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


def _failed_item(case_id: str, label: str, exc: Exception | str, *, elapsed_sec: float | None = None) -> dict[str, Any]:
    item: dict[str, Any] = {
        "id": case_id,
        "label": label,
        "status": "fail",
        "checks": [],
        "error": str(exc),
    }
    if elapsed_sec is not None:
        item["elapsed_sec"] = round(elapsed_sec, 3)
    return item


def _partition_payload(
    case: AcceptanceCase,
    *,
    base_payload: dict[str, Any],
    selected_assets: dict[str, list[dict[str, Any]]],
    carbon_input_dir: Path,
    run_id: str,
) -> dict[str, Any]:
    if case.data_type == "carbon":
        return {
            "batch_id": f"smoke-{run_id}-{case.case_id}",
            "batch_name": case.label,
            "input_dir": str(carbon_input_dir),
            "partition_backend": "ray",
            "ray_address": base_payload["ray_address"],
            "ray_parallelism": base_payload["ray_parallelism"],
            "partition_workers": base_payload["ray_parallelism"],
            "partition_chunk_size": base_payload["chunk_size"],
            "grid_type": case.grid_type,
            "grid_level": case.grid_level,
            "time_granularity": "day",
            "selected_observations": [{"source_index": 0}],
        }
    payload = dict(base_payload)
    payload.update(
        {
            "batch_id": f"smoke-{run_id}-{case.case_id}",
            "job_id": f"smoke-{run_id}-{case.case_id}",
            "dataset": f"smoke_{run_id}_{case.data_type}",
            "sensor": selected_assets[case.data_type][0]["sensor"],
            "grid_type": case.grid_type,
            "grid_level": case.grid_level,
            "grid_level_mode": "manual",
            "selected_assets": selected_assets[case.data_type],
        }
    )
    if case.data_type == "product":
        payload["product_name"] = "smoke_product"
        payload["time_granularity"] = "year"
    return payload


def _run_partition_case(
    case: AcceptanceCase,
    *,
    mode: str,
    base_payload: dict[str, Any],
    selected_assets: dict[str, list[dict[str, Any]]],
    carbon_input_dir: Path,
    run_id: str,
    run_quality: bool,
) -> dict[str, Any]:
    start = time.perf_counter()
    try:
        payload = _partition_payload(
            case,
            base_payload=base_payload,
            selected_assets=selected_assets,
            carbon_input_dir=carbon_input_dir,
            run_id=run_id,
        )
        result = _runner(case.data_type, mode)(payload)
        item = _validate_result(
            case.case_id,
            case.grid_type,
            mode,
            result,
            keep_quality=run_quality,
            data_type=case.data_type,
            require_quality=case.require_quality,
        )
        item.update(
            {
                "id": case.case_id,
                "label": case.label,
                "data_type": case.data_type,
                "elapsed_sec": round(time.perf_counter() - start, 3),
            }
        )
        if run_quality and case.require_quality:
            item["checks"].append(
                _pass_check(
                    "quality",
                    quality_status=item.get("quality_status"),
                    quality_report_id=item.get("quality_report_id"),
                )
            )
        return item
    except Exception as exc:
        return _failed_item(case.case_id, case.label, exc, elapsed_sec=time.perf_counter() - start)


def _quality_acceptance(items: list[dict[str, Any]]) -> dict[str, Any]:
    required = [case for case in ACCEPTANCE_CASES if case.require_quality]
    checks: list[dict[str, Any]] = []
    errors: list[str] = []
    by_id = {str(item.get("id")): item for item in items}
    for case in required:
        item = by_id.get(case.case_id)
        if not item or item.get("status") != "pass":
            message = f"{case.case_id}: partition step did not pass"
            checks.append({"name": case.case_id, "status": "fail", "message": message})
            errors.append(message)
            continue
        quality_status = str(item.get("quality_status") or "")
        quality_report_id = str(item.get("quality_report_id") or "")
        if not quality_status or not quality_report_id:
            message = f"{case.case_id}: quality report metadata missing"
            checks.append({"name": case.case_id, "status": "fail", "message": message})
            errors.append(message)
            continue
        checks.append(
            _pass_check(
                case.case_id,
                quality_status=quality_status,
                quality_report_id=quality_report_id,
            )
        )
    return {
        "id": "quality_checks",
        "label": "quality checks",
        "status": "fail" if errors else "pass",
        "checks": checks,
        "error": "; ".join(errors) if errors else "",
    }


def _bbox_from_corners(corners: list[list[float]]) -> list[float]:
    lons = [float(point[0]) for point in corners]
    lats = [float(point[1]) for point in corners]
    return [min(lons), min(lats), max(lons), max(lats)]


def _aoi_readback_acceptance(
    items: list[dict[str, Any]],
    *,
    selected_assets: dict[str, list[dict[str, Any]]],
    base_payload: dict[str, Any],
    minio: runtime_config.MinioSettings,
    output_path: Path,
) -> dict[str, Any]:
    source = next((item for item in items if item.get("id") == "optical_s2"), None)
    if not source or source.get("status") != "pass":
        return _failed_item("aoi_readback", "AOI readback", "optical_s2 did not pass")
    start = time.perf_counter()
    try:
        from cube_split.read.aoi_reader import read_aoi_rgb

        first_row = _first_jsonl_row(str(source.get("rows_path") or ""))
        _require(bool(first_row), "AOI readback requires optical_s2 index rows")
        asset = selected_assets["optical"][0]
        band = str(first_row.get("band") or asset.get("band") or asset.get("bands", [""])[0])
        _require(bool(band), "AOI readback requires a band")
        read_path = read_aoi_rgb(
            bbox=_bbox_from_corners(asset["corners"]),
            time_bucket=str(first_row.get("time_bucket") or ""),
            bands=[band],
            output=str(output_path),
            postgres_dsn=str(base_payload["postgres_dsn"]),
            minio_endpoint=minio.endpoint,
            minio_access_key=minio.access_key,
            minio_secret_key=minio.secret_key,
            grid_type=str(source.get("grid_type") or "s2"),
            grid_level=int(source.get("grid_level") or 4),
            cover_mode="intersect",
            cube_version=str(base_payload["cube_version"]),
        )
        with rasterio.open(read_path) as ds:
            _require(ds.count >= 1, "AOI readback output should contain at least one band")
            _require(ds.width > 0 and ds.height > 0, "AOI readback output should be non-empty")
            checks = [
                _pass_check("output_exists", output=str(read_path)),
                _pass_check("raster_shape", width=ds.width, height=ds.height, bands=ds.count),
            ]
        return {
            "id": "aoi_readback",
            "label": "AOI readback",
            "status": "pass",
            "checks": checks,
            "rows_path": source.get("rows_path"),
            "output_path": str(read_path),
            "elapsed_sec": round(time.perf_counter() - start, 3),
        }
    except Exception as exc:
        return _failed_item("aoi_readback", "AOI readback", exc, elapsed_sec=time.perf_counter() - start)


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
    carbon_input_dir, carbon_source_uri = _prepare_carbon_input(work_dir, prefix, client, minio.bucket)

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
    for case in ACCEPTANCE_CASES:
        item = _run_partition_case(
            case,
            mode=args.mode,
            base_payload=base_payload,
            selected_assets=selected_assets,
            carbon_input_dir=carbon_input_dir,
            run_id=run_id,
            run_quality=True,
        )
        results.append(item)
        print(json.dumps(item, ensure_ascii=False))

    quality_item = _quality_acceptance(results)
    results.append(quality_item)
    print(json.dumps(quality_item, ensure_ascii=False))

    aoi_item = _aoi_readback_acceptance(
        results,
        selected_assets=selected_assets,
        base_payload=base_payload,
        minio=minio,
        output_path=work_dir / "aoi_readback" / "optical_s2_aoi.tif",
    )
    results.append(aoi_item)
    print(json.dumps(aoi_item, ensure_ascii=False))

    failed = [item for item in results if item.get("status") != "pass"]

    summary = {
        "run_id": run_id,
        "mode": args.mode,
        "status": "fail" if failed else "pass",
        "prefix": prefix,
        "work_dir": str(work_dir),
        "carbon_source_uri": carbon_source_uri,
        "summary_path": str(summary_path),
        "acceptance_count": len(results),
        "failed_count": len(failed),
        "acceptance": results,
        "results": results,
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
