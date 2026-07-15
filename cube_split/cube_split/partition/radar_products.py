from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import rasterio
from pyproj import Transformer

RADAR_ASSET_EXTENSIONS = {".dat", ".tif", ".tiff"}


@dataclass(frozen=True)
class RadarAssetMetadata:
    scene_id: str
    band: str
    acq_time: datetime
    product_family: str
    sensor: str


_SENTINEL1_NAME_RE = re.compile(r"^(?:S1[_-]?)?(\d{8})[_-](VV|VH|HH|HV)$", re.IGNORECASE)


RADAR_ASSET_SCHEMA: list[dict[str, str]] = [
    {"field": "source_uri", "type": "string", "meaning": "雷达栅格源文件路径或 MinIO 对象 URL"},
    {"field": "scene_id", "type": "string", "meaning": "按采集日期归并的 Sentinel-1 场景标识"},
    {"field": "sensor", "type": "string", "meaning": "雷达传感器"},
    {"field": "product_family", "type": "string", "meaning": "雷达产品族"},
    {"field": "band / polarization", "type": "string", "meaning": "极化方式"},
    {"field": "acq_time", "type": "datetime", "meaning": "采集时间"},
    {"field": "resolution", "type": "float", "meaning": "源影像空间分辨率"},
    {"field": "bbox", "type": "float[4]", "meaning": "覆盖范围 bbox（WGS84: min_lon, min_lat, max_lon, max_lat）"},
    {"field": "corners", "type": "float[4][2]", "meaning": "覆盖范围四角点（WGS84 lon/lat）"},
    {"field": "asset_path", "type": "string", "meaning": "标准化 COG 输出路径"},
    {"field": "window_*", "type": "int", "meaning": "格网窗口偏移与尺寸"},
    {"field": "st_code", "type": "string", "meaning": "时空编码"},
]


def parse_radar_asset(path: Path) -> RadarAssetMetadata:
    match = _SENTINEL1_NAME_RE.match(path.stem)
    if not match:
        raise ValueError(f"Invalid Sentinel-1 radar filename: {path.name}")
    date_text, polarization = match.groups()
    acq_time = datetime.strptime(date_text, "%Y%m%d").replace(tzinfo=timezone.utc)
    return RadarAssetMetadata(
        scene_id=f"S1_{date_text}",
        band=polarization.lower(),
        acq_time=acq_time,
        product_family="sentinel1",
        sensor="sentinel1_sar",
    )


def radar_asset_paths(input_dir: Path) -> list[Path]:
    return sorted(
        path
        for path in input_dir.rglob("*")
        if path.is_file() and path.suffix.lower() in RADAR_ASSET_EXTENSIONS
    )


def build_radar_partition_schema(
    input_dir: Path,
    *,
    batch_id: str,
    batch_name: str | None = None,
    source_prefix: str | None = None,
    grid_type: str = "geohash",
    grid_level: int = 5,
    target_crs: str = "EPSG:4326",
    max_auto_retries: int = 1,
) -> dict[str, Any]:
    assets = [
        _schema_asset(path, input_dir=input_dir, source_prefix=source_prefix)
        for path in radar_asset_paths(input_dir)
    ]
    return {
        "batch_id": batch_id,
        "batch_name": batch_name or batch_id,
        "data_type": "radar",
        "source_system": "sentinel1_envi",
        "schema": RADAR_ASSET_SCHEMA,
        "assets": assets,
        "normalized_payload": {
            "batch_id": batch_id,
            "batch_name": batch_name or batch_id,
            "grid_type": grid_type,
            "grid_level": grid_level,
            "target_crs": target_crs,
            "selected_assets": assets,
        },
        "max_auto_retries": max_auto_retries,
    }


def _schema_asset(path: Path, *, input_dir: Path, source_prefix: str | None) -> dict[str, Any]:
    metadata = parse_radar_asset(path)
    corners = _dataset_corners_wgs84(path)
    bbox = _bbox_from_corners(corners)
    return {
        "source_uri": _schema_source_uri(path, input_dir=input_dir, source_prefix=source_prefix),
        "scene_id": metadata.scene_id,
        "acq_time": metadata.acq_time.isoformat().replace("+00:00", "Z"),
        "bands": [metadata.band],
        "band": metadata.band,
        "polarization": metadata.band,
        "resolution": _dataset_resolution(path),
        "bbox": bbox,
        "corners": corners,
        "sensor": metadata.sensor,
        "product_family": metadata.product_family,
    }


def _schema_source_uri(path: Path, *, input_dir: Path, source_prefix: str | None) -> str:
    if not source_prefix:
        return str(path.resolve())
    relative = path.relative_to(input_dir).as_posix()
    return f"{source_prefix.rstrip('/')}/{relative}"


def _dataset_corners_wgs84(path: Path) -> list[list[float]]:
    with rasterio.open(path) as ds:
        bounds = ds.bounds
        corners = [
            (bounds.left, bounds.top),
            (bounds.right, bounds.top),
            (bounds.right, bounds.bottom),
            (bounds.left, bounds.bottom),
        ]
        if ds.crs and ds.crs.to_string().upper() != "EPSG:4326":
            transformer = Transformer.from_crs(ds.crs, "EPSG:4326", always_xy=True)
            xs, ys = zip(*corners)
            lons, lats = transformer.transform(xs, ys)
            return [[round(float(lon), 6), round(float(lat), 6)] for lon, lat in zip(lons, lats)]
        return [[round(float(lon), 6), round(float(lat), 6)] for lon, lat in corners]


def _dataset_resolution(path: Path) -> float | None:
    with rasterio.open(path) as ds:
        x_res = abs(float(ds.transform.a))
        y_res = abs(float(ds.transform.e))
        resolution = max(x_res, y_res)
    return resolution if resolution > 0 else None


def _bbox_from_corners(corners: list[list[float]]) -> list[float]:
    lons = [point[0] for point in corners]
    lats = [point[1] for point in corners]
    return [
        round(min(lons), 6),
        round(min(lats), 6),
        round(max(lons), 6),
        round(max(lats), 6),
    ]
