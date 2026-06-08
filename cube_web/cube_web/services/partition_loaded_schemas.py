from __future__ import annotations

import copy
from typing import Any

from cube_web.services.partition_defaults import DEFAULT_LOGICAL_GRID_LEVEL

MINIO_SOURCE_PREFIX = "s3://cube/cube/source"
OPTICAL_SOURCE_PREFIX = f"{MINIO_SOURCE_PREFIX}/optocal"
PRODUCT_SOURCE_PREFIX = f"{MINIO_SOURCE_PREFIX}/product"
RADAR_SOURCE_PREFIX = f"{MINIO_SOURCE_PREFIX}/radar"
CARBON_SOURCE_URI = f"{MINIO_SOURCE_PREFIX}/carbon/oco2_LtCO2_201231_B11014Ar_220729012824s.nc4"


OPTICAL_ASSET_SCHEMA = [
    {"field": "source_uri", "type": "string", "meaning": "光学栅格 MinIO 对象 URL"},
    {"field": "scene_id", "type": "string", "meaning": "场景标识"},
    {"field": "sensor", "type": "string", "meaning": "光学传感器"},
    {"field": "product_family", "type": "string", "meaning": "光学产品族"},
    {"field": "bands / band", "type": "string[] / string", "meaning": "波段"},
    {"field": "acq_time", "type": "datetime", "meaning": "采集时间"},
    {"field": "resolution", "type": "float", "meaning": "空间分辨率（米）"},
    {"field": "corners", "type": "float[4][2]", "meaning": "覆盖范围四角点（WGS84 lon/lat）"},
]

CARBON_OBSERVATION_SCHEMA = [
    {"field": "sounding_id / observation_id", "type": "string", "meaning": "观测唯一标识"},
    {"field": "time", "type": "datetime", "meaning": "观测时间"},
    {"field": "longitude / latitude", "type": "float", "meaning": "观测中心点"},
    {"field": "xco2", "type": "float", "meaning": "柱平均 CO2 浓度"},
    {"field": "xco2_quality_flag", "type": "int", "meaning": "质量标记"},
    {"field": "vertex_longitude / vertex_latitude", "type": "float[4]", "meaning": "观测足迹四角点"},
]

RADAR_ASSET_SCHEMA = [
    {"field": "source_uri", "type": "string", "meaning": "雷达栅格源文件路径或 MinIO 对象 URL"},
    {"field": "scene_id", "type": "string", "meaning": "Sentinel-1 场景标识"},
    {"field": "sensor", "type": "string", "meaning": "雷达传感器"},
    {"field": "product_family", "type": "string", "meaning": "雷达产品族"},
    {"field": "band / polarization", "type": "string", "meaning": "极化方式"},
    {"field": "acq_time", "type": "datetime", "meaning": "采集时间"},
    {"field": "resolution", "type": "float", "meaning": "空间分辨率（米）"},
    {"field": "bbox", "type": "float[4]", "meaning": "覆盖范围 bbox（WGS84）"},
    {"field": "corners", "type": "float[4][2]", "meaning": "覆盖范围四角点（WGS84 lon/lat）"},
]

PRODUCT_ASSET_SCHEMA = [
    {"field": "source_uri", "type": "string", "meaning": "产品栅格 MinIO 对象 URL"},
    {"field": "product_name", "type": "string", "meaning": "信息产品名称"},
    {"field": "product_year", "type": "int", "meaning": "产品年份"},
    {"field": "scene_id", "type": "string", "meaning": "产品场景标识"},
    {"field": "product_family", "type": "string", "meaning": "产品族"},
    {"field": "sensor", "type": "string", "meaning": "数据来源/产品传感器"},
    {"field": "band", "type": "string", "meaning": "产品值波段"},
    {"field": "acq_time", "type": "datetime", "meaning": "产品时间"},
    {"field": "resolution", "type": "float", "meaning": "空间分辨率（米）"},
    {"field": "target_crs", "type": "string", "meaning": "标准化目标参考系统"},
    {"field": "bbox", "type": "float[4]", "meaning": "产品覆盖范围 bbox（WGS84）"},
    {"field": "corners", "type": "float[4][2]", "meaning": "产品覆盖范围四角点（WGS84 lon/lat）"},
]


def standard_partition_schemas() -> list[dict[str, Any]]:
    return [
        *_optical_schemas(),
        *_carbon_schemas(),
        _radar_schema(),
        _product_schema(),
    ]


def ensure_standard_partition_schemas(store: Any) -> list[str]:
    inserted: list[str] = []
    for schema in standard_partition_schemas():
        batch_id = str(schema["batch_id"])
        if store.get_batch(batch_id) is not None:
            continue
        store.upsert_schema(schema)
        inserted.append(batch_id)
    return inserted


def _optical_schemas() -> list[dict[str, Any]]:
    corners_cut = [[114.757377, 38.503521], [122.774914, 38.503521], [122.774914, 33.857041], [114.757377, 33.857041]]
    corners_202008 = [[108.227954, 38.75], [128.544672, 38.75], [128.544672, 33.499766], [108.227954, 33.499766]]
    return [
        _raster_schema(
            batch_id="OPTICAL_BATCH_20260522_135546",
            batch_name="Shandong_mosaic_optocal",
            data_type="optical",
            source_system="standard_loaded_optical",
            schema=OPTICAL_ASSET_SCHEMA,
            assets=[
                _optical_asset("Shandong_mosaic_2015Q3_sr_band3_cut", "Shandong_mosaic_2015Q3", "2015-07-01T00:00:00Z", "sr_band3", corners_cut),
                _optical_asset("Shandong_mosaic_2017Q2_sr_band2_cut", "Shandong_mosaic_2017Q2", "2017-04-01T00:00:00Z", "sr_band2", corners_cut),
                _optical_asset("Shandong_mosaic_202008_sr_band2", "Shandong_mosaic_202008", "2020-08-01T00:00:00Z", "sr_band2", corners_202008),
                _optical_asset("Shandong_mosaic_2020Q3_sr_band4_cut", "Shandong_mosaic_2020Q3", "2020-07-01T00:00:00Z", "sr_band4", corners_cut),
            ],
        ),
        _raster_schema(
            batch_id="OPTICAL_BATCH_20260522_091000",
            batch_name="Shandong_mosaic_2020Q3_rgb_batch",
            data_type="optical",
            source_system="standard_loaded_optical",
            schema=OPTICAL_ASSET_SCHEMA,
            assets=[
                _optical_asset("Shandong_mosaic_2020Q3_sr_band2_cut", "Shandong_mosaic_2020Q3", "2020-07-01T00:00:00Z", "sr_band2", corners_cut),
                _optical_asset("Shandong_mosaic_2020Q3_sr_band3_cut", "Shandong_mosaic_2020Q3", "2020-07-01T00:00:00Z", "sr_band3", corners_cut),
                _optical_asset("Shandong_mosaic_2020Q3_sr_band4_cut", "Shandong_mosaic_2020Q3", "2020-07-01T00:00:00Z", "sr_band4", corners_cut),
            ],
        ),
        _raster_schema(
            batch_id="OPTICAL_BATCH_20260521_181500",
            batch_name="Shandong_mosaic_2017Q3_batch",
            data_type="optical",
            source_system="standard_loaded_optical",
            schema=OPTICAL_ASSET_SCHEMA,
            assets=[
                _optical_asset("Shandong_mosaic_2017Q3_sr_band3_cut", "Shandong_mosaic_2017Q3", "2017-07-01T00:00:00Z", "sr_band3", corners_cut),
                _optical_asset("Shandong_mosaic_2017Q3_sr_band4_cut", "Shandong_mosaic_2017Q3", "2017-07-01T00:00:00Z", "sr_band4", corners_cut),
            ],
        ),
    ]


def _carbon_schemas() -> list[dict[str, Any]]:
    return [
        _carbon_schema(
            batch_id="CARBON_BATCH_20201231_A",
            batch_name="OCO-2_XCO2_20201231_sample",
            observations=[
                _carbon_observation(0, "2020123100010671", "2020-12-31T00:01:06.700Z", -167.413, 41.1686, 417.384, "1"),
                _carbon_observation(1, "2020123100010673", "2020-12-31T00:01:06.700Z", -167.384, 41.1405, 418.669, "1"),
                _carbon_observation(2, "2020123100040904", "2020-12-31T00:04:09Z", -172.399, 50.5473, 414.811, "1"),
                _carbon_observation(3, "2020123100041037", "2020-12-31T00:04:10.300Z", -172.381, 50.5635, 413.485, "1"),
            ],
        ),
        _carbon_schema(
            batch_id="CARBON_BATCH_20201231_B",
            batch_name="OCO-2_XCO2_20201231_high_latitude",
            observations=[
                _carbon_observation(4, "2020123100041077", "2020-12-31T00:04:10.700Z", -172.392, 50.581, 413.266, "1"),
                _carbon_observation(5, "2020123100041078", "2020-12-31T00:04:10.700Z", -172.372, 50.5631, 414.058, "1"),
                _carbon_observation(6, "2020123100041108", "2020-12-31T00:04:11Z", -172.383, 50.5802, 415.684, "1"),
                _carbon_observation(7, "2020123100041138", "2020-12-31T00:04:11.300Z", -172.393, 50.5973, 414.073, "1"),
            ],
        ),
    ]


def _radar_schema() -> dict[str, Any]:
    corners = [[119.249917, 32.640053], [119.490233, 32.635514], [119.48019, 32.26987], [119.240841, 32.274346]]
    bbox = [119.240841, 32.26987, 119.490233, 32.640053]
    dates = [
        "20180603", "20180615", "20180627", "20180709", "20180721", "20180802", "20180814", "20180826",
        "20190604", "20190616", "20190628", "20190710", "20190722", "20190803", "20190815", "20190827",
        "20200604", "20200616", "20200628", "20200710", "20200722", "20200803", "20200815", "20200827",
    ]
    assets = [
        _radar_asset(date, polarization, bbox, corners)
        for date in dates
        for polarization in ("vh", "vv")
    ]
    return _raster_schema(
        batch_id="RADAR_BATCH_YANGZHOU_S1_2018_2020",
        batch_name="江苏扬州 Sentinel-1 10m 2018-2020 夏季",
        data_type="radar",
        source_system="standard_loaded_sentinel1",
        schema=RADAR_ASSET_SCHEMA,
        assets=assets,
        payload_extra={
            "product_family": "sentinel1",
            "sensor": "sentinel1_sar",
            "target_crs": "EPSG:4326",
        },
    )


def _product_schema() -> dict[str, Any]:
    corners = [
        [100.644783, 27.061367],
        [104.829333, 27.061367],
        [104.829333, 23.28638],
        [100.644783, 23.28638],
    ]
    bbox = [100.644783, 23.28638, 104.829333, 27.061367]
    assets = [_product_asset(year, bbox, corners) for year in (1980, 1990, 2000, 2010, 2020)]
    return _raster_schema(
        batch_id="PRODUCT_BATCH_DIANZHONG_1980_2020",
        batch_name="滇中生态安全评价_1980_2020",
        data_type="product",
        source_system="standard_loaded_product",
        schema=PRODUCT_ASSET_SCHEMA,
        assets=assets,
        payload_extra={
            "product_family": "product",
            "sensor": "data_product",
            "target_crs": "EPSG:4326",
        },
    )


def _raster_schema(
    *,
    batch_id: str,
    batch_name: str,
    data_type: str,
    source_system: str,
    schema: list[dict[str, str]],
    assets: list[dict[str, Any]],
    payload_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_payload = {
        "batch_id": batch_id,
        "batch_name": batch_name,
        "grid_type": "s2",
        "grid_level": DEFAULT_LOGICAL_GRID_LEVEL,
        "grid_level_mode": "auto",
        "target_crs": "EPSG:4326",
        "selected_assets": copy.deepcopy(assets),
    }
    if payload_extra:
        normalized_payload.update(payload_extra)
    return {
        "batch_id": batch_id,
        "batch_name": batch_name,
        "data_type": data_type,
        "source_system": source_system,
        "schema": copy.deepcopy(schema),
        "assets": copy.deepcopy(assets),
        "normalized_payload": normalized_payload,
        "priority": 0,
        "max_auto_retries": 1,
    }


def _carbon_schema(*, batch_id: str, batch_name: str, observations: list[dict[str, Any]]) -> dict[str, Any]:
    normalized_payload = {
        "batch_id": batch_id,
        "batch_name": batch_name,
        "product_type": "xco2",
        "source_uri": CARBON_SOURCE_URI,
        "selected_observations": copy.deepcopy(observations),
    }
    return {
        "batch_id": batch_id,
        "batch_name": batch_name,
        "data_type": "carbon",
        "source_system": "standard_loaded_oco2",
        "schema": copy.deepcopy(CARBON_OBSERVATION_SCHEMA),
        "observations": copy.deepcopy(observations),
        "normalized_payload": normalized_payload,
        "priority": 0,
        "max_auto_retries": 1,
    }


def _optical_asset(directory: str, scene_id: str, acq_time: str, band: str, corners: list[list[float]]) -> dict[str, Any]:
    source_uri = f"{OPTICAL_SOURCE_PREFIX}/{directory}/{directory}.tif"
    return {
        "source_uri": source_uri,
        "scene_id": scene_id,
        "acq_time": acq_time,
        "bands": [band],
        "band": band,
        "resolution": 30,
        "corners": copy.deepcopy(corners),
        "sensor": "optical_mosaic",
        "product_family": "other",
    }


def _carbon_observation(
    source_index: int,
    observation_id: str,
    acq_time: str,
    lon: float,
    lat: float,
    xco2: float,
    quality_flag: str,
) -> dict[str, Any]:
    return {
        "source_uri": CARBON_SOURCE_URI,
        "source_index": source_index,
        "observation_id": observation_id,
        "acq_time": acq_time,
        "lon": lon,
        "lat": lat,
        "xco2": xco2,
        "quality_flag": quality_flag,
        "resolution": 10,
        "sensor": "oco2",
        "product_family": "xco2",
    }


def _radar_asset(date: str, polarization: str, bbox: list[float], corners: list[list[float]]) -> dict[str, Any]:
    source_uri = f"{RADAR_SOURCE_PREFIX}/yangzhou_sentinel1_2018_2020/{date}_{polarization.upper()}.dat"
    return {
        "source_uri": source_uri,
        "scene_id": f"S1_{date}",
        "acq_time": f"{date[:4]}-{date[4:6]}-{date[6:8]}T00:00:00Z",
        "bands": [polarization],
        "band": polarization,
        "polarization": polarization,
        "resolution": 10,
        "bbox": copy.deepcopy(bbox),
        "corners": copy.deepcopy(corners),
        "sensor": "sentinel1_sar",
        "product_family": "sentinel1",
    }


def _product_asset(year: int, bbox: list[float], corners: list[list[float]]) -> dict[str, Any]:
    product_name = "1980-2020年滇中地区30米生态安全评价数据集（第一版）"
    source_uri = f"{PRODUCT_SOURCE_PREFIX}/{product_name}_{year}年.tif"
    return {
        "source_uri": source_uri,
        "scene_id": f"dianzhong_ecological_security_{year}",
        "product_name": product_name,
        "product_year": year,
        "band": "product_value",
        "bands": ["product_value"],
        "acq_time": f"{year}-01-01T00:00:00Z",
        "resolution": 30,
        "bbox": copy.deepcopy(bbox),
        "corners": copy.deepcopy(corners),
        "sensor": "data_product",
        "product_family": "product",
    }
