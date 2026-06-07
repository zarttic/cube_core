# 载入系统数据读取 Schema 需求

更新时间：2026-06-05

本文档用于和载入子系统对接。交互边界是：我方只从载入系统读取已经载入的数据清单和
元数据信息；载入系统不需要调用我方剖分、批次、质量检查或入库接口。后续剖分、质检、
入库和重试都由我方系统内部处理，和载入系统无关。

载入系统可以通过 API、消息、文件或数据库视图提供数据。传输方式可另行约定，但返回的
数据结构需要等价满足本文 schema。

## 1. 我方需要读取的信息

我方需要从载入系统读取四类数据：

| 数据类型 | data_type | 读取对象 | 说明 |
| --- | --- | --- | --- |
| 光学遥感 | `optical` | raster assets | GeoTIFF/COG 影像及其时空范围、波段、传感器信息。 |
| 信息产品 | `product` | raster assets | 年度/周期产品栅格及产品名称、年份、空间范围。 |
| 雷达影像 | `radar` | raster assets | SAR 栅格及极化、sidecar、空间范围。 |
| 碳卫星观测 | `carbon` | observations | OCO-2 等点/足迹观测及 CO2 值、质量标记、位置。 |

## 2. 通用响应结构

建议载入系统按数据集或批次返回。一个响应中可以包含一个数据集，也可以分页返回多个
数据集；字段命名建议保持如下结构。

```json
{
  "schema_version": "1.0",
  "data_type": "optical",
  "dataset_id": "shandong_optical_mosaic",
  "dataset_name": "山东光学镶嵌影像",
  "source_system": "loader",
  "loaded_batch_id": "LOAD_20260605_001",
  "loaded_at": "2026-06-05T10:00:00Z",
  "updated_at": "2026-06-05T10:05:00Z",
  "assets": [],
  "observations": [],
  "page": {
    "next_cursor": null
  }
}
```

### 2.1 顶层字段

| 字段 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `schema_version` | 是 | string | 载入系统返回 schema 版本，当前建议 `1.0`。 |
| `data_type` | 是 | enum | `optical`、`product`、`radar`、`carbon`。 |
| `dataset_id` | 是 | string | 数据集稳定 ID。 |
| `dataset_name` | 建议 | string | 数据集中文或业务名称。 |
| `source_system` | 建议 | string | 载入系统或来源系统标识。 |
| `loaded_batch_id` | 建议 | string | 载入批次 ID。用于追踪数据来自哪次载入。 |
| `loaded_at` | 建议 | datetime | 首次载入完成时间，ISO8601。 |
| `updated_at` | 建议 | datetime | 元数据最近更新时间，ISO8601。 |
| `assets` | 栅格类必填 | array | `optical`、`product`、`radar` 使用。 |
| `observations` | 碳卫星必填 | array | `carbon` 使用。 |
| `page.next_cursor` | 可选 | string/null | 分页游标。无下一页时为 `null`。 |

## 3. 通用约定

### 3.1 URI

`source_uri` 必须是我方计算节点可访问的源数据地址。生产环境优先使用 `s3://`：

```text
s3://cube/cube/source/optocal/...
s3://cube/cube/source/product/...
s3://cube/cube/source/radar/...
s3://cube/cube/source/carbon/...
```

不要返回某台机器上的本地绝对路径，例如 `/home/...`、`/tmp/...`。

### 3.2 时间

时间字段使用 ISO8601，建议 UTC 并带 `Z`：

```text
2020-07-01T00:00:00Z
```

### 3.3 坐标

- 坐标统一使用 WGS84 经纬度。
- 点坐标使用 `[lon, lat]`。
- `lon` 范围为 `[-180, 180]`，`lat` 范围为 `[-90, 90]`。
- `bbox` 使用 `[min_lon, min_lat, max_lon, max_lat]`。
- `corners` 使用 4 个角点，建议顺序为左上、右上、右下、左下，并在同一数据集中保持一致。

### 3.4 分辨率

`resolution` 表示米，可以是数字 `30`，也可以是字符串 `"30m"`。解析后必须大于 0。

## 4. 栅格资产通用 Schema

`optical`、`product`、`radar` 都返回 `assets[]`。每个 asset 表示一个可读取的源栅格文件
或源栅格子资产。

| 字段 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `asset_id` | 是 | string | 载入系统内稳定资产 ID。重跑或分页返回时必须稳定。 |
| `source_uri` | 是 | string | 源文件 URI，生产建议 `s3://...`。 |
| `scene_id` | 是 | string | 场景 ID、产品场景 ID 或影像时间片 ID。 |
| `acq_time` | 是 | datetime | 采集时间、成像时间或产品代表时间。 |
| `sensor` | 是 | string | 传感器或数据来源，例如 `optical_mosaic`、`sentinel1_sar`。 |
| `product_family` | 是 | string | 产品族，例如 `sentinel1`、`product`、`other`。 |
| `bands` | 条件必填 | string[] | 波段列表。`bands`、`band`、`polarization`、`variable` 至少提供一个。 |
| `band` | 条件必填 | string | 单波段名。 |
| `resolution` | 是 | number/string | 空间分辨率，单位米。 |
| `bbox` | 建议 | number[4] | WGS84 覆盖范围。 |
| `corners` | 是 | number[4][2] | WGS84 四角点。 |
| `file_format` | 建议 | string | 例如 `GeoTIFF`、`COG`、`ENVI`、`NetCDF`。 |
| `size_bytes` | 建议 | integer | 源文件大小，便于调度和排障。 |
| `checksum` | 可选 | string | 文件校验值，例如 MD5、SHA256 或 ETag。 |
| `metadata` | 可选 | object | 载入系统额外元数据。 |

## 5. 光学遥感 optical

光学资产使用栅格通用 schema。建议：

- 每个单波段文件返回一条 asset，并用 `band` 标识波段。
- 多波段同文件可以返回一条 asset，并用 `bands` 列出所有波段。
- 同一场景不同波段应共享同一个 `scene_id`。

示例：

```json
{
  "schema_version": "1.0",
  "data_type": "optical",
  "dataset_id": "shandong_optical_mosaic",
  "dataset_name": "山东光学镶嵌影像",
  "source_system": "loader",
  "loaded_batch_id": "LOAD_OPTICAL_20260605_001",
  "loaded_at": "2026-06-05T10:00:00Z",
  "assets": [
    {
      "asset_id": "optical-2020q3-band4",
      "source_uri": "s3://cube/cube/source/optocal/Shandong_mosaic_2020Q3_sr_band4_cut/Shandong_mosaic_2020Q3_sr_band4_cut.tif",
      "scene_id": "Shandong_mosaic_2020Q3",
      "sensor": "optical_mosaic",
      "product_family": "other",
      "bands": ["sr_band4"],
      "band": "sr_band4",
      "acq_time": "2020-07-01T00:00:00Z",
      "resolution": 30,
      "bbox": [114.757377, 33.857041, 122.774914, 38.503521],
      "corners": [
        [114.757377, 38.503521],
        [122.774914, 38.503521],
        [122.774914, 33.857041],
        [114.757377, 33.857041]
      ],
      "file_format": "GeoTIFF"
    }
  ],
  "page": {
    "next_cursor": null
  }
}
```

## 6. 信息产品 product

产品资产在栅格通用字段基础上，需要额外返回产品身份字段。

| 字段 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `product_name` | 是 | string | 产品名称。 |
| `product_year` | 是 | integer | 产品年份。 |
| `product_period` | 可选 | string | 产品周期，例如 `yearly`、`monthly`、`quarterly`。 |
| `variable` | 建议 | string | 产品变量名。也可用 `band` 表示。 |
| `unit` | 可选 | string | 产品值单位。 |

示例：

```json
{
  "schema_version": "1.0",
  "data_type": "product",
  "dataset_id": "dianzhong_ecological_security",
  "dataset_name": "滇中生态安全评价数据集",
  "source_system": "loader",
  "loaded_batch_id": "LOAD_PRODUCT_20260605_001",
  "loaded_at": "2026-06-05T10:00:00Z",
  "assets": [
    {
      "asset_id": "dianzhong-eco-1980",
      "source_uri": "s3://cube/cube/source/product/1980-2020年滇中地区30米生态安全评价数据集（第一版）_1980年.tif",
      "scene_id": "dianzhong_ecological_security_1980",
      "product_name": "1980-2020年滇中地区30米生态安全评价数据集（第一版）",
      "product_year": 1980,
      "product_period": "yearly",
      "sensor": "data_product",
      "product_family": "product",
      "bands": ["product_value"],
      "band": "product_value",
      "variable": "product_value",
      "acq_time": "1980-01-01T00:00:00Z",
      "resolution": 30,
      "bbox": [100.644783, 23.28638, 104.829333, 27.061367],
      "corners": [
        [100.644783, 27.061367],
        [104.829333, 27.061367],
        [104.829333, 23.28638],
        [100.644783, 23.28638]
      ],
      "file_format": "GeoTIFF"
    }
  ],
  "page": {
    "next_cursor": null
  }
}
```

## 7. 雷达 radar

雷达资产在栅格通用字段基础上，需要明确极化和 sidecar 信息。

| 字段 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `polarization` | 是 | string | 极化方式，例如 `vv`、`vh`。 |
| `sidecars` | 条件必填 | array | 对 ENVI `.dat` 等需要 `.hdr` 的格式，必须列出 sidecar。 |
| `orbit_direction` | 可选 | string | `ASCENDING` 或 `DESCENDING`。 |
| `relative_orbit` | 可选 | integer/string | 相对轨道号。 |

`source_uri` 支持 `.dat`、`.tif`、`.tiff`。若 `source_uri` 是 `.dat`，请在 `sidecars`
中返回同名 `.hdr` 文件。

示例：

```json
{
  "schema_version": "1.0",
  "data_type": "radar",
  "dataset_id": "jiangsu_yangzhou_sentinel1",
  "dataset_name": "江苏扬州 Sentinel-1 2018-2020 夏季影像",
  "source_system": "loader",
  "loaded_batch_id": "LOAD_RADAR_20260605_001",
  "loaded_at": "2026-06-05T10:00:00Z",
  "assets": [
    {
      "asset_id": "s1-20180615-vv",
      "source_uri": "s3://cube/cube/source/radar/yangzhou_sentinel1_2018_2020/20180615_VV.dat",
      "scene_id": "S1_20180615",
      "sensor": "sentinel1_sar",
      "product_family": "sentinel1",
      "bands": ["vv"],
      "band": "vv",
      "polarization": "vv",
      "acq_time": "2018-06-15T00:00:00Z",
      "resolution": 10,
      "bbox": [119.240841, 32.26987, 119.490233, 32.640053],
      "corners": [
        [119.249917, 32.640053],
        [119.490233, 32.635514],
        [119.48019, 32.26987],
        [119.240841, 32.274346]
      ],
      "file_format": "ENVI",
      "sidecars": [
        {
          "role": "hdr",
          "uri": "s3://cube/cube/source/radar/yangzhou_sentinel1_2018_2020/20180615_VV.hdr"
        }
      ]
    }
  ],
  "page": {
    "next_cursor": null
  }
}
```

## 8. 碳卫星 carbon

碳卫星返回 `observations[]`。每条 observation 表示一个可剖分的观测点或观测足迹。

| 字段 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `observation_id` | 是 | string | 观测唯一 ID。 |
| `source_uri` | 是 | string | 来源文件 URI，例如 OCO-2 NetCDF 的 `s3://` URI。 |
| `source_index` | 建议 | integer | 观测在源文件中的行号或索引。 |
| `acq_time` | 是 | datetime | 观测时间。 |
| `sensor` | 是 | string | 例如 `oco2`。 |
| `product_family` | 是 | string | 例如 `xco2`。 |
| `product_type` | 建议 | string | 例如 `xco2`。 |
| `lon` / `lat` | 条件必填 | number | 观测中心点。也可用 `center_lon` / `center_lat`。 |
| `xco2` | 建议 | number | 柱平均 CO2 浓度。 |
| `quality_flag` | 建议 | string/int | 质量标记。 |
| `resolution` | 是 | number/string | 观测足迹或等效空间分辨率。 |
| `corners` | 可选 | number[4][2] | 观测足迹四角点。 |
| `footprint` / `footprint_geojson` | 可选 | object/string | 足迹几何。若无足迹，必须提供中心点。 |
| `metadata` | 可选 | object | 载入系统额外元数据。 |

示例：

```json
{
  "schema_version": "1.0",
  "data_type": "carbon",
  "dataset_id": "oco2_xco2",
  "dataset_name": "OCO-2 XCO2 观测",
  "source_system": "loader",
  "loaded_batch_id": "LOAD_CARBON_20260605_001",
  "loaded_at": "2026-06-05T10:00:00Z",
  "observations": [
    {
      "observation_id": "2020123100010671",
      "source_uri": "s3://cube/cube/source/carbon/oco2_LtCO2_201231_B11014Ar_220729012824s.nc4",
      "source_index": 0,
      "acq_time": "2020-12-31T00:01:06.700Z",
      "sensor": "oco2",
      "product_family": "xco2",
      "product_type": "xco2",
      "resolution": 10,
      "lon": -167.413,
      "lat": 41.1686,
      "xco2": 417.384,
      "quality_flag": "1"
    }
  ],
  "page": {
    "next_cursor": null
  }
}
```

## 9. 查询能力建议

载入系统如果提供 API，建议至少支持以下只读查询能力：

| 能力 | 参数 | 说明 |
| --- | --- | --- |
| 按数据类型查询 | `data_type` | 查询某一类已载入数据。 |
| 按数据集查询 | `dataset_id` | 查询指定数据集。 |
| 增量查询 | `updated_since` | 返回指定时间之后新增或更新的数据。 |
| 分页 | `limit`、`cursor` | 大批量资产必须支持分页。 |
| 资产详情 | `asset_id` 或 `observation_id` | 排障时按单个资产查询完整元数据。 |

API 路径不强制要求，可以由载入系统定义。关键是返回字段需要满足本文 schema。

## 10. 我方验收检查

我方读取载入数据后会做以下基础检查：

- `data_type` 必须是 `optical`、`product`、`radar`、`carbon` 之一。
- 栅格类必须返回非空 `assets`，碳卫星必须返回非空 `observations`。
- 每个栅格 asset 必须有 `asset_id`、`source_uri`、`scene_id`、`acq_time`、`sensor`、
  `product_family`、`resolution`、`corners` 和波段/变量信息。
- 每个碳 observation 必须有 `observation_id`、`source_uri`、`acq_time`、`sensor`、
  `product_family`、`resolution` 和位置字段。
- `source_uri` 必须可由我方计算节点读取。
- 时间必须能按 ISO8601 解析。
- 坐标必须在 WGS84 合法范围内。
- `resolution` 必须大于 0。
- 雷达 `.dat` 文件必须返回可访问的 `.hdr` sidecar。
