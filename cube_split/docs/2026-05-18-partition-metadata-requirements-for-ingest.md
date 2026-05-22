# 剖分元数据载入要求

日期：2026-05-18  
适用范围：载入系统向 `cube_split` 剖分系统交付的数据清单

## 1. 目标

载入系统不需要完成格网剖分，但必须为每个资产或观测交付足够支持剖分的结构化元数据。剖分系统基于这些元数据生成：

- 空间格网码：`space_code`
- 时空编码：`st_code`
- 时间桶：`time_bucket`
- 栅格窗口索引或点观测事实行

最低要求是每条记录都能回答四个问题：

- 数据在哪里：`source_uri`
- 数据是什么：`data_type`, `product_type`, `band` 或 `variable`
- 数据发生在什么时候：`acq_time`
- 数据覆盖哪里：`lon/lat`, `footprint` 或 `bbox`

## 2. 统一交付方式

建议载入系统输出 `manifest.jsonl`，每行描述一个可剖分单元。

- 栅格类数据：一行通常对应一个单波段资产、一个极化资产或一个产品变量资产。
- 点观测类数据：一行可以对应一个观测点，也可以交付原始 `.nc4/.h5/.csv/.jsonl` 文件并提供文件级元数据；若由载入系统展开观测，则必须包含观测级 `lon/lat/acq_time`。
- 多波段或多极化数据必须通过 `scene_id + band` 或 `scene_id + polarization` 关联。

所有字段名使用小写蛇形命名。时间统一使用 UTC ISO8601，例如 `2026-05-18T00:00:00Z`。空间范围统一输出为 `EPSG:4326` 经纬度 bbox，格式为 `[min_lon, min_lat, max_lon, max_lat]`。

## 3. 统一字段

### 3.1 必填字段

| 字段 | 类型 | 说明 |
|---|---|---|
| `data_type` | string | 数据类别：`carbon_satellite`、`radar`、`optical`、`product` |
| `asset_id` | string | 载入系统生成的唯一资产 ID |
| `source_uri` | string | 剖分系统可直接读取的数据地址，本地路径、MinIO/S3 URI 或 HTTP URI |
| `format` | string | 文件格式，例如 `tif`、`nc4`、`h5`、`csv`、`jsonl` |
| `acq_time` | string | 采集、成像、观测或产品代表时间，UTC ISO8601 |
| `time_granularity` | string | 时间粒度：`year`、`month`、`day`、`hour`、`minute` |
| `scene_id` | string | 景、轨道、产品批次或观测集合 ID |
| `satellite` | string | 卫星或平台名称；数据产品可填 `data_product` |
| `sensor` | string | 传感器名称；数据产品可填 `data_product` |
| `product_type` | string | 产品类型，例如 `xco2`、`surface_reflectance`、`backscatter`、`ecological_security` |
| `version` | string | 数据版本，例如 `v1` |

### 3.2 空间字段

栅格、影像和产品类数据必须满足以下两种方式之一：

| 字段 | 类型 | 说明 |
|---|---|---|
| `crs` | string | 源数据坐标系，例如 `EPSG:4326` |
| `bbox` | array | `EPSG:4326` 经纬度范围 `[min_lon, min_lat, max_lon, max_lat]` |

如果 `bbox` 没有显式提供，则文件必须可由 GDAL/rasterio 读取出 `crs`、`bounds`、`transform`、`width`、`height`。为了降低剖分侧成本，载入系统应优先显式提供 `bbox`。

点观测类数据必须提供：

| 字段 | 类型 | 说明 |
|---|---|---|
| `lon` | number | 中心点经度，`EPSG:4326` |
| `lat` | number | 中心点纬度，`EPSG:4326` |

有足迹的点观测建议额外提供：

| 字段 | 类型 | 说明 |
|---|---|---|
| `footprint` | array | 足迹多边形坐标，坐标顺序为 `[lon, lat]` |

### 3.3 数据变量字段

不同数据形态至少需要一个变量标识：

| 字段 | 适用数据 | 说明 |
|---|---|---|
| `band` | 光学、数据产品、通用栅格 | 波段名或产品值字段，例如 `B4`、`sr_band2`、`product_value` |
| `polarization` | 雷达 | 极化方式，例如 `VV`、`VH`、`HH`、`HV` |
| `variable` | 碳卫星、数据产品、NetCDF/HDF | 变量名，例如 `xco2`、`co2_profile`、`ndvi` |

## 4. 分类要求

### 4.1 碳卫星数据

适用于 OCO-2、TANSAT、GOSAT 等温室气体点观测或足迹观测。

必填字段：

| 字段 | 说明 |
|---|---|
| `data_type` | 固定为 `carbon_satellite` |
| `satellite` | 卫星名称，例如 `OCO2`、`TANSAT` |
| `sensor` | 传感器或产品来源，例如 `oco2_lite` |
| `product_type` | 当前建议为 `xco2` |
| `observation_id` | 观测 ID，OCO-2 可使用 `sounding_id` |
| `acq_time` | 观测时间 |
| `lon`, `lat` | 观测中心点 |
| `xco2` | XCO2 浓度值 |
| `source_uri` | 原始文件或观测来源 |

建议字段：

| 字段 | 说明 |
|---|---|
| `quality_flag` | 质量标识 |
| `footprint` | 观测足迹四角点或多边形 |
| `source_index` | 在源文件中的行号或数组下标 |
| `orbit` | 轨道号 |
| `retrieval_algorithm` | 反演算法或版本 |
| `metadata` | 其他产品级元数据对象 |

示例：

```json
{"data_type":"carbon_satellite","asset_id":"oco2_20200501_000001","scene_id":"oco2_LtCO2_20200501","source_uri":"s3://cube/carbon/oco2/oco2_LtCO2_20200501.nc4","format":"nc4","acq_time":"2020-05-01T03:12:30Z","time_granularity":"day","satellite":"OCO2","sensor":"oco2_lite","product_type":"xco2","observation_id":"2020050103123001","lon":116.391,"lat":39.907,"xco2":412.35,"quality_flag":"0","variable":"xco2","version":"v1"}
```

### 4.2 雷达数据

适用于 SAR 后向散射、干涉、相干性、形变等产品。当前 `cube_split` 尚未内置专门雷达适配器，因此载入系统必须提供更完整的结构化元数据，避免依赖文件名解析。

必填字段：

| 字段 | 说明 |
|---|---|
| `data_type` | 固定为 `radar` |
| `scene_id` | 雷达景 ID |
| `satellite` | 例如 `Sentinel-1A`、`Sentinel-1B`、`GF-3` |
| `sensor` | 例如 `SAR` |
| `product_type` | 例如 `backscatter`、`coherence`、`insar_displacement` |
| `acq_time` | 成像时间或产品代表时间 |
| `source_uri` | 单极化或单变量栅格资产 |
| `format` | 通常为 `tif` |
| `crs`, `bbox` | 空间参考和范围 |
| `polarization` | `VV`、`VH`、`HH` 或 `HV` |
| `mode` | 成像模式，例如 `IW`、`EW`、`SM` |
| `orbit_direction` | `ascending` 或 `descending` |

建议字段：

| 字段 | 说明 |
|---|---|
| `relative_orbit` | 相对轨道号 |
| `absolute_orbit` | 绝对轨道号 |
| `look_direction` | 视向 |
| `incidence_angle` | 入射角或入射角范围 |
| `resolution` | 空间分辨率，单位米 |
| `calibration_level` | 标定级别 |
| `terrain_correction` | 是否已地形校正 |
| `speckle_filter` | 是否已滤波及方法 |
| `nodata` | 无效值 |

示例：

```json
{"data_type":"radar","asset_id":"s1a_iw_vv_20240101","scene_id":"S1A_IW_20240101T102030","source_uri":"s3://cube/radar/s1/S1A_IW_20240101_VV.tif","format":"tif","acq_time":"2024-01-01T10:20:30Z","time_granularity":"day","crs":"EPSG:4326","bbox":[115.8,35.0,118.4,37.2],"satellite":"Sentinel-1A","sensor":"SAR","product_type":"backscatter","polarization":"VV","mode":"IW","orbit_direction":"ascending","resolution":10,"version":"v1"}
```

### 4.3 光学影像

适用于 Landsat、Sentinel-2、国产光学卫星以及光学镶嵌产品。

必填字段：

| 字段 | 说明 |
|---|---|
| `data_type` | 固定为 `optical` |
| `scene_id` | 光学景 ID 或镶嵌批次 ID |
| `satellite` | 例如 `Landsat-8`、`Sentinel-2A` |
| `sensor` | 例如 `OLI_TIRS`、`MSI` |
| `product_type` | 例如 `surface_reflectance`、`toa_reflectance`、`optical_mosaic` |
| `acq_time` | 成像时间或镶嵌代表时间 |
| `source_uri` | 单波段 TIF/COG |
| `format` | 通常为 `tif` |
| `crs`, `bbox` | 空间参考和范围 |
| `band` | 波段名，例如 `B2`、`B4`、`sr_band2` |
| `resolution` | 空间分辨率，单位米 |
| `processing_level` | 处理级别 |

建议字段：

| 字段 | 说明 |
|---|---|
| `cloud_cover` | 云量百分比 |
| `qa_band` | 质量波段名称或 URI |
| `sun_azimuth` | 太阳方位角 |
| `sun_elevation` | 太阳高度角 |
| `path_row` | Landsat WRS path/row |
| `tile_id` | Sentinel-2 tile ID |
| `nodata` | 无效值 |
| `scale`, `offset` | 数据缩放参数 |
| `unit` | 单位 |

示例：

```json
{"data_type":"optical","asset_id":"lc08_120030_20260204_b4","scene_id":"LC08_L2SP_120030_20260204_20260217_02_T1","source_uri":"s3://cube/optical/landsat/LC08_L2SP_120030_20260204_B4.tif","format":"tif","acq_time":"2026-02-04T00:00:00Z","time_granularity":"day","crs":"EPSG:4326","bbox":[116.1,35.2,118.5,37.1],"satellite":"Landsat-8","sensor":"OLI_TIRS","product_type":"surface_reflectance","band":"B4","resolution":30,"processing_level":"L2SP","cloud_cover":12.4,"version":"v1"}
```

### 4.4 数据产品

适用于生态安全评价、NDVI、土地覆盖、温度、降水、统计栅格等派生产品。

必填字段：

| 字段 | 说明 |
|---|---|
| `data_type` | 固定为 `product` |
| `product_name` | 产品名称 |
| `product_type` | 产品类型或主题，例如 `ecological_security`、`ndvi`、`landcover` |
| `scene_id` | 产品批次 ID，建议包含产品名和时间 |
| `acq_time` | 产品代表时间；年度产品使用当年 `YYYY-01-01T00:00:00Z` |
| `time_granularity` | 产品时间粒度，年度产品为 `year` |
| `source_uri` | 单变量栅格资产 |
| `format` | 通常为 `tif` |
| `crs`, `bbox` | 空间参考和范围 |
| `band` 或 `variable` | 产品变量名，例如 `product_value`、`ndvi` |

建议字段：

| 字段 | 说明 |
|---|---|
| `product_year` | 年度产品年份 |
| `unit` | 单位 |
| `value_range` | 有效值范围 |
| `classification_schema` | 分类表或等级说明 |
| `producer` | 生产单位 |
| `spatial_resolution` | 空间分辨率 |
| `temporal_resolution` | 时间分辨率 |
| `nodata` | 无效值 |

示例：

```json
{"data_type":"product","asset_id":"dianzhong_ecological_security_2020","scene_id":"dianzhong_ecological_security_2020","product_name":"1980-2020年滇中地区30米生态安全评价数据集（第一版）","source_uri":"s3://cube/product/dianzhong/2020.tif","format":"tif","acq_time":"2020-01-01T00:00:00Z","time_granularity":"year","crs":"EPSG:4326","bbox":[100.5,23.1,104.8,26.9],"satellite":"data_product","sensor":"data_product","product_type":"ecological_security","band":"product_value","product_year":2020,"spatial_resolution":30,"version":"v1"}
```

## 5. 质量和可读性要求

载入系统交付前应完成以下校验：

- `source_uri` 可被剖分系统所在环境直接读取。
- 时间字段可被解析为 UTC 时间。
- `bbox` 坐标顺序固定为 `[min_lon, min_lat, max_lon, max_lat]`。
- `bbox` 必须满足 `min_lon < max_lon` 且 `min_lat < max_lat`。
- 经纬度范围必须合法：经度 `[-180, 180]`，纬度 `[-90, 90]`。
- 栅格文件必须可读取 `crs`、`bounds`、`transform`、`width`、`height`。
- 栅格文件应明确 `nodata`、`dtype`，并尽量提供 `scale/offset/unit`。
- 点观测必须有 `lon/lat`；有足迹时应提供 `footprint`。
- 多波段、多极化、多变量资产必须能通过 `scene_id` 关联。
- 不允许只依赖文件名表达关键元数据；关键字段必须结构化输出。

## 6. 剖分侧生成字段

以下字段由 `cube_split` 和 `cube_encoder` 生成，载入系统不需要提供：

| 字段 | 说明 |
|---|---|
| `grid_type` | 剖分任务参数，例如 `geohash`、`mgrs`、`isea4h` |
| `grid_level` | 剖分任务参数 |
| `space_code` | 空间格网码 |
| `st_code` | 时空编码 |
| `time_bucket` | 按任务时间粒度生成的时间桶 |
| `cell_min_lon/cell_min_lat/cell_max_lon/cell_max_lat` | 命中的格网单元范围 |
| `window_col_off/window_row_off/window_width/window_height` | 栅格 COG window 索引 |

载入系统可以在 manifest 中保留 `grid_type` 和 `grid_level` 作为建议值，但剖分任务最终参数以剖分系统配置为准。

## 7. 对接结论

对载入系统的正式要求可以概括为：

```text
请为每个待剖分资产或观测输出结构化 manifest。manifest 必须包含数据位置、数据身份、UTC 时间、EPSG:4326 空间范围或点坐标、变量/波段/极化标识和质量摘要。剖分系统将基于 manifest 生成 space_code、st_code、time_bucket 和索引事实行。
```

