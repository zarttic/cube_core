# 载入子系统数据载入与对账接口设计


本文档只定义载入子系统如何通过接口把四类数据交给剖分系统，以及双方如何对账。

四类数据：

- `optical`：光学遥感栅格。
- `radar`：雷达遥感栅格。
- `product`：信息产品栅格。
- `carbon`：碳卫星观测。

## 1. 总体约定

载入子系统负责：

- 上传源数据到对象存储。
- 生成可被剖分系统识别的批次元数据。
- 调用载入接口，把批次显式交给剖分系统。
- 通过对账接口查询剖分系统是否已接收、处理到哪一步、是否失败。

剖分系统负责：

- 校验载入元数据。
- 维护批次、资产、执行、重试、质检和入库状态。
- 返回批次和资产的当前处理状态。

MinIO 中的 `_meta.json` 可以保留为原始快照，但生产流程不应只依赖扫描 MinIO 目录来发现任务。

## 2. 批次载入接口

### 2.1 接口

```http
POST /v1/partition/schemas/import
Content-Type: application/json
```

用途：载入子系统在一个数据批次上传完成后，调用该接口把批次元数据交给剖分系统。

该接口只负责导入批次和资产元数据，不直接启动剖分任务。

### 2.2 通用请求结构

```json
{
  "schema_version": "1.0",
  "batch_id": "LOAD_BATCH_001",
  "batch_name": "批次展示名称",
  "data_type": "optical",
  "source_system": "loader",
  "loaded_at": "2026-06-13T08:00:00Z",
  "updated_at": "2026-06-13T08:05:00Z",
  "raw_meta_uri": "s3://user-1/raw_data/LOAD_BATCH_001_meta.json",
  "priority": 0,
  "max_auto_retries": 1,
  "assets": []
}
```

顶层字段：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `schema_version` | 是 | 接口 schema 版本，当前为 `1.0`。 |
| `batch_id` | 是 | 批次稳定 ID。重复推送同一批次时必须保持一致。 |
| `batch_name` | 建议 | 批次展示名称。 |
| `data_type` | 是 | `optical`、`radar`、`product`、`carbon`。 |
| `source_system` | 建议 | 来源系统标识，例如 `loader`。 |
| `loaded_at` | 建议 | 载入完成时间，UTC ISO8601。 |
| `updated_at` | 建议 | 元数据更新时间，UTC ISO8601。 |
| `raw_meta_uri` | 建议 | 原始 `_meta.json` 地址，用于审计和排障。 |
| `priority` | 可选 | 批次优先级，默认 `0`。 |
| `max_auto_retries` | 可选 | 自动重试次数，默认 `1`。 |
| `assets` | 栅格类必填 | `optical`、`radar`、`product` 使用。 |
| `observations` | 碳卫星必填 | `carbon` 使用。 |

### 2.3 通用响应结构

```json
{
  "batch_id": "LOAD_BATCH_001",
  "batch_name": "批次展示名称",
  "data_type": "optical",
  "source_system": "loader",
  "status": "pending",
  "ingest_status": "not_ready",
  "attempt_count": 0,
  "created_at": "2026-06-13T08:05:10Z",
  "updated_at": "2026-06-13T08:05:10Z"
}
```

导入成功后，批次进入 `pending` 状态。后续是否执行剖分，由剖分系统内部调度或人工触发。

## 3. optical 光学遥感载入

`optical` 使用 `assets[]`。每个 asset 表示一个可剖分的光学栅格资产。

资产必填字段：

| 字段 | 说明 |
| --- | --- |
| `asset_id` | 资产稳定 ID。 |
| `source_uri` | 源数据地址，推荐 `s3://...`。 |
| `scene_id` | 场景 ID。 |
| `acq_time` | 成像或代表时间，UTC ISO8601。 |
| `sensor` | 传感器或数据来源。 |
| `product_family` | 产品族，例如 `sentinel2`、`landsat`、`other`。 |
| `resolution` | 空间分辨率，单位米，必须大于 0。 |
| `corners` | WGS84 四角点，4 个 `[lon, lat]`。 |
| `bands` / `band` | 波段信息，至少提供一种。 |

示例：

```json
{
  "schema_version": "1.0",
  "batch_id": "OPTICAL_20260613_001",
  "batch_name": "山东光学镶嵌影像",
  "data_type": "optical",
  "source_system": "loader",
  "assets": [
    {
      "asset_id": "optical-2020q3-b4",
      "source_uri": "s3://user-1/raw_data/optical_2020q3_b4.tif",
      "scene_id": "Shandong_mosaic_2020Q3",
      "sensor": "optical_mosaic",
      "product_family": "other",
      "bands": ["sr_band4"],
      "band": "sr_band4",
      "acq_time": "2020-07-01T00:00:00Z",
      "resolution": 30,
      "bbox": [114.75, 33.85, 122.77, 38.50],
      "corners": [
        [114.75, 38.50],
        [122.77, 38.50],
        [122.77, 33.85],
        [114.75, 33.85]
      ],
      "file_format": "GeoTIFF"
    }
  ]
}
```

## 4. radar 雷达遥感载入

`radar` 使用 `assets[]`。每个 asset 表示一个雷达栅格资产。

除通用栅格字段外，雷达资产建议提供：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `polarization` | 是 | 极化方式，例如 `vv`、`vh`。 |
| `sidecars` | 条件必填 | ENVI `.dat` 等需要 `.hdr` 时必须提供。 |
| `orbit_direction` | 可选 | 轨道方向。 |
| `relative_orbit` | 可选 | 相对轨道号。 |

示例：

```json
{
  "schema_version": "1.0",
  "batch_id": "RADAR_20260611221242_1197",
  "batch_name": "江苏扬州 Sentinel-1 2018-2020 夏季影像",
  "data_type": "radar",
  "source_system": "loader",
  "raw_meta_uri": "s3://user-1/raw_data/RADAR_20260611221242_1197_meta.json",
  "assets": [
    {
      "asset_id": "s1-RADAR_20260611221242_1197-vv",
      "source_uri": "s3://user-1/raw_data/radar_20200803_vv.tif",
      "scene_id": "S1_20200803",
      "sensor": "sentinel1_sar",
      "product_family": "sentinel1",
      "bands": ["vv"],
      "band": "vv",
      "polarization": "vv",
      "acq_time": "2020-08-03T00:00:00Z",
      "resolution": 10,
      "bbox": [119.36, 32.22, 119.54, 32.48],
      "corners": [
        [119.36, 32.48],
        [119.54, 32.48],
        [119.54, 32.22],
        [119.36, 32.22]
      ],
      "file_format": "GeoTIFF"
    }
  ]
}
```

## 5. product 信息产品载入

`product` 使用 `assets[]`。每个 asset 表示一个产品栅格资产。

除通用栅格字段外，信息产品建议提供：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `product_name` | 是 | 产品名称。 |
| `product_year` | 是 | 产品年份。 |
| `product_period` | 可选 | `yearly`、`monthly`、`quarterly` 等。 |
| `variable` | 建议 | 产品变量名。 |
| `unit` | 可选 | 产品值单位。 |

示例：

```json
{
  "schema_version": "1.0",
  "batch_id": "PRODUCT_20260611221243_A16C",
  "batch_name": "滇中生态安全评价数据集 1980",
  "data_type": "product",
  "source_system": "loader",
  "assets": [
    {
      "asset_id": "dianzhong-eco-1980",
      "source_uri": "s3://user-1/raw_data/dianzhong_eco_1980.tif",
      "scene_id": "dianzhong_ecological_security_1980",
      "product_name": "1980-2020年滇中地区30米生态安全评价数据集",
      "product_year": 1980,
      "product_period": "yearly",
      "sensor": "data_product",
      "product_family": "product",
      "bands": ["product_value"],
      "band": "product_value",
      "variable": "product_value",
      "acq_time": "1980-01-01T00:00:00Z",
      "resolution": 30,
      "bbox": [100.64, 23.28, 104.82, 27.06],
      "corners": [
        [100.64, 27.06],
        [104.82, 27.06],
        [104.82, 23.28],
        [100.64, 23.28]
      ],
      "file_format": "GeoTIFF"
    }
  ]
}
```

## 6. carbon 碳卫星载入

`carbon` 使用 `observations[]`。每条 observation 表示一个可剖分的碳卫星观测点或足迹。

观测必填字段：

| 字段 | 说明 |
| --- | --- |
| `observation_id` | 观测稳定 ID。 |
| `source_uri` | 源文件地址。 |
| `source_index` | 观测在源文件中的索引，建议提供。 |
| `acq_time` | 观测时间，UTC ISO8601。 |
| `sensor` | 例如 `oco2`。 |
| `product_family` | 例如 `xco2`。 |
| `resolution` | 足迹或等效分辨率。 |
| `lon` / `lat` | 中心点。若无中心点，应提供 `corners` 或 `footprint_geojson`。 |

示例：

```json
{
  "schema_version": "1.0",
  "batch_id": "CARBON_20260613_001",
  "batch_name": "OCO-2 XCO2 观测",
  "data_type": "carbon",
  "source_system": "loader",
  "observations": [
    {
      "observation_id": "oco2-20201231-0001",
      "source_uri": "s3://user-1/raw_data/oco2_20201231.nc4",
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
  ]
}
```

## 7. 对账接口

### 7.1 接口

```http
POST /v1/partition/schemas/reconcile
Content-Type: application/json
```

用途：载入子系统查询剖分系统是否已接收某些批次或资产，以及当前处理状态。

该接口只读，不改变任何状态。

### 7.2 请求结构

```json
{
  "source_system": "loader",
  "batch_ids": [
    "RADAR_20260611221242_1197",
    "PRODUCT_20260611221243_A16C"
  ],
  "asset_ids": [
    "s1-RADAR_20260611221242_1197-vv"
  ],
  "updated_since": "2026-06-13T00:00:00Z",
  "include_assets": true,
  "include_attempts": false
}
```

请求字段：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `source_system` | 建议 | 来源系统标识。 |
| `batch_ids` | 条件必填 | 要对账的批次 ID。和 `asset_ids`、`updated_since` 至少提供一个。 |
| `asset_ids` | 条件必填 | 要对账的资产 ID。和 `batch_ids`、`updated_since` 至少提供一个。 |
| `updated_since` | 条件必填 | 增量对账时间。和 `batch_ids`、`asset_ids` 至少提供一个。 |
| `include_assets` | 可选 | 是否返回资产明细，默认 `true`。 |
| `include_attempts` | 可选 | 是否返回执行历史，默认 `false`。 |

规则：

- 提供 `batch_ids` 时，按批次 ID 精确对账。
- 提供 `asset_ids` 时，按资产 ID 精确对账。
- 未提供 `batch_ids` 和 `asset_ids` 时，`updated_since` 用于增量对账。

### 7.3 响应结构

```json
{
  "source_system": "loader",
  "generated_at": "2026-06-13T12:30:00Z",
  "batches": [
    {
      "batch_id": "RADAR_20260611221242_1197",
      "known": true,
      "data_type": "radar",
      "batch_name": "江苏扬州 Sentinel-1 2018-2020 夏季影像",
      "status": "running",
      "ingest_status": "not_supported",
      "quality_status": null,
      "quality_report_id": null,
      "last_task_id": "partition-abc123",
      "last_error": null,
      "attempt_count": 1,
      "asset_counts": {
        "total": 1,
        "pending": 0,
        "running": 1,
        "succeeded": 0,
        "failed": 0,
        "manual_required": 0
      },
      "assets": [
        {
          "asset_id": "s1-RADAR_20260611221242_1197-vv",
          "source_uri": "s3://user-1/raw_data/radar_20200803_vv.tif",
          "scene_id": "S1_20200803",
          "status": "running",
          "attempt_count": 1,
          "last_error": null,
          "last_run_dir": null,
          "partitioned_at": null
        }
      ],
      "attempts": []
    },
    {
      "batch_id": "PRODUCT_20260611221243_A16C",
      "known": false,
      "status": "missing"
    }
  ],
  "missing_batch_ids": [
    "PRODUCT_20260611221243_A16C"
  ],
  "missing_asset_ids": [],
  "summary": {
    "requested_batches": 2,
    "known_batches": 1,
    "missing_batches": 1,
    "requested_assets": 1,
    "known_assets": 1,
    "missing_assets": 0
  }
}
```

### 7.4 状态枚举

| 状态 | 说明 |
| --- | --- |
| `missing` | 剖分系统未接收到该批次或资产。 |
| `pending` | 已接收，尚未执行。 |
| `queued` | 已入队。 |
| `running` | 正在执行。 |
| `retrying` | 正在重试。 |
| `succeeded` | 处理成功。 |
| `failed` | 处理失败。 |
| `manual_required` | 需要人工处置。 |
| `cancel_requested` | 已请求取消。 |
| `cancelled` | 已取消。 |
| `archived` | 已归档，不再处理。 |

## 8. 幂等要求

载入子系统重复推送同一批次时：

- `batch_id` 必须保持不变。
- 同一资产的 `asset_id` 必须保持不变。
- 同一观测的 `observation_id` 必须保持不变。
- `source_uri` 指向的对象应保持稳定。
- 如果元数据发生修正，应更新 `updated_at`，并在 `metadata` 中记录修正说明。

剖分系统以 `batch_id` 作为批次幂等键，以 `asset_id` / `observation_id` 作为资产或观测幂等键。

## 9. 错误响应

请求不合法时返回 `422`：

```json
{
  "detail": "radar asset #1.source_uri is required"
}
```

常见错误：

- `batch_id` 为空。
- `data_type` 不在四类数据范围内。
- 栅格类没有 `assets`。
- 碳卫星没有 `observations`。
- 缺少 `source_uri`、`acq_time`、`sensor`、`product_family`、`resolution` 或空间字段。
- `acq_time` 不是 ISO8601。
- `corners` 不是 4 个 WGS84 经纬度点。
- `resolution` 小于等于 0。



回应
元数据数据库里面已经定义了，你可以使用了  @zrfGlpfz 
：class ArdPartitionBatch(Base):
    """ARD 剖分系统批次主表 - 完美适配数据载入对账契约"""
    __tablename__ = "ard_partition_batches"

    schema_version = Column(String(20), default="1.0")  # 版本控制字段，便于未来无损在线升级和兼容性维护
    id = Column(Integer, primary_key=True, index=True)
    batch_id = Column(String(100), unique=True, index=True, nullable=False) # 批次稳定 ID (幂等键)
    batch_name = Column(String(200), nullable=True)                          # 批次展示名称
    data_type = Column(String(50), nullable=False)                          # optical, radar, product, carbon
    source_system = Column(String(50), default="loader")                    # 来源系统标识
    loaded_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    raw_meta_uri = Column(String(500), nullable=True)                       # 原始 _meta.json 对象存储地址
    priority = Column(Integer, default=0)                                   # 优先级
    max_auto_retries = Column(Integer, default=1)                           # 最大自动重试次数
    status = Column(String(50), default="pending")                          # pending, running, succeeded, failed

    # 级联关系
    assets = relationship("ArdPartitionAsset", back_populates="batch", cascade="all, delete-orphan")
    observations = relationship("ArdPartitionObservation", back_populates="batch", cascade="all, delete-orphan")


class ArdPartitionAsset(Base):
    """ARD 栅格品类资产明细表（光学、雷达、信息产品共用）"""
    __tablename__ = "ard_partition_assets"

    id = Column(Integer, primary_key=True, index=True)
    batch_id = Column(Integer, ForeignKey("ard_partition_batches.id", ondelete="CASCADE"), nullable=False)
    asset_id = Column(String(100), unique=True, index=True, nullable=False) # 资产稳定 ID
    source_uri = Column(String(500), nullable=False)                        # 生产级归档完结影像对象存储地址
    scene_id = Column(String(100), nullable=True)                           # 场景 ID
    acq_time = Column(DateTime, nullable=True)                              # 成像或代表时间
    sensor = Column(String(100), nullable=True)                             # 传感器
    product_family = Column(String(100), nullable=True)                     # 产品族
    resolution = Column(Float, nullable=True)                               # 空间分辨率(米)
    bbox = Column(JSON, nullable=True)                                      # 边界范围 [min_lon, min_lat, max_lon, max_lat]
    corners = Column(JSON, nullable=True)                                   # 四角点点阵 WGS84
    bands = Column(JSON, nullable=True)                                     # 多波段列表
    band = Column(String(50), nullable=True)                                # 激活波段
    file_format = Column(String(50), default="GeoTIFF")                     # 物理文件格式

    # === 雷达遥感 (radar) 专属拓展字段 ===
    polarization = Column(String(20), nullable=True)                        # 极化方式 (vv, vh 等)
    sidecars = Column(JSON, nullable=True)                                  # 附属侧车文件结构 (如 .hdr 元数据外挂地址)
    orbit_direction = Column(String(20), nullable=True)                     # 轨道方向
    relative_orbit = Column(String(50), nullable=True)                      # 相对轨道号

    # === 信息产品 (product) 专属拓展字段 ===
    product_name = Column(String(200), nullable=True)                       # 产品全名
    product_year = Column(Integer, nullable=True)                           # 业务统计年份
    product_period = Column(String(50), nullable=True)                      # 统计周期 (yearly, monthly)
    variable = Column(String(100), nullable=True)                           # 反演物理变量
    unit = Column(String(50), nullable=True)                                # 变量计量单位

    batch = relationship("ArdPartitionBatch", back_populates="assets")


class ArdPartitionObservation(Base):
    """ARD 非栅格品类碳卫星观测足迹足迹表 (carbon)"""
    __tablename__ = "ard_partition_observations"

    id = Column(Integer, primary_key=True, index=True)
    batch_id = Column(Integer, ForeignKey("ard_partition_batches.id", ondelete="CASCADE"), nullable=False)
    observation_id = Column(String(100), unique=True, index=True, nullable=False) # 观测稳定 ID
    source_uri = Column(String(500), nullable=False)                        # 源科学文件地址
    source_index = Column(Integer, default=0)                               # 文件内部科学矩阵索引号
    acq_time = Column(DateTime, nullable=True)                              # 精确观测时刻
    sensor = Column(String(100), nullable=True)                             # 固化传感器 (如 oco2)
    product_family = Column(String(100), nullable=True)                     # 固化产品族 (如 xco2)
    product_type = Column(String(100), nullable=True)                       # 产品物理类型
    resolution = Column(Float, nullable=True)                               # 足迹等效分辨率
    lon = Column(Float, nullable=True)                                      # 足迹中心点经度
    lat = Column(Float, nullable=True)                                      # 足迹中心点纬度
    xco2 = Column(Float, nullable=True)                                     # 物理观测核心指标值
    quality_flag = Column(String(20), nullable=True)                        # 质量反演控制标签
    corners = Column(JSON, nullable=True)                                   # 足迹多边形点阵

    batch = relationship("ArdPartitionBatch", back_populates="observations")
