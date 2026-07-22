# 载入子系统交付契约

更新时间：2026-07-17

本文定义载入子系统向剖分系统交付光学、雷达、信息产品和碳卫星数据的正式接口。
生产链路只接受 Dataset/Scene 层级，不接受平铺资产批次。

## 1. 领域关系

```text
LoadBatch N:M Scene N:1 Dataset
                    |
                    +-- SceneAsset
                    +-- SceneBand
```

- `LoadBatch` 表示一次载入动作，可以包含多个 Dataset 的 Scene。
- `LoadBatch` 既可由载入子系统导入，也可由数据集页面确认重新载入产生；两者是同一
  领域实体，区别只记录在来源和血缘元数据中。
- `Dataset` 是长期管理、质检、入库和发布的主记录。
- `Scene` 是载入、剖分和入库的最小数据单元。
- 一个 Dataset 可以在多个 LoadBatch 中持续接收 Scene。
- 波段属于 Scene；资产保存实际对象 URI 和校验信息。
- 数据集重新载入复用已有 Scene 和资产时，只新增 LoadBatch 与 Scene 的关联，不复制
  MinIO 对象或 `SceneAsset` 记录。

## 2. 导入接口

```http
POST /v1/partition/schemas/import
Content-Type: application/json
```

启用认证时该接口仍是载入系统的公开交付入口。接口只登记载入批次、Dataset、Scene、
资产和波段，不自动发起剖分。

请求顶层只接受以下字段：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `schema_version` | 否 | 当前建议 `1.0`。 |
| `load_batch_id` | 是 | 载入系统生成的稳定批次 ID。 |
| `batch_name` | 否 | 页面展示名。 |
| `source_system` | 否 | 来源系统标识。 |
| `loaded_at` | 否 | UTC ISO8601 载入时间。 |
| `datasets` | 是 | 非空 Dataset 数组。 |

重复提交相同 `load_batch_id` 应保持 Dataset、Scene 和资产稳定标识一致。接口拒绝旧的
`batch_id`、顶层 `data_type`、`assets` 和 `observations` 平铺结构。

## 3. 完整示例

```json
{
  "schema_version": "1.0",
  "load_batch_id": "LOAD_20260717_001",
  "batch_name": "山东光学与雷达联合载入",
  "source_system": "ard-loader",
  "loaded_at": "2026-07-17T08:00:00Z",
  "datasets": [
    {
      "dataset_id": "GF1-SD-2019-2021",
      "dataset_code": "GF1-SD-ARD",
      "dataset_title": "山东 GF1 地表反射率",
      "data_type": "optical",
      "product_type": "surface_reflectance",
      "scenes": [
        {
          "scene_id": "GF1-SD-20200710",
          "scene_key": "GF1-SD-20200710",
          "acquisition_time": "2020-07-10T02:30:00Z",
          "bbox": [114.75, 33.85, 122.77, 38.50],
          "assets": [
            {
              "asset_id": "GF1-SD-20200710-COG",
              "source_uri": "s3://cube/cube/source/optocal/GF1-SD-20200710.tif",
              "cog_uri": "s3://cube/cube/source/optocal/GF1-SD-20200710.tif",
              "source_kind": "cog",
              "source_format": "cog",
              "checksum": "<sha256>",
              "crs": "EPSG:4326",
              "resolution_m": 16,
              "bands": [
                {"band_code": "B01", "band_name": "Blue", "band_type": "spectral", "display_order": 0},
                {"band_code": "B02", "band_name": "Green", "band_type": "spectral", "display_order": 1}
              ]
            }
          ]
        }
      ]
    }
  ]
}
```

光学、雷达和信息产品资产必须提供可读取的 COG `cog_uri`。碳卫星资产使用原始
NetCDF/HDF5 `source_uri`，不转换为 COG。生产 URI 必须是 MinIO `s3://` 地址，禁止节点
本地绝对路径。

## 4. 波段要求

每个资产提供非空 `bands`：

- 光学使用 `band_type=spectral`。
- 雷达使用 `band_type=polarization`，典型编码为 `VV`、`VH`、`HH`、`HV`。
- 信息产品和碳卫星使用 `band_type=variable`。
- `band_code` 是稳定身份；`band_name` 仅用于展示。

完整命名与展示规则见
[BAND_PRESENTATION_CONTRACT.md](../cube_web/docs/BAND_PRESENTATION_CONTRACT.md)。

## 5. 剖分与对账

导入完成后通过以下接口对账并发起执行：

- `GET /v1/partition/load-batches`
- `GET /v1/partition/load-batches/{load_batch_id}`
- `GET /v1/partition/load-batches/{load_batch_id}/scenes`
- `POST /v1/partition/runs`
- `GET /v1/partition/tasks/{task_id}`
- `POST /v1/partition/tasks/{task_id}/cancel`
- `POST /v1/partition/tasks/{task_id}/retry`

运行请求按 Dataset 选择 Scene，并为每个 Dataset 独立指定格网。生产格网仅允许
`geohash`、`mgrs`、`isea4h`；前两者固定逻辑剖分，ISEA4H 固定实体剖分。

剖分完成后写入 Dataset 的当前输出版本和剖分完成标记；质检通过后才能入库，入库完成后
才能发布。Dataset 管理、质量记录和发布历史不以 LoadBatch 为主记录。

## 6. 失败处理

- 导入校验失败：修正原始 manifest 后使用同一 `load_batch_id` 重试。
- 单个 Scene 失败：通过正式任务重试入口重试，不创建平铺资产批次。
- 对象不可读或 checksum 不符：由载入系统修复对象后重新交付。
- 同一 Scene 不得在一次剖分运行中被多个 Dataset 重复选择。

接口返回 422 表示领域契约不合法，503 表示 Scene 领域存储未配置，其余运行失败通过任务
详情和质量记录追踪。
