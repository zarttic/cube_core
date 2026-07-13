# 剖分格网与方式现状

更新时间：2026-07-13

本文是当前实现的操作参考，不是下一轮 UI/状态机重构方案。历史需求和旧实现对照保留在 Git 历史中。

## 当前矩阵

| 格网 | 逻辑 | 实体 | Web 页面 | 备注 |
| --- | --- | --- | --- | --- |
| `s2` | 支持 | 支持 | 显示 | encoder-backed |
| `tile_matrix` | 支持 | 支持 | 显示 | 页面名称为“经纬度格网” |
| `isea4h` | 支持 | 支持 | 显示 | 页面名称为“六边形格网” |
| `plane_grid` | 实验性支持 | 不支持 | 显示 | 页面名称为“平面格网”，保留源 CRS |
| `mgrs` | SDK/旧客户端兼容 | 不作为生产方式 | 不显示 | 不新增生产调用点 |

适用数据类型：`optical`、`radar`、`product`。`carbon` 继续使用碳观测专用链路，默认以 `isea4h` 组织观测空间编码。

## 方式分派

请求必须尽量显式提供：

```json
{
  "partition_method": "logical",
  "grid_type": "s2",
  "grid_level": 5,
  "grid_level_mode": "auto"
}
```

- `logical`：调用逻辑窗口链路，输出 `index_rows.jsonl`。
- `entity`：调用实体瓦片链路，输出实体瓦片、对象 URI 和实体元数据。
- 旧客户端缺少 `partition_method` 时，后端保留历史默认推断；新客户端不得依赖该兼容规则。
- `plane_grid` 只能是 `logical`，因为它没有 encoder 的通用 cell geometry/cover 能力。

## 默认层级

- 逻辑方式默认 `grid_level=5`，可按分辨率自动推导。
- 实体方式默认 `grid_level=6`；`isea4h` 可继续使用目标像素边长推导。
- `max_cells_per_asset=0` 表示不设上限；smoke、调试和故障复现应显式设置正数。
- `plane_grid` 必须使用空 `target_crs`，否则 runner 拒绝请求并保留源影像 CRS。

## 批次槽位

批次详情按 `batch_id + data_type + grid_type + partition_method` 记录最近一次 attempt。当前有效槽位是 7 个：

```text
tile_matrix/logical, tile_matrix/entity,
plane_grid/logical,
s2/logical, s2/entity,
isea4h/logical, isea4h/entity
```

同一槽位已有成功 attempt 时，重复运行返回冲突；失败或取消槽位可以重试。批次主状态仍表示最近一次运行，不替代槽位历史。

当前已知偏差：前端仍以四种格网与两种方式的交叉展示为基础，某些旧批次可能出现 `plane_grid/entity` 空槽位；地图“加载格网”按钮对 `plane_grid` 仍会尝试通用 cover。两项都属于下一轮整体 UI/状态机重构，不是当前验收契约。

## 结果与入库

- 生产 `run` 默认执行剖分并按 ingest 配置写入 OpenGauss/MinIO。
- `demo` endpoint 仅为旧客户端兼容别名，不新增生产调用点。
- 质检报告写入 OpenGauss `quality_reports`；批次记录保存 `quality_report_id`、`quality_status` 和失败摘要。
- `plane_grid` 输出的 `cell_min_lon/cell_min_lat/...` 字段在当前实现中承载源 CRS x/y 兼容值，不应被下游当作 WGS84 bbox。

## 后续重构边界

下一轮改造应一次性处理：

1. 为 `plane_grid` 引入跨场景稳定的 CRS/origin/resolution 编码。
2. 让质检识别源 CRS bbox，并将 native bounds 与 WGS84 bounds 分开存储。
3. 从前端槽位交叉展示中移除无效 `plane_grid/entity`，并为源平面窗口提供独立预览。
4. 重新校准 OpenGauss fact 唯一键、质量状态和多资产合并语义。
