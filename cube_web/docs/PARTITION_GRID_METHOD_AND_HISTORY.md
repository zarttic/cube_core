# 剖分格网 3x2 选择与多次剖分状态设计

更新时间：2026-06-27

本文记录业务方要求的两个改造：

1. 光学、信息产品、雷达必须支持 `3 x 2` 剖分选择：三种格网 × 两种剖分方式。
2. 同一载入批次可以按不同格网/方式多次剖分，页面要展示已经剖分过和仍可剖分的状态。

本文只做方案落地说明，不写业务代码。

## 1. 必须支持的 3x2 组合

三种格网：

- `tile_matrix`：平面格网。
- `s2`：四边形格网。
- `isea4h`：六边形格网。

两种剖分方式：

- `logical`：逻辑剖分，输出 cell/window 索引行。
- `entity`：实体剖分，输出实体瓦片和实体瓦片元数据。

因此每个数据类型都要支持 6 个组合：

| 格网 | 逻辑剖分 | 实体剖分 |
| --- | --- | --- |
| 平面格网 `tile_matrix` | 必须支持 | 必须支持 |
| 四边形格网 `s2` | 必须支持 | 必须支持 |
| 六边形格网 `isea4h` | 必须支持 | 必须支持 |

适用数据类型：

- `optical`
- `product`
- `radar`

`carbon` 当前不纳入本次 3x2 要求，仍按碳卫星观测链路处理。

## 2. 当前代码现状

当前 Web 和 split 侧不是 3x2，而是把方式隐式绑在格网上：

```text
grid_type == "isea4h" -> run_entity_partition()
其他 grid_type       -> run_logical_partition() 或 run_product_partition()
```

涉及文件：

- `cube_web/frontend/src/views/PartitionView.vue`
- `cube_web/cube_web/schemas.py`
- `cube_web/cube_web/services/partition_runners.py`
- `cube_web/cube_web/services/partition_workflow.py`
- `cube_web/cube_web/services/partition_job_store.py`
- `cube_split/cube_split/jobs/entity_partition_job.py`
- `cube_split/cube_split/jobs/ray_logical_partition_job.py`
- `cube_split/cube_split/jobs/product_partition_job.py`
- `cube_split/cube_split/jobs/ray_partition_core.py`

关键事实：

- `ray_partition_core.build_grid_tasks_driver()` 已经按传入 `grid_type` 生成覆盖任务，具备复用基础。
- `ray_logical_partition_job` 主要缺少 `isea4h` 作为逻辑剖分格网的入口放行。
- `product_partition_job` 主要缺少 `isea4h` 作为逻辑剖分格网的入口放行。
- `entity_partition_job` 当前大量硬编码 `isea4h`，不支持 `s2` 和 `tile_matrix` 实体瓦片。
- Web runner 当前用 `grid_type == "isea4h"` 决定走实体，必须改成用 `partition_method` 决定。

## 3. API 契约改造

请求 payload 必须新增：

```json
{
  "partition_method": "logical",
  "grid_type": "s2",
  "grid_level": 5,
  "grid_level_mode": "auto"
}
```

`partition_method` 取值：

- `logical`
- `entity`

后端模型：

- `cube_web/cube_web/schemas.py` 的 `PartitionDemoRequest` 增加 `partition_method: Literal["logical", "entity"] | None`。
- 保留 `None` 兼容旧客户端，但新前端必须显式发送。

兼容规则：

```text
如果 partition_method 缺失：
  grid_type == "isea4h" -> entity
  其他 grid_type       -> logical
```

这个兼容只用于旧请求。新 UI 和新批次任务必须写入 `partition_method`，否则无法可靠区分 `isea4h + logical` 和 `isea4h + entity`。

## 4. Web runner 分派规则

`cube_web/cube_web/services/partition_runners.py` 需要新增统一解析函数：

```text
_partition_method(payload) -> "logical" | "entity"
```

分派规则改成：

```text
partition_method == "entity"  -> run_entity_partition(args)
partition_method == "logical" -> run_logical_partition(args) 或 run_product_partition(args)
```

不能再用 `grid_type == "isea4h"` 判断方式。

三个数据类型的目标分派：

| 数据类型 | logical | entity |
| --- | --- | --- |
| optical | `run_logical_partition()` | `run_entity_partition()` |
| radar | `run_logical_partition()` | `run_entity_partition()` |
| product | `run_product_partition()` | `run_entity_partition()` |

runner 返回结果必须包含：

```json
{
  "partition_method": "entity",
  "partition_type": "entity",
  "grid_type": "tile_matrix"
}
```

逻辑剖分也要返回：

```json
{
  "partition_method": "logical",
  "partition_type": "logical",
  "grid_type": "isea4h"
}
```

## 5. split 侧改造

### 5.1 逻辑剖分支持 `isea4h`

`ray_logical_partition_job.py`：

- CLI `--grid-type` choices 增加 `isea4h`。
- 保持输出 `index_rows.jsonl`。
- `process_partition()` 已按 `row.grid_type` 生成 ST code，主要复用现有逻辑。

`product_partition_job.py`：

- CLI `--grid-type` choices 增加 `isea4h`。
- `run_product_partition()` 继续走产品逻辑剖分链路。
- `grid_type=isea4h` 时仍输出逻辑索引行，不写实体瓦片。

### 5.2 实体剖分支持三种格网

`entity_partition_job.py` 必须从 ISEA4H 专用改成通用实体剖分。

需要去掉或泛化的硬编码：

- `infer_isea4h_level_for_assets()`：只在 `grid_type=isea4h` 且未显式传层级时使用。
- `_ensure_center_cell_tasks()`：用传入 `grid_type` 调 `sdk.locate()`，不能固定 `isea4h`。
- `_hex_geometry_for_dataset()`：改成 `_cell_geometry_for_dataset(grid_type, space_code)`。
- `_write_entity_tiles()`：
  - tile 目录使用 `task["grid_type"]`。
  - ST code 使用 `task["grid_type"]`。
  - 输出行 `grid_type` 使用 `task["grid_type"]`。
- `_entity_tile_object_key()`：对象 key 中的 `grid=...` 使用 row 的真实 `grid_type`。
- `run_entity_partition()`：
  - 增加 `args.grid_type`。
  - `build_grid_tasks_driver(..., grid_type=args.grid_type)`。
  - report 写真实 `grid_type`。
- CLI `parse_args()` 增加 `--grid-type`，choices 为 `s2/tile_matrix/isea4h`。

实体剖分的业务语义统一为：

```text
对每个格网 cell 取几何边界；
用该 cell 几何 mask 原始栅格；
写出一个实体瓦片；
实体元数据记录 grid_type/grid_level/space_code/st_code/tile_uri。
```

这套语义可覆盖 `s2`、`tile_matrix`、`isea4h`。

## 6. 层级默认值

前端和后端都要按 `partition_method` 处理默认层级：

- `logical` 默认层级：5。
- `entity` 默认层级：6。

`grid_level_mode=auto`：

- `logical` 可继续按分辨率推荐。
- `entity + isea4h` 可保留现有 `target_pixels_per_hex_edge` 推断能力。
- `entity + s2/tile_matrix` 第一版建议使用实体默认层级 6，允许用户手动调整。

不要把 `target_pixels_per_hex_edge` 套到 `s2` 或 `tile_matrix`，它是六边形边长推断参数。

## 7. 多次剖分状态必须按 6 个槽位管理

原需求提到三种格网标志：

```text
[平面格网 | 四边形格网 | 六边形格网]
```

但现在 3x2 是强约束，所以“是否已剖分”不能只按 `grid_type` 判断。否则 `s2 + logical` 完成后会把 `s2 + entity` 也禁用，无法满足 3x2。

状态主键必须是：

```text
batch_id + data_type + grid_type + partition_method
```

建议响应字段：

```json
{
  "partition_slots": [
    {
      "grid_type": "tile_matrix",
      "grid_label": "平面格网",
      "partition_method": "logical",
      "method_label": "逻辑剖分",
      "status": "available",
      "disabled": false,
      "latest_task_id": null,
      "finished_at": null
    },
    {
      "grid_type": "tile_matrix",
      "grid_label": "平面格网",
      "partition_method": "entity",
      "method_label": "实体剖分",
      "status": "completed",
      "disabled": true,
      "latest_task_id": "partition-xxxx",
      "finished_at": "2026-06-27T..."
    }
  ]
}
```

前端可以仍然按三种格网展示，但每个格网下必须展示两个子状态：

```text
平面格网   逻辑: 蓝色可剖分 / 实体: 灰色已完成
四边形格网 逻辑: 灰色已完成 / 实体: 蓝色可剖分
六边形格网 逻辑: 蓝色可剖分 / 实体: 蓝色可剖分
```

颜色规则：

- `available`：蓝色，可提交。
- `completed`：灰色，禁用。
- `running` / `queued` / `retrying`：禁用，显示执行中。
- `failed` / `cancelled`：允许重试或重新执行，建议 warning。

## 8. 后端状态汇总

第一版不新增表，直接从 `partition_job_attempts` 汇总。

汇总来源：

- `partition_job_attempts.batch_id`
- `partition_job_attempts.status`
- `partition_job_attempts.payload->>'grid_type'`
- `partition_job_attempts.payload->>'partition_method'`
- `partition_job_attempts.runner_result->>'grid_type'`
- `partition_job_attempts.runner_result->>'partition_method'`
- `partition_job_attempts.task_id`
- `partition_job_attempts.finished_at`
- `partition_job_attempts.updated_at`

解析优先级：

```text
grid_type:
  runner_result.grid_type -> payload.grid_type -> batch.normalized_payload.grid_type -> "s2"

partition_method:
  runner_result.partition_method
  -> payload.partition_method
  -> runner_result.partition_type
  -> 旧兼容规则(grid_type == isea4h ? entity : logical)
```

`GET /v1/partition/batches` 和 `GET /v1/partition/batches/{batch_id}` 返回 `partition_slots`。

`partition_batches.status` 和 `partition_assets.status` 继续表示最近一次任务或当前任务状态，不用承载 6 个槽位的历史状态。

## 9. 重复提交限制

仅前端禁用不够，后端必须拦截。

在 `PartitionWorkflowService.run_payload()` 和 `run_batch()` 创建 attempt 前校验：

```text
同一 batch_id + grid_type + partition_method 已有 succeeded attempt:
  返回 409，不允许重复执行。

同一 batch_id 当前已有 active attempt:
  第一版继续沿用现有批次级锁，返回 active task 或 409。
```

第一版建议保留“同一批次同一时间只跑一个任务”的现有限制。这样改动小，也避免一批数据同时跑多个 Ray 任务抢 IO。后续如果业务要求并行六个槽位，再把 active lock 从 batch 级改成 slot 级。

## 10. 前端改造

`PartitionView.vue` 需要新增每个模块的方式状态：

- `opticalPartitionMethod`
- `radarPartitionMethod`
- `productPartitionMethod`

提交 payload 必须包含：

- `partition_method`
- `grid_type`
- `grid_level`
- `grid_level_mode`

UI 建议：

```text
剖分格网：平面格网 / 四边形格网 / 六边形格网
剖分方式：逻辑剖分 / 实体剖分
```

或按业务说法顺序展示为两级：

```text
格网：平面 / 四边形 / 六边形
方式：逻辑 / 实体
```

选择任意组合时，不再自动把 `isea4h` 绑定为实体，也不再把 `s2/tile_matrix` 绑定为逻辑。

批次卡片：

- 显示三种格网。
- 每种格网显示逻辑/实体两个子状态。
- 点击蓝色子状态时，选中该批次并设置对应 `grid_type + partition_method`。

`shouldDisplayManagedBatch()`：

```text
隐藏 archived。
如果 6 个槽位都 completed，可隐藏或显示为全灰。
未全完成的批次必须继续显示。
```

## 11. 质量、入库和结果展示

逻辑剖分：

- 继续输出 `index_rows.jsonl`。
- 继续走现有逻辑入库和质量检查。
- `partition_method=logical` 写入 result 和 attempt payload。

实体剖分：

- 继续输出 `entity_index_rows.jsonl` 和兼容的 `index_rows.jsonl`。
- 继续写 `rs_entity_tile_asset`。
- 表中已有唯一键包含 `grid_type/grid_level/space_code`，可支持多格网实体瓦片。
- `partition_method=entity` 写入 result 和 attempt payload。

质量报告和任务详情需要展示：

- 格网类型。
- 剖分方式。
- 格网层级。
- 输出路径。

## 12. 测试建议

后端 Web 测试：

- `partition_method=logical, grid_type=isea4h` 必须走逻辑 runner。
- `partition_method=entity, grid_type=s2` 必须走实体 runner。
- `partition_method=entity, grid_type=tile_matrix` 必须走实体 runner。
- `partition_method=logical, grid_type=tile_matrix` 必须走逻辑 runner。
- 缺失 `partition_method` 时保持旧兼容规则。
- 同一 `batch_id + grid_type + partition_method` 成功后再次提交返回 409。
- 不同 `partition_method` 的同一 `grid_type` 不应互相禁用。

split 测试：

- `run_entity_partition(grid_type="s2")` 输出实体瓦片，rows/report 的 `grid_type` 为 `s2`。
- `run_entity_partition(grid_type="tile_matrix")` 输出实体瓦片，MinIO key 包含真实 grid。
- `run_entity_partition(grid_type="isea4h")` 保持现有行为。
- `run_logical_partition(grid_type="isea4h")` 输出逻辑索引行。
- `run_product_partition(grid_type="isea4h")` 输出产品逻辑索引行。

前端检查：

- 光学、产品、雷达三页都能选择 6 个组合。
- 批次卡片能显示 6 个槽位状态。
- 已完成槽位禁用，未完成槽位可选。

推荐验证命令：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests cube_split/tests
cd cube_web/frontend && npm run build
```

如果改到 SDK 或格网编码，再补：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests
```

## 13. 推荐实施顺序

1. 在 Web schema、runner result、attempt payload 中打通 `partition_method`。
2. 改 Web runner 分派规则，从 `grid_type` 分派改为 `partition_method` 分派。
3. 泛化 `entity_partition_job`，让实体剖分接受 `s2/tile_matrix/isea4h`。
4. 放开逻辑剖分的 `isea4h` 入口。
5. 后端批次响应增加 `partition_slots`，并按 slot 做重复提交校验。
6. 前端改成格网 + 方式两级选择，批次卡片展示 6 个槽位。
7. 补测试和构建验证。

## 14. 明确不做的事

第一版不建议做：

- 新建一套任务系统。
- 新建一张槽位状态表。
- 允许同一批次并行跑多个槽位。
- 把 `carbon` 纳入 3x2。

这些都不是满足当前 3x2 业务要求的必要前提。
