# 剖分提交后入库逻辑说明

更新时间：2026-06-19

本文档记录 `cube_web` 当前“提交剖分任务后是否自动入库”的实际行为，以及前端
“预入库校验 / 确认入库”按钮与现行执行链路之间的偏差，供后续页面和状态机调整使用。

## 1. 结论

当前生产 `run` 流程已经不是“剖分完成后等待人工确认入库”的两段式流程。

- 光学 `run` 默认会在剖分完成后继续执行入库。
- 产品 `run` 默认会在剖分完成后继续执行入库。
- 雷达 `run` 只要开启元数据后端，同样会在剖分完成后继续执行入库。
- `ISEA4H` 实体剖分会在同一作业内完成瓦片上传和元数据写入。

因此，剖分主页面继续展示“预入库校验 / 确认入库”会让操作语义和真实执行链路不一致。

## 2. 当前事实

### 2.1 剖分任务主入口

前端提交剖分任务时，主入口是：

- `POST /v1/partition/{data_type}/tasks/run`

后端由 `PartitionWorkflowService.run_payload()` 创建批次 attempt，再调用生产 `run`
runner 执行异步任务。

### 2.2 光学 `run` 已默认带入库参数

`cube_web` 的 optical ingest 默认配置为：

- `metadata_backend=postgres`
- `asset_storage_backend=minio`

生产光学 runner 在 `partition_run` 模式下会把这组 ingest 参数一并传给
`cube_split.jobs.ray_logical_partition_job.run_logical_partition()`。

`run_logical_partition()` 的实际行为是：

1. 先生成 `index_rows.jsonl`。
2. 再根据 `metadata_backend` / `ingest_enabled` 判断是否执行 `_run_partition_ingest()`。
3. `_run_partition_ingest()` 直接调用 `cube_split.ingest.ray_ingest_job.run_ingest()`。

也就是说，光学主流程已经是“剖分 + 入库”一体化执行。

### 2.3 产品、雷达、实体剖分也不是“仅剖分”

- 产品 runner 默认 `metadata_backend=postgres`、`asset_storage_backend=minio`，
  也会进入入库链路。
- 雷达 runner 只要 `metadata_backend` 不是 `none`，也会在逻辑剖分后触发 ingest。
- `ISEA4H` 实体剖分不会单独再走一个 confirm API；它在同一 job 内完成：
  - 瓦片上传到 MinIO
  - 元数据写入 PostgreSQL

因此，“主流程只做剖分，入库要靠页面按钮确认”这件事，和当前底层实现不一致。

### 2.4 批次重试和失败资产重试也会重新走 `run`

当前批次重试和失败资产重试都不是“只补一次入库”：

- `POST /v1/partition/batches/{batch_id}/retry`
- `POST /v1/partition/assets/retry`

这两条链路最终都会重新进入生产 `run` 执行，只是 retry 时会缩小资产范围或调整
retry 策略。也就是说，手动重试本身已经包含“重跑剖分并再次入库”的语义。

## 3. 现有页面按钮为什么还会出现

`cube_web` 里仍然保留了一套独立的 optical ingest 接口：

- `POST /v1/ingest/optical/preview`
- `POST /v1/ingest/optical/confirm`

这套接口对应前端的：

- “预入库校验”
- “确认入库”

但批次状态机沿用的还是旧语义：

- 当 optical 批次 `status='succeeded'` 且已有 `quality_report_id` 时，
  `PartitionJobStore` 会把 `ingest_status` 刷成 `ready`，而不是 `ingested`。
- 前端据此把批次理解成“可以预校验 / 可以确认入库”，从而继续展示这两个按钮。

这造成了一个明显偏差：

- 实际执行已经完成自动入库；
- 页面状态却把它解释成“待人工确认入库”。

## 4. 这两个按钮现在实际做的事

### 4.1 预入库校验

`/v1/ingest/optical/preview` 不会写生产数据。它会：

1. 读取当前 `run_dir` 下的 `index_rows.jsonl`。
2. 组装即将写入的 raw asset / cube fact 记录。
3. 统计和现有生产表中的唯一键冲突数量。

它更像一个“写入前统计预览”工具，而不是主流程里的必要步骤。

### 4.2 确认入库

`/v1/ingest/optical/confirm` 会再次调用 `run_ingest()`，本质上是对已有剖分结果再执行
一遍入库，而不是接续主流程的“最终确认”。

这意味着它不是主流程的第二阶段，而是一个独立的二次入库入口。

## 5. 当前风险

### 5.1 页面语义误导

对操作者来说，当前页面会传达一个错误事实：

- “提交剖分任务后只是准备好了数据，还没有真正入库。”

但真实情况是主流程通常已经完成入库。

### 5.2 二次入库风险

`confirm_optical_ingest()` 在未显式传版本时，会给 `asset_version` /
`cube_version` 生成 `demo-%Y%m%d` 默认值；而正常 optical `run` 默认使用的是
runner 里的版本值。

因此，“确认入库”并不只是简单重复确认，存在以下风险：

- 用同一批 `index_rows.jsonl` 再写一遍生产表；
- 或者生成另一套版本号不同的新记录。

这进一步说明它不适合作为剖分主页面的常规操作按钮。

## 6. 建议

### 6.1 主剖分页移除这两个按钮

建议从剖分主页面移除或隐藏：

- “预入库校验”
- “确认入库”

原因很直接：它们对应的不是当前主流程真实缺失的步骤。

### 6.2 批次 `ingest_status` 语义改为反映真实执行结果

建议把 optical 批次在生产 `run` 成功后的 ingest 状态，从当前的 `ready`
调整为能表达“该批次已经自动入库完成”的状态，例如：

- `ingested`

这样前端不会再把已完成自动入库的批次误判成“待确认入库”。

### 6.3 如果保留 optical ingest API，应改成运维型工具

`/v1/ingest/optical/preview` 和 `/v1/ingest/optical/confirm` 仍然有保留价值，但定位应改成：

- 仅补入库
- 仅重试入库
- 运维排障工具

适用场景应是：

- 已有 `run_dir` / `report_id`
- 不希望整批重新跑 partition
- 只需要补写 metadata / cube fact

如果沿这个方向保留，建议同时约束：

- 必须复用原批次的 `asset_version` / `cube_version`
- 不再生成 `demo-%Y%m%d` 这类独立默认版本
- 页面入口放在批次详情或管理工具区，而不是主剖分页

### 6.4 手动重试区域不需要再叠加“确认入库”按钮

当前手动重试已经会重新走生产 `run`。因此：

- 批次重试不需要额外“确认入库”
- 失败资产重试也不需要额外“确认入库”

如果后续真要支持“只补入库”，建议单独设计为“仅入库重试”能力，而不是把旧的
“预入库校验 / 确认入库”直接搬到手动重试旁边。

## 7. 推荐后续动作

建议按下面顺序调整：

1. 先移除剖分主页面上的“预入库校验 / 确认入库”按钮。
2. 再统一 `PartitionJobStore`、task result 和前端状态判断中的 `ingest_status` 语义。
3. 最后决定是否保留 `/v1/ingest/optical/*`，以及是否把它收敛成“仅补入库”工具。
