# 质检与重试失败场景报告

**版本**：2026-07-19

**适用范围**：剖分产物质检、质检运行重试、剖分任务重试、质检通过后的手动入库重试。

机器可读的完整矩阵见 [`quality_retry_failure_matrix.json`](quality_retry_failure_matrix.json)。该 JSON 是规则、错误码、当前行为、问题、解决方案和测试证据的基准；本文负责解释业务语义。

## 1. 质检终态

| 终态 | 触发条件 | 当前系统行为 | 是否可入库 |
| --- | --- | --- | --- |
| `pass` | 所有适用规则通过 | 记录完整结果，进入入库候选 | 可以 |
| `warn` | 只有可选规则有发现 | 记录告警；需要数据集或策略明确允许 | 默认不可以 |
| `fail` | 任一必选规则有发现 | 保存全部错误，阻止入库 | 不可以 |
| `error` | 规则、对象读取器或执行器异常 | 记录脱敏异常，`results_complete=false` | 不可以 |

`cancelled` 不是质检规则终态，当前质量运行没有对外取消入口；运行被租约回收或执行异常时进入 `error`，应恢复依赖后重新质检。

## 2. 质检规则与错误归属

### 共享必选规则

`index_schema`、`output_count_consistency`、`output_reference_integrity`、`grid_method_agreement`、`cell_bbox_validity`、`time_bucket_consistency`、`asset_readability`、`window_bounds`。

这些规则发现的是剖分索引、瓦片引用、格网边界、时间桶、对象可读性和像素窗口问题。除读取服务故障外，默认按“原批次局部重建”处理；源时间错误和源文件损坏必须退回载入系统。

### 非碳数据规则

- `asset_crs`：`missing_crs`、`invalid_crs` 通常先修正声明；`crs_metadata_mismatch` 需要核对源文件，文件本身错误时退回载入重制 COG。
- 光学、雷达、信息产品分别使用 `optical_band_contract`、`radar_band_contract`、`product_band_contract`，缺失或错误的波段字段属于元数据修正，不应伪造源数据。

### 碳卫星规则

`carbon_schema`、`carbon_coordinates`、`carbon_xco2_range`、`carbon_quality_flags`、`carbon_observation_duplicates`、`carbon_footprints` 均为必选。缺字段、坐标、物理范围、质量标识、重复观测和足迹问题应退回载入系统；不得在剖分层伪造观测值或足迹。

完整错误码映射以 JSON 和前端 `qualityLabels.js` 为准，当前内置码包括：

`missing_st_code`、`missing_tile_reference`、`missing_output_version`、`tile_grid_mismatch`、`tile_kind_mismatch`、`detail_grid_mismatch`、`invalid_bbox`、`missing_time_bucket`、`time_bucket_mismatch`、`invalid_carbon_source`、`invalid_cog_uri`、`invalid_checksum`、`object_reader_unavailable`、`source_object_unreadable`、`missing_crs`、`invalid_crs`、`crs_metadata_mismatch`、`missing_band_metadata`、`invalid_band_type`、`window_out_of_bounds`、`missing_carbon_indexes`、`missing_carbon_fields`、`duplicate_observation_id`、`missing_footprint`、`invalid_coordinates`、`xco2_out_of_range`、`missing_quality_flag`、`output_count_mismatch`。

## 3. 质检运行失败与恢复

| 场景 | 当前系统应对 | 当前问题 | 目标方案 |
| --- | --- | --- | --- |
| 没有当前输出 | 404，不创建运行 | 页面提示不够明确 | 剖分未完成时禁用质检并说明原因 |
| 当前输出在分配前改变 | 质量触发冲突，不覆盖新输出 | 用户不知道需刷新 | 刷新详情并提示输出已变化 |
| 指定历史输出 | 非管理员拒绝；历史完成不更新当前质量 | 当前/历史容易混淆 | 显式标记历史运行，不作为当前入库门禁 |
| 自动触发重复投递 | `trigger_event_id` 幂等；身份不一致冲突 | 手动请求缺幂等键 | 增加手动操作幂等键或明确并行语义 |
| 租约过期/attempt 不匹配 | 拒绝旧 worker 提交 | 缺少接管提示 | 租约回收后重新排队并展示原因 |
| 规则或对象读取异常 | `error`、结果不完整、异常脱敏 | 无独立系统恢复入口 | 系统故障恢复后重新质检，保留历史运行 |
| 导出筛选无结果 | 返回合法 0 行 CSV/JSON | 用户不知是空匹配还是无错误 | 导出前展示命中数并保留筛选摘要 |
| warn 未获入库许可 | 不进入入库候选 | 页面不直观 | 候选集合显示 warn 门禁和批准依据 |

质检详情支持错误分页、规则/错误码/字段筛选，以及全部错误 CSV/JSON 导出，不以页面当前页作为完整错误结果。

## 4. 剖分重试

剖分重试允许的原任务状态只有 `failed`、`cancelled`、`manual_required`，并且只能重试同一业务批次的最新任务。

- 数据集执行按波段独立产出 outcome；已有明确失败波段时只重试这些波段。
- 部分失败：当前只重试失败波段；同景的成功波段保持完成。
- 取消：当前以 `unfinished_units` 重试未完成波段，成功波段不动；取消历史尚未在数据管理页形成时间线。
- `manual_required`：当前允许人工重试失败单元，但页面没有按错误来源显示下一动作。
- 活动任务、已完成任务、非最新失败任务：返回 409，不重复提交或使用旧 payload。
- 不存在任务：返回 404。
- 历史 payload 无法解析或数据类型不规范：返回 409，需要重新创建批次；应增加 schema 版本和迁移提示。
- 显式空失败集合：不会扩大为整数据集，返回“没有失败数据单元”。

当前任务 attempt 和 `attempt_no` 会保存，但业务页面没有完整展示尝试链，也未定义历史成功输出在下一次失败后的有效性。

## 5. 入库重试

入库只接受质检 `pass`，或显式允许的 `warn`；`fail`、`error`、`pending`、`running`、`cancelled` 不得进入候选。

- 部分失败：运行状态 `partial_failure`，成功波段保持 `completed`，只重试失败波段。
- 全部失败：运行状态 `failed`，可重试全部失败波段或指定失败波段。
- 指定波段：只把该失败波段置为 `queued`，其他失败波段保持失败。
- 混入已完成、排队或运行中的波段：返回 409，原子拒绝，不修改任何波段。
- 不存在或不属于该 run 的波段：返回 404。
- 已经重新排队的失败波段再次重试：返回 409。
- 开始/完成/失败不符合状态机：返回状态转换错误，不写入终态。
- 取消运行：未完成波段置为 `cancelled`，已完成波段不变；终态运行不可再次取消。

当前会把 `retry_history` 写入波段 provenance，但数据入库页面还没有完整的波段级重试时间线，也没有统一的 cancelled 单元恢复入口。

## 6. `OPEN_ISSUES.md` 未闭环项

### 部分重剖分替换整个数据集当前输出

当前 `current_output_version` 指向最新任务整体输出，未参与本次重剖分的景/波段仍在旧 `superseded` 输出中，可能出现“历史有入库记录但当前发布校验认为未剖分”。

建议实现增量有效输出快照：记录 `base_version` 与 `replaced_band_unit_ids`，只替换本次单元，发布和质检针对当前有效集合而不是单一任务输出。验收标准是只重剖一景后，另一景原有剖分、质检、入库能力仍然可用。

### 重试历史和当前有效尝试语义未确定

当前保留 task attempt，但页面只突出当前状态；成功后再次失败时，用户无法判断哪个输出仍可用，也看不到完整失败原因和每次操作日志。

建议定义 `current_successful_attempt`：失败尝试永不覆盖已通过且已入库的有效结果；每个单元保存 `attempt_no`、输出版本、质检运行、入库运行和错误日志，页面提供时间线。验收标准是“失败→成功→再次失败”后仍能发布最近一次成功结果，并能查看三次尝试。

## 7. 已验证证据

- 规则与矩阵契约：`cube_web/tests/test_quality_retry_failure_catalog.py`。
- 质检规则和终态：`cube_web/tests/test_quality_rules.py`。
- OpenGauss 质量租约、历史输出和异常完成：`cube_web/tests/test_quality_repository_opengauss.py`。
- 质检入库门禁：`cube_web/tests/test_quality_ingest_bridge.py`。
- 剖分部分失败、取消、波段级重试：`cube_web/tests/test_partition_dataset_workflow.py`。
- 入库状态机、部分失败、取消和指定波段重试：`cube_web/tests/test_ingest_service.py`、`cube_web/tests/test_ingest_api.py`。
- 规则、错误码和恢复建议的前端展示：`cube_web/frontend/tests/unit/qualityLabels.spec.js`。

自动化测试证明的是当前实现的状态机和约束，不代表上述两个 `OPEN_ISSUES` 已修复；它们仍需按第 6 节的验收标准单独开发。
