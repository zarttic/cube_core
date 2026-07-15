# 剖分、数据集管理与质检子系统重构计划

> 状态：历史开发方案，已由 M1-M3 实现替代；不作为当前生产契约。
> 编制时间：2026-07-13
> 适用范围：`cube_encoder`、`cube_split`、`cube_web`、`cube_web/frontend`
> 实施原则：当前系统仍处于开发阶段，允许破坏式重构，不做历史数据迁移，不保留旧格网和旧接口兼容层。

当前格网、严格剖分、质量、发布和验收契约以代码、测试、`AGENTS.md` 及
`docs/milestone_coordination_ledger.md` 为准。本方案保留原始设计背景，其中的
“当前实现基线”、`published` 状态和外部 publication gateway 设想均不是当前行为。

## 1. 背景与目标

本次重构处理以下业务要求：

1. 格网模型严格限定为 Geohash、MGRS、ISEA4H 三类，格网类型与剖分方式不得混用。
2. 一个批次可以包含多个数据集，后续查询、管理、质检和发布全部以数据集为主维度。
3. 建立剖分数据入库与管理模块，分别管理数据集、瓦片、索引、格网、质检和发布信息。
4. 质检页面改为全部质检记录主表，错误结果必须完整保存并支持全部导出。
5. 统一光学、雷达、信息产品和碳卫星的波段字段与展示方式。
6. 载入子系统已经完成 COG 转换，剖分子系统不得再次转换或重投影 COG。
7. 剖分结果明细成功写入后，必须为数据集写入明确的剖分完成状态和完成时间。

## 2. 范围与非目标

### 2.1 本次范围

- 重构格网 SDK、Web API、剖分作业和前端格网选择。
- 删除 S2、Tile Matrix、Plane Grid 及其生产代码、测试、配置和文档引用。
- 新增 Geohash 引擎并修正 MGRS 实现。
- 移除剖分链路中的 COG 转换。
- 新增数据集结果域表结构、服务和 API。
- 重做质检记录、规则、错误明细与导出。
- 新增数据集管理页面，重构质检页面和剖分参数区域。
- 统一波段数据契约。

### 2.2 非目标

- 不迁移当前开发库中的旧剖分记录。
- 不兼容旧格网编码、旧请求参数和旧返回结构。
- 不保留 `s2`、`tile_matrix`、`plane_grid` 的隐藏入口或别名。
- 不在剖分子系统重新实现载入子系统的 COG 生成能力。
- 不在本阶段建设完整的数据门户、订单、下载审批或计费系统。

## 3. 当前实现基线

### 3.1 格网

当前 `cube_encoder` 同时定义了 `s2`、`mgrs`、`isea4h`、`tile_matrix`、`plane_grid`：

- `cube_encoder/grid_core/app/core/enums.py`
- `cube_encoder/grid_core/app/engines/registry.py`
- `cube_encoder/grid_core/app/engines/s2_engine.py`
- `cube_encoder/grid_core/app/engines/tile_matrix_engine.py`
- `cube_encoder/grid_core/app/engines/mgrs_engine.py`
- `cube_encoder/grid_core/app/engines/isea4h_engine.py`

Web 请求模型只声明了部分格网，而剖分 runner 又允许更多格网，契约存在分裂：

- `cube_web/cube_web/schemas.py`
- `cube_web/cube_web/services/partition_runners.py`
- `cube_web/frontend/src/views/PartitionView.vue`

当前仓库没有 Geohash 引擎。

### 3.2 剖分调度和结果

当前 OpenGauss 表主要用于任务调度与重试：

- `partition_batches`
- `partition_assets`
- `partition_job_attempts`

这些表可继续承担调度职责，但不能代替数据集结果域。当前没有以数据集为主记录并关联瓦片、索引、格网、质量和发布明细的完整模型。

### 3.3 COG 处理

`cube_split/cube_split/jobs/ray_partition_core.py` 中仍包含 `convert_asset_to_cog`、COG 缓存、上传和转换耗时统计。新链路应直接消费载入子系统提供的 COG URI，只保留从 MinIO 下载到 worker 本地缓存的能力。

### 3.4 质检

当前质检报告主要整体保存在 `quality_reports` JSONB 字段中，部分错误指标在生成报告时只保留前 20 条。前端历史记录位于 `QualityHistoryDrawer.vue` 抽屉中，不符合“全部质检记录表作为主界面”的要求。

### 3.5 数据集参考

NODA 数据集页面以数据集为主对象，包含标题、摘要、时间范围、空间范围、关键词、来源、格式、质量和共享方式等元数据。本项目只采用其中与剖分管理直接相关的最小字段集，不复制门户、申请和下载审批能力。

- 数据集检索：https://noda.ac.cn/datasharing/search
- 数据集详情示例：https://noda.ac.cn/datasharing/datasetDetails/1

## 4. 目标业务契约

### 4.1 格网与剖分方式

| `grid_type` | 业务名称 | 固定剖分方式 | 目标能力 |
|---|---|---|---|
| `geohash` | 经纬度格网 | `logical` | 全球定位、覆盖、拓扑、层级和编码 |
| `mgrs` | 平面格网 | `logical` | 标准 MGRS/UTM/UPS 定位、覆盖、几何和邻接 |
| `isea4h` | 六边形格网 | `entity` | 全球六边形覆盖、实体瓦片生成和编码 |

剖分方式不再由用户选择，统一从格网类型派生：

```text
geohash -> logical
mgrs    -> logical
isea4h  -> entity
```

后端仍保存派生后的 `partition_method`，用于查询、审计和展示；请求中若传入与格网不一致的值，直接返回 422。

### 4.2 MGRS 必须先确认的边界

MGRS 不是 Geohash 式的全球单一连续层级树，它由 UTM zone、纬度带、100 km 方格和精度数字组成，极区还涉及 UPS。

本计划默认采用以下验收语义：

- 全球有效坐标均可获得标准 MGRS 或 UPS 编码。
- 编码中保留 zone、band 和精度，不伪造全局连续列号或行号。
- cover 能跨 zone、反经线和 UTM/UPS 边界。
- parent、children 和 neighbors 只承诺标准允许且语义明确的结果。
- 不要求 MGRS 提供与 Geohash 相同的全球统一父子树。

如果业务要求 MGRS 具备 Geohash 式全球连续父子关系，应在开发前重新确认格网名称和标准，不能通过非标准编码冒充 MGRS。

### 4.3 数据集、批次和资产关系

```text
partition_batch 1 ----- n partition_dataset
partition_dataset 1 --- n source_asset
partition_dataset 1 --- n band
partition_dataset 1 --- n tile
partition_dataset 1 --- n index_record
partition_dataset 1 --- n grid_cell
partition_dataset 1 --- n quality_run
partition_dataset 1 --- n publication
```

约束：

- 载入接口必须提供稳定的 `dataset_id`、`dataset_code` 和 `dataset_title`。
- 禁止使用目录名、运行目录或文件路径临时推导数据集身份。
- 一个批次可以包含多个数据集。
- 一个数据集只能有一条主记录。
- 批次用于调度，数据集用于业务管理，两者不能混为同一对象。

### 4.4 状态模型

剖分、质检和发布使用独立状态字段：

```text
partition_status: pending | queued | running | completed | failed | cancelled
quality_status:   pending | running | pass | warn | fail
publish_status:   unpublished | publishing | published | failed | withdrawn
```

剖分完成约束：

- 只有数据集主记录和本次瓦片、索引、格网明细在同一事务中成功提交后，才能设置 `partition_status=completed`。
- `partition_status=completed` 时，`partition_completed_at` 必须非空。
- 质检失败不回滚已经完成的剖分结果，但阻止发布。
- 只有必选质检项全部通过时，数据集才能进入发布流程；是否允许 `warn` 发布由发布策略明确控制，默认不允许。

## 5. 目标数据模型

### 5.1 数据集主表 `partition_datasets`

建议字段：

| 字段 | 含义 |
|---|---|
| `dataset_id` | 载入系统提供的稳定主键 |
| `dataset_code` | 业务编码 |
| `dataset_title` | 数据集名称 |
| `batch_id` | 所属载入/剖分批次 |
| `data_type` | `optical`、`radar`、`product`、`carbon` |
| `product_type` | 具体产品类型 |
| `summary` | 数据集摘要 |
| `keywords` | 关键词数组 |
| `source_system` | 来源系统 |
| `time_start`、`time_end` | 时间范围 |
| `bbox` | WGS84 空间范围 |
| `grid_type` | 三类格网之一 |
| `grid_level` | 格网层级数值 |
| `grid_level_name` | 面向用户的层级名称 |
| `partition_method` | 从格网类型派生 |
| `partition_status` | 剖分状态 |
| `partition_completed_at` | 剖分完成时间 |
| `quality_status` | 最近一次质检状态 |
| `publish_status` | 当前发布状态 |
| `created_at`、`updated_at` | 审计时间 |

### 5.2 数据集资产表 `partition_dataset_assets`

保存数据集和载入资产的关系，至少包含：

- `dataset_id`
- `source_asset_id`
- `scene_id` 或 `observation_id`
- `cog_uri`
- `source_crs`
- `resolution`
- `acq_time`
- `bbox`
- `checksum`

`cog_uri` 必须指向载入系统已经生成的 COG。剖分系统只验证其可读性和元数据，不重新生成 COG。

### 5.3 数据集波段表 `partition_dataset_bands`

统一字段：

| 字段 | 含义 |
|---|---|
| `dataset_id` | 所属数据集 |
| `band_code` | 内部稳定编码 |
| `band_name` | 页面展示名称 |
| `band_type` | `spectral`、`polarization`、`variable` |
| `unit` | 单位，可为空 |
| `display_order` | 展示顺序 |
| `attributes` | 产品特有元数据 JSONB |

输入映射：

| 数据类型 | 输入字段 | 统一结果 |
|---|---|---|
| 光学 | `bands` 或 `band` | `band_type=spectral` |
| 雷达 | `polarization` 或 `band` | 大写 `band_code`，`band_type=polarization` |
| 信息产品 | `band` 或产品变量 | `band_type=variable` |
| 碳卫星 | `xco2` 等观测变量 | `band_type=variable` |

新契约发布后，载入接口应直接输出统一结构，剖分内部不再长期兼容 `bands`、`band`、`polarization` 多套命名。

### 5.4 瓦片表 `partition_tiles`

保存实体瓦片和逻辑瓦片引用：

- `tile_id`
- `dataset_id`
- `source_asset_id`
- `band_code`
- `grid_type`
- `grid_level`
- `space_code`
- `tile_uri`
- `tile_kind`：`logical_reference` 或 `entity_file`
- `bbox`
- `width`、`height`
- `byte_size`
- `checksum`
- `status`
- `created_at`

Geohash 和 MGRS 的逻辑剖分不复制像元，`tile_uri` 可指向源 COG，窗口信息放在索引表；ISEA4H 的实体剖分生成独立瓦片对象。

### 5.5 索引表 `partition_indexes`

保存时空索引和逻辑窗口：

- `index_id`
- `dataset_id`
- `tile_id`
- `source_asset_id`
- `scene_id` 或 `observation_id`
- `band_code`
- `acq_time`
- `time_bucket`
- `grid_type`
- `grid_level`
- `space_code`
- `st_code`
- `window_col_off`
- `window_row_off`
- `window_width`
- `window_height`
- `value_ref_uri`
- `created_at`

### 5.6 格网表 `partition_grid_cells`

按数据集保存实际使用的格网单元：

- `dataset_id`
- `grid_type`
- `grid_level`
- `grid_level_name`
- `space_code`
- `bbox`
- `geometry`
- `tile_count`
- `index_count`

同一数据集、格网类型、层级和空间编码必须唯一。

### 5.7 质检表

`partition_quality_runs`：

- 一次数据集质检一条主记录。
- 保存 `quality_run_id`、`dataset_id`、`batch_id`、`product_type`、状态、错误数量、告警数量、开始时间、完成时间和规则版本。

`partition_quality_results`：

- 每个质检规则一条结果。
- 保存规则编码、规则名称、是否必选、适用产品、状态、错误数量、指标和消息。

`partition_quality_errors`：

- 每个具体错误一条记录。
- 保存规则编码、资产/瓦片/索引标识、行号、字段、错误编码、错误消息和完整上下文 JSONB。
- 任何生成报告的展示限制都不能影响该表的完整写入。

### 5.8 发布表 `partition_publications`

保存：

- `publication_id`
- `dataset_id`
- `service_type`
- `service_name`
- `service_url`
- `version`
- `status`
- `published_at`
- `withdrawn_at`
- `error_message`
- `created_at`、`updated_at`

## 6. 目标 API

### 6.1 数据集管理

```text
GET  /v1/partition/datasets
GET  /v1/partition/datasets/{dataset_id}
GET  /v1/partition/datasets/{dataset_id}/assets
GET  /v1/partition/datasets/{dataset_id}/bands
GET  /v1/partition/datasets/{dataset_id}/tiles
GET  /v1/partition/datasets/{dataset_id}/indexes
GET  /v1/partition/datasets/{dataset_id}/grid
GET  /v1/partition/datasets/{dataset_id}/quality
GET  /v1/partition/datasets/{dataset_id}/publications
POST /v1/partition/datasets/{dataset_id}/publish
POST /v1/partition/datasets/{dataset_id}/withdraw
```

数据集列表支持分页和以下筛选：

- 关键词
- 数据类型
- 产品类型
- 批次
- 格网类型
- 剖分状态
- 质检状态
- 发布状态
- 时间范围

### 6.2 质检

```text
GET  /v1/quality/records
GET  /v1/quality/records/{quality_run_id}
GET  /v1/quality/records/{quality_run_id}/errors
GET  /v1/quality/records/{quality_run_id}/errors/export?format=csv
GET  /v1/quality/records/{quality_run_id}/errors/export?format=json
POST /v1/quality/runs
```

删除当前按 `optical`、`radar`、`product`、`carbon` 重复展开的 history/latest/report 路由，以 `data_type` 和 `product_type` 作为统一筛选字段。

错误导出要求：

- CSV 和 JSON 均导出全部错误，不接受页面分页参数。
- 大结果采用流式响应，避免一次性加载全部错误到 Web 进程内存。
- 文件名包含数据集编码、质检时间和质检运行 ID。

## 7. 质检规则矩阵

### 7.1 状态判定

- 必选规则失败：质检总状态为 `fail`。
- 可选规则异常：质检总状态为 `warn`。
- 所有已启用规则通过：质检总状态为 `pass`。
- `error_count` 表示具体错误明细数量，不是失败规则数量。
- `warning_count` 表示具体告警明细数量。

### 7.2 默认规则

| 数据类型 | 必选规则 | 可选规则 |
|---|---|---|
| 光学 | 索引字段、COG 可读性、资产 CRS、窗口范围、格网 bbox、时间桶、格网/剖分方式一致性 | 像元抽样、重复场景波段 |
| 雷达 | 索引字段、COG 可读性、资产 CRS、窗口范围、格网 bbox、时间桶、极化字段、格网/剖分方式一致性 | 像元抽样、重复资产 |
| 信息产品 | 索引字段、COG 可读性、资产 CRS、窗口范围、格网 bbox、产品时间或年份、产品变量、格网/剖分方式一致性 | 数值范围统计、年份连续性 |
| 碳卫星 | 观测字段、坐标、时间桶、XCO2 值域、格网编码、格网/剖分方式一致性 | 质量标记、重复观测、足迹几何 |

`COG 可读性`属于输入资产质检，不表示剖分子系统负责生成 COG。代码中的 `cog_crs` 等容易产生职责误解的规则名应改为 `asset_crs`、`asset_readability`。

## 8. 前端设计

### 8.1 剖分页面

格网选择只保留：

- 经纬度格网 Geohash
- 平面格网 MGRS
- 六边形格网 ISEA4H DGGS

调整项：

- 删除逻辑/实体剖分单选控件。
- 格网类型、格网层级、格网层级名称和地图操作放在同一参数区。
- 选择格网后直接展示派生的剖分方式，只读不可修改。
- 地图同时展示数据集范围和当前格网。
- “加载格网”保留为明确命令。
- “清空格网”统一改为“重置”。
- “恢复默认”等同类操作统一为“重置”。
- “重置”和“提交剖分”在同一操作行并排展示。
- “格网对象”统一改为“瓦片数据”或“格网单元”，根据实际对象选择准确名称。
- 执行进程中删除“生成 COG”和 COG 转换耗时。

### 8.2 数据集管理页面

新增 `/datasets`，首屏为数据集表格，不使用营销式卡片布局。

主表字段：

- 数据集名称
- 数据类型
- 产品类型
- 所属批次
- 波段/变量
- 格网类型和层级名称
- 瓦片数量
- 索引数量
- 剖分状态
- 质检状态
- 发布状态
- 剖分完成时间
- 操作

详情按标签页展示：概览、资产、波段、瓦片、索引、格网、质检、发布。

打开或切换详情前必须先重置当前 `dataset_id`、详情数据、选中行和分页状态，避免展示上一条数据集内容。

### 8.3 质检页面

将 `/quality` 改为全部质检记录主表。当前历史记录抽屉不再作为主入口。

主表字段：

- 数据集
- 批次
- 产品类型
- 剖分状态
- 质检状态
- 错误数量
- 告警数量
- 质检时间
- 操作

操作：

- 查看质检详情
- 查看错误明细
- 导出全部错误
- 重新质检

错误明细允许分页展示，但导出必须读取完整错误表。

## 9. 分阶段实施计划

### 阶段 0：冻结契约

任务：

- [ ] 书面确认第 4.2 节的 MGRS 全球语义。
- [ ] 确认三类格网的允许层级和层级展示名称。
- [ ] 确认载入接口必传 `dataset_id`、`dataset_code`、`dataset_title` 和统一波段结构。
- [ ] 确认必选/可选质检规则和 `warn` 发布策略。
- [ ] 固定数据集主记录、瓦片、索引和质检错误契约。

完成标准：接口和表字段不再存在影响实现方向的未决项。

### 阶段 1：重构格网 SDK

任务：

- [ ] 新增 `GeohashEngine`，实现 locate、cover、geometry、neighbors、parent、children。
- [ ] 重写或修正 `MGRSEngine` 的 UPS、跨 zone、反经线和大范围 cover。
- [ ] 保留并验证 `ISEA4HEngine`。
- [ ] 将 `GridType` 缩减为 `geohash`、`mgrs`、`isea4h`。
- [ ] 删除 S2、Tile Matrix、Plane Grid 引擎和测试。
- [ ] 更新 SDK、FastAPI 请求模型、ST code 和 Web SDK。

验证：

- [ ] 三类格网的定位、覆盖、拓扑和编码测试通过。
- [ ] Geohash 全球边界和反经线测试通过。
- [ ] MGRS UTM/UPS、跨 zone 和反经线测试通过。
- [ ] ISEA4H 覆盖和实体瓦片所需几何测试通过。

### 阶段 2：重构剖分执行链路

任务：

- [ ] 从请求模型、配置、CLI 和前端删除 `cog_workers` 等转换参数。
- [ ] 删除 `convert_asset_to_cog` 及转换、重新上传和转换耗时统计。
- [ ] 输入统一使用载入系统的 `cog_uri`。
- [ ] MinIO 输入只下载到 worker 本地缓存，不改变格式或 CRS。
- [ ] 从 `grid_type` 派生剖分方式并在服务入口统一校验。
- [ ] 按 `dataset_id` 分组执行，同一批次可生成多个数据集结果。
- [ ] 统一输出瓦片、索引和格网记录。

验证：

- [ ] 光学、雷达、信息产品和碳卫星最小任务通过。
- [ ] Ray worker 可直接消费 MinIO COG。
- [ ] 结果和日志中不再出现剖分阶段 COG 转换。
- [ ] Geohash/MGRS 只产生逻辑结果，ISEA4H 只产生实体结果。

### 阶段 3：建立数据集结果域

任务：

- [ ] 建立第 5 节定义的主表和明细表。
- [ ] 建立数据集、状态、格网编码、ST code、波段和时间索引。
- [ ] 实现数据集结果事务写入服务。
- [ ] 实现重复任务幂等写入，幂等键以数据集和任务输出标识为准。
- [ ] 明细提交成功后写入 `partition_completed_at`。
- [ ] 失败时回滚本次结果写入并保留任务失败状态。

验证：

- [ ] 一个批次可关联多个数据集。
- [ ] 一个数据集只有一条主记录。
- [ ] 数据集可独立查询资产、波段、瓦片、索引和格网。
- [ ] 明细写入失败时不会出现错误的完成标记。

### 阶段 4：实现数据集管理 API

任务：

- [ ] 实现第 6.1 节接口。
- [ ] 实现分页、筛选和排序。
- [ ] 批次详情增加数据集列表，最终业务详情跳转到数据集。
- [ ] 实现发布和撤回状态记录。

验证：

- [ ] 列表按数据集展示而不是按批次展示。
- [ ] 所有明细接口均受 `dataset_id` 约束。
- [ ] 发布前校验剖分完成和质检状态。

### 阶段 5：重构质检

任务：

- [ ] 将质检入口改为按 `dataset_id` 执行。
- [ ] 实现规则元数据和第 7 节规则矩阵。
- [ ] 将规则结果和每条错误分别落表。
- [ ] 删除持久化前的 `[:20]` 错误截断。
- [ ] 实现统一质检记录、详情、错误分页和重新质检接口。
- [ ] 实现 CSV/JSON 全量错误流式导出。
- [ ] 同步更新数据集最近质检状态和错误数量。

验证：

- [ ] 必选项失败产生 `fail`，可选项异常产生 `warn`。
- [ ] 页面预览数量不影响落表数量和导出数量。
- [ ] 导出错误数与数据库错误明细数完全一致。

### 阶段 6：重构前端

任务：

- [ ] 剖分页面只展示三类格网。
- [ ] 删除剖分方式选择，显示自动派生结果。
- [ ] 统一格网类型、层级、层级名称和地图操作区。
- [ ] 统一“重置”文案和按钮位置。
- [ ] 统一波段筛选和展示字段。
- [ ] 新增 `/datasets` 数据集管理页面。
- [ ] 将 `/quality` 改为全量质检记录主表。
- [ ] 实现错误详情和全量导出入口。
- [ ] 所有详情抽屉在打开前重置记录 ID 和旧状态。

验证：

- [ ] 桌面和移动视口无文字或控件重叠。
- [ ] 格网能在地图加载并重置。
- [ ] 重置和提交按钮并排且状态明确。
- [ ] 数据集和质检主表可筛选、分页和进入详情。

### 阶段 7：删除历史实现并全量回归

删除项：

- [ ] S2、Tile Matrix、Plane Grid 引擎、作业分支、默认配置和测试。
- [ ] 旧格网前端选项和标签。
- [ ] 逻辑/实体手工组合入口。
- [ ] 剖分阶段 COG 转换代码和参数。
- [ ] 旧质量 history/latest/report 重复路由。
- [ ] 旧质检历史抽屉主入口。
- [ ] 旧文档中的格网矩阵和 COG 转换描述。

仓库检查：

```bash
rg -n "s2|tile_matrix|plane_grid" cube_encoder cube_split cube_web
rg -n "convert_asset_to_cog|cog_workers|COG耗时|生成 COG" cube_split cube_web
rg -n "清空格网|格网对象" cube_web/frontend/src
```

允许出现的结果仅限带日期的历史性能报告；历史报告必须明确标注为历史测量，不得被生产代码或当前契约引用。

## 10. 完整验收标准

### 10.1 格网

- [ ] 编码和剖分 API 只接受 `geohash`、`mgrs`、`isea4h`。
- [ ] Geohash 和 MGRS 永远使用逻辑剖分。
- [ ] ISEA4H 永远使用实体剖分。
- [ ] 前后端不能构造概念混用的请求。
- [ ] 仓库生产代码中不存在额外格网。

### 10.2 COG 职责

- [ ] 载入系统输出 COG URI。
- [ ] 剖分系统不转换或重投影 COG。
- [ ] 剖分系统仍验证 COG 可读性、CRS 和窗口范围。

### 10.3 数据集管理

- [ ] 一个批次可包含多个数据集。
- [ ] 一个数据集只有一条主记录。
- [ ] 瓦片、索引、格网、质量和发布信息均有独立明细记录。
- [ ] 数据集剖分成功后存在完成状态和完成时间。
- [ ] 所有业务列表和详情以数据集为主维度。

### 10.4 质检

- [ ] 全部质检记录表是质检页面首屏。
- [ ] 主表包含数据集、批次、产品类型、剖分状态、质检状态、错误数量、质检时间和操作。
- [ ] 必选和可选规则有明确元数据。
- [ ] 不同产品类型使用明确的适用规则矩阵。
- [ ] 全部错误完整落表。
- [ ] CSV/JSON 导出数量与错误表一致。

### 10.5 前端

- [ ] 波段名称、筛选值和详情展示使用统一字段。
- [ ] 格网类型、层级、层级名称和地图操作在同一区域。
- [ ] “清空格网”全部改为“重置”。
- [ ] “格网对象”全部替换为准确的“瓦片数据”或“格网单元”。
- [ ] “重置”和“提交剖分”并排展示。
- [ ] 详情组件不会复用上一条记录数据。

## 11. 验证命令

阶段内先运行对应窄测试，最终运行完整回归：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest
```

Web 后端窄范围回归：

```bash
cd cube_web
PYTHONPATH=../cube_encoder:../cube_split:. python3.11 -m pytest tests
```

前端构建：

```bash
cd cube_web/frontend
npm run build
```

分布式剖分最终必须使用 Ray 后端验证，不能只用 thread/process/local 结果替代。建议至少覆盖：

- Geohash 逻辑剖分单数据集。
- MGRS 跨 zone 逻辑剖分单数据集。
- ISEA4H 小层级实体剖分单数据集。
- 一个批次包含两个数据集。
- 质检失败并导出全部错误。
- 质检通过后发布、撤回。

## 12. 推荐执行顺序

```text
冻结契约
  -> 格网 SDK
  -> 剖分去 COG 转换
  -> 数据集结果域
  -> 数据集管理 API
  -> 质检重构
  -> 前端重构
  -> 历史代码删除
  -> 全量与 Ray 回归
```

不得先单独改前端文案或隐藏旧选项。格网契约、执行链路和数据模型完成后再接入页面，避免页面与后端再次形成两套语义。
