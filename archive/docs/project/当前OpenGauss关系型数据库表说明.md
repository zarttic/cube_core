# 当前 OpenGauss 关系型数据库表说明

更新时间：2026-09-09  
适用范围：当前 `cube_web`、`cube_split` 生产链路使用的 OpenGauss 关系型数据库。

本文只整理数据库表，不包含 SQLite、本地文件、MinIO 对象或 Ray 运行时信息。

## 1. 表分组总览

| 表分组 | 表数量 | 主要用途 |
| --- | ---: | --- |
| 场景、数据集与载入域 | 16 | 数据集、场景、波段、载入批次及载入/剖分关联 |
| 剖分任务调度域 | 3 | 剖分批次、资产和任务尝试记录 |
| 版本化剖分域 | 17 | 剖分输出、瓦片、格网索引、质检、发布和事件 |
| 系统业务配置 | 1 | 保存可编辑的业务默认配置 |
| 入库结果域 | 7 | 当前入库链路写入的遥感、产品和碳卫星结果 |
| **合计** | **44** | |

数据库连接使用 PostgreSQL 兼容协议连接 OpenGauss；代码中的类名和环境变量仍保留
`Postgres` 命名，但实际部署数据库为 OpenGauss。

## 2. 场景、数据集与载入域

| 表名 | 主键/关键列 | 用途及关系 |
| --- | --- | --- |
| `datasets` | `dataset_id`；唯一 `dataset_code` | 数据集主表，保存数据类型、状态、自动入库开关和业务属性。被 `scenes`、`load_batch_sources` 等引用。 |
| `scenes` | `scene_id`；唯一 `identity_key`、`dataset_id + scene_key` | 场景主表，保存源数据地址、校验和、采集时间、范围、分辨率和场景状态。归属于一个 `datasets`。 |
| `scene_assets` | `scene_id + asset_id` | 场景资产表，保存原始资产、COG 地址、格式、校验和及空间/时间属性。归属于 `scenes`。 |
| `scene_bands` | `scene_id + asset_id + band_code`；唯一 `band_unit_id` | 场景波段表，保存波段、极化或变量信息，是波段级剖分、质检和入库的最小关联单位。 |
| `dataset_role_restrictions` | `dataset_id + role` | 数据集角色访问限制。 |
| `load_batches` | `load_batch_id` | 载入批次主表，区分子系统导入和数据集重新载入，保存批次状态。 |
| `load_batch_scenes` | `load_batch_id + scene_id` | 载入批次与场景的关联，保存本次载入的源地址、校验和及状态。 |
| `load_batch_sources` | `load_batch_id + source_load_batch_id + source_dataset_id` | 数据集重新载入的来源批次和来源数据集关系。 |
| `partition_runs` | `partition_run_id` | 一次提交的剖分运行记录，保存运行状态、发起人和时间。 |
| `partition_drafts` | `draft_id` | 前端尚未提交的待剖分草稿，不代表正式运行批次。 |
| `partition_run_scenes` | `partition_run_id + selection_id + scene_id` | 剖分运行选中的场景，保存格网配置、输出版本、幂等键和尝试次数。 |
| `partition_data_unit_grid_status` | `band_unit_id + grid_type + grid_level` | 波段单元在某格网层级上的剖分、质检和入库状态。 |
| `ingest_runs` | `ingest_run_id` | 正式入库运行记录，关联一个 `partition_runs`、一个 `datasets`。 |
| `ingest_run_scenes` | `ingest_run_id + scene_id`；唯一 `idempotency_key` | 入库运行中的场景明细，保存选中的 `band_unit_ids`、输出版本、状态和重试信息。 |
| `scene_dataset_audit` | `audit_id` | 场景归属数据集的分配、重新分配审计记录。 |
| `scene_domain_schema_version` | `singleton` | 场景域结构版本和安装统计。当前版本由 `scene_domain_schema.py` 管理。 |

## 3. 剖分任务调度域

| 表名 | 主键/关键列 | 用途及关系 |
| --- | --- | --- |
| `partition_batches` | `batch_id` | 剖分批次主表，保存数据类型、原始/标准化请求、优先级、批次状态以及入库状态。 |
| `partition_assets` | `asset_id`；外键 `batch_id` | 批次内的待剖分资产，保存场景、源地址、资产状态、运行目录和错误信息。 |
| `partition_job_attempts` | `task_id`；外键 `batch_id` | 每次调度/重试的任务尝试，保存资产列表、任务载荷、执行结果、失败原因和 Ray 任务 ID。 |

`partition_output_versions.task_id` 关联 `partition_job_attempts.task_id`；因此调度记录必须先于
版本化剖分结果创建。

## 4. 版本化剖分域

| 表名 | 主键/关键列 | 用途及关系 |
| --- | --- | --- |
| `partition_datasets` | `dataset_id`；唯一 `dataset_code` | 新版剖分域的数据集状态，保存格网类型、层级、剖分方式、覆盖模式、当前输出版本和当前质检运行。 |
| `partition_dataset_assets` | `dataset_id + source_asset_id` | 剖分域源资产，保存 `s3://` 源地址、源类型/格式、校验和、范围、坐标系及时间。 |
| `partition_dataset_bands` | `dataset_id + source_asset_id + band_code` | 剖分域源资产的波段、极化或变量信息。 |
| `partition_output_versions` | `dataset_id + output_version`；唯一 `output_version` | 一次剖分输出的版本记录，保存任务、格网、输出状态、对象前缀和结果数量。 |
| `partition_output_chunks` | `dataset_id + output_version + chunk_id` | 输出分块清单，保存分块对象地址、校验和、字节数及格网/瓦片/索引数量。 |
| `partition_logical_staging_rows` | `dataset_id + output_version + chunk_id + row_number` | 逻辑剖分分块的暂存行，`kind` 区分格网单元、瓦片和索引。 |
| `partition_grid_cells` | `output_id`；唯一数据集/版本/格网/空间编码组合 | 输出格网单元，保存空间编码、拓扑编码、范围/几何及其瓦片和索引计数。 |
| `partition_tiles` | `output_id`；唯一资产/波段/格网/空间/时间/瓦片类型组合 | 输出瓦片清单，保存瓦片对象地址、瓦片类型、尺寸、校验和及发布状态。 |
| `partition_indexes` | `output_id`；唯一资产/波段/格网/空间/时间/时空编码组合 | 输出索引记录，保存时空编码、窗口信息、值引用对象地址及属性。 |
| `partition_quality_runs` | `quality_run_id`；唯一数据集/版本/质检序号 | 一次自动或手动质检运行，保存规则快照、状态、错误/告警计数和领取信息。 |
| `partition_quality_results` | `quality_run_id + rule_code` | 每条质检规则的结果、指标、发现数和执行错误。 |
| `partition_quality_errors` | `quality_error_id` | 质检错误明细，关联规则、源资产、波段、瓦片、索引或输出记录。 |
| `partition_quality_warn_approvals` | `approval_id`；唯一 `quality_run_id` | 对允许继续处理的 Warn 质检结果进行人工批准。 |
| `partition_publications` | `publication_id` | 输出发布状态机，管理激活、撤回、失败、领取和重试。 |
| `partition_publication_targets` | `publication_id + source_asset_id + band_code` | 发布操作涉及的资产/波段目标清单。 |
| `partition_domain_outbox` | `event_id`；唯一数据集/版本/事件类型 | 输出版本完成事件的事务消息表，支持待处理、处理中和已投递状态。 |
| `partition_domain_schema_version` | `singleton` | 版本化剖分域结构版本。当前版本由 `partition_domain_schema.py` 管理。 |

### 4.1 关键约束

- 生产格网类型限定为 `geohash`、`mgrs`、`isea4h`。
- `geohash`、`mgrs` 使用逻辑剖分；`isea4h` 使用实体剖分。
- 源资产、输出分块、瓦片和索引引用的对象地址必须是 `s3://` 地址；数据库保存元数据、索引和引用，不保存影像二进制内容。
- `partition_output_versions` 是版本边界；格网单元、瓦片、索引、质检和发布记录均通过数据集/输出版本关联。
- `partition_datasets.current_output_version` 和 `current_quality_run_id` 用于指向当前生效的输出和质检运行。

## 5. 系统业务配置表

| 表名 | 主键/关键列 | 用途及关系 |
| --- | --- | --- |
| `cube_web_configs` | `scope` | 保存 `partition`、`ingest`、`quality` 三类可编辑业务默认配置，配置主体为 `JSONB`。不保存数据库 DSN、Ray 地址、MinIO 凭据或其他运行时连接信息。 |

## 6. 当前入库结果表

以下表由现行入库链路在 OpenGauss 中按数据类型创建并写入。它们是对外服务的结果/事实表，不替代前面的场景域和版本化剖分域。

| 表名 | 主键/关键列 | 用途及关系 |
| --- | --- | --- |
| `rs_ingest_job` | `job_id` | 入库作业状态、参数、统计、错误和重试记录；光学、产品、实体和碳卫星入库共用该表。 |
| `rs_raw_scene_asset` | 自增 `id`；唯一 `scene_id + band + version` | 逻辑剖分场景/波段资产结果，保存源 COG 引用、版本和入库作业。 |
| `rs_cube_cell_fact` | 自增 `id`；唯一格网/空间/时间/波段/版本组合 | 逻辑剖分格网事实，保存时空编码、格网几何、值引用、来源场景数、质量规则和版本。 |
| `rs_entity_tile_asset` | 自增 `id`；唯一数据集/场景/波段/格网/空间/时间/版本组合 | 实体剖分瓦片资产结果，保存瓦片地址、窗口尺寸、有效像元比例和元数据。 |
| `rs_product_asset` | 自增 `id`；唯一 `dataset + scene_id + version` | 信息产品 COG 资产结果。 |
| `rs_product_cell_fact` | 自增 `id`；唯一数据集/格网/空间/时间/产品波段/版本组合 | 信息产品格网事实，保存空间范围、值引用、样本均值和版本。 |
| `rs_carbon_observation_fact` | 自增 `id`；唯一卫星/观测/产品类型/版本组合 | 碳卫星观测事实，保存采集时间、格网编码、XCO₂、质量标识、足迹和源数据引用。 |

## 7. 主要关系

```text
datasets
  └─ scenes
       ├─ scene_assets ─ scene_bands
       ├─ load_batch_scenes ─ load_batches
       ├─ partition_run_scenes ─ partition_runs
       └─ ingest_run_scenes ─ ingest_runs

partition_batches
  ├─ partition_assets
  ├─ partition_job_attempts
  └─ partition_output_versions
       ├─ partition_output_chunks
       ├─ partition_logical_staging_rows
       ├─ partition_grid_cells
       ├─ partition_tiles ─ partition_indexes
       ├─ partition_quality_runs
       │    ├─ partition_quality_results
       │    ├─ partition_quality_errors
       │    └─ partition_quality_warn_approvals
       ├─ partition_publications ─ partition_publication_targets
       └─ partition_domain_outbox

ingest_runs
  └─ 入库完成后写入 rs_* 结果表
```

`datasets` 与 `partition_datasets` 都使用 `dataset_id` 表示数据集业务标识，但当前 DDL 没有
在两张数据集主表之间建立直接外键；使用时应以应用层的数据集契约和数据集编码保持一致。

## 8. DDL 来源

| 领域 | 当前 DDL 入口 | 版本/说明 |
| --- | --- | --- |
| 场景、载入、运行和入库域 | `cube_web/cube_web/services/scene_domain_schema.py` | `2026-07-23-scene-domain-v12` |
| 剖分任务调度域 | `cube_web/cube_web/services/partition_job_store.py` | `PostgresPartitionJobStore.ensure_schema()` 负责创建 |
| 版本化剖分、质检和发布域 | `cube_web/cube_web/services/partition_domain_schema.py` | `2026-08-09-partition-domain-v4` |
| 业务配置 | `cube_web/cube_web/services/config_store.py` | `PostgresConfigStore.ensure_schema()` 负责创建 |
| 入库结果 | `cube_split/cube_split/ingest/` 及实体剖分作业 | 按入库数据类型幂等创建/更新 `rs_*` 表 |

