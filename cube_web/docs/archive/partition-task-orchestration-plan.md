# 数据驱动剖分任务编排开发规划

更新时间：2026-05-30

归档状态：本文是 2026-05-30 的阶段设计稿，部分内容已经在生产代码中落地，包括
`run` 生产操作、`PartitionJobStore`、批次/资产/attempt API、取消接口、质量状态回写
和演示 seed opt-in。当前实现、接口和测试命令以
[../README.md](../README.md) 与代码为准；本文只用于追溯设计背景，不作为最新操作手册。

## 1. 背景与目标

当前数据剖分页面的批次、资产、schema 主要写在前端
`cube_web/frontend/src/views/PartitionView.vue` 中。页面将选中的静态批次转换为
`selected_assets` 或 `selected_observations`，再调用
`/v1/partition/{data_type}/tasks/{demo|test|retry}`。后端
`cube_web.services.partition_service.PartitionTaskStore` 使用进程内
`ThreadPoolExecutor` 和内存字典保存任务状态，任务重启后不可恢复，也没有批次级和单景级失败记录。

目标是把剖分从“前端样例数据触发”升级为“外部系统 schema 同步后的数据库任务池触发”：

- 外部系统推送或同步待剖分 schema 后，系统能入库为待处理批次和资产。
- 剖分成功的数据从待剖分页移除，数据库打上已剖分标签。
- 剖分失败后自动重试一次；仍失败则进入人工处理队列。
- 支持按批次、按单景筛选失败记录并重跑。
- 支持人工调整配置后手动重跑。
- 支持手动终止正在执行的剖分任务。

## 2. 当前实现证据

| 领域 | 当前实现 | 主要差距 |
| --- | --- | --- |
| 前端数据来源 | `PartitionView.vue` 顶部硬编码 `opticalBatches`、`carbonBatches`、`productBatches` | 不能从数据库动态加载待处理批次，也不能按处理状态隐藏成功数据 |
| 任务 API | `cube_web/cube_web/routes/partition.py` 仅有同步 demo/retry/test、异步提交、查询任务 | 缺少任务列表、取消、失败筛选、单景重试、批次重跑 API |
| 任务状态 | `PartitionTask` 只有 `queued/running/completed/failed`、result、error | 无持久化、无 `cancel_requested/cancelled/retrying/manual_required` 等状态 |
| 自动重试 | optical 可基于质检 WARN 资产缩小重试；product/entity/carbon 基本是完整请求重试 | 没有失败后自动重试一次的通用策略，也没有失败次数记录 |
| 结果持久化 | 质检报告有 `quality_reports` PostgreSQL store；配置有 `cube_web_configs` | 剖分批次、资产、尝试、错误、操作审计还没有同级表 |
| 剖分执行 | `partition_runners.py` 将 payload 转成 `cube_split` job 参数；Ray 作业在 `cube_split` 中执行 | 取消信号无法传递到 COG/Ray worker 过程，任务中断只能靠进程或作业外部机制 |

## 3. 目标架构

建议新增 `PartitionJobStore`，沿用现有 `PostgresQualityReportStore` 和
`PostgresConfigStore` 的风格，通过 `CUBE_WEB_POSTGRES_DSN` 或 `DATABASE_URL` 连接 PostgreSQL。

```text
外部系统 DB/schema 同步
        |
        v
partition_batches / partition_assets / partition_job_attempts
        |
        v
cube_web API: 列表、认领、提交、取消、重试、标记人工处理
        |
        v
PartitionOrchestrator
        |
        +--> cube_web.services.partition_runners payload 适配
        |
        +--> cube_split jobs / Ray / MinIO / quality report
        |
        v
状态回写：成功打标、失败记录、自动重试、人工队列
```

前端剖分页面只读后端“待处理/失败/人工处理/运行中”数据，不再维护静态批次清单。测试/demo 数据可以作为开发 seed 或独立演示模式保留，但不作为主流程数据源。

## 4. 数据模型建议

### 4.1 `partition_batches`

批次级主表，用于页面列表、整体重试、成功隐藏。

| 字段 | 说明 |
| --- | --- |
| `id BIGSERIAL PRIMARY KEY` | 内部主键 |
| `batch_id TEXT UNIQUE NOT NULL` | 外部系统批次号 |
| `batch_name TEXT NOT NULL` | 批次名称 |
| `data_type TEXT NOT NULL` | `optical/product/carbon/radar` |
| `source_system TEXT` | 来源系统 |
| `source_schema JSONB NOT NULL` | 原始同步 schema |
| `normalized_payload JSONB NOT NULL` | 已转换为 partition runner 可用的 payload |
| `status TEXT NOT NULL` | 批次状态 |
| `priority INT NOT NULL DEFAULT 0` | 调度优先级 |
| `attempt_count INT NOT NULL DEFAULT 0` | 批次尝试次数 |
| `max_auto_retries INT NOT NULL DEFAULT 1` | 默认自动重试一次 |
| `last_task_id TEXT` | 最近一次执行任务 |
| `last_error TEXT` | 最近错误摘要 |
| `partitioned_at TIMESTAMPTZ` | 成功完成时间；有值时默认不出现在待剖分页 |
| `manual_required_at TIMESTAMPTZ` | 自动重试后仍失败，需要人工介入 |
| `created_at/updated_at TIMESTAMPTZ` | 审计时间 |

推荐状态：

- `pending`：待剖分。
- `queued`：已进入执行队列。
- `running`：执行中。
- `cancel_requested`：人工请求终止，等待执行侧响应。
- `cancelled`：已终止，可重新排队。
- `retrying`：自动重试或人工重试排队中。
- `succeeded`：剖分成功，待剖分页默认隐藏。
- `failed`：失败但仍可自动重试。
- `manual_required`：自动重试耗尽，需要人工调整配置。
- `ignored`：人工确认不处理。

### 4.2 `partition_assets`

资产或单景级明细表，用于筛选单景失败并单独重跑。

| 字段 | 说明 |
| --- | --- |
| `asset_id TEXT NOT NULL` | 外部资产 ID；没有时用 source_uri hash |
| `batch_id TEXT NOT NULL` | 所属批次 |
| `scene_id TEXT` | 光学场景 ID 或产品年份标识 |
| `source_uri TEXT NOT NULL` | MinIO 或其他源 URL |
| `asset_payload JSONB NOT NULL` | 单资产 normalized payload |
| `status TEXT NOT NULL` | `pending/running/succeeded/failed/cancelled/manual_required` |
| `attempt_count INT NOT NULL DEFAULT 0` | 单资产尝试次数 |
| `last_error TEXT` | 最近错误 |
| `last_run_dir TEXT` | 最近输出目录 |
| `partitioned_at TIMESTAMPTZ` | 单资产成功时间 |

索引：

- `(batch_id, status)`
- `(data_type, status, updated_at DESC)`，可通过冗余 `data_type` 或 join 实现。
- `source_uri` 唯一或局部唯一，取决于外部系统是否允许重复推送同一源。

### 4.3 `partition_job_attempts`

执行尝试表，用于追踪自动重试、人工重跑和审计。

| 字段 | 说明 |
| --- | --- |
| `task_id TEXT PRIMARY KEY` | Web task id |
| `batch_id TEXT NOT NULL` | 批次 |
| `asset_ids TEXT[]` | 如果是单景重试，记录资产集合 |
| `operation TEXT NOT NULL` | `auto_run/auto_retry/manual_retry/cancel/test` |
| `status TEXT NOT NULL` | 尝试状态 |
| `attempt_no INT NOT NULL` | 第几次尝试 |
| `payload JSONB NOT NULL` | 本次实际请求 |
| `runner_result JSONB` | runner 返回结果 |
| `quality_report_id UUID` | 关联质检报告 |
| `error_type/error_message/error_detail` | 失败信息 |
| `started_at/finished_at TIMESTAMPTZ` | 执行时间 |
| `requested_by TEXT` | 人工操作人，自动任务可为 `system` |

### 4.4 `partition_task_events`

事件表记录状态迁移和人工操作，便于排障：

- `task_submitted`
- `task_started`
- `cancel_requested`
- `cancelled`
- `asset_failed`
- `batch_failed`
- `auto_retry_scheduled`
- `manual_config_updated`
- `manual_retry_submitted`
- `batch_succeeded`

## 5. Schema 同步与标准化

新增 `SchemaIngestService`，负责把外部系统同步来的 schema 转换成内部模型。

输入方式建议分两步实现：

1. 拉取模式：从已同步到本库或外部库的表读取。适合当前“应该是通过数据库同步”的情况。
2. 推送模式：补充 `POST /v1/partition/schemas/import`，用于外部系统直接推送或联调。

标准化规则：

- 光学和产品最终都转换为 `selected_assets`，字段至少包括 `source_uri`、`scene_id`、`acq_time`、`bands/band`、`corners/bbox`、`resolution`、`sensor`、`product_family`。
- 碳卫星转换为 `selected_observations` 或一个可被后端解析的观测选择条件；当前 runner 仍依赖本地 sample，后续需要把 source_uri 和观测文件读取真正数据化。
- `source_uri` 优先使用 `s3://cube/cube/source/...`，避免回退到某台机器的本地路径。
- 标准化时做基础校验：必填字段、时间格式、格网类型/层级、bbox/corners、对象扩展名、重复资产。
- 原始 schema 永久保留在 `source_schema`，标准化后的请求保存在 `normalized_payload`，便于人工修改和回放。

## 6. 调度与执行流程

### 6.1 自动剖分

1. schema 同步入库，批次状态为 `pending`。
2. 调度器按 `priority, created_at` 认领批次，将状态改为 `queued`。
3. 生成 `partition_job_attempts` 记录，提交到执行器。
4. 执行器调用现有 `PartitionService.submit` 或新的持久化 runner 封装。
5. runner 完成后：
   - 成功：批次和资产标记 `succeeded`，写 `partitioned_at`，页面待处理列表默认不显示。
   - 失败：记录错误，若 `attempt_count <= max_auto_retries`，进入 `retrying` 并自动提交一次；否则转 `manual_required`。

### 6.2 单景失败处理

当前 `cube_split` 多数作业以批次方式抛出异常，无法天然知道是哪一景失败。建议在 `partition_runners.py` 外层先按资产构建明细记录，并在以下位置补充错误归因：

- manifest 构建、源路径校验失败：可直接标记对应资产失败。
- COG 转换失败：捕获 source asset 与异常，写入 `partition_assets.last_error`。
- grid task 构建失败：记录 scene/source_uri。
- Ray worker 处理失败：让 worker 返回结构化失败项，而不是只让整个 job 抛异常。
- 质检 WARN 或失败：把 `quality_report.assets/checks` 映射回 source_uri 或 rows_path。

单景重试 API 接收 `asset_ids`，从 `partition_assets.asset_payload` 重新组装 `selected_assets`，不需要前端手工拼 payload。

### 6.3 手动重跑

人工处理时允许编辑“运行配置覆盖项”，而不是直接改原始 schema：

- `grid_type/grid_level/grid_level_mode`
- `target_crs`
- `cover_mode`
- `max_cells_per_asset`
- `ray_parallelism/chunk_size`
- `cog_*`
- `minio_*`
- `metadata_backend/asset_storage_backend`

重跑时将原始 `normalized_payload` 与人工 override 合并，生成新的 attempt。所有 override 写入 attempt payload 和事件表，便于审计。

## 7. 手动终止设计

新增 API：

- `POST /v1/partition/tasks/{task_id}/cancel`
- `POST /v1/partition/batches/{batch_id}/cancel`

取消语义分三层：

1. Web 任务层：状态从 `running` 改为 `cancel_requested`，执行器轮询到后停止继续提交新工作。
2. 本地线程层：`ThreadPoolExecutor` 无法强杀运行中的 Python 函数，只能协作式取消，因此 runner 需要在关键阶段检查 `CancellationToken`。
3. Ray 层：记录 Ray object refs 或 actor/job id 后，取消时调用 `ray.cancel(..., force=True)`；如果未来迁移到 Ray Jobs API，则保存 Ray job id 并调用 stop API。

需要补充取消检查点：

- manifest 构建前后。
- COG 转换每个 asset 前后。
- grid task 分组前后。
- Ray task 提交前、等待结果循环中。
- ingest/quality 前。

取消结果：

- 未开始的批次：`cancelled`。
- 执行中的批次：尽力取消；已完成资产保持 `succeeded`，未完成资产改 `cancelled` 或 `pending`，由 API 参数决定是否保留待重跑。
- 对 MinIO 已写入但未入库的中间产物，需要记录到 attempt，后续清理任务按 attempt 清理。

## 8. API 规划

批次与资产：

- `GET /v1/partition/batches?status=pending&data_type=optical&keyword=...`
- `GET /v1/partition/batches/{batch_id}`
- `GET /v1/partition/batches/{batch_id}/assets?status=failed`
- `POST /v1/partition/batches/{batch_id}/run`
- `POST /v1/partition/batches/{batch_id}/retry`
- `POST /v1/partition/batches/{batch_id}/cancel`
- `POST /v1/partition/assets/retry`，body: `{ "asset_ids": [...], "config_override": {...} }`
- `POST /v1/partition/batches/{batch_id}/ignore`
- `PATCH /v1/partition/batches/{batch_id}/config-override`

任务：

- `GET /v1/partition/tasks?batch_id=...&status=...`
- `GET /v1/partition/tasks/{task_id}`
- `POST /v1/partition/tasks/{task_id}/cancel`

Schema 同步：

- `POST /v1/partition/schemas/import`
- `POST /v1/partition/schemas/sync`
- `GET /v1/partition/schemas/{batch_id}/preview`

保留现有 `/demo/test/retry` 接口用于兼容和开发验证，但前端主流程迁移到批次 API。

## 9. 前端改造

剖分页面建议拆成四个视图：

1. 待处理批次：显示 `pending/queued/running/retrying`，成功数据默认不显示。
2. 失败与人工处理：显示 `failed/manual_required/cancelled`，支持按批次、资产、错误类型筛选。
3. 执行中任务：显示进度、当前阶段、运行时长、取消按钮。
4. 历史记录：显示成功批次、质量报告、输出路径、重跑入口。

交互要点：

- 选择批次后从后端拉取 assets/schema，不再依赖静态数组。
- 失败批次可“整体重试”；失败资产可多选后“单景重试”。
- `manual_required` 批次展示最近两次 attempt 的错误、配置 diff、质量报告链接。
- 运行中任务按钮提供“终止剖分”；确认弹窗说明取消是尽力协作式，已写产物会保留记录。
- 成功后列表自动刷新，批次从待处理视图移除，可在历史记录查到。

## 10. 分阶段实施

### 阶段 1：持久化任务池和只读列表

- 新增 `PartitionJobStore` 和数据库表。
- 新增 schema import/sync，将现有前端静态 demo 数据转为 seed，验证列表可从 API 加载。
- 前端把批次列表切到 `GET /partition/batches`。
- 保留原有任务执行路径。

验收：

- 重启 `cube_web` 后批次、资产、attempt 仍存在。
- 成功批次有 `partitioned_at` 后不再出现在待处理列表。

### 阶段 2：执行状态回写和自动重试一次

- 新增 `PartitionOrchestrator`，封装 runner 调用、attempt 记录和状态迁移。
- 失败时自动重试一次。
- 第二次仍失败转 `manual_required`。
- 现有 `/tasks/{demo|retry|test}` 可继续走内存任务，但批次 API 走持久化任务。

验收：

- 注入一次失败后自动产生第二条 attempt。
- 连续失败后批次进入人工处理列表。
- 成功后批次和资产标记 `succeeded`。

### 阶段 3：单景失败归因和单景重试

- 在 runner 和 `cube_split` 作业边界补充 asset-level 错误记录。
- 新增失败资产筛选和 `POST /partition/assets/retry`。
- 前端失败列表支持按单景选择重试。

验收：

- 某一景 source_uri 异常时只标记该 asset failed。
- 单景重试只提交该 asset 的 payload。

### 阶段 4：取消能力

- 为持久化任务增加 `cancel_requested/cancelled` 状态。
- runner 增加 `CancellationToken` 检查点。
- Ray 执行保存 refs 或 job id，取消时尽力停止。
- 前端执行中任务显示终止按钮。

验收：

- queued 任务可立即取消。
- running 任务在最近检查点停止，最终状态为 `cancelled`。
- 取消不会把批次误标为成功。

### 阶段 5：人工配置、审计和运维补齐

- 增加配置 override 编辑与 diff 展示。
- 增加事件表和操作审计。
- 增加后台清理中间产物的任务。
- 增加任务并发、优先级、锁超时、超时失败策略。

## 11. 测试计划

后端单元测试：

- schema 标准化：合法/缺字段/重复资产/非法 URI。
- store：状态迁移、attempt 插入、成功隐藏、失败筛选。
- orchestrator：成功、失败自动重试一次、二次失败进入人工处理。
- cancel：queued 取消、running cancel_requested、重复取消幂等。

API 测试：

- `GET /partition/batches` 状态过滤。
- `POST /partition/batches/{id}/run` 创建 attempt。
- `POST /partition/batches/{id}/retry` 保留 attempt 历史。
- `POST /partition/assets/retry` 只提交选中资产。
- `POST /partition/tasks/{id}/cancel` 状态正确。

集成测试：

- 使用小规模 ISEA4H `grid_level=1`、单景影像、`ray_parallelism=2`、`max_cells_per_asset=50` 验证 Ray 后端。
- 使用 MinIO `s3://cube/cube/source/...` 源数据，不依赖本地绝对路径。
- 验证成功批次从待处理页面消失，并可在历史记录找到。

## 12. 风险与建议

- `ThreadPoolExecutor` 不能强杀运行中的任务，取消必须做成协作式；Ray task 需要额外保存 refs 或迁移到 Ray Jobs API。
- 当前碳卫星 runner 仍偏 demo，真实 schema 驱动时需要补齐 source_uri 文件读取和观测选择逻辑。
- 单景失败归因需要 `cube_split` 作业返回结构化错误；只靠外层异常无法可靠筛出失败景。
- MinIO 凭据不要继续依赖默认 `minioadmin/minioadmin`，应沿用环境变量或节点 `/etc/default/minio` 读取策略。
- 成功隐藏不能删除记录；应通过 `partitioned_at/status=succeeded` 过滤，方便审计、回溯和重新入队。
- 外部 schema 同步建议做幂等：同一个 `batch_id` 重复同步时更新 schema 版本或保留 `source_schema_hash`，避免重复剖分。
