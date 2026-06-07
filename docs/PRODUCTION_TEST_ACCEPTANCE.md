# 生产完整测试内容与验收条件

更新时间：2026-06-04

本文档用于从生产交付角度验证 `cube_core` 仓库。测试目标不是证明单个函数可用，而是证明生产链路在真实运行配置、真实基础设施和真实数据约束下可上线、可回滚、可排障。

## 1. 测试范围

### 1.1 包与模块

- `cube_encoder`：格网 SDK、FastAPI 模型、geohash、MGRS、tile_matrix、ISEA4H 引擎，拓扑和 ST code。
- `cube_split`：光学、产品、雷达、碳卫星剖分；Ray 分布式执行；MinIO 对象读写；PostgreSQL/SQLite 入库；质量检查；AOI/碳观测读取。
- `cube_web`：FastAPI 后端、SDK facade、剖分 run/task/batch API、批次状态存储、配置管理、质量报告存储、ingest preview/confirm、Vue/Vite 前端。

### 1.2 生产链路

- ARD schema 或 manifest 导入。
- 托管批次执行：`pending -> queued -> running -> succeeded/failed/manual_required/cancelled`。
- 生产剖分入口：`/v1/partition/{data_type}/run`、`/v1/partition/{data_type}/tasks/run`、`/v1/partition/batches/{batch_id}/run`。
- 失败重试：自动重试、人工批次重试、失败资产重试。
- 取消：queued 取消、running 取消请求、Ray pending refs 取消。
- 质量检查：光学/产品/碳报告生成、保存、查询、导出。
- 正式入库与回读：PostgreSQL metadata、MinIO COG/瓦片、AOI 读取、碳观测查询。

### 1.3 不作为生产验收主路径的内容

- `demo` endpoint 只验证旧客户端兼容，不作为生产调用点。
- `CUBE_WEB_LOAD_DEMO_PARTITION_SCHEMAS=1` 只在演示环境使用，生产验收默认必须关闭。
- 本地绝对数据路径只允许作为调试输入，生产验收数据源应优先使用 `s3://` 或 schema/manifest 明确声明的数据源。

## 2. 生产环境前置验收

### 2.1 运行时配置

测试内容：

- 环境变量、`CUBE_WEB_ENV_FILE`、本地 `.cube_web.env`、代码默认值的解析顺序。
- PostgreSQL、Ray、MinIO、门户导航配置只来自运行时，不写入 `cube_web_configs`。
- MinIO 凭据优先从运行时环境或节点 `/etc/default/minio` 读取，不假设默认账号。

验收条件：

- `/health?checks=config` 返回 `status=ok`。
- `postgres_dsn` 在接口和配置页面中已脱敏。
- `/v1/config/get` 返回 `runtime.postgres_dsn`、`runtime.ray_address`、`runtime.minio.endpoint`、`runtime.minio.bucket`。
- 更新 `/v1/config/update` 后，`cube_web_configs.config` 不包含 PostgreSQL DSN、Ray address、MinIO access key、MinIO secret key、门户 URL。
- 未设置 `CUBE_WEB_LOAD_DEMO_PARTITION_SCHEMAS` 时，生产启动不会自动 seed demo 批次。

### 2.2 基础设施连通性

测试内容：

- PostgreSQL 连接与 schema 创建权限。
- Ray client 连接、节点数量和资源发现。
- MinIO 连接、bucket 存在性、对象上传/下载。

验收条件：

- `/health?checks=all` 返回整体 `status=ok`，`failed_checks=[]`。
- `postgres` 检查能执行 `SELECT 1`。
- `ray` 检查能返回 alive nodes 和 cluster resources，资源数量符合当前集群预期。
- `minio` 和 `bucket` 检查通过。
- MinIO bucket 不存在时应明确失败，不隐式使用错误 bucket。

## 3. 自动化测试基线

### 3.1 单元与集成测试

测试内容：

- encoder 和 split 默认测试。
- Web 后端测试。
- 前端构建测试。
- Python 包构建。

验收条件：

- 以下命令全部通过，无失败、无错误：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests cube_split/tests
cd cube_web && PYTHONPATH=../cube_encoder:../cube_split:. python3.11 -m pytest tests
cd cube_web/frontend && npm ci && npm run build
cd cube_encoder && python3.11 -m build
cd ../cube_split && python3.11 -m build
cd ../cube_web && python3.11 -m build
```

- 构建产物不提交到后端包内，前端 `dist/` 只作为部署产物。
- pytest 中显式 skip 的真实集群 e2e 测试必须在生产验收阶段单独启用执行。

### 3.2 真实集群冒烟测试

测试内容：

- 使用 Ray、MinIO、PostgreSQL 跑小样本端到端链路。
- 覆盖 optical geohash、optical MGRS、optical ISEA4H、product geohash、carbon ISEA4H。
- 验证质量报告和 AOI 回读。

验收条件：

- `cube_split/scripts/run_all_partition_flows_smoke.py` 结果 `summary.status=pass`。
- 每个 case `status=pass`，`rows > 0`。
- 需要分布式验证的 case `execution_engine=ray`。
- 非碳栅格 case 在 demo/生产入库模式下 `metadata_backend=postgres`、`asset_storage_backend=minio`。
- 逻辑剖分输出行中的 `asset_path` 为 `s3://`。
- ISEA4H 实体剖分输出行中的 `asset_path` 和 `source_asset_path` 为 `s3://`，`uploaded_tile_count > 0`，`metadata_rows > 0`。
- 要求质量的 case 返回 `quality_status` 和 `quality_report_id`。
- AOI 回读输出 GeoTIFF 可由 rasterio 打开，band 数、width、height 都大于 0。

## 4. SDK 与格网编码验收

### 4.1 SDK/API 一致性

测试内容：

- Python `CubeEncoderSDK` 和 Web `/v1` SDK facade 的 locate、cover、topology、ST code 结果一致。

验收条件：

- `geohash`、`mgrs`、`tile_matrix`、`isea4h` 每种格网至少覆盖 locate、cover、code_to_geometry、parent/children、ST code generate/parse。
- 相同输入下 SDK 和 `/v1` 返回的 `space_code`、`grid_level`、bbox/geometry、`st_code` 一致。
- 非法经纬度、非法 level、非法 space_code、非 EPSG:4326 cover 请求返回明确 4xx 或 `ValidationError`，不返回 500。
- `ISEA4H` lazy engine 初始化后可重复调用，不影响其他格网引擎。

### 4.2 编码稳定性

测试内容：

- 固定样例点、bbox、时间戳的回归结果。
- ST code 解析与重建。
- cover 结果数量、bbox 范围和 parent/children 层级关系。

验收条件：

- 固定样例在连续两次测试中结果完全一致。
- `parse_st_code(generate_st_code(...))` 能还原 grid_type、level、space_code、time_code。
- `code_to_bbox` 坐标在 WGS84 合法范围内，且 bbox 有正面积。
- `children(parent(code), target_level)` 包含原 code 对应层级路径或符合引擎定义的父子关系。

## 5. ARD Schema 与 Manifest 验收

### 5.1 栅格类 schema

测试内容：

- `optical`、`product`、`radar` schema import。
- 每个 asset 字段校验。
- 配置覆盖和默认格网层级推导。

验收条件：

- `POST /v1/partition/schemas/import` 可导入非空 `assets`。
- 每个 asset 必须包含 `source_uri`、`scene_id`、`acq_time`、`sensor`、`product_family`、`resolution`、`corners`、band 信息。
- `acq_time` 必须是 ISO8601；`resolution > 0`；`corners` 必须是 4 个 `[lon, lat]` 且坐标范围合法。
- 缺字段、坏时间、坏坐标、空 assets 返回 422。
- 导入后 `partition_batches`、`partition_assets` 有对应记录，初始状态为 `pending`。
- 重复导入同一 `batch_id` 更新业务 payload，但不破坏已有 attempt 历史。

### 5.2 碳观测 schema

测试内容：

- `carbon` schema import。
- `selected_observations` 字段校验。

验收条件：

- 每个 observation 必须包含 `source_uri`、`observation_id`、`acq_time`、`sensor`、`product_family`、`resolution` 和位置字段。
- 位置可来自 `corners`、`footprint`/`footprint_geojson` 或 `lon/lat`。
- 坐标越界、空 observations、坏时间返回 422。
- 导入后 asset 行使用稳定 `asset_id`，可在批次详情中列出。

## 6. 剖分执行链路验收

### 6.1 光学逻辑剖分

测试内容：

- `grid_type=geohash`、`mgrs`、`tile_matrix` 的逻辑剖分。
- Ray backend、线程 fallback、COG 转换、MinIO s3 源下载。

验收条件：

- 生产 `run` 必须提供 `input_dir`、`manifest_path` 或 `selected_assets`，不能隐式使用 demo 数据。
- Ray 模式下 worker 从 MinIO 下载源 TIF 到本地缓存，在 worker 本地转 COG，再上传 COG 到 MinIO。
- 输出 `index_rows.jsonl` 非空，`job_report.json` 存在。
- `job_report` 中 `execution_engine=ray`、`partition_backend_used=ray`、`ray_address` 为运行时配置值。
- 每行包含必填索引字段：`scene_id`、`band`、`asset_path`、`acq_time`、`grid_type`、`grid_level`、`space_code`、`space_code_prefix`、`st_code`、`time_bucket`、cell bbox、window 信息。
- `asset_path` 指向 `s3://` COG，不能是 driver 节点本地 `/tmp/.../cog/*.tif`。
- `time_bucket` 与 `acq_time` 和 `time_granularity` 匹配。
- `max_cells_per_asset` 超限时任务失败并给出明确错误，批次状态进入 `failed` 或 `manual_required`。

### 6.2 光学实体剖分

测试内容：

- 光学页面选择 `grid_type=isea4h` 后触发实体剖分。
- 默认层级、手动层级、自动推导层级。
- 实体瓦片上传和 entity metadata 入库。

验收条件：

- 前端不出现独立“实体剖分”模块；实体剖分由光学遥感页格网类型 `ISEA4H` 触发。
- 小规模验收可用 `grid_level=1`、单景影像、`ray_parallelism=2`、`max_cells_per_asset=50`。
- 输出 `entity_index_rows.jsonl` 和兼容 `index_rows.jsonl`。
- 每行 `partition_type=entity`，包含 `entity_tile_uri` 或上传后的 `asset_path`、`source_asset_path`、window、valid pixel ratio。
- MinIO 中存在实体瓦片对象。
- PostgreSQL `rs_entity_tile_asset` 行数与 `metadata_rows` 对账一致。

### 6.3 产品剖分

测试内容：

- `product` geohash/tile_matrix 逻辑剖分。
- `product` ISEA4H 走实体剖分。
- 产品年份、产品名、sample mean。

验收条件：

- 输出 `index_rows.jsonl` 非空，`time_bucket` 为年份。
- `rs_product_asset` 行数等于去重后的产品场景数。
- `rs_product_cell_fact` 行数等于去重后的产品 cell fact 数。
- `value_ref_uri` 包含 `s3://...#window=col,row,width,height`。
- 质量报告 `product_years` 检查为 `PASS`；指定 expected years 时缺失年份只能接受为明确 `WARN`，不能静默通过。

### 6.4 雷达剖分

测试内容：

- `radar` geohash/MGRS/tile_matrix 逻辑剖分。
- `radar` ISEA4H 实体剖分。
- `.dat` 与 `.hdr` sidecar 下载。

验收条件：

- schema 驱动运行不依赖 Sentinel 文件名。
- `source_uri=s3://...dat` 时 worker 能下载 `.hdr` sidecar。
- 输出行包含 polarization/band，window 在资产尺寸内。
- 雷达逻辑链路可选择不入库；若配置 `metadata_backend=postgres`，对账规则与光学逻辑剖分一致。

### 6.5 碳卫星剖分

测试内容：

- `carbon` / `carbon_satellite` 观测事实剖分。
- `partition_backend=ray` 的观测 chunk 分发。
- `selected_observations` 子集执行。

验收条件：

- 输出 `carbon_observation_rows.jsonl` 非空。
- 每行包含 `satellite`、`product_type`、`observation_id`、`acq_time`、`time_bucket`、`grid_type`、`grid_level`、`space_code`、`st_code`、`xco2`、`quality_flag`、`center_lon`、`center_lat`。
- `xco2` 在质量规则预期范围内，坐标合法。
- `selected_observations` 只处理选定 observation/source index。
- Ray 模式下 `execution_engine=ray` 或 `partition_backend_used=ray`，取消请求可中断 pending chunks。

## 7. 批次、任务、重试与取消验收

### 7.1 异步 task

测试内容：

- `/v1/partition/{data_type}/tasks/run`。
- `/v1/partition/tasks/{task_id}` 轮询。
- 失败、成功、取消状态。

验收条件：

- 创建 task 返回 202，包含 `task_id`、`status=queued`、`data_type`、`operation=run`。
- 任务开始后状态变为 `running`。
- 成功后状态为 `completed`，`result` 包含 run_dir、rows_path、rows 或 total_index_rows。
- 异常后状态为 `failed`，`error` 可读。
- 未知 task_id 返回 404。

### 7.2 托管批次

测试内容：

- `/v1/partition/batches` 列表、详情、assets、attempts。
- `/v1/partition/batches/{batch_id}/run`。
- 同一批次重复提交。

验收条件：

- list 默认不显示 succeeded，`include_succeeded=true` 时显示。
- 详情返回 batch、assets、attempts 对账一致。
- 执行时 `partition_batches.status` 依次变为 `queued/running/succeeded`。
- 同一批次已有 active task 时，重复提交返回同一个 active task，不创建重复 attempt。
- attempt 记录包含 operation、payload、requested_by、started_at、finished_at、runner_result 或 error。

### 7.3 自动重试与人工重试

测试内容：

- transient/unknown 错误自动重试。
- source_missing/validation/permission 转人工或失败。
- 质量 WARN/FAIL 后转 `manual_required`。
- 失败资产重试。

验收条件：

- 自动重试次数不超过 `max_auto_retries`。
- 自动重试 attempt 的 operation 为 `auto_retry`，source_task_id 指向上一次 task。
- 不可重试错误最终状态为 `manual_required` 或 `failed`，`last_error` 和 `error_type` 有值。
- 质量 `WARN` 或 `FAIL` 时 batch 状态为 `manual_required`，并保存 `quality_status`、`quality_report_id`、`quality_failure_reason`。
- `/v1/partition/assets/retry` 只能接受同一批次且状态为 `failed/manual_required` 的资产；混批或非 retryable 资产返回 422。
- 失败资产重试只更新被选资产的 attempt_count/status，不误改其他资产。

### 7.4 取消

测试内容：

- queued task 取消。
- running task 取消请求。
- Ray pending refs 取消。

验收条件：

- queued 取消后 task、attempt、batch、assets 状态为 `cancelled`。
- running 取消后先进入 `cancel_requested`，runner 感知 cancellation_check 后最终 `cancelled`。
- Ray pending refs 调用取消逻辑，不继续提交新 chunk。
- 取消后不能写入 succeeded result，也不能触发自动重试。

## 8. 入库、对象存储与回读验收

### 8.1 普通栅格入库

测试内容：

- 光学/雷达使用 `ray_ingest_job`。
- PostgreSQL + MinIO 后端。
- 幂等 MERGE。

验收条件：

- `rs_raw_scene_asset` 行数等于去重后的 `(scene_id, band, version)`。
- `rs_cube_cell_fact` 行数等于去重后的 `(grid_type, grid_level, space_code, time_bucket, band, cube_version)`。
- `rs_ingest_job` 对应 job_id 最终 `status=succeeded`。
- 重复同一 job/version 不产生重复业务行，只更新 run_id/ingest_time。
- `value_ref_uri` 指向 MinIO COG，并带合法 window fragment。

### 8.2 产品入库

测试内容：

- `product_ingest_job`。

验收条件：

- `rs_product_asset` 和 `rs_product_cell_fact` 表自动创建。
- 业务唯一键冲突时 MERGE 更新，不重复插入。
- `product_year`、`product_band`、`sample_mean` 与 index rows 对账一致。
- `rs_ingest_job.stats_json.product_fact_rows` 与实际 fact 行数一致。

### 8.3 实体瓦片入库

测试内容：

- ISEA4H 实体瓦片 MinIO 上传。
- `rs_entity_tile_asset` 元数据写入。

验收条件：

- `uploaded_tile_count > 0`。
- 所有 `entity_tile_uri` 对象可 stat 和下载。
- `rs_entity_tile_asset` 按 `(dataset, scene_id, band, grid_type, grid_level, space_code, time_bucket, tile_version)` 幂等。
- `valid_pixel_ratio` 在 `[0, 1]`。
- `metadata_json` 包含 partition_type/data_type/source/window 元数据。

### 8.4 AOI 和碳查询回读

测试内容：

- `cube_split.read.aoi_reader.read_aoi_rgb`。
- `cube_split.read.carbon_query.query_carbon_observations`。

验收条件：

- AOI 查询能通过 `CubeEncoderSDK.cover_compact` 找到 space codes。
- PostgreSQL fact 查询能返回 value refs。
- rasterio 能通过 MinIO `/vsis3` 打开对象窗口并拼接输出 GeoTIFF。
- 输出 GeoTIFF band 数、width、height 大于 0，像素非全 0。
- 碳查询能按 bbox、time range、quality flags、product_type、cube_version 过滤，返回行的坐标都落在 bbox 内。

## 9. 质量检查与报告验收

### 9.1 光学/雷达质量

测试内容：

- `index_schema`、`time_bucket`、`cell_bbox`、`logical_duplicates`、`asset_readability`、`cog_crs`、`window_bounds`、`pixel_sample`。

验收条件：

- 生产通过标准为整体 `PASS`。
- `WARN` 可进入人工确认，但不能自动视作生产通过。
- `FAIL` 阻断入库，除非显式 `allow_failed_quality=true` 且有人工批准记录。
- `asset_readability` 必须支持 `s3://` 先解析到本地缓存或通过 MinIO/rasterio 打开，不能用 `Path.exists()` 判断 s3 URL。

### 9.2 产品质量

测试内容：

- 光学通用 index/asset 检查。
- 产品年份检查。

验收条件：

- 无 schema、时间桶、bbox、window、资产可读性失败。
- expected years 配置后，缺失年份为 `WARN` 或 `FAIL`，必须在验收记录中说明处理结论。

### 9.3 碳质量

测试内容：

- carbon schema、time bucket、coordinates、xco2 range、quality flags、duplicates、footprint。

验收条件：

- `carbon_schema`、`carbon_coordinates`、`xco2_range` 必须 `PASS`。
- quality flag 非标准值和重复 observation 可为 `WARN`，但必须进入人工确认。
- `summary.observation_rows` 与输出 jsonl 行数一致。

### 9.4 Web 质量报告存储

测试内容：

- `/v1/quality/{type}/run/latest/history/report/pdf/txt`。
- `quality_reports` PostgreSQL 表。

验收条件：

- `run` 后 `quality_reports` 写入或更新同一 run_dir 的报告。
- `latest` 返回对应 data_type 最新报告。
- `history` 支持 limit，按 generated_at/updated_at 降序。
- `report/pdf/txt` 用 report_id 获取，未知 report_id 返回 404。
- 报告记录包含 report_id、data_type、run_dir、status、summary、checks、assets。

## 10. Web API 与前端验收

### 10.1 API 合同

测试内容：

- `/health`。
- SDK facade。
- partition run/task/batch。
- quality。
- ingest preview/confirm。
- config。
- auth 开关。

验收条件：

- 所有业务 API 挂在 `/v1`。
- 开启 `CUBE_WEB_AUTH_REQUIRED` 时，无 token 的 `/v1/*` 请求被拒绝；关闭时本地可直接访问。
- API 错误格式稳定：GridCoreError 使用 `error.code/message`，业务异常使用 FastAPI `detail`。
- 生产剖分 API 新调用点只使用 `run`，不新增提交 `demo` operation 的生产入口。

### 10.2 前端工作流

测试内容：

- 导航：主页、剖分、质量、编码、配置。
- 批次列表筛选、详情、资产列表、attempt 列表。
- 开始执行、取消、继续重试、失败资产重试。
- 光学格网选择 geohash/tile_matrix/ISEA4H。
- 质量历史、报告详情、导出。
- 配置页面展示和保存。

验收条件：

- Vite 前端通过代理访问 `/v1`、`/api`、`/health`。
- 光学页选择 `ISEA4H` 时显示实体剖分层级输入，但不出现独立实体剖分模块。
- 批次执行时前端展示 prepare/queue/partition/persist 阶段，任务完成后显示 rows、run_dir、quality status。
- 取消 active batch 后 UI 状态刷新为 cancel_requested 或 cancelled。
- 失败资产重试按钮只对 retryable 资产可用。
- 配置页面可展示 PostgreSQL/Ray/MinIO runtime 信息，但保存时不会把这些 runtime 值写入业务配置表。
- 前端构建无错误，页面无明显文本溢出或控件遮挡。

## 11. 性能与容量验收

### 11.1 小规模冒烟性能

测试内容：

- 单景小 TIF、`ray_parallelism=2`、`chunk_size=1`。

验收条件：

- 冒烟链路稳定通过，不依赖 driver 本地 COG 路径。
- `ray_init_elapsed_sec`、`cog_elapsed_sec`、`partition_elapsed_sec`、`ingest_elapsed_sec` 都写入 job_report。
- 连续 3 次运行无随机失败。

### 11.2 生产规模试运行

测试内容：

- 按生产数据类型各选一个代表性批次。
- 光学/产品逻辑剖分默认层级 5 或按分辨率推导。
- ISEA4H 完整级别按资源窗口执行。
- Ray 并行度、chunk size、MinIO 上传并发、PostgreSQL 写入耗时。

验收条件：

- Ray 集群 CPU/内存资源利用在预期范围内，无 worker 大面积 OOM 或重启。
- MinIO 没有持续 5xx、上传失败或对象缺失。
- PostgreSQL 写入无死锁，业务唯一键冲突按 MERGE 处理。
- 产物行数、对象数、metadata 行数能按 job_report 对账。
- 大任务取消可在合理时间内停止提交新 Ray chunk。

## 12. 故障注入验收

测试内容：

- 缺失源对象：MinIO NoSuchKey。
- MinIO 凭据错误或 bucket 缺失。
- PostgreSQL DSN 缺失或连接失败。
- Ray 地址错误或集群不可达。
- manifest/schema 字段非法。
- max_cells_per_asset 超限。
- 质量 FAIL。

验收条件：

- 每个故障都有明确错误信息。
- 批次状态正确进入 failed/manual_required/cancelled。
- transient/unknown 错误按配置自动重试，不可重试错误不无限重试。
- 失败不会写入不完整的 succeeded attempt。
- PostgreSQL ingest job 在失败时记录 `status=failed` 和 `error_msg`。
- MinIO 部分上传失败后重复执行可幂等恢复。

## 13. 安全与发布验收

测试内容：

- 凭据泄露检查。
- 本地缓存和大数据文件检查。
- 生产/演示分离。
- Git 状态和 PR 交付信息。

验收条件：

- 仓库不提交 `.cube_web.env`、凭据、MinIO 明文密钥、本地绝对源数据路径。
- 不提交 `.pytest_cache/`、`__pycache__/`、虚拟环境、大型入库输入。
- 生产分支不包含 demo seed manifest、演示硬编码 source manifest、演示分支专用脚本。
- push 前测试结果、质量报告、冒烟 summary、UI 截图、影响路径写入交接说明或 PR。

## 14. 交付证据

生产验收完成后至少保留以下证据：

- 自动化测试命令和通过结果。
- `/health?checks=all` 响应摘要。
- 真实集群 smoke summary JSON。
- 每类数据至少一个 `job_report.json`。
- 每类要求质量的数据至少一个 `quality_report.json` 或 `quality_reports.report_id`。
- PostgreSQL 对账 SQL 结果：批次、attempt、metadata 表、ingest job。
- MinIO 对象前缀和对象数统计。
- AOI 回读输出路径和 rasterio 打开结果。
- 前端关键页面截图：配置页、批次列表、批次详情、执行结果、质量历史。

## 15. 上线阻断条件

出现以下任一情况，不建议进入生产上线：

- `/health?checks=all` 任一基础设施检查失败。
- 生产 run 入口仍依赖 demo 数据或 driver 本地 `/tmp/.../cog/*.tif` 被 Ray worker 读取。
- 任一核心数据类型端到端 rows 为 0。
- `quality_status=FAIL` 且没有人工批准和隔离措施。
- PostgreSQL metadata 与 job_report 行数无法对账。
- MinIO 对象缺失或 `value_ref_uri`/`entity_tile_uri` 无法读取。
- 批次取消、重试或人工确认状态不正确。
- 运行时配置被写入 `cube_web_configs`。
- 仓库含凭据、本地数据路径或演示专用硬编码内容。
