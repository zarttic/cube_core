# cube_split 文档

更新时间：2026-09-10

## 1. 定位

`cube_split` 是基于 cube_encoder SDK 的剖分与入库工作流实现：按数据族执行剖分、生成 COG/瓦片产物、
把结果入库到 OpenGauss 与 MinIO、对产物做质检，并提供 AOI 回读。执行后端支持 Ray 与本地线程/进程。

目录职责：

| 目录 | 职责 |
| --- | --- |
| `partition/` | 按数据族（optical / radar / carbon / product）的剖分服务与产品解析适配器 |
| `jobs/` | 作业入口与共享核心（网格任务构建、源缓存、分块、取消、Ray runtime env） |
| `ingest/` | 把剖分产物入库到 OpenGauss/SQLite 与 MinIO |
| `quality/` | 对 run_dir 产物执行质检并出报告 |
| `read/` | AOI RGB 读取、碳观测查询 |
| `scripts/` | 维护脚本（如 `migrate_cube_cell_geom.py`） |
| 顶层 | `runtime_config.py`（配置解析）、`partition_timing.py`（计时埋点）、`tile_probe.py`（指标上报） |

## 2. 剖分能力

| 数据族 | 实现 | 格网 | 剖分方式 |
| --- | --- | --- | --- |
| optical | `jobs/ray_logical_partition_job.py` | `geohash`、`mgrs` | 逻辑剖分 |
| radar | `jobs/ray_logical_partition_job.py` | `geohash`、`mgrs` | 逻辑剖分 |
| carbon | `partition/carbon.py` | `geohash`、`mgrs`、`isea4h`（默认 `isea4h`） | 逻辑/实体按作业参数 |
| product | `jobs/product_partition_job.py` | `geohash`、`mgrs` | 逻辑剖分 |
| 实体剖分 | `jobs/entity_partition_job.py` | 强制 `isea4h` | 实体剖分（`clip_mode` 支持 `bbox`、`exact`） |

- 生产格网白名单：`geohash`、`mgrs`、`isea4h`，其余值在作业入口即被拒绝。
- 逻辑剖分只写元数据与窗口引用（`tile_kind="logical_reference"`），落到 staging 表与
  `logical-chunks/*.jsonl.gz`；实体剖分切真实瓦片并写入 `rs_entity_tile_asset`。
- 产品族解析：optical 支持 `landsat`、`sentinel2`、`other`、`generic_tif`；radar 为 `sentinel1`；
  carbon 适配器为 `xco2`、`tansat`、`sif`；product 文件名需以 `_YYYY年` 结尾。

## 3. 作业入口

| 作业 | 入口 | 常用参数 |
| --- | --- | --- |
| 逻辑剖分 | `run_logical_partition(args)` | `--grid-type{geohash,mgrs}`、`--grid-level`、`--cover-mode`、`--time-granularity`、`--max-cells-per-asset`（0 不限）、`--ray-parallelism`、`--chunk-size`、`--partition-backend{auto,ray,thread}`、`--metadata-backend{none,sqlite,postgres}` |
| 逻辑分块 | `run_logical_chunk_jobs(payloads, runtime_env, cancellation_check)` | `CUBE_LOGICAL_MAX_IN_FLIGHT`（4）、`CUBE_LOGICAL_SHARD_DEGREES`（1，需 (0,10]）、`CUBE_LOGICAL_SHARDS_PER_TASK`（16） |
| 碳卫星剖分 | `run_carbon_partition` | `--grid-type`（默认 `isea4h`）、`--grid-level`（默认 6）、`--product-type`、`--partition-backend{auto,ray,process,thread}` |
| 实体剖分 | `run_entity_partition(args)` | 强制 `isea4h`；`clip_mode`、`DEFAULT_ENTITY_TASKS_PER_GROUP=64`、MinIO 上传并发 16 |
| 产品剖分 | `run_product_partition` | `--grid-type{geohash,mgrs}`、`--partition-backend{auto,ray,thread}`、`--asset-storage-backend{local,minio}` |

共享核心 `jobs/ray_partition_core.py`：manifest 解析、网格任务构建、`process_partition`、
源对象缓存与校验（按 URI sha256 隔离，ETag/大小与本地 sha256 双重校验；`ENOSPC` 只清理本 worker 缓存）。

托管批次的执行入口在 `cube_web`：`python3.11 -m cube_web.jobs.partition_batch_job --task-id <id>`。

## 4. Ray 使用方式

- 连接：`ray.init(address=..., ignore_reinit_error=True, include_dashboard=False, logging_level="ERROR", runtime_env=...)`。
- Runtime env：优先使用 `RAY_RUNTIME_ENV_JSON`，否则 `working_dir` 为仓库根、`env_vars` 注入
  `CUBE_PROJECT_ROOT` 与 `PYTHONPATH`，并排除 `.git/**`、`**/__pycache__/**`、`cube_split/data/**`、
  `cube_web/frontend/node_modules/**` 等。
- `CUBE_WEB_RAY_JOB_DRIVER=1` 时不再套 runtime env（由 Ray Jobs 提供运行环境）。
- **凭据不进 task 参数**：MinIO 凭据通过 `runtime_env.env_vars` 注入 worker 运行时环境。
- Ray 模式下实体剖分强制由 worker 上传 MinIO，本地路径直接报错。

## 5. 入库

| 入口 | 说明 |
| --- | --- |
| `ingest/ray_ingest_job.py` | 通用入库，读 `run_dir/index_rows.jsonl`，`--metadata-backend{sqlite,postgres}` + `--asset-storage-backend{local,minio}` |
| `ingest/carbon_ingest_job.py` | 碳卫星入库，仅支持 postgres，读 `carbon_observation_rows.jsonl` |
| `ingest/product_ingest_job.py` | 信息产品入库，sqlite 与 postgres 双实现 |
| `ingest/managed_output_ingest.py` | 托管输出入库：先校验 MinIO 对象，再按数据族分派（carbon / product+logical / 其余栅格），成功与失败都写 job status |

## 6. 产物质检

| 模块 | 检查项 |
| --- | --- |
| `quality/optical_quality.py` | `index_rows`、`index_schema`、`time_bucket`、`cell_bbox`、`logical_duplicates`、`asset_readability`、`cog_crs`、`window_bounds`、`pixel_sample` |
| `quality/radar_quality.py` | 复用 optical 全部检查，`data_type` 置为 `radar` |
| `quality/product_quality.py` | `index_rows` + optical 的 `index_schema`/`cell_bbox`/`duplicates`/`assets`，另加 `product_years`（可用 `--expected-years`） |
| `quality/carbon_quality.py` | `carbon_rows`、`carbon_schema`、`time_bucket`、`carbon_coordinates`、`xco2` 或 `sif`、`carbon_quality_flags`、`carbon_duplicates`、`carbon_footprint` |

## 7. 运行时配置

解析顺序：环境变量 → `CUBE_WEB_ENV_FILE` → 当前目录 `.cube_web.env` → `~/.cube_web.env` → 仓库根 `.cube_web.env`。

主要变量：`CUBE_WEB_POSTGRES_DSN`（或 `POSTGRES_DSN` / `DATABASE_URL`）、`CUBE_WEB_RAY_ADDRESS`（或 `RAY_ADDRESS`）、
`CUBE_WEB_MINIO_ENDPOINT` / `_ACCESS_KEY` / `_SECRET_KEY` / `_BUCKET`（默认 `cube`，`secure=False`）、
`CUBE_SOURCE_CACHE_DIR`（默认 `/tmp/cube_split_source_cache`）、`CUBE_PROJECT_ROOT`、
`RAY_RUNTIME_ENV_JSON`、`CUBE_WEB_RAY_JOB_DRIVER`、`RAY_ACTOR_NODE_RESOURCE`、
`CUBE_WEB_RAY_WORKER_RESOURCE`（默认 `cube_partition_worker`）、`CUBE_CARBON_RAY_PARALLELISM`、
`CUBE_LOGICAL_*`、`AWS_*`（GDAL/rasterio S3）。

完整变量表见 `../docs/OPERATIONS.md`。

## 8. 计时埋点

`partition_timing.py` 提供 `TimingRecorder(scope)`：`phase(name)` 作为上下文管理器累计耗时，
另有 `add_phase`、`add_counter`、`set_attribute`、`finish()`。
`partition_timing_from_workers(timings)` 从各 worker 记录中取最晚开始时间，聚合成
`partition_to_ingest` 区间（无 worker 记录时返回 `None`，不臆造时间戳）。

## 9. 测试

```bash
PYTHONPATH=cube_encoder:cube_split python3.11 -m pytest cube_split/tests
```

覆盖服务注册与格网级别校验、manifest 与源缓存、Ray 作业分块与并行度、实体瓦片与 MinIO 上传、
碳/产品端到端、入库 upsert、质检各项、AOI 读取、配置解析顺序与计时聚合。
`cube_split/tests/` 全部用例使用打桩，不需要外部服务。
