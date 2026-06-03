# cube_split 当前工作流

更新时间：2026-06-01

## 1. 定位

`cube_split` 负责遥感数据剖分、入库、自动化质检和 AOI 回读。格网 locate、cover、topology 和 ST code 能力都来自 `grid_core.sdk.CubeEncoderSDK`。

主链路：

```text
输入资产或观测清单
  -> 解析元数据
  -> 栅格资产标准化为 COG
  -> 调用 CubeEncoderSDK 生成 space_code / st_code
  -> 输出 index_rows.jsonl 或 carbon_observation_rows.jsonl
  -> 写入 PostgreSQL + MinIO，或 SQLite + local
  -> 运行质量检查
  -> AOI / 时间 / 波段回读
```

## 2. 数据族

- `optical`：光学遥感 TIF/COG 资产，输出 cell/window 索引行。支持 `landsat`、`sentinel2` 和 `generic_tif` 兜底解析。
- `product`：栅格产品 TIF，默认输出到 `data/ray_output/product`，时间桶按年份组织。
- `carbon` / `carbon_satellite`：碳卫星点/足迹观测，输出 `carbon_observation_rows.jsonl`，不强制重采样为 GeoTIFF。
- `radar`：雷达栅格资产，使用与光学一致的逻辑剖分链路；`ISEA4H` 格网走实体瓦片剖分。Web schema 驱动运行不依赖 Sentinel 命名，只依赖 ARD 字段。

## 3. ARD Schema 与 Manifest 交付要求

载入系统推荐交付 ARD schema，并通过 Web API 导入：

```bash
curl -X POST http://127.0.0.1:50040/v1/partition/schemas/import \
  -H 'Content-Type: application/json' \
  -d @schema.json
```

也可以给底层命令传入 `manifest.jsonl` 或 `manifest.json`。schema 和 manifest 都要求每条记录能回答四个问题：

- 数据在哪里：`source_uri`
- 数据是什么：`data_type`、`sensor`、`product_family`、`band` / `bands` / `variable` / `polarization`
- 数据发生在什么时候：`acq_time`
- 数据覆盖哪里：栅格资产使用 `corners`；碳观测使用 `corners`、`footprint` / `footprint_geojson` 或 `lon/lat`

字段使用小写蛇形命名。时间使用 UTC ISO8601，例如 `2026-05-18T00:00:00Z`。空间范围使用 `EPSG:4326` 经纬度。

### 栅格资产 schema

`optical`、`product`、`radar` 的 `assets` / `selected_assets` 为非空数组。每个资产必填：

- `source_uri`
- `scene_id`
- `acq_time`
- `sensor`
- `product_family`，旧 manifest 兼容 `product_type`
- `resolution`，数值或类似 `10m` 的字符串，必须大于 0
- `corners`，严格为 4 个 `[lon, lat]` 点
- `bands`、`band`、`variable` 或 `polarization` 至少一个

示例：

```json
{
  "batch_id": "optical_batch_001",
  "data_type": "optical",
  "assets": [
    {
      "source_uri": "Shandong_mosaic_2020Q3_sr_band4_cut.tif",
      "scene_id": "Shandong_mosaic_2020Q3",
      "acq_time": "2020-07-01T00:00:00Z",
      "sensor": "optical_mosaic",
      "product_family": "other",
      "resolution": 10,
      "bands": ["sr_band4"],
      "corners": [[117.0, 36.0], [117.2, 36.0], [117.2, 35.8], [117.0, 35.8]]
    }
  ]
}
```

`product` 资产通常还带 `product_name`、`product_year`；`radar` 资产通常还带 `polarization`。这些字段会原样进入 Web 运行 payload。

### 碳观测 schema

`carbon` 的 `observations` / `selected_observations` 为非空数组。每个观测必填：

- `source_uri`
- `observation_id`
- `acq_time`
- `sensor`
- `product_family`
- `resolution`
- 位置字段：`corners`、`footprint` / `footprint_geojson`、或 `lon` / `lat`

### 目录扫描兜底

未提供 schema、`selected_assets` 或 manifest 时，底层命令仍会扫描输入目录，并按历史文件名解析规则推断元数据。该模式只用于本地调试和老数据兼容；生产试运行应使用 schema/manifest，避免依赖文件命名。

## 4. 剖分命令

光学逻辑剖分：

```bash
PYTHONPATH=../cube_encoder:. python -m cube_split.jobs.ray_logical_partition_job \
  --input-dir data/optocal \
  --manifest-path data/optocal/manifest.jsonl \
  --product-family auto \
  --output-dir data/ray_output/logical_partition \
  --grid-type geohash \
  --grid-level 5 \
  --cover-mode intersect
```

常用参数：

- `--manifest-path`：可选，`.jsonl` 或 `.json` 清单；设置后按清单读取资产。
- `--product-family`：`auto`、`landsat`、`sentinel2`。
- `--target-crs`：可选 COG 目标 CRS；为空则保留源 CRS。
- `--partition-backend`：`ray`、`auto`、`thread`；需要分布式执行时显式提供 Ray 地址。
- `--ray-address`：优先来自 `CUBE_WEB_RAY_ADDRESS` 或 `RAY_ADDRESS`，也可通过命令行参数传入；本机调试可用空值或 `auto`。
- `--timing-mode`、`--skip-verify`：用于性能计时，减少汇总校验开销。
- PostgreSQL、MinIO、Ray 统一从运行时配置读取：PostgreSQL 使用
  `CUBE_WEB_POSTGRES_DSN`、`POSTGRES_DSN` 或 `DATABASE_URL`；Ray 使用
  `CUBE_WEB_RAY_ADDRESS` 或 `RAY_ADDRESS`；MinIO 使用
  `CUBE_WEB_MINIO_*`、`MINIO_*` 或节点 `/etc/default/minio`。业务模块不再内置集群 IP 或默认密钥。

产品剖分：

```bash
PYTHONPATH=../cube_encoder:. python -m cube_split.jobs.product_partition_job \
  --input-dir data/product \
  --output-dir data/ray_output/product \
  --target-crs EPSG:4326 \
  --grid-type geohash \
  --grid-level 5
```

碳卫星剖分按观测事实组织，默认使用 `ISEA4H level=5`，不复用光学影像的窗口剖分格网：

```bash
PYTHONPATH=../cube_encoder:. python -m cube_split.jobs.carbon_partition_job \
  --input-dir data/carbon \
  --output-dir data/ray_output/carbon \
  --grid-type isea4h \
  --grid-level 5 \
  --partition-backend ray \
  --ray-address "$RAY_ADDRESS" \
  --ray-parallelism 4
```

底层通过 `CarbonSatellitePartitionService` 使用 `CarbonPartitionConfig`。`--partition-backend ray` 按观测 chunk 分发到 Ray actor；`auto` 在设置 `--ray-address` 时使用 Ray，否则回退到本地 `process`。测试 monkeypatch 或不适合 fork 的环境可改为 `thread`。

端到端试运行脚本覆盖 `optical`、`radar`、`product` 三类资产，以及 `geohash`、`tile_matrix`、`isea4h` 三种格网。脚本会生成小 TIF，上传到 MinIO，以 `selected_assets` 写入完整 ARD 字段，并通过 Ray 执行剖分：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 cube_split/scripts/run_all_partition_flows_smoke.py \
  --mode test \
  --ray-parallelism 2 \
  --chunk-size 1 \
  --max-cells-per-asset 50 \
  --summary-path /tmp/cube_partition_flow_smoke/summary.json
```

运行前需要环境中可解析以下配置：

- `CUBE_WEB_POSTGRES_DSN`
- `CUBE_WEB_RAY_ADDRESS`
- `CUBE_WEB_MINIO_ENDPOINT`
- `CUBE_WEB_MINIO_BUCKET`
- MinIO 访问密钥，来自 `CUBE_WEB_MINIO_ACCESS_KEY` / `CUBE_WEB_MINIO_SECRET_KEY`、`MINIO_*`，或节点 `/etc/default/minio`

默认不跑质量检查；需要同时验证质量报告写入时加 `--keep-quality`。
生产文档推荐使用 `--mode test` 做剖分链路验证；演示环境需要完整入库冒烟时，在
`demo/*` 分支的演示文档中维护 `--mode demo` 说明。

Ray Client 需要 driver Python 与集群 Python 主版本一致；当前 Ray 集群为 Python 3.11.6，因此 smoke 建议使用 `python3.11`。如需指定其他兼容解释器，pytest 包装器支持 `CUBE_PARTITION_E2E_PYTHON=/path/to/python`。

也可以通过 pytest marker 运行同一个 smoke。默认会 skip，不影响本地单元测试；显式设置环境变量后才会连接 Ray、MinIO 和 PostgreSQL。pytest 包装器会启用 `--keep-quality`，并校验 optical/product 结果带有 `quality_status` 和 `quality_report_id`：

```bash
CUBE_RUN_PARTITION_E2E_SMOKE=1 \
CUBE_PARTITION_E2E_PYTHON=python3.11 \
PYTHONPATH=cube_encoder:cube_split:cube_web \
pytest -m e2e cube_split/tests/test_partition_e2e_smoke.py
```

## 5. 输出文件

光学和产品剖分在 `run_*` 目录中生成：

- `index_rows.jsonl`：格网 cell/window 索引行。
- `job_report.json`：运行参数、计数、耗时和输出路径。
- `quality_report.json`：质检后生成，若尚未运行质检则不存在。

核心索引字段：

- `scene_id`, `band`, `asset_path`, `acq_time`
- `grid_type`, `grid_level`, `space_code`, `space_code_prefix`
- `st_code`, `time_bucket`, `cover_mode`
- `window_col_off`, `window_row_off`, `window_width`, `window_height`
- `cell_min_lon`, `cell_min_lat`, `cell_max_lon`, `cell_max_lat`

碳卫星剖分在 `data/ray_output/carbon/run_*` 目录中生成：

- `carbon_observation_rows.jsonl`
- `job_report.json`

核心字段：

- `satellite`, `product_type`, `observation_id`, `acq_time`
- `grid_type`, `grid_level`, `space_code`, `st_code`, `time_bucket`
- `xco2`, `quality_flag`, `center_lon`, `center_lat`
- `footprint_geojson`, `source_uri`, `source_index`, `metadata_json`

## 6. 入库

光学入库：

```bash
PYTHONPATH=../cube_encoder:. python -m cube_split.ingest.ray_ingest_job \
  --run-dir data/ray_output/logical_partition/run_YYYYMMDD_HHMMSS \
  --job-id optical-job-001
```

产品入库：

```bash
PYTHONPATH=../cube_encoder:. python -m cube_split.ingest.product_ingest_job \
  --run-dir data/ray_output/product/run_YYYYMMDD_HHMMSS \
  --job-id product-job-001
```

碳卫星入库：

```bash
PYTHONPATH=../cube_encoder:. python -m cube_split.ingest.carbon_ingest_job \
  --run-dir data/carbon_out \
  --job-id carbon-oco2-001 \
  --cube-version v1
```

本地调试可使用：

```bash
--metadata-backend sqlite --asset-storage-backend local
```

## 7. 表模型

`rs_raw_scene_asset` 保存原始/标准化资产元数据，唯一键：

```text
scene_id + band + version
```

`rs_cube_cell_fact` 保存栅格类 cell/window 事实，业务唯一键：

```text
grid_type + grid_level + space_code + time_bucket + band + cube_version
```

`rs_carbon_observation_fact` 保存碳卫星观测事实，业务唯一键：

```text
satellite + observation_id + product_type + cube_version
```

`rs_ingest_job` 记录任务状态、参数、统计、失败原因和输出快照。

## 8. 质检

光学质检：

```bash
PYTHONPATH=../cube_encoder:. python -m cube_split.quality.optical_quality \
  --run-dir data/ray_output/logical_partition/run_YYYYMMDD_HHMMSS \
  --target-crs EPSG:4326
```

产品质检：

```bash
PYTHONPATH=../cube_encoder:. python -m cube_split.quality.product_quality \
  --run-dir data/ray_output/product/run_YYYYMMDD_HHMMSS \
  --target-crs EPSG:4326
```

质检输出默认写入 `run_dir/quality_report.json`。通过 Web API 触发质检时，报告会同步写入
PostgreSQL `quality_reports`；Web 的 latest/history/report/pdf/txt 均从该表读取。直接运行
`cube_split.quality` 命令只生成本地报告文件，不会自动写入 Web 报告库。

Web 批处理会把剖分结果中的 `quality_status`、`quality_report_id`、`quality_failure_reason` 持久化到 `partition_batches`：

- `PASS` / `WARN`：保留批次剖分状态，记录报告 ID。
- `FAIL`：剖分资产可保持 `succeeded`，但批次状态转为 `manual_required`，`last_error` 写入失败检查摘要。
- 新的 run/retry 入队时清空上一轮质量结果，避免运行中继续展示旧报告；重试完成后再写入新的质量结果。

任务提交与重试策略：

- 同一批次已有本进程内的 queued/running/retrying/cancel_requested 任务时，重复提交 run/retry 会返回已有 `task_id`，不会新增 attempt 或重复污染批次状态。
- 自动重试只处理剖分失败或资产级可重试失败。质量 `FAIL` 不自动重跑，需要人工确认后通过批次重试或资产重试触发。
- retry attempt 会记录 `source_task_id`、`retry_strategy` 和 `failure_reason`。批次重试使用 `full_batch`，资产重试使用 `selected_assets`，资产级自动重试使用 `retryable_assets`，任务级自动重试使用 `full_batch`。
- `/v1/partition/batches/{batch_id}/attempts` 返回 attempt 历史，可用于追踪 retry 来源任务、策略和失败原因。

取消策略：

- queued 任务取消后，attempt、batch 和目标资产进入 `cancelled`。
- running 任务先进入 `cancel_requested`；runner 检查取消信号后停止，最终标记为 `cancelled`。
- 已生成的运行目录和外部存储对象不由取消接口自动删除。

## 9. AOI 读取

栅格 AOI 读取：

```bash
PYTHONPATH=../cube_encoder:. python -m cube_split.read.aoi_reader \
  --bbox 120.8 44.0 122.2 44.6 \
  --time-bucket 20260204 \
  --bands sr_b2 sr_b3 sr_b4 \
  --output .tmp/aoi_rgb.tif
```

碳卫星查询：

```bash
PYTHONPATH=../cube_encoder:. python -m cube_split.read.carbon_query \
  --bbox -168.0 40.5 -166.5 42.0 \
  --time-start 20201231 \
  --time-end 20201231 \
  --quality-flags 0 \
  --grid-type isea4h \
  --grid-level 5
```

## 10. 历史说明

已删除的 dated 专题文档内容已合并到本文：

- `2026-04-28-carbon-optical-partition-performance.md`：性能结论保留为历史背景，不作为当前性能基线。
- `2026-05-07-minio-cluster-readback-guide.md`：特定批次 MinIO 读回说明已并入入库和 AOI 章节。
- `2026-05-18-partition-metadata-requirements-for-ingest.md`：manifest 交付要求已并入第 3 节。
