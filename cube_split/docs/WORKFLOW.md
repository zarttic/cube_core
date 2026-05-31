# cube_split 当前工作流

更新时间：2026-05-26

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
- `radar`：Web 侧有占位入口，剖分实现尚未落地。

## 3. Manifest 交付要求

载入系统推荐交付 `manifest.jsonl` 或 `manifest.json`。每条记录需要能回答四个问题：

- 数据在哪里：`source_uri`
- 数据是什么：`data_type`、`product_type`、`band` 或 `variable`
- 数据发生在什么时候：`acq_time`
- 数据覆盖哪里：`lon/lat`、`footprint`、`bbox` 或 `corners`

字段使用小写蛇形命名。时间使用 UTC ISO8601，例如 `2026-05-18T00:00:00Z`。空间范围使用 `EPSG:4326` 经纬度。

栅格资产示例：

```json
{
  "batch_id": "optical_batch_001",
  "data_type": "optical",
  "assets": [
    {
      "source_uri": "Shandong_mosaic_2020Q3_sr_band4_cut.tif",
      "scene_id": "Shandong_mosaic_2020Q3",
      "acq_time": "2020-07-01T00:00:00Z",
      "band": "sr_band4",
      "corners": [[117.0, 36.0], [117.2, 36.0], [117.2, 35.8], [117.0, 35.8]]
    }
  ]
}
```

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

质检输出默认写入 `run_dir/quality_report.json`。Web 端会扫描 `cube_split/data/ray_output/*/run_*` 下带有 `index_rows.jsonl` 的目录，用于 latest/history/report 展示。

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
