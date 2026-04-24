# cube_split 文档

更新时间：2026-04-24  
适用范围：`cube_split`

## 1. 定位

`cube_split` 负责遥感数据的剖分、入库和 AOI 回读链路。它不实现格网算法，所有格网覆盖、编码和拓扑能力都来自 `cube_encoder` 的 `grid_core.sdk.CubeEncoderSDK`。

一句话流程：

```text
景级 TIF
  -> COG 标准化
  -> 调用 cube_encoder 计算格网覆盖
  -> 生成 cell/window 索引行
  -> 写入 PostgreSQL 与 MinIO
  -> AOI 解析为 space_code[]
  -> 按 value_ref_uri#window=... 回源读取 COG
  -> 合成 GeoTIFF
```

## 2. 工程边界

`cube_split` 负责：

- 景级输入解析和波段资产管理。
- TIF 到 COG 的标准化。
- 基于格网覆盖结果生成 `index_rows.jsonl`。
- 写入 `rs_raw_scene_asset`、`rs_cube_cell_fact`、`rs_ingest_job`。
- 按 AOI、时间桶、波段读取并合成结果。

`cube_split` 不负责：

- 格网 locate/cover/topology 算法。
- Web 页面渲染。
- PB 级湖表治理的元数据提交协调。

## 3. 入库流程

输入通常是同一景的多个波段 TIF，例如 Landsat `SR_B2/SR_B3/SR_B4/QA_PIXEL`。剖分前先转为 COG，默认输出到 `data/cog/partition_input`。

当前主参数：

- `grid_type = geohash`
- `grid_level = 7`
- `cover_mode = intersect`
- `time_granularity = day`

索引行核心字段：

- `scene_id`, `band`, `asset_path`, `acq_time`
- `grid_type`, `grid_level`, `space_code`, `st_code`, `time_bucket`
- `window_col_off`, `window_row_off`, `window_width`, `window_height`

其中 `space_code` 是空间索引键，`time_bucket` 是时间索引键，`window_*` 是 COG 回源读取窗口。

## 4. 表模型

`rs_raw_scene_asset` 保存景级资产元数据，唯一键：

```text
scene_id + band + version
```

`rs_cube_cell_fact` 是当前读取主表，业务唯一键：

```text
grid_type + grid_level + space_code + time_bucket + band + cube_version
```

关键字段：

- `space_code`
- `time_bucket`
- `band`
- `st_code`
- `cell_min_lon/cell_min_lat/cell_max_lon/cell_max_lat`
- `value_ref_uri`
- `source_scene_count`
- `provenance_json`

`rs_ingest_job` 保存任务状态、参数、统计、失败原因和输出快照。

## 5. value_ref_uri

当前实现不把每个格网单元预切成独立文件，而是在 `value_ref_uri` 中保存 COG 对象路径和窗口位置：

```text
s3://cube/cube/raw/.../SR_B4_cog.tif#window=867,0,3711,866
```

解析后得到：

- 对象位置：`s3://...SR_B4_cog.tif`
- 读取窗口：`col_off=867,row_off=0,width=3711,height=866`

这代表当前链路采用“索引驱动 + COG window 按需读取”的轻量模式。

## 6. AOI 读取

AOI 读取流程：

1. 输入 bbox 或 geometry。
2. 调用 `cube_encoder` 得到 `space_code[]`。
3. 用 `space_code[] + time_bucket + band` 查询 `rs_cube_cell_fact`。
4. 解析每条记录的 `value_ref_uri#window=...`。
5. 从 MinIO 中按窗口读取 COG。
6. 将多个 cell、多个 band 合成为输出 GeoTIFF。

命令示例：

```bash
python -m cube_split.read.aoi_reader \
  --bbox 120.8 44.0 122.2 44.6 \
  --time-bucket 20260204 \
  --bands sr_b2 sr_b3 sr_b4 \
  --output .tmp/aoi_rgb.tif \
  --postgres-dsn postgresql://postgres:postgres@127.0.0.1:55432/cube \
  --minio-endpoint 127.0.0.1:59000
```

已验证样例：

- AOI bbox: `[120.8, 44.0, 122.2, 44.6]`
- 命中有效格网数：`5`
- 输出尺寸：`6714 x 3443`
- 坐标系：`EPSG:32651`
- 波段：`sr_b2 / sr_b3 / sr_b4`

## 7. 运行环境

常用联调环境：

- PostgreSQL: `127.0.0.1:55432`, database `cube`
- MinIO: `127.0.0.1:59000`, bucket `cube`

默认脚本环境变量：

```bash
POSTGRES_DSN='postgresql://postgres:postgres@127.0.0.1:5432/cube'
MINIO_ENDPOINT='127.0.0.1:9000'
MINIO_ACCESS_KEY='minioadmin'
MINIO_SECRET_KEY='minioadmin'
MINIO_BUCKET='cube'
scripts/run_ray_ingest_e2e.sh
```

## 8. 后续工程化方向

- 固化 AOI 多格网读取 API 或脚本。
- 为大 AOI 增加分页、批量限制和超限保护。
- 在读取结果中输出命中/缺失 `space_code`、每个 band 的命中条数和数据质量摘要。
- 根据查询热度决定是否引入热区缓存、month 聚合层或长期物化 tile。

历史长文档保存在 `docs/archive/`。
