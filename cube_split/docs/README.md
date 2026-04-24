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
- 按数据族提供独立剖分服务入口，例如光学遥感与碳卫星。
- 写入 `rs_raw_scene_asset`、`rs_cube_cell_fact`、`rs_ingest_job`。
- 按 AOI、时间桶、波段读取并合成结果。

`cube_split` 不负责：

- 格网 locate/cover/topology 算法。
- Web 页面渲染。
- PB 级湖表治理的元数据提交协调。

## 3. 剖分服务

剖分能力按数据族拆分，统一通过 `cube_split.partition.get_partition_service(data_type)` 路由：

- `optical`：光学遥感影像剖分，沿用现有 COG window 索引链路，核心任务仍在 `cube_split.jobs.ray_logical_partition_job`。
- `carbon_satellite` / `carbon`：碳卫星观测剖分，输入是 OCO-2 Lite `.nc4` 或已经标准化的 JSONL/CSV 观测行，输出为 `carbon_observation_rows.jsonl`。

当前 `optical` 服务支持的产品族：

- `landsat`：Landsat Collection 命名的单波段 TIF，例如 `LC09_L2SP_123033_20240424_20240425_02_T1_SR_B4.TIF`。
- `sentinel_optical`：Sentinel-2 MSI 已展开/已转换的单波段 TIF，例如 `T50TMK_20240424T030539_B08_10m.tif`。

Sentinel-2 SAFE 压缩包/目录解析不是当前入口的一部分。上游需要先把波段资产落成光学服务可识别的 TIF；后续可以在 `optical` 服务内继续新增 SAFE reader，而不改变 COG window 剖分主链路。

碳卫星默认不重采样成 GeoTIFF。它把每个 sounding/footprint 组织为“观测事实行”：

- `satellite`, `observation_id`, `acq_time`, `time_bucket`
- `grid_type`, `grid_level`, `space_code`, `st_code`
- `xco2`, `quality_flag`, `center_lon`, `center_lat`
- `footprint_geojson`, `source_uri`, `source_index`, `metadata_json`

这种组织方式保留碳卫星点/足迹观测的原始粒度，避免把非栅格产品强行重采样，同时仍复用 `cube_encoder` 的统一空间格网和时空编码。

碳卫星服务示例：

```python
from pathlib import Path

from cube_split.partition import CarbonPartitionConfig, CarbonSatellitePartitionService

result = CarbonSatellitePartitionService().run(
    input_dir=Path("data/carbon"),
    output_dir=Path("data/carbon_out"),
    config=CarbonPartitionConfig(grid_type="geohash", grid_level=7, max_observations=1000),
    workers=4,
)
print(result.rows_path, result.total_rows)
```

OCO-2 Lite `.nc4` 读取优先使用 Python `netCDF4`；环境未安装时可回退到 `h5dump` CLI。`sounding_id` 用于派生采集时间，避免 HDF 工具低精度打印 `time` 变量导致秒级偏差。

后续雷达数据、信息产品数据可以按同样模式新增独立服务，保持输入解析和事实行组织互不污染，公共部分只复用 `CubeEncoderSDK` 的格网与时空编码能力。

## 4. 入库流程

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

## 5. 表模型

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

## 6. value_ref_uri

当前实现不把每个格网单元预切成独立文件，而是在 `value_ref_uri` 中保存 COG 对象路径和窗口位置：

```text
s3://cube/cube/raw/.../SR_B4_cog.tif#window=867,0,3711,866
```

解析后得到：

- 对象位置：`s3://...SR_B4_cog.tif`
- 读取窗口：`col_off=867,row_off=0,width=3711,height=866`

这代表当前链路采用“索引驱动 + COG window 按需读取”的轻量模式。

## 7. AOI 读取

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

## 8. 运行环境

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

## 9. 后续工程化方向

- 固化 AOI 多格网读取 API 或脚本。
- 为大 AOI 增加分页、批量限制和超限保护。
- 在读取结果中输出命中/缺失 `space_code`、每个 band 的命中条数和数据质量摘要。
- 根据查询热度决定是否引入热区缓存、month 聚合层或长期物化 tile。

历史长文档保存在 `docs/archive/`。
