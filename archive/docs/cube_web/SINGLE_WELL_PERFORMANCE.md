# 单井完整景剖分性能测试

`run_single_well_performance.py` 测量正式 Web + Ray Jobs 链路，不使用本地 thread/process 结果替代分布式结果。

## 测试矩阵

脚本固定执行 10 个独立场景：

| 数据类型 | geohash | mgrs | isea4h |
|---|---|---|---|
| 光学 | 测试 | 测试 | 测试 |
| 雷达 | 测试 | 测试 | 测试 |
| 产品信息 | 测试 | 测试 | 测试 |
| 碳卫星 | - | - | 测试 |

生产契约下 `geohash`、`mgrs` 使用逻辑剖分，`isea4h` 使用实体剖分。碳卫星只测一个 `isea4h` 六边形格网场景。

## 单井输入口径

性能 manifest 必须包含：

- `well_id`：报告中的单井标识。
- `datasets`：恰好四个数据集，`data_type` 分别为 `optical`、`radar`、`product`、`carbon`。
- manifest 必须声明 `scene_mode=full`；正式验收不使用 64×64 等裁剪窗口。
- 每个数据集恰好一个 `scene`，一个 scene 可以包含多个 asset 和多个 band；本次标准输入为光学四波段、雷达 VV/VH、产品单值波段和碳卫星单变量。
- 光学必须恰好 4 个波段；雷达必须同时包含且仅包含 `VV`、`VH` 两个 asset。
- 非碳数据 asset 使用 `s3://` COG、64 位 SHA-256、`bbox`、`crs`，band 提供 `source_band_index`。
- 碳数据 asset 使用 `s3://` NetCDF/HDF5/SIF 原始文件、64 位 SHA-256 和 `source_format`。
- `grid_levels.geohash`、`grid_levels.mgrs`、`grid_levels.isea4h` 必须显式填写；脚本不会替换为隐式默认层级。
- `carbon_max_observations` 必须显式填写，避免把整景碳卫星原始文件误当成单井输入。

当前生产接口没有独立的井实体或跨数据集共享 AOI 字段，因此本测试的“单井”是每类数据独立提交一个完整 scene；各 asset 保留自己的 `bbox`。这能测量完整景数据单元的性能，但不代表四类源数据已经对齐到同一个地理坐标；报告以 manifest 中的 bbox 和景尺寸作为复核依据。

输入结构示意：

```json
{
  "schema_version": "single-well-performance-v1",
  "well_id": "well-001",
  "scene_mode": "full",
  "expected_band_counts": {"optical": 4},
  "grid_levels": {"geohash": 1, "mgrs": 1, "isea4h": 6},
  "max_cells_per_asset": 0,
  "carbon_max_observations": 1,
  "datasets": [
    {
      "dataset_id": "optical-well-001",
      "dataset_title": "single well optical",
      "data_type": "optical",
      "scenes": [
        {
          "scene_id": "optical-scene-001",
          "scene_key": "optical-scene-001",
          "assets": [
            {
              "asset_id": "optical-asset-001",
              "cog_uri": "s3://cube/cube/source/optocal/example.tif",
              "source_kind": "cog",
              "source_format": "cog",
              "checksum": "<64 hex sha256>",
              "acquisition_time": "2020-01-01T00:00:00Z",
              "bbox": [116.0, 39.0, 116.01, 39.01],
              "crs": "EPSG:4326",
              "attributes": {
                "preparation": "full",
                "width": 7595,
                "height": 6337,
                "count": 4,
                "source_window": {"col_off": 0, "row_off": 0, "width": 7595, "height": 6337}
              },
              "bands": [
                {"band_code": "B01", "band_name": "Band 1", "band_type": "spectral", "source_band_index": 1},
                {"band_code": "B02", "band_name": "Band 2", "band_type": "spectral", "source_band_index": 2},
                {"band_code": "B03", "band_name": "Band 3", "band_type": "spectral", "source_band_index": 3},
                {"band_code": "B04", "band_name": "Band 4", "band_type": "spectral", "source_band_index": 4}
              ]
            }
          ]
        }
      ]
    }
  ]
}
```

其余 `radar`、`product`、`carbon` 数据集沿用相同结构；碳 asset 改用 `source_uri`、`source_kind=raw`、`source_format=netcdf|hdf5|sif`，不使用 `cog_uri`。示意中的 URI 和 checksum 不能直接用于真实测试。

## 正式运行前置条件

必须确认 Web 运行时配置满足：

```text
CUBE_WEB_PARTITION_EXECUTOR=ray_job
CUBE_WEB_RAY_ADDRESS=<Ray GCS address>
CUBE_WEB_RAY_JOB_ADDRESS=<Ray Jobs dashboard address>
CUBE_WEB_POSTGRES_DSN=<OpenGauss DSN>
CUBE_WEB_MINIO_ENDPOINT=<MinIO endpoint>
```

KubeRay 集群保持 worker 节点在线时，不能用“活跃 Worker 数降到 0”作为冷启动条件。本脚本通过 Ray Jobs API 的 `metadata.cube_job_kind=partition`（并兼容 `partition-` 提交 ID）确认当前没有活动 partition Job，然后为每个场景提交一个新的 Ray Job；`partition_job_attempts.ray_job_id` 的新 Job `start_time` 和 driver 内部 `ray.init` 会单独记录。脚本不要求本机直接连接 Ray GCS，因此 `CUBE_WEB_RAY_ADDRESS=auto` 的集群也可测量。输出中的 Jobs API 活跃任务数量会作为环境背景记录。

可以用准备脚本把完整景转换为 worker 可读的 MinIO COG，准备和上传耗时不计入剖分 10 秒门禁：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web \
python3.11 cube_web/scripts/prepare_single_well_manifest.py \
  --scene-mode full \
  --expected-optical-bands 4 \
  --optical-local-source /path/to/optical_4band_scene.tif \
  --radar-vv-source "$CUBE_PERF_RADAR_VV_SOURCE" \
  --radar-vv-header-source "$CUBE_PERF_RADAR_VV_HEADER_SOURCE" \
  --radar-vh-source "$CUBE_PERF_RADAR_VH_SOURCE" \
  --radar-vh-header-source "$CUBE_PERF_RADAR_VH_HEADER_SOURCE" \
  --product-source "$CUBE_PERF_PRODUCT_SOURCE" \
  --carbon-source "$CUBE_PERF_CARBON_SOURCE"
```

准备脚本的源对象通过命令行参数或 `CUBE_PERF_*` 环境变量传入，不把集群专用 source manifest 固定在代码中。例如：

```bash
export CUBE_PERF_OPTICAL_LOCAL_SOURCE='/path/to/optical_4band_scene.tif'
export CUBE_PERF_RADAR_VV_SOURCE='cube/source/.../radar_vv.dat'
export CUBE_PERF_RADAR_VV_HEADER_SOURCE='cube/source/.../radar_vv.hdr'
export CUBE_PERF_RADAR_VH_SOURCE='cube/source/.../radar_vh.dat'
export CUBE_PERF_RADAR_VH_HEADER_SOURCE='cube/source/.../radar_vh.hdr'
export CUBE_PERF_PRODUCT_SOURCE='cube/source/.../product.tif'
export CUBE_PERF_CARBON_SOURCE='cube/source/.../carbon.nc4'
```

运行命令：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web \
python3.11 cube_web/scripts/run_single_well_performance.py \
  --manifest /path/to/single_well_manifest.json
```

正式运行会从 MinIO 实际读取非碳 COG，复核宽高、波段数、波段索引和完整 `source_window`，并校验 SHA-256；`--skip-source-checksum` 不属于正式测试口径。脚本只从命令行或本地运行时配置读取 token、OpenGauss、Ray 和 MinIO 信息，不把凭据写入输出。

## 时间口径和产物

每个场景独立生成 `partition_run_id`，并记录：HTTP 导入/提交、任务轮询、OpenGauss attempt 创建/启动/完成、Ray Job start/end、冷启动门禁、Ray driver、Worker、源数据缓存和 checksum、格网计算、裁剪/写瓦片、MinIO、OpenGauss staging/提交等阶段。

输出目录包含：

- `TEST_REPORT.md`：中文汇总和每个 scope/phase 的耗时明细。
- `summary.csv`：每个场景一行，包含执行区间、端到端区间、队列、Ray Job 和 10 秒判定。
- `phase_timings.csv`：所有内部计时阶段的逐项记录。
- `raw_cases.json`：请求、任务、attempt、Ray Job 信息和原始计时树。
- `run_metadata.json`：运行配置、输入校验、MinIO 源对象校验和导入耗时。
- `namespaced_manifest.json`：本次实际导入的命名空间化 manifest。
- 多轮聚合目录还包含 `multi_round_samples.csv`、`multi_round_summary.csv` 和 `multi_round_phase_timings.csv`；后者保留每个轮次、场景、scope、phase 的详细耗时。
- `MULTI_ROUND_REPORT.html`：不依赖外部资源的静态 HTML 报告，包含输入规格、汇总结果、逐轮样本和全部阶段明细。

默认验收口径为 `both`：任务执行区间和请求到任务完成的端到端区间都必须小于 10 秒。可用 `--threshold-scope partition` 或 `--threshold-scope end_to_end` 只查看单一口径，但正式报告应保留两者。

## 多轮平均

单轮结果不作为稳定性能结论。完成多轮独立运行后，可用聚合脚本生成逐轮样本、原始均值、正常均值、标准差、范围和通过率：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web \
python3.11 cube_web/scripts/aggregate_single_well_performance.py \
  --root /path/to/multi_round_20260810
```

有效样本必须任务成功、Ray Job 为 `SUCCEEDED`、源 checksum 已校验、Worker 计时存在、冷启动门禁有观测且没有活动 partition Job；每轮还必须完整包含固定的 10 个场景，且 `threshold_scope=both`。正常均值只排除有阶段证据的远程调度长尾：driver 的 `ray.wait/ray.wait_get` 大于 10 秒，同时 Worker scope 总时长小于 6 秒；所有被排除的样本仍保留在逐轮 CSV 和原始报告中，不按是否通过 10 秒门槛筛样本。
