# Partition Optimization Real Data Test Report

测试时间：2026-07-05 11:15-11:22 CST

测试分支：`perf/partition-optimization-integration`

测试提交：`4b9b203`

## 1. 测试范围

本轮测试使用集成分支真实运行 Ray、MinIO 和 OpenGauss，不使用合成小样本作为结论依据。

运行环境：

- Ray：`10.3.100.182:6379`
- MinIO bucket：`cube`
- OpenGauss：通过本地运行时配置读取 DSN，报告不记录凭据
- 代码分支：6 个并行优化分支已合并到 `perf/partition-optimization-integration`

MinIO 当前真实源数据清点：

| 前缀 | 对象数 | 总大小 |
| --- | ---: | ---: |
| `cube/source/optocal/` | 8 | 3,486,453,441 bytes |
| `cube/source/product/` | 5 | 7,715,854,094 bytes |
| `cube/source/carbon/` | 1 | 52,918,009 bytes |
| `cube/source/radar/` | 96 | 1,757,686,366 bytes |

本轮执行了：

- 光学逻辑剖分：Shandong 2020Q3 两个真实 `s3://` TIF 源，`s2` 和 `tile_matrix`
- 光学实体剖分：Shandong 2020Q3 两个真实 `s3://` TIF 源，ISEA4H L4，`exact`，不限制 `max_cells_per_asset`
- OpenGauss 入库 smoke：Shandong 2020Q3 真实源，`tile_matrix` L5，验证批量 MERGE 链路
- 雷达逻辑剖分 smoke：Yangzhou Sentinel-1 真实 `.dat/.hdr` 源，`s2` L4

未纳入本轮 smoke：

- 信息产品真实大文件剖分：当前 product 单文件约 1.5GB，本轮不把它混入集成 smoke，避免把验证任务变成长 IO 压测。
- 碳卫星：本轮优化重点是逻辑/实体栅格剖分和入库链路，碳卫星未改核心性能路径。

## 2. 回归测试

集成分支合并 6 个优化分支后，已运行全量 Python 回归：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest
```

结果：

```text
472 passed, 1 skipped, 4 warnings in 21.84s
```

## 3. 真实数据测试结果

### 3.1 光学逻辑剖分 benchmark

数据源：

- `s3://cube/cube/source/optocal/Shandong_mosaic_2020Q3_sr_band2_cut/Shandong_mosaic_2020Q3_sr_band2_cut.tif`
- `s3://cube/cube/source/optocal/Shandong_mosaic_2020Q3_sr_band3_cut/Shandong_mosaic_2020Q3_sr_band3_cut.tif`

运行参数：

- `partition_backend=ray`
- `ray_parallelism=2`
- `chunk_size=1`
- `cog_compress=LZW`
- `timing_mode=true`
- `max_cells_per_asset=500`

结果：

| case | assets | grid tasks | total elapsed | partition elapsed | worker source | worker COG write | worker COG upload | worker rows | COG writes | COG cache hits | COG uploads | MinIO objects | MinIO bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `s2_l5_p2_c1_r1` | 2 | 24 | 10.211s | 10.206s | 0.196s | 13.749s | 2.812s | 0.016s | 2 | 12 | 2 | 2 | 442,691,078 |
| `tile_matrix_l5_p2_c1_r1` | 2 | 4 | 9.983s | 9.981s | 0.262s | 13.796s | 2.507s | 0.003s | 2 | 0 | 2 | 2 | 442,691,078 |

结论：

- 同一个源资产没有跨 actor 重复转 COG；每个 case 都是 2 个源资产、2 次 COG write、2 次 COG upload。
- `s2` case 内部出现 12 次 worker COG cache hit，说明 actor 内资产复用生效。
- 当前小规模逻辑剖分瓶颈仍是 COG 写入/上传，不是行生成。
- 本 benchmark 默认 `target_crs=EPSG:4326`，因此包含源 TIF 到 EPSG:4326 COG 的重投影成本；该成本不等同于坐标系 Transformer 缓存优化的行生成成本。

补充 A/B 校验：

为确认集成分支没有相对 `master` 开倒车，使用同一 Shandong manifest、同一 Ray/MinIO、同一 `s2 L5` 和同一并行参数补跑 A/B。

| 分支 | target_crs | total elapsed | partition elapsed | worker COG write | worker COG upload | worker rows |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `master` (`cb618e9`) | keep source CRS | 6.465s | 6.461s | 4.706s | 2.819s | 2.059s |
| `perf/partition-optimization-integration` (`1201a67`) | keep source CRS | 6.361s | 6.356s | 4.667s | 2.849s | 2.044s |
| `master` (`cb618e9`) | `EPSG:4326` | 10.018s | 10.013s | 13.959s | 2.472s | 0.015s |
| `perf/partition-optimization-integration` (`1201a67`) | `EPSG:4326` | 9.924s | 9.920s | 13.623s | 2.650s | 0.014s |

判断：

- 同参对比下集成分支未慢于 `master`。
- 坐标系 Transformer 缓存直接影响的 `worker rows` 阶段没有回退。
- 10s 级结果来自 `EPSG:4326` COG 重投影写入，`worker COG write` 约 13.6-14.0s；保持源 CRS 时 COG 写入约 4.7s。

输出前缀：

- `cube/benchmark/integration-real/logical/integration_real_20260705_111514/s2_l5_p2_c1_r1`
- `cube/benchmark/integration-real/logical/integration_real_20260705_111514/tile_matrix_l5_p2_c1_r1`

### 3.2 光学实体剖分 ISEA4H L4

数据源：同 Shandong 2020Q3 两个真实 `s3://` TIF 源。

运行参数：

- `partition_backend=ray`
- `ray_parallelism=2`
- `chunk_size=1`
- `grid_type=isea4h`
- `grid_level=4`
- `entity_clip_mode=exact`
- `asset_storage_backend=minio`
- `metadata_backend=none`
- `minio_upload_workers=16`
- 未设置 `max_cells_per_asset` 上限

结果：

| 指标 | 值 |
| --- | ---: |
| source assets | 2 |
| grid tasks | 682 |
| task groups | 14 |
| entity rows / tiles | 332 |
| distinct space codes | 166 |
| rows by band | `sr_band2=166`, `sr_band3=166` |
| uploaded tile count | 332 |
| total elapsed | 11.132s |
| partition elapsed | 9.628s |
| source prepare elapsed | 1.073s |
| worker writer wall | 18.611s |
| worker geometry | 5.806s |
| worker read | 5.614s |
| worker mask | 1.279s |
| worker write | 4.468s |
| worker upload | 17.981s |
| empty tile count | 146 |
| MinIO objects | 332 |
| MinIO bytes | 424,810,164 |

结论：

- 实体剖分没有转 COG，按源 TIF 直接切实体小 GeoTIFF。
- 不限制 `max_cells_per_asset` 时，L4 真实数据完整生成 332 个实体瓦片并上传到 MinIO。
- 当前实体剖分主要耗时仍在上传、geometry/read/write 组合上；`exact` mask 本身已经降到 1.279s worker 聚合时间。
- `minio_upload_workers=16` 能支撑 332 个实体小对象上传，本轮未观察到失败。

输出前缀：

- `cube/benchmark/integration-real/entity/integration_real_20260705_111514/exact_l4`

### 3.3 OpenGauss 入库 smoke

数据源：同 Shandong 2020Q3 两个真实 `s3://` TIF 源。

运行参数：

- `grid_type=tile_matrix`
- `grid_level=5`
- `partition_backend=ray`
- `ray_parallelism=2`
- `metadata_backend=postgres`
- `asset_storage_backend=minio`
- `postgres_batch_size=1000`

结果：

| 指标 | 值 |
| --- | ---: |
| source assets | 2 |
| grid tasks | 4 |
| total index rows | 4 |
| raw asset rows | 2 |
| cube fact rows | 4 |
| materialized COG assets | 2 |
| partition elapsed | 5.479s |
| ingest elapsed | 0.199s |
| total elapsed | 5.681s |
| worker COG write | 4.747s |
| worker COG upload | 2.782s |
| MinIO objects | 2 |
| MinIO bytes | 487,260,390 |

结论：

- Web/API 分支新增的 `postgres_batch_size` 参数最终进入入库链路，实际报告值为 `1000`。
- OpenGauss 批量 MERGE 链路可用，小规模 4 行入库耗时 0.199s。
- 这个 smoke 只验证链路正确性，不代表大规模 OpenGauss 写入吞吐上限。

输出前缀：

- `cube/benchmark/integration-real/logical-pg/integration_real_20260705_111514/tile_matrix_l5`

### 3.4 雷达逻辑剖分 smoke

数据源：

- `s3://cube/cube/source/radar/yangzhou_s1_2018_2020/Data/20180603_VV.dat`
- sidecar：同目录 `20180603_VV.hdr`

Manifest corners 从真实 ENVI header 的 UTM georeference 计算得到，未手写伪造范围。

运行参数：

- `data_type=radar`
- `grid_type=s2`
- `grid_level=4`
- `partition_backend=ray`
- `ray_parallelism=1`
- `metadata_backend=none`
- `asset_storage_backend=minio`

结果：

| 指标 | 值 |
| --- | ---: |
| source assets | 1 |
| grid tasks | 1 |
| total index rows | 1 |
| rows by band | `vv=1` |
| total elapsed | 2.230s |
| partition elapsed | 2.228s |
| worker source resolve | 0.093s |
| worker COG write | 0.110s |
| worker COG upload | 0.296s |
| worker rows | 0.015s |
| COG writes/uploads | 1 / 1 |
| MinIO objects | 1 |
| MinIO bytes | 39,570,083 |

结论：

- 雷达通用逻辑剖分可以走 worker 侧源解析、转 COG、上传和行生成路径。
- 本轮只测单景 VV、小范围 L4 smoke；未对 96 个雷达对象做全量压测。

输出前缀：

- `cube/benchmark/integration-real/radar/integration_real_20260705_111514/s2_l4_verify`

## 4. 产出文件

本轮测试汇总：

- `.tmp/integration_real_20260705_111514/real_data_test_summary.json`

关键 job report：

- `.tmp/integration_real_20260705_111514/logical/integration_real_20260705_111514/summary.json`
- `.tmp/integration_real_20260705_111514/entity/exact_l4/run_20260705_111658/job_report.json`
- `.tmp/integration_real_20260705_111514/logical_pg/tile_matrix_l5/run_20260705_111811/job_report.json`
- `.tmp/integration_real_20260705_111514/radar_s2_l4_verify/run_20260705_112136/job_report.json`

## 5. 合并建议

当前集成分支已满足：

- 6 个 worktree 分支全部合入集成分支
- Python 全量回归通过
- 光学逻辑剖分真实数据通过
- 光学实体剖分真实数据通过，且实体瓦片真实上传到 MinIO
- OpenGauss 小规模入库 smoke 通过
- 雷达逻辑剖分真实数据 smoke 通过

建议：

- 可以把 `perf/partition-optimization-integration` 作为候选合并分支。
- 合并 `master` 前如需覆盖信息产品，应单独安排 product 大文件 IO 压测；不要混在 smoke 里判断整体优化是否可合。
- 生产任务不要设置 `max_cells_per_asset`，保持默认不限制；该参数只用于 smoke/调试/采样。
