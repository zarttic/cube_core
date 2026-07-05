# 逻辑剖分性能优化记录

更新时间：2026-07-05

## 1. 范围

本文记录普通逻辑剖分的性能问题、实测结果和优化思路，主要覆盖：

- `cube_split.jobs.ray_logical_partition_job`
- `cube_split.jobs.product_partition_job`
- `cube_split.jobs.ray_partition_core`

逻辑剖分会把源 TIF 标准化为 COG，再生成 cell/window 索引行。实体剖分不是这个流程：实体剖分直接对源 TIF 裁剪实体瓦片，不应把 COG 转换耗时作为实体剖分瓶颈。

## 2. 问题背景

真实 Ray/MinIO 环境中，逻辑剖分一开始的总耗时明显偏高。前期怀疑点集中在 COG 转换和 Ray 并行调度：

- Ray 按任务组分发，两个源资产的任务可能被多个 actor 分摊。
- actor 内有 `_local_cog_by_source` 缓存，但 actor 之间不共享。
- 同一个源 TIF 如果落到多个 actor，就可能重复下载、重复转 COG、重复上传 COG。
- 任务组粒度较小，worker 内会多次进入 `_process_local_task_group`，带来重复打开数据集和 Python 调度开销。

后续真实计时表明，COG 转换不是唯一瓶颈，甚至不是最大瓶颈。更大的热点来自每条索引行重复构造 CRS Transformer。

## 3. 实测结论

测试数据使用当前载入的两个山东光学源资产：

- `s3://cube/cube/source/optocal/Shandong_mosaic_2020Q3_sr_band2_cut/Shandong_mosaic_2020Q3_sr_band2_cut.tif`
- `s3://cube/cube/source/optocal/Shandong_mosaic_2020Q3_sr_band3_cut/Shandong_mosaic_2020Q3_sr_band3_cut.tif`

关键结果如下。worker 阶段耗时是各 actor 聚合值，可能大于 wall time。

| 场景 | 总剖分耗时 | 源解析/下载 | COG 写入 | COG 上传 | 行剖分 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 优化前，LZW | 33.473s | 未单独统计 | 4.715s | 2.999s | 54.286s |
| 缓存 Transformer 后，LZW | 7.223s | 1.640s | 4.691s | 3.008s | 2.104s |
| 缓存 Transformer 后，NONE | 13.822s | 未单独列出 | 更低压缩成本 | 16.066s | 已降低 |

结论：

- 最大收益来自缓存 CRS Transformer，总耗时从约 33.5s 降到约 7.2s。
- COG 写入约 4.7s，不是 33s 总耗时的主因。
- 不压缩 COG 反而更慢：`NONE` 输出约 1247MB，`LZW` 输出约 465MB，上传时间显著变长。
- 当前默认继续使用 `LZW + predictor=2`，`NONE` 只保留为测试选项。

### 3.1 可复现小规模 benchmark

新增脚本：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 \
  cube_split/scripts/run_logical_partition_benchmark.py \
  --work-dir /tmp/cube_logical_partition_benchmark_worker1 \
  --ray-address 10.3.100.182:6379 \
  --minio-endpoint 10.3.100.179:9000 \
  --grid-types s2,tile_matrix \
  --s2-level 5 \
  --tile-matrix-level 5 \
  --ray-parallelism-values 2 \
  --chunk-sizes 1 \
  --max-cells-per-asset 500
```

默认 manifest 使用山东 2020Q3 的 `sr_band2`/`sr_band3` 两个 MinIO 源资产，运行时生成在
`<work-dir>/<run-id>/manifest/shandong_2020q3_2band.jsonl`。脚本只调用现有
`run_logical_partition`，不复制剖分逻辑；默认 `metadata_backend=none`，只做剖分和 COG
写入 MinIO。

可调参数：

- `--grid-types s2,tile_matrix`：普通逻辑剖分格网，默认覆盖两者。
- `--ray-parallelism-values 1,2,4`：同一输入下横向比较 actor 数。
- `--chunk-sizes 1,4,8`：比较 task group chunk 粒度。
- `--repeat N`：重复每个 case，便于区分冷/热缓存波动。
- `--cog-compress LZW|DEFLATE|ZSTD|NONE`：压缩策略对 CPU 和上传的影响。
- `--manifest-path ...`：替换为其他真实源资产 manifest。

### 3.2 worker1 验证结果

执行时间：2026-07-05。源对象可访问性：

- `Shandong_mosaic_2020Q3_sr_band2_cut.tif`：268105481 bytes。
- `Shandong_mosaic_2020Q3_sr_band3_cut.tif`：277461750 bytes。

Ray 集群：`10.3.100.182:6379`，MinIO endpoint：`10.3.100.179:9000`，bucket：`cube`。
本次 run id：`logical_bench_20260705_013405`。

| case | grid | level | assets | grid tasks | rows | parallelism/chunk | partition | total | worker source | worker COG write | worker COG upload | worker rows | COG writes/uploads | MinIO prefix |
| --- | --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| `s2_l5_p2_c1_r1` | s2 | 5 | 2 | 24 | 24 | 2/1 | 10.181s | 10.186s | 0.186s | 13.834s | 2.600s | 0.014s | 2/2 | `cube/benchmark/logical/logical_bench_20260705_013405/s2_l5_p2_c1_r1` |
| `tile_matrix_l5_p2_c1_r1` | tile_matrix | 5 | 2 | 4 | 4 | 2/1 | 9.572s | 9.574s | 0.198s | 13.615s | 2.566s | 0.003s | 2/2 | `cube/benchmark/logical/logical_bench_20260705_013405/tile_matrix_l5_p2_c1_r1` |

MinIO 输出确认：

- `s2_l5_p2_c1_r1`：2 个 COG 对象，合计 442691078 bytes。
- `tile_matrix_l5_p2_c1_r1`：2 个 COG 对象，合计 442691078 bytes。

解读：

- 两个格网的总耗时接近，因为本规模下固定成本主要是两源资产的 COG 写入和上传。
- `tile_matrix` level 5 只产生 4 行，行剖分耗时约 0.003s；`s2` level 5 产生 24 行，行剖分耗时约 0.014s。
- worker 阶段耗时为 actor 聚合值，可能大于 wall time；本次 COG 写入聚合约 13.6-13.8s，但 wall time 约 9.6-10.2s。

## 4. 根因

### 4.1 同一资产跨 actor 重复准备

原 Ray 调度按 chunk 轮询分发，不按 `asset_path` 分组。结果是同一个源 TIF 的任务可以分布在多个 actor 上。

影响：

- 每个 actor 都会独立下载源 TIF 到本地缓存。
- 每个 actor 都可能独立把同一个源 TIF 转成 COG。
- 每个 actor 都可能独立上传同一个 COG。
- actor 内缓存命中率看起来正常，但全局仍然浪费。

### 4.2 worker 内小批量重复处理

原 worker 对每个 task group 调一次 `_process_local_task_group`。当同一个资产拆成大量小 group 时，会放大函数调用、数据集打开和行迭代开销。

### 4.3 每行重复构造 Transformer

`process_partition` 中每条输出行都要把 WGS84 cell bounds 转到数据集 CRS。原实现每次调用 `_wgs84_to_dataset_bounds` 时都会创建：

```python
Transformer.from_crs("EPSG:4326", ds.crs, always_xy=True)
```

这个对象创建成本高，且同一个打开的数据集 CRS 不变。真实测试中，这个问题把行剖分聚合耗时放大到 54s 级别。

### 4.4 “不压缩更快”的假设不成立

对本批数据，COG 不压缩会减少一部分 CPU 压缩开销，但输出字节数大幅增加，MinIO 上传成为更重的瓶颈。因此 `NONE` 不适合作为默认性能选项。

## 5. 已落地优化

### 5.1 按 asset_path 固定 Ray actor

新增 `_resolve_ray_actor_parallelism` 和 `_chunk_task_groups_by_actor`：

- actor 数上限按不同 `asset_path` 数量收敛。
- 同一个 `asset_path` 的所有 task group 固定到同一个 actor。
- actor 内 COG 缓存变成有效缓存，避免跨 actor 重复准备同一源资产。
- 产品剖分 Ray 路径复用同一分组策略。

这个优化牺牲了“同一资产内多 actor 并行”的能力，但当前没有跨 actor 共享 COG 缓存或分布式锁。相比重复下载/转换/上传，先保证每个源资产只准备一次更稳。

### 5.2 worker 内批量处理 task group

Ray worker 现在把同一批 `prepared_groups` flatten 后一次调用 `_process_local_task_group`。产品剖分和本地 thread 路径也做了同类批处理。

收益：

- 减少 Python 调用次数。
- 减少同一资产反复打开/关闭。
- 让 `process_partition` 内部的数据集复用和 Transformer 缓存真正生效。

### 5.3 按数据集缓存 CRS Transformer

`process_partition` 打开一个数据集后，只创建一次 WGS84 -> dataset CRS 的 Transformer，并传入 `_wgs84_to_dataset_bounds` 复用。

这是本轮实测最大收益点。

### 5.4 增加 worker 阶段计时

Ray 逻辑剖分报告新增 worker 侧聚合指标：

- `worker_source_resolve_elapsed_sec`
- `worker_cog_write_elapsed_sec`
- `worker_cog_upload_elapsed_sec`
- `worker_partition_rows_elapsed_sec`
- `worker_cog_write_count`
- `worker_cog_cache_hit_count`
- `worker_cog_upload_count`

这些字段用于区分下载、COG 写入、上传和索引行生成，避免只看总耗时误判瓶颈。

### 5.5 COG 压缩策略

`cog_creation_options` 支持 `COMPRESS=NONE`，并在 `NONE` 时跳过 `PREDICTOR` 和 `LEVEL`。

默认仍保持：

```text
COMPRESS=LZW
PREDICTOR=2
OVERVIEWS=NONE
NUM_THREADS=ALL_CPUS
```

原因是当前真实数据下 LZW 总耗时更低。

## 6. 后续优化思路

### 6.1 逻辑剖分

优先级从高到低：

1. 继续用 worker timing 定位真实瓶颈，不再凭感觉调 COG 参数。
2. 保持按资产固定 actor，除非实现跨 actor 共享 COG 成果。
3. 如果资产数很少但单资产任务极多，再考虑“每个资产一个准备 actor + 多个计算 actor”的两阶段模型。
4. 如果要恢复同一资产多 actor 并行，需要先做全局 COG 去重，例如基于 MinIO 对象存在性、对象 identity 和分布式锁。
5. 对上传阶段继续使用 identity sidecar 跳过重复上传，避免把 CPU 优化换成网络瓶颈。

### 6.2 对实体剖分的借鉴边界

实体剖分不会转 COG，它直接读取源 TIF 并裁剪实体瓦片。因此逻辑剖分里的 COG 写入、COG 上传指标不能直接套到实体剖分。

可以借鉴的是 Ray 调度原则：

- 同一个源 TIF 的实体任务应固定到同一个 actor，避免重复下载源 TIF。
- worker 内应按资产批量处理，减少重复打开大 TIF。
- 实体剖分需要单独统计 `source_prepare`、`tile_write/mask`、`tile_upload`、`metadata`，不能用 `cog_elapsed_sec` 判断。

实体剖分下一步应直接围绕这些阶段做计时和优化。
