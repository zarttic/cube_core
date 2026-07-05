# 实体剖分性能优化记录

更新时间：2026-07-05

## 1. 范围

本文记录实体剖分的性能问题、实测结果、已落地优化和后续方向，主要覆盖：

- `cube_split.jobs.entity_partition_job`
- Web 侧调用链 `cube_web.services.partition_runners`

实体剖分的业务约束是：必须把源 TIF 按实体格网切分成小 GeoTIFF 瓦片，并上传到 MinIO。实体剖分不做中间 COG 转换，不能把逻辑剖分的 COG 优化结论直接套用到实体剖分。

## 2. 问题背景

实体剖分原链路为：

```text
源 TIF
  -> Ray worker 下载到本地缓存
  -> rasterio.mask 按实体 cell 裁剪
  -> 继承源 TIF profile 写小 GeoTIFF
  -> 批量上传实体瓦片到 MinIO
  -> 写 entity_index_rows.jsonl / index_rows.jsonl
```

初始怀疑点包括：

- 同一源 TIF 任务跨 actor 分发，可能重复下载源文件。
- 实体瓦片写出继承源 TIF 的 LZW 条带 profile，写大量小瓦片时成本高。
- worker 先全部写本地瓦片，再批量上传，写瓦片和网络上传不能重叠。
- 每个瓦片上传前后做 stat / identity 校验，小对象数量多时会放大请求成本。

真实测试后确认：实体剖分主瓶颈不是源下载，而是裁剪、写小瓦片和上传小瓦片。

## 3. 实测数据

测试数据使用当前载入的两个山东光学源资产：

- `s3://cube/cube/source/optocal/Shandong_mosaic_2020Q3_sr_band2_cut/Shandong_mosaic_2020Q3_sr_band2_cut.tif`
- `s3://cube/cube/source/optocal/Shandong_mosaic_2020Q3_sr_band3_cut/Shandong_mosaic_2020Q3_sr_band3_cut.tif`

源 TIF 特征：

- 单景约 268MB / 277MB。
- `int16`，源 profile 为 LZW 条带影像。
- `blockysize=1`，不是适合大量小瓦片写出的输出布局。

关键测试结果如下。worker 阶段耗时是各 actor 聚合值，可能大于 wall time。

| 场景 | 层级 | 小瓦片数 | 总耗时 | worker mask | worker write | worker upload |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 原始实体链路，继承源 profile | L3 | 62 | 10.99s | 未细分 | 未细分 | 未细分 |
| 强制每资产一个 actor | L3 | 62 | 19.62s | 未细分 | 未细分 | 未细分 |
| ZSTD 小瓦片 profile | L3 | 62 | 6.69s | 18.02s | 1.97s | 13.49s |
| ZSTD + 上传重叠 | L3 | 62 | 5.31s | 18.59s | 2.55s | 18.48s |
| ZSTD + fast upload，上传未重叠 | L4 | 332 | 16.66s | 18.83s | 3.98s | 74.17s |
| ZSTD + 上传重叠 | L4 | 332 | 8.83s | 19.10s | 4.65s | 43.58s |

### 3.1 worker 4 CPU/read/mask/write 复测

本轮聚焦 exact clip 默认模式下的 CPU/read/mask/write 阶段，不改变实体剖分必须输出小 GeoTIFF
瓦片的约束。测试仍使用上面的两个山东光学源资产，Ray 后端、MinIO 瓦片输出、`ray_parallelism=8`、
`minio_upload_workers=8`、`metadata_backend=none`，每次运行使用唯一 `minio_prefix`。

改动前 exact 路径每个 band 调一次 `rasterio.mask.mask`，`worker_entity_tile_mask_elapsed_sec`
同时包含窗口计算、读取和 polygon mask。改动后 exact 路径每个 tile 先计算一次
`raster_geometry_mask`，再按 band 窗口读取并复用同一个 2D mask，因此新增
`worker_entity_tile_read_elapsed_sec` 的真实读数；对比 CPU 裁剪时应看 read + mask。

| 场景 | 层级 | 小瓦片数 | 总耗时 | worker read | worker mask | read+mask | worker write | worker upload |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| before，`cb618e9` | L3 | 62 | 5.57s | 0.00s | 18.31s | 18.31s | 2.48s | 18.23s |
| after，手动 window/read/mask | L3 | 62 | 7.98s | 13.33s | 1.29s | 14.63s | 2.49s | 40.14s |
| before，`cb618e9` | L4 | 332 | 11.14s | 0.00s | 18.79s | 18.79s | 4.51s | 60.90s |
| after，手动 window/read/mask | L4 | 332 | 8.70s | 14.44s | 1.40s | 15.84s | 4.87s | 42.92s |

整体结论：

- exact CPU 裁剪阶段有收益：L3 read+mask 约 18.31s -> 14.63s，L4 约 18.79s -> 15.84s。
- L4 端到端也下降：11.14s -> 8.70s。L3 单次端到端变慢主要来自 MinIO upload 聚合值从
  18.23s 抖到 40.14s，而不是 read/mask 回退。
- 新增空 tile 快速路径：`raster_geometry_mask` 结果全空时跳过 band 读取和写出。上述 after
  运行分别跳过 L3 18 个、L4 146 个空 tile。
- 默认仍保持 `entity_clip_mode=exact`。`bbox` 仅保留为实验对比，不作为默认模式。

历史结论：

- 实体剖分不能简单把 actor 数限制到资产数。L3 严格每资产一个 actor 后从 10.99s 变成 19.62s，明显变慢。
- 输出 profile 是有效优化点。把小瓦片从继承源 LZW 条带改为 ZSTD tiled 后，L3 从约 10.99s 降到约 6.69s。
- 上传和裁剪/写出重叠是当前最大收益点。L4 从约 16.66s 降到约 8.83s。
- L4 输出 332 个小瓦片，总上传约 427MB，平均约 1.3MB。这里既有小对象请求成本，也有实际字节量成本。

## 4. 被验证但放弃的方向

### 4.1 每资产固定一个 actor

逻辑剖分中，同一资产固定到同一 actor 可以避免重复转 COG。实体剖分不转 COG，主耗时在 mask/write/upload。

实测 L3：

- `ray_parallelism=8`：约 10.99s。
- `ray_parallelism=2`，两景资产各一个 actor：约 19.62s。

因此实体剖分保留多 actor 并行处理同一资产任务，不按资产数 cap actor。

### 4.2 index-only

曾验证过只生成源 TIF window 索引、不生成小瓦片的上限性能：

- L3 约 2.22s。
- L4 约 2.24s。

但该方向不满足实体剖分“必须切成小块”的业务约束，已从生产代码撤回。后续实体剖分优化必须以物化小瓦片为前提。

### 4.3 bbox 快速切割

`bbox` 模式按 cell 外接窗口切矩形瓦片，确实能绕开 polygon mask，但真实数据上更慢：

- L3 bbox 输出 86 个瓦片，总耗时约 11.72s。
- L3 exact 输出 62 个瓦片，总耗时约 6.63s 到 5.31s。

原因是 bbox 产生更多、更大的瓦片，上传成本上升。默认继续使用 `exact`。

## 5. 已落地优化

### 5.1 实体瓦片输出 profile

原实现直接 `ds.profile.copy()`，导致小瓦片继承源 TIF 的 LZW 条带布局。

当前实体瓦片默认输出：

```text
GTiff
COMPRESS=ZSTD
PREDICTOR=2
ZSTD_LEVEL=1
TILED=YES
BLOCKXSIZE=512
BLOCKYSIZE=512
NUM_THREADS=2
```

真实瓦片微基准显示：

- 继承 LZW 条带：单块约 0.194s，约 7.72MB。
- ZSTD level 1 tiled：单块约 0.041s，约 5.96MB。
- ZSTD level 1 + predictor=2：在 L4 样本上比无 predictor 再小约 5%，写入时间基本不变。

### 5.2 上传与裁剪/写出重叠

原流程是 worker 写完全部本地瓦片后，再调用 `_upload_entity_tiles_to_minio` 批量上传。

当前流程为：

```text
mask / write one tile
  -> 立即提交后台上传任务
  -> worker 继续 mask / write 后续 tile
  -> 最后等待上传 futures 完成并回填 s3 URI
```

上传线程复用同一个 MinIO client，并把 SDK HTTP 连接池 `maxsize` 调整到至少等于
`minio_upload_workers`，避免 `--minio-upload-workers 16/32` 时仍受 SDK 默认 10 连接池影响。

收益：

- 网络上传与 CPU 裁剪/写出重叠。
- L4 小瓦片总耗时从约 16.66s 降到约 8.83s。
- 输出 row 仍然是 MinIO `s3://` URI，语义不变。

2026-07-05 在真实 MinIO 上做 upload-only 微基准，输出前缀使用
`cube/perf/entity-upload/worker3-*`，每次运行唯一前缀：

| 场景 | 对象数/大小 | workers | 上传耗时 |
| --- | ---: | ---: | ---: |
| 旧默认：SDK 默认连接池 + 8 workers | 64 × 1MiB | 8 | 1.62s |
| 新默认：连接池随 workers 扩大 + 16 workers | 64 × 1MiB | 16 | 1.46s |
| 新配置：连接池随 workers 扩大 + 32 workers | 64 × 1MiB | 32 | 1.42s |
| 旧默认：SDK 默认连接池 + 8 workers | 128 × 64KiB | 8 | 1.57s |
| 新默认：连接池随 workers 扩大 + 16 workers | 128 × 64KiB | 16 | 1.36s |
| 新配置：连接池随 workers 扩大 + 32 workers | 128 × 64KiB | 32 | 1.29s |

结论：16 workers 比旧默认 8 workers 稳定更快；32 workers 继续略快但收益变小，并会放大
Ray actor × 上传线程的集群并发。默认值采用 16，压测或大批量任务再显式调到 32。

### 5.3 fast upload

实体瓦片是本次运行新生成对象。性能优先时，默认不再对每个瓦片做上传前后 stat / identity sidecar 校验，直接 `fput_object`。

需要保守模式时可使用：

```bash
--minio-safe-upload
```

该模式会恢复远端 identity 检查，适合重复运行同版本输出、希望跳过已上传对象的场景，但性能会更差。

### 5.4 实体阶段计时

`job_report.json` 新增 worker 侧实体阶段指标：

- `worker_entity_source_resolve_elapsed_sec`
- `worker_entity_dataset_open_elapsed_sec`
- `worker_entity_geometry_elapsed_sec`
- `worker_entity_geometry_cache_hit_count`
- `worker_entity_dataset_bounds_elapsed_sec`
- `worker_entity_tile_read_elapsed_sec`
- `worker_entity_tile_mask_elapsed_sec`
- `worker_entity_tile_mask_cache_hit_count`
- `worker_entity_tile_empty_count`
- `worker_entity_tile_write_elapsed_sec`
- `worker_entity_writer_wall_elapsed_sec`
- `worker_entity_tile_upload_elapsed_sec`
- `worker_entity_tile_upload_count`
- `worker_entity_tile_count`

这些指标用于区分源准备、geometry、mask/read、write 和 upload，避免把实体剖分误判成 COG 问题。

### 5.5 Web 调用链

Web 侧实体剖分调用透传 `entity_clip_mode`，默认值为 `exact`。默认行为仍是生成实体小瓦片，不提供 index-only 生产路径。

## 6. 当前推荐配置

实体剖分性能优先的默认策略：

- `entity_clip_mode=exact`
- `ray_parallelism` 根据任务量和集群资源配置，不按资产数强制收敛
- `minio_upload_workers=16`
- 默认 fast upload
- 输出小瓦片使用 ZSTD tiled profile

示例命令：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m cube_split.jobs.entity_partition_job \
  --input-dir .tmp/perf_entity_loaded_input \
  --manifest-path .tmp/perf_real_loaded/shandong_2020q3_two_band_manifest.jsonl \
  --data-type optical \
  --product-family auto \
  --output-dir .tmp/perf_entity_loaded/exact_zstd_overlap_l4 \
  --grid-type isea4h \
  --grid-level 4 \
  --entity-clip-mode exact \
  --cover-mode intersect \
  --time-granularity day \
  --partition-prefix-len 3 \
  --partition-backend ray \
  --ray-parallelism 8 \
  --metadata-backend none \
  --asset-storage-backend minio \
  --minio-upload-workers 16
```

## 7. 后续优化方向

优先级从高到低：

1. 继续压缩上传体积。当前 L4 332 个实体瓦片约 427MB，上传仍是最大阶段之一。
2. 评估是否允许实体瓦片使用更低精度或额外 nodata 裁剪策略，前提是不破坏业务语义。
3. 继续用真实 L4/L6 任务复核 `minio_upload_workers=16/32` 的集群吞吐拐点。
4. 评估是否把多个小瓦片打包成可索引容器。但这会改变“每个实体瓦片一个 GeoTIFF 对象”的资产管理语义，不能作为默认生产路径。
5. 保留 `bbox` 作为实验选项，但只有在业务接受矩形窗口瓦片且真实数据证明更快时才使用。
