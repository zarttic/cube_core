# 实体剖分优化探索记录（2026-06-20）

文档状态：历史优化记录，不是当前 API 或格网矩阵；当前实体方式支持 `s2`、`tile_matrix`、`isea4h`。

## 1. 范围

本次记录只讨论 `ISEA4H` 实体剖分，不讨论普通逻辑剖分（`geohash` / `MGRS`）。

当前实体剖分覆盖三类数据：

- 光学 `tif`
- 信息产品 `tif`
- 雷达（本次未做完整实测）

其中本轮重点验证的是：

1. 光学单景 level 6 真实数据
2. 产品全量 `tif` 真实数据
3. Ray 并行调度、源文件缓存、实体瓦片上传和正式计时口径

## 2. 优化前是什么样子

优化前实体剖分链路有几个核心问题：

1. Ray worker 侧共享输出不完整
   - worker 生成的实体瓦片默认是本地路径
   - driver 侧不能直接把 worker 本地文件当成正式结果使用

2. 实体剖分沿用了不必要的 COG 思路
   - 逻辑剖分需要按 window/按需读取，COG 有价值
   - 实体剖分是直接切成小块输出，不需要先把整景转成 COG 再切

3. 计时口径混在一起
   - 源文件下载 / 本地缓存
   - 正式剖分
   - 上传
   原先容易混成一个总耗时，无法看清真正瓶颈

4. `cover` 规划过粗
   - 当前 cell 规划按影像外接 bbox 做 `intersect`
   - 不按真实有效像元 footprint 做裁剪
   - 导致会规划大量最终没有输出的 cell

5. Ray 准备阶段会重复缓存源影像
   - 旧实现里每个 actor 都会准备全部 `source_uri`
   - 资产一多时，会把缓存 I/O 和临时盘压力成倍放大

## 3. 这轮已经落地的优化

### 3.1 实体剖分不再转 COG

当前实体剖分 worker 直接读取源 `tif`，切实体瓦片，不再先把源影像转成中间 COG。

这点对实体剖分是正确方向：

- 少一次整景重写
- 少一次中间文件上传
- 更符合“直接切小块输出”的链路

### 3.2 Ray worker 直接上传实体瓦片到 MinIO

当前 Ray 实体剖分已经改成：

1. worker 本地切出实体瓦片
2. worker 直接上传到 MinIO
3. 返回的 index rows 写入 `s3://...` 共享路径

因此：

- driver 不再依赖 worker 本地路径
- `ingest_enabled=False` 只影响元数据入库，不影响 worker 把实体瓦片上传到 MinIO

### 3.3 计时口径拆分

当前报告里已经拆成下面几段：

- `source_prepare_elapsed_sec`
- `source_prepare_worker_elapsed_sec`
- `partition_elapsed_sec`
- `partition_stage_total_elapsed_sec`
- `worker_partition_elapsed_sec`

其中本轮已经按用户要求明确：

- `partition_elapsed_sec` 只统计正式剖分阶段
- 不包含源影像下载 / 缓存时间

### 3.4 实体任务分组改成更适合并行

当前实体任务不再是简单“一 cell 一个任务”，而是：

- 先按 `asset_path + space_code prefix` 分组
- 每组上限动态控制，最大 `64 tasks/group`
- 再按 Ray 并行度切 chunk

这样做的目标是：

- 避免任务过碎，Ray 调度成本过高
- 保留一定空间局部性

### 3.5 实体 Ray 调度改成源影像亲和

这是这轮很关键的改动。

当前 `_write_entity_tile_chunks_ray()` 做了两层变化：

1. actor 只准备自己负责的 `source_uri`
2. task groups 按 `asset_path` 做 actor 亲和分配

目标是避免以下旧问题：

- 5 景产品影像
- 16 个 actor
- 每个 actor 都去缓存全部 5 景

现在的方向是：

- 尽量让一个 actor 只负责单一 source
- 如果某个 source 的 group 太多，再给这个 source 分多个 actor 槽位

这能明显降低准备阶段的重复下载和临时盘压力。

### 3.6 Ray runtime env 透传源缓存目录

当前 Ray runtime env 已经支持透传：

- `CUBE_SOURCE_CACHE_DIR`

这样后续可以把实体剖分的源缓存目录从默认 `/tmp/cube_split_source_cache` 挪到更合适的位置。

## 4. 真实数据测试结论

### 4.1 光学单景 level 6

测试对象：

- `Shandong_mosaic_2020Q3_sr_band2_cut.tif`

已得到的关键结果：

- `grid_task_count = 14927`
- `entity_tile_count = 6225`

说明：

- 规划了 `14927` 个 cell
- 最终只有 `6225` 个实体瓦片输出
- 中间约有 `8700+` 个 cell 做了裁剪/掩膜判断，但没有产出

这已经非常明确地说明：

- 主要时间不是花在上传
- 也不是花在源文件首次缓存
- 而是花在大量“最后为空”的 cell 探测和 raster mask 上

并行度实验结果：

| 场景 | 结果 |
|---|---:|
| Ray 4 workers | `partition_elapsed_sec ≈ 113.87s` |
| Ray 8 workers | `partition_elapsed_sec ≈ 78.606s` |
| Ray 16 workers | 约 `80s` 量级 |
| Ray 32 workers | 约 `74.437s` |

结论：

- 4 太低，明显变慢
- 8 到 16 有收益，但不大
- 16 到 32 只有小幅改善
- 说明瓶颈已经不是简单“worker 数不够”

### 4.2 产品全量 level 6

本次真实产品数据来自 MinIO `cube/source/product/`，共 5 景 `tif`，每景约 `1.54GB`。

规划结果：

- `asset_count = 5`
- `grid_task_count = 23825`
- 每景约 `4765` 个 level 6 cell

这轮没有拿到完整正式耗时，原因不是切片逻辑失败，而是集群运行环境问题：

- Ray 节点 `/tmp` / `/tmp/ray` 空间不足
- 准备阶段下载源影像时报 `No space left on device`

因此当前对产品全量任务的结论是：

1. 调度问题已经被识别出来
   - 旧实现会让 actor 重复准备全部源影像
   - 现在已经朝 source-affinity 方向修正

2. 但全量真实压测还没有在健康集群状态下收完
   - 这部分属于“待补最终压测数据”，不是“逻辑未实现”

## 5. 当前瓶颈判断

### 5.1 主要瓶颈不是下载

对光学单景的真实结果看，源缓存准备阶段大约只有 `1.2s` 左右量级（缓存命中时）。

因此实体剖分当前主瓶颈不是：

- MinIO 下载
- 本地缓存
- 实体瓦片上传

### 5.2 主瓶颈是大量空 cell 的 raster mask / random read

对 level 6 光学单景的局部 profiling，结论很稳定：

- `rasterio.mask.mask` 本身就是主要成本
- 本地 GeoTIFF 写出是次要成本
- `MemoryFile` 也没有带来收益，真实测试反而更慢

换句话说，当前最慢的是：

1. 规划过多候选 cell
2. 对这些 cell 逐个做裁剪/掩膜
3. 其中很大一部分最后为空

### 5.3 当前 cover 方式天然会高估候选 cell

实体剖分当前仍然按影像 bbox 做 `cover_mode=intersect`。

它的问题不是错，而是粗：

- bbox 覆盖到的区域，很多是 nodata
- 但这些 nodata 区域的 cell 仍然会进入后续裁剪判断

所以当前流程里存在明显浪费：

- “bbox 交到就算候选”
- “真正是否有有效像元，留到后面逐 cell 检查”

## 6. 当前最合理的优化方向

### 6.1 第一优先级：有效像元预过滤

最值得做的不是继续堆 worker 数，而是尽量在正式逐 cell 掩膜前减少候选量。

目标：

- 在不漏掉真实有效边缘像元的前提下
- 先做一次粗粒度有效区预判
- 把明显落在 nodata 大区里的 cell 提前排掉

合适方向：

1. 基于 overview / downsample mask 的粗过滤
2. driver 侧一次性做资产级预过滤
3. 这部分时间记入 `source_prepare` 或独立准备阶段，不混进 `partition_elapsed_sec`

不合适方向：

- 直接改 `cover_mode=contain`
  - 会漏边缘有效像元

### 6.2 第二优先级：继续强化 source-affinity 调度

实体剖分天然适合并行，但并行前提是：

- 不要让每个 actor 重复准备同一批大源影像

因此调度原则应该是：

1. 先按 `asset_path` 分桶
2. 在资产内部再按 `space_code prefix` 切 group
3. actor 优先保持 source-affinity
4. 只有单资产 group 太多时，再给这个资产分多个 actor

这个方向对产品全量任务尤其重要，因为产品单景大、资产数少、单资产体积很高。

### 6.3 第三优先级：缓存目录治理

当前真实压测的直接阻塞点是：

- 节点 `/tmp`
- 节点 `/tmp/ray`

所以后续继续做产品/光学完整压测前，至少要满足下面之一：

1. 给 worker 可写的大缓存目录，并通过 `CUBE_SOURCE_CACHE_DIR` 下发
2. 统一清理历史 `/tmp/ray` 和 `/tmp/cube_split_source_cache`
3. 把实体剖分压测固定到临时盘空间健康的节点

否则会出现一种假象：

- 代码逻辑已经改对
- 但任务还会先被临时盘耗尽打断

## 7. 目前不建议继续做的方向

1. 不建议给实体剖分重新引入中间 COG
   - 实测没有带来提速
   - 链路更重

2. 不建议单纯继续堆大并行度
   - 16 到 32 的收益已经很有限
   - 反而更容易放大缓存和临时盘压力

3. 不建议只改上传方式
   - 上传不是当前主瓶颈

## 8. 当前状态总结

本轮关于实体剖分，已经拿到几个比较明确的结论：

1. 实体剖分不需要中间 COG，这个方向已经确认
2. 真正主瓶颈在逐 cell 的 raster mask / random read，不在上传
3. 当前 bbox + intersect 的 cover 方式会规划过多无效 cell
4. 产品全量任务的关键问题不是切片逻辑，而是源缓存和节点临时盘压力
5. Ray 调度要尽量保持 source-affinity，避免 actor 重复准备全部源影像

## 9. 下一步建议

如果继续推进实体剖分优化，建议按下面顺序做：

1. 先把集群缓存目录 / 临时盘问题处理干净
2. 在 driver 侧实现一次安全的有效像元粗过滤
3. 再对光学单景 level 6 做前后对比
4. 最后重跑产品 5 景全量 level 6，收正式报告

判断是否真正有效的标准也应该很明确：

- `grid_task_count` 明显下降
- `entity_tile_count` 基本不变
- `partition_elapsed_sec` 明显下降
- 不再因为 `/tmp` 或 `/tmp/ray` 失败中断
