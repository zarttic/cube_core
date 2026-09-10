# 并行优化真实验收与性能报告

日期：2026-07-27

## 范围

本报告在 `cube_project_fix` 执行，使用 `user-1` MinIO 的真实 GF1 光学、S1
雷达、信息产品 COG 和 TanSat XCO2 NetCDF。验收脚本会在提交任务前对每个
`s3://user-1/...` 源对象执行 `stat_object`，避免把缺失对象误判为调度或入库问题。每轮
使用独立命名空间，覆盖 Geohash、MGRS、ISEA4H、取消重试、质量 Pass/Warn/Fail、手动
入库、发布及撤回。

Ray 环境为 4 个存活节点、48 CPU。所有入库均由 `local-development` 的显式手动请求
创建；自动入库保持默认关闭。

## 实现

- 质量 worker：独立 quality lease 以受限线程池执行，默认 4，环境变量
  `CUBE_WEB_QUALITY_MAX_WORKERS` 可降为 1。
- 入库 worker：只在独立 `dataset_id + output_version` claim group 之间并发；同一组内的
  band unit 继续串行，默认 4，环境变量 `CUBE_WEB_INGEST_MAX_WORKERS` 可降为 1。
- 真实验收：先扇出全部质量请求，确认没有自动入库任务后扇出全部手动入库请求，再等待
  终态。这个改动消除了验收脚本自身逐 Dataset 等待造成的串行瓶颈。
- 分区批次调度（显式设置 `CUBE_WEB_RAY_BATCH_SCHEDULER=1`）：同一分区批次的所有
  Geohash/MGRS scene-band 单元先进入 staging，再由一次 Ray 初始化和一个全局、有界队列
  调度。`CUBE_LOGICAL_MAX_IN_FLIGHT` 默认 4，用于限制同时写入 OpenGauss staging 的
  逻辑 chunk 任务。单 Dataset 入口保留为兼容封装。
- ISEA4H 实体切片和 Carbon 保持原有内部 Ray 执行路径，未为了外层批次化改变其实体输出
  或写入语义。

批次调度默认关闭。真实单次验收尚未证明其在共享 Ray/OpenGauss 环境中有稳定加速，因此
只有明确启用环境变量的任务会走新路径；未设置时保持原有逐 Dataset 调度。

## 性能结果

| 场景 | 分区秒数 | 质量加手动入库秒数 | 说明 |
| --- | ---: | ---: | --- |
| 原验收驱动，逐 Dataset 等待 | 72.289 | 16.420 | 串行请求和等待掩盖 worker 吞吐 |
| 扇出驱动，worker 上限 1 | 81.803 | 4.334 | 同一真实 4 Dataset、8 个入库单元 |
| 扇出驱动，worker 上限 4 | 69.938 | 4.319 | 同一真实工作负载 |
| 批次调度关闭（本次对照） | 65.567 | 4.336 | `user-1` 完整严格验收 |
| 批次调度开启（本次候选） | 71.255 | 4.541 | 同一 manifest、相同验收门禁 |

验收驱动扇出将质量加手动入库阶段由 16.420 秒降至 4.334 秒，减少 73.6%，约 3.79 倍。

在该小型真实队列中，worker 1 与 4 的质量加手动入库时间仅相差 0.015 秒（0.3%）。
原因是 8 个实际入库单元的执行时间低于 worker 1 秒轮询和验收轮询粒度；因此不能宣称
worker 并发在该样本上有可测加速。实现保留，因为单元测试已证明独立 lease/group 的受限
并发，且真实验收确认其不改变数据终态。

本次候选的分区阶段比对照慢 5.688 秒（8.7%），因此不能将它作为性能优化成果。逐格网的
服务端任务耗时如下，连未改动的 ISEA4H 也慢了 2.936 秒，表明单次差异同时包含共享
Ray/OpenGauss/对象存储负载波动，不能归因于逻辑批次队列：

| 格网 | 调度关闭秒数 | 调度开启秒数 | 差异秒数 |
| --- | ---: | ---: | ---: |
| Geohash | 4.505 | 5.123 | +0.618 |
| MGRS | 18.427 | 19.200 | +0.773 |
| ISEA4H | 38.602 | 41.538 | +2.936 |

后续性能判断需要在隔离的 Ray/OpenGauss 负载下做至少三轮交替重复；在得到稳定结果前，新
调度保持 opt-in，正确性验收结果不被当作吞吐提升证据。

## 正确性门禁

本次对照和批次调度验收均通过，并且批次调度验收得到：

- 4 个主 Dataset 质量 `pass`；独立探针分别得到 `warn` 和 `fail`。
- 手动入库创建数为 3、2、2、1，共 8 条已完成入库记录。
- Dataset 7、Scene 8、PartitionRun 6、IngestRun 8；无 Scene、batch Scene、partition
  Scene、ingest Scene 孤儿行，也不存在无 output 的 completed Scene。
- 发布状态完成 `active -> withdrawn`。

最终回归：

- 直接覆盖：95 passed（批次调度、工作流、真实验收门禁、实体/Carbon 切片）。
- 跨包 `not real_aoi`：781 passed，1 skipped，1 deselected；仅有 FastAPI/httpx 弃用和
  rasterio 无地理参考两条既有警告。
- `git diff --check`：通过。
