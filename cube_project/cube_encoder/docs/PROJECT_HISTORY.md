# 项目历史与归档索引

更新时间：2026-04-24  
适用范围：`cube_encoder` 历史交付、测试和方案索引。

## 1. 阶段摘要

2026-03-09 至 2026-03-11 的核心交付：

- 完成 `geohash`、`mgrs`、`isea4h` 多引擎路由。
- 完成 locate、cover、topology、space-time code API 主链路。
- 完成 Python SDK 交付形态。
- 完成 `cover_mode=intersect/contain/minimal` 的主要能力。
- 完成批量拓扑几何接口，用于前端可视化性能优化。
- 建立开发日志、Bug 日志、发布规范和性能烟测。

2026-04 中旬后的设计重点：

- 将逻辑剖分链路从“按景”扩展到“按格网单元 + 时间 + 波段”。
- 明确 Raw 层、Cube 层和 Control 层的职责。
- 将 COG window 引用作为当前轻量读取路径。
- 将 Spark/Iceberg 作为 PB 级扩展方向，而非当前最小闭环的强依赖。

## 2. 当前能力快照

已实现能力：

- Grid APIs: `/v1/grid/locate`, `/v1/grid/cover`
- Topology APIs: `/v1/topology/neighbors`, `/v1/topology/parent`, `/v1/topology/children`, `/v1/topology/geometry`, `/v1/topology/geometries`
- ST Code APIs: `/v1/code/st`, `/v1/code/st/batch`, `/v1/code/parse`
- SDK: `grid_core.sdk.CubeEncoderSDK`
- Performance smoke: `python -m grid_core.app.perf_smoke`

历史质量基线记录：

- 2026-03 阶段曾记录完整测试结果 `72 passed`。
- 后续提交应以当前代码运行结果为准，不直接复用历史数字。

## 3. 测试报告摘要

2026-04-16 数据剖分测试报告的有效结论：

- 真实 Landsat 样例可完成 COG 与逻辑剖分性能测试。
- Driver 模式和 Spark 模式均围绕 COG + grid cover 生成展开。
- 大规模并行测试的瓶颈主要集中在任务拆分、数据准备、运行环境和资源控制。
- 当前报告已归入 `docs/test_reports/`，作为实验记录而非当前操作手册。

## 4. 归档文件

历史设计与阶段汇报已移至 `docs/archive/`：

- `info.md`：早期系统设计长文。
- `STATUS_AND_PLAN.md`：2026-03 阶段状态与计划。
- `SPARK_COG_LOGICAL_PARTITION_REPORT.md`：Spark + COG 技术报告。
- `2026-04-19-OSS+Iceberg实施方案.md`：OSS/Iceberg 扩展方案。
- `2026-04-21-无景化遥感立方体入库方案.md`：无景化入库设计。
- `2026-04-21-无景化入库DDL与接口契约.md`：最小 DDL 和接口契约。
- `ARCHIVE_2026-03-09_to_2026-03-11.md`：阶段技术归档。
- `ARCHIVE_MGMT_2026-03-09_to_2026-03-11.md`：阶段管理归档。

归档文件不再作为当前唯一依据。若归档内容与 `ARCHITECTURE.md`、`INGEST_STORAGE_DESIGN.md` 或当前代码冲突，以后者为准。
