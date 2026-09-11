# 归档文档

最近整理时间：2026-09-11

本目录只保留**历史追溯**用的文档：一次性验收/性能报告、事故报告、阶段性设计稿、
交付交接单和已完成迁移的记录。这些内容不作为当前契约或运行依据，不随当前代码更新。

当前文档以以下位置为准：

- 仓库协作规则：[AGENTS.md](../../AGENTS.md)
- 项目入口：[README.md](../../README.md)
- Web API 与运行：[cube_web/docs/README.md](../../cube_web/docs/README.md)
- 剖分与入库工作流：[cube_split/docs/README.md](../../cube_split/docs/README.md)
- 编码器架构与 SDK：[cube_encoder/docs/README.md](../../cube_encoder/docs/README.md)

文件按来源目录分到子目录，原始路径用 Git 历史可查（`git log --follow <file>`）。

## 本次归档：原根目录 `docs/`

以下文件原位于仓库根目录 `docs/`，现统一保存在 `archive/docs/project/`。它们只用于历史追溯、测试证据和问题上下文，不作为当前契约或运行依据。

| 文件 | 原路径 | 说明 |
| --- | --- | --- |
| [`project/README.md`](project/README.md) | `docs/README.md` | 原项目级文档索引 |
| [`project/ARCHITECTURE.md`](project/ARCHITECTURE.md) | `docs/ARCHITECTURE.md` | 系统架构与数据流 |
| [`project/OPERATIONS.md`](project/OPERATIONS.md) | `docs/OPERATIONS.md` | 部署运行与运行时配置 |
| [`project/ACCEPTANCE_AND_TESTING.md`](project/ACCEPTANCE_AND_TESTING.md) | `docs/ACCEPTANCE_AND_TESTING.md` | 测试与验收门禁 |
| [`project/OPEN_ISSUES.md`](project/OPEN_ISSUES.md) | `docs/OPEN_ISSUES.md` | 待处理问题历史记录 |
| [`project/quality_retry_failure_matrix.json`](project/quality_retry_failure_matrix.json) | `docs/quality_retry_failure_matrix.json` | 质检失败场景机器可读基准 |
| [`project/当前OpenGauss关系型数据库表说明.md`](project/当前OpenGauss关系型数据库表说明.md) | `docs/当前OpenGauss关系型数据库表说明.md` | OpenGauss 表结构与存储说明 |
| [`project/操作员权限接入说明.md`](project/操作员权限接入说明.md) | `docs/操作员权限接入说明.md` | 操作员权限接入说明 |

## 既有项目级归档

| 文件 | 原路径 | 说明 |
| --- | --- | --- |
| `DB_POOL_LEAK_REPORT.md` | `docs/` | OpenGauss 连接池泄露问题报告（2026-08-09） |
| `INGEST_WORKFLOW.md` | `docs/` | 剖分批次入库流程旧稿（2026-07-24） |
| `LOADER_SCHEMA_HANDOFF.md` | `docs/` | 载入子系统交付契约（2026-08-09） |
| `MULTI_GRID_PARTITION_DESIGN.md` | `docs/` | 多格网剖分流程设计稿（2026-07-24） |
| `PRODUCTION_TEST_ACCEPTANCE.md` | `docs/` | 生产测试与验收基线（2026-08-16） |
| `QUALITY_RETRY_FAILURE_REPORT.md` | `docs/` | 质检与重试失败场景报告（2026-07-21） |
| `auth-integration-guide.md` | `docs/` | 认证集成方案（2026-08-11） |
| `测试大纲执行结果_20260824.md` | `docs/` | 测试大纲执行结果（2026-08-24） |
| `分析就绪数据剖分管理系统测试大纲.docx` | `docs/` | 测试大纲原稿 |
| `environment_versions.txt` | 仓库根 | 2026-07-13 开发者环境版本快照（文件自述非生产契约） |

## KubeRay 迁移包（`kuberay-migration/`）

| 文件 | 原路径 | 说明 |
| --- | --- | --- |
| `README.md`、`01-our-work.md`、`02-business-handoff.md` | `docs/kuberay-migration/` | 迁移包说明、执行清单与业务交接单 |
| `templates/` | `docs/kuberay-migration/templates/` | 迁移期 raycluster、runtime-env、Dockerfile 模板 |

KubeRay 已投入运行，运行约束以 [cube_web/docs/KUBERAY_OPERATIONS.md](../../cube_web/docs/KUBERAY_OPERATIONS.md) 为准。

## 性能产物（`single_scene_performance/`、`quality-performance/`）

单景与实体剖分的多轮性能报告、原始 CSV/HTML 产物，以及质检吞吐量报告。
历史性能报告保留原始结论，但**不代表当前契约或当前性能**，测量时间以文件内记录为准。

| 文件 | 原路径 | 说明 |
| --- | --- | --- |
| `single_scene_performance/MULTI_ROUND_REPORT.md`、`MULTI_ROUND_REPORT.html`、`multi_round_*.csv|json` | `docs/single_scene_performance/` | 单景剖分多轮性能测试（2026-08-16） |
| `single_scene_performance/entity_optimization/` | `docs/single_scene_performance/entity_optimization/` | 实体剖分优化报告与采样 |
| `single_scene_performance/minio_tile_io_*` | `docs/single_scene_performance/` | MinIO 瓦片 IO 基准 |
| `quality-performance/quality-throughput-report.html` | `docs/quality-performance/` | 质检吞吐量报告 |

## 包级

| 文件 | 原路径 | 说明 |
| --- | --- | --- |
| `cube_web/PARALLELISM_PERFORMANCE_REPORT_2026-07-27.md` | `cube_web/docs/` | 并行优化验收与性能报告 |
| `cube_web/SINGLE_WELL_PERFORMANCE.md` | `cube_web/docs/` | 单井完整景剖分性能测试 |
| `cube_split/CELL_GEOM_MIGRATION.md` | `cube_split/docs/` | `rs_cube_cell_fact.cell_geom` 迁移记录（已完成） |
