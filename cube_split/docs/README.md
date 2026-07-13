# cube_split 文档索引

更新时间：2026-07-13

本文档目录维护 `cube_split` 当前剖分、入库、质检和读取链路。旧联调记录和过时方案已经清理出当前文档集。

## 当前文档

- [WORKFLOW.md](WORKFLOW.md)：当前剖分、manifest、入库、质检、AOI 读取和运行参数说明。
- [PLANE_GRID_PARTITION.md](PLANE_GRID_PARTITION.md)：源 CRS 保留型逻辑窗口剖分的当前契约和已知限制。
- [LOGICAL_PARTITION_PERFORMANCE.md](LOGICAL_PARTITION_PERFORMANCE.md)：逻辑剖分性能问题、实测瓶颈和优化思路记录。
- [ENTITY_PARTITION_PERFORMANCE.md](ENTITY_PARTITION_PERFORMANCE.md)：实体剖分小瓦片性能问题、实测瓶颈和优化思路记录。

## 运行版本

- Python 运行基线：`python3.11`，当前机器为 Python 3.11.6。
- Ray 集群要求 driver Python 与集群 Python 主版本一致。
- 生产数据库实际为 OpenGauss，使用 PostgreSQL 兼容 DSN；文档中的 PostgreSQL 仅表示协议/客户端兼容层。

## 相关包

- `cube_encoder` 文档：[../../cube_encoder/docs/README.md](../../cube_encoder/docs/README.md)
- `cube_web` 文档：[../../cube_web/docs/README.md](../../cube_web/docs/README.md)
