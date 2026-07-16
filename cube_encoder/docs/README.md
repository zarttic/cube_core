# cube_encoder 文档索引

更新时间：2026-07-14

本文档目录只维护 `cube_encoder` 的 SDK、API、格网引擎和发布规则。剖分入库执行链路见
[cube_split/docs/README.md](../../cube_split/docs/README.md)，Web 管理入口、托管剖分 API
和质检报告接口见 [cube_web/docs/README.md](../../cube_web/docs/README.md)。

## 主文档

- [ARCHITECTURE.md](ARCHITECTURE.md)：当前格网编码引擎的职责、分层、API 和 SDK 边界。
- [SDK_RELEASE.md](SDK_RELEASE.md)：SDK 版本、发布检查和兼容性规则。

## 格网边界

- 生产格网严格限定为三类：`geohash`、`mgrs`、`isea4h`；编码器为三者提供 locate、cover、topology 和 ST code 能力。
- 派生剖分方式固定：`geohash`→logical、`mgrs`→logical、`isea4h`→entity；不由调用方选择。
- `isea4h` 为纯 Python 实现，对齐 DGGRID v8.44；运行时不依赖 H3 或 DGGRID。
- `isea4h` 的 `space_code` 为未补零十进制 DGGRID SEQNUM，`cell_count(r) = 10 * 4**r + 2`；请求使用 `requested_grid_level`，返回 cell 保留实际 `grid_level`。

Current production grid contract: `geohash` and `mgrs` use logical partitioning; `isea4h` uses entity partitioning. Native levels are Geohash `1..12`, MGRS `0..5`, and ISEA4H `0..15`.

## 运行版本

- Python 运行基线：`python3.11`，当前机器为 Python 3.11.6。
- 包配置：`requires-python = ">=3.11"`，ruff target 为 `py311`。

## 变更记录

- [../CHANGELOG.md](../CHANGELOG.md)：对外变更记录。

旧版 Python、早期 demo 前端和历史长方案已从当前文档集中清理。如需追溯，使用 Git 历史查看旧文档。
