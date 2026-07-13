# cube_encoder 文档索引

更新时间：2026-07-13

本文档目录只维护 `cube_encoder` 的 SDK、API、格网引擎和发布规则。剖分入库执行链路见
[cube_split/docs/README.md](../../cube_split/docs/README.md)，Web 管理入口、托管剖分 API
和质检报告接口见 [cube_web/docs/README.md](../../cube_web/docs/README.md)。

## 主文档

- [ARCHITECTURE.md](ARCHITECTURE.md)：当前格网编码引擎的职责、分层、API 和 SDK 边界。
- [SDK_RELEASE.md](SDK_RELEASE.md)：SDK 版本、发布检查和兼容性规则。

## 格网边界

- `s2`、`mgrs`、`tile_matrix`、`isea4h`：编码器提供 locate、cover、topology 和 ST code 能力。
- `plane_grid`：编码器只提供 ST code 的 prefix、格式校验和解析；源 CRS、像素窗口和布局由 `cube_split` 的逻辑剖分实现。
- `mgrs` 仍是 SDK/API 兼容能力，但不再是 Web 生产剖分页面的可选项。

## 运行版本

- Python 运行基线：`python3.11`，当前机器为 Python 3.11.6。
- 包配置：`requires-python = ">=3.11"`，ruff target 为 `py311`。

## 变更记录

- [../CHANGELOG.md](../CHANGELOG.md)：对外变更记录。

旧版 Python、早期 demo 前端和历史长方案已从当前文档集中清理。如需追溯，使用 Git 历史查看旧文档。
