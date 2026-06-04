# cube_encoder 文档索引

更新时间：2026-06-04

本文档目录只维护 `cube_encoder` 的 SDK、API、格网引擎和发布规则。剖分入库执行链路见
[cube_split/docs/README.md](../../cube_split/docs/README.md)，Web 管理入口、托管剖分 API
和质检报告接口见 [cube_web/docs/README.md](../../cube_web/docs/README.md)。

## 主文档

- [ARCHITECTURE.md](ARCHITECTURE.md)：当前格网编码引擎的职责、分层、API 和 SDK 边界。
- [SDK_RELEASE.md](SDK_RELEASE.md)：SDK 版本、发布检查和兼容性规则。

## 运行版本

- Python 运行基线：`python3.11`，当前机器为 Python 3.11.6。
- 包配置：`requires-python = ">=3.11"`，ruff target 为 `py311`。

## 变更记录

- [../CHANGELOG.md](../CHANGELOG.md)：对外变更记录。

旧版 Python、早期 demo 前端和历史长方案已从当前文档集中清理。如需追溯，使用 Git 历史查看旧文档。
