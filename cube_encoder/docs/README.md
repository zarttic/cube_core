# cube_encoder 文档索引

本文档目录只维护 `cube_encoder` 的 SDK、API、格网引擎和历史设计内容。剖分入库执行链路见 [cube_split/docs/README.md](../../cube_split/docs/README.md)，Web 演示见 [cube_web/docs/README.md](../../cube_web/docs/README.md)。

## 主文档

- [ARCHITECTURE.md](ARCHITECTURE.md)：当前格网编码引擎的职责、分层、API 和 SDK 边界。
- [INGEST_STORAGE_DESIGN.md](INGEST_STORAGE_DESIGN.md)：历史入库、OSS/Iceberg、Ray/Spark 方案的合并版设计归档，用于理解上层系统如何消费 encoder 能力。
- [SDK_RELEASE.md](SDK_RELEASE.md)：SDK 版本、发布检查和兼容性规则。
- [PROJECT_HISTORY.md](PROJECT_HISTORY.md)：阶段交付、测试报告和历史方案的压缩索引。

## 过程记录

- [DEVELOPMENT_LOG.md](DEVELOPMENT_LOG.md)：开发任务流水，保留 append-only 记录。
- [BUG_LOG.md](BUG_LOG.md)：问题排查记录。
- [DOC_WORKFLOW.md](DOC_WORKFLOW.md)：文档维护规则。
- [../CHANGELOG.md](../CHANGELOG.md)：对外变更记录。

## 归档材料

历史长文档已移入 [archive/](archive/)。归档文件不作为当前执行说明，若与主文档冲突，以主文档和当前代码为准。
