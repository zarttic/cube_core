# 原根目录项目文档（已归档）

归档时间：2026-09-11

原始路径：`docs/`

本目录保存原根目录 `docs/` 的项目级文档和质检目录基准。它们只用于历史追溯、测试证据和问题上下文，
不作为当前契约、运行或运维依据。归档总索引见 [archive/docs/README.md](../README.md)，当前入口见
[仓库 README](../../../README.md) 及各包的当前文档。

## 文件索引

| 文档 | 内容 |
| --- | --- |
| [ARCHITECTURE.md](ARCHITECTURE.md) | 系统架构：三个包的职责边界、基础设施、剖分/质检/入库数据流 |
| [OPERATIONS.md](OPERATIONS.md) | 部署运行、端口与健康检查、运行时配置、后台 worker、常见运维动作 |
| [ACCEPTANCE_AND_TESTING.md](ACCEPTANCE_AND_TESTING.md) | 测试与验收命令、pytest 标记和外部服务门禁 |
| [当前OpenGauss关系型数据库表说明.md](当前OpenGauss关系型数据库表说明.md) | OpenGauss 表结构、存储与对象组织 |
| [操作员权限接入说明.md](操作员权限接入说明.md) | 操作员权限接入与账号角色 |
| [OPEN_ISSUES.md](OPEN_ISSUES.md) | 待处理问题历史记录 |
| [quality_retry_failure_matrix.json](quality_retry_failure_matrix.json) | 质检失败场景机器可读基准；由 `test_quality_retry_failure_catalog.py` 校验 |

当前包级文档：

- [cube_encoder/docs/README.md](../../../cube_encoder/docs/README.md)
- [cube_split/docs/README.md](../../../cube_split/docs/README.md)
- [cube_web/docs/README.md](../../../cube_web/docs/README.md)
