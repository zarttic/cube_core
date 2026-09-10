# 项目级文档

更新时间：2026-09-10

本目录只放**当前有效**的项目级文档。一次性报告、阶段性设计稿、已完成迁移的记录和交接单
统一放在 [archive/docs/](../archive/docs/README.md)，那里的内容不作为当前契约或运行依据。

## 索引

| 文档 | 内容 |
| --- | --- |
| [ARCHITECTURE.md](ARCHITECTURE.md) | 系统架构：三个包的职责边界、基础设施、剖分/质检/入库数据流 |
| [OPERATIONS.md](OPERATIONS.md) | 部署运行：启动方式、端口与健康检查、运行时配置、后台 worker、常见运维动作 |
| [ACCEPTANCE_AND_TESTING.md](ACCEPTANCE_AND_TESTING.md) | 测试与验收：命令、pytest 标记、需要外部服务的门禁 |
| [当前OpenGauss关系型数据库表说明.md](当前OpenGauss关系型数据库表说明.md) | OpenGauss 表结构、存储与对象组织 |
| [操作员权限接入说明.md](操作员权限接入说明.md) | 操作员权限接入与账号角色 |

## 包级文档

| 文档 | 内容 |
| --- | --- |
| [cube_encoder/docs/README.md](../cube_encoder/docs/README.md) | 格网 SDK、格网矩阵、独立服务与发布规范 |
| [cube_split/docs/README.md](../cube_split/docs/README.md) | 剖分作业、入库、质检与 AOI 回读 |
| [cube_web/docs/README.md](../cube_web/docs/README.md) | Web API、任务编排、认证与前端 |

## 协作规则

仓库协作规则、命令、生产与演示分离要求、基础设施集群信息（OpenGauss、MinIO、Ray）
和安全配置规范见 [AGENTS.md](../AGENTS.md)。

## 文档维护约定

- 契约类文档以代码与测试为准；代码变更后同步更新对应文档。
- 历史性能报告保留原始结论，但必须标注测量时间，不得冒充当前契约。
- 不再适用的文档移到 `archive/docs/` 并在归档索引中记录原路径，不直接删除。
