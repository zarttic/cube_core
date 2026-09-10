# cube_core

更新时间：2026-09-10

本仓库是 Python monorepo，覆盖格网编码、遥感数据剖分、入库/回读、质检和 Web 管理入口。

**协作规则见 [AGENTS.md](AGENTS.md)**：模块边界、命令、生产/演示分离、基础设施集群信息和安全配置规范。

## 快速入口

| 用途 | 命令 |
| --- | --- |
| 运行全量测试 | `PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest` |
| 启动 Web API | `PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m uvicorn cube_web.app:app --host 0.0.0.0 --port 50039` |
| 前端开发服务 | `cd cube_web/frontend && npm run dev`（端口 50040，把 `/v1`、`/api`、`/health` 代理到 50039） |
| 前端构建 / 单测 | `cd cube_web/frontend && npm run build` / `npm run test:unit` |
| 构建 encoder 包 | `cd cube_encoder && python3.11 -m build` |

## 包结构

| 包 | 职责 | 端口 / 入口 |
| --- | --- | --- |
| `cube_encoder/` | 格网编码 SDK（`grid_core`）与独立 FastAPI 服务：locate/cover、拓扑、时空编码 | `grid_core.app.main:app`，端口 50012 |
| `cube_split/` | 剖分、入库、质检与 AOI 回读作业，支持 Ray 与本地线程/进程后端 | 作业脚本 |
| `cube_web/` | FastAPI 后端 + Vue/Vite 前端：托管剖分批次、质检任务链、SDK facade | 后端 50039 / 前端 50040 |

`cube_encoder` 只提供格网能力，其余包必须通过 `grid_core.sdk.CubeEncoderSDK` 或 Web SDK 后端使用，不重复实现格网逻辑。

运行时数据库是 OpenGauss 的 PostgreSQL 兼容接口。环境变量沿用历史名 `CUBE_WEB_POSTGRES_DSN`，
不要理解为独立的 PostgreSQL 服务。

## 生产格网契约

| 格网 | 层级范围 | 默认层级 | 剖分方式 |
| --- | --- | --- | --- |
| `geohash` | 1–12 | 4 | 逻辑剖分 |
| `mgrs` | 0–5 | 1 | 逻辑剖分 |
| `isea4h` | 1–6 | 6 | 实体剖分 |

Web、剖分作业和编码 SDK 共用这一契约。`s2`、`tile_matrix`、`plane_grid` 已从代码移除，
不得在前端、API 或作业参数中重新暴露。

## 文档

| 位置 | 内容 |
| --- | --- |
| `AGENTS.md` | 仓库协作规则与基础设施信息 |
| [docs/README.md](docs/README.md) | 项目级文档索引 |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | 系统架构与数据流 |
| [docs/OPERATIONS.md](docs/OPERATIONS.md) | 部署运行、运行时配置与运维动作 |
| [docs/ACCEPTANCE_AND_TESTING.md](docs/ACCEPTANCE_AND_TESTING.md) | 测试与验收门禁 |
| [docs/当前OpenGauss关系型数据库表说明.md](docs/当前OpenGauss关系型数据库表说明.md) | 数据库表与存储说明 |
| [docs/操作员权限接入说明.md](docs/操作员权限接入说明.md) | 操作员权限接入 |
| [cube_encoder/docs/README.md](cube_encoder/docs/README.md) | 编码器架构、SDK 与发布规范 |
| [cube_split/docs/README.md](cube_split/docs/README.md) | 剖分、入库、质检作业 |
| [cube_web/docs/README.md](cube_web/docs/README.md) | Web API、任务编排与前端 |
| [archive/docs/README.md](archive/docs/README.md) | 历史文档归档，不作为当前依据 |
