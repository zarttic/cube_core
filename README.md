# cube_core

更新时间：2026-07-13

本仓库是 Python monorepo，覆盖格网编码、遥感数据剖分、入库/回读、质检和 Web 管理入口。

**详细协作指南见 [AGENTS.md](AGENTS.md)**，包含：
- 项目结构与模块边界
- 构建、测试与开发命令
- 代码风格、生产/演示分离规则
- 基础设施集群信息（OpenGauss、MinIO、Ray）
- 安全与配置规范

## 快速入口

| 用途 | 命令 |
|------|------|
| 运行全量测试 | `PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest` |
| 启动 Web API | `PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m uvicorn cube_web.app:app --host 0.0.0.0 --port 50039` |
| 构建 encoder 包 | `cd cube_encoder && python3.11 -m build` |
| 前端开发 | `cd cube_web/frontend && npm run dev` |

## 包结构

- `cube_encoder/` — 格网 locate/cover、拓扑、时空编码 SDK。提供 `grid_core.sdk.CubeEncoderSDK`。
- `cube_split/` — 光学/产品/碳卫星/雷达剖分，Ray/本地执行，OpenGauss/MinIO 入库，质检和 AOI 回读。
- `cube_web/` — FastAPI 后端、Vue/Vite 前端、SDK facade、托管剖分和质检报告 API。

运行时数据库是 OpenGauss 的 PostgreSQL 兼容接口；文档沿用环境变量名
`CUBE_WEB_POSTGRES_DSN`，但不要把它理解成独立 PostgreSQL 服务。

## 文档入口

| 位置 | 内容 |
|------|------|
| `AGENTS.md` | 仓库协作规则、命令、基础设施信息 |
| `docs/` | 根级专题文档（认证集成、生产验收、载入接口等） |
| `cube_encoder/docs/README.md` | 编码器架构与 SDK 发布规范 |
| `cube_split/docs/README.md` | 剖分、入库、质检、AOI 工作流 |
| `cube_web/docs/README.md` | Web API、前端、任务编排 |

当前格网选择：编码器完整拓扑格网为 `s2`、`mgrs`、`tile_matrix`、`isea4h`；Web 生产页面不暴露已废弃的 `mgrs`。`plane_grid` 是源 CRS 保留型逻辑窗口模式，当前不是通用 locate/cover/topology 引擎。

`cube_encoder` 只负责格网能力，其他包必须通过 SDK 使用，不重复实现格网逻辑。
