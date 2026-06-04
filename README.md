# cube_core

本仓库是一个 Python monorepo，覆盖格网编码、遥感数据剖分、入库/回读、
质检和 Web 管理入口。

## 当前运行版本

- Python：`python3.11`，当前机器为 Python 3.11.6。命令示例均显式使用 `python3.11`。
- Node.js：当前机器为 v24.16.0。
- npm：当前机器为 11.13.0。

## 包结构

- `cube_encoder`：格网 locate/cover、拓扑、时空编码 API，以及
  `grid_core.sdk.CubeEncoderSDK` SDK 提供方。
- `cube_split`：光学/产品/碳卫星/雷达剖分，Ray 或本地执行，PostgreSQL/MinIO
  或本地后端入库，质检和 AOI 回读。
- `cube_web`：FastAPI 后端、独立 Vue/Vite 前端、进程内 SDK API facade、
  托管剖分接口和质检报告接口。

`cube_encoder` 只负责格网能力。其他包必须通过
`grid_core.sdk.CubeEncoderSDK` 或 Web SDK facade 使用 encoder 能力，不重复实现格网逻辑。

## 文档入口

- `cube_encoder/docs/README.md`：encoder 架构、SDK/API 边界、发布规则和历史设计索引。
- `cube_split/docs/README.md`：剖分、manifest、入库、质检和回读说明。
- `cube_web/docs/README.md`：Web 主机、路由、前端构建、任务/质检存储和测试说明。
- `AGENTS.md`：仓库协作规则、默认测试命令、生产/演示分离规则和本地基础设施说明。

旧版运行基线、早期 demo 前端、历史大方案和一次性联调报告已经从当前文档集中清理。
如需追溯历史，请使用 Git 历史查看旧文档；当前开发、运行和交付只以本文档入口和代码为准。

## 开发命令

在仓库根目录运行默认跨包测试：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests cube_split/tests
```

Web/API 变更后运行 Web 测试：

```bash
cd cube_web
PYTHONPATH=../cube_encoder:../cube_split:. python3.11 -m pytest tests
```

构建 Python 包：

```bash
cd cube_encoder && python3.11 -m build
cd ../cube_split && python3.11 -m build
cd ../cube_web && python3.11 -m build
```

构建前端：

```bash
cd cube_web/frontend
npm ci
npm run build
```

使用仓库内 SDK 和剖分后端运行 Web API：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m uvicorn cube_web.app:app --host 0.0.0.0 --port 50039
```

启动前端开发服务：

```bash
cd cube_web/frontend
npm run dev
```

前端服务监听 `50040`，并把 `/v1`、`/api` 和 `/health` 代理到后端 `50039`。

## 当前链路快照

1. 通过 Web API 或底层命令接收 ARD schema、manifest 或本地调试目录。
2. `cube_split` 在需要时把栅格资产标准化为 COG。
3. `cube_split` 通过 `CubeEncoderSDK` 生成 `space_code`、`st_code` 和 cell/window 元数据。
4. 剖分产物写入 `cube_split/data/ray_output/.../run_*`，分布式链路使用 Ray 和 MinIO。
5. 元数据和资产写入 PostgreSQL + MinIO；本地调试可使用 SQLite/local 后端。
6. 质检报告由 `cube_split.quality` 生成，并由 `cube_web` 持久化到 PostgreSQL `quality_reports`。
7. AOI、时间和波段回读通过 `cube_split.read` 完成。

生产剖分操作使用 `run` 命名；`demo` 接口仅作为旧客户端兼容别名。演示专用数据、
运行配置示例和展示脚本只放在 `demo/*` 分支。
