# cube_web

更新时间：2026-07-17

`cube_web` 承载 FastAPI 后端、独立 Vue/Vite 前端，以及面向前端的
encoder SDK facade、托管剖分任务和质检报告 API。

详细说明见 [docs/README.md](docs/README.md)。

## 职责边界

- `cube_web`：HTTP API、可视化交互、API 请求适配、剖分任务编排和质检报告展示。
- `cube_encoder`：格网 locate、cover、topology 和时空编码行为。
- `cube_split`：剖分、入库、质检实现和 AOI 回读链路。

## 运行

从仓库根目录运行：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m uvicorn cube_web.app:app --host 0.0.0.0 --port 50039
```

质检报告和托管剖分任务使用 OpenGauss 持久化，并通过 PostgreSQL 兼容 DSN/`psycopg` 连接。使用这些链路前需要设置
`CUBE_WEB_POSTGRES_DSN`、`POSTGRES_DSN` 或 `DATABASE_URL`。

认证默认开启，并可由运行时环境变量 `CUBE_WEB_AUTH_REQUIRED` 显式控制。本地自测可设置
`CUBE_WEB_AUTH_REQUIRED=false`，跳过前端登录跳转和后端 `/v1/*` Bearer Token 校验。
启用认证时，载入系统调用的 `POST /v1/partition/schemas/import` 保持公开；其他 `/v1/*` 默认需要 Bearer Token。非管理员前端只展示公共编码入口，并在直接进入剖分页面时返回门户首页。

剖分运行从运行时配置读取 Ray、MinIO 和 OpenGauss 设置。使用分布式后端时设置
`CUBE_WEB_RAY_ADDRESS`、`CUBE_WEB_MINIO_ENDPOINT`、`CUBE_WEB_MINIO_ACCESS_KEY`、
`CUBE_WEB_MINIO_SECRET_KEY` 和 `CUBE_WEB_MINIO_BUCKET`。MinIO 凭据也可以来自节点本地
MinIO 服务环境。

内置演示剖分批次默认不加载。只有演示环境才设置
`CUBE_WEB_LOAD_DEMO_PARTITION_SCHEMAS=1`；生产启动不自动写入剖分批次表。

本地前端开发：

```bash
cd cube_web/frontend
npm install
npm run dev
```

前端开发服务监听 `50040`，并把 `/v1`、`/api` 和 `/health` 代理到后端 `50039`。
构建后的前端资产写入 `cube_web/frontend/dist/`，不再提交到后端包内。

## API 概览

所有业务 API 都在 `/v1` 下：

- `/v1/grid/*`、`/v1/topology/*`、`/v1/code/*`：进程内 `CubeEncoderSDK` facade。
- `/v1/partition/schemas/import`：载入系统交付 Dataset/Scene manifest。
- `/v1/partition/load-batches/*`：按载入批次查询 Dataset 和 Scene。
- `/v1/partition/runs`：按 Dataset 选择 Scene 并提交剖分。
- `/v1/partition/tasks/*`：任务查询、取消和重试。
- `/v1/datasets/*`：Dataset 结果、质量、入库和发布管理。
- `/v1/ingest-runs/*`：Scene 入库状态、取消和失败重试。
- `/v1/quality/*`：规则、全部质检记录、详情、错误和完整导出。

Web 生产剖分格网严格限定为 `geohash`、`mgrs`、`isea4h`。剖分方式不独立选择：
`geohash`、`mgrs` 固定为逻辑剖分，`isea4h` 固定为实体剖分。

## 测试

在本包内运行：

```bash
PYTHONPATH=../cube_encoder:../cube_split:. python3.11 -m pytest tests
```

在仓库根目录运行跨包测试命令，见 [../AGENTS.md](../AGENTS.md)。
