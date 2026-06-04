# cube_web 文档

更新时间：2026-06-02

历史设计稿：

- [数据驱动剖分任务编排开发规划](archive/partition-task-orchestration-plan.md)：2026-05-30
  阶段设计稿，已部分落地并归档；当前 API 和运行说明以本文为准。

## 1. 定位

`cube_web` 提供 cube 项目的 Web 入口和 FastAPI API facade。它不实现格网算法、
COG 转换或底层入库读取；格网能力来自 `CubeEncoderSDK`，剖分和质检执行能力来自
`cube_split`。Web 侧负责持久化剖分任务元数据、批次状态和质检报告索引。

当前前端是独立 Vue/Vite 工程，源码位于 `cube_web/frontend/`。后端只提供 API，
不再提交或托管 `cube_web/cube_web/web/` 构建产物。

## 2. 服务边界

`cube_web` 负责：

- 提供后端 API 和前端开发代理目标。
- 提供 `/health`。
- 在 `/v1` 下暴露 SDK facade、剖分执行、异步任务和质检报告接口。
- 将前端请求转换为 `cube_split` 作业参数。
- 通过 PostgreSQL `PartitionJobStore` 管理剖分批次、资产、attempt 和质量状态。
- 通过 PostgreSQL `quality_reports` 管理质检报告、latest/history 和导出内容。

`cube_web` 不负责：

- 格网 locate/cover/topology 的算法实现。
- COG 转换、剖分核心逻辑、入库或 AOI 读取。
- 底层数据库和对象存储 schema 管理。

## 3. 运行方式

从仓库根目录运行：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m uvicorn cube_web.app:app --host 0.0.0.0 --port 50039
```

开发前端：

```bash
cd cube_web/frontend
npm install
npm run dev
```

前端开发服务监听 `50040`，并把 `/v1`、`/api` 和 `/health` 代理到后端 `50039`。
构建前端后，产物写入 `cube_web/frontend/dist/`：

```bash
cd cube_web/frontend
npm run build
```

## 4. API

所有业务 API 挂在 `/v1`。

### 4.1 SDK facade

- `POST /v1/grid/locate`
- `POST /v1/grid/cover`
- `POST /v1/topology/neighbors`
- `POST /v1/topology/geometry`
- `POST /v1/topology/geometries`
- `POST /v1/topology/parent`
- `POST /v1/topology/children`
- `POST /v1/code/st`
- `POST /v1/code/st/batch`
- `POST /v1/code/parse`

这些接口直接调用进程内 `CubeEncoderSDK`，不需要独立启动 `cube_encoder` HTTP 服务。

### 4.2 剖分接口

- `POST /v1/partition/{data_type}/run`
- `POST /v1/partition/{data_type}/demo`（兼容旧演示客户端）
- `POST /v1/partition/{data_type}/retry`
- `POST /v1/partition/{data_type}/tasks/run`
- `POST /v1/partition/{data_type}/tasks/demo`（兼容旧演示客户端）
- `POST /v1/partition/{data_type}/tasks/retry`
- `POST /v1/partition/{data_type}/tasks/test`
- `GET /v1/partition/tasks/{task_id}`
- `POST /v1/partition/tasks/{task_id}/cancel`
- `POST /v1/partition/{data_type}/test`
- `POST /v1/partition/schemas/import`
- `GET /v1/partition/batches`
- `GET /v1/partition/batches/{batch_id}`
- `GET /v1/partition/batches/{batch_id}/assets`
- `GET /v1/partition/batches/{batch_id}/attempts`
- `POST /v1/partition/batches/{batch_id}/run`
- `POST /v1/partition/batches/{batch_id}/retry`
- `POST /v1/partition/batches/{batch_id}/cancel`
- `POST /v1/partition/assets/retry`

`data_type` 当前支持：

- `optical`
- `carbon`
- `product`
- `radar`

短期异步执行状态保存在进程内 `PartitionTaskStore`。批次、资产、attempt、质量摘要
和重试/取消状态由 `PartitionJobStore` 持久化到 PostgreSQL。

内置演示批次默认不会写入生产库。只在演示环境设置
`CUBE_WEB_LOAD_DEMO_PARTITION_SCHEMAS=1` 时，启动时才会自动装载光学、产品、
雷达和碳卫星示例批次。

### 4.3 质检接口

- `POST /v1/quality/optical/run`
- `POST /v1/quality/optical/latest`
- `POST /v1/quality/optical/report`
- `POST /v1/quality/optical/report/pdf`
- `POST /v1/quality/optical/report/txt`
- `POST /v1/quality/optical/history`
- `POST /v1/quality/carbon/run`
- `POST /v1/quality/carbon/latest`
- `POST /v1/quality/carbon/report`
- `POST /v1/quality/carbon/report/pdf`
- `POST /v1/quality/carbon/report/txt`
- `POST /v1/quality/carbon/history`
- `POST /v1/quality/product/run`
- `POST /v1/quality/product/latest`
- `POST /v1/quality/product/report`
- `POST /v1/quality/product/report/pdf`
- `POST /v1/quality/product/report/txt`
- `POST /v1/quality/product/history`

`run` 会先解析允许范围内的 `run_dir`，调用 `cube_split.quality` 生成报告，然后写入
PostgreSQL `quality_reports`。`latest`、`history`、`report`、`pdf` 和 `txt` 都从
`quality_reports` 读取，不再通过扫描 `cube_split/data/ray_output/*/run_*` 组织历史列表。

## 5. 前端服务

- 开发环境使用 `cube_web/frontend` 的 Vite 服务，端口为 `50040`。
- 后端 FastAPI 服务只提供 `/health` 和 `/v1/*` 等 API，端口为 `50039`。
- 构建产物保留在 `cube_web/frontend/dist/`，由前端部署链路发布，不提交到后端包内。

## 5.1 认证开关

- 运行时通过环境变量 `CUBE_WEB_AUTH_REQUIRED` 控制是否启用认证。
- 取值为 `1/true/yes/on` 时，前端会跳转统一认证，后端 `/v1/*` 也会校验 Bearer Token。
- 取值为 `false` 或未设置时，适合本地自测，可直接进入剖分、质检、编码页面。

## 6. 测试

从 `cube_web/` 目录运行：

```bash
PYTHONPATH=../cube_encoder:../cube_split:. pytest tests
```

涉及 API 契约变化时，同时检查：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web pytest cube_encoder/tests cube_split/tests
```
