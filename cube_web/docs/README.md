# cube_web 文档

更新时间：2026-05-26

## 1. 定位

`cube_web` 提供 cube 项目的 Web 入口和 FastAPI API facade。它不实现格网算法，也不持久化入库数据；格网能力来自 `CubeEncoderSDK`，剖分和质检执行能力来自 `cube_split`。

当前前端是 Vue/Vite 构建产物，生产静态资源位于 `cube_web/cube_web/web/`。

## 2. 服务边界

`cube_web` 负责：

- 托管 SPA 页面和静态资源。
- 提供 `/health`。
- 在 `/v1` 下暴露 SDK facade、剖分 demo、异步任务和质检报告接口。
- 将前端请求转换为 `cube_split` 作业参数。
- 扫描 `cube_split/data/ray_output/*/run_*` 下的质检历史。

`cube_web` 不负责：

- 格网 locate/cover/topology 的算法实现。
- COG 转换、剖分核心逻辑、入库或 AOI 读取。
- 数据库和对象存储 schema 管理。

## 3. 运行方式

从仓库根目录运行：

```bash
PYTHONPATH=cube_encoder:cube_web uvicorn cube_web.app:app --host 0.0.0.0 --port 50040
```

开发前端：

```bash
cd cube_web/frontend
npm install
npm run dev
```

构建前端后，产物写入 `cube_web/cube_web/web/`：

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

- `POST /v1/partition/{data_type}/demo`
- `POST /v1/partition/{data_type}/retry`
- `POST /v1/partition/{data_type}/tasks/demo`
- `POST /v1/partition/{data_type}/tasks/retry`
- `GET /v1/partition/tasks/{task_id}`
- `POST /v1/partition/optical/test`

`data_type` 当前支持：

- `optical`
- `carbon`
- `product`
- `radar` 为占位，返回 501。

异步任务保存在进程内 `PartitionTaskStore`，适合演示和联调，不是持久任务队列。

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

`latest` 和 `history` 会扫描 `cube_split/data/ray_output/*/run_*` 下包含 `index_rows.jsonl` 的批次目录。`product` 历史只取父目录名以 `product` 开头的批次；`optical` 历史排除这些产品批次。

## 5. 静态路由

- `/` 返回 `web/index.html`。
- `/partition.html`、`/quality.html`、`/encoding.html` 和 `门户首页.html` 映射到同一个 SPA 入口。
- 其他静态资源从 `cube_web/cube_web/web/` 读取。

## 5.1 认证开关

- 运行时通过环境变量 `CUBE_WEB_AUTH_REQUIRED` 控制是否启用认证。
- 取值为 `1/true/yes/on` 时，前端会跳转统一认证，后端 `/v1/*` 也会校验 Bearer Token。
- 取值为 `false` 或未设置时，适合本地自测，可直接进入剖分、质检、编码页面。

## 6. 测试

从 `cube_web/` 目录运行：

```bash
PYTHONPATH=../cube_encoder:. pytest tests
```

涉及 API 契约变化时，同时检查：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web pytest cube_encoder/tests cube_split/tests
```
