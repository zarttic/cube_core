# cube_web

`cube_web` 承载 FastAPI Web 主机、Vue 构建后的静态 UI，以及面向前端的
encoder SDK facade、托管剖分任务和质检报告 API。

详细说明见 [docs/README.md](docs/README.md)。

## 职责边界

- `cube_web`：HTTP 托管、静态资源、可视化交互、API 请求适配、剖分任务编排和质检报告展示。
- `cube_encoder`：格网 locate、cover、topology 和时空编码行为。
- `cube_split`：剖分、入库、质检实现和 AOI 回读链路。

## 运行

从仓库根目录运行：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.8 -m uvicorn cube_web.app:app --host 0.0.0.0 --port 50040
```

质检报告和托管剖分任务使用 PostgreSQL 持久化。使用这些链路前需要设置
`CUBE_WEB_POSTGRES_DSN`、`POSTGRES_DSN` 或 `DATABASE_URL`。

认证开关由运行时环境变量 `CUBE_WEB_AUTH_REQUIRED` 控制。本地自测可设置
`CUBE_WEB_AUTH_REQUIRED=false`，跳过前端登录跳转和后端 `/v1/*` Bearer Token 校验。

剖分运行从运行时配置读取 Ray、MinIO 和 PostgreSQL 设置。使用分布式后端时设置
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

构建后的前端资产从 `cube_web/cube_web/web/` 提供服务。

## API 概览

所有业务 API 都在 `/v1` 下：

- `/v1/grid/*`、`/v1/topology/*`、`/v1/code/*`：进程内 `CubeEncoderSDK` facade。
- `/v1/partition/{data_type}/run`：同步剖分运行，支持 `optical`、`carbon`、`radar`、`product`。
- `/v1/partition/{data_type}/demo`：旧演示客户端兼容别名，不作为新生产调用入口。
- `/v1/partition/{data_type}/retry`：使用上一轮请求 payload 重试。
- `/v1/partition/{data_type}/tasks/run` 和 `/tasks/retry`：异步剖分任务提交。
- `/v1/partition/{data_type}/tasks/demo`：异步兼容别名。
- `/v1/partition/batches/*`：托管批次、资产、attempt、重试和取消接口。
- `/v1/quality/{optical|product|carbon}/run`、`/latest`、`/report`、`/report/pdf`、
  `/report/txt`、`/history`：质检报告链路。

## 测试

在本包内运行：

```bash
PYTHONPATH=../cube_encoder:../cube_split:. python3.8 -m pytest tests
```

在仓库根目录运行跨包测试命令，见 [../AGENTS.md](../AGENTS.md)。
