# 部署运行与运维

更新时间：2026-09-10

## 1. 启动

| 服务 | 命令 | 端口 |
| --- | --- | --- |
| Web API | `PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m uvicorn cube_web.app:app --host 0.0.0.0 --port 50039` | 50039 |
| 前端开发服务 | `cd cube_web/frontend && npm run dev` | 50040 |
| 编码器服务（可选） | `uvicorn grid_core.app.main:app --port 50012` | 50012 |

前端代理 `/v1`、`/api`、`/health` 到后端；生产部署使用 `npm run build` 产物，不提交 `dist/`。

服务启动时会：

1. 构造质检运行时（三条常驻线程：outbox 派发、租约抢占与规则执行、入库扫描）；
2. 执行一次孤儿任务对账 `reconcile_orphaned_tasks()`；
3. 启动质检 runtime；关闭时停止。

## 2. 健康检查

```bash
curl 'http://127.0.0.1:50039/health'
curl 'http://127.0.0.1:50039/health?checks=all'     # config + postgres + ray + minio + bucket
```

`/health` 默认只查 `config`；支持 `?checks=` 或 `?check=` 传入 `config`、`postgres`（别名 `db`）、`ray`、
`minio`、`bucket`，以及 `all`、`deep`、`full`。任一项失败返回 `status=degraded` 并列出 `failed_checks`。
响应还包含 `checks.partition_queue`（队列深度与最大并发）。

## 3. 运行时配置

解析顺序：进程环境变量 → `CUBE_WEB_ENV_FILE` 指定的文件 → 当前目录 `.cube_web.env` →
`~/.cube_web.env` → 仓库根 `.cube_web.env` → 代码默认值。`.cube_web.env` 不入库。

| 变量 | 默认 | 作用 |
| --- | --- | --- |
| `CUBE_WEB_POSTGRES_DSN` | 空 | OpenGauss（PostgreSQL 兼容）DSN |
| `CUBE_WEB_RAY_ADDRESS` | 空 | Ray 集群地址 |
| `CUBE_WEB_MINIO_ENDPOINT` / `_ACCESS_KEY` / `_SECRET_KEY` / `_BUCKET` | 空 / 空 / 空 / `cube` | 对象存储连接信息 |
| `CUBE_WEB_ENV_FILE` | 无 | 指定 env 文件（由运行时代码解析，不会自动导出到 shell） |
| `CUBE_WEB_AUTH_REQUIRED` | `True` | 鉴权总开关；关闭仅用于受控本地测试 |
| `CUBE_WEB_AUTH_MAIN_SYSTEM_URL` / `_CLIENT_ID` / `_CLIENT_SECRET` / `_JWT_SECRET_KEY` / `_JWT_ALGORITHM` | 空 / `system_ard` / 空 / 空 / `HS256` | 统一认证对接 |
| `CUBE_WEB_PORTAL_HOME_URL` / `_PARTITION_SERVICE_URL` / `_DISPATCH_URL` / `_DATA_INGEST_URL` / `_ADMIN_URL` | 空 | 门户导航项，属运行时配置 |
| `CUBE_WEB_PARTITION_MAX_WORKERS` | 4 | 剖分线程池并发 |
| `CUBE_WEB_QUALITY_MAX_WORKERS` | 4 | 质检执行并发 |
| `CUBE_WEB_INGEST_MAX_WORKERS` | 4 | 入库扫描并发 |
| `CUBE_WEB_PG_POOL_SIZE` / `CUBE_WEB_PG_POOL_ACQUIRE_TIMEOUT_SECONDS` | 8 / 30 | 连接池大小与获取超时 |
| `CUBE_WEB_PARTITION_EXECUTOR` | 本地 | 设为 `ray_job` 走 Ray Jobs |
| `CUBE_WEB_RAY_JOB_ADDRESS` / `_TIMEOUT_SECONDS` / `_WORKER_RESOURCE` | 空 / 见代码 / `cube_partition_worker` | Ray Jobs 提交参数 |

配置页面可展示 OpenGauss DSN、Ray 与 MinIO 的运行时信息，但**不写回** `cube_web_configs` 表；
该表只保存用户可编辑的 `partition`、`ingest`、`quality` 业务默认值。

## 4. 后台机制

| 组件 | 说明 |
| --- | --- |
| 质检派发线程 | 消费领域 outbox，为输出版本分配质检 run |
| 质检执行线程 | 抢占租约（默认 300 秒）执行规则；批量写错误时续约 |
| 入库扫描线程 | 按 claim token 分组并行处理排队场景 |
| 剖分执行 | Ray Jobs 或进程内线程池；Ray 模式下 driver 为 `cube_web.jobs.partition_batch_job` |

## 5. 常见运维动作

| 动作 | 入口 |
| --- | --- |
| 查看剖分任务与队列 | `GET /v1/partition/tasks`；`/health` 的 `checks.partition_queue` |
| 取消 / 终止剖分 | `POST /v1/partition/tasks/{task_id}/cancel`、`/terminate`（管理员） |
| 终止整批剖分 | `POST /v1/partition/runs/{partition_run_id}/cancel` |
| 重剖失败数据 | `POST /v1/partition/runs/{partition_run_id}/retry-failed` |
| 触发 / 重跑质检 | `POST /v1/quality/runs`、`POST /v1/partition/runs/{id}/quality` |
| 导出质检结果 | `GET /v1/quality/records/{id}/export?format=xlsx`、`/errors/export?format=csv|json` |
| 数据集级联删除 / 归档 | `DELETE /v1/datasets/{id}`、`POST /v1/datasets/{id}/archive` |

## 6. 排障要点

- 质检长时间停在 `running`：检查质检执行线程日志；租约过期后会被重新抢占，任务终态由终结事务写入。
- 剖分任务停在活动态但 Ray Job 已结束：由读取路径或启动对账置为 `manual_required`（`ray_job_incomplete`）。
- Ray worker 读不到源对象：确认源对象已同步到 MinIO，且 worker 本地缓存目录（默认 `/tmp/cube_split_source_cache`）可用。
- 接口返回 5xx：响应体 `error.request_id` 与 `X-Request-ID` 对应服务端日志，便于定位；5xx 不回显内部细节。
