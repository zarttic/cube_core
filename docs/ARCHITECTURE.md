# 系统架构与数据流

更新时间：2026-09-10

## 1. 分层与职责边界

```
cube_web   FastAPI 后端 + Vue/Vite 前端    —— 托管批次、任务编排、质检链、SDK facade
   │
cube_split 剖分 / 入库 / 质检 / AOI 回读作业 —— Ray 或本地线程、进程后端
   │
cube_encoder 格网编码 SDK（grid_core）      —— locate / cover / topology / 时空编码
```

| 包 | 负责 | 不负责 |
| --- | --- | --- |
| `cube_encoder` | 格网定位、覆盖、拓扑、时空编码；可选独立 HTTP 服务（端口 50012） | 剖分、入库、任何业务持久化 |
| `cube_split` | 按数据族执行剖分、COG/瓦片产物生成、入库、产物质检、AOI 回读 | Web 任务编排、用户与权限 |
| `cube_web` | API facade、前端、剖分批次与质检任务持久化、状态编排、发布 | 格网算法（一律走 SDK）、底层 schema 管理 |

`cube_encoder` 是 SDK 提供方，其他包必须通过 `grid_core.sdk.CubeEncoderSDK` 或 Web SDK 后端使用格网能力。

## 2. 基础设施

| 组件 | 用途 | 连接配置 |
| --- | --- | --- |
| OpenGauss | 领域数据持久化（数据集、景、载入批次、剖分运行、输出版本、质检、入库、发布） | `CUBE_WEB_POSTGRES_DSN`（PostgreSQL 兼容协议） |
| MinIO | 源数据、COG、瓦片与实体剖分产物的对象存储 | `CUBE_WEB_MINIO_ENDPOINT` / `ACCESS_KEY` / `SECRET_KEY` / `BUCKET`（默认 `cube`） |
| Ray | 分布式剖分与入库执行 | `CUBE_WEB_RAY_ADDRESS`；Ray Jobs 模式下 `CUBE_WEB_RAY_JOB_ADDRESS` |

集群拓扑与凭据规范见 [AGENTS.md](../AGENTS.md)。

## 3. 核心数据流

```
schema 导入 / 载入批次
        │
        ▼
剖分批次（partition_run）  ── 提交为任务（attempt）
        │                        ├─ Ray Jobs：cube_web.jobs.partition_batch_job
        │                        └─ 本地：PartitionTaskStore 线程池
        ▼
输出版本（output_version）  ── 逻辑剖分写 staging 后 promote；实体剖分写瓦片与索引
        │
        ▼
质检任务（quality run）     ── outbox 事件 → 分配 run → 抢占租约 → 规则执行 → 终态
        │
        ▼
入库（ingest run / scene）  ── 按 claim token 分组并行写入
        │
        ▼
发布（publication）         ── 可撤回
```

- 剖分执行后端由 `CUBE_WEB_PARTITION_EXECUTOR` 决定；`ray_job` 走 Ray Jobs 提交器，否则使用进程内线程池
  （默认并发 `CUBE_WEB_PARTITION_MAX_WORKERS`，默认 4）。
- 质检由 `QualityRuntime` 的三条常驻线程驱动：outbox 派发、租约抢占与规则执行、入库扫描。
- 逻辑剖分（`geohash`、`mgrs`）只写元数据与窗口引用；实体剖分（`isea4h`）切真实瓦片并上传对象存储。

## 4. 状态机

| 对象 | 状态 |
| --- | --- |
| 剖分批次 / attempt | `pending`、`queued`、`running`、`retrying`、`cancel_requested` → `completed`、`failed`、`manual_required`、`cancelled` |
| 质检 run | `pending`、`running` → `pass`、`warn`、`fail`、`error`、`cancelled` |
| 入库 scene | `pending`、`queued`、`running`、`completed`、`failed`、`cancelled` |
| 入库 run | `pending`、`queued`、`running`、`completed`、`partial_failure`、`failed`、`cancelled` |

## 5. 可靠性设计

- **租约与心跳**：质检 run 抢占租约（默认 300 秒）并在批量写错误时续约；租约过期后可被其他 worker 重新抢占。
- **事务边界**：质检把「启动」「执行」「失败终结」放在不同事务里，避免数据库报错后的事务终止掩盖根因。
- **对账**：服务启动时执行 `reconcile_orphaned_tasks()`；读取活跃任务时也会刷新 attempt（Ray Job 已成功但未落终态的会置为 `manual_required`）。
- **失败不覆盖根因**：剖分投影、回调、补偿和 Ray 终止请求失败时只记录日志并给出告警，不改写已确认的根因错误。

## 6. 前端

Vue/Vite 单页应用，源码在 `cube_web/frontend/`，开发端口 50040，代理 `/v1`、`/api`、`/health` 到后端 50039。
主要页面：剖分（`/partition`）、数据管理、质检（`/quality`）、格网编码（`/encoding`，非管理员落点）、系统配置（`/config`）。
页面可见性由主认证系统返回的权限控制，不是后端业务授权的替代。
