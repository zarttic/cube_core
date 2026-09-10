# cube_web 文档

更新时间：2026-09-10

## 1. 定位

`cube_web` 提供 cube 项目的 Web 入口与 FastAPI API facade：托管剖分批次、编排剖分与质检任务、
持久化领域状态，并提供 Vue/Vite 前端。它不实现格网算法、COG 转换或底层入库读取——
格网能力来自 `CubeEncoderSDK`，剖分与质检执行能力来自 `cube_split`。

- 后端：`cube_web/cube_web/`，端口 50039
- 前端：`cube_web/frontend/`，开发端口 50040，代理 `/v1`、`/api`、`/health` 到 50039

## 2. 运行

```bash
# 后端（仓库根目录）
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m uvicorn cube_web.app:app --host 0.0.0.0 --port 50039

# 前端开发
cd cube_web/frontend && npm run dev

# 前端构建与单测
cd cube_web/frontend && npm run build
cd cube_web/frontend && npm run test:unit

# 后端测试
cd cube_web && PYTHONPATH=../cube_encoder:../cube_split:. python3.11 -m pytest tests
```

启动流程：构造质检运行时（三条常驻线程）→ 执行孤儿任务对账 → 启动质检 runtime。
`/health` 支持 `?checks=`，可选 `config`、`postgres`、`ray`、`minio`、`bucket` 与 `all|deep|full`；
失败返回 `status=degraded`。

## 3. 认证与授权

- `CUBE_WEB_AUTH_REQUIRED` 默认 **开启**（接受 `1/true/yes/on`）；仅受控本地测试可关闭。
- 鉴权只作用于 `/v1/` 前缀；`/api/*`、`/health`、`/` 不校验。
- 公开入口：`POST /v1/partition/schemas/import`（载入系统交付入口）。
- 其余 `/v1/*` 需要 Bearer Token；无凭证 401，非管理员角色 403。
- 管理员判定为角色归一化后等于 `ADMIN`（含 `admin`、`administrator`、`管理员` 等别名）。
- 前端非管理员只保留公共编码入口，直接访问剖分页面会跳回编码页或门户首页；
  页面可见性按主认证系统返回的权限控制，不替代后端业务授权。

## 4. 错误响应契约

所有业务错误统一返回：

```json
{
  "error": {
    "code": "dataset_not_found",
    "message": "数据集不存在或已被删除，请刷新数据后重试",
    "request_id": "b7f2c0d4..."
  },
  "detail": null
}
```

- 响应头带 `X-Request-ID`；入站同名头（仅允许 `A-Za-z0-9._:-`，最长 128）会被复用，否则生成随机 ID。
- **5xx 不回显内部细节**：只给固定中文文案与请求 ID；连接串、对象地址、异常堆栈一律不返回。
- 4xx 透出领域错误或校验明细（`detail` 保留结构化校验列表，便于前端定位字段）。
- 服务端会记录未处理异常的堆栈，日志中带请求 ID、方法与路径。

常见稳定错误码：

| 码 | HTTP | 含义 |
| --- | --- | --- |
| `unauthorized` / `forbidden` | 401 / 403 | 未认证或角色不足 |
| `validation_error` | 422 | 请求参数校验失败 |
| `dataset_not_found`、`output_version_not_found`、`quality_run_not_found`、`scene_not_found` | 404 | 资源不存在 |
| `output_version_not_completed`、`quality_trigger_conflict` | 409 | 当前状态不允许该操作 |
| `partition_batch_active`、`partition_batch_archived`、`partition_batch_not_requeueable` | 409 | 批次已运行 / 已归档 / 不可重新排队 |
| `dataset_action_conflict` | 409 | 数据集管理动作冲突 |
| `bad_gateway`、`service_unavailable`、`internal_error` | 502 / 503 / 500 | 上游或服务端异常，可重试 |
| `request_timeout` | — | 前端请求超时（前端语义，提示到任务列表确认） |

## 5. API 一览

### 5.1 SDK facade（`/v1`，需认证）

`POST /v1/grid/locate`、`POST /v1/grid/cover`（可带 `preview_mode=continuous` 返回仅用于显示的
`preview_cells`）、`POST /v1/topology/neighbors|geometry|geometries|parent|children`、
`POST /v1/code/st|parse|st/batch`、`POST /v1/query/st`。

MGRS 连续预览：服务仍在 `cells` 返回真实 MGRS 单元，只在 `preview_cells` 中返回显示用方格，
不改变剖分编码、源影像 CRS 或入库几何。

### 5.2 剖分（`/v1/partition`）

`GET /tasks`、`GET /tasks/{task_id}`、`POST /tasks/{id}/cancel`、`POST /tasks/{id}/terminate`、
`POST /tasks/{id}/retry`、`POST /schemas/import`（公开）。

### 5.3 载入批次与剖分运行（`/v1/partition`）

`GET /load-batches`、`GET /load-batches/{id}`、`POST /load-batches/{id}/archive`、
`GET /load-batches/{id}/scenes`、`POST /carbon/footprints`、`POST /carbon/grid-preview`、
`POST /runs`（提交剖分）、`GET /runs`、`GET /runs/{id}/quality`、`POST /runs/{id}/cancel`、
`POST /runs/{id}/quality`、`POST /runs/{id}/retry-failed`、
`GET /drafts`、`POST /drafts`、`POST /reload-batches`、`POST /drafts/{id}/submitted`。

### 5.4 数据集（`/v1/datasets`）

`GET /datasets`、`GET /datasets/{id}`、`GET|PUT /datasets/{id}/role-restrictions`、`PATCH /datasets/{id}`、
`PATCH /datasets/{id}/assets/{asset_id}`、`POST /datasets/{id}/scenes/{scene_id}/reassign`、
`POST /datasets/{id}/quality-runs`、`POST /datasets/{id}/bands/{band}/ingest-retry`、
`DELETE /datasets/{id}/bands/{band}/grids/{grid_type}`、`POST /datasets/{id}/ingest`、
`POST /datasets/{id}/publish`、`POST /datasets/{id}/publications/{pub}/withdraw`、
`POST /datasets/{id}/archive`、`DELETE /datasets/{id}`。

动态详情子资源 `GET /datasets/{id}/{detail}`，`detail` 取值：
`scenes`、`assets`、`bands`、`outputs`、`grid`、`tiles`、`indexes`、`ingest-records`、`quality`、
`publications`、`provenance`。

### 5.5 质检（`/v1/quality`）

`GET /rules`、`PUT /rules/settings`、`PUT /rules/{code}/enabled`、`GET /records`、
`GET /records/{id}`、`GET /records/{id}/results`、`GET /records/{id}/results/export`（csv/json）、
`GET /records/{id}/export`（xlsx 工作簿，成功/失败双 Sheet）、`GET /records/{id}/errors`、
`GET /records/{id}/errors/export`、`POST /runs`（触发/重跑质检）。

错误导出接口流式返回完整内容，不使用页面的 `page`/`page_size`。

### 5.6 入库与配置

入库：`GET /v1/ingest-runs`、`GET /v1/ingest-runs/collections`、
`POST /v1/ingest-runs/collections/{partition_run_id}/ingest`、`GET /v1/ingest-runs/{id}`、
`POST /v1/ingest-runs/{id}/retry`、`POST /v1/ingest-runs/{id}/cancel`。

配置：`POST /v1/config/get`、`POST /v1/config/update`、`POST /v1/config/reset`。
`cube_web_configs` 只保存用户可编辑的 `partition`、`ingest`、`quality` 默认值，
不保存 DSN、Ray 地址、MinIO 端点或凭据。

### 5.7 认证（`/api`）

`GET /api/config`、`GET /api/auth/login`（重定向）、`GET /api/callback`、`GET /api/verify`、
`GET /api/me`、`/api/auth/me`、`POST /api/logout`、`/api/auth/logout`、`GET /api/auth/verify`。

## 6. 任务与状态机

| 对象 | 状态 |
| --- | --- |
| 剖分批次 / attempt | `pending`、`queued`、`running`、`retrying`、`cancel_requested` → `completed`、`failed`、`manual_required`、`cancelled` |
| 质检 run | `pending`、`running` → `pass`、`warn`、`fail`、`error`、`cancelled` |
| 入库 run / scene | `pending`、`queued`、`running`、`completed`、`partial_failure`、`failed`、`cancelled` |

- 剖分执行默认用进程内线程池（`CUBE_WEB_PARTITION_MAX_WORKERS`，默认 4）；
  `CUBE_WEB_PARTITION_EXECUTOR=ray_job` 时改为 Ray Jobs 提交。
- 取消走 `request_cancel` 标记，终止（`terminate`）会立即发布取消终态并回收未关闭的输出版本。
- 质检 run 由常驻线程抢占租约执行，租约默认 300 秒并按需续约；失败在独立事务中终结为 `error`。

## 7. 前端

路由：`/partition`（需 `data_import:view`）、`/data-management`、`/quality`、`/encoding`（非管理员落点）、
`/config`（需 `system_config:view`）、`/callback`，未匹配路径回到 `/partition`。
门户导航项由 `GET /api/config` 的 `navigation` 下发，运行时可配。

## 8. 本目录其他文档

| 文档 | 内容 |
| --- | --- |
| `ARD_TO_PARTITION_INGEST_QUALITY_WORKFLOW.md` | ARD 数据到剖分、入库与质检的完整链路 |
| `PARTITION_GRID_CONTRACT.md` | 当前格网与剖分方式契约 |
| `SCENE_DOMAIN_OPERATIONS.md` | Dataset/Scene/LoadBatch 关系与 Schema 安装 |
| `LOAD_BATCH_PARTITION_STATUS.md` | 按载入批次获取剖分状态 |
| `KUBERAY_OPERATIONS.md` | Ray Jobs 与 KubeRay RayCluster 运行约束 |
| `QUALITY_RULE_CATALOG.md` | 质检规则集的必选/可选与产品适用范围 |
| `BAND_PRESENTATION_CONTRACT.md` | 波段命名、类型、筛选与展示契约 |
