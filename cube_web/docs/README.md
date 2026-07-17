# cube_web 文档

更新时间：2026-07-17

当前运行基线：

- Python：`python3.11`，当前机器为 Python 3.11.6。
- Node.js：当前机器为 v24.16.0。
- npm：当前机器为 11.13.0。

旧版运行基线、早期 demo 前端、阶段规划稿和一次性验收报告已经清理出当前文档集。
如需追溯历史，使用 Git 历史查看旧文档；当前 API 和运行说明以本文为准。

## 当前文档

- [ARD_TO_PARTITION_INGEST_QUALITY_WORKFLOW.md](ARD_TO_PARTITION_INGEST_QUALITY_WORKFLOW.md)：
  面向新读者说明 ARD 数据如何进入系统，以及从 schema 导入、剖分执行、自动入库到质检报告落库的完整链路，并附常见 QA。
- [PARTITION_INGEST_LOGIC.md](PARTITION_INGEST_LOGIC.md)：记录当前剖分提交后自动入库、
  optical 预入库/确认入库按钮与现行状态机不一致的事实，以及后续调整建议。
- [PARTITION_GRID_METHOD_AND_HISTORY.md](PARTITION_GRID_METHOD_AND_HISTORY.md)：当前格网/剖分方式矩阵、多槽位状态和后续重构边界。
- [PARTITION_TEST_CLEANUP_GUIDE.md](PARTITION_TEST_CLEANUP_GUIDE.md)：测试环境清理和批次状态重置指南，执行前必须确认目标 OpenGauss schema 和 MinIO bucket。
- [M6_SCENE_DOMAIN_OPERATIONS.md](M6_SCENE_DOMAIN_OPERATIONS.md)：M6 Dataset/Scene/LoadBatch
  关系、additive 迁移、运行时切换、回滚和 API 交接说明。
- [M6_MOCK_ACCEPTANCE.md](M6_MOCK_ACCEPTANCE.md)：基于真实 MinIO source 元数据构造的
  四类数据与全状态 Mock 集成验收。
- [QUALITY_RULE_CATALOG.md](QUALITY_RULE_CATALOG.md)：当前质检规则集的必选/可选属性和产品适用范围。
- [BAND_PRESENTATION_CONTRACT.md](BAND_PRESENTATION_CONTRACT.md)：剖分数据单元的波段命名、类型、筛选和展示契约。

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
- 通过 OpenGauss `PartitionJobStore` 管理剖分批次、资产、attempt 和质量状态。
- 通过 OpenGauss `quality_reports` 管理质检报告、latest/history 和导出内容。

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

M6 `m6-read`/`m6-primary` 接口：

- `GET /v1/partition/load-batches`
- `GET /v1/partition/load-batches/{load_batch_id}/scenes`
- `POST /v1/partition/runs`（仅 `m6-primary`）
- `GET /v1/datasets`
- `GET /v1/ingest-runs`

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
- `POST /v1/partition/batches/{batch_id}/archive`
- `POST /v1/partition/assets/retry`

`data_type` 当前支持：

- `optical`
- `carbon`
- `product`
- `radar`

短期异步执行状态保存在进程内 `PartitionTaskStore`。批次、资产、attempt、质量摘要
和重试/取消状态由 `PartitionJobStore` 持久化到 OpenGauss。

内置演示批次默认不会写入生产库。只在演示环境设置
`CUBE_WEB_LOAD_DEMO_PARTITION_SCHEMAS=1` 时，启动时才会自动装载光学、产品、
雷达和碳卫星示例批次。

### 4.3 质检接口

- `GET /v1/quality/records`
- `GET /v1/quality/records/{quality_run_id}`
- `GET /v1/quality/records/{quality_run_id}/results`
- `GET /v1/quality/records/{quality_run_id}/errors`
- `GET /v1/quality/records/{quality_run_id}/errors/export`
- `GET /v1/quality/rules`
- `POST /v1/quality/runs`

错误导出接口流式返回完整 CSV 或 JSON，不使用页面的 `page`/`page_size`。
规则必选/可选属性和产品适用范围见 `QUALITY_RULE_CATALOG.md`。

## 5. 前端服务

- 开发环境使用 `cube_web/frontend` 的 Vite 服务，端口为 `50040`。
- 后端 FastAPI 服务只提供 `/health` 和 `/v1/*` 等 API，端口为 `50039`。
- 构建产物保留在 `cube_web/frontend/dist/`，由前端部署链路发布，不提交到后端包内。

## 5.1 认证开关

- 运行时通过环境变量 `CUBE_WEB_AUTH_REQUIRED` 控制是否启用认证。
- 取值为 `1/true/yes/on` 时，前端会跳转统一认证，后端 `/v1/*` 也会校验 Bearer Token。
- 取值为 `false` 或未设置时，适合本地自测，可直接进入剖分、质检、编码页面。
- 开启认证时，`POST /v1/partition/schemas/import` 是载入系统公开交付入口；其余 `/v1/*` 默认要求 Bearer Token。
- 非管理员前端只显示公共编码导航；剖分页直接访问会跳回门户首页。前端可见性不替代后端授权。

## 5.2 当前剖分矩阵

| 格网 | 逻辑剖分 | 实体剖分 | 说明 |
| --- | --- | --- | --- |
| `geohash` | 支持 | 不支持 | 当前 Web/strict runner 合同 |
| `mgrs`（标准 MGRS） | 支持 | 不支持 | 当前 Web/strict runner 合同 |
| `isea4h`（六边形格网） | 支持 | 支持 | 当前实体剖分格网 |

本表记录当前分支实际的 Web `GridType` 与 `ray_partition_core` 合同。SDK 中存在的
`s2`、`tile_matrix` 和 `plane_grid` 尚未接入这条生产 strict runner 链路，不能只在前端
暴露后直接提交。

## 6. 测试

从 `cube_web/` 目录运行：

```bash
PYTHONPATH=../cube_encoder:../cube_split:. python3.11 -m pytest tests
```

涉及 API 契约变化时，同时检查：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests cube_split/tests
```
