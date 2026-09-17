# T1 ARD数据载入 技术手册

> 一句话定位：本子系统负责把外部业务系统/离线 Excel 清单中的多源遥感数据接入到 ARD 平台，
> 完成清单核验、幂等去重、源文件下载、异构格式解析、标准化元数据提取、COG 归档、
> 批次调度、结果回环反馈，并把批次元数据推送给剖分数据服务。
> 代码落点：**C = `/home/lyajun/projects/cube_docs_workspace/snapshots/poufennode04_my_demo`**
> （= `10.3.100.182:/home/guanwang/my_demo` 只读快照）。对应官方大纲：表 19~表 24。
> 部署版本：**未能确认 commit**。该快照目录内没有 `.git`，`git rev-parse HEAD` 返回
> `fatal: not a git repository`，也无法从文件内容中取到版本号；`backend/app/main.py:319`
> 的应用版本号固定写为 `1.0.0`、`core/config.py:16` 的 `VERSION` 写为 `2.0.0`，二者不一致
> 且都不能当作代码 commit。文档基于代码版本：快照文件系统时间 `2026-09-09/2026-09-10`。

---

## 1. 子系统定位与边界

### 1.1 负责什么（能力清单）

| # | 能力 | 实现状态 | 主要代码位置 |
| --- | --- | --- | --- |
| 1 | 网络清单接收（外部系统主动下发 JSON） | 已实现 | `backend/app/routers/orders.py:440`、`services/order_watcher.py:2151` |
| 2 | 离线清单上传（JSON/XML/Excel） | 已实现 | `backend/app/routers/orders.py:453`、`services/order_watcher.py:2212` |
| 3 | 本地监听目录扫描（`.json`/`.xlsx`/`.xls` → 处理成功后改名为 `*.done`） | 已实现 | `services/order_watcher.py:1176`、`services/order_watcher.py:1199` |
| 4 | 业务元数据库定时轮询（`ard_dataset_metadata` / `ard_image_metadata`） | 已实现 | `services/order_watcher.py:427`、`services/order_watcher.py:3223` |
| 5 | 数据集级幂等去重（`source_dataset_id` + `source_version`、`source_fingerprint` 双唯一索引） | 已实现 | `services/order_watcher.py:1493`、`main.py:104`~`main.py:120` |
| 6 | 优先级队列 + 数据库持久化调度（跨重启可恢复） | 已实现 | `services/order_watcher.py:301`、`services/order_watcher.py:351`、`routers/ard.py:1145` |
| 7 | 异构格式解析（GeoTIFF/ENVI、NetCDF/HDF5、Shapefile、ZIP） | 已实现 | `services/metadata_extractor.py:439`、`:461`、`:510`、`:638`、`:910` |
| 8 | 标准化元数据构建（16 条内置规则） | 已实现 | `services/metadata_extractor.py:63`、`:2583` |
| 9 | 字典映射（`mapping_rules` 表驱动的字段/单位转换） | 已实现 | `services/metadata_extractor.py:2746`、`routers/ard.py:462`~`:627` |
| 10 | COG 归档（GeoTIFF→COG；地理定位 NetCDF→多波段 COG） | 已实现 | `services/COG.py:121`（`tif_to_cog`）、`services/COG.py:57`（`netcdf_to_cog`） |
| 11 | MinIO 归档与批次目录命名 | 已实现 | `services/metadata_extractor.py:944`~`:1252` |
| 12 | 回环反馈（把结果状态写回业务数据集表 4 个 `feedback_*` 字段） | 已实现 | `services/order_watcher.py:2879`、`main.py:127`~`main.py:187` |
| 13 | Cube 剖分 schema 导入推送（`POST /v1/partition/schemas/import`） | 已实现 | `services/metadata_extractor.py:1692`、`:1829` |
| 14 | Cube 载入批次删除/归档（删除 ARD 批次时级联清理剖分域产物） | 已实现 | `services/cube_sync.py:120`（`delete_cube_load_batches`）、`:88`（`archive_cube_load_batches`）、`routers/ard.py:660` |
| 15 | 批次/资产删除（单资产、批量） | 部分实现（见第 9 节 F-1） | `routers/ard.py:660`、`routers/ard.py:808` |
| 16 | WebSocket 实时进度广播 | 部分实现（见第 9 节 F-3） | `routers/orders.py:519`、`services/order_watcher.py:3468` |
| 17 | API 配额熔断（Redis 计数） | 未实现（中间件短路，见第 9 节 F-2） | `main.py:352`~`main.py:386`、`services/quota_guard.py:8` |

### 1.2 明确不负责什么

- **不做格网剖分**。本子系统只产出"载入批次 + Scene/Band 清单"，剖分由 ②分析就绪数据剖分
  （`cube_split`/`cube_web`）执行。见 `services/metadata_extractor.py:1829`
  `_build_partition_import_payload`，它只构造导入报文并 POST。
- **不做格网编码**。全仓库无 encoder 依赖；`geohash`/`mgrs`/`isea4h` 不在本子系统出现。
- **不写外部业务库**。`services/order_watcher.py:2879` 注释明确 "将批次结果写回监听数据集表，
  不再调用外部 HTTP 回调接口"；唯一写回对象是 `ard_dataset_metadata.feedback_*` 四列。
- **不管理用户/团队/配额后台**。虽然 `User`、`Team`、`PointQuota` 等模型与本子系统同库，
  路由 `/api/admin/*` 属于 ⑤后台管理子系统。
- **`services/file_watcher.py` 不是文件监听器**。该文件 485 行全部是瓦片 OTel Span 指标
  统计（`FileMonitorService`，`:11`），与 ARD 清单监听无关——真正的目录扫描在
  `services/order_watcher.py:1176`。文件名具有误导性，接手时务必注意。

### 1.3 上游 / 下游

| 方向 | 对方 | 交互方式 | 代码位置 |
| --- | --- | --- | --- |
| 上游 | 外部业务服务系统 | `POST /api/orders/receive-manifest`（JSON 下发） | `routers/orders.py:440` |
| 上游 | 外部业务服务系统 | 共享目录 HTTP 拉取 `{REMOTE_SYSTEM_URL}/api/data/request` | `services/order_watcher.py:2977` |
| 上游 | 业务元数据库（同 OpenGauss 实例，LATIN1 连接） | SQLAlchemy ORM 读 `ard_dataset_metadata`/`ard_image_metadata` | `db/database.py:19`、`services/order_watcher.py:427` |
| 上游 | 人工 | 上传 Excel/JSON 离线清单 | `routers/orders.py:453` |
| 上游 | NFS 本地数据根 | `/nfs/public_data1/raw_data/ARD数据`、`/nfs/public_data1/shared_delivery_ard` | `core/config.py:29`、`:32` |
| 下游 | ②分析就绪数据剖分（`cube_web` @ `10.3.100.179:50039`） | `POST /v1/partition/schemas/import` | `services/metadata_extractor.py:28`、`:1692` |
| 下游 | ②分析就绪数据剖分 | `GET /v1/partition/load-batches/{id}/scenes`（查询剖分状态） | `services/cube_sync.py:22`、`routers/ard.py:278` |
| 下游 | ②分析就绪数据剖分 | `POST /v1/partition/load-batches/{id}/delete`、`/archive`、`POST /v1/datasets/{id}/archive` | `services/cube_sync.py:10`、`:14`、`:18` |
| 下游 | MinIO 分布式集群 | 用户桶 `user-{user_id}`（`services/minio_service.py:23`）、团队桶 `team-{team_id}`（`:168`、`:189`、`:279` 等多处） | `services/minio_service.py:23`、`:168` |
| 下游 | 前端 `ARD.vue` | REST `/api/ard/*` + WebSocket `/api/ws/orders` | `frontend/src/views/ARD.vue:1607` |
| 下游 | ⑤后台管理 | 共享 `users`/`teams`/`audit_logs` 表；本子系统调用 `quota_guard` | `services/quota_guard.py`、`services/quota_checker.py` |

### 1.4 与其它 5 个子系统的边界（一句话各自）

| 子系统 | 与 T1 的边界 |
| --- | --- |
| ② 分析就绪数据剖分 | T1 只是 Cube 的**前端调用方**：推送 schema、查询剖分状态、删除/归档批次；剖分本身不在此实现。 |
| ③ 剖分数据服务 | T1 不直接访问 Cube 数据库，全部通过 `cube_web` 的 `/v1/...` HTTP 接口（`services/cube_sync.py:10`~`:26`）。 |
| ④ 资源调度 | T1 只消费 `quota_guard` 的 **CPU 令牌桶**（`services/metadata_extractor.py:789`、`:1504`）与 MinIO 桶限额，不做全局资源分配策略；`quota_guard.enforce_storage_limit` 无调用点（见 7.1）。 |
| ⑤ 后台管理 | 二者同库同进程；`/api/admin/*`（`routers/admin.py`，2697 行）属 ⑤，`/api/ard/*` 属 T1。 |
| ⑥ 全球离散格网模型与编码 | 无代码依赖关系；T1 不引入 `cube_encoder`，不出现 `grid_type`/`grid_level` 概念。 |

---

## 2. 运行实例与部署

### 2.1 部署表

| 项 | 值 | 证据 |
| --- | --- | --- |
| 机器 | `10.3.100.182`（poufennode04） | `start-tmux-services.sh`（C 仓根）未写 IP；`core/config.py:96` `ME_system_url = "http://10.3.100.182:5177"`；`main.py:333` CORS 白名单含 `http://10.3.100.182:5177` |
| 仓内路径 | `/home/guanwang/my_demo` | `start-tmux-services.sh:9`、`:17`、`:24` |
| 后端进程工作目录 | `/home/guanwang/my_demo/backend/app` | `start-tmux-services.sh:9`（`tmux new-session -c`） |
| 后端启动命令 | `/home/guanwang/miniconda3/envs/ard/bin/python -m uvicorn main:app --host 0.0.0.0 --port 6000` | `start-tmux-services.sh:10` |
| 后端端口 | `6000` | 同上；`main.py:407` `port=6000` |
| 前端启动命令 | `npm run dev`（`frontend/` 目录） | `start-tmux-services.sh:17` |
| 前端端口 | `5177`，`/api` 代理到 `http://127.0.0.1:6000` | `frontend/vite.config.js:14`、`:19` |
| Mock 外部系统 | `MOCK_PORT=8001`，`mock_remote_system.py`；**快照内不存在脚本所引用的 `backend/datas/mock_remote_system.py`**（只有仓根 `simulate_remote_system.py`），按脚本无法拉起 mock | `start-tmux-services.sh:24`~`:25` |
| 运行用户 | **未能确认**（无证据表明 tmux 会话语主） | `start-tmux-services.sh` 未指定用户；快照文件属主为 `lyajun`，但那是同步产物、非远端真实属主 |
| Python 版本 | 3.11（README 要求；环境名 `ard`） | `README.md:60`、`start-tmux-services.sh:10` |
| 应用版本号 | `main.py:319` 写 `1.0.0`；`core/config.py:16` 写 `2.0.0` | 两处不一致，无实际语义 |
| 前端构建 | Vue 3 + Vite（`frontend/package.json`） | `frontend/src/router/index.js` 使用 `createWebHistory` |

### 2.2 依赖的中间件

| 中间件 | 端点 | 用途 | 代码位置 |
| --- | --- | --- | --- |
| OpenGauss 7.0.0-RC3 主节点 | `10.3.100.180:15400`，db `postgres` | 业务表 + 监听业务表（同库双引擎） | `core/config.py:71`~`:81`、`db/database.py:7`（主引擎）、`:19`（`client_encoding=LATIN1` 业务引擎） |
| MinIO 分布式集群 | `http://10.3.100.179:9000` | 源数据下载、COG/原始归档、缩略图 | `core/config.py:174`、`services/minio_service.py:14` |
| Redis | `redis://localhost:16379/0` | 配额计数、鉴权 code、邮件验证码 | `core/config.py:147`、`services/audit_service.py:36` |
| ② 剖分数据服务（cube_web） | `http://10.3.100.179:50039` | schema 导入 + 批次 scenes/delete/archive | `services/metadata_extractor.py:28`、`services/cube_sync.py:10`~`:26` |
| 外部业务系统（mock） | `http://127.0.0.1:8001` | 网络清单/数据投递 | `core/config.py:35`、`services/order_watcher.py:2982` |
| NODA 统一身份认证 | `https://noda.ac.cn/ca/oauth/*` | OAuth2 登录 | `core/config.py:122`~`:125` |
| Prometheus/Grafana | `monitoring/docker-compose.yml` | 节点与 MinIO 监控（非 T1 必需） | `monitoring/` |
| GDAL 命令行 | `gdalwarp`/`gdalbuildvrt`/`gdal_translate` | 仅 NetCDF→COG 用 | `services/COG.py:110`、`:114`、`:115` |
| `mc` 客户端 | `mc quota set\|clear minio_local/<bucket>` | MinIO 桶硬限额下发（shell 调用） | `services/minio_service.py:43`、`:47` |

---

## 3. 架构与模块地图

### 3.1 目录树（仅列 T1 相关，行数为实测 `wc -l`）

```
snapshots/poufennode04_my_demo/
├── backend/
│   ├── app/
│   │   ├── main.py                     410  FastAPI 入口、lifespan、启动迁移、路由注册
│   │   ├── core/config.py              183  运行配置（pydantic-settings）
│   │   ├── core/security.py            103  JWT 签发/校验、get_current_user、require_permission
│   │   ├── core/permissions.py         194  权限常量、内置岗位模板、legacy 映射
│   │   ├── db/database.py               57  双引擎（UTF-8 主库 + LATIN1 业务库）
│   │   ├── models/models.py            765  全部 ORM
│   │   ├── schemas/schemas.py          607  Pydantic 请求/响应模型
│   │   ├── routers/ard.py             1292  ARD 主路由（24 个操作 / 23 个路径）
│   │   ├── routers/orders.py           892  清单接入、订单、资产批量删除、WS
│   │   ├── routers/storage.py          249  中央存储网关（/api/storage）
│   │   ├── routers/order_mgmt.py       910  订单管理（独立模块，见 T5）
│   │   ├── routers/admin.py           2697  后台管理（T5）
│   │   ├── routers/auth.py            1221  认证（含 NODA/TOTP）
│   │   ├── routers/monitor.py         1139  监控/大屏/OTel 接收 + `/ws/tile` 瓦片指标 WebSocket
│   │   ├── routers/users.py            195  `/api/users`（profile/avatar/logout，注册于 `main.py:391`）
│   │   ├── routers/resource.py          24  `/api/resource/request` 资源申请（注册于 `main.py:395`）
│   │   ├── services/order_watcher.py  3487  监听 + 调度 + 批次工作流 + 回环反馈
│   │   ├── services/metadata_extractor.py 2843 格式解析 + 标准化 + COG + Cube 推送
│   │   ├── services/COG.py             174  tif_to_cog / netcdf_to_cog
│   │   ├── services/minio_service.py   392  MinIO 封装（用户桶/团队桶/配额）
│   │   ├── services/cube_sync.py       191  Cube 删除/归档 HTTP 客户端
│   │   ├── services/business_interface_contract.py 42  260727 接口字段白名单
│   │   ├── services/websocket_manager.py 41 订单事件连接管理器（供 order_mgmt 用）
│   │   ├── services/file_watcher.py    485 瓦片 OTel 指标（非文件监听）
│   │   ├── services/tile_processor.py  257 纯 Python 模拟切片器（演示用）
│   │   ├── services/quota_guard.py     107  Redis API 计数 + CPU 令牌 + 存储封顶
│   │   ├── services/quota_checker.py    40  用户资源拨备（桶 + 限额 + Redis）
│   │   ├── services/audit_service.py    67  redis_client 单例 + 临时 code
│   │   ├── services/metrics.py          31  Prometheus 指标定义 + `collect_metrics()`
│   │   ├── services/tile_logger.py      65  瓦片监控上报脚本（`report_tile_metric`，配套仓根 TILE_METRIC_REPORTING.md）
│   │   └── services/sitecustomize.py   206  侵入式监控探针（见 9.F-6）
│   ├── tests/                          test_cog_conversion.py / test_partition_schema_contract.py
│   ├── test_cube_sync.py               31  unittest（990 B）
│   └── test_closed_loop_feedback.py   103  unittest（3250 B）
├── frontend/src/views/ARD.vue        3036  ARD 载入页面
├── prepare_offline_data.py                 离线数据预上传脚本
├── offline_manifest_{optical,radar,info,cabsat_l2}.json  离线清单样例
└── docs/COG_CONVERSION_IMPLEMENTATION.md 等 4 份运维说明
```

### 3.2 模块职责表

| 模块 | 文件 | 行数 | 职责 | 关键函数（行号） |
| --- | --- | --- | --- | --- |
| 应用入口 | `backend/app/main.py` | 410 | lifespan 建表/迁移/启动调度器；注册 9 个 router；配额中间件 | `lifespan:277`、`init_database:211`、`migrate_ard_idempotency_columns:36`、`migrate_business_interface_columns:127`、`enforce_realtime_api_quota_middleware:353` |
| 运行配置 | `core/config.py` | 183 | 目录、库连接、JWT、NODA、Redis、MinIO 常量 | `Settings:14`、`DATABASE_URL:78`、`MinIOConfig:172` |
| 双库引擎 | `db/database.py` | 57 | UTF-8 主库 + `client_encoding=LATIN1` 业务库 | `engine:7`、`business_engine:19`、`get_db:48` |
| ORM | `models/models.py` | 765 | 30 张表的映射 | `Order:310`、`ArdPartitionBatch:357`、`ARDManifest:603`、`MappingRule:617`、`ArdBusinessSyncState:562` |
| ARD 路由 | `routers/ard.py` | 1292 | 清单、监听、提取、字典、批次、优先级、Cube 重推、日志 | 24 个操作 / 23 个路径（见 4.1） |
| 清单/订单路由 | `routers/orders.py` | 892 | 清单上传/接收、订单查询、批量删除、WS 广播泵 | `receive_external_manifest:440`、`upload_offline_manifest:453`、`reprocess_order:483`、`order_change_callback:519`、`orders_websocket:857` |
| 监听与调度 | `services/order_watcher.py` | 3487 | 单例服务：目录扫描、业务表轮询、幂等键、优先级队列、批次工作流、数据加载管道、回环反馈、WS 事件 | 见 8.1~8.5 |
| 元数据引擎 | `services/metadata_extractor.py` | 2843 | 格式解析、标准化、字典映射、COG 归档、MinIO 上传、Cube 推送 | 见 8.6~8.8 |
| COG 转换 | `services/COG.py` | 174 | `tif_to_cog:121`（CRS 保留/赋值/拒绝）、`netcdf_to_cog:57`（GDAL 逐变量 warp + buildvrt + translate） | 同名函数 |
| 对象存储 | `services/minio_service.py` | 392 | boto3 S3 客户端、桶命名、前缀删除保护、配额下发、团队迁移 | `_get_bucket_name:23`、`delete_prefix:75`、`set_bucket_quota:34` |
| Cube 同步 | `services/cube_sync.py` | 191 | 批次物理删除、批次归档、数据集归档 | `archive_cube_datasets:52`、`archive_cube_load_batches:88`、`delete_cube_load_batches:120`、`_load_batch_id_from_dataset_id:32` |
| 字段契约 | `services/business_interface_contract.py` | 42 | 260727 文档的 47 个数据集字段 + 41 个影像字段白名单与表头识别 | `DATASET_FIELDS:3`、`IMAGE_FIELDS:15`、`is_dataset_header:31`、`is_image_header:36` |
| 台账 | `services/business_interface_contract.py` | — | 见上 | — |
| 配额守卫 | `services/quota_guard.py` | 107 | API 计数/CPU 令牌桶/存储封顶 | `check_and_incr_api:8`、`enforce_storage_limit:28`（**0 调用点**）、`lease_cpu_cores:56`、`return_cpu_cores:98` |
| 监控路由 | `routers/monitor.py` | 1139 | 监控大屏、OTel `/v1/traces`、瓦片指标 `/ws/tile`、PDF 导出 | `@router.post("/v1/traces"):1099`、`tile_websocket:1122`、PDF 惰性导入 reportlab`:640`~`:647` |
| 用户/资源路由 | `routers/users.py`、`routers/resource.py` | 195 / 24 | `/api/users` 个人资料、`/api/resource/request` 资源申请 | `users.py:41`/`:90`/`:121`/`:177`；`resource.py:12` |
| 指标聚合 | `services/metrics.py` | 31 | Prometheus Gauge/Counter/Histogram 定义与 `collect_metrics()` | `collect_metrics:18`；被 `routers/monitor.py:498` 的 `/metrics` 端点调用 |
| 瓦片上报 | `services/tile_logger.py` | 65 | `report_tile_metric()` 组装 OTel span 并 POST 到硬编码的 `http://10.136.1.14:8000/api/v1/traces`（`:21`） | `report_tile_metric:3` |
| Redis 单例 | `services/audit_service.py` | 67 | `redis_client`、`save_code`、`save_email_verification_code` | `redis_client:36` |
| WS 管理器 | `services/websocket_manager.py` | 41 | 按 `user_id` 分组推送；`send_order_event` 同时抄送操作员 | `connect:8`、`send_order_event:29` |
| 前端页面 | `frontend/src/views/ARD.vue` | 3036 | 清单上传、批次表、字典映射弹窗、进度日志 WS、资产删除 | `connectLogSocket:1602`、`executeOfflineManifestUpload:1728`、`retryCubeSync:1073` |

---

## 4. 对外接口契约

统一前缀与鉴权说明：

- `routers/ard.py` 由 `main.py:393` 以 `prefix="/api/ard"` 注册，`router = APIRouter()`（`routers/ard.py:45`）自身无前缀。
- `routers/orders.py` 由 `main.py:396` 以 `prefix="/api"` 注册。
- `routers/storage.py:15` 自带 `prefix="/api/storage"`，`main.py:398` 不额外加前缀。
- 鉴权方式：`Depends(require_permission(...))`（`core/security.py:85`）。
  权限常量：`PERMISSION_DATA_IMPORT_VIEW = "data_import:view"`、
  `PERMISSION_DATA_IMPORT_OPERATE = "data_import:operate"`（`core/permissions.py:30`、`:31`）。
- **运行时证据**（2026-09-12 实测，只读 curl）：
  - `curl -s -o /dev/null -w '%{http_code}' http://10.3.100.182:6000/health` → `200`，body `{"status":"healthy"}`
  - `curl -s -o /dev/null -w '%{http_code}' http://10.3.100.182:6000/api/ard/listen/status` → `401`，body `{"detail":"Not authenticated"}`
  - `curl -s http://10.3.100.182:6000/openapi.json` → HTTP 200，共 **125** 条路径。

### 4.1 `/api/ard/*` 路由清单（24 个操作 / 23 个路径，全部经 openapi.json 交叉验证存在）

| # | 方法 | 路径 | 鉴权权限 | 请求体关键字段 | 响应关键字段 | 代码位置 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | GET | `/api/ard/manifests` | `data_import:view` | — | `batch_id,source,result,status,version,size,time` | `routers/ard.py:302` |
| 2 | POST | `/api/ard/listen/start` | `data_import:operate` | — | `status,message` | `routers/ard.py:312` |
| 3 | POST | `/api/ard/listen/stop` | `data_import:operate` | — | `status,message` | `routers/ard.py:318` |
| 4 | GET | `/api/ard/listen/status` | `data_import:view` | — | `is_listening,mode="HYBRID_BROADCAST_AND_OFFLINE"` | `routers/ard.py:324` |
| 5 | GET | `/api/ard/pending-orders` | `data_import:view` | `page,page_size` | `PaginatedResponse[OrderResponse]` | `routers/ard.py:330` |
| 6 | POST | `/api/ard/extract/batch` | `data_import:operate` | `order_ids: list[int]` | `success_count,failed_count,failed_details` | `routers/ard.py:373` |
| 7 | POST | `/api/ard/extract/{order_id}` | `data_import:operate` | 路径参数 | `message,order_id` | `routers/ard.py:404` |
| 8 | POST | `/api/ard/extract/remote` | `data_import:operate` | query `url`,`source_type` | `status,meta` | `routers/ard.py:425` |
| 9 | GET | `/api/ard/dict/source-fields` | `data_import:view` | — | `tables[].{key,label,table_name,row_count,fields[]}` | `routers/ard.py:462` |
| 10 | GET | `/api/ard/dict/builtin-rules` | `data_import:view` | — | `title,mapping_applied=False,rules[]` | `routers/ard.py:533` |
| 11 | GET | `/api/ard/dict/rules` | `data_import:view` | `source,search,page,page_size` | `MappingRulePage{total,page,page_size,items[]}` | `routers/ard.py:546` |
| 12 | POST | `/api/ard/dict/rule` | `data_import:operate` | `source_type,source_field,target_field,rule_type,transform_rule,badge` | `message,id` | `routers/ard.py:583` |
| 13 | PUT | `/api/ard/dict/rule/{rule_id}` | `data_import:operate` | 同 12 | `message` | `routers/ard.py:598` |
| 14 | DELETE | `/api/ard/dict/rule/{rule_id}` | `data_import:operate` | — | `message` | `routers/ard.py:616` |
| 15 | POST | `/api/ard/start-processing` | `data_import:operate` | — | `message,allocated_slots` | `routers/ard.py:630` |
| 16 | DELETE | `/api/ard/ard/completed-batch/{batch_id}` | `data_import:operate` | 路径参数 | `status,message` | `routers/ard.py:660` |
| 17 | POST | `/api/ard/completed-batches/batch-delete` | `data_import:operate` | `batch_ids: list[str]` | `status,message,failed[]` | `routers/ard.py:808` |
| 18 | POST | `/api/ard/excel-watcher/toggle` | `data_import:operate` | `enabled: bool`,`interval_minutes: int` | `status,message,interval_minutes,target` | `routers/ard.py:927` |
| 19 | GET | `/api/ard/excel-watcher/status` | `data_import:view` | — | `is_running,interval_minutes,interval_seconds,dataset_table,image_table,business_shared_root,dataset_storage_root,max_files_per_batch` | `routers/ard.py:959` |
| 20 | GET | `/api/ard/batches` | `data_import:view` | query `reconcile_partition_status: bool=False` | `[{batch_id,batch_name,data_type,extract_status,cog_status,load_status,overall_status,priority,dispatch_status,queue_position,loaded_at,total_orders,status_counts}]` | `routers/ard.py:980` |
| 21 | PUT | `/api/ard/batches/{batch_id}/priority` | `data_import:operate` | `priority: int`（0~100 夹取） | `status,batch_id,priority,dispatch_status` | `routers/ard.py:1145` |
| 22 | GET | `/api/ard/batches/{batch_id}/orders` | `data_import:view` | — | 订单数组 | `routers/ard.py:1184` |
| 23 | POST | `/api/ard/batches/{batch_id}/cube-sync` | `data_import:operate` | — | `status="cube_pushed",batch_id`；失败 422 + 日志 detail | `routers/ard.py:1214` |
| 24 | GET | `/api/ard/processing-logs` | `data_import:view` | `action`（逗号分隔）、`limit`（1~500） | `[{id,order_id,action,old_status,new_status,details,created_at,created_by}]` | `routers/ard.py:1260` |

**逐条补充说明（只写表格表达不了的）：**

- **#15 `start-processing` 是占位实现**。它申请 CPU 槽位后执行的内层函数
  （`routers/ard.py:637`~`:651`）只做 `psutil` 亲和性绑定 + `from services.tile_processor import
  TileProcessor`，从不实例化该对象，也不写任何数据；注释自述 "专属算力瓦片剖分任务执行完毕"。
  真正的切片能力不在本子系统。
- **#16 路径前缀重复**。`@router.delete("/ard/completed-batch/{batch_id}")` 叠加
  `prefix="/api/ard"` 后实际路径为 `/api/ard/ard/completed-batch/{batch_id}`。运行时实测：
  `curl -o /dev/null -w '%{http_code}' -X DELETE .../api/ard/ard/completed-batch/xxx` → 无凭证时
  路由存在（`GET` 该路径返回 `405 Method Not Allowed`，证明路由已注册）；
  而前端调用的是 `/ard/completed-batch/${orderId}`（`frontend/src/views/ARD.vue:1483`，
  axios `baseURL="/api"`，`frontend/src/api/index.js:7`）→ 实际请求
  `/api/ard/completed-batch/xxx`，运行时返回 **404 Not Found**。该接口前后端不匹配，见 9.F-1。
- **#20 `reconcile_partition_status`** 为 `true` 时会对每个批次查 Cube
  `GET /v1/partition/load-batches/{id}/scenes`，若该批次下**所有** band 的
  `grid_statuses[].partition_status == "completed"`，则该批次被从列表中隐藏
  （`routers/ard.py:249` `_batch_is_partitioned`、`:278` `_partitioned_load_batch_ids`）。
  Cube 查询失败（非 200 或异常）时返回 `None`，批次保持可见（`:288`）。
- **#23 `cube-sync`** 的入参是 ARD 侧 `batch_id`，不是 Cube 的 `load_batch_id`；内部从
  `orders.parsed_data.load_batch_id` 反查（`services/metadata_extractor.py:1657` `retry_batch_sync`）。

### 4.2 `/api/orders/*` 与 WebSocket

| # | 方法 | 路径 | 鉴权 | 请求体 | 代码位置 | 说明 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | GET | `/api/orders` | `get_current_user` | query `status,limit,batch_id,page,page_size,verify_minio` | `routers/orders.py:420` | 转 `order_watcher.get_orders`（`services/order_watcher.py:2996`） |
| 2 | POST | `/api/orders/receive-manifest` | **无鉴权依赖** | 任意 JSON `manifest`；query `user_id` | `routers/orders.py:440` | 仅靠 `order_watcher._running` 判断（`services/order_watcher.py:2152`），关闭时返回 403 |
| 3 | POST | `/api/orders/upload-manifest` | `data_import:operate` | `multipart/form-data`，字段名 `file`，后缀限 `.json/.xml/.xlsx/.xls` | `routers/orders.py:453` | 离线入口 |
| 4 | POST | `/api/orders/{batch_id}/reprocess` | `get_current_user_obj` + 归属/权限二选一 | 路径参数 | `routers/orders.py:483` | 重置失败订单重新拉取 |
| 5 | POST | `/api/orders/batch-delete` | `get_current_user_obj` + 生效权限 `order_manage:operate` | `batch_ids: list[str]`（批次 ID 或 order_id；空则 400 `未指定编号`） | `routers/orders.py:562` | 展开为局部变量 `identifiers` 后级联删 MinIO + Cube 批次/数据集；响应含 `duplicate_cleanup`/`cube_duplicate_cleanup`（见下） |
| 6 | WS | `/api/ws/orders?token=<JWT>` | JWT 查询参数 | 文本帧 `ping` / `refresh` | `routers/orders.py:857` | 首次连接下发 `{"type":"initial",...}`；进度事件由 `order_change_callback`（`:519`）广播 |

### 4.3 前端调用但后端不存在的接口（全部经 openapi.json 核对）

| 前端调用 | 位置 | 后端实际 | 结果 |
| --- | --- | --- | --- |
| `api.post('/ard/manifests/${id}/retry')` | `frontend/src/api/index.js:139` | 不存在 | 死代码（`ARD.vue` 已改用 `/orders/{id}/reprocess`） |
| `api.get('/ard/meta/result')` | `frontend/src/api/index.js:140` | 不存在 | 死代码 |
| `api.post('/config/watch-dir', ...)` | `frontend/src/api/index.js:142` | 不存在 | 死代码 |
| `api.delete('/ard/completed-batch/${id}')` | `ARD.vue:1483` | 后端为 `/api/ard/ard/completed-batch/{id}` | **404，功能不可用** |

**`/api/orders/batch-delete` 的 Cube 清理链路（与 `services/cube_sync.py` 并行的第二条同步/归档路径，全部在 `routers/orders.py`）：**

1. 请求体读 `payload.get("batch_ids", [])`（`:568`），空则 400 `未指定编号`；认证令牌取 `_request_bearer_token(request)`（`:364`）。
2. `_expand_duplicate_batch_ids(db, ids, ...)`（`:113`~`:186`）把批次 ID / order_id 展开为局部变量 `identifiers`，并返回 `duplicate_cleanup`（同数据集重复批次清理记录）。
3. 批次级分支：`_cube_load_batch_ids_for_ard_batch`（`:312`）把 ARD 批次桥接到 Cube `load_batch_id`；`_find_stale_cube_duplicate_ids`（`:189`）用 `PARTITION_BATCH_LIST_URL`（`:46`~`:49`，默认 `http://10.3.100.179:50039/v1/partition/load-batches`，查询参数 `limit=500&status=succeeded`，调用点 `:209`）找出 Cube 侧陈旧重复批次，再调 `services.cube_sync.archive_cube_load_batches`（`:648`）归档，结果进 `cube_duplicate_cleanup`（`:654`）。
4. 批次级/订单级删除：调 `services.cube_sync.delete_cube_load_batches`（`:660`、`:812`；内部先 delete、404 回落 archive）。
5. MinIO 实体与 `raw_meta.json` 逐订单清理（`:667`~`:698`），单条失败只记入 `failed[]` 不阻断其余删除（`:837`~`:840`）。
6. 响应固定返回 `duplicate_cleanup` 与 `cube_duplicate_cleanup` 两个字段（`:850`~`:851`）。

> 同一文件里的 `_archive_cube_datasets_for_delete`（`:372`）与 `_archive_cube_load_batches_for_delete`（`:395`）**定义后从未被调用**（死代码），实际归档走 `services.cube_sync`。`PARTITION_*_URL_TEMPLATE`（`:30`~`:50`）是 orders.py 自带的一套独立定义，并非只依赖 `services/cube_sync.py`；两处默认端点相同但实现独立，改 Cube 接口时要同步改两处。

---

## 5. 数据模型

### 5.1 表清单

| 表名 | 归属库 | 作用 | 关键字段 | DDL 位置 |
| --- | --- | --- | --- | --- |
| `orders` | 主库 | 单个数据文件（一个场景/一个资产）的载入订单 | `id,order_id,external_id,source_type,status,user_id,raw_data,parsed_data,data_hash,version,file_path,file_size,batch_identifier,retry_count,error_message` | `models/models.py:310`~`:355` |
| `ard_partition_batches` | 主库 | 载入批次（幂等键 + 调度单元 + 状态机） | `batch_id,source_dataset_id,source_version,source_fingerprint,status,execution_payload,submitted_by,priority,extract_status,cog_status,load_status,overall_status,raw_meta_uri,data_type` | `models/models.py:357`~`:392`；列迁移 `main.py:36`~`main.py:124` |
| `ard_partition_assets` | 主库 | 非碳数据的资产子表（Scene/Band 级） | `batch_id(FK),asset_id,source_uri,scene_id,acq_time,sensor,product_family,resolution,bbox,corners,bands,band,file_format,polarization,sidecars,orbit_direction,relative_orbit` | `models/models.py:393`~`:427` |
| `ard_partition_observations` | 主库 | 碳卫星（碳）观测点级子表 | `batch_id(FK),observation_id,source_uri,source_index,acq_time,lon,lat,xco2,quality_flag,corners` | `models/models.py:428`~`:450` |
| `ard_dataset_metadata` | 业务库（LATIN1） | 外部业务系统的数据集元数据（**只读 + 只写 feedback 四列**） | `dataset_id,title,cstr,archivedPath,dataVersion,pushDate,productFormat,dataScore,fileSize,fileItemNum,feedback_status,feedback_time,feedback_message,feedback_batch_id` | `models/models.py:451`~`:512`；列迁移 `main.py:127`~`main.py:187` |
| `ard_image_metadata` | 业务库（LATIN1） | 影像元数据（`entity_path` 指向实体文件） | `id,dataset_id,title,entity_path,shoot_time,file_type,thumb_img,geometry` | `models/models.py:513`~`:561` |
| `ard_business_sync_states` | 主库 | ARD 自维护的业务同步状态（**不回写外部表**）；`image_id="__dataset__"` 作为数据集级标记 | `dataset_id,image_id,batch_id,order_id,status,source_path,target_path,error_message,retry_count,raw_snapshot`；唯一约束 `(dataset_id,image_id)` | `models/models.py:562`~`:586` |
| `order_processing_logs` | 主库 | 订单级处理日志 | `order_id(FK),action,old_status,new_status,old_hash,new_hash,details,created_by` | `models/models.py:587`~`:602` |
| `ard_manifests` | 主库 | 载入批次清单（前端"已完成资产"列表的数据源） | `batch_id(unique),source,result,status,version,size,time,checksum,meta_data` | `models/models.py:603`~`:616` |
| `mapping_rules` | 主库 | 标准映射字典 | `source_type,source_field,target_field,rule_type,transform_rule,badge` | `models/models.py:617`~`:627` |
| `system_config` | 主库 | 全局开关（如 `order_listening_enabled`） | `key(unique),value,value_bool` | `models/models.py:16`~`:46` |
| `users` / `teams` / `team_members` / `audit_logs` / `permission_templates` | 主库 | 与 ⑤后台管理共用 | — | `models/models.py:60`,`:200`,`:216`,`:165`,`:104` |
| `tile_metrics` / `tile_monthly_metrics` / `tile_trace_records` | 主库 | 瓦片指标（由 `services/file_watcher.py` 维护，非 T1 主链路） | `tile_type,day/week/month/year/total_count` | `models/models.py:718`,`:737`,`:752` |

### 5.2 关键状态枚举（全部从代码抄出）

**`OrderStatus`（`models/models.py:295`）**

| 枚举名 | 值（中文） | 含义 |
| --- | --- | --- |
| `PENDING` | `待处理` | 新订单，待解析 |
| `PROCESSING` | `处理中` | 正在解析 |
| `COMPLETED` | `已完成` | 解析完成 |
| `FAILED` | `失败` | 解析失败 |
| `UPDATED` | `已更新` | 数据有更新，需重新处理 |

**`OrderSource`（`models/models.py:302`）**：`HIGLASS`、`HLS`、`TANSAT`、`REFLECTANCE`、`RADAR`、`OTHER`。

**清单 `data_type` → `OrderSource` 映射**（`services/order_watcher.py:71` `SOURCE_MAPPING`，由
`resolve_order_source`（`:97`）兜底为 `OTHER`）：

| 清单值 | 映射结果 |
| --- | --- |
| `OPTICAL` / `REFLECT` / `REFLECTANCE` | `REFLECTANCE` |
| `CABSAT_L2` / `TANSAT` / `CARBON` | `TANSAT` |
| `INFO` / `MIXED` / `OTHER` | `OTHER` |
| `RADAR` | `RADAR` |

**批次三子状态 + 总状态**（`models/models.py:382`~`:385`）：

- `extract_status` / `cog_status` / `load_status`：注释写 `pending/processing/succeeded/failed`，
  但**实际写入值不含 `succeeded`**。三个子列的真实取值集合为
  `pending` / `processing` / `completed` / `failed` / `n/a` / `stale_reingest`：
  - `pending`：建批次时初始值（`services/order_watcher.py:406`~`:407`）；
  - `processing`：`_claim_next_batch` 领取（`services/order_watcher.py:372`~`:375`）与状态反推（`:1770`~`:1772`）；
  - `completed` / `n/a`：提取落库（`services/metadata_extractor.py:1293`~`:1295`，碳数据 `cog_status = "n/a"` 在 `:1294`）；
  - `failed`：提取失败（`services/metadata_extractor.py:1463`~`:1465`）、批次订单全失败（`services/order_watcher.py:1737`~`:1739`、`:2145`~`:2147`）；
  - `stale_reingest`：启动对账标记陈旧批次时，**三个子状态与 `status`/`overall_status` 一并被写成 `stale_reingest`**（`services/order_watcher.py:1714`~`:1718`）。
  因此“`stale_reingest` 只出现在 `status`/`overall_status`”不成立。
- `status`（调度状态，`models/models.py:378` 注释 `pending, running, succeeded, failed`）：
  真实写入为 `pending`（`services/order_watcher.py:406` 建批次）、
  `running`（`_claim_next_batch:371` 领取）、`succeeded`（`_execute_dataset_batch_workflow` 收尾，`:2122`）、
  `failed`（`_mark_batch_failed:2139`），以及 `stale_reingest`（`:1714`；父批次 `:659`）。
- `overall_status` 的取值：`pending` / `processing` / `completed` / `failed` /
  `cube_pushed` / `cube_push_failed` / `stale_reingest`。
  最后两个由 `routers/ard.py:1226`~`:1229`（`cube-sync` 重推成功/失败）与
  `services/metadata_extractor.py:1629`（`CUBE_SYNC_FAILED` 日志）决定。
- **前端展示态与 DB 态不同**。`GET /api/ard/batches` 用订单状态**反推**批次状态
  （`routers/ard.py:1063`~`:1123`），并把 `dispatch_status` 归一为
  `completed` / `failed` / `running` / `queued`（`routers/ard.py:1125`~`:1134`）。
  当批次无关联订单时强制置为 `failed`（`routers/ard.py:1068`~`:1071`，注释说明这是历史 bug 的修复）。

**`ArdBusinessSyncState.status`**（`models/models.py:571`，默认 `"discovered"`，注释未给全集）：
代码中出现的值为 `discovered` / `copying` / `processing` / `completed` / `failed`
（`services/order_watcher.py:478` 的 `status.in_([...])` 过滤器给出真实集合）。

**`OrderProcessingLog.action`** 真实取值（`grep` 全仓）：`METADATA_EXTRACTED_AND_COG`
（`services/metadata_extractor.py:1408`）、`CUBE_SYNC`（`:1716`、`:1738`）、
`CUBE_SYNC_FAILED`（`:1774`、`:1809`，批次跳过时 `:1642`）。
函数文档注释里的 `PARSE, UPDATE, ERROR` 已不在代码中出现（`models/models.py:592`）。

**`ARDManifest.result` / `.status`**：真实写入恒为 `result="success"`、`status="completed"`
（`services/metadata_extractor.py:1379`~`:1395`）；`models/models.py:609`~`610` 注释里的
`已加载 / 监听中 / 待重试` 与 `success / warning / danger` 三档未在代码中产生。

**`MappingRule.rule_type`**：`models/models.py:624` 注释 `field, unit, code, missing`；
`services/metadata_extractor.py:2779` 只对 `rule_type == "unit"` 实现数值变换，
`transform_rule` 支持 `×10^6` 与 `×0.0001` 两个字面量（`:2782`、`:2784`）。

### 5.3 关键关系

```
ard_partition_batches.batch_id
  ├─ orders.batch_identifier              （1:N，批次内订单）
  ├─ ard_partition_assets.batch_id (FK→id)（1:N，非碳资产）
  ├─ ard_partition_observations.batch_id  （1:N，碳观测）
  ├─ ard_manifests.batch_id               （逻辑关联，非 FK；值为 ard-load-<uuid>）
  ├─ ard_business_sync_states.batch_id    （逻辑关联）
  └─ order_processing_logs.order_id (FK→orders.id)

ard_dataset_metadata.dataset_id
  ├─ ard_image_metadata.dataset_id        （1:N）
  └─ ard_business_sync_states.dataset_id  （1:N，image_id 可为 "__dataset__"）
```

**重要不一致**：`ard_partition_batches.batch_id`（业务批次，如 `B260721155716_A1B2C3`、
`OFFB...`、`DB1...`）与 `ard_manifests.batch_id`（Cube 载入批次 `ard-load-<32位 hex>`）
**是两个不同的 ID 空间**。代码在 `services/metadata_extractor.py:1374` 用
`manifest_batch_key = ard_load_batch_id` 写清单，而 `:1370` 把 `order.batch_identifier`
保持为业务批次 ID。`routers/ard.py:255` `_load_batch_ids_for_ard_batch` 负责在两者之间桥接
（同时读 `orders.parsed_data.load_batch_id` 和 `ard_manifests.meta_data`）。

---

## 6. 配置项

### 6.1 环境变量 / 类属性（`core/config.py`，可被同名环境变量或 `.env` 覆盖）

`Settings.Config.env_file = ".env"`（`core/config.py:170`）。**注意：快照仓库内不存在 `.env`
文件**（`ls .env*` 无结果），因此生产部署实际依赖进程环境变量或代码默认值。

| 名称 | 作用 | 默认值 | 来源 | 必填 |
| --- | --- | --- | --- | --- |
| `SHARE_ROOT` | 共享交换区根 | `/nfs/public_data1` | `core/config.py:26` | 是 |
| `SHARED_DELIVERY_DIR` | 与外部系统的数据交换区 | `/nfs/public_data1/shared_delivery_ard` | `core/config.py:29` | 是 |
| `RAW_DATA_ROOT` | 离线源数据根 | `/nfs/public_data1/raw_data/ARD数据` | `core/config.py:32` | 是 |
| `REMOTE_SYSTEM_URL` | 外部业务系统 HTTP 地址 | `http://127.0.0.1:8001` | `core/config.py:35` | 是（network 模式） |
| `MAX_FILES_PER_BATCH` | 单批次最多处理文件数（0=不限） | `10` | `core/config.py:38` | 否 |
| `POLL_INTERVAL_SECONDS` | 远程清单轮询默认间隔（秒） | `600` | `core/config.py:58` | 否 |
| `BUSINESS_DATASET_TABLE` | 业务数据集表名 | `ard_dataset_metadata` | `core/config.py:60` | 是 |
| `BUSINESS_IMAGE_TABLE` | 业务影像表名 | `ard_image_metadata` | `core/config.py:61` | 是 |
| `ARD_SYNC_STATE_TABLE` | ARD 自维护同步状态表名 | `ard_business_sync_states` | `core/config.py:62` | 是 |
| `BUSINESS_SHARED_ROOT` | 业务实体数据源根 | `/nfs/public_data1/shared_delivery_ard` | `core/config.py:64` | 是 |
| `DATASET_STORAGE_ROOT` | ARD 专用数据集存储区 | `/nfs/public_data1/ard_dataset_storage` | `core/config.py:65` | 是 |
| `BUSINESS_METADATA_POLL_LIMIT` | 单轮读取数据集条数 | `50` | `core/config.py:66` | 否 |
| `DB_USER` / `DB_PASSWORD` / `DB_HOST` / `DB_PORT` / `DB_NAME` | OpenGauss 连接 | `core/config.py:71`~`:75`（**均硬编码，值不在此抄录**） | `core/config.py:71`~`:75`，URL 拼接 `:78`~`:81` | 是 |
| `DB_POOL_SIZE` / `DB_MAX_OVERFLOW` / `DB_POOL_RECYCLE` | 连接池 | `20` / `40` / `3600` | `core/config.py:85`~`:87` | 否 |
| `SECRET_KEY` | JWT 签名密钥 | `core/config.py:90`（**硬编码占位值，值不抄录**） | `core/config.py:90` | 是（生产必须替换） |
| `ALGORITHM` | JWT 算法 | `HS256` | `core/config.py:91` | 否 |
| `ACCESS_TOKEN_EXPIRE_MINUTES` | 会话空闲过期 | `60` | `core/config.py:93` | 否 |
| `REDIS_URL` | Redis 连接 | `redis://localhost:16379/0` | `core/config.py:147` | 是 |
| `NODA_CLIENT_ID` / `NODA_CLIENT_SECRET` | NODA OAuth 凭据 | `core/config.py:118`~`:119`（**硬编码，值不抄录**） | `core/config.py:118`~`:119` | 否 |
| `NODA_REDIRECT_URI` | OAuth 回调（经前端 5177 转发） | `http://10.3.100.182:5177/api/auth/noda/callback` | `core/config.py:130` | 否 |
| `MinIOConfig.ENDPOINT` | MinIO API | `http://10.3.100.179:9000` | `core/config.py:174` | 是 |
| `MinIOConfig.ACCESS_KEY` / `SECRET_KEY` | MinIO 凭据 | `core/config.py:175`~`:176`（**硬编码，值不抄录**） | 同上 | 是 |
| `MinIOConfig.BUCKET_PREFIX` | 用户桶前缀 | `user-` | `core/config.py:177` | 否 |
| `MinIOConfig.RAW_PREFIX` / `TILES_PREFIX` / `TEMP_PREFIX` / `AVATAR_PREFIX` | 对象前缀常量 | `raw_data/` / `tiles/` / `tmp/` / `avatar/` | `core/config.py:178`~`:181` | 否 |

### 6.2 通过 `os.getenv` 读取的变量

| 名称 | 作用 | 默认值 | 来源 | 必填 |
| --- | --- | --- | --- | --- |
| `PARTITION_SCHEMA_IMPORT_URL` | Cube schema 导入端点 | `http://10.3.100.179:50039/v1/partition/schemas/import` | `services/metadata_extractor.py:28` | 否 |
| `PARTITION_BATCH_DELETE_URL_TEMPLATE` | 批次物理删除 | `http://10.3.100.179:50039/v1/partition/load-batches/{load_batch_id}/delete` | `services/cube_sync.py:10` | 否 |
| `PARTITION_BATCH_ARCHIVE_URL_TEMPLATE` | 批次归档 | `.../load-batches/{load_batch_id}/archive` | `services/cube_sync.py:14` | 否 |
| `PARTITION_DATASET_ARCHIVE_URL_TEMPLATE` | 数据集归档 | `http://10.3.100.179:50039/v1/datasets/{dataset_id}/archive` | `services/cube_sync.py:18` | 否 |
| `PARTITION_BATCH_SCENES_URL_TEMPLATE` | 批次 scenes 查询 | `.../load-batches/{load_batch_id}/scenes` | `services/cube_sync.py:22` | 否 |
| `PARTITION_BATCH_LIST_URL` | Cube 批次列表查询（订单批量删除时找陈旧重复批次） | `http://10.3.100.179:50039/v1/partition/load-batches` | `routers/orders.py:46`~`:49`，调用点 `:209` | 否 |
| `PARTITION_SCHEMA_AUTH_TOKEN` | Cube 侧长期令牌（用于删除/归档/查询；**未用于 schema 导入**） | 空字符串 | `services/cube_sync.py:26`、`routers/orders.py:50`（orders.py 自带一套同名变量与 URL 模板，见 4.2） | 删除/归档时必填；为空则抛 `RuntimeError` |
| `ARD_MONITOR_SERVER` | 监控探针上报地址 | `http://localhost:6000`（模块常量 `services/sitecustomize.py:21`）；仅当 CWD 不存在 `ard_probe_config.json` 时由 `:66` 读环境变量 | `services/sitecustomize.py:66` | 否 |
| `ARD_TARGET_MODULE` / `ARD_TARGET_CLASS` | 探针目标 | `services/sitecustomize.py:67`~`:68` | 同上 | 否 |
| `ARD_PARTITION_METHOD` / `ARD_QA_METHOD` / `ARD_STORAGE_METHOD` | 探针劫持的方法名 | `services/sitecustomize.py:69`~`:71` | 同上 | 否 |
| `ARD_TOTAL_TILES` / `ARD_BATCH_ID` / `ARD_TILE_SIZE_MB` / `ARD_IDX_*` | 探针伪造的上报数据 | `services/sitecustomize.py:72`~`:77` | 同上 | 否 |
| `CUBE_BACKEND_URL` | OTel wrapper 上报目标 | `http://127.0.0.1:6000` | `otel_wrapper.py:68` | 否 |
| `MOCK_PORT` / `MOCK_RAW_DATA_ROOT` / `MOCK_SHARED_DELIVERY_DIR` / `MOCK_EXCEL_PATH` | mock 外部系统 | 见 `start-tmux-services.sh:29` | 同上 | 否 |

**安全提示**：`core/config.py` 与 `services/order_watcher.py:68` 之外，本子系统**没有**
从环境变量读取数据库口令、MinIO 密钥或 NODA secret 的路径——它们全部是代码内字面量
（`core/config.py:71`~`:76`、`:118`~`:119`、`:174`~`:176`）。这是与 AGENTS.md「不把凭据写入
仓库」相冲突的实现方式，接手指南：迁移到环境变量后再改动该文件。本手册不抄录任何取值。

### 6.3 运行时开关（存 `system_config` 表，非环境变量）

| key | 作用 | 默认 | 读取位置 |
| --- | --- | --- | --- |
| `order_listening_enabled` | 应用启动时是否自动拉起监听服务 | `False` | `main.py:286`（`SystemConfig.get_bool`） |

---

## 7. 依赖关系

### 7.1 内部依赖（跨子系统接口）

| 依赖对象 | 接口 | 调用位置 | 失败行为 |
| --- | --- | --- | --- |
| ② 分析就绪数据剖分 | `POST /v1/partition/schemas/import` | `services/metadata_extractor.py:1701` | 捕获 `HTTPStatusError`/`Exception`，写 `CUBE_SYNC_FAILED` 日志并返回 `False`；**不回滚 ARD 本地结果**（`:1697` 注释） |
| ② 分析就绪数据剖分 | `GET /v1/partition/load-batches/{id}/scenes` | `routers/ard.py:279` | 超时 10s；非 200 / 异常 → 返回 `None`，批次保持可见 |
| ② 分析就绪数据剖分 | `POST .../load-batches/{id}/delete` → 404 时回落 `/archive` | `services/cube_sync.py:156`（delete）、`:165`（404 回落）、`:175`/`:182`（失败抛错） | 抛 `RuntimeError`，删除接口整体失败 |
| ② 分析就绪数据剖分 | `POST /v1/datasets/{id}/archive` | `services/cube_sync.py:72`（调用）、`:80`（失败抛错） | 抛 `RuntimeError` |
| ② 分析就绪数据剖分 | `GET PARTITION_BATCH_LIST_URL` + `archive/delete load-batches`（订单批量删除的独立实现） | `routers/orders.py:209`、`:648`、`:660`、`:812` | 单条失败记入 `failed[]`，不阻断其余删除 |
| ⑤ 后台管理 | 共享 `users` 表；`user.cpu_used += 0.5`、`api_used += 1` 计费写回 | `services/metadata_extractor.py:1410`~`:1413` | 无独立异常处理 |
| ④ 资源调度 | `quota_guard.lease_cpu_cores` / `return_cpu_cores` | `services/metadata_extractor.py:783`、`:798` | `HTTPException` → 订单置 `FAILED`（`:797`）；其他异常也置 `FAILED`（`:809`） |
| ④ 资源调度 | `quota_guard.enforce_storage_limit` | **仓库内 0 次调用**：定义在 `services/quota_guard.py:28`，`grep -rn enforce_storage_limit` 仅命中该定义行。存储侧唯一落地动作是 `services/minio_service.py:34` `set_bucket_quota` 通过 `mc quota set`（`:47`）下发 MinIO 桶硬限额 | 不构成 ARD 链路的拦截点；函数体可抛 `HTTPException 507`，但当前永不触发 |
| ⑤ 后台管理 | `MinIOConfig.BUCKET_PREFIX` 桶命名约定（`user-{id}`） | `services/minio_service.py:23` | — |
| ③ 剖分数据服务 | 无直接依赖，全部经 ② 的 HTTP 接口 | — | — |
| ⑥ 全球离散格网模型与编码 | 无依赖 | — | — |

### 7.2 外部依赖

**Python 包**：`requirements.txt` 为 **UTF-16LE 编码**（`iconv -f UTF-16` 才能读），内容与代码
导入**严重不一致**。下表的缺失清单由对 `backend/` 全量 `.py` 做 AST import 抽取后与
`requirements.txt` 逐名比对得出，共 **15 个包（14 行，`boto3`/`botocore` 合并一行）**：

| 包 | 代码中使用位置 | 在 requirements.txt | 后果 |
| --- | --- | --- | --- |
| `boto3` / `botocore` | `services/minio_service.py:1`~`:2` | **缺失** | 按文件安装后无法启动 |
| `requests` | `routers/auth.py:17`（**模块级导入**，`main.py:390` 注册路由时即加载）、`services/tile_logger.py:1`、`services/sitecustomize.py:11` | **缺失** | 启动即 `ImportError`（硬失败） |
| `httpx` | `services/order_watcher.py:15`、`services/cube_sync.py:6` | **缺失** | 同上 |
| `redis` | `services/audit_service.py:4` | **缺失** | 同上 |
| `rasterio` | `services/metadata_extractor.py:35`、`services/COG.py:7` | **缺失** | COG 链路不可用 |
| `netCDF4` | `services/metadata_extractor.py:43`、`services/COG.py:12` | **缺失** | 雷达/碳数据不可用 |
| `geopandas` | `services/metadata_extractor.py:49` | **缺失** | Shapefile 不可用 |
| `aiohttp` | `services/metadata_extractor.py:2818`（`process_remote_file` 内） | **缺失** | `/extract/remote` 不可用 |
| `pandas` | 多处（`_extract_*` 后处理） | **缺失** | — |
| `openpyxl` | `services/order_watcher.py:1204`、`:2227` | **缺失** | Excel 清单不可用 |
| `prometheus_client`、`pyotp`、`qrcode` | `routers/monitor.py`、`routers/auth.py` | **缺失** | 监控/2FA 不可用 |
| `reportlab` | `routers/monitor.py:640`~`:647`（**惰性导入**，PDF 导出时） | **缺失** | 监控大屏 PDF 导出不可用 |
| `alembic` | requirements 有，代码**未使用**（无 `alembic.ini`、无 `versions/`，`find` 无结果） | 有 | 迁移实为 `main.py` 内联 `ALTER TABLE` |

注意区分：`requirements.txt` 里的 `pillow`（提供 `PIL`）、`PyJWT`（提供 `jwt`）、
`python-jose`（提供 `jose`）、`pydantic-settings` 确实存在，**不算缺失**。直接 `import` 未命中的
顶层运行时包只有 `aiosqlite`、`watchdog`、`alembic`、`cryptography`、`bcrypt` 五项；`passlib`
被 `core/security.py:5` 使用，其余 requirements 条目多为 FastAPI/SQLAlchemy/uvicorn 的传递依赖
或工具链依赖。

**系统/集群组件**：

| 组件 | 版本/地址 | 用途 |
| --- | --- | --- |
| Python | 3.11 | 运行时（`README.md:60`） |
| Node.js + Vite | 18+ | 前端 5177 |
| OpenGauss | 7.0.0-RC3，主 `10.3.100.180:15400`；asyncpg 驱动 + `ssl=disable` | 全部关系数据 |
| MinIO | 4 节点分布式，API `10.3.100.179:9000`，bucket `user-{id}` / `team-{id}` | 对象存储 |
| Redis | 7.x，`127.0.0.1:16379` | 配额/验证码 |
| GDAL CLI | 需在 PATH 中，缺失时 `services/COG.py:30`~`:32` 抛 `RuntimeError` | NetCDF→COG |
| `mc` | 需在 PATH 中 | MinIO 桶配额下发（`services/minio_service.py:47` 用 `subprocess.run(shell=True)`，调用在 `:51`） |

---

## 8. 关键业务流程

### 8.1 流程 A：离线 Excel 清单导入（主流程）

| 步 | 动作 | 代码位置 |
| --- | --- | --- |
| 1 | `POST /api/orders/upload-manifest` 上传 `.xlsx`，校验扩展名 | `routers/orders.py:457` |
| 2 | 校验 `data_import:operate` 权限，读取 bytes | `routers/orders.py:454`、`:461` |
| 3 | 转 `process_offline_uploaded_manifest` | `services/order_watcher.py:2212` |
| 4 | Excel → `_process_offline_excel` → `openpyxl.load_workbook` | `services/order_watcher.py:2226` |
| 5 | 逐 sheet 用 `_parse_interface_dataset_sheet` / `_parse_interface_image_sheet` 识别表头（`is_dataset_header` / `is_image_header`，要求 `title` + (`archivedPath` 或 `dataVersion`) / `id` + `dataset_id` + (`entity_path` 或 `shoot_time`)） | `services/order_watcher.py:2387`、`:2400`、`services/business_interface_contract.py:31`、`:36` |
| 6 | 构造标准化条目 | `services/order_watcher.py:2305` `_build_interface_item`、`:2285` `_interface_time` |
| 7 | 按 `dataset_id` 或 `local_path` 分组，组内去重（`dataset_id + version + local_path` 三元组） | `services/order_watcher.py:2415`~`:2441` |
| 8 | 计算幂等身份 `source_dataset_id / source_version / source_fingerprint`；**此阶段不扫描 NFS 目录**（`source_paths=[]`，注释见 `:2461`） | `services/order_watcher.py:1493` |
| 9 | 进程内预留（`_reserve_source`）+ DB 查重（`_find_existing_dataset_batch`）；命中则 `skip_count += 1` | `services/order_watcher.py:189`、`:1531`、`:2477` |
| 10 | 生成批次 ID `OFF<dataset_id前10位>_<yymmddHHMMSS>_<6位hex>`，`asyncio.create_task(_run_all())` 后台并发 | `services/order_watcher.py:1272`、`:2503` |
| 11 | 返回 `{"status":"success","summary":{"new":N,"updated":0,"skipped":M},"scheduled_batches":[...]}` | `services/order_watcher.py:2520`~`:2524` |
| 12 | 后台 `_enqueue_dataset_batch` 写入 `ard_partition_batches`（`status="pending"`、priority 夹取在 `:405`~`:407`，`execution_payload` 含 items `:408`~`:413`），进入函数体第二行即触发 `start_dispatcher()` | `services/order_watcher.py:379`（def）、`:384`（`self.start_dispatcher()`）、`:415`~`:419`（`_send_step` 入队日志） |
| 13 | `_dispatch_loop` 每 1 秒 `_claim_next_batch`：`SELECT ... FOR UPDATE SKIP LOCKED ORDER BY priority DESC, loaded_at ASC`，置 `status="running"` | `services/order_watcher.py:305`、`:351`~`:376` |
| 14 | 执行 `_execute_dataset_batch_workflow(mode="offline")`：直接用清单 `local_path`，不请求 mock | `services/order_watcher.py:1779`、`:1893` |
| 15 | 每文件调度 `_data_loading_pipeline` → `MetadataExtractor.process` | `services/order_watcher.py:2742`、`services/metadata_extractor.py:711` |
| 16 | 全部订单完成后 `batch_sync_to_cube` 推 Cube | `services/order_watcher.py:2067`（工作流内）、`services/metadata_extractor.py:1543` |
| 17 | 回环反馈写业务表 `feedback_*` | `services/order_watcher.py:2879` |

> **老解析器入口的活跃度**（接手时容易误判）：`_parse_real_manifest_sheet`（`services/order_watcher.py:3281`）仍被 `:1212` 与 `:2253` 调用，是活代码；`_parse_legacy_manifest`（`:3449`）经 `:3313` 被 `_parse_real_manifest_sheet` 回落调用，也是活代码。相反，`_verify_and_schedule_loading`（`:2527`，network 心跳/核验路径）与 `_resolve_local_dataset_copy`（`:3254`）**定义后无任何调用点**，属历史遗留死代码。

### 8.2 流程 B：业务元数据库定时轮询（network 模式）

| 步 | 动作 | 代码位置 |
| --- | --- | --- |
| 1 | `POST /api/ard/excel-watcher/toggle {"enabled":true,"interval_minutes":N}` | `routers/ard.py:927` |
| 2 | `start_remote_excel_polling` 创建 `asyncio.Task`，key = `remote_excel_polling_{user_id or 1}` | `services/order_watcher.py:3179`、`:3189` |
| 3 | `_excel_polling_loop` 每轮调 `_scan_business_metadata`，结束后 `asyncio.sleep(interval_seconds)` | `services/order_watcher.py:3223` |
| 4 | 用 `BusinessSessionLocal`（LATIN1）读 `ard_dataset_metadata`，`ORDER BY push_date ASC NULLS FIRST`，`LIMIT 50` | `services/order_watcher.py:443`~`:452` |
| 5 | GBK/GB18030 文本恢复：`_decode_business_value`（`encode('latin1')` 后依次试 `utf-8`/`gb18030`） | `services/order_watcher.py:230` |
| 6 | 幂等检查：查 `ard_business_sync_states`（`image_id="__dataset__"`，status ∈ discovered/copying/processing/completed/failed）；命中则再调 `_batch_artifacts_missing` 验证 MinIO 产物是否齐全 | `services/order_watcher.py:467`~`:502`、`:585` |
| 7 | 产物缺失 → `_release_stale_business_dataset` 释放幂等状态，标记重接入 | `services/order_watcher.py:631`、`:1684` |
| 8 | 产物齐全 → `skipped += 1`，本轮不再处理 | `services/order_watcher.py:502` |
| 9 | 新数据集 → `_ingest_business_dataset` | `services/order_watcher.py:716` |
| 10 | 实体路径解析优先级：`archivedPath` 目录 → 逐 `image.entity_path` → 单个 `archivedPath` | `services/order_watcher.py:760`~`:778` |
| 11 | `_safe_entity_source` 校验路径必须落在 `BUSINESS_SHARED_ROOT` 内（防路径穿越） | `services/order_watcher.py:691` |
| 12 | 按 `MAX_FILES_PER_BATCH` 分片复制到 `DATASET_STORAGE_ROOT/{dataset_id}/...` | `services/order_watcher.py:831`~`:839` |
| 13 | 幂等身份：`contract_identity = dataset.cstr or dataset_id`，`version = dataset.data_version` | `services/order_watcher.py:789`~`:795` |
| 14 | `POST {REMOTE_SYSTEM_URL}/api/data/request` 请求外部系统把数据投递到 `SHARED_DELIVERY_DIR/{batch_id}/{dataset_id}` | `services/order_watcher.py:1883`、`:2977` |
| 15 | 后续同流程 A 的 15~17 步 | — |

### 8.3 流程 C：单文件元数据提取 + COG 归档（核心，`MetadataExtractor.process`）

`services/metadata_extractor.py:711`。下表按代码顺序给出，每步带行号。

| 步 | 动作 | 行号 |
| --- | --- | --- |
| 1 | `async with _extraction_semaphore`（全局并发上限 5；信号量定义 `:57`、常量 `:56`） | `:721` |
| 2 | 生成/复用 `load_batch_id = ard-load-<uuid4().hex>` | `:724` |
| 3 | 打开独立 `AsyncSessionLocal`；订单不存在则打印并返回 `None` | `:728`~`:733` |
| 4 | 立即置 `order.status = PROCESSING` 并 commit（防静默失败仍显示"待处理"） | `:735`~`:737` |
| 5 | `_refresh_business_metadata_snapshot`：从业务库重读最新 `dataset`/`image` 快照并回写 `raw_data`/`parsed_data` | `:744`、`:255` |
| 6 | 解析 `file_path`：`s3://` 去桶名取 key；计算 `original_batch_id = order.batch_identifier or order.order_id` | `:756`~`:763` |
| 7 | 申请 CPU 槽 `quota_guard.lease_cpu_cores(user_id, required_cores=1)`；失败 → `status=FAILED` | `:782`~`:810` |
| 8 | 建本地工作目录 `/tmp/extractor_worker/user_{uid}` | `:779` |
| 9 | 本地绝对路径：存在则直接用；目录则取首个数据文件；不存在 → 尝试 MinIO 下载到本地 | `:819`~`:900` |
| 10 | `.zip` → 解压到 `<stem>_unzipped`，按 `[.tif,.tiff,.img,.nc,.nc4,.h5,.hdf,.hdf5,.shp]` 递归找**第一个**文件作为处理对象 | `:904`~`:934` |
| 11 | 计算批次对象前缀 `datas/{batch_folder}`（`batch_folder` 来自 `batch_name`/`dataset_name`/批次 ID，做 `_object_key_segment` 消毒：替换 `/`、`\`、去控制字符） | `:956`、`:226` |
| 12 | 上传 `<stem>_raw_meta.json`，得 `production_raw_meta_uri = s3://<bucket>/<key>` | `:969`~`:987` |
| 13 | 上传**原始文件**（与 COG 共存，便于追溯） | `:989`~`:994` |
| 14 | ENVI `.img` 上传 `.hdr`；Shapefile 上传 `.shx/.dbf/.prj/.cpg/.sbn/.sbx`，写入 `sidecars` | `:997`~`:1020` |
| 15 | 异构解析分流（见 8.4），产出 `archive_format` 与 `file_uri` | `:1049`~`:1252` |
| 16 | 写回 `schema_metadata["raw_meta_uri"]`、`["archive_format"]`、`["file_uri"]`、`["checksum"]` | `:1043`~`:1257` |
| 17 | 用 `_sha256_file` 对归档产物算校验和 | `:1822` |
| 18 | 查/建 `ard_partition_batches`（按 `batch_id == original_batch_id`），写 `extract_status/cog_status/load_status/overall_status = "completed"` | `:1286`~`:1318` |
| 19 | 碳数据 → `ArdPartitionObservation` 逐条插入；其他 → `ArdPartitionAsset` 逐条插入 | `:1320`~`:1368` |
| 20 | `order.parsed_data = json.dumps(schema_metadata)`；`order.status = COMPLETED` | `:1369`~`:1372` |
| 21 | upsert `ard_manifests`（key = `ard_load_batch_id`），`result="success"`、`status="completed"` | `:1374`~`:1400` |
| 22 | 删除用户桶中的**源临时对象** `minio_src_key` | `:1398`~`:1402` |
| 23 | 写 `OrderProcessingLog(action="METADATA_EXTRACTED_AND_COG")`；`user.cpu_used += 0.5`、`api_used += 1` | `:1407`~`:1416` |
| 24 | 若未传 `batch_load_batch_id`（手动单订单路径）→ 立即 `_sync_partition_schema`；否则交由批次聚合 | `:1419`~`:1424` |
| 25 | 广播 progress=100 / `COMPLETED` | `:1426`~`:1434` |
| 26 | 异常路径：置批次三状态 + `overall_status = "failed"`，订单 `FAILED`，写 `OrderProcessingLog` 与 WS 日志 | `:1444`~`:1535` |

### 8.4 流程 C 子步：异构数据解析分流

`_extract_spatial_metrics`（`services/metadata_extractor.py:439`）按扩展名分流：

| 输入 | 解析器 | 依赖 | 产出字段 | 代码位置 |
| --- | --- | --- | --- | --- |
| `.tif` `.tiff` `.img` | `_extract_raster_metrics` | `rasterio`（`HAS_RASTERIO`） | `band_count,resolution,spatial_coverage,center_coord,corners,tags,data_format=driver,parse_status` | `:461` |
| `.nc` `.nc4` `.h5` `.hdf` `.hdf5` | `_extract_netcdf_metrics` | `netCDF4`（`HAS_NETCDF`） | `tags`（NetCDF 全局属性）、`time_coverage` 等 | `:510` |
| `.shp` | `_extract_shapefile_metrics` | `geopandas`（`HAS_GEOPANDAS`） | `band_count=len(columns)`、`tags.feature_count`、`time_coverage`（自动识别含 `time/date/year/period` 的列取 min~max） | `:638` |
| 其他 / 依赖缺失 | 直接返回初值 | — | `parse_status="unsupported"`，`parse_error="解析器不可用或未安装: {suffix}"` | `:458`~`:460` |
| 解析抛异常 | 捕获后 `_apply_coordinate_fallback` 用原始 `corners` 兜底 | — | `parse_error` 截断 500 字符 | `:507`~`:509`、`:2554` |

**CRS 策略**（`services/COG.py:121` `tif_to_cog`）：

| 场景 | 行为 | 行号 |
| --- | --- | --- |
| 有 CRS 且不等于 `target_crs` | `WarpedVRT` 重投影到目标 CRS | `:157`~`:159` |
| 无 CRS 但提供了**已验证**的 `assumed_source_crs` | `WarpedVRT(src_crs=assumed, crs=assumed)` | `:160`~`:162` |
| 无 CRS 且无假设 | 抛 `ValueError("...requires an input CRS or a verified assumed source CRS")`，**绝不猜测** | `:163`~`:166` |
| `.img` 缺 `.hdr` | 抛 `FileNotFoundError("ENVI format requires .hdr header file...")` | `:143`~`:145` |

**NetCDF→COG**（`services/COG.py:57` `netcdf_to_cog`）：要求 2D 经纬度变量且维度一致，
对每个数值型同维度变量依次 `gdalwarp -geoloc -t_srs EPSG:4326 -ot Float32`（`:110`），
再 `gdalbuildvrt -separate`（`:114`），最后 `gdal_translate -of COG -co COMPRESS=LZW -co PREDICTOR=3`
（`:115`）。波段顺序 = NetCDF 变量声明顺序，并回写 `source_band_index`。

### 8.5 流程 D：回环反馈（`_send_closed_loop_feedback`）

`services/order_watcher.py:2879`。

| 步 | 动作 | 行号 |
| --- | --- | --- |
| 1 | 状态归一：`SUCCESS/COMPLETED/SUCCEEDED` → `SUCCESS`；`FAILED/FAILURE/ERROR` → `FAILED`；其他保留原值 | `:2891`~`:2898` |
| 2 | `feedback_message = json.dumps(msg[:2000], ensure_ascii=True)[1:-1][:4000]` —— **故意转义为 ASCII**，因为 LATIN1 连接写中文会 `UnicodeEncodeError` 并回滚整个 update | `:2904`~`:2908` |
| 3 | 缺 `dataset_id` → 记日志返回 `False` | `:2909`~`:2916` |
| 4 | 在 `BusinessSessionLocal` 中查 `ard_dataset_metadata`，找不到 → 返回 `False` | `:2921`~`:2940` |
| 5 | 写 `feedback_status` / `feedback_time` / `feedback_message` / `feedback_batch_id` | `:2937`~`:2940` |
| 6 | 回主库查批次显示名（失败不影响已提交的反馈） | `:2944`~`:2958` |
| 7 | 记审计日志"数据库反馈成功，数据集 X，批次 Y，状态 Z" | `:2961`~`:2966` |

**反馈触发点**：`services/order_watcher.py:2836`（成功）与 `:2870`（失败），均在
`_data_loading_pipeline` 内。此外 `:567`（`_record_business_dataset_failure`）与 `:1129`
（`_ingest_business_dataset` 收尾）也会调用。**注意**：批次聚合推送路径
（`batch_sync_to_cube`）**不**调用该函数。

### 8.6 流程 E：Cube 推送与失败重试

| 步 | 动作 | 代码位置 |
| --- | --- | --- |
| 1 | 批次全部订单完成后，`batch_sync_to_cube(batch_results, batch_uuid, user_id, load_batch_id)` | `services/metadata_extractor.py:1543` |
| 2 | 过滤 `None` 结果；按 `data_type` 分三组：`carbon` / `radar` / 其余（须 `archive_format == "cog"`，否则记日志跳过） | `:1563`~`:1581` |
| 3 | 对每组聚合 `assets` / `observations`，强制写入 `load_batch_id` | `:1587`~`:1600` |
| 4 | `_sync_partition_schema` → `_build_partition_import_payload` 校验并构造报文 | `:1692`、`:1829` |
| 5 | 报文骨架：`{schema_version, load_batch_id, batch_name, source_system="ard_loader", loaded_at, datasets:[1]}`；`dataset_id = ard-{data_type}-{load_batch_id}`，`dataset_code = ARD-{DATA_TYPE}-{load_batch_id[-12:]}` | `:1847`~`:1867` |
| 6 | 场景构造：碳 → `_build_carbon_import_scenes`（至少 1 个 observation）；其他 → `_build_cog_import_scenes`（要求 `archive_format=="cog"` 且至少 1 个 asset） | `:1949`、`:1869` |
| 7 | 硬校验：`_require_s3_uri`（必须 `s3://`）、`_require_checksum`、`_require_timestamp`、`_require_bbox`（4 元素）、`_require_coordinate`（经纬度范围） | `:2050`~`:2092` |
| 8 | `scene_key` 去重：`f"{original_scene_key}::{load_batch_id}"`，保证重载新 COG 不与旧批次冲突，同批次重试幂等 | `:1937`~`:1944` |
| 9 | `httpx.AsyncClient(timeout=10.0).post(PARTITION_SCHEMA_IMPORT_URL, json=payload)` —— **不携带 Authorization 头** | `:1700`~`:1703` |
| 10 | 成功 → `OrderProcessingLog(action="CUBE_SYNC", new_status="synced")`，返回 `True` | `:1711`~`:1755` |
| 11 | 失败（`HTTPStatusError`）→ 先拼 `error_detail = "HTTP {status}: {body[:2000]}"`（`:1752`），再记 `OrderProcessingLog(action="CUBE_SYNC_FAILED", new_status="cube_push_failed", details=f"Cube import failed: {error_detail}")`（`:1774`~`:1777`），返回 `False` | `:1749`~`:1783` |
| 12 | 失败（其他异常）→ `error_detail = "{类型名}: {exc}"`（`:1786`），同样记 `CUBE_SYNC_FAILED` / `new_status="cube_push_failed"` / `details=f"Cube import failed: {error_detail}"`（`:1809`~`:1812`） | `:1785`~`:1816` |
| 13 | 手动重试：`POST /api/ard/batches/{batch_id}/cube-sync` → `MetadataExtractor.retry_batch_sync` 从 `orders.parsed_data` 重建结果，**不重新处理源文件** | `routers/ard.py:1214`、`services/metadata_extractor.py:1657` |
| 14 | 重试结果写 `batch.overall_status = "cube_pushed"` 或 `"cube_push_failed"`；失败时 HTTP 422 + 最近一条 `CUBE_SYNC_FAILED` 的 `details` | `routers/ard.py:1226`~`:1246` |
| 15 | 前置条件：所有订单必须是 `COMPLETED`（否则 `retry_batch_sync` 无结果返回 `False`） | `services/metadata_extractor.py:1670` |

### 8.7 流程 F：失败 / 重试路径汇总

| 失败点 | 系统行为 | 代码位置 |
| --- | --- | --- |
| 幂等并发冲突（两 worker 同时通过预检） | `IntegrityError` 被捕获，按重复接入处理返回 `{"status":"skipped"}` | `services/order_watcher.py:1836`~`:1852` |
| NFS 离线路径不存在 | `FileNotFoundError(f"离线路径不存在: {local}")`，批次进入失败分支 | `services/order_watcher.py:1901` |
| MinIO 源对象缺失（陈旧批次） | 启动时 `reconcile_stale_reingest_batches` 打 `stale_reingest` 标记，`GET /batches` 过滤掉 | `main.py:282`、`services/order_watcher.py:1684`、`routers/ard.py:990` |
| 清单里某订单状态不允许提取 | `POST /extract/{id}` 返回 400，`POST /extract/batch` 记入 `failed_details` | `routers/ard.py:417`、`:389`、`:393` |
| 队列批次被调度器领取后再调优先级 | HTTP 409 `"批次已被调度器领取，不能再调整优先级"` | `routers/ard.py:1173` |
| 已结束批次调整优先级 | HTTP 409 `"已结束批次不能调整优先级"` | `routers/ard.py:1171` |
| Cube 删除需要凭证但未提供 | `RuntimeError("Cube 载入批次物理删除需要当前登录凭证")` | `services/cube_sync.py:149` |
| Cube 批次删除 404 | 自动回落调 `/archive`；archive 也 404 则视为已删除继续 | `services/cube_sync.py:165`~`:180` |
| 订单批量删除的 Cube 清理失败 | 批次级/订单级删除与陈旧重复批次归档都在 `routers/orders.py` 内联实现；单条异常被捕获 → 记入 `failed[]`，不阻断其余删除 | `routers/orders.py:648`、`:660`、`:812`、`:837`~`:840` |
| 存储超配额 | **无 Python 层拦截**：`services/quota_guard.py:28` `enforce_storage_limit` 全仓 0 调用点；实际限额由 MinIO 桶硬配额（`services/minio_service.py:34`）在写入时拒绝 | `services/quota_guard.py:28`、`services/minio_service.py:34` |
| 批次批量删除中单个失败 | 捕获异常 → `rollback()` → 记入 `failed[]`，继续下一个 | `routers/ard.py:919`~`:922` |
| 批次删除时 Cube 推送成功但本地已有剖分产物 | 启动对账 `reconcile_stale_reingest_batches` 打 `stale_reingest`，`GET /batches` 过滤掉 | `main.py:282`、`services/order_watcher.py:1700`、`routers/ard.py:990`~`:991` |
| `_dispatch_loop` 内异常 | 记日志 + `_mark_batch_failed(batch.batch_id)`；循环不退出 | `services/order_watcher.py:333`~`:340` |

---

## 9. 失败模式与已知问题

| 编号 | 现象 | 触发条件 | 代码位置 | 系统行为 | 规避方式 |
| --- | --- | --- | --- | --- | --- |
| **F-1** | 前端"删除已完成资产"按钮必失败（404） | 任何点击 | 前端 `frontend/src/views/ARD.vue:1483` 调 `/ard/completed-batch/{id}`；后端路由为 `/api/ard/ard/completed-batch/{batch_id}`（`routers/ard.py:660` 的 `/ard/...` 叠加 `main.py:393` 的 `prefix="/api/ard"`） | 前端弹 `删除失败: Not Found`；数据未删除 | 前端改调 `/ard/ard/completed-batch/{id}`（或后端把 `routers/ard.py:660` 的路径前缀去掉 `/ard`）；实测对照：`GET /api/ard/completed-batch/xxx` → 404，`GET /api/ard/ard/completed-batch/xxx` → 405（证明仅双前缀版本注册成功） |
| **F-2** | API 配额熔断永不生效 | 任何请求 | `main.py:360`：`if not path.startswith("/api/ard") or path == "/api/ard/listen/status" or path.startswith("/api"): return await call_next(request)`。第三个条件 `path.startswith("/api")` 覆盖全部 `/api/*`，前两个条件恒被吞掉 | `quota_guard.check_and_incr_api`（`services/quota_guard.py:8`）成为死代码，`api_used` 不再经此累加 | 删除第三个 `or path.startswith("/api")` 子句；注意 `services/metadata_extractor.py:1416` 另有一处 `api_used += 1` 计费路径仍然有效 |
| **F-3** | 前端进度/日志 WS 事件可能收不到 | `order_watcher.register_callback` 在 `routers/orders.py:854` 模块导入时注册，而 `order_change_callback` 定义在同文件 `:519` | `routers/orders.py:854`、`:519` | 正常；但若 `routers/orders.py` 未被导入，或 `_send_step`/`_send_log`（`services/order_watcher.py:3468`、`:3155`）在 WS 尚未连接时触发，事件丢弃（无缓冲/重放） | 前端可轮询 `GET /api/ard/batches` + `GET /api/ard/processing-logs` 兜底（`ARD.vue:1047`、`:1907` 已实现） |
| **F-4** | `ConnectionManager` 双实现，签名不兼容 | 误 import 错的模块 | `services/websocket_manager.py:4` 的 `connect(self, user_id, websocket, user_info=None)` vs `routers/orders.py:56` 的 `connect(self, websocket, user_info)` | 用错会抛 `TypeError: connect() missing 1 required positional argument` | 当前 `orders.py` 用本地类（`routers/orders.py:876` 传 `(websocket, user_info=...)` 自洽），`order_mgmt.py` 用 services 版（`routers/order_mgmt.py:901` 传 `(user_id, websocket, user_info=...)`）；重构时须统一 |
| **F-5** | 启动迁移失败直接阻断服务启动 | 存量库有重复 `source_dataset_id`/`source_fingerprint` 或列类型冲突 | `main.py:124`、`main.py:187` 重抛（两处 `except` 先打印再 `raise`）；对边 `migrate_operator_columns`（`main.py:23`）只打印不重抛 | `lifespan` 中 `await init_database()`（`main.py:281`）抛异常 → 应用启动失败 | 迁移脚本先清理重复键；同时确认 `users.role` 的存量枚举是 `ADMIN` 还是 `管理员` |
| **F-6** | `services/sitecustomize.py` 是侵入式探针 | 仅当 `services/` 目录进入 `sys.path` 且 Python 启动时自动导入 `sitecustomize` 时生效；当前仓库无任何模块 `import sitecustomize`（`grep -rn sitecustomize backend/` 只命中文件自身），后端以 cwd=`backend/app` 启动（`start-tmux-services.sh:9`），该目录下并无 `sitecustomize.py` | `services/sitecustomize.py:1`~`:206`；通过劫持 `ARD_PARTITION_METHOD`/`ARD_QA_METHOD`/`ARD_STORAGE_METHOD` 指定方法（`bind_aop_hooks:128`~`:191`），并向 `ARD_MONITOR_SERVER` 上报**硬编码伪造指标**（默认 `TOTAL_TILES = 100` `:27`、`TILE_SIZE_MB = 0.25` `:29`）。配置文件路径是 `JSON_CONFIG_PATH = Path("./ard_probe_config.json").resolve()`（`:18`）——**按进程 CWD 解析，不是模块目录**；只有 CWD 下无该文件才回落到 `ARD_*` 环境变量（`:64`~`:78`） | **已落盘事实**：`backend/app/services/ard_probe_config.json` 确实存在（526 B），内容为 `target_module=prod_tiler`、`target_class=RemoteSensingTilerEngine`、`ard_monitor_server=http://10.136.1.14:6000`、`batch_id=TASK_GF2_20260523_001`、`total_tiles=850`、`single_tile_size_mb=0.25`。但按当前启动 cwd，生效路径是 `backend/app/ard_probe_config.json`（**不存在**），所以走环境变量分支；未设 `ARD_*` 时 `TARGET_MODULE`/`TARGET_CLASS` 为空，`IS_AGENT_ACTIVE = bool(...)`（`:80`）为 False，不劫持。 | 确认实际启动 cwd 与 `sys.path`；若探针真的生效，上报目标是 `10.136.1.14:6000`（另一消费方），不是本 ARD 后端；不需要监控时移除该文件或确保不被 Python 自动加载 |
| **F-7** | 雷达数据 `cog_status` 语义混乱 | Sentinel-1 `.nc` 走 `archive_format="raw"` | `services/metadata_extractor.py:1294`：`cog_status = "completed" if cog_success else ("n/a" if data_type=="carbon" else "failed")`。radar 的 `archive_format` 可能是 `raw`，此时 `cog_status="failed"`，但 `batch_sync_to_cube` 仍允许 radar 推送（`:1568`） | 前端批次表显示 `cog_status=failed` 但批次实际成功 | 把 radar 也纳入 `n/a`；`routers/ard.py:1077`~`:1079` 的状态反推逻辑会在订单全完成时把 `cog_status` 覆盖为 `completed`，属临时掩盖 |
| **F-8** | ZIP 只处理第一个文件 | 上传含多个 GeoTIFF 的 `.zip` | `services/metadata_extractor.py:921`~`:941`：`extracted_files[0]` 被选中，其余文件被忽略（未上传、未建 asset） | 静默丢数据；`ArdPartitionAsset` 少条目 | 一个 zip 只装一个资产；或改造为遍历 `extracted_files` |
| **F-9** | 业务库反馈消息不可读 | 任何含中文的反馈 | `services/order_watcher.py:2904` 故意 `ensure_ascii=True` | `feedback_message` 列存的是 `\uXXXX` 转义串，需前端/消费方再次 unescape | 已由 `backend/test_closed_loop_feedback.py` 覆盖；若要可读，需把业务库连接编码改为 UTF-8 |
| **F-10** | `requirements.txt` 与实际依赖严重不符 | 照文件建环境 | `requirements.txt`（UTF-16LE）缺 `boto3/botocore`、`requests`、`httpx`、`redis`、`rasterio`、`netCDF4`、`geopandas`、`aiohttp`、`pandas`、`openpyxl`、`prometheus_client`、`pyotp`、`qrcode`、`reportlab` 共 14 行 / 15 个包；其中 `requests` 在 `routers/auth.py:17` 是模块级导入，启动即硬失败 | 后端起不来，或格式解析全走 `parse_status="unsupported"` 静默降级（`services/metadata_extractor.py:458`） | 用 conda 环境 `ard` 冻结实际依赖后再重建 `requirements.txt` |
| **F-16** | 存储配额守卫函数是死代码 | — | `services/quota_guard.py:28` `enforce_storage_limit` 全仓 **0 次调用**（`grep -rn enforce_storage_limit` 仅命中定义行）；CPU 侧守卫是活的（`services/metadata_extractor.py:789`、`:1504`、`routers/ard.py:638`、`:652`） | ARD 写入路径不做 Python 层的存储预检，超配额只能等 MinIO 桶硬限额拒绝 | 若验收要求“超存储配额返回 507”，需在实体复制前接回该函数；否则改为验证 MinIO 侧拒绝行为 |
| **F-11** | `POST /api/orders/receive-manifest` 无鉴权 | 任何网络可达方 | `routers/orders.py:440`~`:441` 无 `Depends`；仅 `services/order_watcher.py:2152` 检查 `self._running` | 监听开启时任意人可以注入任意 `items` 清单，触发下载与批次创建 | 需前置网关鉴权或改用带 token 的入口 |
| **F-12** | 前端存在 3 个后端不存在的接口封装 | 调用即 404 | `frontend/src/api/index.js:139`、`:140`、`:142` | 404 | 删除死代码 |
| **F-13** | `core/config.py` 硬编码凭据 | — | `core/config.py:71`~`:76`、`:118`~`:119`、`:174`~`:176` | 凭据随源码分发；`SECRET_KEY` 默认值未改则 JWT 可被伪造 | 迁移到环境变量 + 轮换全部密钥（本手册不记录取值） |
| **F-13b** | 首次建库自动创建固定口令的 `admin` 账号，并在 stdout 明文打印 | 空库启动 | `main.py:236`~`:262`（`get_password_hash` 传入源码字面量于 `:242`，`print` 直接回显于 `:262`） | 任何能访问服务日志的人可获得管理员权限 | 首次登录后立即改口令；上线前删除该自动建号分支（口令取值不在本手册记录） |
| **F-14** | `POST /extract/remote` 未验证即运行 | 调用该接口 | `routers/ard.py:425`~`:436` → `services/metadata_extractor.py:2816`：下载任意 URL 到临时文件后按**文本**读取（`open(tmp_path,'r',encoding='utf-8')`），再套用 `_interpret_by_product_family` | 二进制文件必然解码失败或产出垃圾元数据；`geo_properties` 全部是硬编码初值（`band_count=1, resolution=30.0, center_coord=(0,0), corners=[]`） | 视为调试用接口；生产应下线或重写 |
| **F-15** | `standalone` 的前端 `ARD.vue` 首屏依赖大量轮询 | — | `ARD.vue:1047` `fetchBatches`、`:1907` `fetchProcessingLogs` | 无自动停止条件 | 一般不构成故障 |

**仓库内已有测试记录（供参考）**：

- `backend/tests/test_cog_conversion.py`、`backend/tests/test_partition_schema_contract.py`、
  `backend/test_cube_sync.py`（31 行 / 990 B，验证 `delete_cube_load_batches` 能从 dataset_id 反推
  load_batch_id 并触发 2 次 POST）、`backend/test_closed_loop_feedback.py`（103 行 / 3250 B，验证反馈
  写入业务数据集表且批次名走本地库查询）。
- 仓内审计文档（快照根目录）：`ARD_BUSINESS_INTERFACE_260727_AUDIT.md`、
  `ARD数据载入与后台管理子系统_实施方案功能对应性复核报告_20260831.md`、
  `ARD后台管理子系统实施方案与测试大纲诊断报告_20260829.md`、
  `分析就绪数据剖分管理系统测试大纲_10160722_ARD后台管理可执行性复核报告_20260902.md`、
  `docs/LOAD_BATCH_PARTITION_STATUS.md`、`docs/COG_CONVERSION_IMPLEMENTATION.md`。
  **本手册未引用这些文档中的结论作为代码事实**，仅作为交接索引。

---

## 10. 验证方法

以下命令中，标 **[已执行]** 的是本次勘察真实运行并记录输出的；其余为**建议验证命令，未执行**。

### 10.1 服务存活与路由清点 **[已执行]**

```bash
curl -s -o /dev/null -w '%{http_code}\n' http://10.3.100.182:6000/health
# 实测输出：200
curl -s http://10.3.100.182:6000/health
# 实测输出：{"status":"healthy"}

curl -s http://10.3.100.182:6000/openapi.json | python3 -c "import json,sys;print(len(json.load(sys.stdin)['paths']))"
# 实测输出：125
```

### 10.2 鉴权边界 **[已执行]**

```bash
curl -s -o /dev/null -w '%{http_code}\n' http://10.3.100.182:6000/api/ard/listen/status
# 实测输出：401   body: {"detail":"Not authenticated"}
curl -s -o /dev/null -w '%{http_code}\n' http://10.3.100.182:6000/api/ard/batches
# 实测输出：401
```

### 10.3 复现 F-1（路由双前缀） **[已执行]**

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X GET  http://10.3.100.182:6000/api/ard/completed-batch/xxx
# 实测输出：404   → 前端调用的路径不存在
curl -s -o /dev/null -w '%{http_code}\n' -X GET  http://10.3.100.182:6000/api/ard/ard/completed-batch/xxx
# 实测输出：405   → 路由存在（仅注册了 DELETE）
curl -s http://10.3.100.182:6000/openapi.json | python3 -c \
  "import json,sys; print([p for p in json.load(sys.stdin)['paths'] if 'completed-batch' in p])"
# 实测输出：['/api/ard/ard/completed-batch/{batch_id}']
```

### 10.4 API 配额中间件短路（静态验证）**[已执行]**

```bash
sed -n '352,362p' backend/app/main.py
# 实测：第 360 行条件含 "or path.startswith(\"/api\")"，对全部 /api/* 恒真 → 直接放行
grep -rn "check_and_incr_api" backend/app
# 实测：仅 main.py:379 一处调用（在已被短路的代码块内）
```

### 10.5 带凭证的端到端离线导入 **[建议验证命令，未执行]**

> 未执行原因：勘察阶段无可用账号口令，且规范禁止修改被描述仓库与重启服务。以下命令仅作
> 接手后的自测模板，**不要把真实口令写进脚本或提交到仓库**。

```bash
# 1) 登录拿 token（口令从本地运行时配置读取，不落盘）
TOKEN=$(curl -s -X POST http://10.3.100.182:6000/api/login \
  -H 'Content-Type: application/json' \
  -d "{\"username\":\"$ARD_USER\",\"password\":\"$ARD_PASS\",\"role\":\"管理员\"}" \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["access_token"])')

# 2) 前置：把数据集放到 RAW_DATA_ROOT（离线模式读这里，不是 shared_delivery_ard）
python3 prepare_offline_data.py --dataset "<数据集名称>" --source /data/export/<目录> --dry-run

# 3) 上传离线清单
curl -s -X POST http://10.3.100.182:6000/api/orders/upload-manifest \
  -H "Authorization: Bearer $TOKEN" -F "file=@offline_manifest_optical.json" \
  | python3 -m json.tool
# 预期：{"status":"success","filename":"...","summary":{"new":N,"updated":0,"skipped":M},"scheduled_batches":["OFF..."]}
# 依据：services/order_watcher.py:2508

# 4) 观察批次（reconcile_partition_status=true 时已完成剖分的批次会被隐藏）
curl -s "http://10.3.100.182:6000/api/ard/batches?reconcile_partition_status=false" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
# 预期字段：batch_id,batch_name,data_type,extract_status,cog_status,load_status,
#          overall_status,priority,dispatch_status,queue_position,total_orders,status_counts
# 依据：routers/ard.py:1136~1159

# 5) 查处理日志（含 Cube 推送结果）
curl -s "http://10.3.100.182:6000/api/ard/processing-logs?action=CUBE_SYNC,CUBE_SYNC_FAILED&limit=20" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
# 依据：routers/ard.py:1260
```

### 10.6 数据库侧核对 **[建议验证命令，未执行]**

```bash
# 需要 OpenGauss 客户端与 DSN；DSN 从运行时环境变量读取，不写死
psql "$CUBE_WEB_POSTGRES_DSN" -c \
  "select batch_id,batch_name,data_type,status,overall_status,source_dataset_id,source_version \
   from ard_partition_batches order by id desc limit 20;"

# 确认幂等唯一索引存在
psql "$CUBE_WEB_POSTGRES_DSN" -c \
  "select indexname from pg_indexes where tablename='ard_partition_batches';"
# 预期包含：ux_ard_partition_batch_source_version、ux_ard_partition_batch_source_fingerprint
# 依据：main.py:101、main.py:106

# 回环反馈四列
psql "$CUBE_WEB_POSTGRES_DSN" -c \
  "select dataset_id,feedback_status,feedback_time,feedback_batch_id,left(feedback_message,60) \
   from ard_dataset_metadata where feedback_status is not null order by feedback_time desc limit 10;"
# 依据：models/models.py:507~510、services/order_watcher.py:2937
```

### 10.7 单元测试 **[未执行]**

```bash
cd backend && python3 -m unittest test_cube_sync.py test_closed_loop_feedback.py -v
cd backend && python3 -m pytest tests/ -v
```

**未执行原因**：快照内无 `.venv`/conda 环境，且 `requirements.txt` 缺 `boto3`/`httpx`/`redis`
等必需包（见 F-10），当前机器直接跑会 import 失败。

### 10.8 WebSocket 连通性 **[建议验证命令，未执行]**

```bash
# 需要 token；连接后应立即收到 {"type":"initial","count":N,"orders":[...]}
# 依据：routers/orders.py:879
python3 - <<'PY'
import asyncio, json, os, websockets
async def main():
    uri = f"ws://10.3.100.182:6000/api/ws/orders?token={os.environ['ARD_TOKEN']}"
    async with websockets.connect(uri) as ws:
        print(json.loads(await ws.recv())['type'])   # 预期 initial
        await ws.send("ping")
        print(await ws.recv())                       # 预期 {"type":"pong"}
asyncio.run(main())
PY
```

---

## 11. 与官方大纲的差异

覆盖范围：官方大纲 **表 19~表 24**。表号→功能对应关系依照任务给定的映射
（表19 数据载入、表20 元数据解析、表21 数据加载与回环反馈、表22 异构数据解析、
表23 元信息提取、表24 标准映射字典）。

> 说明：大纲 docx（`archive/docs/project/分析就绪数据剖分管理系统 (3).docx`）本次**未逐条
> 打开比对**。下表"大纲功能名"采用任务给出的六项名称与代码中的实际能力对照，
> 因此**不声称逐条覆盖大纲细目**。若需逐条追溯，应以 docx 表 19~表 24 的细目行逐行复核。

| 大纲表号 | 大纲功能名 | 状态 | 代码证据 | 说明 |
| --- | --- | --- | --- | --- |
| 表19 | 数据载入 | **已实现** | `routers/orders.py:440`（网络下发）、`:453`（离线上传）、`services/order_watcher.py:1176`（目录扫描）、`:427`（业务表轮询）、`:301`+`:351`（持久化优先级队列）、`:1272`（批次 ID 生成）、`main.py:282`（重启后陈旧批次对账） | 三条入口（网络 / 离线 / 定时轮询）+ 幂等键 + 跨重启调度齐全。**差异**：无"手工单文件上传并直接入批"入口；`POST /api/ard/start-processing`（`routers/ard.py:630`）虽名为"启动处理"，实为 CPU 亲和性占位，不参与数据载入。 |
| 表20 | 元数据解析 | **已实现** | `routers/ard.py:330`（待处理订单分页）、`:373`（批量）、`:404`（单个）、`:302`（manifest 列表）；`services/metadata_extractor.py:711`（主流程）、`:1374`（manifest 写入） | 与大纲一致的"清单→订单→解析→manifest"链路完整。**差异**：`POST /api/ard/extract/remote`（`routers/ard.py:425`）实现不完整（见 F-14），不构成可用的"远程解析"能力。 |
| 表21 | 数据加载与回环反馈 | **部分实现** | 加载：`services/metadata_extractor.py:969`~`:1252`（MinIO 归档）、`:1286`~`:1368`（批次/资产落库）。反馈：`services/order_watcher.py:2879`（写业务表 `feedback_*`）、`main.py:127`~`:187`（列迁移） | **回环反馈只走数据库，不走外部 HTTP 回调**——`services/order_watcher.py:2888` 注释明确"不再调用外部 HTTP 回调接口"。若大纲要求"HTTP 回调通知业务系统"，则该项**未实现**。另外 `_send_closed_loop_feedback` 只在 `_data_loading_pipeline`（`:2836` 成功、`:2870` 失败）与两个业务数据集入口（`:567`、`:1129`）被调用，批次聚合推送路径不触发。 |
| 表22 | 异构数据解析 | **已实现（依赖可选）** | `services/metadata_extractor.py:439`（分流）、`:461`（栅格）、`:510`（NetCDF/HDF5）、`:638`（Shapefile）、`:904`（ZIP）；`services/COG.py:121`（GeoTIFF→COG）、`:57`（NetCDF→COG）；`services/order_watcher.py:94`（`DATA_FILE_EXTS`） | 支持 `.tif/.tiff/.img/.nc/.nc4/.h5/.hdf/.hdf5/.shp/.zip`。**差异 1**：ZIP 只取第一个文件（F-8）。**差异 2**：`rasterio`/`netCDF4`/`geopandas` 缺失时**静默降级**为 `parse_status="unsupported"`+`parse_error`（`:458`），订单仍可能走到 `COMPLETED`，只是没有空间信息——需要人工核对 `parse_error`。 |
| 表23 | 元信息提取 | **已实现** | `services/metadata_extractor.py:63` `BUILTIN_STANDARD_METADATA_RULES`（实测 16 条，见下）、`:2583` `_interpret_by_product_family`、`:2155` `_build_compliant_schema_payload`、`:163` `format_file_size`、`:174` `_format_to_iso8601` | 16 个标准字段（按数组顺序）：`acquisition_time`、`spatial_coverage`、`center_coord`、`corners`、`cloud_cover`、`sensor`、`platform`、`band_count`、`bands`、`resolution`、`solar_elevation`、`solar_azimuth`、`viewing_zenith`、`time_coverage`、`data_format`、`metadata`（`:63`~`:160`）。**差异**：`_format_to_iso8601`（`:174`）对非法月份/日期返回 `None` 而非伪造默认时间（`:176` 注释"严禁伪造时间"），但 `resolution` 的默认值硬编码为 `30.0` 米（`services/metadata_extractor.py:2209` `res_val = meta.get("resolution", 30.0)`；规则表 `:147` 亦写"30.0 米"），属**伪造默认值**，与"不伪造时间"的策略不一致。 |
| 表24 | 标准映射字典 | **已实现** | 表 `mapping_rules`（`models/models.py:617`）；CRUD `routers/ard.py:583`/`:598`/`:616`/`:546`；源字段目录 `:462`；内置规则目录 `:533`；应用逻辑 `services/metadata_extractor.py:2746` `_apply_dictionary_mapping`、`:2681` `_lookup_mapping_source`；审计结果写回 `schema_metadata["dictionary_mappings"]`（`:2803`） | 支持 `source_field → target_field`，`rule_type="unit"` 时按 `transform_rule` 做 `×10^6`/`×0.0001`；`missed`/`skipped`/`applied` 三态审计（`:2766`、`:2775`、`:2800`）。**差异**：`models/models.py:624` 注释的 `code`、`missing` 两种 `rule_type` 无实现分支；`_lookup_mapping_source` 支持 `a.b.c` 路径与同名 key 别名（`:2686`~`:2705`），但无单位换算表之外的表达式求值。 |

### 11.1 大纲之外、代码中实际存在但不在表 19~表 24 的能力

以下功能在代码中真实存在，但按任务给出的表号映射无法归入表 19~表 24 中的一项，
特此列出，避免拼装时遗漏：

| 能力 | 代码位置 | 备注 |
| --- | --- | --- |
| Cube 载入批次删除/归档级联 | `services/cube_sync.py:120`、`routers/ard.py:660`、`:808` | 属"资产生命周期"，大纲中可能落在别处 |
| 订单批量删除的第二条 Cube 清理链 | `routers/orders.py:46`~`:49`、`:189`、`:648`、`:660`、`:812` | 用 `PARTITION_BATCH_LIST_URL` 查陈旧重复批次并归档，返回 `cube_duplicate_cleanup`，详见 4.2 |
| Cube 剖分状态反查与批次隐藏 | `routers/ard.py:249`、`:278`、`:980` | 与 ② 的接口契约 |
| 批次优先级调整 | `routers/ard.py:1145` | 调度器排序键 `priority DESC, loaded_at ASC, id ASC`（`services/order_watcher.py:355`） |
| Cube 推送失败重试 | `routers/ard.py:1214`、`services/metadata_extractor.py:1657` | 不重跑源文件 |
| `/api/storage/*` 中央存储网关 | `routers/storage.py:31`、`:135`、`:190` | 与 T1 同进程，供前端下载归档产物 |
| 瓦片 OTel 指标接收 | `routers/monitor.py:1099`~`:1100`（POST `/v1/traces`）、`:1116`（调 `file_monitor.process_standard_otel_spans`）、`services/file_watcher.py:292`、`:326` | 与 ARD 载入无直接关系，但在同一后端进程 |
| 瓦片指标 WebSocket | `routers/monitor.py:1122`~`:1139` | 每 2s 推送 heartbeat/update，前端监控大屏用 |
| Prometheus 指标聚合 | `services/metrics.py:18`（`collect_metrics`） | `prometheus_client` 未列入 requirements（见 7.2） |
| 瓦片指标上报客户端 | `services/tile_logger.py:3`（`report_tile_metric`） | 配套仓根 `TILE_METRIC_REPORTING.md`，供外部任务上报 |
| 用户资料 / 资源申请路由 | `routers/users.py:41`/`:90`/`:121`/`:177`、`routers/resource.py:12` | `/api/users`、`/api/resource`，`main.py:391`/`:395` 注册 |
| NODA OAuth 登录 | `core/config.py:118`~`:135`、`routers/auth.py` | 影响 T1 接口的鉴权来源 |

---

## 附录：勘察中的重大发现

以下 7 项会直接影响交付验收与维护判断，单独列出。全部有代码行号或运行时输出支撑，
无推测成分。

1. **前端"删除已完成资产"功能完全不可用（路径前缀重复）**。
   `routers/ard.py:660` 写成 `@router.delete("/ard/completed-batch/{batch_id}")`，而
   `main.py:393` 以 `prefix="/api/ard"` 注册 → 实际路径 `/api/ard/ard/completed-batch/{batch_id}`；
   前端 `frontend/src/views/ARD.vue:1483` 调 `/ard/completed-batch/{orderId}`（axios baseURL
   `/api`，`frontend/src/api/index.js:7`）→ `/api/ard/completed-batch/{id}`，运行时 **404**。
   批量删除入口 `/api/ard/completed-batches/batch-delete`（`routers/ard.py:808`）路径正确，
   因此**批量删除可用、单个删除不可用**。修复方式二选一：后端去掉路径里的 `/ard`，或前端
   改调双前缀路径。

2. **API 配额熔断中间件是死代码**。`main.py:360` 的守卫条件是
   `if not path.startswith("/api/ard") or path == "/api/ard/listen/status" or path.startswith("/api")`。
   第三个子句对任何 `/api/*` 恒为真，直接 `return await call_next(request)`，
   因此 `quota_guard.check_and_incr_api`（`services/quota_guard.py:8`，唯一调用点
   `main.py:379`）**永不执行**。用户 `api_quota` 不会通过该路径熔断。
   若大纲/测试用例声称"API 配额超限返回 429"，则结论不成立。

3. **回环反馈不走 HTTP 回调**。`services/order_watcher.py:2888` 的 docstring 与
   `main.py:296`~`:299` 被注释掉的 `start_remote_excel_polling` 调用共同说明：
   与外部业务系统的回环通道已从 HTTP 回调**改为直接 UPDATE 业务库的四列**
   （`feedback_status`/`feedback_time`/`feedback_message`/`feedback_batch_id`，
   `models/models.py:507`~`:510`）。且该函数只在 `_data_loading_pipeline`（`:2836` 成功、
   `:2870` 失败）、`_record_business_dataset_failure`（`:567`）与
   `_ingest_business_dataset`（`:1129`）触发；批次聚合推送成功路径
   （`services/metadata_extractor.py:1543` `batch_sync_to_cube`）**不写反馈**。若验收要求"每个批次完成后业务系统都收到通知"，当前实现不满足。

4. **`services/sitecustomize.py` 是侵入式探针，但当前启动方式下不会生效**。
   该文件通过 `os.environ.get("ARD_PARTITION_METHOD")` 等变量（`:69`~`:71`）选择要劫持的
   模块/类/方法名，并把硬编码的模拟值上报到 `ARD_MONITOR_SERVER`（默认值 `:21`，环境变量读取 `:66`）。
   伪造指标的默认常量在 `:27`（`TOTAL_TILES = 100`）与 `:29`（`TILE_SIZE_MB = 0.25`），
   且会尝试读取 `Path("./ard_probe_config.json").resolve()`（`:18`）覆盖它们——**按进程 CWD 解析**。
   已确认落盘的文件是 `backend/app/services/ard_probe_config.json`（526 B，指向
   `prod_tiler` / `RemoteSensingTilerEngine` / `http://10.136.1.14:6000` / `total_tiles=850`）；
   而后端以 cwd=`backend/app` 启动（`start-tmux-services.sh:9`），生效路径
   `backend/app/ard_probe_config.json` 并不存在，因此只能走 `:64`~`:78` 的环境变量回落分支；
   同时仓库内无任何模块 `import sitecustomize`（它只可能在 `services/` 进入 `sys.path` 时被
   Python 自动加载），所以快照部署下 `IS_AGENT_ACTIVE`（`:80`）为 False。
   同时存在 `services/metrics.py`（31 行，Prometheus 指标）与 `otel_wrapper.py`（仓根）。
   手册**未能确认远端进程的实际 cwd/sys.path 与 `ARD_*` 环境变量**，接手时**必须优先确认**，
   否则会把生产监控数据理解为真实值。

5. **`file_watcher.py` 名不符实**。485 行的 `FileMonitorService`（`:11`）是瓦片 OTel Span
   统计器（`process_standard_otel_spans:292`、`_record_monthly_counts:65`），与"文件监听"
   无关。真正的清单目录监听是 `services/order_watcher.py:1176` `_scan_listen_directory`，
   由 `_polling_loop`（`:290`）按 `POLL_INTERVAL_SECONDS`（默认 600s）驱动。
   两份"watcher"并存且语义不同，是本子系统最容易误读的地方。

6. **依赖清单不可用于重建环境**。`requirements.txt` 是 UTF-16LE 编码，且缺少
   `boto3`/`botocore`、`requests`、`httpx`、`redis`、`rasterio`、`netCDF4`、`geopandas`、
   `aiohttp`、`pandas`、`openpyxl`、`prometheus_client`、`pyotp`、`qrcode`、`reportlab`
   共 14 行 / 15 个包（`requests` 在 `routers/auth.py:17` 模块级导入，启动即硬失败）；
   同时包含 `alembic` 却无任何 alembic 目录（迁移全在 `main.py:23`~`main.py:187` 内联执行）。
   按该文件建环境会导致服务无法启动或全部格式解析静默降级。

7. **存储配额守卫是“只有函数、没有接入”的死代码**。
   `services/quota_guard.py:28` `enforce_storage_limit` 全仓 **0 次调用**
   （`grep -rn "enforce_storage_limit"` 只命中定义行；`services/order_watcher.py:2895`
   是 `_send_closed_loop_feedback` 内的 `feedback_status` 归一化表达式，与实体复制无关）。
   ARD 载入链路没有 Python 层的存储超限预检；唯一的存储侧限制是
   `services/minio_service.py:34` `set_bucket_quota` 通过 `mc quota set minio_local/<bucket> --size {}GB`
   （`:47`，`subprocess.run(shell=True)` 在 `:51`）下发的 MinIO 桶硬限额。
   若验收用例要求“超出存储配额时返回 507 并拒绝归档”，Python 层不会触发；要观察的是
   MinIO 侧的写入拒绝。CPU 侧守卫是活的（`services/metadata_extractor.py:789`、`:1504`、
   `routers/ard.py:638`、`:652`），API 计数守卫见第 2 项。

---

### 附录 B：二次校验争议记录（2026-09-12）

复核对校验者结论时发现两处校验者本身不准之处、一处口径澄清，均以代码实测为准：

1. 校验项 13 称 `self.start_dispatcher()` 在 `services/order_watcher.py:380`；实测 `:380`
   是 `_enqueue_dataset_batch` 函数体首行 `source_paths = [`，`self.start_dispatcher()`
   在 **`:384`**（原手册的 `:427` 也确为误引）。手册已按 `:379`（def）/`:384`（调用）修正。
2. 校验项 15 称 `sitecustomize.py` 的 `BACKEND_URL` 默认值在 `:22`、环境变量读取在
   `:68`~`:79`、“手册统一少 2 行”；实测默认值在 **`:21`**（`:22` 是 `TARGET_MODULE`），
   环境变量读取在 **`:66`**~**`:77`**，`:80` 是 `IS_AGENT_ACTIVE`。手册 6.2 的
   `:66` / `:67`~`:68` / `:69`~`:71` / `:72`~`:77` 引用原本正确，本次只修正“同目录读取”
   这一错误表述，并补充 `backend/app/services/ard_probe_config.json` 已落盘及其内容。
3. 校验项 3 给出的缺失清单为“至少 14 项”；按 7.2 的 AST 实抽结果为 **15 个包**，按
   手册“`boto3`/`botocore` 合并一行”的口径为 **14 行**，两者一致。

---

**文档结束。** 全文技术结论均以 `文件:行号` 或实测命令输出为依据；未能确认的点已在对应
位置显式标注“未能确认”并给出原因，未作推测填充。
