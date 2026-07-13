# 生产测试与验收基线

更新时间：2026-07-13

本文定义当前 `master` 的生产验收范围。历史性能数字见 `cube_split/docs/*PERFORMANCE*.md`，不得替代本清单。

## 1. 验收范围

- `cube_encoder`：`s2`、`mgrs`、`tile_matrix`、`isea4h` 的 locate/cover/topology/ST code；`plane_grid` 只验 ST code 格式。
- `cube_split`：光学、雷达、产品、碳卫星的剖分、OpenGauss/MinIO 入库、质量检查和读取。
- `cube_web`：FastAPI、Vue/Vite、认证、托管批次、attempt、重试/取消/归档、质量报告。

生产操作使用 `run`。`demo` endpoint 只验证兼容性，生产启动默认不得 seed 演示批次。

## 2. 自动化回归

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest
```

Web 变更额外运行：

```bash
cd cube_web
PYTHONPATH=../cube_encoder:../cube_split:. python3.11 -m pytest tests
```

前端变更运行：

```bash
cd cube_web/frontend
npm run build
```

encoder 发布变更运行：

```bash
cd cube_encoder
python3.11 -m build
```

验收要求：测试和构建退出码为 0；已知 warning 必须记录，不得隐藏失败。

## 3. 运行时配置

运行时解析顺序：进程环境变量、`CUBE_WEB_ENV_FILE`、本地 `.cube_web.env`、代码默认值。

必须验证：

- OpenGauss 使用 `CUBE_WEB_POSTGRES_DSN` 的 PostgreSQL 兼容 DSN，通过 `psycopg` 连接。
- Ray 使用 `CUBE_WEB_RAY_ADDRESS`，真实分布式验收不能用 thread/process 代替。
- MinIO 使用 `CUBE_WEB_MINIO_*`，源数据 URI 为 `s3://cube/cube/source/...`。
- `cube_web_configs` 只保存 `partition`、`ingest`、`quality`，不保存 DSN、Ray、MinIO、门户 URL 或凭据。
- `.cube_web.env`、明文密钥、本地绝对源数据路径不进入 Git。

## 4. 认证与权限

- `CUBE_WEB_AUTH_REQUIRED=0` 时，本地自测可匿名访问。
- `CUBE_WEB_AUTH_REQUIRED=1` 时，除 `POST /v1/partition/schemas/import` 外的 `/v1/*` 默认要求 Bearer Token。
- 导入接口只登记批次和资产，不启动运行。
- 非管理员前端只显示公共编码入口；直接进入剖分页返回门户首页。
- 前端隐藏不替代后端鉴权，敏感接口仍需服务端验证。

## 5. 数据交付

载入系统通过 `POST /v1/partition/schemas/import` 交付：

- `optical`、`radar`、`product` 使用非空 `assets`。
- `carbon` 使用非空 `observations`。
- 栅格资产至少包含 `source_uri`、稳定 ID、`acq_time`、产品/传感器、分辨率、波段和 WGS84 四角点。
- 重复导入相同 `batch_id` 必须幂等更新，不产生重复资产。

## 6. 剖分矩阵

| 方式 | 支持格网 | 默认层级 |
| --- | --- | --- |
| logical | `s2`、`tile_matrix`、`isea4h` | 5 或按分辨率推导 |
| entity | `s2`、`tile_matrix`、`isea4h` | 6；ISEA4H 可自动推导 |
| logical（实验） | `plane_grid` | 5；保持源 CRS |

要求：

- `mgrs` 不出现在 Web 生产剖分选择中。
- `plane_grid` 必须使用空 `target_crs`，不得选择 entity。
- `max_cells_per_asset=0` 表示不设上限；smoke 使用显式小正数。
- Ray worker 从 MinIO 下载源对象到本地缓存并在 worker 侧转 COG，不读取 driver 的 `/tmp/.../cog/*.tif`。
- 输出 URI 使用 `s3://`，质检读取对象前先解析为本地缓存或使用对象存储兼容读取。

`plane_grid` 当前不作为生产上线阻断项：跨场景编码唯一性、native/WGS84 bbox 分离、质量检查和地图预览将在下一轮整体重构中验收。现阶段只允许单资产实验验证，并在结果中明确标注。

## 7. 运行、入库和状态

- 生产 `run` 必须提供 schema/manifest/selected assets 或明确输入目录，不能隐式读取 demo 数据。
- 逻辑剖分输出 `index_rows.jsonl`；实体剖分输出实体瓦片和元数据；碳链路输出观测事实。
- OpenGauss 写入使用业务唯一键幂等 MERGE/upsert，MinIO 对象可 stat/download。
- 成功 attempt 记录 runner result、run dir、行数、对象数和 ingest 状态。
- 同一批次同一槽位已成功时，重复运行返回冲突。
- queued/running/retrying/cancel_requested 任务可取消；失败和人工确认批次可重试或归档。

## 8. 质量检查

- 光学/雷达：schema、时间桶、bbox/window、重复、资产可读性、CRS、像素样本。
- 产品：光学通用检查加产品年份。
- 碳：schema、坐标、XCO2 范围、质量标记、重复和 footprint。
- `PASS`/`WARN` 保存报告；`FAIL` 将托管批次置为 `manual_required` 并保存失败摘要。
- Web 报告写入 OpenGauss `quality_reports`，latest/history/report/pdf/txt 均从数据库读取。

## 9. 集群 smoke

每类数据至少选一个小样本，通过真实 Ray、MinIO、OpenGauss 跑通：

```text
schema import -> run -> worker COG/tile -> MinIO -> OpenGauss -> quality -> Web report
```

建议 ISEA4H smoke：单景、`grid_level=1`、`ray_parallelism=2`、`max_cells_per_asset=50`。记录 Ray job、MinIO prefix、OpenGauss 行数、`job_report.json` 和 `quality_report_id`。

## 10. 故障验收

至少覆盖：MinIO 对象缺失/凭据错误、OpenGauss DSN 缺失、Ray 不可达、schema 非法、格网数超限、质量 FAIL、运行中取消。

验收要求：错误可读；attempt/batch/asset 状态一致；失败不写成功结果；重试幂等；取消停止继续提交工作。

## 11. 发布阻断

以下任一项阻断发布：

- 全量 pytest 或前端构建失败。
- 真实 Ray smoke 只在本地后端通过。
- 生产运行依赖 demo seed、节点本地源路径或 driver `/tmp` COG。
- OpenGauss、MinIO 和 job report 无法对账。
- 运行时配置或凭据被写入业务配置表/Git。
- 核心数据类型输出 0 行或质量 FAIL 未进入人工处理。
- 重试、取消、归档或报告读取状态不一致。

## 12. 交付证据

- 测试与构建命令及结果。
- 真实集群 smoke summary。
- 每类数据的 `job_report.json`、MinIO prefix、OpenGauss 对账结果。
- `quality_report.json` 或 `quality_reports.report_id`。
- 配置页、批次列表/详情、运行结果、质量历史截图。
