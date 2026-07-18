# 生产测试与验收基线

更新时间：2026-07-16

本文定义当前 `master` 的生产验收范围。历史性能数字只作为已发生的测量记录，不得替代本清单。

## 1. 当前契约

Current production grid contract: `geohash` and `mgrs` use logical partitioning; `isea4h` uses entity partitioning. Native levels are Geohash `1..12`, MGRS `0..5`, and ISEA4H `0..15`.

请求使用 `requested_grid_level`。返回的 cell 保留实际 `grid_level`；`minimal` cover 可以返回不同于请求层级的 cell。ISEA4H 的 `space_code` 是未补零的十进制 DGGRID SEQNUM，且 `cell_count(r) = 10 * 4**r + 2`。运行时不依赖 H3 或 DGGRID。

正式请求只消费 loader 已交付的完整 `DatasetInput`。同一批次可以包含不同 `data_type`，每个数据集通过 `datasets[].partition` 独立选择格网、层级和剖分参数。光学、雷达和信息产品资产使用非空 COG `cog_uri`；碳卫星资产保留原始 `source_uri`，格式为 NetCDF/HDF5（包括 `.nc4`、`.hdf5`），不要求也不生成 TIFF/COG。所有数据集提供数据集级 `bands`，Ray worker 只通过生产缓存路径读取 MinIO 中可 stat 的输入对象。

发布记录生命周期只能是 `publishing|active|withdrawing|failed|withdrawn`。数据集派生发布状态只能是 `unpublished|publishing|active|withdrawing|failed|withdrawn`。旧的终态标签不得在生产接口、当前文档、验收结果或证据中使用。

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
npm run test:unit
npm run build
npx playwright test
```

验收要求：命令退出码为 0；真实门禁没有 skip、deselection、xfail、mock 或 fallback。

## 3. 运行时与认证

- OpenGauss 通过 `CUBE_WEB_POSTGRES_DSN` 的 PostgreSQL 兼容 DSN 使用 `psycopg` 连接。
- Ray 使用 `CUBE_WEB_RAY_ADDRESS`；真实分布式验收不能用本地执行器代替。
- MinIO 使用 `CUBE_WEB_MINIO_*`；输入对象为可 stat 的 `s3://cube/cube/source/...` URI。
- `cube_web_configs` 只保存 `partition`、`ingest`、`quality`；不保存运行时端点或凭据。
- `.cube_web.env`、明文密钥、本地绝对数据路径和真实数据不进入 Git。
- `CUBE_WEB_AUTH_REQUIRED=0` 仅用于受控本地测试。启用认证时，除公开 schema import 入口外，`/v1/*` 默认要求 Bearer Token。

## 4. 数据、质量与发布验收

真实验收仅使用 MinIO 中经审阅、可读取的 loader 输入：非碳数据使用 COG，碳卫星使用原始 NetCDF/HDF5。每个场景先验证对象存在，再由 Ray worker 经生产缓存读取。不得在验收中生成输入、写入 source 前缀或修复缺失对象。

真实场景必须覆盖：Geohash logical、MGRS 跨区 logical、ISEA4H entity、四类产品、一个批次的多个数据集与多景、取消和重试、质量 Pass/Warn/Fail、CSV/JSON 全量和过滤导出、入库，以及 active-to-withdrawn 发布历史。

正式真实源验收入口：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web \
python3.11 cube_web/scripts/run_real_partition_acceptance.py \
  --manifest /tmp/cube-real-acceptance-prepared.json
```

该入口必须连接真实 Web API、OpenGauss、MinIO 和 Ray。浏览器验收必须点击真实页面并访问
真实后端，不得用 Playwright 路由拦截结果替代完整链路。

质量导出中，CSV/JSON 的全量与过滤计数必须和相同条件下的 OpenGauss count 一致。发布验收只检查精确记录的 OpenGauss 状态及规范 API 可见性；本仓库没有外部发布网关。

## 5. 交付证据

每次完整验收保存脱敏命令摘要、退出码、计数、SHA-256 摘要、真实输入对象的匿名标识和场景结果。证据不得包含凭据或本地绝对路径。
