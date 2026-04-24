# 入库与存储设计归档

更新时间：2026-04-24  
适用范围：历史设计归档，说明上层入库系统如何消费 `cube_encoder` 能力。

## 1. 文档定位

本文件合并并压缩以下历史方案：

- Ray + 对象存储 + 湖表的无景化入库方案。
- 最小 PostgreSQL DDL 与 Ray 入库接口契约。
- OSS + Iceberg 的 PB 级扩展方案。
- Spark + COG + 时空格网索引技术报告。

当前工程执行细节以 [cube_split/docs/README.md](../../cube_split/docs/README.md) 为准。本文件保留架构原则、表模型和扩展路径，避免多份方案重复描述。

## 2. 核心目标

入库系统的目标是把景级遥感数据组织为可按“格网单元 + 时间 + 波段”查询的事实层：

```text
原始景级 TIF
  -> COG 标准化
  -> cube_encoder 计算格网覆盖和时空编码
  -> 生成 cell 级索引行
  -> 写入元数据/事实表
  -> AOI 查询按 space_code + time_bucket + band 回源读取
```

关键原则：

- 对外查询不以 `scene_id` 为主键。
- 景级数据保留为 Raw 层，用于血缘、审计和重算。
- `space_code` 是空间索引键，不只是展示编码。
- `value_ref_uri#window=` 是索引层连接 COG 数据层的读取桥梁。

## 3. 分层模型

推荐分三层：

```text
Raw 层
  保存原始景级 COG、STAC/元数据和血缘信息。

Cube 层
  保存 cell-time-band 事实，作为对外读取主表。

Control 层
  保存任务状态、参数、统计、错误和输出快照。
```

对象存储路径建议：

```text
s3://<bucket>/rs_cube/raw/
  dataset=<dataset>/sensor=<sensor>/acq_date=YYYY/MM/DD/scene_id=<scene_id>/version=<v>/*.tif

s3://<bucket>/rs_cube/cube/
  grid_type=<grid>/grid_level=<level>/time_bucket=<tb>/band=<band>/part-*.parquet

s3://<bucket>/rs_cube/staging/
  run_id=<run_id>/part-*.parquet
```

## 4. 最小表模型

`rs_raw_scene_asset` 保存景级资产元数据，唯一键为：

```text
scene_id + band + version
```

核心字段：

- `dataset`, `sensor`, `scene_id`, `band`, `acq_time`
- `raw_cog_uri`, `checksum`, `crs`, `resolution`
- `bbox`, `cloud_cover`, `quality_score`
- `version`, `run_id`, `ingest_time`, `metadata_json`

`rs_cube_cell_fact` 是对外查询主表，业务唯一键为：

```text
grid_type + grid_level + space_code + time_bucket + band + cube_version
```

核心字段：

- `grid_type`, `grid_level`, `space_code`, `time_bucket`, `band`
- `st_code`
- `cell_min_lon`, `cell_min_lat`, `cell_max_lon`, `cell_max_lat`
- `value_ref_uri`
- `source_scene_count`, `provenance_json`
- `quality_rule`, `cube_version`, `run_id`, `ingest_time`

`rs_ingest_job` 保存任务控制信息：

- `job_id`, `status`, `params_json`, `stats_json`
- `error_msg`, `retry_count`
- `started_at`, `finished_at`, `output_snapshot`

## 5. 幂等与版本

写入规则：

- Raw 层按 `scene_id + band + version` upsert。
- Cube 层按 `grid_type + grid_level + space_code + time_bucket + band + cube_version` upsert。
- 同一 `job_id` 失败重跑时允许 `run_id` 变化，但业务事实不重复。

版本规则：

- Raw 使用 `version` 表示景级资产版本。
- Cube 使用 `cube_version` 表示融合事实版本。
- 历史版本不覆盖，查询默认读最新激活版本。

## 6. 融合策略

V1 推荐固定 `best_quality_wins`，避免结果不可解释。

排序优先级：

1. `cloud_cover` 升序。
2. `acq_time` 降序。
3. `sensor_priority` 降序。
4. `scene_id` 字典序，作为稳定 tie-break。

每条 cube 事实必须保留：

- `source_scene_count`
- `provenance_json`
- `quality_rule`

## 7. 计算与提交模式

中小规模或当前工程阶段：

```text
Ray worker
  -> 生成/校验 COG
  -> 调用 cube_encoder SDK 计算覆盖
  -> 生成索引行
  -> 批量写 PostgreSQL 和对象存储
```

PB 级扩展阶段：

```text
Ray workers 并行计算并写 staging Parquet
  -> Spark/Flink 协调器统一提交 Iceberg
  -> Iceberg 管理快照、增量写、回溯和 compaction
```

不建议每个 Ray worker 直接并发提交 Iceberg 元数据，容易引入 commit 冲突和小文件问题。

## 8. 验收口径

最小闭环验收：

- 同一 AOI + 时间 + band 查询不暴露 `scene_id` 主键依赖。
- `rs_cube_cell_fact` 中每条事实具备 `value_ref_uri` 和 `provenance_json`。
- 重跑同一批数据不会产生重复业务事实。
- AOI 能解析为 `space_code[]`，并按 `value_ref_uri#window=` 从 COG 回源读取。

历史长文档保存在 `docs/archive/`，仅用于追溯背景。
