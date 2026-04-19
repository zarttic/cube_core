# OSS + Iceberg 实施方案（遥感影像逻辑剖分，PB 级）

更新时间：2026-04-19  
适用项目：`cube_encoder` 并行化逻辑剖分与在线任务计算场景

---

## 1. 目标与范围

### 1.1 目标

- 建立“索引与数据分离”的 PB 级可扩展架构。
- 支持基于 COG 的按需读取与后台在线计算任务。
- 保证可重跑、可回溯、可增量写入。

### 1.2 范围

- `Ray 并行剖分 -> OSS staging -> Iceberg 入湖 -> 在线任务检索 -> COG 按需读取 -> 结果回写 OSS`

### 1.3 非目标

- 毫秒级强实时检索。
- 一次性引入高复杂度组件（如全量 HBase 主存）。

---

## 2. 总体架构

### 2.1 分层

1. 数据层：`OSS`
- 存 COG 原始影像、索引 Parquet、结果文件。

2. 索引真相层：`Iceberg`
- 管理全量索引表，提供 ACID、快照、增量写、回溯。

3. 控制层：`PostgreSQL`
- 存任务参数、状态、结果引用，不存 PB 级明细。

4. 计算层：`Ray + Spark/Flink`
- `Ray` 负责并行剖分/并行计算。
- `Spark/Flink` 负责 Iceberg 事务提交（协调器角色）。

### 2.2 设计原则

- 计算与提交解耦：Ray 负责产出，协调器负责入湖提交。
- 全量真相在 Iceberg，OSS 承载大文件。
- 先保证稳定与可维护，再逐步做热点加速。

---

## 3. OSS 目录规范

```text
oss://<bucket>/rs_cube/
  cog/
    scene_date=YYYYMMDD/scene_id=.../*.tif
  staging/
    grid_index/run_id=<run_id>/part-*.parquet
    compute_result/job_id=<job_id>/part-*.parquet
  warehouse/
    iceberg/rs_image_catalog/...
    iceberg/rs_grid_index/...
    iceberg/rs_job_result_ref/...
  result/
    job_id=<job_id>/...
```

---

## 4. Iceberg 表设计

## 4.1 影像资产目录表 `rs_image_catalog`

建议字段：

- `image_id` STRING
- `scene_id` STRING
- `band` STRING
- `acq_time` TIMESTAMP
- `bbox` STRING/STRUCT
- `cog_uri` STRING
- `cloud_cover` DOUBLE
- `qc_flag` STRING
- `ingest_time` TIMESTAMP

用途：

- 维护影像级元数据与 COG 路径映射。

## 4.2 逻辑剖分索引主表 `rs_grid_index`

建议字段（与当前作业输出对齐）：

- `image_id` STRING
- `scene_id` STRING
- `band` STRING
- `acq_time` TIMESTAMP
- `grid_type` STRING
- `grid_level` INT
- `space_code` STRING
- `space_code_prefix` STRING
- `time_bucket` STRING
- `st_code` STRING
- `cover_mode` STRING
- `cell_min_lon` DOUBLE
- `cell_min_lat` DOUBLE
- `cell_max_lon` DOUBLE
- `cell_max_lat` DOUBLE
- `window_col_off` INT
- `window_row_off` INT
- `window_width` INT
- `window_height` INT
- `sample_mean_band1` DOUBLE
- `run_id` STRING
- `ingest_time` TIMESTAMP

分区建议：

- `time_bucket`、`grid_type`

排序建议：

- `space_code`、`acq_time`

## 4.3 任务结果引用表 `rs_job_result_ref`

建议字段：

- `job_id` STRING
- `status` STRING
- `result_uri` STRING
- `summary_json` STRING
- `created_at` TIMESTAMP
- `finished_at` TIMESTAMP
- `retry_count` INT

---

## 5. 唯一键与幂等策略

建议业务唯一键：

- `image_id + band + grid_type + grid_level + space_code + time_bucket`

策略：

- staging 允许重复写（便于失败重试）。
- 入湖使用 `MERGE` 去重。
- 任务重跑不产生重复业务记录。

---

## 6. Ray 并行写 OSS 与分布式设计

## 6.1 结论

- Ray 并行写 OSS 可行且适合本场景。
- 不建议每个 Ray worker 直接并发提交 Iceberg 元数据。

## 6.2 推荐两阶段写入

1. 阶段 A：`Ray workers` 并行计算并写 `staging Parquet` 到 OSS。  
2. 阶段 B：`Spark/Flink 协调器` 批量读取 staging，统一 `MERGE/APPEND` 到 Iceberg。

## 6.3 分布式关键点

1. 小文件控制
- 目标单文件大小建议 `256MB~512MB`。
- worker 侧做批量缓冲，避免大量小文件。

2. 提交冲突控制
- 仅协调器提交 Iceberg 元数据，避免高并发 commit 冲突。

3. 分区与并行粒度
- 延续当前 `space_code_prefix + time_bucket` 作为主要并行键。
- 热点前缀可增加 `salt` 打散。

4. 失败恢复
- 用 `run_id/job_id` 目录隔离，支持失败批次重放。

5. 生命周期治理
- staging 设置短 TTL（例如 7-14 天）。
- 定期执行 compaction 与 manifest 优化。

---

## 7. 在线计算任务执行流

1. 提交任务：写 `PostgreSQL job_task`。
2. 空间离散：由 `cover` 得到 `space_code[]`。
3. 候选筛选：按 `space_code + time_bucket` 查询 Iceberg `rs_grid_index`。
4. 后台计算：并行读取 `cog_uri + window_*` 做按需计算。
5. 结果落盘：写 OSS `result/job_id=...`。
6. 状态回写：更新 `PostgreSQL` 状态和结果引用。

---

## 8. 运维与监控基线

建议重点监控：

- 入湖提交耗时与失败率
- Iceberg 提交冲突率
- 小文件数量与平均文件大小
- 查询裁剪率（扫描文件数/命中文件数）
- 任务端到端耗时（排队、筛选、计算、写回）

建议定期任务：

- 每日/每批次 compact
- 每日 manifest 优化
- 过期 staging 清理

---

## 9. 分阶段落地计划

## Phase A（1-2 周）

- 完成 OSS 路径规范。
- 创建 3 张核心 Iceberg 表。
- 打通 `Ray -> staging -> Spark merge` 主链路。
- 完成幂等键与失败重试。

## Phase B（1-2 周）

- 接入在线任务控制面（PostgreSQL）。
- 打通任务筛选、COG 按需读取、结果回写。
- 完成基础监控与告警。

## Phase C（持续优化）

- 自动 compaction 与写入优化。
- 查询性能调优（分区/排序/裁剪）。
- 如并发提升明显，再评估引入 HBase 热索引层。

---

## 10. 风险与应对

1. 风险：并发提交 Iceberg 发生冲突  
应对：统一由协调器提交，Ray 只写 staging。

2. 风险：PB 级小文件导致查询性能退化  
应对：写入文件大小控制 + 周期性 compact。

3. 风险：重跑导致重复数据  
应对：业务唯一键 + `MERGE` 幂等写入。

4. 风险：长期运行元数据膨胀  
应对：定期 manifest 优化与快照治理。

---

## 11. 结论

对于“遥感逻辑剖分 + COG 按需读取 + 后台在线计算 + PB 级数据量”的场景，推荐主路径为：

- `OSS` 承载数据文件
- `Iceberg` 承载全量索引真相
- `PostgreSQL` 承载任务控制面
- `Ray` 负责并行计算，`Spark/Flink` 负责入湖提交

该方案在复杂度、可维护性、扩展性之间更平衡，适合科研项目先落地、后优化。
