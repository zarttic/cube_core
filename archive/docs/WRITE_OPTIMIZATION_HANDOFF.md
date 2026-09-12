# 写入路径优化与真实剖分入库测试 Handoff

> 状态更新：2026-09-11 晚。目标（用户 goal）：实现推荐的写入优化，并**必须做真实剖分入库测试**（KubeRay 预热时间不算入库时间）。

## 一、当前状态总览

| 事项 | 状态 |
|---|---|
| 代码优化（5 文件） | ✅ 已完成，单测通过，全量 pytest 待最终确认 |
| 代码提交 | ⏳ 未提交（工作区 5 个文件 Modified） |
| OpenGauss 全局参数调优 | ✅ 已生效（主备两节点，7 项） |
| DB 维护（TRUNCATE/VACUUM） | ✅ 已完成（staging 3.59GB→0，库 7.9→4.32GB） |
| **真实剖分测试** | ❌ 未执行（下一步最关键） |
| **真实入库测试** | ❌ 未执行 |
| Linear 事项 | ⏳ 建议新建（如 ZAR-9x「剖分/入库写入路径优化」）记录提交与测试结果 |

## 二、已完成的代码改动（未提交）

全部通过对应单测（57 passed：test_ray_ingest_job / test_managed_output_ingest / test_product_workflow / test_partition_domain_store；此前 cube_web 域存储相关 107 passed）。

### 1. `cube_web/cube_web/services/partition_domain_store.py`

- **`_merge_insert_many` → `_copy_insert_many`**：批量写入改为「CTAS 建临时表（类型取自目标表）+ COPY 写入 + 单条 `INSERT ... SELECT ... WHERE NOT EXISTS`」。`complete_output` 的批量阈值 500→5000。
- **`promote_logical_staging`**：3 个逐批 JSONB `MERGE` 改为 3 个集合式
  `INSERT ... SELECT DISTINCT ON (output_id) ... FROM staging WHERE NOT EXISTS(...) ORDER BY output_id, chunk_id, row_number`（保持原幂等/去重语义）。
- **`ensure_schema`**：加 `_domain_schema_current` 版本预检 + 实例级 `_schema_applied` 缓存，schema 已是最新时不再跑全量 DDL。

### 2. `cube_web/cube_web/services/partition_job_store.py`

- `PostgresPartitionJobStore.ensure_schema` 增加 `_schema_ready` 预检（9 个列标记 + 4 个索引标记 + domain schema 版本，单条 SELECT），已就绪则跳过 9×ALTER + 4×CREATE INDEX + apply_schema——针对历史上 `driver.bootstrap` 102.8s（DDL 锁等待）的修复。

### 3. `cube_split/cube_split/ingest/ray_ingest_job.py`

- `upsert_raw_assets_postgres` / `upsert_cube_facts_postgres`：改为「COPY 到类型化临时表（`_copy_rows_to_temp`）→ 单条 `MERGE FROM (SELECT DISTINCT ON (keys) * FROM temp)`」，替代逐 1000 行 VALUES MERGE。
- 新增辅助：`_copy_rows_to_temp` / `_deduplicated_temp_source` / `_drop_temp_table`；`_batches`/`_flatten`/`_postgres_values_source` 保留（product_ingest_job 仍在用）。

### 4. 测试同步更新

- `cube_web/tests/test_partition_domain_store.py`：monkeypatch 目标改名 `_copy_insert_many`。
- `cube_split/tests/test_ray_ingest_job.py`：`test_postgres_upserts_copy_rows_then_merge_once`（FakeCursor 增加 `copy()`，断言 1×CREATE TEMP + 1×COPY + 1×MERGE + 1×DROP、行数、ST_SetSRID、DISTINCT ON）。

## 三、OpenGauss 端已生效的调优（不依赖代码）

主备两节点（Primary 10.3.100.180 / Standby 10.3.100.181；.179/.182 不跑 DB）通过 `gs_guc reload -D /data/og_user/openGauss/install/data/dn -c 'k=v'` 写入：

| 参数 | 原值 | 现值 |
|---|---|---|
| checkpoint_completion_target | 0.5 | 0.9 |
| autovacuum_naptime | 600s | 120s |
| autovacuum_vacuum_scale_factor | 0.2 | 0.1 |
| autovacuum_analyze_scale_factor | 0.1 | 0.05 |
| bgwriter_delay | 2000ms | 200ms |
| log_min_duration_statement | 1800000ms | 5000ms |
| track_io_timing | off | on |

维护：TRUNCATE staging、11 表 VACUUM ANALYZE。回滚 = 同命令写回原值。

## 四、待办清单（按优先级）

1. **提交代码**（用户要求先把直接修改 commit）：5 个文件 + 建议同时在 Linear 新建事项记录。
2. **真实剖分测试**：
   - 触发方式候选：`PartitionWorkflowService.retry_task`（仅 failed/cancelled/manual_required 可直接重试；completed 需要 band_unit_ids）或对既有 succeeded 批次重新提交流程；确认入口后经 Ray Job（`CUBE_WEB_RAY_JOB_ADDRESS=http://10.3.100.183:30826`）提交，runtime_env 会上传本地工作目录 → 未提交改动会生效。
   - 采集指标：`partition_job_attempts.runner_result.timings` 里的 `opengauss.promote_logical_staging` / `opengauss.complete_output` / `opengauss.logical_stage` 与 `driver.bootstrap`，**与 baseline 对比（见下）**；`ray.wait` 属于 KubeRay 调度/预热，不计入入库时间。
3. **真实入库测试**：候选路径 `cube_web/services/ingest_worker.process_queued_ingest_scenes`（executor 为 `cube_split.ingest.managed_output_ingest.ingest_managed_output`）或直接触发 `ray_ingest_job`；记录新代码下的 upsert 耗时/行数。
4. 视结果决定是否同样优化 `product_ingest_job`（仍在用逐批 VALUES MERGE）。

## 五、Baseline（对比用）

**微基准（OpenGauss，临时表，20k 行，同构索引）**：
- 旧写法 MERGE 逐批：500 行/批 ≈ **14.9k rows/s**（batch 越大越慢，5000 行/批 ≈ 9.8k）
- 新写法 COPY 直写 ≈ **89.6k rows/s**；COPY→staging + INSERT SELECT ≈ **88.8k rows/s**
- 提交延迟 sync=on 1.5ms / off 0.62ms；8 线程并发提交 ~3.1k commits/s
- openGauss 7.0.0-RC3 不支持 `INSERT ... ON CONFLICT`、无 `lock_timeout`（只有 `lockwait_timeout`）

**历史真实运行分解（改前）**：
- `partition-d680d717da27`（逻辑 mgrs，40 chunk）：总 91.6s；ray.wait 88.1s；staging 1.64s；promote 2.08s
- `partition-990b7e15a1e3`：88.2s；ray.wait 86.5s；DB 阶段 1.4s
- `partition-02a29f0d59c1`（碳，ISEA4H）：132.0s；ray.wait_get 86.6s；**opengauss.complete_output 44.75s**
- `partition-bd81e80f4b60`：job_driver 126.7s；**driver.bootstrap 102.8s**；staging 8.94s；promote 7.31s
- 入库：carbon 70909 行 18.8s 总耗时；cube_fact 3283 行 3.8s

## 六、环境与访问注意事项

- openGauss 初始用户 `og_user` 禁止远程登录；管理路径：`ssh root@10.3.100.180` → `su - og_user` → `gsql -h /data/og_user/openGauss/tmp -p 15400 -d postgres`（socket，免密）。
- 凭据：用户提供的 root/DB 密码一律不入库、不写文件（对话中已明文出现，建议轮换）。
- KubeRay Ray Job Server：`http://10.3.100.183:30826`（api/jobs 可查作业状态与时长）。
- 工作区是共享的：提交时只 add 本任务文件；`git add -u cube_split cube_web` 前先 `git status` 核对。
