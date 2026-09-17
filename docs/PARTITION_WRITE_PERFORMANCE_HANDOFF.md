# 剖分/入库性能瓶颈 Handoff（2026-09-12）

> 上游文档：`archive/docs/WRITE_OPTIMIZATION_HANDOFF.md`（写入路径优化 + 真实验收，已完成）。
> 本文档目的：把「当前性能瓶颈在哪」用实测数据讲清楚，并给出下一步诊断/优化清单，供下一个 agent 直接接手。
> 边界：本文档只做**定位与计划**，不含新代码改动。
>
> **后续状态（2026-09-18 核查，HEAD `dead6bf`）**：本文是 2026-09-12 的测量快照，原始数字保留。
> - 实体路径不再「下载后转 COG」：源数据本身已是 COG，worker 只经 `cache_source_cog` 原样缓存（2026-09-13 起）。
> - 逻辑剖分的生产层级口径已改为调用方请求更粗层级（mgrs L0）：本文 §3.1 的 mgrs L1 26,024 行是当时的大批次场景；
>   单景全波段 <10 s 的最终口径见 `PERF_SINGLE_SCENE_10S_20260913.md`。
> - 表/索引体量是 2026-09-12 的值；例如 `partition_indexes` 行数已从 291,920 增至 397,439，且三张 `partition_*` 表未 REINDEX（2026-09-18 只读核查）。
> - 本文 §5.1 提出的 promote 分语句计时已由 `61ad6fa`（2026-09-12）落地，但本轮核查未见子阶段实测值记录。

## 一、结论（TL;DR）

1. **逻辑剖分（geohash/mgrs）的唯一主导瓶颈是 `promote_logical_staging`**：mgrs L1 一轮 attempt 133.3s 中占 **106.9s（80%）**，吞吐 **243 rows/s**（26,024 行），且三个数据集**串行**执行。
2. **实体剖分（isea4h）的瓶颈不在 DB**：attempt 171.0s 里 DB 阶段合计约 7s，其余是 Ray worker（下载 577MB/594MB 源、转 COG、写实体瓦片）+ `ray.wait`。
3. **入库已不是瓶颈**：`ray_ingest_job` 的「COPY 到临时表 + 单条 MERGE」在 4 个真实数据集上作业级 0.127–2.745s，整段 quality+ingest 15.6s。
4. **冷启动**：worker 冷时首个任务 `ray.wait` ≈ 86.6s；worker 热时同量级任务 3–20s。
5. 已测证据指向：成本在**往 `partition_tiles` / `partition_indexes` 逐行写入**（索引体积 678 MB / 804 MB，均**大于**表本体），既不是 SQL 写法也不是表 bloat（`n_dead_tup` ≈ 0）。

## 二、当前状态

| 项 | 状态 |
|---|---|
| 写入优化代码 | ✅ `69bfb52`（COPY / 集合式写入），在 HEAD 链上 |
| 真实验收 | ✅ `status=passed`，命名空间 `real-accept-0d411c506d50`（6 个 run + 3 探针 + 4 数据集入库 + publication） |
| 结果回写 | ✅ `503ba0c`（§九 证据）+ `4fe4734`（表/索引实测）→ `archive/docs/WRITE_OPTIMIZATION_HANDOFF.md` |
| 回归 | ✅ `PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest` → 947 passed（干净环境） |
| 遗留数据 | `real-accept-0d411c506d50%`（datasets 7 / scenes 10 / partition_runs 6 / tiles=indexes 26,671 / grid_cells 5,747）、`smoke-b2f5e0588245%` |
| 已知偏差 | radar 数据集质量 `warn`（非强制规则 `radar_band_contract`，源为 Sentinel-1 风场产品）；本地 DSN 实际指向 `cube_v3` 库（AGENTS.md 写的是 `postgres`） |

## 三、实测数据

### 3.1 逻辑路径 mgrs L1（`partition-9890fd667e48`，4 数据集）

attempt 133.261s（Ray Job 137.13s）。阶段耗时来自 `partition_job_attempts.runner_result`：

| 阶段 | 耗时 | 次数 | 说明 |
|---|---|---|---|
| `workflow_dataset.opengauss.promote_logical_staging` | **106.932s** | 3 | **80% 墙钟时间**，数据集串行 |
| `logical_worker.opengauss.logical_stage` | 28.125s | 58 | worker 侧 COPY 进 staging，与 `ray.wait` 并行 |
| `logical_batch_driver.ray.wait` | 20.108s | 62 | KubeRay 调度等待，不计入 DB 写入 |
| `workflow_dataset.opengauss.verify_output_chunks` | 2.113s | 3 | |
| `workflow_dataset.opengauss.complete_output` | 1.970s | 4 | |
| `workflow_dataset.opengauss.record_output_chunks` | 0.285s | 3 | |
| `ray_job_driver.driver.bootstrap` | 0.126s | 1 | 历史 `partition-bd81e80f4b60` 为 102.8s，已被 schema 预检消除 |

promote 分数据集（关键：吞吐与数据量不成正比）：

| 数据集 | 组成 | 输出行数（每行 1 tile + 1 index） | promote 耗时 | 吞吐 | 扣除固定开销后 ms/行 |
|---|---|---|---|---|---|
| optical | 2 scene × 3 band × 428 cell | 2,568 | 18.908s | 136 rows/s | ≈ 7.3 |
| radar | 3,256 cell × 2 asset × 3 band | 19,536 | 66.262s | 295 rows/s | ≈ 3.4 |
| product | 1,960 cell × 2 scene | 3,920 | 21.761s | 180 rows/s | ≈ 5.5 |
| carbon | 碳路径，不经过 promote | 100 | — | — | — |

注意：三个数据集的 **ms/行 相差 2 倍以上**（3.4–7.3），说明除行数外，**行宽 / JSONB 负载 / 分片形状也参与成本**（optical 行数最少却最慢，待 §5.1 诊断确认）。

`logical_stage` 分数据集：optical 1.481s、radar 23.884s、product 2.759s。

### 3.2 对照组 geohash L1（`partition-7102999ad725`）

attempt 10.255s：`logical_stage` 2.992s（58 次）、**promote 0.286s**（3 次调用 / 9 条语句）、`complete_output` 1.598s、`ray.wait` 3.250s、`bootstrap` 0.122s。

→ 两次运行走同一套 promote 代码（3 个数据集 × 3 条语句 = 9 条），差别只在非碳输出行数（geohash **14 行** vs mgrs **26,024 行**）：geohash 的 0.286s 基本是 9 条语句的固定开销，mgrs 的增量约 **4.1 ms/行**（≈ 244 rows/s）。即瓶颈是行级的，不是语句级的。

### 3.3 实体路径 isea4h L6（`partition-871854359def`）

attempt 171.032s（Ray Job 174.74s）：

| 阶段 | 耗时 | 次数 |
|---|---|---|
| `raster_batch_driver.ray.wait` | 2070.193s（**累计**，154 次等待） | worker 下载/转 COG/写瓦片 |
| `workflow_dataset.opengauss.complete_output` | 6.227s | 4（optical 0.759 / radar 1.387 / product 0.562 / carbon 3.518） |
| `workflow_dataset.opengauss.start_output` | 0.895s | 4 |
| promote / logical_stage | — | 实体路径不存在这两个阶段 |

输出（tiles/indexes/grid_cells）：60/60/10、222/222/37、48/48/24、100/100/3。写目标表的行数为 430 tiles + 430 indexes（+74 个 grid_cell），6.227s ≈ **138 rows/s** —— 与逻辑 promote 同为「百行级/秒」，指向同一个写入成本。

### 3.4 入库（`rs_ingest_job` + RS 明细表）

| 数据集 | output_version | ingest run 时长 | RS 行数（stats_json，已与按 `run_id` 聚合的实际行数核对一致） | 作业级时长 |
|---|---|---|---|---|
| optical | `884d1cde004b73b209dbb844d6dd9c97` | 4.84s | `rs_cube_cell_fact` 10 / `rs_raw_scene_asset` 1 / `rs_entity_tile_asset` 10 | 0.127–1.384s |
| radar | `aa6729b40862c811f160a53cb83507d2` | 5.07s | 37 / 1 / 37 | 0.134–1.033s |
| product | `b57a15c5d7b900655124a331c1e17515` | 4.00s | 24 / 1 / 24 | 1.440–1.946s |
| carbon | `d96e36279119718a6970e11ff5fa6da2` | 6.34s | `rs_carbon_observation_fact` 50 | 2.745s |

`ingest_run_scenes` 16/16 completed，重复 ingest 不产生重复行（幂等成立）。整段 `quality_ingest_seconds=15.559`。

### 3.5 冷启动与调度

- 冷启动 smoke（`partition-dd60c0875325`，geohash L1 单数据集）：attempt 87.793s，其中 `ray.wait` **86.608s**（n=92），`bootstrap` 0.246s。
- 热集群下**小任务**明显变快（geohash L1 10.3s、cancel 探针 4.6s、quality 探针 2.2–2.9s）；**重任务**仍由自身瓶颈支配（mgrs 大行数 → §3.1、isea4h 大影像 → §3.3），冷启动只在首个任务上体现。
- Ray Job Server 总时长 − attempt 时长 ≈ 4–14s（提交/排队/收尾），例如 mgrs 137.13s vs 133.26s、isea4h 174.74s vs 171.03s。

### 3.6 目标表与索引体量（只读实测，2026-09-12）

| 表 | 行数 | 表大小 | **索引大小** | 索引明细 | 死元组 / 更新 |
|---|---|---|---|---|---|
| `partition_indexes` | 291,920 | 489 MB | **678 MB** | 8 列复合唯一索引 493 MB、主键 182 MB、`tile_output_id` 局部 3 MB | `n_dead_tup=1` |
| `partition_tiles` | 293,338 | 340 MB | **804 MB** | 复合唯一索引 529 MB、主键 157 MB、`searchable` 局部 118 MB | `n_dead_tup=505`、`n_tup_upd=74,776` |
| `partition_grid_cells` | 268,748 | 460 MB | 467 MB | 复合 306 MB、主键 161 MB | `n_dead_tup=11,246` |
| `partition_logical_staging_rows` | 0 | 0 B | 46 MB | 主键 28 MB、`version_kind` 18 MB | `n_tup_ins=480,057`（TRUNCATE 不回收索引） |
| `rs_cube_cell_fact` | 60,400 | 41 MB | 27 MB | 复合 23 MB、主键 4 MB | `n_dead_tup=424` |

`partition_indexes` 复合唯一索引 ≈ **1,770 B/行**（493 MiB ÷ 291,920），说明键非常宽（多列 text）—— 这是后续优化的主要抓手。

## 四、瓶颈定位与根因

### 4.1 已测（事实）

1. 逻辑路径的墙钟时间被 `promote_logical_staging` 支配（mgrs 80%），近似随输出行数线性增长（混合均值 ≈ 244 rows/s；单数据集 136–295 rows/s）。
2. 小批次（geohash 非碳 14 行）的 promote 只花 0.286s → 9 条语句的固定开销可忽略，**瓶颈完全是行级写入**（≈4.1 ms/行）。
3. 实体路径 `complete_output` 的速率同为 ~138 rows/s，两条路径量级一致 → 共同点是**写同两张目标表**。
4. 两张目标表的索引体积均大于表本体积，且死元组极少（不是 bloat）。
5. staging 侧已经是 COPY（`logical_stage` 在 worker 内完成，radar 19,536 行约 24s ≈ 817 rows/s），比 promote 快 3 倍以上。
6. 入库（`rs_*` 表，体量小一个数量级）在同一套 COPY+MERGE 模式下是亚秒级 → 说明**瓶颈是目标表的规模/索引，而不是 COPY/MERGE 这套写法本身**。

### 4.2 推断（**尚未验证**，下一步要证实或证伪）

1. 每条输出行要在 `partition_indexes`(2 个 B-tree) + `partition_tiles`(3 个) + `partition_grid_cells`(2 个) 上共维护 ~7–8 个索引条目，且复合唯一索引键宽（~1,770 B/条）→ 单行写入成本高。
2. 微基准（20k 行、全新临时表、同构索引）能到 88.8k rows/s，与生产 243 rows/s 差 365 倍；差额可能来自：宽索引在大表上的维护成本、`INSERT ... SELECT DISTINCT ON ... WHERE NOT EXISTS` 的排序/反连接、目标表锁竞争、或 WAL/checkpoint 压力（`checkpoint_completion_target=0.9` 已调）。
3. 待排查项：promote 里三条语句各自耗时、`NOT EXISTS` 反连接的代价、是否有 `lockwait_timeout` 等待、规划器是否退化为嵌套循环全表扫描。

### 4.3 规模影响（为什么要管）

按 243 rows/s：10 万行 ≈ 7 分钟、100 万行 ≈ 68 分钟。逻辑剖分在大 AOI/高层级（mgrs L1 单雷达成像就 3,256 cells）时会线性劣化，而 entity 路径的 DB 成本虽低但同样受此限。

## 五、下一步

### 5.1 先做诊断（只读，不改代码）

1. **promote 分语句计时**：在 `partition_domain_store.promote_logical_staging` 里把 3 条 `INSERT ... SELECT` 包成独立 `TimingRecorder.phase`（例如 `opengauss.promote.tiles` / `.indexes` / `.grid_cells`），跑一个 mgrs L1 小批次即可定位是「哪张表」慢。
2. **EXPLAIN**：把实际 promote SQL 取出执行 `EXPLAIN`（不要 `ANALYZE`，避免副作用），确认三张表的插入计划、`NOT EXISTS` 反连接方式与是否出现全表扫描。
3. **锁与 IO**：`SELECT * FROM pg_stat_activity WHERE state <> 'idle'`、OpenGauss 慢日志（`log_min_duration_statement=5000`）按 `promote`/`partition_indexes` 过滤；必要时 `track_io_timing` 已有数据可看。
4. **对照实验（可选，需谨慎）**：在临时 schema 里用同样的 DDL 建 `partition_indexes`/`partition_tiles` 副本（仅索引结构，不复制 29 万行），用相同行宽插入 2 万行，测「索引结构 vs 表体量」哪个是主因；也可以复制 29 万行后再测，得到体量曲线。

### 5.2 优化候选（按预期收益排序）

1. **降索引成本**（收益最大、最直接）
   - 收窄 8 列复合唯一索引（`dataset_id,output_version,source_asset_id,band_code,grid_type,grid_level,space_code,time_bucket,...`）：能删的列删掉，或拆成两个窄索引；`output_version` 这类低区分度高基数列的顺序也值得重排。
   - 评估主键与部分索引的必要性（`idx_partition_tiles_searchable` 118 MB、`idx_partition_indexes_tile_output_id` 3 MB）。
   - 注意 `partition_tiles.n_tup_upd=74,776`，更新会重写全部 3 个索引，需一并评估。
2. **按 `dataset_id` / `output_version` 分区**：写入局部化，索引变小，同时便于按 output_version 淘汰。
3. **去掉二次搬运**：现在是「worker COPY→staging → 集合式 INSERT SELECT→目标表」。新 output_version 本就不冲突（重试时可先按版本 delete），可评估**直接 COPY 进目标表**或先 `DELETE WHERE output_version=...` 再批量插入，避免 staging→目标表这一跳。
4. **`product_ingest_job` 迁移到 COPY + 单条 MERGE**（上游文档 §9.7 已记录理由：只在 product + logical 组合生效，非当前瓶颈，属机械改动）。
5. **实体路径**：瓶颈是 worker 侧 IO（下载 577MB/594MB float64、转 COG、写瓦片），若要做，方向是并行度/缓存/裁剪（不是 DB）。
6. **冷启动**：worker 冷时首个任务 `ray.wait` ≈ 86.6s，可考虑常驻 worker 池或预热任务；这部分**不应计入入库耗时**（上游文档已约定）。

### 5.3 验收口径（沿用上游文档）

- 任何优化都要在**真实 MinIO 源 + Ray Job + OpenGauss** 上复跑 `cube_web/scripts/run_real_partition_acceptance.py`，最终必须 `status=passed`；`ray.wait` / Ray Job 排队单独记录，不计入 DB 写入耗时。
- 回归必须干净环境跑全量：`PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest`（**不要**先 `export` `.cube_web.env`，会让 `test_env_text_reads_local_env_file` 因环境变量优先而假失败）。

## 六、复现材料

上游文档 `archive/docs/WRITE_OPTIMIZATION_HANDOFF.md` 的 §9.2（真实源清单 + 流式 SHA-256）、§9.3（计时表）、§9.4（入库行数）、§9.9（复现命令）已包含全部细节，这里只补本次诊断用的查询。

### 6.1 从 runner_result 提取阶段耗时

```python
# partition_job_attempts.runner_result 的阶段是嵌套结构：
#   runner_result -> timings -> {ray_job_driver, ...}
#   runner_result -> datasets[] -> timings -> {units:[{driver, workers:[...]}]}
# 用递归收集所有 {"scope":..., "phases":{name:{"elapsed_sec":...,"count":...}}} 节点并累加：
WATCH = ("opengauss.", "driver.bootstrap", "ray.wait", "chunk_upload")

def walk(node, out):
    if isinstance(node, dict):
        scope, phases = node.get("scope"), node.get("phases")
        if scope and isinstance(phases, dict):
            for name, stats in phases.items():
                key = f"{scope}.{name}"
                if any(t in key for t in WATCH):
                    bucket = out.setdefault(key, {"elapsed_sec": 0.0, "count": 0})
                    bucket["elapsed_sec"] += float(stats.get("elapsed_sec") or 0)
                    bucket["count"] += int(stats.get("count") or 0)
        for value in node.values():
            walk(value, out)
    elif isinstance(node, list):
        for item in node:
            walk(item, out)

# 取数：SELECT runner_result FROM partition_job_attempts WHERE task_id = 'partition-9890fd667e48'
```

### 6.2 目标表/索引体检（只读）

```sql
-- 大小与索引明细
SELECT pg_size_pretty(pg_relation_size('partition_indexes'))  AS table_size,
       pg_size_pretty(pg_indexes_size('partition_indexes'))   AS index_size;
SELECT indexrelname, pg_size_pretty(pg_relation_size(indexrelid)) AS size, idx_scan
FROM pg_stat_user_indexes WHERE relname = 'partition_indexes'
ORDER BY pg_relation_size(indexrelid) DESC;

-- bloat / 写入量
SELECT n_live_tup, n_dead_tup, n_tup_ins, n_tup_upd,
       last_vacuum, last_autovacuum, last_analyze
FROM pg_stat_user_tables WHERE relname IN ('partition_tiles', 'partition_indexes', 'partition_grid_cells');
```

### 6.3 运行验收门禁

见上游文档 §9.9：受控 `set -a; . .cube_web.env; set +a` → 起 uvicorn（127.0.0.1:50039）→ `--prepare-only` 只读校验 manifest → 用进程内签发 admin token 的 wrapper 跑完整门禁（**不要**用 `CUBE_WEB_AUTH_REQUIRED=0` 宣称通过）。

## 七、踩坑与安全边界

- **不要**打印 `.cube_web.env`、DSN、MinIO 凭据、Bearer token；token 只放临时进程环境。
- OpenGauss 只有 `og_user` 可本地 socket 免密管理（`ssh root@10.3.100.180` → `su - og_user` → `gsql -h /data/og_user/openGauss/tmp -p 15400 -d postgres`）；本地应用 DSN 指向 `cube_v3`，两者是**不同库**，核对计数时别串库。
- 只读诊断优先（`EXPLAIN` 而非 `EXPLAIN ANALYZE`；不要在生产表上做未计划的写入实验）。
- `s3://` 相关质检必须下载到节点本地缓存再用 rasterio 打开，不能用 `Path.exists()`。
- 验收/诊断产生的资源（Ray Job、临时表、staging）用完要清理；验收数据目前保留在 `real-accept-0d411c506d50%` 与 `smoke-b2f5e0588245%` 供取证。
