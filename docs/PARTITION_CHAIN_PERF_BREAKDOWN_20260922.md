# 剖分链路（格网 → 入库）逐阶段性能剖析（2026-09-22）

> 对逻辑剖分（geohash / mgrs）全链路逐阶段实测：格网覆盖 → 行生成 → 序列化/压缩 →
> MinIO chunk → OpenGauss 写入 → web 编排校验 → 入库（managed ingest）。
> 上游文档：`PERFORMANCE_OPTIMIZATION_STEPS_20260913.md`（方法论与已落地优化）、
> `PARTITION_WRITE_PERFORMANCE_HANDOFF.md`（写入路径）、`PERF_SINGLE_SCENE_10S_20260913.md`（10s 目标口径）。
>
> **测量时间**：2026-09-22 17:15–18:20。测量机为开发沙箱（loadavg 0.2–1.2，各输出首尾有记录）；
> OpenGauss / MinIO 为共享生产集群（10.3.100.180 / 10.3.100.179），负载未控。
> 计时只在同批内排序使用；硬证据（格元数、行数、字节数、执行计划节点）不受负载影响。
>
> **范围**：逻辑剖分（生产格网契约与 10s 性能目标所在）。实体剖分（isea4h）本轮未测。
> 生产路径 = `cube_split.jobs.ray_logical_chunk_job`（`CUBE_LOGICAL_CHUNK_PERSIST=direct`）；
> `cube_web/services/partition_dataset_runner.py::_run_logical_dataset_on_ray` 为无调用方的死代码副本（见 §7）。
>
> **后续状态（写作时）**：§8 优化建议均未实施，本文为基线与决策记录。原始测量值归档于
> `~/perf-probe-evidence-20260922-partition-chain/`（脚本 + `*.out` 原始输出 + `REPORT.md`）。

---

## 0. 测量方法与可信度约定

| 约定 | 做法 |
|---|---|
| 计时可信度 | 每例多轮，报**最小值 + 中位**；单轮计时可能被负载抬高 |
| 顺序混淆 | A/B 对比（dict_row/tuple_row、st_code 去重等）**轮转执行顺序** |
| 硬证据 | 格元数、行数、字节数、`EXPLAIN ANALYZE` 计划节点（Sort Method / 索引形态） |
| 真实库探针 | 只写 scratch schema **同名影子表** + `SET search_path` + `'<表名>'::regclass::oid` 解析断言 + 前后生产表行数断言（本轮全部守卫通过、生产表行数不变） |
| 凭据 | 文档、脚本、输出均不打印 DSN / MinIO 口令 |

---

## 1. 阶段总览（20k 行 chunk / 单景 4 波段口径）

| 阶段 | 单位成本 | 20k 行 chunk | 备注 |
|---|---|---|---|
| S1 格网覆盖 `sdk.cover` | geohash 3.6 万格元/s；mgrs 0.4 万格元/s | — | mgrs 单格元贵 9–24× |
| S2 行生成 | output_id 6.3µs/次；st_code 7.2µs/次（按格元×波段重复算） | 0.7–1.2s（双波段） | mgrs 几何 95µs/格元 |
| S3 序列化 + gzip(L9) | serialize ≈32MB/s；gzip L9 | serialize 0.3s + gzip 0.25s | gzip 默认 **level 9** |
| S4 MinIO chunk 往返 | stat 4ms / put 25ms / get+解析 135ms | ≈165ms | 非瓶颈 |
| S5 OpenGauss `write_chunk_rows` | 57–87µs/行 + **每 chunk 固定 ≈0.4s** | min 1.09s / 中位 1.85s | 另有 19ms/连接 |
| S6 web `verify_output_chunks` | 135ms/chunk（下载+gunzip+逐行 json 数 kind） | 135ms | direct 模式下为重复功 |
| S7 入库 `ingest_managed_output` | `_load_snapshot` 70k 行 **10.4–13.4s** | — | 8.5s 是 JSONB 自动解析 |

---

## 2. S1 格网覆盖（worker，每 shard）

| 用例 | 格元数 | 耗时 min/median |
|---|---|---|
| geohash L4（鲁南小景） | 12 | 0.0007 / 0.0008 s |
| geohash L5（山东 8°×4.6°） | 19,581 | 0.53 / 0.57 s（3.6 万格元/s） |
| mgrs L0（小景） | 2 | 0.0016 s |
| mgrs L1（山东） | 3,919 | 0.93 / 0.97 s（0.4 万格元/s） |

MGRS 覆盖单格元成本约为 geohash 的 9–24 倍：`grid_core/app/engines/mgrs/cover.py` 对每个候选
格元做 `cell_geometry_clipped`（shapely 域裁剪）+ AOI 逐变体 `intersects/intersection`，
且 BFS 会探索大量落选候选格元。geohash 已有格点算术快路径（`_lattice`），MGRS 没有对应物。

## 3. S2 行生成（worker，逐格元/逐波段）

| 子步骤 | 单位成本 | L5 山东（19,581 格元×2 波段 = 97,905 行）合计 |
|---|---|---|
| `code_to_geometry` | geohash 3.2µs / mgrs 95µs 每格元 | 0.062s（geohash）/ 0.35s（mgrs，3,919 格元） |
| `logical_output_id`（json.dumps+sha256） | 6.3µs × 格元×(1+波段) | 0.72–0.75s（11.7 万次） |
| `generate_st_code` | 7.2µs × 格元×波段 | 0.33s（7.8 万次；**st_code 无波段维度，其中 2/3 为波段间重复计算**，A/B 实测 40k 次 0.288s vs 20k 次 0.140s） |
| 其余 dict 组装 | — | 0.11s |

## 4. S3 序列化 + 压缩（worker，per chunk）

- `serialize_logical_chunk_rows`（`sorted` + `json.dumps(sort_keys=True)`）：98k 行 / 43MB
  = 1.35–1.43s（≈32MB/s，行生成之后最大的单块 CPU）。
- `compress_logical_chunk` = `gzip.compress(content, mtime=0)`，Python 3.11 签名默认
  **`compresslevel=9`**（已核实 `inspect.signature`）。同一 43MB 内容：L9 1.21s（5.80MB）；
  L6 0.61s（5.88MB，+1.4%）；L1 0.24s（6.80MB，+17%）。mgrs 20k 行：L9 0.90s vs L1 0.115s。
- 20k 行 chunk 体量参考：8.2MB JSONL → 237KB gz（L9）。

## 5. S4–S5 MinIO 与 OpenGauss 写入

**MinIO（真实集群，20k 行 / 237KB chunk，5 轮）**：stat 未命中 4ms / put 25ms / stat 命中 4ms /
get+gunzip+逐行 json 计数 135ms。非瓶颈。

**OpenGauss `write_chunk_rows`（影子表与生产同名、含同款唯一索引，20k 行 chunk）**：

| 用例 | 行数 | min / median |
|---|---|---|
| mixed 新鲜写（4k cells + 8k tiles + 8k indexes） | 20,000 | 1.09s / 1.85s |
| mixed 全重复重放（NOT EXISTS 短路） | 20,000 | 0.89s / 1.27s |
| 仅 grid_cells | 4,000 | 0.25s / 0.27s（62µs/行） |
| 仅 tiles | 8,000 | 0.69s / 0.73s（87µs/行） |
| 仅 indexes | 8,000 | 0.44s / 0.46s（57µs/行） |
| mixed 新鲜写 | 2,000 | 0.53s / 0.63s |

- **每 chunk 固定开销 ≈0.4s**（2k 行与 20k 行的差分推得；3×TEMP 表 CTAS + COPY + anti-join
  INSERT 的语句往返），另 `psycopg.connect` 建立+关闭 18–19ms（worker 每 chunk 新建连接）。
- 行级 57–87µs/行的成本主要在每行 3–4 条宽联合唯一索引的维护（见 §7 膨胀数据）。
- 只读探针：2 万个 output_id miss 对**真实** partition_tiles / indexes / grid_cells 的
  `NOT EXISTS` 探测 0.059–0.096s —— 即使表已膨胀，查找侧不贵。

## 6. S6 web 编排与 S7 入库

**S6 `verify_output_chunks`**：对每个 chunk **下载 + gunzip + 逐行 `json.loads` 只为数 kind**
（135ms/chunk，8 并发）。direct 写模式下这些行已在目标表，`complete_output` 还会用 SQL 再数
一遍 —— 纯重复功（staging 回退路径的历史对比见 AGENTS：36 万行 65.6s vs 7.8s）。

**S7 `ingest_managed_output`（真实数据只读 + 写入打桩；MERGE 用影子表）**：

| 样本 | `_load_snapshot` | `_verify_minio_objects` | 全程（写入打桩） |
|---|---|---|---|
| product 69,758 索引行 | **10.4–13.4s** | 0.20s | 13.0–14.5s |
| carbon 70,909 索引行 | 4.3–5.8s | 0.14s | 7.7–8.3s |
| optical 100 索引行 | 0.02s | 0.03s | 0.06s |

`_load_snapshot` 归因（cProfile，总 11.99s）：

- **8.46s = 279,048 次 `json.loads`** —— psycopg 对 JSONB 列（`attributes` /
  `scene_attributes` / `asset_attributes`）自动解析，纯客户端 CPU；产品/光学路径根本不消费
  这些解析结果（仅 carbon 路径读 `attributes`）。
- 2.35s = SQL 执行/等待；其余为行物化（dict_row vs tuple_row A/B 轮转实测同级：
  3.37s vs 3.56s，非因子）。只取所需列的变体：product 2.97s / carbon 1.73s。
- 大 JOIN 的 `ORDER BY i.output_id` 使服务端走 `Sort Method: external merge Disk: 100,680kB`
  （carbon 137,648kB）—— 每次入库在服务端落盘百兆级外排。
- MERGE `upsert_cube_facts_postgres`（影子，20k 行）：全 MATCHED 更新 0.98s（非 HOT 更新
  churn 的来源）；全 NOT MATCHED 插入（含 `ST_GeomFromGeoJSON`）1.56s。
- `_verify_minio_objects`：0.14–0.2s（逐唯一 URI 串行 stat，当前体量无碍；实体逐 tile URI
  会线性放大）。
- `_verify_targets`：`WHERE CAST(run_id AS VARCHAR(128))=%s` 无可用索引 → 全表 seq scan，
  随 `rs_*` 表增长线性变慢。

---

## 7. 结构性发现（本轮顺带核出）

1. **死代码副本**：`cube_web/cube_web/services/partition_dataset_runner.py::_run_logical_dataset_on_ray`
   （约 170 行）是 cube_split 生产实现的分叉旧拷贝（缺 direct 写与分片规划器），全仓无调用方，
   违反「其他包不允许复制格网逻辑」，建议删除。
2. **探针残留**：生产库存在 `probe_idx_impact_20260919.rs_cube_cell_fact`（25k 行 / 8MB 及其
   索引），系 2026-09-19 探针遗留，建议授权后 `DROP SCHEMA`（本轮未动）。
3. **膨胀**：`partition_logical_staging_rows` 0 行但总大小 **1.5GB**；partition_tiles 938MB /
   149k 行、partition_indexes 852MB / 149k 行、partition_grid_cells 610MB / 78k 行（多条宽
   联合唯一索引）。`VACUUM FULL`/`REINDEX` 属维护窗口操作（AGENTS 已列）。
4. **逐行遥测阻塞入库**：`ray_ingest_job._report_cube_fact_metrics` 对每条 fact 生成一个 span、
   每 200 条**同步 HTTP POST** 到硬编码 `http://10.3.100.182:6000/api/v1/traces`（2s 超时），
   且硬编码机器 IP 违反 AGENTS 配置规则。

---

## 8. 优化建议（等价性 → 收益排序，均未实施）

| # | 优化 | 预期收益 | 等价性 / 风险 |
|---|---|---|---|
| P0-1 | `_load_snapshot` 不自动解析 JSONB（`attributes::text` 取文本，仅 carbon 路径解析） | 70k 行快照 10.4s → ~3s（入库段 −60%+） | 语义等价 |
| P0-2 | `generate_st_code` 按格元缓存（st_code 不含波段维度） | 行生成 st 段 −50%（2 波段）~−75%（4 波段） | 代数恒等，完全等价 |
| P0-3 | `compress_logical_chunk` 降 gzip L6（或 L1） | 压缩段 CPU −50%~−80%（+1.4%/+17% 体积） | **字节输出变化 → checksum 变化**：跨版本重放旧 chunk 会触发 immutable collision，需按版本或开关处理 |
| P0-4 | `_verify_targets` 去掉 CAST 或建 `(run_id)` 表达式索引 | 免全表扫描，收益随 `rs_*` 增长递增 | 索引需 DDL 授权 |
| P1-1 | direct 模式跳过 `verify_output_chunks` 下载重解析（manifest 计数 + `complete_output` SQL 计数已闭环） | 每 chunk −135ms + 网络，百 chunk 批次省数秒 | 保留 manifest 行数核对口径防 chunk 缺失 |
| P1-2 | serialize 去 `sort_keys`（行已按 (kind, output_id) 排序，消费端解析为 dict） | serialize −30~40% | 同 P0-3 的 checksum 迁移注意 |
| P1-3 | `write_chunk_rows` 每 worker 复用连接 / 合并 chunk 任务 | 每 chunk −0.4s 固定开销与 19ms 连接 | 需连接生命周期管理 |
| P1-4 | 大 JOIN 去 `ORDER BY i.output_id`（消费端未依赖行序做检索） | 免服务端百兆外排落盘 | 需确认无隐式顺序依赖 |
| P2-1 | MGRS 为 bbox AOI 增加算术枚举快路径（对齐 geohash `_lattice`） | mgrs 覆盖/几何 95–240µs/格元 → 数 µs 级 | cube_encoder 改动，需格网正确性回归 |
| P2-2 | 删除死代码 `_run_logical_dataset_on_ray` | 维护性 | 无运行时影响（无调用方） |
| P2-3 | 遥测批量化/异步化，`TRACE_URL` 改运行时配置 | 消除入库路径同步 HTTP | 无 |
| P2-4 | 清理 `probe_idx_impact_20260919`、回收 staging 表 1.5GB、`partition_*` 膨胀 | 存储 + 行级写入成本 | 需维护窗口 / 授权 |

---

## 9. 证据与复现

脚本与原始输出：`~/perf-probe-evidence-20260922-partition-chain/`（`bench_cpu_stages.py`、
`bench_minio.py`、`bench_db_write.py`、`bench_ingest.py`、`bench_snapshot_breakdown.py`、
`bench_micro_attribution.py` 及对应 `*.out`）。库探针脚本沿用同名影子表 + 解析断言 + 前后行数
断言模板（与 `~/perf-probe-evidence-20260912/probe_guard.py` 同规则）。复现入口：

```bash
cd /home/lyajun/projects/cube_project
python3.11 ~/perf-probe-evidence-20260922-partition-chain/bench_cpu_stages.py
python3.11 ~/perf-probe-evidence-20260922-partition-chain/bench_minio.py
python3.11 ~/perf-probe-evidence-20260922-partition-chain/bench_db_write.py
python3.11 ~/perf-probe-evidence-20260922-partition-chain/bench_ingest.py {snapshot|merge}
python3.11 ~/perf-probe-evidence-20260922-partition-chain/bench_snapshot_breakdown.py
python3.11 ~/perf-probe-evidence-20260922-partition-chain/bench_micro_attribution.py
```
