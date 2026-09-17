# 性能优化方向探索报告（格网 / 剖分 / 入库）

> 上游文档：`docs/PARTITION_WRITE_PERFORMANCE_HANDOFF.md`（写入路径优化 + 真实验收）。
> 本文档目的：在既有 handoff 之上，用**实测 + 对抗性验证**给出三个维度的优化方向与优先级；同时**显式记录被证伪的方向**，避免后续 agent 重复把预算花在错误目标上。
> 边界：本文档只做定位、量化与排序；除「事故清理」外不含任何代码改动。

## 〇、测量时间、方法与可信度边界（务必先读）

- **测量时间**：2026-09-12 19:35–20:18 CST。库 `cube_v3` / OpenGauss 7.0.0-RC3，本机 `poufennode01`（10.3.100.179），16 vCPU。
- **执行方式**：6 条并行工作流，每条走「探测 → 对抗性验证 → 最小改动设计」；另有 1 个跨工作流完备性评审。19 个 agent 全部完成。
- **口径纪律**：全部微基准在**session 级 TEMP 表 / 本地进程**内完成；真实表只被只读访问；禁止提交 Ray job、禁止写业务表（唯一例外见 §0.1 事故）。
- **噪声边界（重要）**：本机在测量期间同时跑 6 个 agent，`load average` 10–15/16 vCPU。**同一基准的会话间漂移实测 1.3–2.1×**（个别项 8×）。
  → 因此本文档中**只有"行数 / 条目数 / 页密度 / 集合相等性"这类整数是可作硬判据的**；所有绝对 µs/ms 值只能当量级参考（±30% 起，跨会话可能 ±2×）。
  → 凡"两个工作流对同一现象给出 2–3 倍差异"的数字，本文档一律标注为**未定值**并给出统一口径的方法，不选边。
- **与 handoff 的关系**：handoff 的原始结论**保留不改**，但本文档修正了其中 3 处口径错误（§4.4），后续引用请以本文档为准。

### 0.1 数据完整性事故（必须先处理）

一条探测工作流的 harness 在同一未提交事务内回滚了 TEMP 影子表的创建，导致随后两次"影子基准"**实际写入了生产表 `rs_cube_cell_fact`**。

独立复核（只读，2026-09-12 20:20）：

| 项 | 实测 |
|---|---|
| 表总行数 | 260,400（事故前 60,400） |
| 污染行数 | **200,000**（`run_id='probe'`；两个 `cube_version`：`probe-one-…` 100,000 + `probe-batch-…-0..9` 各 10,000） |
| 原有数据 | 60,400 行未被修改（`n_tup_upd=0`） |
| 写入时间窗 | 19:47:01.513 – 19:47:32.278（即 ingest 工作流测量窗口 19:35–19:48 **跨在事故两侧**） |
| 体积 | heap 41 MB → 174 MB |

影响：假格网行（`space_code` 带 `-<i>` 后缀、`cube_version` 不在 `partition_output_versions` 中）会污染 AOI 读取与容量口径；且该表的体积/统计已不可作为基线。

**建议清理（需 owner 授权后执行，勿盲目跑）**：

```sql
-- 同一事务内先断言再删
SELECT count(*) FROM rs_cube_cell_fact WHERE run_id='probe' AND cube_version LIKE 'probe-%';  -- 期望 200000
SELECT count(*) FROM rs_cube_cell_fact WHERE run_id<>'probe' OR cube_version NOT LIKE 'probe-%'; -- 期望 60400
DELETE FROM rs_cube_cell_fact WHERE run_id='probe' AND cube_version LIKE 'probe-%';
-- 提交后: VACUUM (ANALYZE) rs_cube_cell_fact;
```

清理后，任何涉及 `rs_cube_cell_fact` 的性能/容量数字都必须重测。

---

## 一、结论 TL;DR

### 1.1 三个维度当前的真实瓶颈（均已实测 + 独立复现）

| # | 维度 | 瓶颈 | 关键实测 | 优化向量 |
|---|---|---|---|---|
| 1 | 入库（promote） | **逐行写目标表的随机页 I/O**，不是 SQL 写法 | 生产 1.85–2.05 ms/写入行；同构 TEMP 表 0.023–0.038 ms/行 → **49–64×**；生产索引密度 4.3–4.6 entries/page vs 新鲜 26–83（**膨胀 6–9×**）；冷/热随机叶页探针差 **84–162×**；真冷读 680 µs（`track_io_timing`） | REINDEX + 索引常驻率；并发掩蔽 I/O；见 §4.2 |
| 2 | 剖分（实体 isea4h） | **cover 冷启动悬崖 + worker 侧不可解释的 75%** | 8.0°→8.5° AOI 使 cover 从 25.2 ms 跳到 3,817 ms（**151×**）；生产 radar 极区 bbox 3.616 s vs 小景 0.033 s；L6 冷索引 3.86 s 只取决于 grid_level、与 AOI 无关；另：缓存命中每次仍做全文件 sha256（549 ms → 3 ms，**149–179×**） | 见 §3.2；且必须先补相位归因（§7） |
| 3 | 剖分（逻辑）/ 格网 | **worker 侧重复发射与重复几何**、**cover 逐 cell 的 shapely 与 Python 位运算** | 发射 101,664 行只对应 57,692 唯一 output_id（**1.762×**）；radar grid_cells 放大 6.553×；cover 调用 808 次对 168 个唯一 shard；geohash cover 80 µs/cell 中 62–71% 是 shapely box+intersection+area | 见 §2、§3.1 |

### 1.2 三个"看起来是瓶颈但已证伪"的目标（不要再投预算）

| 曾被怀疑 | 实测结论 |
|---|---|
| `NOT EXISTS` 反连接 / `DISTINCT ON` | 反连接热态仅 **+0.007–0.016 ms/行**；反连接探针命中的正是紧随其后 INSERT 要写的同一张 pkey 叶页。重写为 `DELETE + 纯 INSERT` 在热态净收益 ≈ 0 |
| 9 列宽复合唯一索引拖慢插入 | TEMP 实测加/不加该索引只差 **+4.4–5%**（95.3→99.5 µs/行）。它主要是**体积**问题（tiles 529 MB + indexes 493 MB），不是 promote 速率的主杠杆 |
| `VACUUM`/`ANALYZE` 能救 promote | `VACUUM` 后 0 行时索引字节数**完全不变**（实测 32,997,376 B）；索引膨胀不可回收，只有 REINDEX 有效 |

### 1.3 必须补的关键测量（最大缺口）

1. **实体剖分 171 s 的相位归因**：逐元素成本之和只有 25–39 s，**>75% 无解释**。代码里已埋点（`partition_dataset_runner.py:766–930` 的 `source.cache/download/checksum`、`grid.cover`、`entity.tile_mask/write/checksum`、`minio.tile_stat/upload`），只需把 `workers[].phases` 取出来对账。这是全仓库唯一能把 140 秒变成秒级账单的动作。
2. **DB 写并发扩展性**（1/2/4/8 连接）：决定 promote 是"每行 6–8 次冷页 IOPS 受限"还是"CPU/WAL 受限"，进而决定"加并发"与"REINDEX"的相对权重。全部现有探针都是单会话。
3. **Web/AOI 读侧**：6 个工作流对 `cube_web` endpoint 与 `cube_split/read/aoi_reader.py` **零覆盖**——没有任何 p50/p95、没有 JSON/pydantic 序列化成本、没有"用户查一个 AOI 要多久"。

---

## 二、格网速度

### 2.1 实测基线（本机单线程，量级参考）

| API | 成本 | 备注 |
|---|---|---|
| `geohash` locate_space_code L6/L9/L12 | 5.8 / 8.1 / 9.9 µs/op | 跨 level 平坦 |
| `geohash` locate 10k 批量 | 6.6–6.7 µs/pt（≈150k pt/s） | |
| `mgrs` locate_space_code L1/L3 | 14–16 µs/op | 编码本身便宜 |
| `mgrs` locate 10k 批量 | 14.8–16.4 µs/pt | 大量时间在返回几何 |
| `isea4h` `locate_cell` 标量 | **6.1 µs/pt，与 grid_level 无关**（res 0–12 波动 <5%） | 23 次三角函数/点 |
| `isea4h` `locate_cells` 批量 | 2.55 µs/pt（2.4×） | 尾部 `tri→q2di` 仍逐点，占 56% |
| `isea4h` `locate_point`（含 geometry） | **482–507 µs/pt = locate_cell 的 78–80×** | `_make_cell` 454 µs，且每个点算两遍环 |
| `geohash` cover | **80 µs/cell，L4–L6 完全平坦**（无 O(cells²)） | 62–71% 在 shapely `box + intersection + area` |
| `mgrs` cover | 冷 **603** / 热 **285** µs/cell（2.11×） | 热态 57% 是邻居枚举 |
| `isea4h` cover 冷索引 | L4/L5/L6 = 251 / 967 / 3,857 ms | **Θ(cell_count(level))，与 AOI 大小无关** |
| `isea4h` cell 几何 | `code_to_geometry` 365–411 µs/cell（shapely ≈ 346 µs） | |
| `isea4h` 拓扑 | neighbors 6.4 / children 14.2 / parent 25.3 µs | `parent` 25% 是死代码 |

### 2.2 已量化热点（含独立复现）

**A. `isea4h` cover 的 8° 阈值悬崖（单体收益最大）**

判据在 `isea4h_engine.py:248–263`：`|Δlon|≤8 且 |Δlat|≤8 且 max_lon<170 且 |lat|<80` 才走 WALK，否则若 `cell_count(level) ≤ 200,000` 就构建**全层 STRtree 包络索引**。

- 实测：`8.0×1.0` 度 = 26.0 ms（WALK）→ `8.5×1.0` 度 = 3,873.6 ms（INDEX），**149×**；对高纬维度对称（`1.0×8.0` = 28.7 ms → `1.0×8.5` = 3,693.4 ms）。
- 已独立复现于生产侧：radar 极区 bbox 70 cells = 3.616 s（另一个工作流的 live SDK 实测），而 product 24 cells = 0.034 s、optical 27 cells = 0.033 s（走 WALK）。
- **WALK 与 INDEX 的 `space_code` 集合在 27 组场景（含极区、跨日期变更线、quad 边界、细长条、14×11 省域）实测 `identical=True`、missing/extra 均为 0。**
- 代价分解（L6 冷 3.86 s）：标量 `snyder_inv` 245,760 次角投影 / **952,402 次 Newton 迭代** = 1.24 s；6 旋转候选 `normalize_ring_longitudes` ≈ 0.95 s；`shapely.box` 40,962 次 ≈ 0.91 s。

**B. `intersects_area` 的 5×5 经度变体网格（低风险高收益）**

`isea4h_engine.py:299–305` 对每个候选做 `5(cell 变体) × 5(target 变体)` = 最多 25 次 intersection。代数上等价于「cell 不动 × target 平移 d∈[-4,4]」共 9 次：

- 实测最坏（无命中）1,152–1,190 µs vs 单次 intersection 11 µs（**103–112×**）；单个候选最多发 25 次 intersection + 5 次 `affinity.translate`（29.5 µs/次）。
- 补丁副本实测（A/B 交错、best-of-3）：生产型景 res6 intersect 冷 19.4→12.2 ms、热 17.8→11.3 ms（**1.58×**），省域 res8 WALK 1,734→1,174 ms（1.48×），输出集合一致。
- 关键顺序：`relative_shifts = (0,-1,1,-2,2,-3,3,-4,4)`，`d=0` 优先（命中场景 95.2→25.5 µs）；用 `range(-4,5)` 顺序反而变慢。
- 改动面：**单文件内部闭包**，无 API/DDL/配置/依赖变化。

**C. `geohash` cover 的两个独立改进**

- 向量化 `shapely.box + intersects`：实测 82.4→17.8 µs/cell（4.53×），1×1° L6 `identical_set=True`。
  **但对抗性验证证伪了"通用等价"**：纯 `intersects` 会把零面积接触的 cell 选进来，违反 `intersect` 语义与 `cube_encoder/tests/test_geohash_engine.py:294 test_cover_boundary_contact_excluded_from_intersect`。**保留 `area>0` 门后实际约 2.9×**（27.9 µs/cell），省级 L6 预期 55 s→~19 s（不是 12 s）。
- 候选枚举改整数索引递增（去掉 `decode→shift→encode` 往返）：实测每 cell 调 `_decode_bbox` 4.01 次、`_encode` 1.00 次、`_validate_space_code` 4.01 次（逐位复现）；`_cells_for_bbox` 单独 11–12.9 µs/cell（占 cover 16%）。

**D. `mgrs` cover 的结构性开销**

- `(domain, band)` 的 `domain∩band` 裁剪多边形**未缓存**：加缓存后单 cell 224.9→149.6 µs/cell（**−33.5%**），300 个码几何逐字节等价。机械改动、单文件。
- 邻居枚举：每 cell 枚举 18 个邻居码（8× `UTMToMGRS` + 8× `toMGRS`），实测 80,244 次调用对应 13,612 个唯一码（**5.90× 冗余**），占热覆盖 57%。
- `decode_utm` 内 `warnings.catch_warnings(record=True)` 使单次解码 6.2→8.9 µs（**+44%**，报告原值 +71% 因裸基线偏差），并制造 71,338 次 `ctypes.create_string_buffer`。
- 单 cell 环构造 `_utm_raw_geometry` 108–117 µs，其中纯 Python densify 29 µs、pyproj 32 点变换、`normalize_ring_longitudes` + Polygon 93 µs。
- 规模外推（有实测支撑的推断，非端到端）：p2 省级 41.7 万 cell ≈ 259 s；p3 省级（4.2e7 cell）数小时级——**注意这是"外推的外推"，不可作为验收基线**。

**E. `isea4h` 拓扑与对象开销**

- `cell_parent` 在 L6 全层 40,962 次调用中 `cell_children=1.000/call`、`cell_neighbors=1.000/call` → 为扩展而算的 6 邻域**从未被使用**，白付 25%（全层 1,074 ms → ~800 ms）。
- `engine.*` 包装层每次返回 `GridAddress`（pydantic，1.53 µs/个）：neighbors 6.4→20.9、children 14.2→29.0、parent 25.3→28.7 µs。**批量拓扑 API 返回对象形态的固定开销 3–14 µs/cell。**
- `locate_point` 每个点把 `cell_boundary_polygon` 算两遍（计数器实测 2 次/点）。
- 向量化可行性已被两条独立路径证实：`np_locate` 与标量 `locate_cell` 在 res 1/6/9 × 20,000 点 **0 处不一致**；`np_snyder_inv` 0/20,000、最大差 7.99e-15 rad、逆投影 5.03→0.71 µs/pt。
  **但注意**：`_resolve_overage` 实测 **98.48% 单轮 / 1.52% 需要两轮**（原报告称 100% 单轮）；按"单轮掩码"实现会算错这 1.5% 的点。另外 `np_locate` 的加速比在同进程复测为 **2.4×**（非 3.76×），对内存带宽敏感。

### 2.3 建议顺序

1. `intersects_area` 5×5→9 平移（单文件、输出等价、1.48–1.58×，含生产型景与省域 WALK）。
2. `mgrs` `(domain, band)` 裁剪缓存（单文件、逐字节等价、−33.5% 单 cell）。
3. 去掉 `decode_utm` 的 `catch_warnings`（保留非法 band 抛 `ValidationError` 的回归用例）。
4. 消除 `isea4h` 6 旋转候选 + 环复用（L6 冷索引约 −0.95 s；`locate_point` 少算一遍环）。
5. 大 AOI 走 WALK 替代全层索引（35.7×）——**正确性风险最高**，必须保留索引路径作 fallback 并做穷举对照。
6. `geohash` 向量化（须保留 `area>0`）、整数候选枚举、邻居码算术化：收益真实但需要逐案差分测试。

---

## 三、剖分速度

### 3.1 逻辑剖分（geohash / mgrs）Ray 链路

真实批次 `partition-9890fd667e48`（mgrs L1，4 数据集）attempt 133.261 s：

| 阶段 | 耗时 | 占比 |
|---|---|---|
| `promote_logical_staging`（3 数据集**串行**） | 106.932 s | **80%** |
| `logical_stage`（worker COPY→staging） | 28.124 s / 58 chunk | 21% |
| `logical_batch_driver.ray.wait` | 20.108 s / 62 | 15% |
| `logical_worker.grid.cover` | 27.289 s / 808 次（33.8 ms/次） | 20% |
| `logical_worker.grid.geometry` | 3.809 s / 27,908 次 | — |
| `logical_worker.logical.chunk_serialize` | 8.199 s / 58 | — |
| `logical_worker.minio.chunk_upload` | 2.402 s / 58 | — |

**新增发现（本次探测，非 handoff 已有）：**

1. **行级放大 1.762×**：整个批次发射 101,664 行，只对应 57,692 个唯一 `output_id`。
   - `grid_cells` 放大：optical 6.000× / radar **6.553×** / product 2.043×（**跨 chunk（跨 band-unit）**造成，chunk 内已由 cells dict 去重）。
   - `tiles`/`indexes` 放大 1.175–1.499×（shard 边界重叠；radar 3,256 cells 在 sharded 规划下变 4,881）。
   - geometry 占 grid_cells payload 字节的 **81%**（25.60 MB / 31.51 MB），平均 1,198–1,325 B/geometry。
2. **`sdk.cover` 按 (asset, band) 单元重复执行**：radar 1 asset × 6 band → 同一 bbox cover 6 次；808 次调用对应 168 个唯一 shard（radar 720→120、optical 48→16、product 40→20）。
3. **in-flight 被默认上限 4 封顶，不是算力**：`worker_container_limit=0` → `_logical_task_limit()` 退回 `CUBE_LOGICAL_MAX_IN_FLIGHT=4`；worker 总占用 76.048 s / ray.wait 20.108 s = **有效并行 3.78**，只观测到 4 个 pod；driver 自身几乎无工作（submit 0.095 s、result_get 0.014 s）。
4. **promote 的 SELECT 侧不是瓶颈**：用真实 payload 重建 79,908 行 staging 后对真实目标表跑三条 SELECT，radar 合计仅 1.098–1.757 s（占 66.262 s 的 **1.7–2.7%**）。
   ⚠️ 该结论**对统计信息敏感**：不加 `ANALYZE` 时计划退化为 Nested Loop Anti Join，验证方实测到一次 **14.747 s**（tiles 9.168 + indexes 5.370）。生产 `partition_logical_staging_rows` 在 promote 时刻 `n_live_tup=0` 且 `last_analyze` 晚于该批次，因此生产计划类型**必须用真实 staging 表确认一次**。
5. **staging COPY 生产 299 µs/行 vs TEMP 42–60 µs/行（5–7×）**，其中 48 次 `psycopg.connect`（中位 18.1 ms）≈ 0.87 s 可直接扣除。
6. **死代码**：`partition_dataset_runner.py:326–475` 的 `_run_logical_dataset_on_ray` 全仓库无调用点，是逻辑路径的第二份实现（同样逐 shard cover、逐 cell geometry，无 staging、无 timing），会持续漂移并误导性能分析。
7. 任务粒度本身合理：58 个 task，单 task 固定开销 ≈ connect 21.9 ms + MinIO stat 6.4 ms + put 41 ms；最慢 task 只是均值的 1.7–2.1×（无长尾）。

**收益修正（重要）**：worker 内按 `(kind, output_id)` 去重的**真实收益是 shard 重叠那部分 ≈ 17.6%（17,940 行/批次）**，不是 43.2%——`grid_cells` 的 6× 放大跨 chunk，单 chunk 去重拿不到。任何把 "R1 去重 −43%" 与 "R2 cover 复用 808→168" 相加的算法都是重复计数。

### 3.2 实体剖分（isea4h）+ 碳卫星 worker IO

已实测背景：attempt 171.0 s 中 DB 阶段合计约 7 s，其余为 Ray worker（下载 577 MB/594 MB float64 源、转 COG、写实体瓦片）+ `ray.wait`（累计 2,070 s / 154 次）。

**结构性问题（逐条独立复现）：**

1. **生产入口不是 `entity_partition_job._write_entity_tiles`**，而是 `cube_web/services/partition_dataset_runner.py` 的 `_run_dataset_on_ray.execute`；任务粒度 = 每 asset 切 `ray_parallelism`（默认 16）个空间 shard，每 shard 一个 Ray task。→ **针对实体 IO 的优化必须改 `partition_dataset_runner.py`**，改 `entity_partition_job.py` 对生产无收益。
2. **同一 asset 的 N 个 shard 各自独立调用 `cache_source_cog`**：product 与 optical 各 2 个 asset 时，24 cells @par16 产生 16 个非空 shard → 同一 URI 被下载/校验多次（最坏 = task 数，最好 = 被调度到的节点数）。
3. **缓存命中仍付全文件 sha256，且在 flock 内串行**（`ray_partition_core.py:104–170`）：命中路径 = `LOCK_EX → LOCK_SH → stat_object → _local_file_identity(1MB chunk 全文件 sha256) → 比对`，全部在锁内。
   - 实测：真实 577 MB 对象，**命中 549.6 / 529.6 ms（min 521 / 517）→ 改 stat 身份（size+mtime_ns+ino）后 3.07 / 3.55 ms＝149–179×**；每次省 ~527–547 ms。4 线程同 URI 并发命中的 `LOCK_EX` 串行化同时被消除。
   - 本机 sha256 速率实测 981–1,028 MB/s（线性于文件大小，1.44 GB 源约 1.5 s/次）。
4. **每 tile 2 次 MinIO 往返只对小对象成立**：`stat_object` 8.64 ms 是**环境噪声**（同 prefix 三次重复稳定在 ~3 ms）；且 minio-py 对 >5 MiB 的 tile 走 multipart（product 29 MB/95 MB tile），不是单次 PUT。
5. **输出对象粒度 = 每 (cell, band) 一个 TIFF**，尺寸跨 4 个数量级：product 均 29.09 MB / optical 均 465 KB（464–475 KB）/ radar 均 5.23 KB。radar 用 222 个对象只承载 1.16 MB。
6. **源 profile 的真相（修正探测阶段的错误前提）**：探针本地副本无压缩，但**验收源全部存在且带压缩**——product = float64+LZW（因此 tile 天然约为 raw 的 34%）、optical = int16+deflate、radar = float32+LZW+overviews。
   → 因此「继承源 profile 会导致与 raw float64 等大的 tile」和「dtype=float32 通用收益 2–3×」**对生产不成立**：float32 只对 product 有收益，对 int16 的 optical 反而约翻倍字节。
7. **WALK 已实现窗口裁剪**：真实源 24 个 cell 实测 `reads=24, distinct_windows=24, full_image_reads=0`。

**最大缺口**：实测逐元素成本之和（传输 5–9 s + worker CPU 12–26 s + stat RTT + cover）**只有 25–39 s，而 attempt 是 171 s**。残差方向两个工作流一致（都远小于 171 s），但归因相反（调度/锁/ENOSPC 重下 vs 源 profile）。**必须做 §7.1 的相位归因**才能定价下面 5 条候选。

**候选（收益因缺口未定级）：** 缓存命中改 stat 身份（已实测 149–179×/次）＞ shard/asset 节点亲和（把 ≤16 次冷下载压到 ≤4）＞ 实体瓦片只在 float64 源降 float32＋ZSTD（新 output_version）＞ 仅当 cell 数极少时下调并行度默认值 ＞ pre-put `stat_object` 移出关键路径。

**明确不要做**：合并小 tile 对象（会撞 `partition_object_store` 的 manifest 去重校验，且 UNIQUE 约束含 `space_code`，必须放弃 per-cell 行）；`min(parallelism, len(cells))` 主机制无效（24 cells 时 `min(16,24)=16`，与现状相同）。

---

## 四、入库速度

### 4.1 入库作业（ray_ingest / product / carbon / entity）

当前规模下入库**不是瓶颈**的机制已实测清楚：单次 ingest 作业固定下限 **0.21 s**（4 个空跑 run 实测 0.2077–0.2547 s，与数据量无关），数据集 run 的 2.97–7.93 s 主要不是 DB 写。当前每数据集只有 10–50 行。

**四条结构性缺陷（全部独立复现，与规模无关）：**

| # | 缺陷 | 实测 |
|---|---|---|
| 1 | `ray_ingest_job` 的 **`batch_size` 是无效旋钮**：`upsert_raw_assets_postgres`(693–748) 与 `upsert_cube_facts_postgres`(751–853) **完全不引用** `batch_size`，一次 ingest 全部行进同一 temp 表/同一 MERGE/同一事务 | `batch_size=1` 与 `1000` 产生**同样 4 条语句**（CREATE TEMP/COPY/MERGE/DROP）；`rows=values` 直接喂全量 |
| 2 | `product_ingest_job` 走**参数化 VALUES-MERGE**，每 1000 行一条语句 | M=483,734（生产体量）N=5,000：**0.9134 s → 0.1911 s（COPY+单条 MERGE）= 4.98–5.95×**；N=20,000 时 2.65–3.94×；两条路径 EXPLAIN 计划形状相同，差距来自 17,000 个绑定参数的解析/计划/取值 |
| 3 | `carbon_ingest_job:238–241` 的 MERGE ON 键被 `CAST(target.x AS VARCHAR(n))` 包裹 → **唯一索引完全不可用** | 计划 = `Hash Right Join + Seq Scan on rs_carbon_observation_fact (227 MB)`；耗时 0.414–0.512 s 且**与 N 无关**（O(M) 固定下限）。去掉 CAST 后计划变 `Index Only Scan`，no-CAST 版本执行成功、`delta=N`、同 `cube_version` 重跑 `delta=0`（幂等成立） |
| 4 | `entity_partition_job:1231` 用**逐行 `executemany` MERGE**（26 个 `%s` 的单行 VALUES，不 COPY、不分批） | N=100：0.0294 s → COPY+单 MERGE 0.0316 s（1.22×）；N=500：0.1211 → 0.0781 s（**1.74×**，0.249→0.156 ms/行）；当前表仅 840 行，绝对收益 <0.06 s |

**规模相关（可复现的部分）：**
- `DISTINCT ON` 排序落盘阈值实测在 **N = 60,000–100,000 之间**（`work_mem=64 MB`；N=60,000 `quicksort Memory: 63793kB`；N=100,000 `external merge Disk: 67112–70400kB`，排序 0.407–0.469 s）。
- COPY 单行成本恒定 **13.6–14.6 µs/行（70–74k rows/s）**，与 M、N 无关。
- 目标表体量 **M 从 6e4→1.03e6（17×）只让 MERGE 单行成本从 38 µs 涨到 41 µs（+8%）**。

**索引与统计体检（只读，已复现）：**
- `rs_entity_tile_asset` 唯一索引 **6,085 B/行** vs 同键新建 **351 B/行（17–24×）**；`rs_cube_cell_fact` uq 当前 138.9 B/行（原报 399 B/行 已被事故污染，不可再用）。
- `rs_carbon_observation_fact` 的 3 个二级索引合计 **77,168,640 B（256.9 B/行）**，`idx_scan=0`、`idx_tup_read=0`，全仓库仅出现在 `carbon_ingest_job.py:81/87/94` 的 `CREATE INDEX`——**纯写入负担**。
- `rs_product_cell_fact` 实际 483,734 行，但 `pg_stat_user_tables` 报 `n_live_tup=0 / n_tup_ins=0 / last_autoanalyze=NULL`（`reltuples=483,448`）；`rs_carbon_observation_fact` 统计停在 2026-09-08。**用 `pg_stat_user_tables` 做容量判断会得到错误结论。**

### 4.2 `promote_logical_staging`（当前最大单体瓶颈）

**生产 vs 同构 TEMP（这是全文最硬的一组对照）：**

| 场景 | 吞吐 | 每行 |
|---|---|---|
| 生产 promote（26,024 对 tile+index，52,248 行 + 5,644 grid_cells） | **488–541 行/s** | **1.85–2.05 ms/行** |
| TEMP 复刻，目标表预置 292k–312k 行，生产 SQL 形状 + 真实 DDL/索引/行宽 | 26,292–43,319 rows/s | 0.023–0.038 ms/行 |
| 差距 | **49–64×** | |

**逐项排除（全部实测）：**

| 假设 | 实测 | 结论 |
|---|---|---|
| SQL 形状（`DISTINCT ON` + `NOT EXISTS`） | 反连接热态 **+0.007–0.016 ms/行**；DELETE+纯 INSERT 在热态净收益 ≈0 | ❌ 不是主因 |
| 索引数量/宽度 | 加/不加 78 MB 宽唯一索引仅差 **+4.4–5%** | ❌ 不是主因 |
| 目标表行数 | TEMP 表预置 292k–312k 行仍 26–43k rows/s | ❌ 不是主因 |
| FK 校验 | 两个真实父表 FK 实测 **+0.037 ms/行（~1%）** | ❌ 不是主因 |
| 局部索引 | `idx_partition_tiles_searchable`（谓词要求 `publication_status='published'`，promote 插入默认 `pending`）与 `idx_partition_indexes_tile_output_id`（谓词 `tile_output_id IS NOT NULL`，promote 显式插 NULL）**都不被 promote 触碰**（条目数 826 / 72,592） | ❌ 不是主因（handoff 把它们算进每行成本是错的） |
| 堆 bloat | `n_dead_tup≈0`，但**索引 bloat 严重** | ⚠️ handoff 混淆了堆 bloat 与索引 bloat |
| **索引页密度（真实主因）** | 生产 `partition_indexes_pkey` 12.5 entries/page（655.6 B/entry）、9 列 uq **4.3–4.6 entries/page（1,759–1,890 B/entry）**、`tiles_pkey` 14.6；同构 TEMP 新鲜密度 **26–83 entries/page** → **膨胀 6–9×**。`VACUUM` 后 0 行时字节数完全不变；一轮 delete+refill 永久放大 2.0–2.7×（三轮 ~3.1× 平台）；生产 pkey 达新鲜密度 5.1× | ✅ |
| **随机叶页延迟** | 冷/热探针 **84–162×**（0.971–1.344 ms vs 0.005–0.008 ms）；`track_io_timing` 实测真冷读 **680 µs/次**（原报 1.67–1.98 ms 是"总墙钟 ÷ 物理读次数"，高估 2.4–2.9×）；顺序扫描带宽正常（489 MB heap scan 0.289 s） | ✅ |

**机制自洽性**：每输出行约 4.1 KB 堆+索引（indexes 行均 948 B + uq/pk 2.4 KB；tiles 787 B + 2.5 KB），26,024 行 ≈ 208 MB / 107 s ≈ **1.95 MB/s** —— 这是**随机 I/OPS 受限**的速率，不是带宽受限。

**尚未解释的残差**：`partition_logical_staging_rows` 清空后仍持有 **46–48 MB 索引**（pk 28 MB + version_kind 18 MB），每次重新加载都在一张"空但有几万页索引"的表上写；staging 一跳本身实测 1,772 rows/s（0.56 ms/staging 行，每输出行 2 个 staging 行 ≈ 1.1 ms/输出行，占 27%）。

**候选（按证据强度）：**

1. **REINDEX 三张目标表**（`REINDEX INDEX CONCURRENTLY`，逐索引、autocommit）：唯一能消除既有膨胀的动作（实测可回到 96–127 B/entry）。**必须绑进清理周期**，否则下一个 delete+refill 周期即回退 2×。
   ⚠️ 收益**未端到端验证**：没有任何一次"REINDEX 后在生产表上跑 promote"的测量；且 REINDEX 不修复堆膨胀（实测堆文件是活跃行字节的 1.6–1.85×）。把「3–10×」当预期收益是**未验证假设**，只能作为方向。
2. **去掉 staging→目标表二次搬运**（省 ~1.1 ms/输出行 + 一次 JSONB 重解析）；或 **COPY 装载替代 `INSERT…SELECT`**（TEMP 实测 COPY 39.0 vs executemany INSERT 129.3 µs/行 = 3.3×；另一工作流实测 COPY 比 executemany 快 4.5–5.7×）。
   风险：COPY 无法带 `WHERE NOT EXISTS`，会丢掉幂等门；`verify_output_chunks` / `_iter_persisted_chunk_rows` 依赖 staging 做对象清单校验。**必须先保留不可变性校验**。
3. **删/收窄 9 列复合唯一索引**：语义上 `output_id = sha256(dataset_id, output_version, source_asset_id, band_code, grid_type, grid_level, space_code, topology_code, time_bucket[, window_identity])`，即主键本身是这些列的函数（数据侧验证：按 9 列分组 `distinct output_id > 1` 的组 = **0**；去掉 `tile_kind` 或 `st_code` 后冲突数仍为 0；当前 `topology_code` 全为 NULL）。
   不过两个工作流的收益口径**互相冲突**（一个报"每行 2 次索引页操作、25–40%"，另一个实测"仅 +5%"）→ **先统一口径再决定**；且与 REINDEX 收益不可相加。
4. 按 `(dataset_id, output_version)` 分区或按版本重建索引（新版本行只落新叶页）：OpenGauss 对分区表 FK/CASCADE 支持未验证，属破坏性 DDL。
5. 处理 `partition_tiles` 的 **0% HOT**（`n_tup_upd=74,794 / n_tup_hot_upd=0`，因 `idx_partition_tiles_searchable` 谓词含 `status/publication_status`）：收益全在**发布路径**，对 promote 为 0。
6. 分语句计时（`promote.tiles` / `.indexes` / `.grid_cells`）：零风险，是归因 ~13 s/数据集固定项的**前提**（需同步 `cube_web/tests/test_scene_api.py:797/815` 的阶段字典断言）。

### 4.3 已证伪的入库数字（不要引用）

- ~~单条 MERGE 在 N=1e5 超线性退化 2.7–3.2×（103–132 µs/行）~~ → 环境噪声：同一操作在同一 harness 内有 **4.8762 s 与 10.3139 s** 两条记录（内部差 2.11×），第三次独立测量 3.3449 s。COPY 侧三方一致（12.8–13.9 µs/行）。
- ~~幂等重跑比首跑贵 4.6–5.8× 且随表体量增长~~ → 跨 run 比对造成的混淆；同一 run 内为 2.70× / 4.45× / 1.12×，波动几乎全部来自 `commit_s`。
- ~~1e6 行 130–150 s 外推（含 WAL/checkpoint）~~ → 基线已证伪，WAL 部分从未实测。
- ~~`rs_cube_cell_fact` uq 膨胀 3.2×~~ → 该表已被事故污染，事故前稀疏度不可分离。
- 因此「切批收益 1.5–2.5×」不成立；切批的真实收益只有"消除 N≥1e5 的那次 ~0.47 s 排序落盘 + 降低单事务 WAL/commit 峰值"。

### 4.4 对 handoff 的 3 处口径修正

1. **倍数修正**：handoff 的「243 rows/s」分母是 `(tile,index)` 对，TEMP 的分母是单条目标表行 → 「142–178×」应修正为 **「49–64×」**（统一为"每写入行 ms"：生产 1.85–2.05 ms/行）。
2. **"不是 bloat"修正**：`n_dead_tup≈0` 只说明**堆**没有死元组；**索引**页是空着的（VACUUM 不回收），生产 pkey 稀疏度达新鲜密度 5.1×。
3. **"缓冲区 miss 1.67–1.98 ms"修正**：该值用总墙钟除以物理读次数，把 CPU/协议开销算进了读延迟；`track_io_timing` 直接测得真冷读 **680 µs**。另：`db_blks_read` 统计的是 `read()` 系统调用，可能由 OS page cache 服务，**不能用它论证磁盘带宽**——后续诊断请用 `pg_stat_database.blk_read_time` / `pg_stat_statements` 的 io 时间。

---

## 五、优先级（跨工作流合并，按证据强度 × 收益 × 落地成本）

| 优先级 | 动作 | 证据 | 落地 | 备注 |
|---|---|---|---|---|
| **P0** | 清理 `rs_cube_cell_fact` 的 200,000 行污染 + 给所有影子基准加防误写断言 | 高（整数可复算） | 需授权，5 分钟 | 前置门：否则一切容量/基线不可信 |
| **P1** | 索引膨胀治理（`REINDEX INDEX CONCURRENTLY` 四张表）+ 接入清理周期 | 高（密度逐位复现；VACUUM 无效、迟滞 2–2.7×/轮） | 中（需维护窗口，在线重建） | 唯一"硬证据 + 立即可做"的优化动作；收益幅度仍待端到端验证 |
| **P2** | `isea4h` `intersects_area` 5×5→9 平移 | 高（输出等价 + A/B 1.48–1.58×） | 低（单文件闭包） | 单体性价比最高；含极区/跨经线回归 |
| **P3** | `cache_source_cog` 命中改 stat 身份（去掉锁内全文件 sha256） | 高（149–179×/次，含并发串行化消除） | 低（单文件 + 3 个单测） | 保留 sidecar sha256 作审计与迁移兜底 |
| **P4** | 入库三处机械改动：product COPY（4.98–5.95×）、carbon 去 CAST、entity COPY（1.2–1.74×） | 高（同表同 N 的进程内 A/B） | 中低（需同步契约测试断言） | 当前规模绝对收益小；为规模增长预留 |
| **P5** | `mgrs` `(domain,band)` 裁剪缓存 + 去 `decode_utm` warnings | 高（−33.5% 单 cell / +44% 解码） | 低（单文件） | 逐字节等价 |
| **P6** | 逻辑 worker 去重（真实 −17.6%）+ 每 asset 一次 cover/geometry（808→168） | 中高（整数行数硬判据；收益归因已被修正） | 中（chunk 幂等需加计划版本盐） | 两件事不可相加；改 chunk 内容必须改 chunk 身份 |
| **P7** | 大 AOI 走 WALK 替代全层索引（35.7×）、`isea4h` 6 旋转候选/环复用、向量化 | 高（27 组集合等价、0/20,000 数值一致） | 高（正确性风险） | 保留索引 fallback + 穷举对照 |
| **P8** | 死代码清理（`_run_logical_dataset_on_ray`）、分语句计时、`cell_parent` 死工作、目标表索引瘦身 | 中 | 低 | 可顺手做 |
| **P9** | 补测量：实体相位归因、DB 并发扩展性、Web/AOI 读侧、质量阶段伸缩 | — | 低 | 见 §7 |

---

## 六、缺失测量清单（按价值排序）

| # | 缺什么 | 怎么测 | 能回答什么 |
|---|---|---|---|
| 1 | 实体剖分 171 s 的相位归因（**>75% 无解释**） | 从 attempt 返回体取 `raster_batch_driver.workers[].phases` + `counters`（`source.cache/download/checksum`、`grid.cover`、`entity.tile_*`、`minio.tile_*`、`source_cache_hit/miss_count`）与墙钟对账；或用 `CUBE_ENTITY_RAY_PARALLELISM=2` 跑 smoke | 决定实体路径投缓存/亲和/并行度/瓦片 profile 中的哪一条 |
| 2 | DB 写并发扩展性（1/2/4/8 连接） | session TEMP 同构影子上开 N 连接跑生产形状的 `INSERT…SELECT DISTINCT ON…NOT EXISTS`，记录总 rows/s、`wait_event`、`blk_read_time` 增量 | promote 是 I/OPS 受限还是 CPU/WAL 受限 → 决定"加并发"与"REINDEX"的相对权重 |
| 3 | Web API 层吞吐与延迟（6 个工作流零覆盖） | 进程内 FastAPI 对只读 endpoint 打 1/8/32 并发，记录 p50/p95/p99 + cProfile 拆 pydantic 序列化 vs DB 往返 | 用户可感知延迟在哪；大 AOI 响应是否被序列化主导 |
| 4 | AOI 读取路径端到端（`aoi_reader.py` → `cover_compact` → `partition_*`） | 只读跑 1°/8°/省级 AOI，分相位计时（SQL 扫描 / cover / 几何序列化）+ `EXPLAIN (COSTS)` | 用户消费剖分成果的成本；也验证"删 9 列 uq"对读侧是收益还是风险 |
| 5 | 质量阶段伸缩性（现只有 N=10–50 单点） | N=50/500/5000 资产跑同组 9 条规则，测串行与两条 IO 规则（`asset_readability`/`asset_crs` 占 95%+）并行化后的墙钟 | 大批量剖分时质量门禁是否成为瓶颈 |
| 6 | 缓存/索引的 RSS 与 GC 预算 | 常驻 isea4h L0–L7 索引（L7=58.6 MB）、`cell_geometry_clipped` lru 提到 65536、冷 cover，记录 `VmRSS` 增量与 `gc.get_stats()` | 两个工作流都在建议加缓存，但都没有内存上限依据 |
| 7 | 真实 workload 分布 | 只读统计 `partition_job_attempts` / `partition_datasets` / `partition_grid_cells` 的 asset 数、AOI 面积、band 数、行数分布 | 为所有外推锚定分母（"1000 景 = 15 h"、"p3 = 7.6 h" 目前都缺锚点） |
| 8 | 争议绝对值的误差棒 | 同一小时内每项重复 5 次、A/B 交错、记录 load average 作协变量，报 min/median/p95 | 把"3 个工作流 headline 差 2–3 倍"从矛盾变成误差范围 |

---

## 七、复现材料

- 探测/验证/设计脚本：`/tmp/perf-probe/{grid-speed,isea4h,logical-ray,partition-entity-io,verify-ingest,plan-ingest,verify-geohash-mgrs,against-isea4h,design,...}/`（临时目录，未入库；如需长期保留应整理进 `cube_split/scripts` 或 `cube_web/scripts`）。
- 工作流完整产出（19 个 agent 的原始 JSON）：`/tmp/perf-probe/results/{probe,verify,design,critique}__*.json`。
- 既有基准设施（**本次未使用，建议后续先用它建回归基线**）：`cube_encoder/grid_core/app/perf_smoke.py`（含 `geohash_locate` / `mgrs_cover_intersect` / `topology_batch_geometries_20` 与 `threshold_ms`）与 `cube_encoder/tests/test_perf_smoke.py`。
- 真实门禁：`cube_web/scripts/run_real_partition_acceptance.py`（本文档未运行；任何 promote/实体/入库改动的收益与回归都必须在它上面复跑并 `status=passed`，`ray.wait` / Ray Job 排队单独记录、不计入 DB 写入耗时）。

## 八、边界与安全

- 本文档**不含**任何 DSN、口令、MinIO 凭据或 token。运行时凭据只从环境变量 / `CUBE_WEB_ENV_FILE` / 本地 `.cube_web.env` 读取。
- 所有微基准都遵守「真实表只读、写入实验仅用 session TEMP 表」；唯一例外是 §0.1 记录的 harness 事故（已定位、已量化、待授权清理）。
- 本文档的绝对耗时数字来自**共享集群（load 10–15/16 vCPU）单次或 best-of-3 测量**，只能作方向性依据；进入验收基线前必须按 §6#8 补误差棒，并把 `ray.wait` / 排队单独剥离。
