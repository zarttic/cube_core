# 剖分/入库优化 第 1 轮实施记录（2026-09-12）

> 上游：`docs/PERFORMANCE_OPTIMIZATION_DIRECTIONS_20260912.md`（探索与优先级）、`docs/PARTITION_WRITE_PERFORMANCE_HANDOFF.md`（写入路径 handoff）。
> 本轮范围：把探索阶段排序里的**零风险/低风险**项落地。含 2 个 commit，均未触碰公共接口与生产数据。

## 一、本轮改动

| commit | 文件 | 改动 | 风险 |
|---|---|---|---|
| `39e323c` | `cube_split/cube_split/ingest/carbon_ingest_job.py` + 对应测试 | carbon MERGE 的 ON 键去掉两侧 `CAST(... AS VARCHAR(n))` | 低（两侧本就有类型；已做执行级验证） |
| `61ad6fa` | `cube_web/cube_web/services/partition_domain_store.py`、`partition_workflow.py` | promote 三条 INSERT 各自计时（`opengauss.promote.grid_cells/.tiles/.indexes`） | 零（纯计时；SQL 逐字未变） |

**还没做的**（见 §四）：product `VALUES`→COPY、entity `executemany`→COPY、`ray_ingest` 的 `batch_size` 接通、REINDEX 与索引治理、staging→目标表二次搬运的去除。

## 二、carbon 去 CAST

### 问题

`upsert_carbon_facts_postgres` 的 MERGE ON 键把两侧都写成了 `CAST(target.satellite AS VARCHAR(128)) = CAST(source.satellite AS VARCHAR(128))`（4 个键各一对）。staging 表由显式列清单（`satellite TEXT NOT NULL, ...`）建立、由 COPY 填充，两侧本就是 `TEXT`，因此 CAST 的唯一作用是把唯一索引从规划器视野里藏掉。

只读 EXPLAIN（2026-09-12，`cube_v3`）：

| ON 子句 | 计划 | cost |
|---|---|---|
| 带 CAST（原） | `Seq Scan on rs_carbon_observation_fact target` | **38,029.56** |
| 去 CAST（新） | `Index Only Scan using rs_carbon_observation_fact_satellite_observation_id_product_key` | **8.28** |

`rs_carbon_observation_fact` 当前 300,369 行 / 227 MB。原计划下每次 carbon ingest 都付一次 O(M) 全表扫描（探索阶段实测 0.41–0.51 s 且**与批大小无关**，随表体量线性增长）。

### 验证（执行级，影子表）

用 session 级 TEMP 影子表（21 列 + 4 键唯一索引，与生产同名同键）调用**未改动的生产函数**：

| 场景 | 结果 |
|---|---|
| 首次 300 行 | 0 → 300（delta=300） |
| 同 `cube_version` 重跑 | 300 → 300（**delta=0，幂等保持**） |
| 新 `cube_version` 300 行 | 300 → 600（delta=300） |
| 同版本半重叠 150 行 | 600 → 600（delta=0） |
| 影子表计划 | `Index Only Scan using probe_carbon_uq (cost=0.00..8.28)` |
| 真实表行数 | 300,369 **未变**（全程只读真实表） |

测试：`cube_split/tests/test_carbon_ingest_query.py` 原断言「MERGE 文本里必须有 `CAST(target.satellite ...)`」，已翻转为**断言不得再有 CAST** 并写入原因（该断言曾把待修形态固化）。`cube_split` 227 passed。

## 三、promote 分语句计时

### 问题

handoff 已实测 `opengauss.promote_logical_staging` = 106.9 s / 占逻辑剖分墙钟 80%，但**不知道这 107 秒落在哪张目标表**（tiles / indexes / grid_cells），后续任何优化都缺归因。

### 改动

- `promote_logical_staging(self, result, timing=None)`：三个实现（基类 / 内存 / OpenGauss）统一接受可选 recorder；传 `None` 时用 `nullcontext()`，store 保持可独立使用。
- 三条 `INSERT ... SELECT` 分别包进 `opengauss.promote.grid_cells` / `.tiles` / `.indexes`；父阶段 `opengauss.promote_logical_staging` 不变。
- 两个 workflow 调用点传入已有的 `workflow_timing`。

子阶段**不加入** `_PARTITION_WRITE_PHASES`，所以写入汇总 API 的键集不变（无接口变更、无需改 `test_scene_api` 的断言）；数据落在 `runner_result.datasets[].timings.workflow.phases`，正是 handoff §6.1 那个递归 walker 读取的位置。

### 验证（等价性 + 产数）

用假连接捕获实际下发的语句，对改动前（`HEAD`）与改动后各跑一次同一批 staging 数据：

| 项 | 旧 | 新 |
|---|---|---|
| 语句数 | 7 | 7 |
| SQL 文本 + 参数序列 | — | **完全一致（逐条相等）** |
| 记录到的阶段 | `['opengauss.promote_logical_staging']` | `['opengauss.promote_logical_staging', 'opengauss.promote.grid_cells', 'opengauss.promote.tiles', 'opengauss.promote.indexes']` |

即：**对数据库说的话一字未变**，只是多记了三个计时点。回归：969 passed，`ruff` 全通过。

## 四、下一步（按「收益 × 风险」，均需单独授权或更大改动面）

| 优先级 | 项 | 预期 | 为什么还没做 |
|---|---|---|---|
| 1 | **REINDEX 治理**（`partition_tiles` / `partition_indexes` / `partition_grid_cells`） | 生产索引密度 4.3–4.6 entries/page vs 同构新鲜表 26–83（膨胀 6–9×）；`VACUUM` 完全不回收 | 破坏性 DDL/需要维护窗口与 owner 授权；且「REINDEX 后 promote 提速多少」从未端到端验证，属未验证假设，不应写成既定收益 |
| 2 | **promote 结构改动**：去掉 staging→目标表二次搬运（或 COPY 装载） | 探索实测 staging 一跳 ≈1.1 ms/输出行（占 4.11 ms/行的 27%） | 会动幂等门（`verify_output_chunks` / `NOT EXISTS`），需先有第 3 项的归因数据 |
| 3 | **读 promote 子阶段实测值**（本轮已具备条件） | 归因 107 s 的构成，决定 1/2 的取舍 | 需要在真实 Ray + OpenGauss 上复跑一次小批次（不改代码即可） |
| 4 | product `VALUES`→COPY | 同表同 N 的进程内 A/B 实测 4.98–5.95× | 需同步改 `test_product_workflow.py:933-947` 的契约断言（现断言 SQL 里含 `VALUES`） |
| 5 | entity `executemany`→COPY | N=100/500 实测 1.22–1.74×（当前表仅 840 行，绝对收益 <0.06 s） | 收益小、改动面中等（需 COPY + 单条 MERGE 的临时表与键） |
| 6 | `ray_ingest` 的 `batch_size` 接通 | 消除 N≥6e4 的 `DISTINCT ON` 排序落盘（~0.47 s） | **会改失败语义**（单事务→分批提交），测试需重写，且当前 ingest 规模远小于阈值 |

### 口径提醒（避免把诊断当收益）

- `61ad6fa` 是**诊断**，不产生任何加速；`39e323c` 的收益是「去掉每次 carbon ingest 的 O(M) 固定项」（300k 行时 0.41–0.51 s），在**当前每数据集 10–50 行**的规模下不改变端到端墙钟（探索实测单次 ingest 固定开销 0.21 s、数据集 run 2.97–7.93 s 主要由输出 chunk 读取/资产核对支配）。
- 剖分侧真正的墙钟大头仍是 promote（80%）与实体路径的未归因部分（>75%），两者都需要上面第 1–3 项。
