# 性能优化实施记录：每一步的方法与收益（2026-09-13 汇总）

> 汇总本轮已实施/已验证的性能优化，逐条给出**优化方法**与**实测收益**。
> 上游文档：`PERFORMANCE_OPTIMIZATION_DIRECTIONS_20260912.md`（探索与优先级）、
> `GRID_OPTIMIZATION_ROUND1_20260912.md`（格网第 1 轮细节）、
> `PARTITION_INGEST_OPTIMIZATION_ROUND1_20260912.md`、`PARTITION_WRITE_PERFORMANCE_HANDOFF.md`（瓶颈定位）。
>
> **测量时间**：2026-09-12（本文写作于 2026-09-13 00:30 CST）。测量机当时负载 10–15/16 vCPU，
> 计时项噪声很大；**整数计数才是硬证据**（格元数、行数、脏页数、逐字节一致数）。

---

## 0. 测量方法与可信度约定（读数字前先读这一节）

| 约定 | 做法 |
|---|---|
| 计时可信度 | 所有计时都取**多轮最小值**并给出中位；单轮中位数可能被负载抬高 2–3× |
| 顺序混淆 | A/B 对比**轮转执行顺序**（否则"总是最后测"的变体会虚高 20–30% 收益） |
| 硬证据 | 格元数、行数、脏页数、几何逐字节一致数、索引计划（Index Only Scan）——与负载无关 |
| 真实库探针 | 只写 scratch schema 的**同名影子表** + `'表名'::regclass::oid` 解析断言 + 前后行数断言；生产表在每轮结束时核对未变（`rs_cube_cell_fact` 始终 60,400 行） |
| 凭据 | 文档、脚本、输出都不打印 DSN / MinIO 口令 |

---

## 1. 总览

| # | 优化 | 一句话方法 | 收益 | 状态 |
|---|---|---|---|---|
| 1 | geohash 覆盖向量化 | shapely 广播 + 保留 `area>0` 门限 | **2.9×** | 已提交 `c06dcbd` |
| 2 | isea4h 覆盖：25 次求交 → 9 次 | 相对经度位移的代数等价变换 | **1.25–1.95×**（格元数一致） | 已提交 `2a9a074` |
| 3 | mgrs 裁剪/解码缓存 | `(domain, band)` 裁剪缓存 + decode 缓存 | **1.40×**（cover/几何）；decode 8000 次 25.0→0.2 ms | 已提交 `ceb2c6f` |
| 4 | mgrs 解码去 warnings 竞争 | 直接读 C 扩展状态位，不碰进程级过滤器 | 并发泄漏 71/320 → **0**；冷调用 **9.3→4.58 µs** | 已提交 `60628d8` |
| 5 | carbon MERGE 去掉 ON 键 CAST | 两侧同为 TEXT，CAST 把唯一索引藏起来了 | seq scan(227 MB, cost 38030) → **Index Only Scan(cost 8.28)**；0.41–0.51 s/次且不再随表增长 | 已提交 `39e323c` |
| 6 | promote 三段分开计时 | 把 `promote_logical_staging` 拆成三阶段 | 定位 promote = 逻辑剖分 **80%**（106.9 s / 133.3 s） | 已提交 `61ad6fa` |
| 7 | cube fact MERGE 不再重写几何 | UPDATE 分支去掉 `ST_GeomFromGeoJSON(cell_geom)` | **min −30.6% / 中位 −12%**（48.8→33.8 µs/行） | **未提交** |
| 8 | `_load_snapshot` 批量取格元 | TOAST 几何逐行 JOIN → 按版本取一次 | **8.71 s → 5.30 s（−39%）** | **未提交** |

---

## 2. 逐条详解

### 2.1 geohash 覆盖向量化（`c06dcbd`）

- **方法**：把逐格元 `cell.intersects(aoi)` 循环改成 shapely 广播（`shapely.box([...])` + `shapely.intersection` + `shapely.area`），**并保留 `cover_mode="intersect"` 的 `area > 0` 门限**。
- **收益**：**2.9×**。
- **注意**：探索阶段报出的 4.53× 是**错的**——那是纯 `intersects` 的结果，会把"只在边/顶点接触"的零面积格元也算进来，与 `cover_mode=intersect` 的既有语义冲突，已撤回。回归测试见 `cube_encoder/tests/test_geohash_engine.py`（零面积接触用例）。
- 细节与完整对比见 `GRID_OPTIMIZATION_ROUND1_20260912.md`。

### 2.2 isea4h 覆盖：经度变体 5×5 → 9 个相对平移（`2a9a074`）

- **方法**：利用代数恒等式
  `area(translate(cell, 360a) ∩ translate(target, 360b)) == area(cell ∩ translate(target, 360(b−a)))`，
  把"每个 cell 对 25 个平移变体求交"降为"**9 个相对位移**"；同时让每个 cell 由一条 ring 构造、去掉不必要的旋转。
- **收益（格子数逐例完全一致）**：

| 场景 | 优化前 | 优化后 | 幅度 |
|---|---|---|---|
| scene res6 | 21.8 ms | 14.8 ms | 1.47× |
| province res6 | 173.1 ms | 135.0 ms | 1.28× |
| local res6（warm） | 11.8 ms | 7.7 ms | 1.54× |
| dateline res4 | 102.0 ms | 52.2 ms | 1.95× |
| province res8 | 2420.5 ms | 1938.8 ms | 1.25× |
| dateline res4（warm） | 102.2 ms | 52.7 ms | 1.94× |

- **正确性**：新增 2 个测试——36 组定位格元（含 ±179.99、179.9/62.4、极区）对 6 个目标盒（含跨日期变更线、`box(170,-1,190,1)`、极带、顶点接触小盒）与旧 5×5 实现逐一比对；以及"两条候选路径（局部 WALK / 全层 STRtree）对 `_exact_intersections` 一致"。

### 2.3 mgrs：`(domain, band)` 裁剪缓存 + decode 缓存（`ceb2c6f`）

- **方法**：`@lru_cache` 缓存 `(domain, band)` 的裁剪多边形（`_band_polygon`）与 `decode_utm` 结果；`_utm_band_polygon` 退化为薄包装；domain×band 合法性判定也缓存。
- **收益**：

| 场景 | 优化前 | 优化后 | 幅度 |
|---|---|---|---|
| cover local p1 | 11.8 ms | 8.4 ms | 1.40× |
| cell geometry（zone 43, p1） | 83.5 ms | 59.7 ms | 1.40× |
| `decode_utm` × 8000 次 | 25.0 ms | 0.2 ms | 110×（**缓存主导**） |

- **注意**：110× 是"同一批码重复调用"的上限值，**不含**冷调用收益；冷调用的真实收益见 2.4（9.3 → 4.58 µs）。
- **验证**：新增 7 个测试（缓存命中/共享 band 裁剪、与未缓存重算逐值一致、band 不匹配拒绝、只有成功解码才入缓存）。

### 2.4 mgrs 解码：去掉进程级 `warnings` 竞争（`60628d8`）——正确性 + 性能

- **方法**：原实现用 `catch_warnings()` 安装**进程级**过滤器把 C 扩展的纬度警告提升为异常，该窗口不是线程安全的（并发调用恢复旧过滤器时会把提升丢掉，而结果又进了缓存，于是**错码被永久接受**）。现改为：从共享库**二次绑定不带 errcheck 的解码函数**，自己读状态位（先错后纬度警告，与库的优先级一致）；主路径完全不碰进程级状态；只有 fallback 路径保留原实现并整窗加锁。
- **收益**：并发不一致码泄漏 **71/320 → 0**；`decode_utm` 冷调用 **9.3 → 4.58 µs（2.0×）**。
- **验证**：9600 个生成码与 `mgrs.MGRSToUTM` **0 差异**；1914 用例跨版本（对 `4fe4734`）**0 差异**；并发 0 泄漏且合法码不被拒；新测试对修复前文件有 3 个失败。

### 2.5 carbon MERGE：去掉 ON 键上的 CAST（`39e323c`）

- **方法**：carbon 的 `MERGE` 在每个 ON 键两侧都写了 `CAST(... AS VARCHAR(n))`。两侧本来就是 TEXT（staging 表由显式列清单建表 + COPY 填充），CAST 的唯一作用是**把唯一索引从计划里藏掉**。
- **收益**：计划从 `partition_indexes` 风格的 **seq scan（227 MB 事实表，cost 38030）** 变为 **Index Only Scan（cost 8.28）**；此前每次 ingest 固定 **0.41–0.51 s 且随表增长线性增长**，现在与表规模解耦。
- **验证**：在会话 TEMP 影子表上（生产函数未改）：300 行新增 → delta 300；同 `cube_version` 重跑 → delta 0（幂等）；新版本 → 300；半重叠重跑 → 0；真表未被修改。

### 2.6 promote 三段分开计时（`61ad6fa`）——这是诊断，不是加速

- **方法**：`promote_logical_staging` 接受可选 recorder，把三条 `INSERT ... SELECT` 分别记为 `opengauss.promote.grid_cells / .tiles / .indexes`。
- **收益**：把"逻辑剖分 133.3 s 里 106.9 s（80%）在 promote、吞吐 243 rows/s"这件事的**账目**补齐，下一步优化有了落点。
- **验证**：假连接证明新旧实现语句文本与参数序列完全一致（7 条语句），只是多了 3 个子阶段；969 passed。

### 2.7 `rs_cube_cell_fact` MERGE：不再重写由键决定的 `cell_geom`（**未提交**）

- **问题**：`WHEN MATCHED THEN UPDATE SET` 里包含
  `cell_geom = ST_SetSRID(ST_GeomFromGeoJSON(source.cell_geom_geojson), 4326)`。
  同一 `(grid_type, grid_level, space_code)` 的网格几何是**确定的**，而这三列都在 ON 键里——命中行重写几何只是把相同的值重新解析一遍 GeoJSON、再过一遍 `geometry(Polygon,4326)` 的类型/SRID 校验。
- **方法**：UPDATE 分支删掉这一列（10 行里删 1 行），INSERT 分支保持不变（新行仍解析 GeoJSON）；新增 `_backfill_missing_cell_geom` 兜住迁移/历史遗留的 `cell_geom IS NULL`（先做一次探测，没有 NULL 行时零成本）。
- **收益**（N=20,000 行**全部 MATCHED**，影子表与生产同名同索引，4 轮轮转顺序）：

| 指标 | 优化前 | 优化后 | 幅度 |
|---|---|---|---|
| 最小值 | 976.1 ms | **676.9 ms** | **−30.6%** |
| 中位 | 1060.1 ms | **936.5 ms** | −12% |
| 每行 | 48.8 µs | **33.8 µs** | −15.0 µs |
| 脏页数 | 2236 | 2234 | **不变**（纯 CPU 收益，非 IO） |

- **未采纳**：进一步去掉 `st_code`/`cell_min/max_lon/lat`（同样是键决定的）实测**无额外收益**（min 690.3 vs 676.9），却多承担语义风险，故不采纳。
- **已撤回**：早先报的"脏页数 −29%"是**测量顺序混淆**造成的假象（轮转顺序后三变体脏页数完全相同），不成立。
- **语义论证**：几何修正通过**新的 `cube_version`** 交付（内容哈希 → 新键 → 走 INSERT 分支，照常解析 GeoJSON）；历史 NULL 几何由 `_backfill_missing_cell_geom` 兜底（生产当前 0 行 NULL）。
- **验证**：真库功能检查 5/5 通过（历史 NULL 几何被回填；其余 MATCHED 行几何**逐字节未变**；`st_code`/`run_id`/`value_ref_uri` 仍更新；NOT MATCHED 行照常插入带几何；生产表未变）；新增 2 个单元测试。

### 2.8 `_load_snapshot`：格元几何改为按版本批量取一次（**未提交**）

- **问题**：`managed_output_ingest._load_snapshot` 的主查询对**每个索引行**都 JOIN `partition_grid_cells` 取 `bbox`/`geometry`，而 `geometry` 是 **TOAST 列** → 上万次随机 TOAST 读。
- **关键事实**：被测 carbon 版本 `a17df5d3…` 有 **70,909 个索引行，却只有 248 个格元**；整版格元一次性取回只要 **0.01 s**。
- **方法**：主查询不再 `SELECT g.bbox, g.geometry`（**JOIN 保留**，它同时承担"索引行必须有对应格元"的过滤语义）；新增一次按 `(dataset_id, output_version)` 的批量查询，在 Python 侧按 `(grid_type, grid_level, space_code, topology_code)` 回填；配不到格元时**立刻报错**。
- **收益**：

| 阶段 | 优化前 | 优化后 | 幅度 |
|---|---|---|---|
| `_load_snapshot`（70,909 行） | **8.71 s** | **5.30 s** | **−3.41 s（−39%）** |
| 其中格元几何获取 | ~1.8 s | 0.01 s | ~180× |

- **等价性**：248/248 个不同格元的 `cell_bbox` + `cell_geometry` **逐字节一致**（顺序无关的规范化比对）；返回行数不变（70,909 = 70,909）。
- **同时被证伪的假设**：`_verify_minio_objects`（逐 URI 串行 `stat_object`）实测只有 **0.21 s**，**不是**瓶颈；该 SQL 的**服务端**执行只有 ~1.1 s，剩余时间在**客户端行传输 + psycopg dict 物化**（70,909 行 × ~2 KB ≈ 145 MB）。
- **验证**：新增 2 个测试（主查询不得再取几何且 JOIN 仍在；格元缺失必须报错）。

---

## 3. 被证伪 / 已撤回（含我自己的错误结论）

| 结论 | 状态 | 事实 |
|---|---|---|
| `_verify_minio_objects` 串行 stat 是 ingest 瓶颈 | **证伪** | 0.21 s |
| MERGE 再去掉 bounds/`st_code` 能继续提速 | **证伪** | min 690.3 vs 676.9，无收益 |
| "脏页数 −29%" | **撤回** | 顺序混淆假象；轮转后三变体脏页数完全相同 |
| geohash 覆盖 4.53× | **撤回** | 纯 `intersects` 会纳入零面积接触；保留 `area>0` 门限为 **2.9×** |
| "跳过重复 ingest 写入"（作业级短路） | **不可行（已修正）** | `managed_output_ingest._verify_targets` 用 `run_id = job_id AND cube_version` 计数并要求 `cell_geom IS NOT NULL`；只跳过写入而不刷新 `run_id` 会让自检失败 |
| 其余探索阶段的撤回项（promote 49–64×、buffer miss 680 µs、ingest N=1e5 超线性等） | 见 | `PERFORMANCE_OPTIMIZATION_DIRECTIONS_20260912.md` |

---

## 4. 尚未优化（按优先级，含前置条件）

1. **重复 ingest 的 134 万次非 HOT 更新**：来源已定位——`rs_ingest_job` 里 **16 个版本对应 214 次 ingest 作业**，同一 `output_version` 最多被 ingest **60 次**（主要是验收/演示重跑）；`n_tup_upd=1,340,000` vs `n_tup_ins=203,613`，`n_tup_hot_upd` 仅 2,121（0.16%）。要消除必须**同时**改 `_verify_targets` 口径（改成按版本 + 作业台账）**或**让重复更新走 HOT。
2. **`fillfactor` + HOT**：语义不变、只让更新便宜（当前 0.16% HOT 说明页里没空间）。**前置：维护窗口授权**（`ALTER TABLE ... SET (fillfactor=70~85)` + 重写表才生效）。
3. **回收膨胀**：`rs_cube_cell_fact` heap **410 MB / 6 万活行**、索引密度 4–5 entries/page。**前置：授权** `VACUUM FULL` + `REINDEX`。
4. **`_load_snapshot` 剩余 ~4 s**：行传输 + `dict_row` 物化。候选：`SELECT i.*` 收敛到实际消费的 20 列（注意：单独去掉 `a.attributes`（−428 B/行）**没有**时间收益，说明字节数不是唯一驱动）、换 tuple 游标 / 分批流式。
5. **`ORDER BY i.output_id` 的落盘排序**：`Sort Method: external merge Disk: 171 MB`；去掉 ORDER BY 只省 0.08 s（计划改变），需专门设计（如按主键索引序读取）后再评估。
6. **promote 三段里的哪一段最贵**：`grid_cells / tiles / indexes` 已埋点，**尚未测量**。
7. **`postgres_batch_size` 是死参数**；单次 ingest > 6 万行时会触发排序落盘（当前最大版本 32,830 行，尚未触发）。
8. **首次写入（INSERT 路径）**：只能靠减行数或减索引，尚未动。

---

## 5. 复现与证据位置

```bash
# 跨包门禁（本记录最后一轮：973 passed）
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest

# 格网 A/B（含 checksum 与逐用例格元数）
python3.11 /tmp/perf-probe/grid-opt/bench_grid.py <label>      # 结果 json 与补丁副本在 /tmp/perf-probe/grid-opt/

# MERGE 缩窄 A/B（影子表，轮转顺序）
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 /tmp/verify-grid/merge_narrowset.py
# MERGE 缩窄的真库功能验证（同名影子表 + OID 守卫）
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 /tmp/verify-grid/merge_narrowset_real.py
# 影子表安全模板（任何真实库探针都用它）
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 /tmp/verify-grid/probe_guard.py
```

- 探索阶段原始证据归档：`~/perf-probe-evidence-20260912/`（19 个 agent JSON + workflow JSONL）
- 本文档引用的提交：`ceb2c6f`、`c06dcbd`、`2a9a074`、`60628d8`、`39e323c`、`61ad6fa`

---

## 6. 状态与提交

| 项 | 内容 |
|---|---|
| 已提交 | `ceb2c6f` `c06dcbd` `2a9a074` `60628d8`（格网）、`39e323c` `61ad6fa`（入库/剖分） |
| **未提交（4 文件）** | `cube_split/cube_split/ingest/ray_ingest_job.py`、`cube_split/cube_split/ingest/managed_output_ingest.py`、`cube_split/tests/test_ray_ingest_job.py`、`cube_split/tests/test_managed_output_ingest.py`；本轮全量 **973 passed** |
| 真实库边界 | 本轮所有真库操作均只读 + scratch schema 同名影子表 + OID 断言；`rs_cube_cell_fact` 全程 60,400 行未变（有一次探针事故已在 `PERFORMANCE_OPTIMIZATION_DIRECTIONS_20260912.md` §0.1 记录并清理） |
