# 格网优化 第 1 轮实施记录（2026-09-12）

> 上游：`docs/PERFORMANCE_OPTIMIZATION_DIRECTIONS_20260912.md`（探索与优先级）。
> 本文档记录**已实施**的格网优化、验证证据与未完成项。

## 一、范围

| 文件 | 改动 | 风险 |
|---|---|---|
| `cube_encoder/grid_core/app/engines/isea4h_engine.py` | cover 经度变体 5×5 → 9 个相对平移 | 低（代数等价，单文件内部闭包） |
| `cube_encoder/grid_core/app/engines/isea4h/topology.py` | `cell_parent` 邻域扩展改惰性 | 低（构造上等价，含 fallback） |
| `cube_encoder/grid_core/app/engines/mgrs/geometry.py` | `(domain, band)` 裁剪多边形缓存；`decode_utm` 错误路径 + 结果缓存 | 低（纯函数；逐字节等价有测试） |
| `cube_encoder/grid_core/app/engines/geohash_engine.py` | cover 选择向量化（保留正面积判据）；`_decode_bbox` 记忆化；候选枚举改整数索引算术（码与 bbox 一并产出） | 中（须保 `area>0` 语义，已有回归测试守护） |
| `cube_encoder/grid_core/app/engines/isea4h_engine.py`（第 2 处） | `_continuous_ring` 非跨线 cell 只归一化一次；`_make_cell` 共用一个 ring（不再 geometry/bbox 各算一次）；删除死代码 `_closed_ring` | 低（差分测试：2,594 cell 的 ring/shape/bbox 与原 6 旋转 oracle 全等） |
| `cube_encoder/tests/test_isea4h_cover.py` | +2 用例：9 平移 vs 5×5 的等价性（含日期变更线/极区/顶点接触）、双路径 cover 对穷举 oracle 的等价性 | — |
| `cube_encoder/tests/test_mgrs_geometry_cache.py`（新增） | +7 用例：缓存等价性、缓存命中、band 不一致拒绝、`decode_utm` 只缓存成功结果 | — |
| `cube_encoder/tests/test_geohash_candidate_enumeration.py`（新增） | +5 用例：整数枚举 vs 原 seed-and-walk 差分（14 组 bbox/精度）、算术 bbox 与 bisection 解码**逐位相等**（>10,000 cell）、超界 bbox 终止性 | — |
| `cube_encoder/tests/test_isea4h_ring_fastpath.py`（新增） | +2 用例：ring 快速路径 vs 原 6 旋转搜索（res 0–6 抽样 + 日期变更线/极区，>1,500 cell，ring/shape/bbox 全等）、`locate_point` 的 bbox 与原式一致 | — |

不改任何公共接口、不新增依赖（`shapely>=2.0` 已声明；向量化用 `shapely.box` / `shapely.covers` / `shapely.intersection`，无需直接 `import numpy`）。

## 二、实测收益（A/B：HEAD 源码 vs 本轮改动）

方法：同一脚本 `/tmp/perf-probe/grid-opt/bench_grid.py` 分别在 HEAD 源码与本轮改动上运行；用 `git show HEAD:<path>` / 副本做源码切换并校验 sha256。测量时间 2026-09-12 21:00–21:30 CST，`load average ≈ 1.3`（不同于探索阶段 10–15 的共享负载）。
**硬判据 = 输出 cell 数必须完全一致**（下表全部一致）。

| 场景 | 基线 | 本轮 | 加速 |
|---|---|---|---|
| `isea4h` cover 单景 res6 intersect（10 cell） | 21.7 ms | 15.0 ms | 1.45× |
| `isea4h` cover 省级 res6 warm（163 cell，INDEX 路径） | 168.8 ms | 138.8 ms | 1.22× |
| `isea4h` cover 局地 res6 warm（4 cell，WALK 路径） | 12.0 ms | 7.8 ms | 1.54× |
| `isea4h` cover 日期变更线 res4（1 cell，INDEX 路径） | 100.3 ms | 54.2 ms | 1.85× |
| `isea4h` cover 省级 res8（2,346 cell，WALK 路径） | 2,442.5 ms | 1,977.8 ms | 1.24× |
| `isea4h` `cell_parent` L6 × 2,000 | 49.6 ms | 34.5 ms | 1.44× |
| `mgrs` cover 局地 p1 冷（36 cell） | 11.8 ms | 8.3 ms | 1.42× |
| `mgrs` 单 cell 裁剪几何 p1 冷（256 code） | 83.0 ms | 59.4 ms | 1.40× |
| `mgrs` `decode_utm` 8,000 次调用（676 唯一码） | 24.8 ms | 0.2 ms | 124×（热缓存） |
| `geohash` cover（6 个场景，见 §2.1 配对测量） | — | — | **2.83–6.12×** |

### 2.1 `geohash` cover：配对（交错）A/B 测量

首次单次进程间对比被机器噪声淹没（同一用例两次运行可差 10%），因此对 geohash 单独做了**基线/优化后交替运行 3 轮 × 每轮 3 次取 min** 的配对测量（脚本 `/tmp/perf-probe/grid-opt/bench_geohash.py`，源码切换后校验 sha256）：

| 场景 | 基线 | 本轮 | 加速 | cell 数 |
|---|---|---|---|---|
| 1°×1° L6 `intersect` | 1,349.8 ms | **386.1 ms** | **3.50×** | 16,836（一致） |
| 省级 8°×5° L5 `intersect` | 1,530.5 ms | **480.1 ms** | **3.19×** | 19,581（一致） |
| 日期变更线 L6 `intersect` | 294.0 ms | **82.8 ms** | **3.55×** | 3,660（一致） |
| 1/4 省域 L6 `intersect` | 12,833.6 ms | **4,531.7 ms** | **2.83×** | 154,818（一致） |
| 1°×1° L6 `contain` | 1,413.7 ms | **230.9 ms** | **6.12×** | 16,290（一致） |
| 省级 L5 `minimal` | 1,526.9 ms | **491.4 ms** | **3.11×** | 19,581（一致） |

### 2.2 `isea4h` ring 快速路径与共享 ring：配对测量

| 场景 | 基线 | 本轮 | 加速 | n |
|---|---|---|---|---|
| `locate_point` L6 ×400 | 186.3 ms | **147.9 ms** | **1.26×** | 400 |
| `cell_bbox` sweep L6 ×4,000 | 277.2 ms | **205.7 ms** | **1.35×** | 4,000 |
| `code_to_bbox` L6 ×400 | 24.3 ms | **17.0 ms** | **1.43×** | 400 |
| `cover_geometry` global res4（含全层索引构建） | 2,019.4 ms | **1,704.7 ms** | **1.18×** | 2,562 |
| `code_to_geometry` L6 ×400 | 142.7 ms | 131.3 ms | 1.09× | 400 |
| `cover_geometry` 单景 res6 | 14.8 ms | 13.3 ms | 1.11× | 10 |

补充观测（global res4 cover 的 cProfile，用于下一轮定位）：

- `snyder_inv` **33,280 次 / 0.76 s cum**（占主导）——即每个输出 cell 的 ring 被算了两次：一次在索引构建（bbox），一次在 `_make_cell`（geometry）。`cover_geometry` 是唯一需要完整 geometry 的路径；剖分生产链路走的是 `cover_compact`（只要 bbox，不构造 shape），因此此项影响面限于 `cover_geometry` / `locate_point` / `code_to_geometry` 类 API。
- 跨日期变更线的 cell 极少（res 4/5/6 各只有 **2 个**）→ `_cell_bbox` 的 `_cell_shape` 回退不是热点，“只归一化一次”的快速路径已覆盖几乎所有 cell。
- `shapely.affinity._affine_coords` 15,395 次 / 0.167 s（`_to_wgs84_shape` 每 cell 3 次 translate）。

补充的口径说明（避免误读）：

- **`geohash` 的 2.8–6.1×** 由三部分构成：向量化选择（探索阶段实测 82.4→17.8 µs/cell）、`_decode_bbox` 记忆化（1.09–1.64×）、候选枚举整数化（枚举步单独实测 210 ms → 92 ms，**2.3×**）。
- **`decode_utm`**：从「记录告警」改为**直接读 C 层状态位掩码**（见 §4.2 的竞态修复），严格冷解码 **9.3 µs → 4.58 µs**，缓存命中约 0.07 µs/次。收益取决于码的重复率（cover 滑窗场景重复率高）。
- **`mgrs` 单 cell 1.40×（−29%）** 与探索阶段的 −33.5% 同量级（当时含更多冷启动占比）。
- **`isea4h` 的 1.22–1.85×** 来自 `intersects_area`：INDEX 路径（候选多）受益最大（1.85×），WALK 路径 1.54×，省级 res8（访问 cell 多）1.24×。

## 三、改动要点（供 review）

1. **`_RELATIVE_LONGITUDE_SHIFTS = (0, -1, 1, -2, 2, -3, 3, -4, 4)`**（`isea4h_engine.py`）：
   由 `area(translate(cell,360a) ∩ translate(target,360b)) == area(cell ∩ translate(target,360(b-a)))` 与 `b-a ∈ [-4,4]` 得：原 5×5=25 次判定等价于「cell 不动 × 9 个相对平移」，`d=0` 优先使同圈命中只花 1 次 intersection。
   `longitude_variants` 保留给 `indexed_candidates` 的 STRtree 候选收集（该段语义与耗时不变）。`> 0.0` 严格比较、`intersect`/`minimal`/`contain` 三态语义都不变。
2. **`cell_parent` 惰性邻域**：先只判主候选 `primary`，未命中才枚举 `cell_neighbors(primary, pres)`，最后仍是原穷举 fallback。构造上等价（原实现是「先无条件构造 `[primary, *neighbors]` 再按序返回首个命中」）。穷举验证：res 1–6 共 54,612 个 cell，`seqnum ∈ cell_children(parent, res-1)` 不一致数 **0**。
3. **`_valid_utm_domain(domain, band)` + `_band_polygon(band)`**：键空间 = 60 zone × 2 hemisphere × 20 band；`_utm_band_polygon(code)` 保留为薄包装（签名/异常语义不变），`cell_geometry_clipped` 改用缓存版本。测试对**无缓存重算**做 `.equals()` 逐字节比对。
4. **`decode_utm`**：`catch_warnings(record=True)` + 全量记录 → `catch_warnings()` + `filterwarnings("error", message=r'Warning in "Convert_MGRS_To_UTM"', category=RuntimeWarning)`，只把**那一条**告警提升为异常，其余告警行为不变；`@lru_cache(16384)` 只缓存成功结果（失败不缓存，测试断言 `currsize`）。
5. **`geohash` cover 向量化**：`shapely.box(...)` 一次性构造全部候选框，`intersect`/`minimal` 用 `shapely.area(shapely.intersection(boxes, aoi)) > 0.0`，`contain` 用 `shapely.covers(aoi, boxes)`；输出顺序仍与候选枚举顺序一致。
   **未采用纯 `shapely.intersects`**：对抗性验证证明它会把零面积接触的 cell 选入，违反 `intersect` 语义（`test_geohash_engine.py:294`）——若采用，标题收益是 4.6× 而非实际的 ~2.9×。
6. **`_decode_bbox` 记忆化**：`@lru_cache(32768)`。它是 cover 枚举（每 cell 4.01 次）、`_make_cell`、`code_to_bbox/center/geometry` 的共同热点。
7. **候选枚举整数化**（`_cell_index_to_code` / `_cells_and_boxes_for_bbox`）：geohash 候选 cell 在（经度索引, 纬度索引）格点上构成矩形，索引范围可算术求得，并直接为每个 cell 生成码与 bbox，替代「从 seed 逐 cell 右移/上移 + 每 cell 一次 decode-shift-encode」。
   `lon_bits = (5P+1)//2`、`lat_bits = 5P//2`；位交错与 `_encode` 一致（经度占偶数位、MSB 优先）；半开区间/边界取整/经度 clamp/两极 clamp 都与原 walk 对齐；`_cells_for_bbox` 保留为薄封装。**算术 bbox 与 bisection 解码逐位相等**（cell 边界都是可精确表示的二进制小数），已有 >10,000 cell 的断言。

## 四、验证

```bash
# 全量跨包（含真实 OpenGauss 的 non-skipping 用例）
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest -q
# -> 963 passed, 2 warnings in 99.95s

# ruff
PYTHONPATH=cube_encoder python3.11 -m ruff check cube_encoder/
# -> All checks passed!
```

语义守护（新增用例）：
- `test_relative_longitude_shifts_match_the_legacy_variant_grid`：6 类 cell（普通/日期变更线两侧/高纬日期变更线/南北极）× 6 类 target（含跨经线 170..190、极区、**顶点零面积接触**）= 36 组，9 平移与 5×5 网格逐组布尔一致。
- `test_cover_matches_the_legacy_oracle_across_both_candidate_paths`：日期变更线（INDEX）与局地（WALK）两条路径的 cover 集合都等于穷举 oracle。
- `test_cell_geometry_clipped_equals_an_uncached_band_clip`：缓存版与无缓存重算 `.equals()`。
- `test_cells_for_bbox_matches_the_walk_enumeration`：14 组 bbox/精度下，整数枚举与原 seed-and-walk 的候选集合逐组相等（含全球 bbox、两极 clamp、经度 180 clamp、零宽/倒置 bbox、恰好落在 cell 边界上的 bbox）。
- `test_arithmetic_bboxes_are_bit_identical_to_the_bisection_decoder`：>10,000 个 cell 的算术 bbox 与 `_decode_bbox` **逐位相等**（`==` 而非近似）——这是 cover 选择正确性的前提。
- 既有 `test_geohash_engine.py::test_cover_boundary_contact_excluded_from_intersect` 与 `test_mgrs_topology.py::test_invalid_latitude_band_candidate_is_rejected_before_collection` 全部保持通过。

### 4.1 附带发现：原实现在超界 bbox 下会死循环
原 `_cells_for_bbox` 在 `lon_max > 180`（例如手写 bbox `(179.99, -0.01, 180.5, 0.01)`）时永远无法满足 `cell.lon_max >= lon_max`，而右邻在日期变更线处**不会**返回自身，于是无限循环（实测跑满 200,000 步仍未退出）。`aoi.bounds` 由 Shapely 给出、不会超过 180，所以生产路径不会触发，属潜在健壮性缺陷。整数枚举按 clamp 处理并正常终止，已加 `test_out_of_range_bbox_terminates_and_stays_in_range` 守护。

### 4.2 跨版本正确性对照（独立于自写测试）

自写测试只能证明「我以为的 oracle」成立，所以另做了一套**跨版本对照**：把改动前的代码（`4fe4734`）整棵树取出，用同一份用例集分别在旧/新两版上跑，逐用例比对。

工具：`/tmp/verify-grid/dump.py`（已归档到 `~/perf-probe-evidence-20260912/grid-correctness/`），固定随机种子 20260912。每个用例记录三份指纹：`wkb`（原始几何字节哈希）、`norm`（归一化几何哈希）、`val`（码/bbox/中心/错误文本/计数的精确 repr）。

| 检查 | 结果 |
|---|---|
| **1,914 个用例**（geohash / mgrs / isea4h × locate / cover 三模式 × code_to_* / 拓扑 / 错误路径） | **语义差异 0，连几何 WKB 字节都完全一致** |
| 用例覆盖 | geohash 8 层级 × 23 点位（含 ±180/±90/84/-80）、12 个 AOI × 3 模式 × (compact/full)、带洞面/多面体、5 个错误路径；mgrs 6 层级 × 24 点位（含 zone 边界、Norway/Svalbard、±80/±84、两极）、9 个 AOI、band 不一致与非法码；isea4h 8 层级 × 20 点位（含 quad 边界/极点）、14 个 AOI（含 8° 阈值两侧、全球、日期变更线、极区）、seqnum 几何/bbox、错误路径 |
| 高精度 cover（precision 9–12 的极小/exact-cell/半 cell AOI） | 长度校验全通过；`contain`/`minimal` 集合均为 `intersect` 的子集 |
| 缓存状态无关性（同一批请求冷/热两遍） | 4 组 AOI 的 cover 集合完全一致；bbox/geometry 全一致 |
| `decode_utm` 与 mgrs 自有 `MGRSToUTM` 在 9,600 个生成码上的判定 | 一致 9,600 / 不一致 0 |

两项**强化**验证（一次性运行，不是常规测试，耗时已记录）：

| 强化验证 | 规模 | 结果 | 耗时 |
|---|---|---|---|
| `isea4h` ring 快速路径 vs 原 6 旋转 oracle：**res 0–7 全量穷举** | 54,624 + 163,842 = **218,466 cell**（含 855 个跨日期变更线） | ring / shape / bbox **差异全为 0** | 41 s + 122 s |
| `geohash` 整数枚举 vs 原 seed-and-walk：**2,000 组随机 bbox**（precision 1–9，含零宽/倒置/边界对齐/极区/日期变更线/全球） | 累计命中 **26,569,421 cell** | **不一致 0** | 1,490 s |

覆盖范围说明（不夸大）：res 8–15 未穷举（同一代码路径 + 抽样对照）；cover 用例是「抽样 AOI × 层级 × 三模式」而非对连续输入空间的证明；两个**有界**缓存（`_decode_bbox` 32,768 / `decode_utm` 16,384）已验状态无关，未新增无界缓存。

### 4.3 对照测试抓到的真缺陷：`decode_utm` 竞态（已修复，commit `60628d8`）

线程压力测试发现：我最初的实现用 `warnings.catch_warnings()` + `filterwarnings("error", ...)` 把 C 扩展的纬度带告警提升为异常，而这会改**进程全局**的 warnings 过滤器。两个线程交错时，一个线程退出时把过滤器列表恢复成更旧的快照，另一个线程的提升就被抹掉 → **band 不一致的码被静默接受**；又因结果进了 `lru_cache`，该错误结果会被后续所有调用复用。

| 并发下 band 不一致码的漏检次数 | 旧实现（本仓库 4fe4734） | 我引入的 warnings 版 | 修复后 |
|---|---|---|---|
| 320 次并发尝试（8 线程） | 0 | **71** | **0** |

修复方式：从同一个共享库**再绑定一份不懂 errcheck 的解码器**，直接读状态位掩码（先判错误位、再判断告警位，与库自身优先级一致），主路径完全不再碰 `warnings`。退路（仅在无法建立绑定时使用）保留 warnings 实现，但整段持锁。

由于该竞态是时序性的（修复前也只有 ~22% 命中率），**时序型测试不可靠**（我写的第一版并发测试对着有竞态的代码也能通过）。因此回归测试改为**确定性机制断言**：

- `test_primary_decode_path_does_not_touch_the_warnings_filters`：把模块的 `warnings` 引用换成会报错的 stub，断言主路径仍能正常解码/拒绝（回退到 warnings 方案就会确定性失败）。
- `test_fallback_decode_path_holds_the_warnings_lock`：强制退路路径，断言锁被获取 2 次（有效码 + 不一致码）。
- `test_concurrent_fallback_decode_always_rejects_a_band_mismatch`：退路路径下的并发交错。
- `test_decode_utm_matches_the_mgrs_library`：与库自身在 320 个码上逐一对照（防未来 `mgrs` 版本变 ABI/位含义）。

已验证：这 4 项（除并发项为时序性）对 `ceb2c6f`（有竞态版）**确定性失败 3 项**，对修复版全部通过。

修复后的主路径高并发压测（12 线程 × 200 次，80% 合法码 + 20% 不一致码，无需任何 monkeypatch）：**不一致码漏检 0，合法码误拒 0**（2,400 次）。附注：压测脚本第一版把 `32TPU00` 当成不一致码，得到 138 次“漏检”——实际它被既有测试 `test_valid_latitude_band_boundary_candidate_is_collected` 明确断言为**合法**码，是脚本的误报，不是缺陷。

## 五、未完成（按收益/风险排序，供下一轮选择）
| 候选 | 预期 | 风险 |
|---|---|---|
| `isea4h` 非局地 AOI 走邻域 WALK 替代全层 STRtree 索引（消 8° 阈值悬崖） | 省级 L6 3.8 s → 0.1–0.2 s（**20–35×**），并消除 8.0°→8.5° 的 151× 跳变 | 高（漏格是正确性问题）；已验证 WALK 与 INDEX 集合在 27 场景等价，需保留索引 fallback + 穷举对照 |
| `isea4h` `cover_geometry` 不再“每 cell 算两次 ring”（索引构建一次 + `_make_cell` 一次） | 按实测 `snyder_inv` 0.76 s 中的一半 + 一次 `_to_wgs84_shape` 折算，global res6 预计 **1.5–1.8×** | 中（`_cell_shape` 加界 LRU，内存上限需实测；仅影响 `cover_geometry`/`locate_point` API 路径，不影响剖分生产链路） |
| `isea4h` 向量化 `snyder_inv`（逆投影） | 原型实测 5.03→0.71 µs/点（**7.1×**，20,000 点 0 处不一致）；global res4 的 `snyder_inv` 0.76 s → ~0.11 s | 高（numpy 从可选变热路径硬依赖；极点收敛域需单独回归）|
| `geohash` `_cell_index_to_code` 位展开 / `_make_cell` 对象构造 | cover 剩余时间构成：shapely 相交 44%、码生成约 40%、pydantic 构造 25%；位展开可再省约 0.1 s/16k cell | 低 |
| `isea4h` 消除 `_continuous_ring` 的 6 旋转候选 + 环复用 | L6 冷索引 3.86 s → ~2.9 s（省 0.95 s） | 中（跨经线连续性语义） |
| `isea4h` WALK 路径 `_cell_shape` LRU 复用 | 省级 res8 约 −10~15%（每访问 cell 重建 300 µs 形状） | 中（内存上限） |
| `mgrs` 邻居码算术生成 + 有效性记忆化 | 热 cover 约 **2×**（邻居枚举占 57%） | 高（纬度带别名/跨 zone 语义） |
| `isea4h` `locate_point` 每点算两遍环 | ~78× 的 API 路径小幅改善 | 低 |

## 六、状态

- 本轮已提交 5 个 commit（`a687033` / `ceb2c6f` / `c06dcbd` / `2a9a074` / `60628d8`），工作区对 `cube_encoder/` 干净。
- 其中 `60628d8` 是**正确性修复**：修掉本轮自己引入的 `decode_utm` warnings 竞态（详见 §4.3）。
- 验证口径：每个 commit 提交前都跑过完整跨包 pytest（最终 **969 passed**）与 `ruff check cube_encoder/`（全通过），另有一套 1,914 用例的跨版本对照（§4.2）。
- 真实门禁不受影响（本轮只动 `cube_encoder` 纯计算路径，未触碰剖分/入库/API 契约）；但按 `AGENTS.md`，推送前仍需完整跨包 pytest。
