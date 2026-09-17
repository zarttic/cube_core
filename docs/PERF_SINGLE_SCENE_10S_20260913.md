# 单景全波段「剖分→入库 < 10s」实测与优化记录（2026-09-13）

> 目标：指定 ARD 数据集（`ard-optical-ard-load-d42e8791e5194a88ba42b499274b29ff`，2021 年中国黄土高原
> GF-1 ARD 反射率）**单景 × 全部 4 波段**，在 **geohash / mgrs 两种逻辑格网**下，从剖分到入库的
> 端到端墙钟 **< 10 s**（KubeRay Jobs API，worker 已预热）。
> 范围：仅逻辑格网；**不含六边形/实体剖分（isea4h）**。

## 一、结论（最终口径：geohash L4 + mgrs L0）

**口径说明**：经确认，MGRS 采用"往上选一层（更粗、格元更少）"的层级选择，即调用方对 mgrs 请求
**L0**（100 km 网格；数据集自身配置也是 0），geohash 用 **L4**。层级与格元尺寸的关系是
`格元尺寸 = 100 km / 10^level`（`cube_web/services/grid_preview.py`）。

| 格网 | 格元数 | 索引行 | 剖分 | 质检 | 入库 | **端到端（DB 时钟）** | 判定 |
|---|---|---|---|---|---|---|---|
| **geohash L4** | 35 | 140 | 4.59–5.39 s | 0.26–0.49 s | 0.18–0.55 s | **5.43 / 5.77 / 6.20 / 6.43 s** | **✓ 稳定达标** |
| **mgrs L0** | 9 | 36 | 4.67–4.79 s | 0.11–0.58 s | 0.27–0.98 s | **5.60 / 5.60 / 5.91 / 6.06 s / 5.6 s** | **✓ 稳定达标** |

生产表核对：每轮 `rs_cube_cell_fact` 增量 **精确等于**写入行数（geohash +140、mgrs +36），无脏写；
瓦片 100% `tile_kind=logical_reference`（逻辑剖分不下载、不转 COG）。

### 层级选择与达标边界（重要）

端到端时间随**每景格元数**增长，与格网类型无关（同一数据集、单景全 4 波段、热态实测）：

| 用例 | 格元数 | 索引行 | 端到端 | 判定 |
|---|---|---|---|---|
| mgrs **L0** | 9 | 36 | **5.6–6.1 s** | ✓ |
| geohash **L4** | 35 | 140 | **5.4–7.3 s** | ✓ |
| mgrs **L1** | 165–182 | 660–728 | 9.3–14.6 s | ✗ |
| geohash **L5** | 720 | 2 880 | 19.4 s | ✗ |

→ **经验边界：每景格元数 ≲ 300 时本链路可稳定 < 10 s**；超过后按约 3–4 ms/行（剖分）与
3–7 ms/行（服务内入库，含 GIL 放大）线性增长。选择层级时请按此约束（MGRS 往上选一层、
geohash 不要用过细的 L5+）。

## 一、附：历史口径（mgrs L1，未达标）

| 格网 | 端到端（DB 时钟，多轮） | 判定 |
|---|---|---|
| **geohash L4** | 5.41 / 5.46 / 5.75 / 5.87 / 5.97 / 6.03 / 6.04 / 6.12 / 6.23 / 6.24 / 6.36 / 7.26 / 7.33 s | **✓ 稳定达标** |
| **mgrs L1** | 9.27 / 9.67 / 9.76 / 9.92 / 10.04 / 10.05 / 10.20 / 10.28 / 10.44 / 10.72 / 10.93 / 11.03 / 11.11 / 11.42 / 14.57 s | **✗ 未稳定**（需再省 1–2 s） |

逐项要求达成情况：

| 要求 | 状态 |
|---|---|
| geohash 端到端 < 10 s | ✓ 稳定（13 轮全部 < 7.4 s） |
| mgrs 端到端 < 10 s | ✗ 未稳定（15 轮中 3 轮过线） |
| 分段耗时记录（剖分/质检/入库） | ✓ 客户端轮询 + DB 时间戳双口径 |
| 格元数 / 索引行 / 入库行 | ✓ 35 / 140 / 140（geohash），165–182 / 660–728 / 同数（mgrs） |
| 生产表核对（无脏写） | ✓ 每轮 `rs_cube_cell_fact` 增量精确等于写入行数（+140 / +660 / +728） |
| 逻辑剖分不下载源 COG | ✓ 瓦片 100% `tile_kind=logical_reference`；源 COG 转换归载入子系统 |
| 不改跨包公共接口 | ✓ 全部改动落在 `cube_web` 服务层与 `cube_split` 内部排除表 |

## 二、计量方法与一条重要教训

脚本：`/tmp/perf_e2e_two_grids.py`（每轮清该景的 `perf-` 守卫行即可复用同一景）。同时输出：

- **[客户端轮询]**：提交 → 任务终态 → 质检 pass → 全部 ingest run completed；
- **[DB 时间戳]**：`submit_db = SELECT now()` → `partition_quality_runs.started_at/completed_at`
  → `ingest_runs.created_at/started_at/completed_at`（服务端口径，不含客户端采样误差）。

**教训（已导致一次误判）**：只看墙钟会把"**0 行入库**"当成达标。任何"优化"后必须同时校验
**入库行数 = 索引行数**（进程化后端那次多数轮次写 0 行、墙钟很短，被脚本误判为 ✓）。

## 三、阶段分解（热态，DB 时钟）

| 阶段 | geohash L4（35 格元/140 行） | mgrs L1（165–182 格元/660–728 行） |
|---|---|---|
| 提交响应 | 0.2–0.8 s | 0.2–0.6 s |
| 提交 → driver running | ~2.6 s（**worker 侧下载解包 runtime env**） | ~2.6 s |
| driver 内部 | bootstrap 0.12 s + `workflow.run` ~2.2–3.0 s | bootstrap 0.12 s + `workflow.run` ~3.7 s |
| **剖分合计** | **4.7–5.4 s** | **5.4–8.8 s** |
| **质检** | 0.11–0.55 s | 0.11–1.55 s |
| **入库** | **0.12–0.90 s** | **1.66–4.74 s** |
| **端到端** | **5.4–7.3 s ✓** | **9.3–14.6 s ✗** |

入库的实际工作量（同版本、真实提交、隔离进程）：**一次调用带 4 波段 0.809 s**，逐波段 4 次串行
1.254 s，**4 个独立进程并行 0.499 s**；而服务内线程池路径是 1.66–4.32 s → 差口主要是
**服务进程内的 GIL 争用**（每行都是纯 Python 工作：构建 fact、拼 GeoJSON、jsonb 序列化）。

## 四、已落地优化与实测增益

| # | 改动 | 位置 | 实测增益 |
|---|---|---|---|
| 1 | 作业体瘦身：补 `excludes`（`.claude`/`.dggrid_src`/`docs`/`archive`/`**/build`/`**/tests`/`**/scripts`/`**/*.md`/`**/*.pyc`） | `cube_split/jobs/ray_logical_partition_job.py` | 打包 105 MB → 10.8 MB → **4.47 MB**；解掉 Jobs API 100 MiB 的 413（此前每次剖分被拒并落 `manual_required`）；mgrs 剖分段 5.9–6.4 s → 5.4–6.0 s |
| 2 | 轮询间隔可配 `CUBE_WEB_WORKER_POLL_SECONDS`（默认仍 1.0） | `cube_web/services/quality_worker.py` | 质检段 2.42 s → 0.4 s 量级 |
| 3 | 入库组内 band item 并行（原为串行） | `cube_web/services/ingest_worker.py` | geohash 入库 1.33 s → 0.87 s |
| 4 | 派发循环：每事件的配置读取提到循环外 + 单轮批量 100→20 | `cube_web/services/quality_worker.py` | 降低派发迭代时长（不再随 backlog 线性增长） |
| 5 | B：`fillfactor=80` + `VACUUM FULL rs_cube_cell_fact` + 4 张表 `REINDEX/ANALYZE` | 数据库 | `rs_cube_cell_fact` 154→**88 MB**、product 326→260、carbon 365→293、entity 6.3→1.2 MB；**行数零变化**；geohash 入库降到 0.12–0.26 s（**mgrs 入库无明显变化**） |
| 6 | ~~进程化入库后端（`CUBE_WEB_INGEST_EXECUTION=process`）~~ | `cube_web/services/ingest_worker.py` | **失败，代码已删除**（无收益且多数轮次写 0 行，见下） |

## 五、被证伪 / 已回退（负面结果清单）

1. **"入库慢是 DB 膨胀/索引页争用"** → B 做完整表重建+REINDEX 后 **mgrs 入库段不变**（1.83–3.52 s）；
   MERGE 本身只 0.10 ms/行。
2. **"并发争用"** → `INGEST_MAX_WORKERS` 取 4/2/1 三种都落在 2.5–4.1 s；4 线程直调（各自连接）墙钟
   1.43 s vs 求和 5.25 s，**并行本身有效**。
3. **"入库卡在认领/排队"** → `ingest_runs` 的 `created→started` 仅 0.04–0.25 s。
4. **"value_ref_uri 逐行 stat"** → 每版本只有 1 个不同 URI。
5. **"working_dir 上传是 2.6 s 的来源（客户端）"** → 客户端 `submit_job` 仅 0.33 s（带包）vs 0.01 s（不带）；
   2.6 s 在 **worker 侧解包**，故缩包体有效、关 working_dir 无效（且镜像比仓库旧，关了会 `worker_container_limit` 校验失败）。
6. **"按景合并入库 = 1 次提交就能省 1.5 s"（C）** → 实施后 mgrs 入库仍 2.3–2.9 s，**已回退**（并恢复
   "每波段一个 ingest run"的台账粒度）。
7. **"进程化入库"** → 多数轮次写 0 行（spawn 子进程失败 → run 被标 failed → 墙钟极短），
   成功轮次也只要 1.4–3.9 s（无收益）。**代码已删除**（连同 `working_dir` 开关），只保留本轮
   真正有效的改动：excludes 瘦身、轮询可配、入库组内并行、派发循环修复、按格元缓存。
8. **"多实例抢认领导致抖动"** → 假警报：`pgrep -f` 把 shell 包装算进去了，本机只有 50039（用户服务）与 50041（测试实例）。
9. **"outbox/队列表膨胀"** → outbox 77 行全部 `delivered`，其他表 ≤1073 行，计数 1 ms。

## 六、剩差与建议路径（mgrs 需再省 1–2 s）

1. **把服务内循环从关键路径挪开**：quality / ingest / dispatch 三个循环都在同一个进程里抢 GIL，
   而隔离脚本同等工作只要 0.5–1.25 s。方案：入库执行放**独立进程/独立服务实例**（我已试过进程池但采坑失败，
   更稳的是把 ingest worker 拆成独立部署单元）。
2. **降低每行 Python 开销**：`_ingest_raster` 每行构建 fact + 拼 GeoJSON + jsonb 序列化；
   可考虑把 provenance 序列化改为一次 `json.dumps` 复用、geometry 只按格元构造一次（与 block 内格元去重同思路）。
3. **剖分抖动**：mgrs 剖分 5.4–8.8 s 的波动与 `workflow.run`（覆盖+瓦片+索引写入）相关，
   下一步可给 driver 内部的 chunk 写入加阶段埋点。
4. 可选的结构性项：`fillfactor=80` 已生效，继续观察多轮 HOT 命中是否改善重复 ingest。

## 七、复现命令

```bash
cd /home/lyajun/projects/cube_project
set -a; . .cube_web.env; set +a
# 1) 测试实例（用户服务在 50039，不要动它；只按端口占用 PID 杀 50041）
PYTHONPATH=cube_encoder:cube_split:cube_web CUBE_WEB_PARTITION_EXECUTOR=ray_job \
  CUBE_WEB_WORKER_POLL_SECONDS=0.2 setsid nohup python3.11 -m uvicorn cube_web.app:app --port 50041 &
# 2) 计时（双口径分段 + 入库行数/表增量核对）
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 /tmp/perf_e2e_two_grids.py
# 3) 进程 vs 线程对照（同一版本、幂等提交）
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 /tmp/perf_proc_parallel.py <output_version> <band|scene> ...
```

注意：同一 `(band unit, grid_type, grid_level)` 重复提交会被幂等守卫拒绝（用别的景或先清
`perf-` 守卫行）；跑之前先确认 worker 已预热（空置 30 分钟会被回收，冷启动约 90 s，不计入剖分时间）。

## 八、本轮新增的否定结果（工程侧"砍每行成本"）

- `_ingest_raster` 内按格元缓存 `_bbox` / `cell_geometry_geojson`（原先每 fact 重复 3 次 bbox、
  每波段重复 1 次 GeoJSON 序列化）：**179 测试通过、无回归，但实测无收益**（mgrs 入库段
  1.82 / 2.96 s vs 改前 1.66–2.90 s）→ 每行构建 fact/解析几何**本就不是瓶颈**（664 行整段仅约 0.5 s）。
  该改动中性、保留（行数更多时方向正确）。
- 服务端"mgrs 自动往上选一层"的归一化（`DatasetPartitionConfig` + `CUBE_WEB_MGRS_COARSER_LEVEL`）：
  行为正确，但会让"生效剖分配置 ≠ 请求层级"，**打破 2 个既有测试**（`test_submit_mixed_persists_effective_dataset_partitions`、
  `test_task_logs_bind_task_id_context`）→ **已回退**，改由调用方直接请求 L0（参数层选择）。
