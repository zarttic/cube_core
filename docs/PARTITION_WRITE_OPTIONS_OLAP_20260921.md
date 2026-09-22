# 剖分写入性能实测与存储选型探索（OpenGauss 优化 vs Doris / StarRocks）

> 上游文档：`docs/PARTITION_WRITE_PERFORMANCE_HANDOFF.md`、`docs/PERFORMANCE_OPTIMIZATION_DIRECTIONS_20260912.md`、
> `docs/PERFORMANCE_OPTIMIZATION_STEPS_20260913.md`、`AGENTS.md`「性能优化与真实库验证」。
> 本文档目的：用实测数据回答两件事——**① 继续优化 OpenGauss 的剖分写入能拿到多少；② 把剖分结果三表
> （`partition_grid_cells` / `partition_tiles` / `partition_indexes`）迁到 Doris 或 StarRocks 是否划算。**
> 边界：本文档只做测量、定位与选型建议，**不含任何代码改动**；文中所有「建议改动」均未落地。

## 〇、测量时间、环境、口径与安全边界（务必先读）

- **测量时间**：2026-09-21 00:45–02:10 CST（生产库只读检查与 scratch 探针 00:45–01:10，
  本机容器与 StarRocks 实验 01:10–02:05）。
- **生产库**：`cube_v3` @ `10.3.100.180:15400`，OpenGauss 7.0.0-RC3，实例于 **2026-09-20 20:25:11 重启**
  （`shared_buffers=8GB`、`wal_buffers=64MB`、`max_process_memory=18GB`、`other_used_memory=34MB`）。
  重启前的 malloc 膨胀问题已消失。
- **本机对照实例**：
  - OpenGauss 7.0.0-RC1 单节点容器（podman，`127.0.0.1:15432`，`shared_buffers=1GB`，无备机）。
    该实例按生产库 DDL 重建了三张表（列、默认值、生成列、主键、复合唯一索引、部分索引全部一致）。
    容器口令不写入本文档。
  - StarRocks 3.3.9 all-in-one（podman，1 FE + 1 BE，`replication_num=1`，aarch64 镜像
    `starrocks/allin1-ubuntu:3.3.9`）。
- **口径纪律**：每组配置多轮执行并**轮转顺序**；报「最小值 + 中位」；同一命令的会话间漂移如实写出。
  本文档中凡「行数 / 计数 / 逐列一致」类整数可作硬判据，绝对吞吐/延迟仅作量级参考。
- **探针安全**：所有写入只落在 scratch schema 的影子表（**与生产表同名**）内，同一连接
  `search_path = <scratch>, public`，执行前后断言 `::regclass` 解析到影子表 OID（非生产 OID），
  并比对生产三表行数不变（实验前后均为 `70,070 / 141,091 / 141,091`）。实验结束已 DROP 全部 scratch schema。
- **注意：本机 10.3.100.179 就是 poufennode01**（集群节点之一，同时跑 MinIO / Web / 前端），
  因此「本机容器」数字**不是生产硬件对照**，只用于回答「引擎/代码本身能跑多快」。

### 0.1 必须优先处理的事故：生产库数据卷写满

2026-09-21 01:40 左右，生产库上的 scratch 探针写入 587 MB 时数据库报：

```
psycopg.errors.DiskFull: could not extend file "base/115603/264189": No space left on device
```

`115603` 即 `cube_v3` 的库目录。DROP 该 scratch schema 后写入立刻恢复正常 ⇒ **该数据卷剩余空间
< 约 600 MB**（当时全库仅 4,216 MB）。此后已停止一切对生产库的写入。
在扩容/清理之前，生产剖分与导入随时可能因磁盘写满失败；**这是当前所有性能问题的前置阻塞项**。
（本次未能读取该节点的 `df -h`：无 SSH 凭据，且 `10.3.100.180` 未开 node_exporter。）

> **2026-09-21 23:30 更正**：上述括号里两句均不成立——`ssh root@10.3.100.180`（及 `.179/.181/.182`）
> 密钥免密可用，`10.3.100.180` 也有原生 `node_exporter(:9100)` 并被 `10.3.100.182` 上的 Prometheus
> 正常抓取。当时的真实障碍是「以为没有 SSH 凭据」这个假设。本条测量数据（含该项不可读）保持原样，
> 但后续引用不要再用「.180 未开 node_exporter」作为理由；主机指标是有的，缺的是告警通知
> （无 Alertmanager），详见 `docs/OPENGAUSS_XLOG_RETENTION_20260921.md` 第四节。

## 一、结论速览

| 问题 | 实测答案 | 置信度 |
|---|---|---|
| 写入慢是代码问题吗？ | **不是**。同一份 `write_chunk_rows`，本机单节点 OpenGauss **133–135k 行/s**，生产集群 **12–18k 行/s**，差 7–10 倍 | 高（同代码、同数据、同并发，仅环境不同） |
| 生产慢在哪？ | WAL 刷盘 / 关系扩展等待（`WALFlushWait`、`extend`、`DataFileExtend`）+ 三备复制 + 磁盘余量近乎为零 + 8 GB 脏页回刷 | 中（等待事件直证 + 环境相关性） |
| 最划算的代码改动？ | chunk 内「DDL+COPY+合并」收成**一个事务**：生产 **29.4/34.0 s → 20.1/21.2 s（+50–70%）**，方差收窄；本机无副作用（±0%） | 高（生产轮转 A/B 两轮复现） |
| 换 StarRocks 快多少？ | 本机 all-in-one：12 并发 Stream Load **155k 行/s**，三表混合 480k 行 **3.47 s（138k 行/s）**；但**同机 OpenGauss 也有 133k 行/s** ⇒ 引擎差别 ≈4%，当前生产慢是环境问题 | 高（同机同数据对照） |
| 读侧换 StarRocks 划算吗？ | 聚合类快 3–4 倍（count/group by），**点查/首屏慢 5–40 倍**；分页 2 万行以上两者接近 | 高（30 万行同机对照） |
| 现在就整体迁移到 Doris/StarRocks？ | **不建议**（见 §七）：无外键/无二级唯一键、幂等要重做、跨库 JOIN 元数据没有可用方案、多一套 FE/BE 运维 | 中（官方文档 + 实测） |

## 二、生产环境现状（2026-09-21 01:00 前后，只读）

| 项 | 值 |
|---|---|
| 库总大小 | 4,216 MB |
| `partition_tiles` | 141,091 行 / 堆 83 MB / **索引 804 MB** |
| `partition_indexes` | 141,091 行 / 堆 127 MB / **索引 678 MB** |
| `partition_grid_cells` | 70,070 行 / 堆 111 MB / **索引 467 MB**（`geometry` JSONB 平均约 1 KB/行） |
| `partition_logical_staging_rows` | 0 行，但 `pg_total_relation_size` 仍 **1,475 MB**（历史膨胀，回收需维护窗口） |
| 数据分布 | 4 个 `output_version`（carbon/isea4h 70,909；product/mgrs 69,758；radar 324；optical 100） |
| 分区 | 三表均为普通堆表（`relkind='r'`），**未按 dataset/output_version 分区** |
| 等待事件（24 并发 × 2 chunk，35 s 采样） | `WALFlushWait` 484、`extend`（关系扩展锁）63、`DataFileExtend` 6、`wait transaction sync` 16；同步窗口内 `blks_read` 77,427 块（约 620 MB 从盘读回） |
| 关键参数 | `synchronous_commit=on`（`synchronous_standby_names=''`，即异步复制）、`wal_sync_method=fdatasync`、`enable_thread_pool=off`、`pagewriter_sleep=200ms`、`dirty_page_percent_max=0.3`、`max_connections=5000` |

## 三、实验一：写入路径各变体

### 3.1 生产集群（真实 `write_chunk_rows`，chunk = 3,333 格元 = 9,999 行）

| 并发 | 数据量 | 耗时（多轮） | 吞吐 | 单 chunk 延迟（中位） |
|---|---|---|---|---|
| 1 | 9,999 行 | 0.60 / 0.69 / 1.92 / 2.81 s | ≤14.5k 行/s | 0.6–2.8 s |
| 12 | 119,988 行（1 chunk/写者） | 5.51 / 6.28 / 6.30 / 13.24 s（另一次 3.71 s） | 9k–32k 行/s | 3.7–13 s |
| 12 | 479,952 行（4 chunk/写者） | 25.8 / 31.7 / 33.9 / 34.6 / 36.4 s | **12.2–18.6k 行/s** | 6.9–8.7 s |
| 24 | 479,952 行 | 37.6 s | 12.8k 行/s | 17.0 s |
| 36 | 359,964 行 | 5.74（突发）/ 16.8 / 27.0 / 29.4 / 38.5 s | 9.3k–63k 行/s | 23–29 s |
| 36 | 719,928 行 | 49.8 s | 14.4k 行/s | 23.6 s |

**12 并发之后负扩展**：36 并发总吞吐并不比 12 并发高，但单 chunk 延迟从 7–9 s 涨到 23–29 s。

### 3.2 生产集群 A/B：提交方式（12 并发 × 4 chunk = 479,952 行，轮转顺序、各 2 轮）

| 变体 | 两轮耗时 | 中位吞吐 | 相对现状 |
|---|---|---|---|
| 现状（autocommit，每 chunk 约 9 次提交） | 29.4 / 34.0 s | ≈15k 行/s | — |
| **整 chunk 单事务**（临时表按会话复用） | 20.1 / 21.2 s | ≈23k 行/s | **+50–70%** |
| 整 chunk 单事务（临时表按 chunk 建/删） | 17.8 / 23.0 s | ≈23k 行/s | +50–70%（与上者等价） |

### 3.3 本机单节点 OpenGauss 容器（同一份代码、同样 480k 行/12 并发，含全部索引结构）

| 变体 | 吞吐 | 相对现状 |
|---|---|---|
| 现状（部署中的代码路径） | **133–135k 行/s** | — |
| 整 chunk 单事务 | 132–133k | ≈0 |
| 单事务 / 4 chunk | 120–123k | −9% |
| COPY 直插目标表（无临时表、无反连接） | 158–159k | +18% |
| 只保留主键索引（去掉 2 个复合唯一 + 部分索引） | 167–174k | +26% |
| UNLOGGED 表 | 139–140k | +4% |
| 会话级 `synchronous_commit=off` | 131–133k | ≈0 |

**结论**：单事务提交只在生产有效（生产每次提交要等 WAL 刷盘/IO）；「临时表 + `WHERE NOT EXISTS`
反连接」与索引维护是常数级开销，各占 18–26%；临时表复用本身无收益（§3.4 证伪项）。

### 3.4 被证伪 / 无效的方向（避免重复投入）

- **临时表复用**：单独 A/B（复用 vs 每 chunk 重建）吞吐一致 ⇒ DDL 不是瓶颈。
- **`synchronous_commit=off`**：本机 ±0%；生产未测（属降级持久性，不建议作为生产手段）。
- **UNLOGGED 表**：本机仅 +4%，且语义上不适合作为需要长期保存的剖分产物。
- **4 chunk/事务的更大批提交**：本机 −9%，无收益。
- **更早的「直写 46k 行/s」说法**：该数字来自简化探针（单表、临时表建一次、每批一次提交），
  与真实三表路径不可直接比较；同一探针 2026-09-20 实测为 7.84 s / 20.39 s，
  2026-09-21 同为 14.73–27.96 s，说明**生产库当天的写入能力本身在漂移**。

## 四、实验二：StarRocks 实测（本机 all-in-one，aarch64）

数据与 OpenGauss 实验完全一致（同样的 `make_rows`，含 JSON 字段），Stream Load 走
`PUT /api/{db}/{table}/_stream_load`（先 307 到 BE，需自行跟随重定向并保留 Basic 认证）。

### 4.1 写入

| 场景 | 结果 |
|---|---|
| 3,333 行/批，**PRIMARY KEY 模型** | 1 并发 32.1k；4 并发 89.1k；**12 并发 158.3k**；24 并发 157.5k 行/s（单批延迟中位 0.10–0.39 s） |
| 3,333 行/批，DUPLICATE 模型 | 1 并发 27.9k；4 并发 99.3k；12 并发 155.3k；24 并发 149.3k 行/s |
| **重灌同一批（等价 upsert）** | PK 156.1k、DUP 138.4k 行/s（不降速） |
| **三表混合 480k 行**（对齐生产口径，144 次 load，12 并发） | **3.47 s = 138.3k 行/s**，失败 0，单批延迟中位 0.205 s |
| 单表批量 180 万行（540 文件，4 并发） | indexes 57.7k 行/s（31.2 s）、tiles 78.4k（23.0 s）、cells 94.4k（19.1 s），计数逐一核对一致 |
| **小批惩罚**：36 并发 × 1,000 行（48k 行） | **28.1k 行/s，单批延迟中位 1.25 s / 最大 1.70 s**（比 3,333 行批慢 5.5 倍） |

### 4.2 读取（180 万行 PK 表）

| 探针 | 最小 / 中位 |
|---|---|
| `count(*)` by dataset_id | 54.3 / 58.3 ms |
| `count(*)` by dataset+version | 80.3 / 84.7 ms |
| 分页 50 行 offset 0 | 57.1 / 85.2 ms |
| 分页 50 行 offset 100,000 | 60.5 / 82.7 ms |
| 分页 50 行 offset 1,000,000 | 204.9 / 214.0 ms |
| 点查 by `output_id` | 11.3 / 12.3 ms |
| `grid_cells` count（180 万行） | 26.6 / 27.8 ms |
| `group by band_code` | 71.8 / 75.2 ms |

### 4.3 同机对照（30 万行，本机 OpenGauss 容器 vs 本机 StarRocks）

| 项 | OpenGauss | StarRocks |
|---|---|---|
| 装载同一批 299,970 行 | 12.0 s（约 25k 行/s，单文件 COPY 直插） | 3.24 s（约 92.6k 行/s，Stream Load 4 并发） |
| `count(*)` by dataset | 145 / 151 ms | 33 / 35 ms |
| 分页 50 行 offset 0 | **1.0 / 1.8 ms** | 41 / 49 ms |
| 分页 50 行 offset 20,000 | 59 / 64 ms | 71 / 72 ms |
| 点查 by `output_id` | **0.48 / 0.55 ms** | 10 / 13 ms |
| `group by band_code` | 215 / 216 ms | 55 / 56 ms |

⇒ StarRocks 强在聚合/全表扫描，OpenGauss 强在点查与首屏分页；工程上「详情页」类查询迁过去反而会变慢。

### 4.4 稳定性观察（all-in-one 容器内）

- 540 文件 × 12 并发批量装载时出现 `FE RPC failure ... No more data to read` 与连接重置；
  降到 4 并发 + 重试后全部成功。**注意**：这是单节点 all-in-one 容器，不能推断生产集群行为。
- 小批（1,000 行）时单批延迟从 0.2 s 涨到 1.25 s，与官方文档「小批会带来版本膨胀与 compaction 压力」一致。

## 五、读写契约：迁移需要重建什么（代码梳理结论）

- **写者共 6 类**：`logical_row_writer` 直写、staging→promote、`complete_output` 的 COPY 批量插入、
  发布/撤回与入库回写的 `publication_status` UPDATE、按 band-unit 与数据集级的 DELETE
  （按 FK 顺序 indexes→tiles→grid_cells）、运维/验收脚本。
- **真实 HTTP 读链路只有 2 条**：`/v1/datasets/{id}/{tiles|indexes|grid}`（`dataset_management._detail_sql`，
  LIMIT/OFFSET 分页，直接回传 `bbox/geometry` JSONB）与 `/v1/quality/records/{run}/errors`
  （JOIN 三表 + scenes）。`partition_domain_store.list_*` 仅测试调用。
- **无 PostGIS、无空间谓词、JSONB 只透传**（JSON 仅在 Python 侧解析）⇒ 迁移利好。
- **必须重建的约束**：PK 反连接幂等（唯一冲突重试）、3 个复合唯一键、3 个外键
  （2 个 `ON DELETE CASCADE` 指向 `partition_output_versions`，1 个 `NO ACTION` 的
  `indexes.tile_output_id → tiles.output_id` 决定写入/删除顺序）、若干 CHECK、
  生成列 `normalized_topology_code`（参与唯一键）、2 个部分索引。
- **行宽**：`grid_cells` 平均 1,560 B（max 1,984）、`indexes` 1,217 B、`tiles` 656 B；
  其中 `grid_cells.geometry` 平均约 1 KB，且**由 `(grid_type, grid_level, space_code)` 唯一确定，可推导**。
- **可推导冗余**：logical 场景 `indexes` 与 `tiles` 1:1（`output_id = <tile_output_id>-index`），
  当前 logical 数据里 `indexes.tile_output_id` 全为 NULL，`window_*` 全为 NULL。

## 六、调研结论（Doris / StarRocks，附出处）

1. **小批高频必须开服务端攒批**（Doris Group Commit / StarRocks Merge Commit），否则每次导入 = 1 事务 +
   1 版本，会触发 `-235` 反压或 `too many versions`。官方 Doris Group Commit 实测（3.0.1，1FE+3BE，
   2.47 亿行）：10 KB × 10 并发 = 112,181 行/s。
   <https://doris.apache.org/docs/4.x/data-operate/import/load-best-practices/group-commit-manual/>
   StarRocks：「Merge Commit ... designed for high concurrency, small-batch (from KB to tens of MB)」，但不建议单并发。
   <https://docs.starrocks.io/docs/loading/StreamLoad/>
2. **主键 upsert 有额外代价**：StarRocks 官方说明主键模型需主键索引查找 + Delete Vector，需与 compaction 资源权衡。
   <https://docs.starrocks.io/zh/docs/best_practices/primarykey_table/>
3. **ARM64 现状**：Doris 官方镜像为多架构（含 `linux/arm64`，如 `apache/doris:all-in-one-4.1.3`），
   官方也发布 arm64 二进制 <https://doris.apache.org/community/developer-guide/all-in-one-image/>；
   StarRocks 官方明确「binary distribution packages ... support deployments only on x86-based CPU」，
   ARM 需用 `starrocks/artifacts-{OS}:{Version}` 镜像，且官方推荐生产用 x86/AVX2。
   <https://docs.starrocks.io/docs/deployment/preparation/prepare_deployment_files/>
   本次实测：`starrocks/allin1-ubuntu:3.3.9` 存在 arm64 manifest，在本机 aarch64 可正常跑通（见 §四）。
4. **事务与唯一性**：Doris 支持显式多语句事务（多表 insert），StarRocks 自 3.5.0 起支持 SQL 事务
   （限同库、仅 INSERT/UPDATE/DELETE、无写冲突检查）；**两者都没有真正的写入时外键**，
   唯一键/外键声明只用于 MV 改写与 join 剪枝；幂等统一靠 label（at-most-once）。
   <https://doris.apache.org/docs/4.x/data-operate/transaction/>、
   <https://docs.starrocks.io/docs/loading/SQL_transaction/>
5. **与 OpenGauss 集成**：两边都有 JDBC catalog 可直连 PG 协议，但官方均声明仅适合作数据集成/小表 join，
   不适合常规查询；StarRocks 侧实测 `COUNT(*)` 不下推（第三方 issue #69762）。反向（OpenGauss 查 Doris/StarRocks）
   无官方标准做法，需 `mysql_fdw` 之类变通。
   <https://doris.apache.org/docs/dev/lakehouse/catalogs/jdbc-catalog-overview/>
6. **同类替代**：ClickHouse 官方建议单批 10,000–100,000 行、小批用 async_insert
   <https://clickhouse.com/docs/concepts/best-practices/selecting-an-insert-strategy>；
   湖仓（Iceberg/Delta）同样有小文件/compaction 问题。**没有任何方案能绕过「小批高频」本身的约束，
   区别只是把攒批放在客户端、服务端还是文件层。**

## 七、建议路线（按优先级）

1. **（阻塞项，先做）数据盘清理/扩容**：给 `cube_v3` 数据卷留出至少现有体积的 2–3 倍空间；
   顺带回收 `partition_logical_staging_rows` 的 1,475 MB 历史膨胀（需维护窗口授权）。
2. **（低风险高收益）写入路径改造**：`write_chunk_rows` 把「3 次临时表 DDL + 3 次 COPY + 3 次合并」
   收成**一个事务**，唯一冲突改为 chunk 级重试（TTL 内重放整块，代价可接受：10% 重复键实测仅 2 次重试/36 万行）；
   同时把 `CUBE_LOGICAL_MAX_IN_FLIGHT` 从 36 降到 12（36 并发负扩展）。预期生产 12–18k → 20–30k 行/s，
   抖动显著收窄。需配套：等价性逐列比对 + 全量 pytest + 回滚开关（现有 `CUBE_LOGICAL_CHUNK_PERSIST=staging` 可复用思路）。
3. **（中期，结构瘦身，收益可叠加）**
   - 批量导入后建二级唯一索引（或改为应用层校验）——本机实测去掉后可 +26%；
   - 按 `output_version` 分区（顺带修掉「按批次删除」的全表扫）；
   - 不再落 `grid_cells.geometry`（读时用 SDK 复算），或压缩为 bbox + WKB；
   - logical 场景 `indexes` 行可由 `tiles` 推导，考虑不落库或落成派生视图。
4. **（按需）OLAP 只读旁路**：若痛点是「多租户大范围查询/统计」而非写入，可把三表异步派生一份到
   StarRocks/Doris 专供分析，**OpenGauss 仍是 system of record**。收益是聚合快 3–4 倍；
   代价是同步链路 + 一致性口径 + 一套 FE/BE 运维。
5. **（当前不建议）把三表主存整体迁到 Doris/StarRocks**：需要重建幂等/唯一性/外键顺序、拆掉与元数据表的
   跨库 JOIN（详情页、质检、入库快照都依赖）、重做删除语义（无小文件回收/级联）、并承担 ARM 生产的官方
   支持风险；而同机对照显示引擎写入能力只差约 4%（138k vs 133k 行/s），当前 7–10 倍差距来自生产环境而非引擎。

## 八、复现方法

实验脚本位于本机 `/tmp/write_speed_20260921/`（**本地 scratch，未纳入仓库**，可按下列描述重建）：

| 脚本 | 用途 |
|---|---|
| `bench.py` | scratch schema 影子表 + 安全断言（OID/行数）；模式 `setup / full / phases / matrix / steady / reuse / ab / teardown` |
| `opt_ab.py` | 写入变体 A/B/N：`standard / txn1 / txn4 / copy_direct / noindex / unlogged / sync_off` |
| `og_local_setup.py` | 从生产目录读取 DDL（只读），在本机容器重建三表（含生成列与全部索引） |
| `sr_bench.py` | StarRocks 建表（PK / DUPLICATE）、`gen` 造数、`load` Stream Load、`read` 读探针 |
| `sr_sweep.py` | Stream Load 并发扫描 + 三表混合装载 |
| `read_head2head.py` | 同机 30 万行读对照（OpenGauss vs StarRocks） |

关键环境变量（**不打印凭据**）：`CUBE_WEB_POSTGRES_DSN` 指向目标库；
本机容器实验用 `LOCAL_OG_DSN` + `BENCH_SKIP_SETUP=1`（表结构已由 `og_local_setup.py` 建好）。
StarRocks 容器：`podman start sr-test`（现为 stopped，保留数据）后再执行 `sr_bench.py setup`。

## 九、未验证事项与风险

- 生产库数据卷剩余空间、以及 7–10 倍差距中「三备复制 / 磁盘余量 / 脏页回刷」各自的权重**未拆分**；
  需要节点侧 `df -h`、`iostat`、`sar` 或扩容后再测一次同口径探针。
- 本文档只测了「批量写入」；**未测** 三表迁移后的删除性能、发布/撤回路径、入库快照 JOIN 的实际代价。
- StarRocks 数字来自**单节点 all-in-one 容器**（`replication_num=1`、无 compaction 压力、
  与客户端同机），不能外推到生产集群；官方生产推荐 x86/AVX2，本机为 aarch64。
- 生产侧单事务改造尚未做语义等价验证（临时表生命周期、唯一冲突重试粒度、崩溃中断后的
  `_verify_targets` 计数校验口径都需重新确认）。
- 本机实验期间 `load average` 2–5/16 vCPU，未做独占；同命令漂移最大 1.9 倍已在 §三 中给出区间。
