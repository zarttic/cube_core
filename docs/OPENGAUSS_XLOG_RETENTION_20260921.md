# OpenGauss pg_xlog 不回收把根盘顶到 95%：定位、处置与复发防护（2026-09-21）

> 上游：`AGENTS.md`「基础设施集群信息 / OpenGauss 数据库」「性能优化与真实库验证」。
> 本文档记录一次**运行时治理**：不含任何代码改动，改的是生产实例的复制槽状态。
> 处置时间：**2026-09-21 22:51:24 → 22:51:32 CST**；文中所有容量/计数均为该窗口的实测值。
> 证据快照：`~/cube-cleanup-evidence-20260921/{before_state.txt,after_state.txt}`（本机，未入库）。

## 一、现象与口径（先确认「log」指什么）

`10.3.100.180`（poufennode02，OpenGauss Primary）根文件系统已经 95%：

| 检查项 | 实测 |
|---|---|
| `df -h /` | `91G` 总，`82G` 已用，**`4.8G` 可用（95%）** |
| `du -x -d1 /` | `/data 47G` / `/opt 22G` / `/usr 11G` |
| `du -h -d1 <dn>` | **`pg_xlog 39G`** / `base 6.8G` / `global 514M` |
| `pg_xlog` 明细 | **2467 个 16MB 段 = 39G**，最早 `...000F000000CC`（2026-08-21 15:43） |
| `pg_log`（文本日志） | 177M（21 个文件，2026-09-05 起）——**不是元凶** |

所以「40G 的 opengauss log」= **WAL（`pg_xlog`）**，不是 `pg_log` 文本日志。这一点必须先分清：
这两者的处置手段完全不同，`pg_log` 可以按保留份数清理，`pg_xlog` 只能靠实例自己回收。

## 二、根因：一个 8 月 21 日起就不再推进的非活跃复制槽

`pg_replication_slots` 当时有两行：

| slot_name | slot_type | active | restart_lsn |
|---|---|---|---|
| `dn_6002` | physical | **f（非活跃）** | **`F/CDD97408`**（冻结） |
| `dn_6004` | physical | t | `19/6CB2ECB0`（= 当时主库最新位点） |

- `dn_6002.restart_lsn = F/CDD97408` 正好等于**最早存活 WAL 段** `...000F000000CC` 的起点：
  该槽把 2026-08-21 15:43 之后的**全部** WAL 钉住，一个月里一段都没能回收。
  磁盘上 `000F` 只留 52 段、`0000000100000010`–`18` 每个 logid 都是满 256 段（4G/个），
  形态与「从某个点之后再没回收过」完全一致。
- 槽的 `state` 文件 mtime = **2026-08-21 19:15**（与 `.179` 数据目录 `postgresql.conf` mtime 同刻），
  一个月没有被任何进程写过；主库 `pg_stat_replication` 里只有 `poufennode03 (10.3.100.181)` 一条 sender，
  `gs_ctl query` 的 Senders info 也只有它。
- 主库文本日志每分钟重复一行 `LOG: slotname: dn_6002, dummy: 0, restartlsn: F/CDD97408`
  （`grep "dn_6002" pg_log` 可见，从 09-05 一路相同），即「清理 xlog 被该槽阻塞」的直接证据。

槽的归属：`dn_6002` 是 `10.3.100.179`（poufennode01）的实例名（该节点 `postgresql.conf`
`log_directory=.../pg_log/dn_6002`）。它的实例**已经不在运行**：`gs_ctl query` 报
`the postmaster process 3897318 is not running`，`postmaster.pid` 停留在 2026-08-08，
`pg_xlog` 最后写入时间 2026-08-21 19:15。该槽自 8 月 21 日起就是一个**没有任何消费者**的钉子。

放大它的两个配置事实：

- `enable_xlog_prune = on`、`synchronous_commit = on`（= 文档要求的生效前提都满足），
  但 **`max_size_for_xlog_prune = 2147483647 kB`（≈2 TB，即默认值「事实上无上限」）**，
  所以主库永远不会因为「给断连备机留太多」而回收。
- `archive_mode = off`（排除归档阻塞）、`wal_keep_segments = 64`、`checkpoint_segments = 64`
  → 正常保留量应为 `(64 + 64*2 + 1) × 16MB ≈ 3.1G`。**39G 远超这个公式**，属明确异常。
  （参考：openGauss 官方博客《openGauss 的 xlog 不回收原因和修复方案》「复制槽不推进引起主备节点
  xlog 均不回收」条目，处置方式即删除不再使用的复制槽。）

## 三、处置（22:51:24 → 22:51:32）

1. 备份证据：`/root/wal-slot-backup-20260921/dn_6002/{state,state.backup}`（root，2026-09-21 22:51）。
2. 以初始用户 `og_user` 本地 socket 执行：
   ```sql
   select pg_drop_replication_slot('dn_6002');
   checkpoint;
   ```
3. 不做任何手工 `rm`：`pg_xlog` 的段必须由实例按最小保留 LSN 回收，手工删除会破坏
   恢复/备机链路。

**效果（30 秒内完成，无需重启）：**

| 指标 | 前 | 后 |
|---|---|---|
| `pg_xlog` | 39G / 2467 段 | **3.1G / 195 段**（= 第二节公式的正常值，+`wal_file_preinit_threshold=100` 预分配） |
| `/` 可用 | 4.8G（95%） | **41G（54%）** |
| 最早 WAL | 2026-08-21 | 当天（`0000000100000019...`） |
| 复制槽 | `dn_6002`(f) + `dn_6004`(t) | **只有 `dn_6004`(t)** |

**未受影响（同刻核对）：** 主库 `db_state=Normal`；备机 `poufennode03` 仍 `Streaming`、
`sync_percent=100%`、LSN 与主库一致；生产库 `cube_v3` 正常读写（`partition_datasets` 4 行、
`partition_indexes` 141091 行，146 ms）。10 分钟后复查仍为 `3.1G / 195 段`，无回涨。

## 四、复发防护（1、2 已于 2026-09-21 23:07–23:12 落地）

1. **给保留量加硬上限 —— 已落地**：`max_size_for_xlog_prune` 由默认 2 TB 改为
   **`5242880 kB`（5 GB）**，`enable_xlog_prune` 显式写成 `on`（原为注释态默认值）。
   该参数是 **SIGHUP**，已 `gs_ctl reload` 生效（`pg_settings.source = configuration file`），无需重启。
   5 GB 相当于本库约 4 天的 WAL（实测生成速率 39G/月 ≈ 1.3 G/天），仍高于正常保留量 3.1G，
   只在异常钉住时才触发。
   官方文档对机制的限制仍要记住：**「所有备机断联且无逻辑复制槽时不回收日志」**、
   **「有备机正在 build 时该参数不生效，日志全量保留」**（`gs_ctl build` 期间要挑低写入时段）。
2. **配置与现实对齐 —— 已落地**：主库 `replconninfo1` 的 remotehost 由 `10.3.100.182`（已死）
   改为 **`10.3.100.181`（真实备机）**；备机 `.181` 上指向不存在实例的 `replconninfo2/3` 已注释掉。
   配置里挂着连不上的备机，正是官方博客记录的头号「xlog 不回收」成因，
   而 `.179`/`.182` 的重建属于另一件事（见 `docs/OPENGAUSS_CLUSTER_MEMBERSHIP_20260921.md` 第五节，需授权）。
3. **磁盘水位告警 —— 监控其实在位，缺的是「通知」**（2026-09-21 23:30 复核，
   **纠正本文档早前版本「该节点没有 node_exporter」的说法**）：`10.3.100.182` 上以 docker 跑着
   `prometheus(:19090)` + `grafana(:13000)` + `node-exporter(:9100)`，`.180`/`.181` 也有原生
   `node_exporter(:9100)`；Prometheus 正在抓 `.165/.179/.180/.181/.182` 五台主机
   （另有一个 js `fastapi-app` job）。规则组 `system_alerts`（15s 评估）里已有
   `高磁盘使用率 = (1 - avail/size)*100 > 80`（`severity=critical`，无 `for` 延迟）。
   **实测：该告警对 `.180:9100` 在 15 天窗口内 309 个小时采样全部为 `firing`（从 09-06 起就没停过），
   但 `/api/v1/alertmanagers` 为空——没有 Alertmanager，告警只留在 Prometheus/Grafana 里，没人被通知。**
   所以正确修复不是「加一条 cron 巡检」，而是：① 接 Alertmanager（或轻量通知器：邮件/Webhook/企微）并配路由；
   ② 修 `.179:9100` 目标自 09-12 17:30 起一直 down 的问题；③ 目前**没有任何数据库 exporter**，
   主机层之外的 DB 内部/`pg_xlog` 体积无监控（`storage.tsdb.retention.time=15d`）。
4. **装一个「不认槽」的硬闸（最大缺口，待授权）**：`max_size_xlog_force_prune` **目前是 0（不生效）**。
   官方文档原文：*「当设置大于 0 时，在满足多数派备机正常的情况下，强制清理掉主机的 xlog
   （**忽略备机连接与否、也忽略是否有残留复制槽**）」* —— 这是全套参数里**唯一明确不管残留槽**的闸门，
   而本次事故正是残槽钉死。建议设 **8GB**：正常保留量只有 3.1G，5GB 那个是同场景的软闸，
   8GB 保证只在异常堆积时动作（实测 build 只需 ~2 分钟、期间 WAL 增量 <100MB，碰不到 8GB，不会误伤 build）。
   两条边界要记住：① 它需要「满足多数派备机正常」（我们现在 `.181`/`.182` 两个备机都健康 → 满足；
   备机全挂时它不生效，那时靠第 1 条 `max_size_for_xlog_prune=5GB` 兜）；
   ② `enable_xlog_prune=on` 时最终边界取两者 segno 的**较大值**，所以两个都得设才有意义。
5. **把「钉住」做成指标（目前完全无监控，且可用现有账号实现）**：用 `.cube_web.env` 里的应用账号
   `remote_user` 实测可查（无需超管、无需新凭据）：
   - 僵尸槽：`select slot_name, active, restart_lsn from pg_replication_slots` —— `active=false` 且
     `restart_lsn` 不推进即告警（本次 `dn_6002` 冻结了 30 天）；
   - **被钉住的 WAL 体积**（提前几周的单一数字）：
     `pg_xlog_location_diff(pg_current_xlog_location(), (select min(restart_lsn) from pg_replication_slots));`
     实测现在两个健康槽都是 **0 MB**；建议 >6GB 告警、>10GB 严重；
   - 磁盘可用率：node_exporter 已有（`node_filesystem_avail_bytes{mountpoint="/"}`）。
   ⚠️ `pg_stat_replication`（备机延迟）对应用账号返回**空集**，要用特权账号或授权；
   `pg_ls_dir('pg_xlog')`/`pg_stat_file` 需要初始账号——所以别依赖目录扫描，用上面的 LSN 口径。
6. **运维纪律（不花钱、最有效）**：
   - **退役/下掉任何备机，必须同时 `pg_drop_replication_slot()` 它的槽**——这次 30 天钉死就是漏了这步，
     建议写进备机下线清单。
   - 配置里不挂不存在的备机（`replconninfo` 与真实拓扑一致），已按此改 `.180/.181/.179`。
   - build 选低写入时段（build 期间 normal 闸不生效，见 `max_size_for_xlog_prune` 文档第 3 条）。
   - 每月 5 分钟巡检：槽（active/restart_lsn）+ pg_xlog 体积 + 备机延迟。
7. **宿主机层兜底（不依赖数据库/Prometheus）**：在 `.180` 放一个 5 分钟 cron：根盘可用 <10%
   或 `pg_xlog` >10GB 就写日志/发通知，并可选择**自动**删「非活跃且 restart_lsn 冻结 >24h」的槽。
   理由：数据库自己卡死时监控常同时失效（本次就是 exporter 掉了 21 小时）。
8. **把结论变成验证过的**：`.179` 已出集群且版本相同（7.0.0-RC3），适合当演练环境：造一个僵尸槽 →
   让 WAL 涨过阈值 → 验证 `max_size_xlog_force_prune` 真的会剪、以及剪掉后备机只能重建（预期行为）。
   本地 `ogbench` 容器（7.0.0-RC1，`127.0.0.1:15432`）因无备机拓扑无法满足「多数派备机正常」前提，不适合验这个。

9. **监控在关键时刻自己断了 21 小时**：`up{instance="10.3.100.18[01]:9100"}` 的小时序列显示，
   `.180`/`.181` 的 node_exporter 从 **09-21 约 02:00 一直到 22:59** 全是 down（两者都是 `nobody` 用户在
   **22:59:25/26** 重新拉起的，即我 22:51:32 释放 36G 后 8 分钟；节点上没有 systemd unit 或 cron 在管它们，
   二进制属 `zweitao:xiangchao`（2024-05）——是外部团队的部署）。
   后果：**目标 down ⇒ `node_filesystem_*` 序列消失 ⇒ 磁盘告警从 firing 静默变回 inactive**
   （`ALERTS` 里 `.180` 的高磁盘使用率确实停在 09-21 00:30），而没有 Alertmanager 时
   `up==0` 也不产生任何通知——**采集、抓取、规则都在位，整条链路只在「通知」这一环断了，
   而且它断的时候恰好是磁盘最危险的那一段。**
10. **文本日志噪声（可选）**：备机 `.181` 的 `pg_log` 有 612M，20MB 一轮，内容以
   `CreateRestartPoint ...`、`attempting to remove WAL ...`、`[batch flush] DW truncate ...`
   这类内部 LOG 为主（非报错）。不构成容量风险，可用 `log_max_count`/verbosity 收敛。

## 五、复现与复核命令（只读）

```bash
# 1) 根盘与 pg_xlog 体积
ssh root@10.3.100.180 'df -h /; du -sh /data/og_user/openGauss/install/data/dn/pg_xlog; \
  ls /data/og_user/openGauss/install/data/dn/pg_xlog | grep -cE "^[0-9A-F]{24}$"'

# 2) 复制槽（关键：active 与 restart_lsn 两列）
su - og_user -c "gsql -h /data/og_user/openGauss/tmp -p 15400 -d postgres \
  -c 'select slot_name, slot_type, active, restart_lsn from pg_replication_slots'"

# 3) 「是不是某个槽钉住了」：主库文本日志里每 1 分钟一行的 slotname / restartlsn
grep -h "slotname" /data/og_user/openGauss/log/omm/og_user/pg_log/dn_6004/postgresql-*.log | tail -3

# 4) 节点侧 HA 实况（备机是否真在流复制）
su - og_user -c 'gs_ctl query -D /data/og_user/openGauss/install/data/dn'
```

**准入方式（本次实测）**：`ssh root@10.3.100.180` 免密可用（`.179/.180/.181/.182` 同样可用；
`10.3.100.183` KubeRay 不接受该密钥）。OpenGauss 管理入口为
`su - og_user` → `gsql -h /data/og_user/openGauss/tmp -p 15400`（初始用户本地 socket 免密，
`remote_user` @ `cube_v3` 是**另一个库**的 DSN，不是节点管理入口）。

## 六、同时发现的事实（详见集群成员取证文档）

> 本节事实的完整证据、时间线与重建前置条件见 `docs/OPENGAUSS_CLUSTER_MEMBERSHIP_20260921.md`。

| 事实 | 影响 |
|---|---|
| `10.3.100.179` 的集群实例自 2026-08-21 19:15 起未运行（`postmaster.pid` 停留 08-08，进程已消失）；同一节点上另有一个**系统安装**的 OpenGauss（`/usr/local/opengauss/bin/gaussdb`，数据目录 `/var/lib/opengauss/data`）在跑，与本集群无关 | 集群名义 1 主 3 备，实际只有 `.181` 一个可用备机 |
| `10.3.100.182` 是**合法集群成员**（同 system identifier、同安装 XML、曾有全量 build 产物、2026-08-08 仍 100% 流复制），实例自 2026-08-20 13:48 起未运行，机器 09-17 重启后无人拉起 | 「182 不在集群里」是**实例没跑**，不是被移出集群；**已于 2026-09-21 23:24 全量重建并改名 `dn_6003` 重新入集群** |
| `.180/.181/.182` 的 `application_name` 都是 `dn_6004`（身份配置是复制的，未逐节点区分），而复制槽名 = 备机实例名 | 直接拉起 `.182` 会与正在服务的 `.181` **撞同一个槽名**，重建前必须先改实例名 |
| 四个节点**都没有自启机制**（无 systemd unit、`rc.local` 无 gauss 行），实例全靠人工 `gs_ctl start` | 节点重启或人工停机后无人拉起，是备机静默消失一个月的根因 |
| `.179` 已于 2026-09-21 23:18 出集群（`replconninfo` 全部停用） | 定稿拓扑 = `180` 主 + `181`/`182` 备；`.179` 的数据目录（15G）待授权后回收 |
| 主库 `static_connections=1`，备机 `.181` 报 `static_connections=3` | 两侧 HA 元信息不一致，排障时不要以单侧为准 |
| `.179` 根盘 85%（74G/91G）、`.182` 74%（64G）；`/home`、`/opt` 各占十几到 41G | 与本事故无关，但同属容量风险 |
