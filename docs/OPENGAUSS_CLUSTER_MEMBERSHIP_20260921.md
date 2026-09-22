# OpenGauss 集群成员取证与整理（2026-09-21）

> 起因：`docs/OPENGAUSS_XLOG_RETENTION_20260921.md` 里的僵尸复制槽属于 `10.3.100.179`，
> 顺着它追出「名义 1 主 3 备、实际只有 1 个备机在跑」的问题。本文档回答
> **「`10.3.100.182` 为什么不在集群里」**，并记录本次整理动作与恢复路径。
> 取证与改动时间：2026-09-21 23:07–23:26 CST。
> **后续结论（当日 23:24）**：`.179` 正式出集群（不参与 HA），`.182` 已全量重建并改名 `dn_6003` 重新入集群，
> 现拓扑为 **`180` 主 + `181`/`182` 备**（详见第四节与第五节）。

## 一、一句话结论

**`10.3.100.182` 从来就是集群成员**——集群的定义确实是 1 主 3 备（poufennode02 主 +
poufennode01/03/04 备），`.182` 与其它三个节点共享同一个 system identifier、2026-07-04 由
gs_om 安装、2026-08-08 02:13 还在 100% 流复制。它「看起来不在集群里」的真实原因是：
**它的实例自 2026-08-20 13:48 起没有再运行，而集群没有任何自启机制把它拉起来。**

## 二、成员资格证据（都可复核）

| 证据 | 内容 |
|---|---|
| 安装定义 XML `/home/og_user/xml_output_20260704160044.xml`（.180） | `clusterName=dbCluster`；`nodeNames=poufennode02,poufennode01,poufennode03,poufennode04`；`backIp1s=10.3.100.180,10.3.100.179,10.3.100.181,10.3.100.182`；主节点 poufennode02 的 `dataNode1` = `/data/og_user/openGauss/install/data/dn` 依次配 `poufennode01 / poufennode03 / poufennode04` → **1 主 + 179/181/182 三备** |
| `cluster_static_config`（各节点 `install/app_f08516a2/bin/`，二进制，`strings` 可读） | 四个节点名与 IP 都在，且每个节点都记录另外三个对端 IP，`.182` 在其中 |
| `pg_controldata` | 四节点 **同一个 `Database system identifier = 2913499697507312732`**；`.180` = `in production`，`.179/.181/.182` = `in archive recovery` |
| `.182` 的 `gs_ctl` 日志末行 | `sync_percent: 100%`、`channel: 10.3.100.182:40256<--10.3.100.180:15401`：它曾是连上主库的流复制备机 |
| `.182` 数据目录 | `build_completed.done`、`backup_label.old`、`full_backup_label`、`gs_build.pid`（均 2026-07-04 16:2x）= 全量 build 的产物 |

配置侧也**没有**把它移除：主库 `replconninfo1` 的 remotehost 一直是 `10.3.100.182`。

## 三、四节点实况时间线（客观事实）

| 节点 | 实例名 `application_name` | 最后活动 | 现状 |
|---|---|---|---|
| `10.3.100.180` 主 | `dn_6004` | 现在（今天 16:06 重启过） | `in production`，正常 |
| `10.3.100.181` 备 | `dn_6004`（**与主库同名**） | 现在 | **唯一在流复制的备机**，100% |
| `10.3.100.179` 备 | `dn_6002` | `postmaster.pid` 2026-08-08 02:32；数据目录与日志 2026-08-21 19:15 停 | 实例未运行；它留下的复制槽把 39G WAL 钉死 |
| `10.3.100.182` 备 | `dn_6004`（**与主库、`.181` 三者同名**） | `postmaster.pid` 2026-07-29 17:59；最后 restartpoint 2026-08-20 13:48 | **已于 2026-09-21 23:24 全量重建并改名 `dn_6003`，现已流复制** |

补充事实（对以后重建很重要）：

- 四节点 `pgxc_node_name` 都是 `dn_6001`（= XML 里的逻辑 `dataNode1`）；`log_directory`/`audit_directory`
  在 `.180/.181/.182` 上都指向 `dn_6004`，只有 `.179` 指向 `dn_6002`。**身份配置是复制出来的，不是逐节点区分过的。**
- 复制槽名 = **备机实例名**（`application_name`）：主库现在的 `dn_6004` 服务 `.181`；已删除的 `dn_6002` 就是 `.179`。
  ⇒ 若直接把 `.182` 拉起来，它会以 `dn_6004` 自报家门，**与正在服务的 `.181` 撞名（撞同一个槽）**。
- 四节点**都没有自启机制**：`systemctl list-unit-files | grep -i gauss` 为 0，`/etc/rc.d/rc.local` 无 gauss 行，
  实例全靠人工 `gs_ctl start`。`.182` 所在节点 2026-09-17 15:20 重启过，之后没人拉起实例。
  这是「备机消失一个月无人察觉」的结构性原因。
- `.179` 上另有一个**系统安装**的 OpenGauss（`/usr/local/opengauss/bin/gaussdb`，数据目录
  `/var/lib/opengauss/data`）在运行，与本集群无关——排查时不要把它当成集群实例。

## 四、本次整理动作（全部无重启或仅 SIGHUP，已逐项核对）

| 时机 | 节点 | 改动 | 前 | 后 |
|---|---|---|---|---|
| 23:07 | `.180` 主 | `enable_xlog_prune` / `max_size_for_xlog_prune` | 默认 `on` / 2TB | **`on` / 5242880 kB（5GB）** |
| 23:10 | `.180` 主 | `replconninfo1` 的 remotehost | `10.3.100.182` | **`10.3.100.181`** |
| 23:11 | `.181` 备 | `replconninfo2/3`（→`.179`/`.182`） | 保留 | 注释掉 |
| 23:18 | `.179` | `replconninfo1-3` **全部停用** | 指向 `.180/181/182` | 无（该节点正式出集群） |
| 23:19 | `.182` | `application_name`/`log_directory`/`audit_directory` | `dn_6004`（撞槽） | **`dn_6003`** |
| 23:19 | `.180` 主 | 新增 `replconninfo2` → `.182` | 无 | 已启用（`static_connections=2`） |
| 23:20–23:21 | `.182` | `gs_ctl build -b full -q`（7125 MB） | 08-20 的陈旧数据 | 与主库一致（同 system identifier） |
| 23:24 | `.182` | 手写 `recovery.conf` + `gs_ctl start -M standby` | 实例未运行 | **Standby / Normal / 100%** |
| 23:24 | `.182` | `replconninfo2`（→`.179`） | 保留 | 注释掉（保留 `1`→主库、`3`→`.181`） |

**最终核对（23:26）**：`pg_stat_replication` 两条 `Streaming`（`.181`、`.182`）；`pg_replication_slots` 两个
active 槽 `dn_6003`/`dn_6004` 且 `restart_lsn` 完全一致（`19/6E000D48`）；`pg_xlog` 3.1G / 195 段；
根盘 41G 可用；`cube_v3` 读 141091 行正常。
build 期间的 WAL 保留例外已按文档发生（主库日志 `keep all the xlog segments, because there is a build task`），
23:21:52 build 结束后即恢复正常回收。

配置备份：`.180`: `/root/wal-slot-backup-20260921/`；`.181` / `.182` / `.179`: `/root/cluster-tidy-20260921/`。

> 两个容易踩的坑（已实测）：
> ① `gs_ctl build` **不会**生成 `recovery.conf`——这正是 `.182` 上次 build（07-04）后再也起不来的原因；
> ② `gs_ctl query` 的 `static_connections` **不是热更参数**（`pg_settings` 里没有这个 GUC），
> 删改 replconninfo 后要下次重启才归位。**判断实况请看 `pg_stat_replication` / `pg_replication_slots` / `Senders-Receiver info`。**

## 五、最终拓扑与剩余事项

**定稿拓扑（2026-09-21 23:26）**：

| 节点 | 角色 | 实例名 | 复制槽 | 状态 |
|---|---|---|---|---|
| `10.3.100.180` | Primary | `dn_6004` | （主） | 正常 |
| `10.3.100.181` | Standby | `dn_6004` | `dn_6004` | Streaming 100% |
| `10.3.100.182` | Standby | `dn_6003` | `dn_6003` | Streaming 100% |
| `10.3.100.179` | **已出集群** | （`dn_6002`，未运行） | （已删） | 配置已停用，保持停机 |

剩余事项（**未执行，需授权**）：

1. **自启/托管**：四节点仍无 systemd unit 或 `gs_om` 托管，实例全靠人工 `gs_ctl start`——
   这才是 `.179`/`.182` 能「静默消失」一个月的结构性原因。建议给三个在运行的实例加开机自启。
2. **磁盘水位告警**：见 `docs/OPENGAUSS_XLOG_RETENTION_20260921.md` 第四节第 3 条。
3. **`.179` 空间回收（可选、破坏性）**：它已出集群，但 15G 数据目录与 121M 日志仍在，
   根盘 85%（14G 可用）。彻底退役需先确认无人需要其历史数据（如做归档），再删数据目录。
4. **同步 `static_connections`**：三个实例的该值要下次重启才与新配置一致（纯显示项，不影响复制）。
