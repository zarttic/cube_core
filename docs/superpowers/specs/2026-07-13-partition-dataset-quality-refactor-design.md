# 剖分、数据集管理与质检子系统重构设计

> 日期：2026-07-13
> 状态：待用户审阅
> 规范优先级：本文档是本次重构唯一规范性来源
> 非规范参考：`cube_web/docs/PARTITION_DATASET_QUALITY_REFACTOR_PLAN.md`（仅作为需求输入；与本文冲突时以本文为准）
> 适用范围：`cube_encoder`、`cube_split`、`cube_web`、`cube_web/frontend`

## 1. 目标与原则

本次工作是开发阶段的破坏式重构，不迁移旧结果数据，不保留旧格网、旧请求参数、旧返回结构或旧质检接口兼容层。

目标如下：

1. 生产格网仅保留 Geohash、扩展 MGRS 和真正的 ISEA4H。
2. 批次只承担调度职责，数据集成为查询、管理、质检和发布的业务聚合根。
3. 剖分系统直接消费载入系统生成的 COG，不再转换、重投影或重新上传输入 COG。
4. 数据集结果以版本化事务原子提交，完成状态不能早于全部明细提交。
5. 质检规则结果和每条错误完整落表，支持分页查看和全量或筛选流式导出。
6. 前端引入正式路由、领域 Store、应用壳层和共享基础设施。
7. 按里程碑使用隔离 worktree 和多 Agent 开发，严格执行独立质量门禁。

## 2. 格网架构

### 2.1 生产格网契约

| `grid_type` | 算法基线 | 固定剖分方式 | 原生层级 |
|---|---|---|---|
| `geohash` | 标准 Base32 Geohash | `logical` | 字符精度 1–12 |
| `mgrs` | 标准 UTM/UPS MGRS + 项目拓扑扩展层 | `logical` | 数字精度 0–5 |
| `isea4h` | 仓库内纯 Python ISEA aperture-4 hexagon | `entity` | 原生 resolution 0–15 |

原生层级和展示名称固定如下：

- Geohash `1–12`：`Geohash 精度 N`；
- MGRS `0=100 km`、`1=10 km`、`2=1 km`、`3=100 m`、`4=10 m`、`5=1 m`，展示为对应距离；
- ISEA4H `0–15`：`ISEA4H 分辨率 N`。

API 请求使用 `requested_grid_level`；数据集主记录保存该值及展示名称。每个格网结果保存自己的 `grid_level`，从而允许 `minimal` cover 产生混合层级。

`partition_method` 由 `grid_type` 唯一派生：

```text
geohash -> logical
mgrs    -> logical
isea4h  -> entity
```

请求若携带不一致的 `partition_method`，服务返回 422。删除 S2、Tile Matrix、Plane Grid 和当前 H3 兼容实现的生产入口、代码分支、配置、测试及当前文档引用。

### 2.2 Geohash

Geohash 使用标准小写 Base32 编码，层级等于字符精度。引擎实现定位、覆盖、几何、邻接、父级和子级，并明确处理反经线、极区和边界坐标。父级删除末字符，直接子级为标准 32 个后缀；多级 children 使用目标精度。

覆盖接口保持现有 `intersect`、`contain`、`minimal` 语义。若 `minimal` 返回混合精度，每个结果单元保存自身层级，数据集同时保存请求层级；不得把请求层级错误写入所有结果。

### 2.3 MGRS 标准码与拓扑码

MGRS 必须以 UTM/UPS 平面方格为基础，支持全球有效坐标、跨 zone、反经线以及 UTM/UPS 边界。

确定性空间归属与覆盖规则如下：

1. 点定位先按标准 UTM/UPS 域选择规则确定唯一域；恰位于 zone 或纬度带边界的点用半开区间归属，普通边界归东/北侧，`180°` 规范化为 `-180°`，UTM/UPS 分界按标准有效纬度决定；
2. 每个标准 MGRS 单元几何必须裁剪到其 UTM/UPS 有效域后参与 cover、去重和窗口计算，不把投影方格超出有效域的重叠部分计入结果；
3. 跨多个域的 AOI 先按反经线和 UTM/UPS/zone 有效域切分，在各域生成候选，最后按 `topology_code` 去重；边界相接但非面积重叠的单元保留各自身份；
4. `intersect` 选择与裁剪后 AOI 有非空面积交集的单元，`contain` 选择裁剪后单元几何完全被 AOI 覆盖的单元；仅边界接触不计为面积交集；
5. `minimal` 从 `intersect` 结果开始，只在同一标准 MGRS/UPS 域内把完整的同父精度子单元集合合并为父单元；不得跨 zone、UTM/UPS 或反经线合并；
6. 跨域邻接通过裁剪后几何共享边界判定，点接触不是邻居；边界容差以 WGS84 大地距离固定为目标单元名义边长的 `1e-8`，同时不得小于 `1e-6 m`；
7. AOI 窗口由裁剪后的格网几何与源 COG 像元网格求交产生，同一源资产和 `topology_code` 最多一条规范窗口记录。

每个 MGRS 单元同时具有：

- `space_code`：第三方 MGRS 工具可解析的标准 MGRS/UPS 编码；
- `topology_code`：项目稳定拓扑编码，用于全球唯一身份和跨区拓扑查询。

规则如下：

1. 定位、空间展示和对外交换以标准 `space_code` 为准。
2. zone 内精度父子关系遵循标准 MGRS 精度语义。
3. 跨 zone、反经线和 UTM/UPS 边界的邻接及层级关联由几何解析器计算，并使用 `topology_code` 持久化。
4. 边界处允许一对多和不规则邻接数量，API 不承诺固定 8 个邻居或固定数量子单元。
5. 拓扑 API 返回项同时携带 `space_code` 和 `topology_code`，不得把项目扩展编码冒充标准 MGRS。
6. `partition_grid_cells` 与 `partition_indexes` 保存 `topology_code`；格网单元唯一键为 `(dataset_id, output_version, grid_type, grid_level, topology_code)`。

MGRS 实现应将 UTM 与 UPS 的解析、投影、几何和边界发现拆成明确模块。跨边界 cover 不能仅依赖当前单区邻接 flood-fill。

### 2.4 真正的 ISEA4H

当前 H3 封装必须被仓库内纯 Python 的 ISEA aperture-4 hexagon 实现替换。H3 只可作为工程接口、拓扑异常处理和测试方法的参考；不得复用 H3 单元编码或把 H3 结果称为 ISEA4H。

规范数学基线固定为：

- ISEA（Icosahedral Snyder Equal Area）投影；
- HEXAGON topology；
- PURE aperture 4；
- WGS84 authalic sphere，半径 `6371.007180918475 km`；
- 固定二十面体朝向：顶点经度 `11.25°`、纬度 `58.28252559°`、相邻顶点方位角 `0°`；
- 原生 resolution `0–15`；
规范 `space_code` 使用 DGGRID v8.44 `SEQNUM`（即 `GLOBAL_SEQUENCE`）的 1-based 十进制编号。该编号不是实现自行选择的遍历序号，其规范定义为：

1. 先把单元规范化为 DGGRID `Q2DI=(quad,i,j)` 地址；北极和南极单元分别占序列两端，普通单元按 quad `1..10` 排列；
2. 每个 quad 的偏移为 `(resolution_cell_count - 2) / 10`；quad 内按 aperture-4 hexagonal Q2DI 有界地址的规范递增顺序编号；
3. 总单元数及各分辨率偏移必须与 DGGRID v8.44 `ISEA4H` 统计和 `Q2DI <-> SEQNUM` 行为一致；
4. 仓库模块同时实现 `Q2DI -> SEQNUM` 与 `SEQNUM -> Q2DI`，二者必须对全部可枚举低分辨率单元互逆；
5. 规范测试元数据提交用于生成向量的完整 metafile，至少固定：`dggs_type ISEA4H`、上述朝向、`dggs_res_specify_type SPECIFIED`、目标 resolution、`proj_datum WGS84_AUTHALIC_SPHERE`、`input/output_address_type Q2DI|SEQNUM` 和输出精度；任何未列参数使用 v8.44 preset 默认值；
6. 分辨率 0–6 对全部单元与固定参考向量交叉验证，更高分辨率使用按 quad、面边、顶点、反经线和两极分层的确定性样本，并验证双向转换性质。

规范 `space_code` 按不带前导零的 ASCII 字符串持久化。内部 Q2DI 地址用于计算并可作为诊断元数据，但不得取代公共编码。

实现拆分为独立模块：球面与二十面体常量、面选择、ISEA 正逆投影、aperture-4 分辨率变换、六边形 Q2DI 地址、SEQNUM 双向转换、几何构造、邻接、父子和区域覆盖。公共引擎只组合这些模块，不在一个文件内混合全部数学与 I/O 逻辑。

验证要求：

1. 仓库提交固定权威向量，覆盖每个分辨率、普通面、面边、顶点异常、反经线和两极；
2. 权威向量使用 DGGRID `v8.44`（Git tag commit `126881d40b32abd9ac57034d792f26a2fecf5243`）的 `ISEA4H` preset 生成；运行单元测试和生产服务不得要求 DGGRID；
3. CI 可选从该固定 commit 构建 DGGRID，并用 `dggrid -v` 校验输出包含 `DGGRID version 8.44` 后执行交叉验证；缺失时固定向量测试仍必须运行；
4. 对定位、global-sequence 往返、几何闭合、邻接对称、父子包含和 cover 完整性执行性质测试；
5. 设置候选单元、输出单元和运行时间上限，超限返回结构化错误；
6. ISEA4H 实现可以与其他无文件冲突的里程碑工作并行，但其共享 SDK 契约在并行开始前冻结；
7. ISEA4H 未通过权威向量、性质测试、独立审查和对抗验证前，格网里程碑不得提交或进入剖分集成。

DGGRID 仅为开发验证工具，不是 Python 包、Web 服务、Ray worker 或生产部署依赖。

### 2.5 编码语法与 ST code

所有公共编码先规范化再验证，并使用 ASCII：

- Geohash `space_code`：正则 `^[0123456789bcdefghjkmnpqrstuvwxyz]{1,12}$`，只接受小写，长度必须等于 `grid_level`；
- MGRS `space_code`：去除输入空白后转大写，持久化时不含空白；UTM 使用标准 `zone + band + 100km square + 2×precision digits`，UPS 使用标准极区格式；数字精度 `0–5` 必须与 `grid_level` 相等；
- MGRS `topology_code`：`mgrs-topo-v1:<domain>:<level>:<canonical-space-code>`，其中 `domain` 为 `utm-<zone><hemisphere>` 或 `ups-<hemisphere>`，`hemisphere` 仅为 `n|s`；最大 96 个 ASCII 字符；
- ISEA4H `space_code`：正则 `^[1-9][0-9]*$`，表示固定格网定义和分辨率下的 DGGS `GLOBAL_SEQUENCE`，最大 32 个字符；
- ISEA4H 的分辨率属于单元身份的一部分，必须与 `space_code` 一起验证。

MGRS 结果身份必须同时包含标准 `space_code` 和 `topology_code`，不得使用二选一字段。Geohash 和 ISEA4H 的 `topology_code` 为空。

ST code 语法固定为：

```text
<prefix>:<level>:<space_code>:<time_code>
```

前缀固定为 `gh`、`mgrs`、`i4h`。`level` 为无前导零十进制数；`time_code` 为 UTC 数字时间码，继续使用 second/minute/hour/day/month 对应的既有长度。`space_code` 禁止包含冒号。MGRS ST code 使用标准 `space_code`；全局拓扑查询通过关联的 `topology_code` 完成。删除旧 `s2`、`hx`、`tm` 和 `pg` 前缀，不解析旧 ST code。

ST code 和三类空间编码的语法、规范化、长度、级别匹配与错误码必须由 SDK、HTTP API 和数据库约束共同测试。

## 3. 数据集业务域

### 3.1 聚合关系

```text
partition_batch 1 ----- n partition_dataset
partition_dataset 1 --- n source_asset
partition_dataset 1 --- n band
partition_dataset 1 --- n output_version
output_version 1 ------- n tile
output_version 1 ------- n index_record
output_version 1 ------- n grid_cell
output_version 1 ------- n quality_run
quality_run 1 ---------- n quality_result
quality_run 1 ---------- n quality_error
quality_run 1 ---------- 0..1 warn_approval
output_version 1 ------- n publication
publication n ---------- 1 quality_run
```

调度继续由 `partition_batches`、`partition_assets` 和 `partition_job_attempts` 承担。结果域以 `partition_datasets` 为聚合根，批次状态不能代替数据集业务状态。

`partition_output_versions` 是不可变输出版本主表，至少保存 `output_version`、`dataset_id`、来源 `task_id`、请求格网和层级、状态 `staging|completed|failed|superseded`、MinIO 版本前缀、瓦片/索引/格网计数、创建时间和完成时间。所有瓦片、索引、格网、质检和发布记录必须显式外键关联 `output_version`。

读取语义固定如下：

- 不传 `output_version` 的数据集明细接口读取 `partition_datasets.current_output_version`；
- 传入版本选择器时读取指定不可变版本，并校验其属于该数据集；
- 每次质检必须绑定一个 `output_version`；手工重新质检默认绑定触发时的当前版本，也允许管理员显式选择该数据集的已完成版本；
- 只有同时引用同一 `output_version` 的质检运行才能授权发布该版本。

### 3.2 严格载入契约

剖分入口只接受新契约。每个数据集必须提供：

- 稳定的 `dataset_id`、`dataset_code`、`dataset_title`；
- `data_type` 和可选 `product_type`；
- 统一波段数组；
- 每个资产稳定的 `source_asset_id`；
- 已由载入系统生成的 `cog_uri`；
- 可用的空间、时间及校验信息。

统一波段字段为 `band_code`、`band_name`、`band_type`、`unit`、`display_order` 和 `attributes`。不长期兼容 `bands`、`band`、`polarization` 等旧输入，不从目录名、运行目录或文件路径推导数据集身份。契约不满足时在同步入口返回 422，异步接收入口将任务标记为拒绝并保存结构化原因。

## 4. 剖分执行、版本和事务

### 4.1 执行粒度

一个批次先按 `dataset_id` 分组。每个数据集独立执行、提交、重试和失败隔离；同批次中一个数据集失败不回滚其他已成功数据集。

数据流如下：

```text
载入批次
  -> 严格校验数据集、资产、波段和 COG 契约
  -> 按 dataset_id 分组
  -> 数据集独立 Ray 执行
  -> 生成版本化暂存结果
  -> OpenGauss 单数据集事务提交
  -> 切换当前 output_version 并标记 completed
  -> 自动触发首次质检
```

MinIO COG 只下载到 Ray worker 的本地缓存。Geohash 和 MGRS 只生成逻辑窗口及源 COG 引用；ISEA4H 生成实体瓦片。

### 4.2 版本化原子替换

每次数据集输出生成不可变 `output_version`。新版本完整成功前，旧成功版本保持可读。

单数据集提交事务必须：

1. 锁定或创建数据集主记录；
2. 验证任务尝试仍有效且未取消；
3. 写入或关联数据集资产和统一波段；
4. 写入该 `output_version` 的瓦片、索引和格网单元；
5. 校验数量、外键和唯一约束；
6. 将数据集 `current_output_version` 指向新版本；
7. 设置 `partition_status=completed` 和非空 `partition_completed_at`；
8. 提交事务。

任意一步失败，回滚新版本数据库结果，旧成功版本继续可用；数据集本次尝试记录为失败，不写错误完成标记。

结果记录使用确定性 `output_id` 防止同一版本重试产生重复项。组成至少包括：

```text
dataset_id + output_version + source_asset_id + band_code
+ grid_type + result_grid_level + space_code
+ mgrs_topology_code（仅 MGRS，其他格网为空）
+ time_bucket + window_identity
```

### 4.3 MinIO 对象提交

ISEA4H 实体瓦片写入不可变前缀：

```text
s3://<bucket>/partition/<dataset_id>/versions/<output_version>/tiles/...
```

对象路径本身不承担可变“当前版本”指针职责；规范指针是 OpenGauss 中 `partition_datasets.current_output_version`。所有读取先在数据库事务快照中解析版本，再按该版本记录的 `object_prefix` 读取对象，因此不会看到尚未完成的版本。数据库指针是唯一事实源，不额外维护可能与事务失配的 MinIO manifest 指针对象。

提交顺序固定为：

1. 创建 `staging` 输出版本并分配不可变对象前缀；
2. 上传实体瓦片并记录对象清单和校验和；
3. 在单个数据库事务中写入全部结果、把版本设为 `completed`，并切换 `current_output_version`；
4. 事务失败时保持旧指针，新版本标记或恢复为 `failed`，重试使用同一版本及确定性对象键验证后复用，或创建新版本；
5. 提交结果未知时先查询版本状态和当前指针，禁止盲目重复切换。

失败或失去引用的版本对象由幂等清理任务回收。清理前必须确认：

- 版本未被数据集当前指针引用；
- 版本未被任何发布快照引用；
- 超过安全保留时间；
- 对象清单属于预期数据集和版本前缀。

## 5. 数据库与开发重建

新结果域由以下表组成：

```text
partition_datasets
partition_dataset_assets
partition_dataset_bands
partition_output_versions
partition_tiles
partition_indexes
partition_grid_cells
partition_quality_runs
partition_quality_results
partition_quality_errors
partition_quality_warn_approvals
partition_publications
partition_domain_outbox
partition_domain_schema_version
```

调度域由 `partition_batches`、`partition_assets`、`partition_job_attempts` 组成。旧 `quality_reports` 在新质检接口接入时删除。当前仓库中不存在其他由 `cube_web` 管理的旧剖分结果表；因此 reset 的 legacy allowlist 固定为 `quality_reports` 和三个调度表。实现前的 schema inventory 测试必须查询目标 schema 中表、视图、物化视图、序列、索引和约束：若发现名称匹配 `partition_%` 或 `quality_%`、但不在本节新表清单或 legacy allowlist 中的对象，reset 必须拒绝执行并打印未知对象，不得猜测删除。

`cube_web_configs`、载入系统 `ard_*` 表以及 `cube_split` 的 `rs_*` 入库事实表不属于本重构 reset 范围，不得删除。S2、Tile Matrix、Plane Grid 的结果若位于 `rs_*` 事实表，也不由本 reset 删除；本轮通过删除生产写入路径和新结果域重建停止继续生成，历史 `rs_*` 数据的清理由其所属载入/入库子系统单独处理。

所有大表按数据集、版本、状态、时间和空间查询建立组合索引。schema bootstrap 写入 `partition_domain_schema_version`，版本不匹配时应用拒绝写入并提示执行显式重建，不做隐式破坏式升级。

开发阶段不迁移旧数据。显式 reset/bootstrap 按外键逆序删除：outbox、放行、发布、错误、规则结果、质检运行、索引、瓦片、格网、输出版本、波段、数据集资产、数据集，再删除 `quality_reports` 和三个调度表；随后按依赖正序创建调度域及上述新结果域。

reset 默认不删除 MinIO 版本对象。若同时提供独立的 `--purge-partition-objects` 危险参数，命令只删除配置 bucket 下 `partition/` 前缀，并在删除前完成对象清单预览和同等三重确认。

重建命令采用三重保护：

1. `CUBE_WEB_ENV=development`；
2. 明确命令行危险操作确认标志；
3. 调用者输入与实际连接匹配的固定数据库名。

任一条件不满足即拒绝执行。测试环境使用独立测试模式和测试数据库。应用正常启动绝不隐式删表。

## 6. 质检设计

### 6.1 触发与并发

数据集新输出版本提交完成后必须自动创建首次质检运行，同时提供手工重新质检 API。同一数据集允许多个质检运行并行。

自动触发不直接依赖事务提交后的进程内回调。输出版本完成事务同时写入唯一键为 `(dataset_id, output_version, event_type)` 的 `partition_domain_outbox` 事件；后台分发器幂等创建绑定该版本的质检运行。事件暂未分发时，数据集质量状态为 `pending`；分发失败记录重试次数和错误，不回滚已完成输出版本。

每次自动或手工触发都在数据库事务中锁定数据集序号计数器，分配单调递增的 `quality_sequence` 并创建运行，从而避免并发触发取得相同序号。只有同时满足以下条件的运行可以更新数据集当前质量字段：

1. 运行绑定 `partition_datasets.current_output_version`；
2. 该运行在此输出版本上具有最高 `quality_sequence`。

旧输出版本或旧序号运行即使更晚完成，也只能作为历史记录，不能覆盖当前状态。新输出版本切换时，数据集 `quality_status` 原子重置为 `pending`，`current_quality_run_id` 清空。

### 6.2 状态

质检运行状态为：

```text
pending | running | pass | warn | fail | error | cancelled
```

- 必选规则正常执行并报告一个或多个领域错误时产生 `fail`；
- 只有可选规则正常执行并报告领域告警时产生 `warn`；
- 全部启用规则正常执行且未报告错误或告警时产生 `pass`；
- 任何规则抛出未处理异常，或引擎、数据库、对象存储及其他依赖执行异常时，整次运行产生 `error`，与该规则是否必选无关；
- 质检失败或异常不回滚已完成剖分结果。

### 6.3 规则快照与完整错误

每次运行保存 `rule_set_version` 和完整规则快照，包括规则编码、名称、适用产品、必选性、参数和实现版本。后续配置变更不能改变历史运行的解释。

错误按批次持续写入 `partition_quality_errors`，不截断。每条错误保存规则编码、资产/瓦片/索引标识、行号、字段、错误编码、消息和完整上下文。规则完成后更新规则结果汇总；运行结束时汇总总状态和计数。

执行异常时保留已写错误并标记运行结果不完整，以支持诊断。展示分页限制不能影响落表和导出数量。

### 6.4 导出

CSV 和 JSON 使用流式响应，不一次加载全部错误到 Web 进程内存。导出忽略页面分页，支持：

- 无错误筛选时导出该运行全部错误；
- 带规则、错误类型等筛选时导出全部匹配错误。

文件名包含数据集编码、质检时间、运行 ID；筛选导出额外标记 `filtered`。API 必须对筛选字段使用白名单和参数化查询。

## 7. 发布设计

发布必须锁定不可变快照：

```text
dataset_id + output_version + quality_run_id
```

发布条件在一个锁定事务中校验：

- 输出版本已完成且是数据集的 `current_output_version`；
- `quality_run_id == partition_datasets.current_quality_run_id`；
- 该质检运行绑定同一 `current_output_version`，且其序号是该版本当前最高有效序号；
- 当前运行状态为 `pass`，或为 `warn` 且已获得管理员对该 `quality_run_id` 的有效放行；
- `fail`、`error`、`running` 和 `pending` 禁止发布。

发布 API 不允许用较旧质检运行替代当前运行，即使旧运行曾通过或获得 Warn 放行。

Warn 放行记录保存批准人、时间、原因、`quality_run_id` 和规则集版本，只对该次运行有效。新质检运行成为当前运行后，旧放行不能授权新运行。

发布进行中产生的新剖分或质检版本不改变本次发布内容。新版本需要创建新的发布版本。撤回必须指定属于该数据集的 `publication_id`；服务在锁定发布记录后调用服务端停用该发布快照对应的服务版本，保存撤回状态和审计信息，不删除瓦片、索引、质量记录或发布历史。已撤回版本再次发布时创建新的 `publication_id`，不复用旧记录。

## 8. API 设计

### 8.1 分页与排序

数据集、质检记录、资产、瓦片、索引、格网和错误明细统一使用：

```text
page >= 1
page_size: 服务端限制最大值
sort_by: 资源白名单字段
sort_order: asc | desc
```

响应为：

```json
{
  "items": [],
  "total": 0,
  "page": 1,
  "page_size": 20
}
```

默认按更新时间或质检时间倒序，并始终追加稳定主键作为第二排序键。

### 8.2 数据集 API

```text
GET  /v1/partition/datasets
GET  /v1/partition/datasets/{dataset_id}
GET  /v1/partition/datasets/{dataset_id}/assets
GET  /v1/partition/datasets/{dataset_id}/bands
GET  /v1/partition/datasets/{dataset_id}/tiles
GET  /v1/partition/datasets/{dataset_id}/indexes
GET  /v1/partition/datasets/{dataset_id}/grid
GET  /v1/partition/datasets/{dataset_id}/quality
GET  /v1/partition/datasets/{dataset_id}/publications
POST /v1/partition/datasets/{dataset_id}/quality-runs
POST /v1/partition/datasets/{dataset_id}/quality-runs/{quality_run_id}/warn-approvals
POST /v1/partition/datasets/{dataset_id}/publish
POST /v1/partition/datasets/{dataset_id}/publications/{publication_id}/withdraw
```

所有明细查询强制受 `dataset_id` 限制。发布请求明确携带或由服务解析并锁定 `output_version` 与 `quality_run_id`。

### 8.3 质检 API

```text
GET  /v1/quality/records
GET  /v1/quality/records/{quality_run_id}
GET  /v1/quality/records/{quality_run_id}/results
GET  /v1/quality/records/{quality_run_id}/errors
GET  /v1/quality/records/{quality_run_id}/errors/export?format=csv
GET  /v1/quality/records/{quality_run_id}/errors/export?format=json
POST /v1/quality/runs
```

新接口和前端接入完成后，在同一里程碑直接删除按数据类型拆分的 history/latest/report 路由及旧响应，不提供弃用兼容期。

## 9. 前端架构

### 9.1 框架重构

本轮完整重构前端应用框架：

1. 引入 Vue Router，定义 `/partition`、`/datasets`、`/quality`、`/encoding`、`/config` 等正式路由；
2. 用路由守卫和路由元数据表达认证及管理员权限，保留现有外部认证跳转语义；
3. 建立 `AppLayout`、导航和统一页面容器；
4. 按领域拆分 Pinia Store，至少包含 partition、datasets 和 quality；
5. 建立共享 API 模块、分页契约、状态标签、表格、详情抽屉、错误导出和请求取消工具；
6. 将巨型 `PartitionView.vue` 按格网参数、批次资产、任务队列和执行结果拆分为边界清楚的组件。

### 9.2 数据集页面

`/datasets` 首屏为分页表格，使用右侧大抽屉展示详情。详情标签包括概览、资产、波段、瓦片、索引、格网、质检和发布。

打开或切换数据集前同步清空 `dataset_id`、详情数据、选中项、标签页分页和请求令牌。请求使用 `AbortController` 尽可能取消旧请求，同时使用递增 request token；只有 ID 和 token 都匹配当前状态时才允许回写。

### 9.3 质检页面

`/quality` 首屏为全部质检记录表。详情使用统一抽屉，内部含规则结果和错误明细标签。错误明细可分页筛选；导出按钮明确区分“导出全部”和“导出当前筛选结果”。主表和抽屉均提供重新质检入口，并展示运行序号、规则版本和是否为数据集当前质量结果。

### 9.4 剖分页面

格网选择只保留 Geohash、MGRS 和 ISEA4H。删除剖分方式选择，显示自动派生的只读方式。格网类型、原生层级、层级名称和地图操作位于同一参数区；统一使用“重置”，并与“提交剖分”在同一操作行。

执行进度删除生成 COG 和转换耗时。对象文案按实际含义使用“瓦片数据”或“格网单元”。

## 10. 错误处理与可观测性

跨包错误使用稳定的错误编码和结构化上下文。至少区分：

- 输入契约拒绝；
- 格网编码或覆盖错误；
- ISEA4H 数学、编码、覆盖或权威向量不一致；
- COG 不可读、CRS 或窗口错误；
- Ray 任务取消或失败；
- 数据库事务或唯一约束失败；
- MinIO 上传、读取或清理失败；
- 质检规则失败和质检执行异常；
- 发布策略拒绝和外部发布服务失败。

日志携带 `batch_id`、`dataset_id`、`task_id`、`output_version`、`quality_run_id` 和 `publication_id` 中适用的标识。错误响应不泄露 DSN、凭据或内部对象存储密钥。

## 11. 多 Agent 开发与集成

### 11.1 里程碑

采用以下顺序，里程碑间严格串行，里程碑内只并行不重叠模块。每个里程碑必须拥有独立实现计划，列出文件所有权、共享接口、真实依赖门禁、四级质量证据、失败阻断条件和本地提交边界：

1. 冻结三格网共享契约；Geohash、MGRS、纯 Python ISEA4H 在隔离 worktree 并行实现和验证；第一里程碑的 ISEA4H 真实数据门禁使用独立 SDK/grid-cover harness 读取真实 AOI 与资产元数据，不依赖第二里程碑的版本化剖分链路；
2. 去除 COG 转换、严格载入契约和数据集版本化结果域；
3. 数据集 API、规范化质检、Warn 放行和发布；
4. Vue Router、领域 Store、数据集/质检页面和剖分页拆分；
5. 历史实现删除、全量回归和真实集群验收。

每个实现 Agent 使用隔离 worktree。主 Opus 在里程碑开始前冻结共享接口和文件所有权，并按依赖顺序集成，避免多个 Agent 同时修改共享 schema、路由注册或配置入口。

### 11.2 模型分配

- Opus：主规划、格网标准边界、ISEA4H/MGRS 算法、事务与并发集成、疑难问题和最终裁决；
- Sonnet：边界明确的后端服务、API、前端模块实现和高强度代码审查；
- Haiku：机械删除、文档同步、静态检索和窄测试执行，不独立承担架构或标准算法。

### 11.3 四级质量门禁

每个实现切片依次经过：

1. 实现 Agent 自测；
2. 独立 Sonnet 或 Opus 代码审查；
3. 对抗性验证 Agent 尝试复现边界问题并反驳实现正确性；
4. 主 Opus 综合结论并亲自运行适用测试。

每一级都保存命令、结果摘要和未解决发现，作为里程碑提交说明的一部分。测试失败、高严重度审查问题或真实依赖未验证均阻断下一里程碑。问题修复并重新通过门禁前，不创建该里程碑提交。

### 11.4 Git 操作

每个通过完整门禁的里程碑由主 Opus 创建一个本地 commit。不得自动 push、创建远端分支或 PR；任何远端操作需要用户另行明确授权。提交前只暂存本里程碑文件，避免把无关或用户已有改动纳入提交。

## 12. 测试与验收

### 12.1 自动测试

每个里程碑先运行窄测试，最终必须通过：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests cube_split/tests
cd cube_web && PYTHONPATH=../cube_encoder:../cube_split:. python3.11 -m pytest tests
python3.11 -m ruff check cube_encoder cube_split cube_web
python3.11 -m mypy cube_encoder/grid_core cube_split/cube_split cube_web/cube_web
cd cube_web/frontend && npm ci && npm run build
```

格网测试使用权威向量及性质测试，覆盖反经线、极区、UTM zone、UTM/UPS 边界、ISEA 分面边界、混合层级 cover、拓扑双编码和 ST code 解析。

事务测试覆盖多数据集部分失败、并发重试、版本原子替换、旧版本保留、取消竞态、数据库失败以及 MinIO 孤儿对象清理。

质检测试覆盖并行运行、`quality_sequence` 防旧结果覆盖、规则快照、批量完整错误、异常 `error` 状态、筛选/全量导出计数和 Warn 放行失效。

前端测试与构建覆盖正式路由、权限守卫、Store、详情竞态保护、桌面/移动布局、分页筛选、下载和旧入口删除。

### 12.2 真实依赖门禁

纯 Python ISEA4H、OpenGauss、MinIO 和 Ray 在对应里程碑执行能力预检；单元测试可以使用 fake，但里程碑完成前必须运行适用的真实依赖集成测试。ISEA4H 的“真实”门禁指仓库实现对固定权威向量、性质测试和真实剖分资产的验证；DGGRID 仅为 CI 可选交叉检查，不是必需运行依赖。

最终真实环境至少覆盖：

1. Geohash 逻辑剖分单数据集；
2. MGRS 跨 zone、反经线和 UTM/UPS 边界逻辑剖分；
3. 纯 Python ISEA4H 小层级实体剖分，并与固定权威向量一致；
4. 一个批次两个数据集，其中一个失败且另一个保持成功；
5. 质检并行运行、完整错误落表以及全量/筛选导出；
6. Pass 发布、Warn 按运行放行发布、快照锁定和撤回。

任何因环境不可用而跳过的真实测试均视为该里程碑未完成，必须明确报告阻塞，不得以本地或 mock 结果替代。

## 13. 完成标准

只有同时满足以下条件才可宣告重构完成：

- 生产代码和当前契约只包含 Geohash、扩展 MGRS、真正 ISEA4H；
- MGRS 标准码与拓扑码职责清晰，全球边界行为通过测试；
- 当前 ISEA4H 不再调用 H3，纯 Python ISEA4H 通过固定权威向量、性质测试和真实资产验证；
- 剖分链路不存在 COG 转换或重投影；
- 多数据集批次按数据集独立提交，结果版本可原子替换；
- 完成状态和结果明细处于同一事务；
- 数据集、质检和发布均具有规范化独立记录；
- 错误明细完整落表，导出计数与查询条件对应的数据库计数一致；
- Warn 放行按质检运行审计，发布锁定不可变快照；
- 新前端路由、Store、应用壳层和页面完成，旧接口及入口删除；
- pytest、ruff、mypy、前端构建和全部真实依赖场景通过；
- 每个里程碑通过四级质量门禁并具有本地里程碑提交。
