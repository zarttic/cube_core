# cube_split 当前工作流

更新时间：2026-07-26

## 1. 定位

`cube_split` 负责已交付遥感数据的剖分执行、OpenGauss/MinIO 入库、质量检查和 AOI 回读。格网 locate、cover、topology 和 ST code 能力都来自 `grid_core.sdk.CubeEncoderSDK`；调用方不得复制格网逻辑。

Current production grid contract: `geohash` and `mgrs` use logical partitioning; `isea4h` uses entity partitioning. Native levels are Geohash `1..12`, MGRS `0..5`, and ISEA4H `1..6`.

## 2. 输入与执行边界

生产剖分接收正式 `StrictPartitionRequest` 和 loader 交付的完整 `DatasetInput`。同一批次允许包含不同数据类型，每个数据集可在 `datasets[].partition` 中独立选择格网、层级和剖分参数。光学、雷达和信息产品必须提供非空 COG `assets`；`carbon` 使用原始 NetCDF/HDF5 `source_uri`（包括 `.nc4`、`.hdf5`），不要求 TIFF/COG。TanSat SIF 使用 `product_type=sif` 的 NetCDF4 原始文件，读取 `SIF_758nm` 和 `SIF_771nm` 两个观测量，不转换为 COG。所有数据集在数据集层提供 `bands`。

请求使用 `requested_grid_level`。输出 cell 保留实际 `grid_level`；`minimal` cover 可以返回不同于请求层级的 cell。Geohash 与 MGRS 输出逻辑索引，ISEA4H 输出实体瓦片及其元数据。

输入 `s3://` 对象必须先由 MinIO stat 验证存在。Ray worker 从 MinIO 下载到按 URI 稳定哈希隔离的本地 source cache，在该 worker 内读取后写出规范结果对象。driver 不得将本地临时文件作为跨节点输入。

剖分不创建、转换、重投影、上传或修复 loader 输入。真实验收只使用生产 Ray cache 可读取的审阅 COG 或碳卫星原始 NetCDF/HDF5；TanSat SIF 通过 NetCDF4 变量契约读取，source 前缀在验收期间为只读。

## 3. ISEA4H 约束

ISEA4H 是 entity 格网。其 `space_code` 使用未补零十进制 DGGRID SEQNUM，`cell_count(r) = 10 * 4**r + 2`，生产分辨率为 `1..6`。ISEA4H 运行时和测试运行时不依赖 H3 或 DGGRID。

## 4. Ray 实体执行

生产 ISEA4H 按源 COG 的空间范围拆成固定 `4 x 4` 的 16 个 Ray 分片。每个分片以
`num_cpus=1` 独立调度；并行度来自 16 个可调度 task，而不是向单 task 声明 16 CPU。为避免
分片边界漏格，分片范围带有小范围 WGS84 重叠；driver 按稳定 `output_id` 去重实体瓦片、
索引和 cell。

同一 worker 节点的分片可复用按源 URI 稳定哈希命名的本地缓存。源下载的 `.part` 临时文件
必须持有源级文件锁，避免并发任务互相覆盖。worker 从 MinIO 缓存源对象并将实体瓦片上传回
MinIO；任何 driver 本地临时路径都不得作为跨节点输入。

`max_cells_per_asset=0` 仍表示无上限，不会因采用 16 分片而自动降低层级或截断。2026-07-26
在同一景、同一波段、ISEA4H level 11 的真实 COG 上，4783 个实体瓦片的剖分从单 task 约
192 秒降至 16 分片约 24 秒墙钟时间。该环境测量仅用于容量评估，不是性能承诺。

## 5. 运行时配置

运行时配置按进程环境变量、`CUBE_WEB_ENV_FILE`、本地 `.cube_web.env` 和代码默认值的顺序解析。OpenGauss 使用 PostgreSQL 兼容的 `CUBE_WEB_POSTGRES_DSN`；Ray 使用 `CUBE_WEB_RAY_ADDRESS`；MinIO 使用 `CUBE_WEB_MINIO_*`。这些值只属于运行时，绝不能写入业务配置表或 Git。

生产操作名为 `run`。演示输入、seed 批次、绝对本机路径和凭据只允许留在隔离的演示环境，不能作为生产工作流或真实验收的前提。

## 6. 质量、输出与发布

结果对象使用 `s3://` URI；质量读取对象时通过对象存储兼容路径或 worker cache 打开。OpenGauss 保存数据集、输出、质量结果/错误和发布历史。CSV/JSON 的全量与过滤导出计数必须和同条件 OpenGauss count 一致。

发布记录生命周期只能为 `publishing|active|withdrawing|failed|withdrawn`；数据集派生发布状态只能为 `unpublished|publishing|active|withdrawing|failed|withdrawn`。旧的终态标签禁止使用。本仓库没有外部发布网关；发布和撤回以精确 OpenGauss 状态及其保留历史为准。

## 7. 验证

常规回归：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest
```

真实验收要求 OpenGauss、MinIO 和 Ray 均可用，并执行三种格网、四类产品、多数据集多景、取消与重试、质检、入库和发布场景。缺少基础设施、输入对象不可读取、skip、deselection、xfail、mock、fallback、计数不一致或任一场景失败都应以非零状态结束。
