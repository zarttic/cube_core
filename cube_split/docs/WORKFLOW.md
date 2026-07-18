# cube_split 当前工作流

更新时间：2026-07-16

## 1. 定位

`cube_split` 负责已交付遥感数据的剖分执行、OpenGauss/MinIO 入库、质量检查和 AOI 回读。格网 locate、cover、topology 和 ST code 能力都来自 `grid_core.sdk.CubeEncoderSDK`；调用方不得复制格网逻辑。

Current production grid contract: `geohash` and `mgrs` use logical partitioning; `isea4h` uses entity partitioning. Native levels are Geohash `1..12`, MGRS `0..5`, and ISEA4H `0..15`.

## 2. 输入与执行边界

生产剖分接收正式 `StrictPartitionRequest` 和 loader 交付的完整 `DatasetInput`。同一批次允许包含不同数据类型，每个数据集可在 `datasets[].partition` 中独立选择格网、层级和剖分参数。光学、雷达和信息产品必须提供非空 COG `assets`；`carbon` 使用原始 NetCDF/HDF5 `source_uri`（包括 `.nc4`、`.hdf5`），不要求 TIFF/COG。所有数据集在数据集层提供 `bands`。

请求使用 `requested_grid_level`。输出 cell 保留实际 `grid_level`；`minimal` cover 可以返回不同于请求层级的 cell。Geohash 与 MGRS 输出逻辑索引，ISEA4H 输出实体瓦片及其元数据。

输入 `s3://` 对象必须先由 MinIO stat 验证存在。Ray worker 从 MinIO 下载到按 URI 稳定哈希隔离的本地 source cache，在该 worker 内读取后写出规范结果对象。driver 不得将本地临时文件作为跨节点输入。

剖分不创建、转换、重投影、上传或修复 loader 输入。真实验收只使用生产 Ray cache 可读取的审阅 COG 或碳卫星原始 NetCDF/HDF5；source 前缀在验收期间为只读。

## 3. ISEA4H 约束

ISEA4H 是 entity 格网。其 `space_code` 使用未补零十进制 DGGRID SEQNUM，`cell_count(r) = 10 * 4**r + 2`，分辨率为 `0..15`。ISEA4H 运行时和测试运行时不依赖 H3 或 DGGRID。

## 4. 运行时配置

运行时配置按进程环境变量、`CUBE_WEB_ENV_FILE`、本地 `.cube_web.env` 和代码默认值的顺序解析。OpenGauss 使用 PostgreSQL 兼容的 `CUBE_WEB_POSTGRES_DSN`；Ray 使用 `CUBE_WEB_RAY_ADDRESS`；MinIO 使用 `CUBE_WEB_MINIO_*`。这些值只属于运行时，绝不能写入业务配置表或 Git。

生产操作名为 `run`。演示输入、seed 批次、绝对本机路径和凭据只允许留在隔离的演示环境，不能作为生产工作流或真实验收的前提。

## 5. 质量、输出与发布

结果对象使用 `s3://` URI；质量读取对象时通过对象存储兼容路径或 worker cache 打开。OpenGauss 保存数据集、输出、质量结果/错误和发布历史。CSV/JSON 的全量与过滤导出计数必须和同条件 OpenGauss count 一致。

发布记录生命周期只能为 `publishing|active|withdrawing|failed|withdrawn`；数据集派生发布状态只能为 `unpublished|publishing|active|withdrawing|failed|withdrawn`。旧的终态标签禁止使用。本仓库没有外部发布网关；发布和撤回以精确 OpenGauss 状态及其保留历史为准。

## 6. 验证

常规回归：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest
```

真实验收要求 OpenGauss、MinIO 和 Ray 均可用，并执行三种格网、四类产品、多数据集多景、取消与重试、质检、入库和发布场景。缺少基础设施、输入对象不可读取、skip、deselection、xfail、mock、fallback、计数不一致或任一场景失败都应以非零状态结束。
