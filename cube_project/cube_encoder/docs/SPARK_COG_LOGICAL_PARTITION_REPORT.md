# 基于 Spark + COG + 时空格网索引的遥感影像并行化逻辑剖分技术报告

## 1. 报告目的与适用范围

本文给出一套可工程落地的“逻辑剖分”方案：
- 以 `cube_encoder` 作为时空格网编码底座（`geohash/mgrs/isea4h`）；
- 以 COG（Cloud Optimized GeoTIFF）作为影像标准存储；
- 以 Spark 作为并行执行引擎；
- 通过“索引驱动 + 按需读取”替代“全量物理切片”。

适用场景：
- 光学/雷达/碳监测等遥感产品的时空检索、统计、拼接、分析；
- 数据规模从 TB 到 PB 的离线批处理 + 准实时增量更新；
- 需要统一空间编码、统一时间编码、统一数据访问契约的平台型系统。

---

## 2. 核心理念：什么是逻辑剖分

传统物理剖分：
- 对每景影像按固定 tile 全量切片并生成大量实体文件；
- 优点是直接读取简单；缺点是写放大、存储膨胀、重算成本高。

逻辑剖分（本文方案）：
- 不强制把每个格网单元都落为独立文件；
- 只构建“格网索引 + 影像索引 + 时空索引”；
- 查询/任务执行时，根据索引计算 COG window，按需读取对应数据块；
- 必要时可把热点结果缓存或物化。

一句话：**先索引，后读取；先定位，后计算；以访问路径替代预切片实体。**

---

## 3. 与 cube_encoder 的衔接定位

`cube_encoder` 在此架构中的角色是“统一时空编码中枢”，主要能力：
- `grid/locate`：点 -> 格网单元；
- `grid/cover`：`geometry/bbox` -> 覆盖格网集合（`intersect/contain/minimal`）；
- `topology/*`：邻接、父子层级、编码反算几何；
- `code/st`：时空编码生成与解析。

推荐使用原则：
1. 空间索引主键统一为：`grid_type + level + space_code`；
2. 时空索引主键统一为：`st_code`（或拆分字段 + 组合索引）；
3. 所有上层任务先走索引过滤，再触发 COG 按需读取；
4. `cover_mode=minimal` 用于降低索引规模，`intersect` 用于保守召回。

---

## 4. 总体架构（分层）

1. 数据接入层
- 原始影像接入（对象存储/消息队列/目录扫描）；
- 元数据抽取（时间、轨道、传感器、云量、footprint）。

2. 标准化层（COG 生产）
- 统一 CRS、NODATA、像元类型；
- 转换为 COG（内部分块 + overviews + 压缩）；
- 写入 `cog_uri` 到资产目录。

3. 索引构建层
- 影像 footprint 调用 `cover` 得到格网集合；
- 建立 `image_id -> space_code[]` 映射；
- 结合采集时间生成 `st_code` 索引；
- 输出到湖仓索引表。

4. Spark 执行层
- 任务以 `(space_code, time_bucket)` 或其前缀分区并行；
- task 内根据空间关系计算 window 并按需读 COG；
- 计算统计/聚合/融合结果，输出结果表。

5. 查询与服务层
- API 按“时间 + 空间（格网）+ 产品条件”检索候选影像；
- 返回结果索引、可选预签名访问路径、可选聚合结果；
- 热点可缓存（Redis/Delta 物化视图）。

---

## 5. 数据标准与表模型（建议）

建议使用 Iceberg/Delta/Hudi 存储索引表，以支持增量写、ACID、时间旅行与 compaction。

### 5.1 影像资产目录 `rs_image_catalog`

关键字段：
- `image_id` STRING（唯一 ID）
- `product_type` STRING（L1/L2/反演产品）
- `sensor` STRING
- `acq_time` TIMESTAMP
- `acq_date` DATE（分区字段）
- `crs` STRING
- `bbox` ARRAY<DOUBLE> `[min_lon,min_lat,max_lon,max_lat]`
- `footprint_wkt` STRING
- `cloud_cover` DOUBLE
- `band_meta` STRING/JSON
- `cog_uri` STRING
- `overview_levels` ARRAY<INT>
- `tile_size` INT
- `nodata` DOUBLE
- `ingest_version` STRING

### 5.2 空间索引表 `rs_grid_index`

关键字段：
- `grid_type` STRING（geohash/mgrs/isea4h）
- `level` INT
- `space_code` STRING
- `image_id` STRING
- `cover_mode` STRING
- `intersect_ratio` DOUBLE（可选）
- `cell_bbox` ARRAY<DOUBLE>
- `image_bbox` ARRAY<DOUBLE>
- `acq_time` TIMESTAMP
- `partition_date` DATE

建议分区：
- 一级：`partition_date`
- 二级：`grid_type`
- 排序/聚簇：`space_code`, `acq_time`

### 5.3 时空索引表 `rs_st_index`

关键字段：
- `st_code` STRING（如 `gh:7:wtw3sjq:202603091530:v1`）
- `grid_type` STRING
- `level` INT
- `space_code` STRING
- `time_code` STRING
- `time_bucket` STRING（天/小时）
- `image_id` STRING
- `version` STRING

建议：
- 既保存完整 `st_code`，也拆分结构化字段，兼顾可读性和查询性能。

### 5.4 任务结果表 `rs_logical_partition_result`

关键字段：
- `job_id` STRING
- `space_code` STRING
- `time_bucket` STRING
- `selected_images` ARRAY<STRING>
- `window_refs` ARRAY<STRING/JSON>
- `stats_json` STRING
- `quality_flag` STRING
- `created_at` TIMESTAMP

---

## 6. COG 生产与读取规范

### 6.1 生产规范

1. 空间参考
- 平台内部统一 CRS（推荐 EPSG:4326 或产品统一投影）；
- 明确像元对齐规则（origin、resolution、snap 策略）。

2. COG 参数建议（按业务微调）
- internal tile：`512x512`（常用）
- 压缩：`DEFLATE` 或 `ZSTD`
- predictor：浮点栅格建议启用
- overviews：2,4,8,16...
- BIGTIFF：根据数据体量自动开启

3. 元数据一致性
- 记录 band 顺序、scale/offset、nodata、mask；
- 输出 `cog_quality_report`（完整性与可读性校验）。

### 6.2 按需读取策略

1. 查询阶段只取必要波段；
2. 根据目标分辨率自动命中 overview，避免全分辨率读取；
3. 只读取与 `space_code` 对应几何相交 window；
4. 对重复热点 window 增加 executor 级短时缓存；
5. 控制单 task 最大读取字节数，超限切分子任务。

---

## 7. Spark 并行化设计

### 7.1 任务拆分

推荐主键：`(grid_type, level, space_code_prefix, time_bucket)`
- `space_code_prefix` 用于削峰和并行度控制；
- 城市热点可追加 `salt`（哈希后缀）缓解倾斜。

### 7.2 两段过滤（避免全量空间 Join）

1. 粗过滤（低成本）
- 时间条件：`acq_time BETWEEN ...`
- bbox 条件：`image_bbox` 与查询 bbox 相交

2. 精过滤（高精度）
- 基于 `space_code` 索引命中候选影像
- 必要时做 cell geometry 与 footprint 的精确相交

### 7.3 Join 策略

- 小维表（任务参数、热点网格）用 `broadcast`；
- 大表用 bucket/sort + AQE；
- 避免在 UDF 中执行重型几何运算，尽量前置索引过滤。

### 7.4 失败恢复与幂等

- 输出采用 `MERGE INTO` 或分区覆盖写；
- 每条结果带 `(job_id, run_id, attempt_id)`；
- 支持失败分区重跑，不影响已完成分区。

### 7.5 性能参数基线（起步）

- `spark.sql.adaptive.enabled=true`
- `spark.sql.adaptive.skewJoin.enabled=true`
- `spark.sql.shuffle.partitions` 按数据量动态设置
- `spark.task.maxFailures=8`（按集群稳定性调整）

---

## 8. 端到端流程（离线批处理）

1. 接入新影像 -> 写 `rs_image_catalog`
2. 影像转 COG -> 质量校验 -> 更新 `cog_uri`
3. 调 `cube_encoder.grid.cover` 计算格网覆盖
4. 写 `rs_grid_index`
5. 按时间粒度生成 `st_code` -> 写 `rs_st_index`
6. Spark 读取索引执行逻辑剖分任务
7. 输出结果到 `rs_logical_partition_result`
8. 产出监控与审计报告

---

## 9. 关键算法流程（伪代码）

```python
# Step 1: 获取任务网格
query_cells = get_cells_by_geometry_and_time(grid_type, level, geometry, time_range)

# Step 2: 候选影像召回（索引驱动）
candidates = (
    spark.table("rs_grid_index")
    .where(col("grid_type") == grid_type)
    .where(col("level") == level)
    .join(query_cells, ["space_code"], "inner")
    .join(time_filter_df, ["image_id"], "inner")
)

# Step 3: 分区并行处理
# 每个分区内：space_code -> window -> COG range read -> stats
result = candidates.repartition("space_code_prefix", "time_bucket").mapPartitions(process_partition)

# Step 4: 幂等写入
upsert_result(result, table="rs_logical_partition_result", keys=["job_id", "space_code", "time_bucket"])
```

`process_partition` 核心逻辑：
1. 读取 cell geometry / bbox；
2. 计算与 COG 像素坐标对应 window；
3. 选择最佳 overview 与必要波段；
4. 读取并计算（均值、分位数、掩膜后有效像元占比等）；
5. 输出 window 引用与统计。

---

## 10. 实时/准实时增量路径

适用于“新影像持续入湖”的场景：
1. 新影像触发增量索引任务（仅处理新增 `image_id`）；
2. 增量写 `rs_grid_index/rs_st_index`；
3. Structured Streaming 或微批消费新增索引；
4. 实时更新热点区域缓存/物化视图。

关键点：
- 增量任务必须幂等（去重键：`image_id + grid_type + level + space_code`）；
- 严格区分“事件时间”和“处理时间”；
- 对迟到数据设置补算窗口。

---

## 11. 监控、SLO 与验收标准

### 11.1 运行监控指标

1. 吞吐与时延
- 每分钟处理 cell 数
- 每批任务完成时长
- P50/P95/P99 任务时延

2. 计算与资源
- Executor CPU、内存、GC 时间
- Shuffle 读写字节
- Skew task 比例

3. I/O 与数据质量
- 单任务 COG 读取字节数
- overview 命中率
- 有效像元占比
- 索引命中率（召回后有效读取比例）

4. 可靠性
- task 重试率
- 失败分区数
- 幂等冲突数

### 11.2 验收建议

1. 正确性
- 随机抽样 cell 与基准工具比对，误差在阈值内；
- `minimal` 扩展后应为 `intersect` 子集（跨引擎一致）。

2. 性能
- 在目标数据规模下，批处理时长满足 SLA；
- COG 按需读取平均字节数显著低于全图读取。

3. 稳定性
- 连续 N 天增量任务成功率达到目标（如 >99%）；
- 可重复重跑且结果一致。

---

## 12. 常见问题与规避策略

1. 层级语义不一致（尤其 MGRS）
- 问题：前端、API、引擎内部 precision 语义混淆；
- 建议：统一“平台层级 level”定义，显式做 `level <-> precision` 映射。

2. CRS 与像元对齐误差
- 问题：window 偏移导致统计漂移；
- 建议：统一重投影流程，保留对齐参数并在回归测试中锁定。

3. 空间 Join 过重
- 问题：直接几何 join 导致作业爆炸；
- 建议：先索引过滤，再小规模几何精算。

4. 热点倾斜
- 问题：热点城市网格造成长尾 task；
- 建议：prefix+salt 二次分桶，热点单独调度。

5. 小文件与元数据膨胀
- 问题：增量高频写导致查询变慢；
- 建议：定期 compact + clustering + vacuum。

---

## 13. 分阶段实施路线图

### Phase A（2-4 周）：闭环打通

目标：最小可用逻辑剖分链路
- COG 转换与校验流水线
- `grid/cover + st_code` 索引构建
- Spark 批处理读取索引并输出统计

交付：
- 可运行任务 DAG
- 三张核心表（catalog/grid/st）
- 端到端样例任务与结果验证报告

### Phase B（4-8 周）：规模化与治理

目标：可在生产稳定运行
- 增量索引更新
- AQE/倾斜优化
- 监控告警与失败重跑
- 数据质量规则完善

交付：
- SLA 与告警阈值
- 稳定性周报模板
- 回归测试集

### Phase C（8-12 周）：产品化增强

目标：面向业务应用的高可用服务
- 多时相融合策略（最优片/质量优先）
- 缓存与物化视图
- 统一查询 API 与权限审计

交付：
- 标准服务接口
- 热点区域低时延查询能力
- 成本与性能优化报告

---

## 14. 与本项目的直接落地建议（下一步）

1. 在 `grid_core/app/services` 新增“逻辑剖分服务层”编排：
- 输入：`geometry + time_range + grid_type + level + bands`
- 输出：`space_code list + st_code list + window refs`

2. 新增索引构建作业入口（可先 Python 批处理，再迁 Spark）。

3. 补齐契约文档：
- 层级定义（尤其 MGRS）
- cover 模式选择规范
- st_code 版本策略

4. 增加基准数据集与对照测试：
- dateline、极区、UTM 分区边界场景
- 不同传感器分辨率场景

---

## 15. 结论

该方案将 `cube_encoder` 的格网编码能力、COG 的随机读取能力、Spark 的分布式并行能力进行职责解耦与协同：
- `cube_encoder` 负责统一空间/时空索引语义；
- COG 负责高效存储与按需读取；
- Spark 负责大规模并行调度与计算。

在生产上，这种“索引驱动逻辑剖分”能显著降低存储膨胀和重算成本，并保持查询灵活性，是遥感数据立方体平台的优先架构路径。

---

## 16. 实施附录 A：Spark SQL DDL（Iceberg 示例）

> 说明：以下以 Iceberg + Hive Metastore 为例，若使用 Delta/Hudi 仅需替换建表语法与表属性。

```sql
CREATE DATABASE IF NOT EXISTS rs_cube;

CREATE TABLE IF NOT EXISTS rs_cube.rs_image_catalog (
  image_id STRING,
  product_type STRING,
  sensor STRING,
  acq_time TIMESTAMP,
  acq_date DATE,
  crs STRING,
  bbox ARRAY<DOUBLE>,
  footprint_wkt STRING,
  cloud_cover DOUBLE,
  band_meta STRING,
  cog_uri STRING,
  overview_levels ARRAY<INT>,
  tile_size INT,
  nodata DOUBLE,
  ingest_version STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (acq_date)
TBLPROPERTIES (
  'format-version'='2',
  'write.distribution-mode'='hash',
  'write.metadata.delete-after-commit.enabled'='true'
);
```

```sql
CREATE TABLE IF NOT EXISTS rs_cube.rs_grid_index (
  image_id STRING,
  grid_type STRING,
  level INT,
  space_code STRING,
  cover_mode STRING,
  intersect_ratio DOUBLE,
  cell_bbox ARRAY<DOUBLE>,
  image_bbox ARRAY<DOUBLE>,
  acq_time TIMESTAMP,
  partition_date DATE,
  ingest_version STRING,
  created_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (partition_date, grid_type)
TBLPROPERTIES (
  'format-version'='2',
  'write.distribution-mode'='hash'
);
```

```sql
CREATE TABLE IF NOT EXISTS rs_cube.rs_st_index (
  st_code STRING,
  grid_type STRING,
  level INT,
  space_code STRING,
  time_code STRING,
  time_bucket STRING,
  image_id STRING,
  version STRING,
  partition_date DATE,
  created_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (partition_date, grid_type)
TBLPROPERTIES (
  'format-version'='2'
);
```

```sql
CREATE TABLE IF NOT EXISTS rs_cube.rs_logical_partition_result (
  job_id STRING,
  run_id STRING,
  space_code STRING,
  time_bucket STRING,
  grid_type STRING,
  level INT,
  selected_images ARRAY<STRING>,
  window_refs ARRAY<STRING>,
  stats_json STRING,
  quality_flag STRING,
  output_uri STRING,
  created_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (time_bucket, grid_type)
TBLPROPERTIES (
  'format-version'='2',
  'write.distribution-mode'='hash'
);
```

```sql
-- 可选：为查询优化增加排序写（不同引擎语法略有差异）
-- ALTER TABLE rs_cube.rs_grid_index WRITE ORDERED BY (space_code, acq_time);
-- ALTER TABLE rs_cube.rs_st_index WRITE ORDERED BY (space_code, time_code);
```

---

## 17. 实施附录 B：PySpark 作业骨架

### 17.1 工程目录建议

```text
jobs/
  logical_partition/
    __init__.py
    config.py
    main.py
    index_builder.py
    planner.py
    cog_reader.py
    metrics.py
```

### 17.2 配置对象示例

```python
# jobs/logical_partition/config.py
from dataclasses import dataclass


@dataclass
class JobConfig:
    job_id: str
    run_id: str
    grid_type: str
    level: int
    time_start: str
    time_end: str
    geometry_geojson: str
    bands: list[int]
    time_granularity: str = "day"
    st_version: str = "v1"
    source_db: str = "rs_cube"
    target_db: str = "rs_cube"
    shuffle_partitions: int = 800
    output_mode: str = "merge"
```

### 17.3 主作业入口

```python
# jobs/logical_partition/main.py
import json
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F
from jobs.logical_partition.config import JobConfig
from jobs.logical_partition.planner import build_candidate_cells_df, build_candidate_images_df
from jobs.logical_partition.cog_reader import process_partition_rows


def create_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .getOrCreate()
    )
    return spark


def run(cfg: JobConfig) -> None:
    spark = create_spark(f"logical-partition-{cfg.job_id}")
    spark.conf.set("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))

    cells_df = build_candidate_cells_df(spark, cfg)
    candidates_df = build_candidate_images_df(spark, cfg, cells_df)

    # 控制并行粒度：space_code 前缀 + time_bucket
    keyed_df = (
        candidates_df
        .withColumn("space_prefix", F.substring("space_code", 1, 5))
        .withColumn("time_bucket", F.date_format("acq_time", "yyyyMMdd"))
        .repartition("space_prefix", "time_bucket")
    )

    schema = """
      job_id string,
      run_id string,
      space_code string,
      time_bucket string,
      grid_type string,
      level int,
      selected_images array<string>,
      window_refs array<string>,
      stats_json string,
      quality_flag string,
      output_uri string,
      created_at timestamp
    """

    result_rdd = keyed_df.rdd.mapPartitions(lambda it: process_partition_rows(it, cfg))
    result_df = spark.createDataFrame(result_rdd, schema=schema)

    result_df.createOrReplaceTempView("tmp_rs_logical_partition_result")

    if cfg.output_mode == "merge":
        spark.sql(f"""
          MERGE INTO {cfg.target_db}.rs_logical_partition_result t
          USING tmp_rs_logical_partition_result s
          ON t.job_id = s.job_id
             AND t.run_id = s.run_id
             AND t.space_code = s.space_code
             AND t.time_bucket = s.time_bucket
          WHEN MATCHED THEN UPDATE SET *
          WHEN NOT MATCHED THEN INSERT *
        """)
    else:
        result_df.writeTo(f"{cfg.target_db}.rs_logical_partition_result").append()

    spark.stop()


if __name__ == "__main__":
    # 示例：生产中建议使用 argparse + 配置中心
    cfg = JobConfig(
        job_id="job_demo_20260407",
        run_id="run_001",
        grid_type="geohash",
        level=7,
        time_start="2026-04-01T00:00:00Z",
        time_end="2026-04-07T23:59:59Z",
        geometry_geojson=json.dumps({"type": "Polygon", "coordinates": [[[116.2,39.8],[116.6,39.8],[116.6,40.1],[116.2,40.1],[116.2,39.8]]] }),
        bands=[1,2,3,4],
    )
    run(cfg)
```

### 17.4 索引规划器（调用 cube_encoder）

```python
# jobs/logical_partition/planner.py
import json
import requests
from pyspark.sql import functions as F


def _cover_cells(cfg):
    # 可替换为 SDK 直连，或在 Spark 广播后批量请求
    payload = {
        "grid_type": cfg.grid_type,
        "level": cfg.level,
        "cover_mode": "minimal",
        "boundary_type": "bbox",
        "geometry": json.loads(cfg.geometry_geojson),
        "crs": "EPSG:4326",
    }
    r = requests.post("http://cube-encoder.service/v1/grid/cover", json=payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    return [c["space_code"] for c in data.get("cells", [])]


def build_candidate_cells_df(spark, cfg):
    codes = _cover_cells(cfg)
    rows = [(cfg.grid_type, cfg.level, c) for c in codes]
    return spark.createDataFrame(rows, ["grid_type", "level", "space_code"])


def build_candidate_images_df(spark, cfg, cells_df):
    catalog = spark.table(f"{cfg.source_db}.rs_image_catalog")
    grid_idx = spark.table(f"{cfg.source_db}.rs_grid_index")

    t0 = F.to_timestamp(F.lit(cfg.time_start))
    t1 = F.to_timestamp(F.lit(cfg.time_end))

    time_filtered = (
        catalog
        .where((F.col("acq_time") >= t0) & (F.col("acq_time") <= t1))
        .select("image_id", "acq_time", "cog_uri", "bbox", "cloud_cover")
    )

    candidates = (
        grid_idx.alias("g")
        .join(cells_df.alias("c"), on=["grid_type", "level", "space_code"], how="inner")
        .join(time_filtered.alias("i"), on="image_id", how="inner")
        .select(
            "g.grid_type", "g.level", "g.space_code", "i.image_id", "i.acq_time", "i.cog_uri", "i.bbox", "i.cloud_cover"
        )
    )
    return candidates
```

### 17.5 COG 分区处理函数（示意）

```python
# jobs/logical_partition/cog_reader.py
import json
from datetime import datetime, timezone


def process_partition_rows(iterator, cfg):
    # 真实生产中建议：
    # 1) 对同一 cog_uri 做会话级复用
    # 2) window 读取走 rasterio/rioxarray + vsicurl/s3
    # 3) 输出精确统计（mean/std/pXX/valid_ratio）
    bucket = {}

    for row in iterator:
        key = (row.space_code, row.acq_time.strftime("%Y%m%d"))
        bucket.setdefault(key, []).append(row)

    for (space_code, time_bucket), rows in bucket.items():
        selected_images = [r.image_id for r in rows]
        window_refs = [f"{r.cog_uri}#window=auto" for r in rows]

        stats = {
            "count_images": len(rows),
            "avg_cloud_cover": float(sum([r.cloud_cover or 0.0 for r in rows]) / max(len(rows), 1)),
        }

        yield (
            cfg.job_id,
            cfg.run_id,
            space_code,
            time_bucket,
            cfg.grid_type,
            cfg.level,
            selected_images,
            window_refs,
            json.dumps(stats, ensure_ascii=False),
            "OK",
            "",
            datetime.now(timezone.utc),
        )
```

---

## 18. 实施附录 C：调度编排示例

### 18.1 Airflow DAG（简版）

```python
# dags/rs_logical_partition_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def run_cog_ingest(**ctx):
    # 触发 COG 转换与质量校验
    pass


def run_index_build(**ctx):
    # 触发索引构建任务（catalog/grid/st）
    pass


def run_spark_partition(**ctx):
    # 提交 spark-submit jobs/logical_partition/main.py
    pass


with DAG(
    dag_id="rs_logical_partition",
    start_date=datetime(2026, 4, 1),
    schedule="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["remote-sensing", "logical-partition"],
) as dag:
    t1 = PythonOperator(task_id="cog_ingest", python_callable=run_cog_ingest)
    t2 = PythonOperator(task_id="index_build", python_callable=run_index_build)
    t3 = PythonOperator(task_id="spark_logical_partition", python_callable=run_spark_partition)

    t1 >> t2 >> t3
```

### 18.2 DolphinScheduler 工作流 YAML（简版）

```yaml
workflow:
  name: rs_logical_partition
  schedule: "0 0/30 * * * ?"
  tasks:
    - name: cog_ingest
      type: SHELL
      command: "bash scripts/cog_ingest.sh"
    - name: build_index
      type: SHELL
      command: "bash scripts/build_index.sh"
      deps: [cog_ingest]
    - name: spark_partition
      type: SPARK
      mainClass: "org.apache.spark.deploy.PythonRunner"
      mainJar: "local:///opt/spark/python/lib/pyspark.zip"
      appName: "rs-logical-partition"
      programType: PYTHON
      mainArgs: "jobs/logical_partition/main.py --conf conf/prod.yaml"
      deps: [build_index]
```

---

## 19. 实施附录 D：配置清单与运行命令

### 19.1 Spark 提交命令（示例）

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.skewJoin.enabled=true \
  --conf spark.sql.shuffle.partitions=1200 \
  --conf spark.executor.memory=8g \
  --conf spark.executor.cores=4 \
  --conf spark.dynamicAllocation.enabled=true \
  jobs/logical_partition/main.py
```

### 19.2 关键配置项建议

- `GRID_TYPE`：`geohash|mgrs|isea4h`
- `GRID_LEVEL`：平台统一层级
- `COVER_MODE`：`minimal`（默认）/`intersect`（高召回）
- `TIME_GRANULARITY`：`hour|day|month`
- `MAX_BYTES_PER_TASK`：单 task 最大读取字节阈值
- `MAX_IMAGES_PER_CELL`：单格网最多参与影像数
- `FAIL_ON_EMPTY_INDEX`：索引为空是否失败

---

## 20. 实施附录 E：与 cube_encoder 的 API 契约建议

### 20.1 覆盖请求（用于索引构建）

```json
{
  "grid_type": "geohash",
  "level": 7,
  "cover_mode": "minimal",
  "boundary_type": "bbox",
  "geometry": {
    "type": "Polygon",
    "coordinates": [[[116.2,39.8],[116.6,39.8],[116.6,40.1],[116.2,40.1],[116.2,39.8]]]
  },
  "crs": "EPSG:4326"
}
```

### 20.2 时空编码请求（用于 st 索引）

```json
{
  "grid_type": "geohash",
  "level": 7,
  "space_code": "wtw3sjq",
  "timestamp": "2026-04-07T12:00:00Z",
  "time_granularity": "day",
  "version": "v1"
}
```

---

## 21. 实施附录 F：最小 PoC 验收脚本清单

1. `scripts/poc_ingest_and_cog.sh`
- 输入原始影像，输出 COG 与 `rs_image_catalog` 行。

2. `scripts/poc_build_index.sh`
- 调 `grid/cover` + `code/st`，构建 `rs_grid_index`、`rs_st_index`。

3. `scripts/poc_run_spark.sh`
- 提交 Spark 逻辑剖分任务并写 `rs_logical_partition_result`。

4. `scripts/poc_validate.sh`
- 抽样校验：
  - `minimal` 扩展后是 `intersect` 子集；
  - 同一输入重跑结果一致；
  - COG 平均读取字节低于基线阈值。

至此可形成“数据接入 -> 索引构建 -> 并行计算 -> 结果校验”的闭环 PoC。

---

## 22. V1 极简落地方案（低复杂度优先）

> 目标：在不引入额外复杂组件的前提下，尽快跑通 PB 级数据的逻辑剖分主流程。

### 22.1 技术栈（仅保留必要组件）

- `cube_encoder`：空间覆盖与时空编码（已有）
- `Spark`：批量并行构建索引与任务计算
- `HBase`：在线索引存储
- `COG + 对象存储`：按需读取遥感影像

不引入：
- Iceberg/Delta/Hudi（后续再升级）
- 复杂多级缓存
- 过多二级索引

### 22.2 最小表设计

#### 表 1：`rs:st_index`

用途：主检索索引（空间 + 时间 -> 影像）

- RowKey：`{space_code}#{yyyymmdd}#{image_id}`
- 列族与字段：
  - `c:uri`：COG 路径
  - `m:acq_time`：采集时间
  - `m:grid_type`：格网类型
  - `m:level`：层级
  - `q:cloud`：云量
  - `q:qc_flag`：质量标识

#### 表 2：`rs:job_result`

用途：保存逻辑剖分任务输出（可设置 TTL）

- RowKey：`{job_id}#{space_code}#{yyyymmdd}`
- 列族与字段：
  - `r:stats_json`
  - `r:selected_images`
  - `r:status`

### 22.3 写入流程（索引构建）

1. Spark 读取影像元数据：`image_id, acq_time, footprint, cog_uri, cloud`。
2. 调用 `cube_encoder /v1/grid/cover`，将 footprint 转为 `space_code[]`。
3. 展开为多行索引记录：`(space_code, yyyymmdd, image_id, cog_uri, ...)`。
4. 使用 Spark `foreachPartition` 批量写入 `rs:st_index`。

实现建议：
- 每分区复用 HBase 连接；
- 批大小按 1000~5000 控制；
- 历史回灌优先用 BulkLoad（后续优化项）。

### 22.4 读取流程（在线查询）

1. 输入：`geometry + time_range + grid_type + level`。
2. 服务端调用 `cover` 得到 `space_code[]`。
3. 按 `space_code + 日期范围` 扫 `rs:st_index`。
4. 按 `cloud/qc` 过滤候选影像。
5. 取 `cog_uri` 计算 window，按需读取 COG。
6. 返回结果，必要时写 `rs:job_result`。

### 22.5 Spark 最小任务拆分

- 分区键：`space_code`（可加日期桶）
- 每个 task 处理一组 `space_code + day`，避免超大分区
- 大范围请求直接切离线批任务，不走同步接口

### 22.6 PB 级下的最小治理措施

1. 控制查询范围：限制一次请求的 `space_code` 数与时间跨度。
2. 控制任务大小：设置单任务最大候选影像数阈值。
3. 控制结果体积：`job_result` 开 TTL，避免结果表无限增长。
4. 控制热点：若局部热点明显，再追加 `salt` 前缀（可作为 V1.1）。

### 22.7 V1 验收标准（简单可执行）

1. 正确性：随机抽样 cell 的 COG 读取结果与基准一致。
2. 完整性：输入区域在目标时间内可召回有效影像。
3. 性能：典型任务在目标时限内完成（按业务自定义）。
4. 稳定性：连续重跑结果一致，失败可重试恢复。

### 22.8 后续升级路径（保持兼容）

V1 跑稳后可平滑升级：
1. 增加二级索引与热点优化。
2. 引入湖仓存储承接全量历史。
3. 引入流批一体增量更新。

当前阶段建议先坚持“最小方案先落地、先跑通、再优化”。
