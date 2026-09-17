# cube_split

更新时间：2026-09-18

`cube_split` 负责剖分、入库、质检和 AOI 回读。它不实现格网算法，而是通过
`grid_core.sdk.CubeEncoderSDK` 使用 `cube_encoder` 能力。

当前工作流说明见 [docs/README.md](docs/README.md)。

## 职责边界

- `cube_split`：输入解析、grid/window 剖分行与实体瓦片生成、元数据/资产入库、质检和 AOI 回读。
- `cube_encoder`：格网 locate、cover、topology 和时空编码生成。
- `cube_web`：可视化、托管剖分 API、任务编排和 Web 质检报告展示。

剖分链路不做 COG 转换：源 COG 由 `jobs/ray_partition_core.cache_source_cog` 原样缓存后读取，逻辑剖分只落 `s3://` 引用；COG 转换属于载入子系统。

## 常用命令

运行光学逻辑剖分（默认 `geohash`）：

```bash
PYTHONPATH=../cube_encoder:. python3.11 -m cube_split.jobs.ray_logical_partition_job \
  --input-dir data/optocal \
  --manifest-path data/optocal/manifest.jsonl \
  --output-dir data/ray_output/logical_partition
```

运行标准 MGRS 平面格网逻辑剖分：

```bash
PYTHONPATH=../cube_encoder:. python3.11 -m cube_split.jobs.ray_logical_partition_job \
  --input-dir data/optocal \
  --manifest-path data/optocal/manifest.jsonl \
  --grid-type mgrs \
  --grid-level 1 \
  --max-cells-per-asset 50
```

`max-cells-per-asset=0` 表示不限制格网数量；smoke 和调试命令应使用小的正数。

运行产品剖分：

```bash
PYTHONPATH=../cube_encoder:. python3.11 -m cube_split.jobs.product_partition_job \
  --input-dir data/product \
  --output-dir data/ray_output/product
```

使用 ISEA4H 和 Ray 运行碳卫星剖分：

```bash
PYTHONPATH=../cube_encoder:. python3.11 -m cube_split.jobs.carbon_partition_job \
  --input-dir data/carbon \
  --output-dir data/ray_output/carbon \
  --grid-type isea4h \
  --grid-level 5 \
  --partition-backend ray
```

`--ray-address` 省略时默认取 `runtime_config.ray_address()`；`.cube_web.env` 的变量不会自动导出到 shell，不要用 `"$CUBE_WEB_RAY_ADDRESS"` 展开传参。

光学入库端到端检查脚本已归档，见
[archive/scripts/cube_split/run_ray_ingest_e2e.sh](../archive/scripts/cube_split/run_ray_ingest_e2e.sh)；
它需要外部 Ray、MinIO 与 OpenGauss，当前不随代码维护。

Ray 剖分与入库作业从 `CUBE_WEB_POSTGRES_DSN`/`POSTGRES_DSN`/`DATABASE_URL`、`CUBE_WEB_RAY_ADDRESS`/`RAY_ADDRESS`、`CUBE_WEB_MINIO_ENDPOINT`/`MINIO_ENDPOINT`、
`CUBE_WEB_MINIO_ACCESS_KEY`/`MINIO_ACCESS_KEY`、`CUBE_WEB_MINIO_SECRET_KEY`/`MINIO_SECRET_KEY` 和 `CUBE_WEB_MINIO_BUCKET`/`MINIO_BUCKET` 读取 OpenGauss、Ray 和 MinIO 配置。
分布式后端缺少必需配置时会显式失败。

运行光学 Ray 剖分并在同一作业内入库：

```bash
PYTHONPATH=../cube_encoder:. python3.11 -m cube_split.jobs.ray_logical_partition_job \
  --input-dir data/optocal \
  --manifest-path data/optocal/manifest.jsonl \
  --output-dir data/ray_output/logical_partition
```

运行 AOI 回读：

```bash
PYTHONPATH=../cube_encoder:. python3.11 -m cube_split.read.aoi_reader \
  --bbox 120.8 44.0 122.2 44.6 \
  --time-bucket 20260204 \
  --bands sr_b2 sr_b3 sr_b4 \
  --output .tmp/aoi_rgb.tif
```

## 测试

在本包内运行：

```bash
PYTHONPATH=../cube_encoder:. python3.11 -m pytest tests
```

在仓库根目录运行：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests cube_split/tests
```
