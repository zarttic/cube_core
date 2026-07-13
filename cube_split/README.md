# cube_split

更新时间：2026-07-13

`cube_split` 负责剖分、入库、质检和 AOI 回读。它不实现格网算法，而是通过
`grid_core.sdk.CubeEncoderSDK` 使用 `cube_encoder` 能力。

当前工作流说明见 [docs/README.md](docs/README.md)。

## 职责边界

- `cube_split`：输入解析、COG 转换、grid/window 剖分行、元数据写入、质检和 AOI 回读。
- `cube_encoder`：格网 locate、cover、topology 和时空编码生成。
- `cube_web`：可视化、托管剖分 API、任务编排和 Web 质检报告展示。

## 常用命令

运行光学逻辑剖分（默认 `s2`）：

```bash
PYTHONPATH=../cube_encoder:. python3.11 -m cube_split.jobs.ray_logical_partition_job \
  --input-dir data/optocal \
  --manifest-path data/optocal/manifest.jsonl \
  --output-dir data/ray_output/logical_partition
```

源 CRS 保留型平面窗口剖分：

```bash
PYTHONPATH=../cube_encoder:. python3.11 -m cube_split.jobs.ray_logical_partition_job \
  --input-dir data/optocal \
  --manifest-path data/optocal/manifest.jsonl \
  --grid-type plane_grid \
  --target-crs "" \
  --max-cells-per-asset 50
```

`plane_grid` 只适合逻辑窗口输出，不支持实体瓦片，也不能当作 encoder 的全球拓扑格网。

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
  --partition-backend ray \
  --ray-address "$RAY_ADDRESS"
```

运行光学入库端到端检查：

```bash
scripts/run_ray_ingest_e2e.sh
```

脚本从 `CUBE_WEB_POSTGRES_DSN`/`POSTGRES_DSN`、`CUBE_WEB_RAY_ADDRESS`/`RAY_ADDRESS`、`CUBE_WEB_MINIO_ENDPOINT`/`MINIO_ENDPOINT`、`MINIO_ACCESS_KEY`、
`MINIO_SECRET_KEY` 和 `MINIO_BUCKET` 读取 OpenGauss、Ray 和 MinIO 配置。
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
