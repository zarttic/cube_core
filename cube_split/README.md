# cube_split

`cube_split` owns partitioning, ingest, quality checks, and AOI readback. It
does not implement grid algorithms; it consumes `cube_encoder` through
`grid_core.sdk.CubeEncoderSDK`.

Current workflow documentation: [docs/README.md](docs/README.md).

## Boundary

- `cube_split`: input parsing, COG conversion, grid/window partition rows,
  metadata writes, quality checks, and AOI readback.
- `cube_encoder`: grid locate, cover, topology, and space-time code generation.
- `cube_web`: visualization, demo APIs, and web-hosted quality reports.

## Common Commands

Run optical logical partition:

```bash
PYTHONPATH=../cube_encoder:. python -m cube_split.jobs.ray_logical_partition_job \
  --input-dir data/optocal \
  --manifest-path data/optocal/manifest.jsonl \
  --output-dir data/ray_output/logical_partition
```

Run product partition:

```bash
PYTHONPATH=../cube_encoder:. python -m cube_split.jobs.product_partition_job \
  --input-dir data/product \
  --output-dir data/ray_output/product
```

Run carbon satellite partition with ISEA4H and Ray:

```bash
PYTHONPATH=../cube_encoder:. python -m cube_split.jobs.carbon_partition_job \
  --input-dir data/carbon \
  --output-dir data/ray_output/carbon \
  --grid-type isea4h \
  --grid-level 5 \
  --partition-backend ray \
  --ray-address auto
```

Run optical ingest E2E:

```bash
POSTGRES_DSN='postgresql://USER:PASSWORD@HOST:5432/cube' \
MINIO_ENDPOINT='HOST:9000' \
MINIO_ACCESS_KEY='ACCESS_KEY' \
MINIO_SECRET_KEY='SECRET_KEY' \
MINIO_BUCKET='cube' \
scripts/run_ray_ingest_e2e.sh
```

Run AOI readback:

```bash
PYTHONPATH=../cube_encoder:. python -m cube_split.read.aoi_reader \
  --bbox 120.8 44.0 122.2 44.6 \
  --time-bucket 20260204 \
  --bands sr_b2 sr_b3 sr_b4 \
  --output .tmp/aoi_rgb.tif \
  --postgres-dsn postgresql://USER:PASSWORD@HOST:5432/cube \
  --minio-endpoint HOST:9000 \
  --minio-access-key ACCESS_KEY \
  --minio-secret-key SECRET_KEY
```

## Tests

From this package:

```bash
PYTHONPATH=../cube_encoder:. pytest tests
```

From the repository root:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web pytest cube_encoder/tests cube_split/tests
```
