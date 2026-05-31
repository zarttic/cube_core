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
PYTHONPATH=../cube_encoder:. python3.8 -m cube_split.jobs.ray_logical_partition_job \
  --input-dir data/optocal \
  --manifest-path data/optocal/manifest.jsonl \
  --output-dir data/ray_output/logical_partition
```

Run product partition:

```bash
PYTHONPATH=../cube_encoder:. python3.8 -m cube_split.jobs.product_partition_job \
  --input-dir data/product \
  --output-dir data/ray_output/product
```

Run carbon satellite partition with ISEA4H and Ray:

```bash
PYTHONPATH=../cube_encoder:. python3.8 -m cube_split.jobs.carbon_partition_job \
  --input-dir data/carbon \
  --output-dir data/ray_output/carbon \
  --grid-type isea4h \
  --grid-level 5 \
  --partition-backend ray \
  --ray-address "$RAY_ADDRESS"
```

Run optical ingest E2E:

```bash
scripts/run_ray_ingest_e2e.sh
```

The script reads PostgreSQL, Ray, and MinIO settings from `POSTGRES_DSN`,
`RAY_ADDRESS`, `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, and
`MINIO_BUCKET`. Missing required settings fail explicitly for distributed
backends.

Run optical Ray partition and ingest in one job:

```bash
PYTHONPATH=../cube_encoder:. python3.8 -m cube_split.jobs.ray_logical_partition_job \
  --input-dir data/optocal \
  --manifest-path data/optocal/manifest.jsonl \
  --output-dir data/ray_output/logical_partition
```

Run AOI readback:

```bash
PYTHONPATH=../cube_encoder:. python3.8 -m cube_split.read.aoi_reader \
  --bbox 120.8 44.0 122.2 44.6 \
  --time-bucket 20260204 \
  --bands sr_b2 sr_b3 sr_b4 \
  --output .tmp/aoi_rgb.tif
```

## Tests

From this package:

```bash
PYTHONPATH=../cube_encoder:. python3.8 -m pytest tests
```

From the repository root:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.8 -m pytest cube_encoder/tests cube_split/tests
```
