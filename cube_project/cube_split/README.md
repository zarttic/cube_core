# cube_split

`cube_split` hosts partition jobs, ingest pipeline, AOI readback utilities, test fixtures, and partition regression tests.

It consumes grid capabilities from `cube_encoder` through the installed Python package (`grid_core.sdk.CubeEncoderSDK`).
Before partitioning, each source band TIF is converted to COG, then grid/window partitioning is executed on COG inputs.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
cd ../cube_encoder
python -m pip install --upgrade pip build
python -m build
python -m pip install --force-reinstall dist/*.whl
cd ../cube_split
python -m pip install -r requirements.txt
```

## Run

```bash
POSTGRES_DSN='postgresql://postgres:postgres@127.0.0.1:5432/cube' \
MINIO_ENDPOINT='127.0.0.1:9000' \
MINIO_ACCESS_KEY='minioadmin' \
MINIO_SECRET_KEY='minioadmin' \
MINIO_BUCKET='cube' \
scripts/run_ray_ingest_e2e.sh
```

Defaults:
- Metadata backend: PostgreSQL (`METADATA_BACKEND=postgres`)
- Asset backend: MinIO (`ASSET_STORAGE_BACKEND=minio`)

Local fallback (for debugging only):

```bash
METADATA_BACKEND=sqlite \
ASSET_STORAGE_BACKEND=local \
scripts/run_ray_ingest_e2e.sh
```

## AOI Readback

Use the AOI reader to resolve `space_code[]` through `cube_encoder`, query `rs_cube_cell_fact`, read COG windows from MinIO, and merge them into a multi-band GeoTIFF:

```bash
python -m cube_split.read.aoi_reader \
  --bbox 120.8 44.0 122.2 44.6 \
  --time-bucket 20260204 \
  --bands sr_b2 sr_b3 sr_b4 \
  --output .tmp/aoi_rgb.tif \
  --postgres-dsn postgresql://postgres:postgres@127.0.0.1:55432/cube \
  --minio-endpoint 127.0.0.1:59000
```

## Distributed Ray Cluster

Prerequisites:
- All Ray nodes use the same Python env and code version.
- Input/output paths are shared across nodes with identical absolute paths (for example NFS mount).

1. Start head node:

```bash
scripts/start_ray_head.sh
```

2. Join worker nodes (run on each worker):

```bash
scripts/start_ray_worker.sh <HEAD_IP:6379>
```

3. Run distributed partition test (run on head):

```bash
RAY_ADDRESS=<HEAD_IP:6379> \
REPEAT=3 \
TARGET_SEC=10 \
scripts/run_distributed_partition_test.sh
```

4. Stop Ray on any node:

```bash
scripts/stop_ray_cluster.sh
```

## Tests

```bash
pytest -q tests
```
