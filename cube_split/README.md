# cube_split

`cube_split` owns partitioning, Ray ingest, AOI readback, and storage-facing workflows. It consumes grid capabilities from `cube_encoder` through `grid_core.sdk.CubeEncoderSDK`.

Detailed workflow documentation: [docs/README.md](docs/README.md).

## Boundary

- `cube_split` handles scene-level input processing, COG conversion, grid/window partitioning, metadata writes, and AOI readback.
- `cube_encoder` handles grid locate, cover, topology, and space-time code generation.
- `cube_web` handles visualization and demo pages.

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

## Run Ingest E2E

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

Local fallback for debugging only:

```bash
METADATA_BACKEND=sqlite \
ASSET_STORAGE_BACKEND=local \
scripts/run_ray_ingest_e2e.sh
```

## AOI Readback

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

- All Ray nodes use the same Python environment and code version.
- Input/output paths are shared across nodes with identical absolute paths.

Start head node:

```bash
scripts/start_ray_head.sh
```

Join worker nodes:

```bash
scripts/start_ray_worker.sh <HEAD_IP:6379>
```

Run distributed partition test:

```bash
RAY_ADDRESS=<HEAD_IP:6379> \
REPEAT=3 \
TARGET_SEC=10 \
scripts/run_distributed_partition_test.sh
```

Stop Ray:

```bash
scripts/stop_ray_cluster.sh
```

## Tests

```bash
pytest -q tests
```
