#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INPUT_DIR="${1:-$ROOT_DIR/data/landsat8}"
RAY_OUTPUT_DIR="${2:-$ROOT_DIR/data/ray_output/e2e_ingest}"
DB_PATH="${3:-$ROOT_DIR/data/ingest/e2e_ingest.db}"
COG_OUTPUT_ROOT="${4:-$ROOT_DIR/data/cog/raw}"
COG_INPUT_DIR="${COG_INPUT_DIR:-$ROOT_DIR/data/cog/partition_input}"
METADATA_BACKEND="${METADATA_BACKEND:-postgres}"
ASSET_STORAGE_BACKEND="${ASSET_STORAGE_BACKEND:-minio}"
POSTGRES_DSN="${POSTGRES_DSN:-postgresql://postgres:postgres@127.0.0.1:5432/cube}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-127.0.0.1:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
MINIO_BUCKET="${MINIO_BUCKET:-cube}"
MINIO_PREFIX="${MINIO_PREFIX:-cube/raw}"
MINIO_UPLOAD_WORKERS="${MINIO_UPLOAD_WORKERS:-8}"
JOB_ID="${JOB_ID:-e2e-$(date +%Y%m%d-%H%M%S)}"
GRID_TYPE="${GRID_TYPE:-geohash}"
GRID_LEVEL="${GRID_LEVEL:-7}"
COVER_MODE="${COVER_MODE:-intersect}"
TIME_GRANULARITY="${TIME_GRANULARITY:-day}"
PARTITION_BACKEND="${PARTITION_BACKEND:-ray}"
RAY_ADDRESS="${RAY_ADDRESS:-auto}"
RAY_PARALLELISM="${RAY_PARALLELISM:-0}"
CHUNK_SIZE="${CHUNK_SIZE:-1}"
COG_WORKERS="${COG_WORKERS:-8}"
COG_COMPRESS="${COG_COMPRESS:-LZW}"
COG_PREDICTOR="${COG_PREDICTOR:-2}"
COG_LEVEL="${COG_LEVEL:-0}"
COG_NUM_THREADS="${COG_NUM_THREADS:-ALL_CPUS}"
DATASET="${DATASET:-landsat8}"
SENSOR="${SENSOR:-L8}"
ASSET_VERSION="${ASSET_VERSION:-v1}"
CUBE_VERSION="${CUBE_VERSION:-v1}"
QUALITY_RULE="${QUALITY_RULE:-best_quality_wins}"

cd "$ROOT_DIR"

python -m cube_split.jobs.ray_logical_partition_job \
  --input-dir "$INPUT_DIR" \
  --output-dir "$RAY_OUTPUT_DIR" \
  --cog-input-dir "$COG_INPUT_DIR" \
  --cog-workers "$COG_WORKERS" \
  --cog-compress "$COG_COMPRESS" \
  --cog-predictor "$COG_PREDICTOR" \
  --cog-level "$COG_LEVEL" \
  --cog-num-threads "$COG_NUM_THREADS" \
  --grid-type "$GRID_TYPE" \
  --grid-level "$GRID_LEVEL" \
  --cover-mode "$COVER_MODE" \
  --time-granularity "$TIME_GRANULARITY" \
  --partition-backend "$PARTITION_BACKEND" \
  --ray-address "$RAY_ADDRESS" \
  --ray-parallelism "$RAY_PARALLELISM" \
  --chunk-size "$CHUNK_SIZE" \
  --timing-mode

LATEST_RUN_DIR="$(ls -dt "$RAY_OUTPUT_DIR"/run_* | head -n 1)"

python -m cube_split.ingest.ray_ingest_job \
  --run-dir "$LATEST_RUN_DIR" \
  --job-id "$JOB_ID" \
  --dataset "$DATASET" \
  --sensor "$SENSOR" \
  --asset-version "$ASSET_VERSION" \
  --cube-version "$CUBE_VERSION" \
  --quality-rule "$QUALITY_RULE" \
  --metadata-backend "$METADATA_BACKEND" \
  --asset-storage-backend "$ASSET_STORAGE_BACKEND" \
  --postgres-dsn "$POSTGRES_DSN" \
  --minio-endpoint "$MINIO_ENDPOINT" \
  --minio-access-key "$MINIO_ACCESS_KEY" \
  --minio-secret-key "$MINIO_SECRET_KEY" \
  --minio-bucket "$MINIO_BUCKET" \
  --minio-prefix "$MINIO_PREFIX" \
  --minio-upload-workers "$MINIO_UPLOAD_WORKERS" \
  --cog-output-root "$COG_OUTPUT_ROOT" \
  --db-path "$DB_PATH" \
  --cog-materialize-mode copy

echo "E2E completed"
echo "run_dir=$LATEST_RUN_DIR"
echo "metadata_backend=$METADATA_BACKEND"
echo "asset_storage_backend=$ASSET_STORAGE_BACKEND"
echo "postgres_dsn=$POSTGRES_DSN"
echo "minio_endpoint=$MINIO_ENDPOINT"
echo "minio_bucket=$MINIO_BUCKET"
echo "db_path=$DB_PATH"
echo "cog_input_dir=$COG_INPUT_DIR"
echo "cog_output_root=$COG_OUTPUT_ROOT"
echo "job_id=$JOB_ID"
echo "partition_backend=$PARTITION_BACKEND"
echo "ray_address=$RAY_ADDRESS"
