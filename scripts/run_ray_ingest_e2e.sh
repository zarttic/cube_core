#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INPUT_DIR="${1:-$ROOT_DIR/data/landsat8}"
RAY_OUTPUT_DIR="${2:-$ROOT_DIR/data/ray_output/e2e_ingest}"
DB_PATH="${3:-$ROOT_DIR/data/ingest/e2e_ingest.db}"
COG_OUTPUT_ROOT="${4:-$ROOT_DIR/data/cog/raw}"
JOB_ID="${JOB_ID:-e2e-$(date +%Y%m%d-%H%M%S)}"
GRID_TYPE="${GRID_TYPE:-geohash}"
GRID_LEVEL="${GRID_LEVEL:-7}"
COVER_MODE="${COVER_MODE:-intersect}"
TIME_GRANULARITY="${TIME_GRANULARITY:-day}"
DATASET="${DATASET:-landsat8}"
SENSOR="${SENSOR:-L8}"
ASSET_VERSION="${ASSET_VERSION:-v1}"
CUBE_VERSION="${CUBE_VERSION:-v1}"
QUALITY_RULE="${QUALITY_RULE:-best_quality_wins}"

cd "$ROOT_DIR"

python grid_core/ray_jobs/logical_partition_job.py \
  --input-dir "$INPUT_DIR" \
  --output-dir "$RAY_OUTPUT_DIR" \
  --grid-type "$GRID_TYPE" \
  --grid-level "$GRID_LEVEL" \
  --cover-mode "$COVER_MODE" \
  --time-granularity "$TIME_GRANULARITY" \
  --timing-mode

LATEST_RUN_DIR="$(ls -dt "$RAY_OUTPUT_DIR"/run_* | head -n 1)"

python -m grid_core.ingest.ray_ingest_job \
  --run-dir "$LATEST_RUN_DIR" \
  --db-path "$DB_PATH" \
  --job-id "$JOB_ID" \
  --dataset "$DATASET" \
  --sensor "$SENSOR" \
  --asset-version "$ASSET_VERSION" \
  --cube-version "$CUBE_VERSION" \
  --quality-rule "$QUALITY_RULE" \
  --cog-output-root "$COG_OUTPUT_ROOT" \
  --cog-materialize-mode copy

echo "E2E completed"
echo "run_dir=$LATEST_RUN_DIR"
echo "db_path=$DB_PATH"
echo "cog_output_root=$COG_OUTPUT_ROOT"
echo "job_id=$JOB_ID"
