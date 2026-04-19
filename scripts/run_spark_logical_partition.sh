#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INPUT_DIR="${1:-$ROOT_DIR/data/landsat8}"
OUTPUT_DIR="${2:-$ROOT_DIR/data/spark_output/logical_partition}"
GRID_TYPE="${GRID_TYPE:-geohash}"
GRID_LEVEL="${GRID_LEVEL:-5}"
COVER_MODE="${COVER_MODE:-intersect}"
TIME_GRANULARITY="${TIME_GRANULARITY:-day}"
REPARTITION="${REPARTITION:-0}"
PARTITION_PREFIX_LEN="${PARTITION_PREFIX_LEN:-3}"
MAX_CELLS_PER_ASSET="${MAX_CELLS_PER_ASSET:-5000}"

cd "$ROOT_DIR"

unset SPARK_HOME
unset SPARK_CONF_DIR
unset HADOOP_CONF_DIR
unset YARN_CONF_DIR

export PYSPARK_PYTHON="/home/hadoop/anaconda3/bin/python3.11"
export PYSPARK_DRIVER_PYTHON="/home/hadoop/anaconda3/bin/python3.11"
export PYSPARK_DRIVER_PYTHON_OPTS=""

python grid_core/spark_jobs/logical_partition_job.py \
  --input-dir "$INPUT_DIR" \
  --output-dir "$OUTPUT_DIR" \
  --grid-type "$GRID_TYPE" \
  --grid-level "$GRID_LEVEL" \
  --cover-mode "$COVER_MODE" \
  --time-granularity "$TIME_GRANULARITY" \
  --repartition "$REPARTITION" \
  --partition-prefix-len "$PARTITION_PREFIX_LEN" \
  --max-cells-per-asset "$MAX_CELLS_PER_ASSET"
