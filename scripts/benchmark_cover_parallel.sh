#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INPUT_DIR="${1:-$ROOT_DIR/data/landsat8}"
OUTPUT_DIR="${2:-$ROOT_DIR/data/spark_output/logical_partition_bench}"

GRID_TYPE="${GRID_TYPE:-geohash}"
GRID_LEVEL="${GRID_LEVEL:-7}"
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

run_once() {
  local mode="$1"
  local start_ms end_ms elapsed_ms
  start_ms="$(date +%s%3N)"
  SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost \
    python grid_core/spark_jobs/logical_partition_job.py \
      --input-dir "$INPUT_DIR" \
      --output-dir "$OUTPUT_DIR/$mode" \
      --grid-type "$GRID_TYPE" \
      --grid-level "$GRID_LEVEL" \
      --cover-mode "$COVER_MODE" \
      --time-granularity "$TIME_GRANULARITY" \
      --repartition "$REPARTITION" \
      --partition-prefix-len "$PARTITION_PREFIX_LEN" \
      --max-cells-per-asset "$MAX_CELLS_PER_ASSET" \
      --cover-execution "$mode" \
      >/tmp/cube_encoder_bench_"$mode".log 2>&1
  end_ms="$(date +%s%3N)"
  elapsed_ms="$((end_ms - start_ms))"
  echo "$elapsed_ms"
}

echo "=== Benchmark: cover execution mode ==="
echo "input_dir=$INPUT_DIR"
echo "grid_type=$GRID_TYPE grid_level=$GRID_LEVEL cover_mode=$COVER_MODE"

driver_ms="$(run_once driver)"
spark_ms="$(run_once spark)"

python - <<PY
driver_ms = int("$driver_ms")
spark_ms = int("$spark_ms")
speedup = (driver_ms / spark_ms) if spark_ms > 0 else 0.0
print(f"driver_ms={driver_ms}")
print(f"spark_ms={spark_ms}")
print(f"speedup={speedup:.3f}x")
PY

echo "driver_log=/tmp/cube_encoder_bench_driver.log"
echo "spark_log=/tmp/cube_encoder_bench_spark.log"
