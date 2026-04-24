#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

INPUT_DIR="${INPUT_DIR:-$ROOT_DIR/data/landsat8}"
OUTPUT_DIR="${OUTPUT_DIR:-$ROOT_DIR/data/ray_output/distributed}"
COG_INPUT_DIR="${COG_INPUT_DIR:-$ROOT_DIR/data/cog/partition_input}"
RAY_ADDRESS="${RAY_ADDRESS:-auto}"
GRID_TYPE="${GRID_TYPE:-geohash}"
GRID_LEVEL="${GRID_LEVEL:-7}"
COVER_MODE="${COVER_MODE:-intersect}"
TIME_GRANULARITY="${TIME_GRANULARITY:-day}"
RAY_PARALLELISM="${RAY_PARALLELISM:-0}"
CHUNK_SIZE="${CHUNK_SIZE:-1}"
COG_WORKERS="${COG_WORKERS:-8}"
COG_COMPRESS="${COG_COMPRESS:-LZW}"
COG_PREDICTOR="${COG_PREDICTOR:-2}"
COG_LEVEL="${COG_LEVEL:-0}"
COG_NUM_THREADS="${COG_NUM_THREADS:-ALL_CPUS}"
REPEAT="${REPEAT:-3}"
TARGET_SEC="${TARGET_SEC:-10}"
SUMMARY_PATH="${SUMMARY_PATH:-$OUTPUT_DIR/summary_$(date +%Y%m%d_%H%M%S).csv}"

mkdir -p "$OUTPUT_DIR"

echo "distributed partition test"
echo "ray_address=$RAY_ADDRESS"
echo "repeat=$REPEAT target_sec=$TARGET_SEC"
echo "summary_path=$SUMMARY_PATH"

python - <<PY
import csv
import json
import subprocess
import time
from pathlib import Path

root = Path("${ROOT_DIR}")
output_dir = Path("${OUTPUT_DIR}")
summary_path = Path("${SUMMARY_PATH}")
repeat = int("${REPEAT}")
target = float("${TARGET_SEC}")

cmd = [
    "python", "-m", "cube_split.jobs.ray_logical_partition_job",
    "--input-dir", "${INPUT_DIR}",
    "--output-dir", "${OUTPUT_DIR}",
    "--cog-input-dir", "${COG_INPUT_DIR}",
    "--cog-overwrite",
    "--cog-workers", "${COG_WORKERS}",
    "--cog-compress", "${COG_COMPRESS}",
    "--cog-predictor", "${COG_PREDICTOR}",
    "--cog-level", "${COG_LEVEL}",
    "--cog-num-threads", "${COG_NUM_THREADS}",
    "--grid-type", "${GRID_TYPE}",
    "--grid-level", "${GRID_LEVEL}",
    "--cover-mode", "${COVER_MODE}",
    "--time-granularity", "${TIME_GRANULARITY}",
    "--timing-mode",
    "--partition-backend", "ray",
    "--ray-address", "${RAY_ADDRESS}",
    "--ray-parallelism", "${RAY_PARALLELISM}",
    "--chunk-size", "${CHUNK_SIZE}",
]

rows = []
for i in range(repeat):
    t0 = time.perf_counter()
    p = subprocess.run(cmd, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=True)
    real_sec = time.perf_counter() - t0
    marker = "=== Ray logical partition job completed ===\n"
    idx = p.stdout.find(marker)
    report = json.loads(p.stdout[idx + len(marker):].strip())
    rows.append({
        "run": i + 1,
        "real_sec": round(real_sec, 3),
        "total_elapsed_sec": report["total_elapsed_sec"],
        "ray_init_elapsed_sec": report["ray_init_elapsed_sec"],
        "cog_elapsed_sec": report["cog_elapsed_sec"],
        "partition_elapsed_sec": report["partition_elapsed_sec"],
        "run_dir": report["run_dir"],
        "pass_target": float(report["total_elapsed_sec"]) <= target,
    })

summary_path.parent.mkdir(parents=True, exist_ok=True)
with summary_path.open("w", newline="", encoding="utf-8") as fh:
    w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)

failed = [r for r in rows if not r["pass_target"]]
print("runs=", len(rows), "failed=", len(failed), "summary=", summary_path)
for row in rows:
    print(row)
if failed:
    raise SystemExit(2)
PY
