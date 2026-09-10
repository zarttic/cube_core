#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-6379}"
DASHBOARD_PORT="${DASHBOARD_PORT:-8265}"
RAY_TMP_DIR="${RAY_TMP_DIR:-/tmp/ray}"

ray stop --force >/dev/null 2>&1 || true
ray start \
  --head \
  --port "$PORT" \
  --dashboard-host 0.0.0.0 \
  --dashboard-port "$DASHBOARD_PORT" \
  --temp-dir "$RAY_TMP_DIR" \
  --disable-usage-stats

echo "Ray head started"
echo "address=$(hostname -I | awk '{print $1}'):$PORT"
echo "dashboard=http://$(hostname -I | awk '{print $1}'):$DASHBOARD_PORT"
