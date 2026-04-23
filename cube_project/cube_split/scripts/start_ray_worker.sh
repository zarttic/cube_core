#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <HEAD_IP:PORT>"
  exit 1
fi

HEAD_ADDR="$1"
RAY_TMP_DIR="${RAY_TMP_DIR:-/tmp/ray}"

ray stop --force >/dev/null 2>&1 || true
ray start \
  --address "$HEAD_ADDR" \
  --temp-dir "$RAY_TMP_DIR" \
  --disable-usage-stats

echo "Ray worker joined: $HEAD_ADDR"
