#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENCODER_DIR="${ENCODER_DIR:-$ROOT_DIR/../cube_encoder}"

cd "$ENCODER_DIR"
python -m build --no-isolation

LATEST_WHEEL="$(ls -t dist/cube_encoder-*.whl | head -n 1)"
python -m pip install --force-reinstall "$LATEST_WHEEL"

cd "$ROOT_DIR"
python -m pip install -r requirements.txt

echo "installed cube_encoder package: $LATEST_WHEEL"
