#!/usr/bin/env bash
set -euo pipefail

ray stop --force
echo "Ray stopped on this node"
