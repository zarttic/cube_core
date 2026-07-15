#!/usr/bin/env python3
"""Run the non-skipping M2 real-service gate and emit a JSON outcome."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-name", required=True)
    parser.add_argument("--dangerously-reset-partition-domain", action="store_true")
    args = parser.parse_args()
    if not args.dangerously_reset_partition_domain:
        raise SystemExit("--dangerously-reset-partition-domain is required")
    root = os.getcwd()
    env = os.environ.copy()
    reset = [sys.executable, "cube_web/scripts/reset_partition_domain.py", "--database-name", args.database_name,
             "--dangerously-reset-partition-domain", "--execute"]
    subprocess.run(reset, cwd=root, env=env, check=True)
    command = [sys.executable, "-m", "pytest", "cube_web/tests/test_partition_domain_real.py", "-m", "m2_real", "--strict-markers", "-vv", "-s"]
    completed = subprocess.run(command, cwd=root, env=env, text=True, capture_output=True)
    print(completed.stdout, end="")
    print(completed.stderr, end="", file=sys.stderr)
    if completed.returncode:
        raise SystemExit(completed.returncode)
    summary = completed.stdout + completed.stderr
    if not re.search(r"\b8 passed\b", summary) or re.search(r"\b(?:skipped|xfailed|deselected)\b", summary):
        raise RuntimeError("M2 real gate requires exactly eight executed, non-skipped scenarios")
    print(
        json.dumps(
            {
                "gate": "m2-partition-domain-real",
                "status": "passed",
                "scenarios": {
                    "geohash_logical": "passed",
                    "isea4h_entity": "passed",
                    "partial_failure": "passed",
                    "atomic_rollback": "passed",
                    "unknown_commit": "passed",
                    "cleanup": "passed",
                    "catalog_handoff": "passed",
                },
                "skipped": 0,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
