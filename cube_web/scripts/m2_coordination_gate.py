from __future__ import annotations

import argparse
import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

PASSED = "PASSED"
SHA40 = re.compile(r"^[0-9a-f]{40}$")
DOCUMENTATION_ONLY_HASHES = {
    "d667e25e0d2597219554e206d8bd91876c83802e",
    "3da8c5f1fc4148c670a33214144ed451df9b3254",
}
CHECKPOINT_COLUMNS = ("l1_status", "l2_status", "l3_status", "l4_status", "review_status")


def require_m2_kickoff(m1_rows: list[Mapping[str, str]], m2_row: dict[str, str]) -> dict[str, str]:
    if len(m1_rows) != 1 or m1_rows[0].get("milestone") != "M1":
        raise RuntimeError("M2 requires exactly one milestone=M1 coordination row")
    m1 = m1_rows[0]
    integration_hash = m1.get("integration_hash", "")
    if m1.get("status") != PASSED:
        raise RuntimeError("M2 requires M1 status=PASSED")
    if not SHA40.fullmatch(integration_hash) or integration_hash in DOCUMENTATION_ONLY_HASHES:
        raise RuntimeError("M2 requires the 40-hex M1 implementation integration_hash; documentation-plan commits are invalid")
    failed = [name for name in CHECKPOINT_COLUMNS if m1.get(name) != PASSED]
    if failed:
        raise RuntimeError(f"M2 requires passed M1 checkpoints: {failed}")
    if m2_row.get("milestone") != "M2":
        raise RuntimeError("coordination target row must be milestone=M2")
    existing = m2_row.get("predecessor_integration_hash", "")
    if existing not in {"", integration_hash}:
        raise RuntimeError("M2 predecessor_integration_hash conflicts with M1 integration_hash")
    m2_row["predecessor_integration_hash"] = integration_hash
    if m2_row["predecessor_integration_hash"] != m1["integration_hash"]:
        raise RuntimeError("M2 predecessor hash copy failed")
    return m2_row


def verify_ledger(path: Path) -> dict[str, str]:
    payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise RuntimeError("coordination ledger rows must be a list")
    m1_rows = [row for row in rows if isinstance(row, dict) and row.get("milestone") == "M1"]
    m2_rows = [row for row in rows if isinstance(row, dict) and row.get("milestone") == "M2"]
    if len(m2_rows) != 1:
        raise RuntimeError("M2 requires exactly one milestone=M2 coordination row")
    result = require_m2_kickoff(m1_rows, m2_rows[0])
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify and persist the M2 predecessor ledger gate")
    parser.add_argument("--ledger", default="docs/milestone_coordination.json")
    args = parser.parse_args()
    row = verify_ledger(Path(args.ledger))
    print(json.dumps({"m2_predecessor_integration_hash": row["predecessor_integration_hash"]}, ensure_ascii=True))


if __name__ == "__main__":
    main()
