from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def _load_gate_module():
    script = Path(__file__).resolve().parents[1] / "scripts" / "m2_coordination_gate.py"
    spec = importlib.util.spec_from_file_location("m2_coordination_gate", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _m1_row(integration_hash: str = "a" * 40) -> dict[str, str]:
    return {
        "milestone": "M1",
        "status": "PASSED",
        "integration_hash": integration_hash,
        "l1_status": "PASSED",
        "l2_status": "PASSED",
        "l3_status": "PASSED",
        "l4_status": "PASSED",
        "review_status": "PASSED",
    }


@pytest.mark.parametrize("m1_rows", [[], [_m1_row(), _m1_row()]])
def test_requires_exactly_one_m1_row(m1_rows):
    gate = _load_gate_module()

    with pytest.raises(RuntimeError, match="exactly one milestone=M1"):
        gate.require_m2_kickoff(m1_rows, {"milestone": "M2"})


@pytest.mark.parametrize("status", ["PLANNED", "RUNNING", "FAILED"])
def test_requires_passed_m1_status(status):
    gate = _load_gate_module()
    row = _m1_row()
    row["status"] = status

    with pytest.raises(RuntimeError, match="status=PASSED"):
        gate.require_m2_kickoff([row], {"milestone": "M2"})


@pytest.mark.parametrize(
    "integration_hash",
    ["abcdef0", "a" * 39, "a" * 41, "g" * 40, *sorted(_load_gate_module().DOCUMENTATION_ONLY_HASHES)],
)
def test_rejects_invalid_or_documentation_hashes(integration_hash):
    gate = _load_gate_module()

    with pytest.raises(RuntimeError, match="implementation integration_hash"):
        gate.require_m2_kickoff([_m1_row(integration_hash)], {"milestone": "M2"})


@pytest.mark.parametrize("checkpoint", _load_gate_module().CHECKPOINT_COLUMNS)
def test_requires_every_m1_checkpoint(checkpoint):
    gate = _load_gate_module()
    row = _m1_row()
    row[checkpoint] = "FAILED"

    with pytest.raises(RuntimeError, match="passed M1 checkpoints"):
        gate.require_m2_kickoff([row], {"milestone": "M2"})


def test_rejects_wrong_target_and_conflicting_predecessor():
    gate = _load_gate_module()
    with pytest.raises(RuntimeError, match="target row"):
        gate.require_m2_kickoff([_m1_row()], {"milestone": "M3"})
    with pytest.raises(RuntimeError, match="conflicts"):
        gate.require_m2_kickoff([_m1_row()], {"milestone": "M2", "predecessor_integration_hash": "b" * 40})


def test_copies_valid_predecessor_hash():
    gate = _load_gate_module()
    m2 = {"milestone": "M2"}

    result = gate.require_m2_kickoff([_m1_row()], m2)

    assert result["predecessor_integration_hash"] == "a" * 40
