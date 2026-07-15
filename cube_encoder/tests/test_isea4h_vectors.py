"""Offline DGGRID vector checks for the public ISEA4H engine."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from grid_core.app.engines.isea4h.addressing import q2di_to_seqnum, seqnum_to_q2di
from grid_core.app.engines.isea4h.topology import cell_children, cell_neighbors
from grid_core.app.engines.isea4h_engine import ISEA4HEngine

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "isea4h"
ENGINE = ISEA4HEngine()


def _rows(name: str) -> list[dict]:
    return [json.loads(line) for line in (FIXTURE_DIR / name).read_text(encoding="utf-8").splitlines()]


def _longitude_error(first: float, second: float) -> float:
    return abs((first - second + 180.0) % 360.0 - 180.0)


@pytest.mark.parametrize("resolution", range(7))
def test_exhaustive_dggrid_centers_locate_and_roundtrip(resolution: int) -> None:
    for row in _rows(f"vectors_res{resolution}.jsonl"):
        address = ENGINE.locate_space_code(row["center_lon"], row["center_lat"], resolution)
        assert address.space_code == row["seqnum"]
        assert address.grid_level == resolution
        center = ENGINE.code_to_center(address)
        assert _longitude_error(center[0], row["center_lon"]) < 1e-4
        assert abs(center[1] - row["center_lat"]) < 1e-4
        quad, i, j = seqnum_to_q2di(int(row["seqnum"]), resolution)
        assert q2di_to_seqnum(quad, i, j, resolution) == int(row["seqnum"])


@pytest.mark.parametrize("resolution", range(5))
def test_exhaustive_dggrid_neighbor_vectors(resolution: int) -> None:
    for row in _rows(f"neighbors_res{resolution}.jsonl"):
        assert cell_neighbors(int(row["seqnum"]), resolution) == [int(value) for value in row["neighbors"]]


@pytest.mark.parametrize("resolution", range(4))
def test_exhaustive_dggrid_children_vectors(resolution: int) -> None:
    for row in _rows(f"children_res{resolution}.jsonl"):
        assert cell_children(int(row["seqnum"]), resolution) == [int(value) for value in row["children"]]
