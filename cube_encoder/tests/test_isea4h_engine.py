"""Tests for the pure-Python ISEA4H engine."""
from __future__ import annotations

import json
import os

import pytest

from grid_core.app.engines.isea4h.addressing import cell_count
from grid_core.app.engines.isea4h_engine import ISEA4HEngine
from grid_core.app.models.grid_address import GridAddress
from grid_core.app.utils.geometry import bbox_to_polygon

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "isea4h")
ENGINE = ISEA4HEngine()


def _load_vectors(res: int) -> list[dict]:
    path = os.path.join(FIXTURE_DIR, f"vectors_res{res}.jsonl")
    with open(path) as fh:
        return [json.loads(line) for line in fh]


def test_locate_north_pole() -> None:
    # Verified against the DGGRID v8.44 binary (TRANSFORM_POINTS, res 0):
    # the geographic north pole (0, 90) is NOT a res-0 cell centre; it falls
    # inside the pentagon with seqnum 2.
    cell = ENGINE.locate_point(0.0, 90.0, 0)
    assert cell.space_code == "2"


def test_locate_south_pole() -> None:
    # Verified against the DGGRID binary: (0, -90) at res 0 -> seqnum 12.
    expected = str(cell_count(0))
    cell = ENGINE.locate_point(0.0, -90.0, 0)
    assert cell.space_code == expected


def test_locate_returns_correct_grid_type_and_level() -> None:
    cell = ENGINE.locate_point(10.0, 20.0, 3)
    assert cell.grid_type == "isea4h"
    assert cell.grid_level == 3


@pytest.mark.parametrize("res", [0, 1, 2])
def test_fixture_center_roundtrip(res: int) -> None:
    vectors = _load_vectors(res)
    failures = []
    for v in vectors:
        expected = v["seqnum"]
        cell = ENGINE.locate_point(v["center_lon"], v["center_lat"], res)
        if cell.space_code != expected:
            failures.append(
                f"seqnum {expected}: got {cell.space_code} "
                f"at ({v['center_lon']:.4f}, {v['center_lat']:.4f})"
            )
    assert not failures, f"{len(failures)} roundtrip failures at res {res}: {failures[:3]}"


def test_cover_bbox_small_returns_cells() -> None:
    bbox_geom = {
        "type": "Polygon",
        "coordinates": [[[-1.0, -1.0], [1.0, -1.0], [1.0, 1.0], [-1.0, 1.0], [-1.0, -1.0]]],
    }
    cells = ENGINE.cover_geometry(bbox_geom, 3, "intersect")
    assert len(cells) > 0
    for cell in cells:
        assert cell.grid_type == "isea4h"
        assert cell.grid_level == 3


def test_geometry_is_closed_polygon() -> None:
    addr = GridAddress(grid_type="isea4h", grid_level=1, space_code="1")
    geom = ENGINE.code_to_geometry(addr)
    assert geom["type"] == "Polygon"
    coords = geom["coordinates"][0]
    assert len(coords) >= 4
    assert coords[0] == coords[-1]


def test_center_returns_two_floats() -> None:
    addr = GridAddress(grid_type="isea4h", grid_level=0, space_code="1")
    center = ENGINE.code_to_center(addr)
    assert len(center) == 2


def test_bbox_valid_range_or_antimeridian_wrap() -> None:
    addr = GridAddress(grid_type="isea4h", grid_level=1, space_code="2")
    bbox = ENGINE.code_to_bbox(addr)
    assert len(bbox) == 4
    assert bbox[1] <= bbox[3]
    assert all(-180.0 <= lon <= 180.0 for lon in (bbox[0], bbox[2]))
    assert not bbox_to_polygon(bbox).is_empty


def test_neighbors_returns_grid_addresses() -> None:
    addr = GridAddress(grid_type="isea4h", grid_level=2, space_code="5")
    nbs = ENGINE.neighbors(addr)
    assert len(nbs) >= 1
    for nb in nbs:
        assert nb.grid_type == "isea4h"
        assert nb.grid_level == 2


def test_parent_returns_lower_resolution() -> None:
    addr = GridAddress(grid_type="isea4h", grid_level=2, space_code="5")
    parent = ENGINE.parent(addr)
    assert parent.grid_type == "isea4h"
    assert parent.grid_level == 1
    seqnum = int(parent.space_code)
    assert 1 <= seqnum <= cell_count(1)


def test_children_returns_higher_resolution() -> None:
    addr = GridAddress(grid_type="isea4h", grid_level=0, space_code="1")
    children = ENGINE.children(addr, 1)
    assert len(children) >= 1
    for ch in children:
        assert ch.grid_type == "isea4h"
        assert ch.grid_level == 1


def test_no_h3_import() -> None:
    import ast
    engine_path = os.path.join(
        os.path.dirname(__file__), "..", "grid_core", "app", "engines", "isea4h_engine.py"
    )
    with open(engine_path) as fh:
        source = fh.read()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = ([a.name for a in node.names] if isinstance(node, ast.Import)
                     else [node.module or ""])
            for name in names:
                assert "h3" not in (name or "").lower(), f"Forbidden import: {name}"
