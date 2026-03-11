import h3
import pytest

from grid_core.app.core.exceptions import ValidationError
from grid_core.app.engines.isea4h_engine import ISEA4HEngine


def test_isea4h_locate_point_returns_cell():
    engine = ISEA4HEngine()
    cell = engine.locate_point(lon=116.391, lat=39.907, level=6)
    assert cell.grid_type == "isea4h"
    assert h3.is_valid_cell(cell.space_code)
    assert h3.get_resolution(cell.space_code) == 6
    assert len(cell.bbox) == 4
    assert len(cell.center) == 2


def test_isea4h_code_to_bbox_and_geometry():
    engine = ISEA4HEngine()
    code = engine.locate_point(lon=116.391, lat=39.907, level=5).space_code
    bbox = engine.code_to_bbox(code)
    geometry = engine.code_to_geometry(code)
    assert bbox[0] < bbox[2]
    assert bbox[1] < bbox[3]
    assert geometry["type"] == "Polygon"


def test_isea4h_neighbors_parent_children():
    engine = ISEA4HEngine()
    code = engine.locate_point(lon=116.391, lat=39.907, level=4).space_code
    neighbors = engine.neighbors(code, k=1)
    parent = engine.parent(code)
    children = engine.children(parent, target_level=4)
    assert len(neighbors) > 0
    assert code not in neighbors
    assert code in children


def test_isea4h_cover_modes():
    engine = ISEA4HEngine()
    geometry = {
        "type": "Polygon",
        "coordinates": [[[116.37, 39.89], [116.43, 39.89], [116.43, 39.93], [116.37, 39.93], [116.37, 39.89]]],
    }
    intersect = {c.space_code for c in engine.cover_geometry(geometry, level=6, cover_mode="intersect")}
    contain = {c.space_code for c in engine.cover_geometry(geometry, level=6, cover_mode="contain")}
    minimal = {c.space_code for c in engine.cover_geometry(geometry, level=6, cover_mode="minimal")}
    expanded_minimal: set[str] = set()
    for code in minimal:
        code_level = h3.get_resolution(code)
        if code_level == 6:
            expanded_minimal.add(code)
        else:
            expanded_minimal.update(h3.cell_to_children(code, 6))
    assert contain.issubset(intersect)
    assert expanded_minimal.issubset(intersect)
    assert len(minimal) <= len(expanded_minimal)


def test_isea4h_validation():
    engine = ISEA4HEngine()
    with pytest.raises(ValidationError):
        engine.locate_point(lon=116.391, lat=39.907, level=13)
    with pytest.raises(ValidationError):
        engine.neighbors("invalid-cell", k=1)
    with pytest.raises(ValidationError):
        engine.neighbors(engine.locate_point(lon=116.391, lat=39.907, level=6).space_code, k=0)
    with pytest.raises(ValidationError):
        root_code = engine.locate_point(lon=116.391, lat=39.907, level=1).space_code
        engine.parent(root_code)
