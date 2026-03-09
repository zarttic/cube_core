import pytest
from shapely.geometry import box

from grid_core.app.core.exceptions import ValidationError
from grid_core.app.engines.mgrs_engine import MGRSEngine


def test_mgrs_locate_point_returns_cell():
    engine = MGRSEngine()
    cell = engine.locate_point(lon=116.391, lat=39.907, level=5)

    assert cell.grid_type == "mgrs"
    assert cell.space_code.startswith("50S")
    assert len(cell.center) == 2
    assert len(cell.bbox) == 4


def test_mgrs_code_to_bbox_and_geometry():
    engine = MGRSEngine()
    code = engine.locate_point(lon=116.391, lat=39.907, level=5).space_code

    bbox = engine.code_to_bbox(code)
    geometry = engine.code_to_geometry(code)

    assert bbox[0] < bbox[2]
    assert bbox[1] < bbox[3]
    assert geometry["type"] == "Polygon"


def test_mgrs_level_validation():
    engine = MGRSEngine()
    with pytest.raises(ValidationError):
        engine.locate_point(lon=116.391, lat=39.907, level=6)


def test_mgrs_cover_intersect_returns_cells():
    engine = MGRSEngine()
    geometry = {
        "type": "Polygon",
        "coordinates": [[[116.385, 39.903], [116.397, 39.903], [116.397, 39.911], [116.385, 39.911], [116.385, 39.903]]],
    }
    cells = engine.cover_geometry(geometry, level=3, cover_mode="intersect")
    target = box(116.385, 39.903, 116.397, 39.911)

    assert len(cells) > 0
    assert all(box(*cell.bbox).intersects(target) for cell in cells)


def test_mgrs_cover_mode_validation():
    engine = MGRSEngine()
    geometry = {"type": "Point", "coordinates": [116.391, 39.907]}
    with pytest.raises(ValidationError):
        engine.cover_geometry(geometry, level=3, cover_mode="minimal")


def test_mgrs_cover_contain_is_subset_of_intersect():
    engine = MGRSEngine()
    geometry = {
        "type": "Polygon",
        "coordinates": [[[116.37, 39.89], [116.43, 39.89], [116.43, 39.93], [116.37, 39.93], [116.37, 39.89]]],
    }
    intersect_codes = {c.space_code for c in engine.cover_geometry(geometry, level=2, cover_mode="intersect")}
    contain_codes = {c.space_code for c in engine.cover_geometry(geometry, level=2, cover_mode="contain")}

    assert contain_codes.issubset(intersect_codes)
    assert len(contain_codes) <= len(intersect_codes)


def test_mgrs_neighbors_k1_non_empty():
    engine = MGRSEngine()
    code = engine.locate_point(lon=116.391, lat=39.907, level=5).space_code
    codes = engine.neighbors(code, k=1)

    assert len(codes) > 0
    assert code not in codes
    assert all(engine._precision_from_code(c) == 5 for c in codes)


def test_mgrs_neighbors_k_validation():
    engine = MGRSEngine()
    code = engine.locate_point(lon=116.391, lat=39.907, level=5).space_code
    with pytest.raises(ValidationError):
        engine.neighbors(code, k=0)


def test_mgrs_parent_and_children_roundtrip():
    engine = MGRSEngine()
    code = engine.locate_point(lon=116.391, lat=39.907, level=3).space_code
    parent = engine.parent(code)
    children = engine.children(parent, target_level=3)

    assert parent == code[:-2]
    assert len(children) == 100
    assert code in children


def test_mgrs_parent_validation():
    engine = MGRSEngine()
    code = engine.locate_point(lon=116.391, lat=39.907, level=1).space_code
    with pytest.raises(ValidationError):
        engine.parent(code)


def test_mgrs_children_validation():
    engine = MGRSEngine()
    code = engine.locate_point(lon=116.391, lat=39.907, level=3).space_code
    with pytest.raises(ValidationError):
        engine.children(code, target_level=3)
