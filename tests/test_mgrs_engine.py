import pytest

from grid_core.app.core.exceptions import NotImplementedCapabilityError, ValidationError
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


def test_mgrs_cover_not_implemented_yet():
    engine = MGRSEngine()
    with pytest.raises(NotImplementedCapabilityError):
        engine.cover_geometry({"type": "Point", "coordinates": [116.391, 39.907]}, level=5, cover_mode="intersect")
