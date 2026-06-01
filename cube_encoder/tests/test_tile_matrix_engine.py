from __future__ import annotations

import pytest
from shapely.geometry import box

from grid_core.app.core.exceptions import ValidationError
from grid_core.app.engines.tile_matrix_engine import TileMatrixEngine


def test_tile_matrix_locate_point_returns_crs84_quad_cell():
    engine = TileMatrixEngine()

    cell = engine.locate_point(lon=116.391, lat=39.907, level=3)

    assert cell.grid_type == "tile_matrix"
    assert cell.space_code == "3/13/2"
    assert cell.bbox == [112.5, 22.5, 135.0, 45.0]
    assert cell.metadata["tile_matrix_set"] == "WorldCRS84Quad"
    assert cell.metadata["x"] == 13
    assert cell.metadata["y"] == 2


def test_tile_matrix_code_to_bbox_and_geometry_roundtrip():
    engine = TileMatrixEngine()

    bbox = engine.code_to_bbox("3/13/2")
    geometry = engine.code_to_geometry("3/13/2")

    assert bbox == [112.5, 22.5, 135.0, 45.0]
    assert geometry["type"] == "Polygon"
    assert geometry["coordinates"][0][0] == [112.5, 22.5]


def test_tile_matrix_cover_intersect_returns_regular_cells():
    engine = TileMatrixEngine()
    target = box(116.385, 39.903, 116.397, 39.911)

    cells = engine.cover_geometry(target.__geo_interface__, level=8, cover_mode="intersect")

    assert len(cells) > 0
    assert all(cell.grid_type == "tile_matrix" for cell in cells)
    assert all(cell.space_code.count("/") == 2 for cell in cells)
    assert all(box(*cell.bbox).intersects(target) for cell in cells)


def test_tile_matrix_cover_contain_is_subset_of_intersect():
    engine = TileMatrixEngine()
    geometry = box(112.5, 22.5, 135.0, 45.0).__geo_interface__

    intersect_codes = {cell.space_code for cell in engine.cover_geometry(geometry, level=3, cover_mode="intersect")}
    contain_codes = {cell.space_code for cell in engine.cover_geometry(geometry, level=3, cover_mode="contain")}

    assert "3/13/2" in contain_codes
    assert contain_codes.issubset(intersect_codes)


def test_tile_matrix_neighbors_parent_and_children():
    engine = TileMatrixEngine()
    code = "3/13/2"

    assert engine.parent(code) == "2/6/1"
    children = engine.children("2/6/1", target_level=3)
    assert children == ["3/12/2", "3/13/2", "3/12/3", "3/13/3"]
    neighbors = engine.neighbors(code, k=1)
    assert len(neighbors) == 8
    assert "3/13/2" not in neighbors


def test_tile_matrix_validation():
    engine = TileMatrixEngine()

    with pytest.raises(ValidationError):
        engine.locate_point(lon=116.391, lat=39.907, level=13)
    with pytest.raises(ValidationError):
        engine.code_to_bbox("3/16/2")
    with pytest.raises(ValidationError):
        engine.children("3/13/2", target_level=3)
