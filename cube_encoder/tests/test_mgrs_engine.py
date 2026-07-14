from __future__ import annotations

import pytest
from shapely.geometry import box

from grid_core.app.core.exceptions import ValidationError
from grid_core.app.engines.mgrs_engine import MGRSEngine


def _expand_mgrs_to_level(engine: MGRSEngine, addresses: list, target_level: int) -> set:
    out: set[str] = set()
    for address in addresses:
        if address.grid_level == target_level:
            out.add(address.space_code)
        else:
            out.update(c.space_code for c in engine.children(address, target_level))
    return out


def test_mgrs_locate_point_returns_cell():
    engine = MGRSEngine()
    cell = engine.locate_point(lon=116.391, lat=39.907, requested_grid_level=5)

    assert cell.grid_type == "mgrs"
    assert cell.space_code.startswith("50S")
    assert len(cell.center) == 2
    assert len(cell.bbox) == 4


def test_mgrs_code_to_bbox_and_geometry():
    engine = MGRSEngine()
    address = engine.locate_space_code(lon=116.391, lat=39.907, requested_grid_level=5)

    bbox = engine.code_to_bbox(address)
    geometry = engine.code_to_geometry(address)

    assert bbox[0] < bbox[2]
    assert bbox[1] < bbox[3]
    assert geometry["type"] == "Polygon"


def test_mgrs_level1_geometry_handles_global_cells_without_bbox_errors():
    engine = MGRSEngine()

    cell = engine.locate_point(lon=161.82540138891696, lat=14.910591740788163, requested_grid_level=0)

    assert cell.space_code == "57PZS"
    assert cell.bbox[0] < cell.bbox[2]
    assert cell.bbox[1] < cell.bbox[3]
    assert cell.geometry["type"] == "Polygon"
    ring = cell.geometry["coordinates"][0]
    assert len(ring) >= 4
    assert ring[0] == ring[-1]


def test_mgrs_level1_geometry_keeps_antimeridian_cells_mappable():
    engine = MGRSEngine()

    cell = engine.locate_point(lon=179.7867081825727, lat=62.449852732870454, requested_grid_level=0)
    coords = cell.geometry["coordinates"][0]

    assert cell.space_code == "60VXQ"
    assert cell.bbox[0] < cell.bbox[2]
    assert max(abs(coords[index + 1][0] - coords[index][0]) for index in range(len(coords) - 1)) < 180.0


def test_mgrs_level_validation():
    engine = MGRSEngine()
    cell = engine.locate_point(lon=116.391, lat=39.907, requested_grid_level=5)
    assert cell.grid_type == "mgrs"

    with pytest.raises(ValidationError):
        engine.locate_point(lon=116.391, lat=39.907, requested_grid_level=6)

    with pytest.raises(ValidationError):
        engine.locate_point(lon=116.391, lat=39.907, requested_grid_level=-1)


def test_mgrs_cover_intersect_returns_cells():
    engine = MGRSEngine()
    geometry = {
        "type": "Polygon",
        "coordinates": [[[116.385, 39.903], [116.397, 39.903], [116.397, 39.911], [116.385, 39.911], [116.385, 39.903]]],
    }
    cells = engine.cover_geometry(geometry, requested_grid_level=3, cover_mode="intersect")
    target = box(116.385, 39.903, 116.397, 39.911)

    assert len(cells) > 0
    assert all(box(*cell.bbox).intersects(target) for cell in cells)


def test_mgrs_cover_mode_validation():
    engine = MGRSEngine()
    geometry = {"type": "Point", "coordinates": [116.391, 39.907]}
    with pytest.raises(ValidationError):
        engine.cover_geometry(geometry, requested_grid_level=3, cover_mode="invalid_mode")


def test_mgrs_cover_contain_is_subset_of_intersect():
    engine = MGRSEngine()
    geometry = {
        "type": "Polygon",
        "coordinates": [[[116.37, 39.89], [116.43, 39.89], [116.43, 39.93], [116.37, 39.93], [116.37, 39.89]]],
    }
    intersect_codes = {c.space_code for c in engine.cover_geometry(geometry, requested_grid_level=2, cover_mode="intersect")}
    contain_codes = {c.space_code for c in engine.cover_geometry(geometry, requested_grid_level=2, cover_mode="contain")}

    assert contain_codes.issubset(intersect_codes)
    assert len(contain_codes) <= len(intersect_codes)


def test_mgrs_cover_minimal_expanded_is_subset_of_intersect():
    engine = MGRSEngine()
    geometry = {
        "type": "Polygon",
        "coordinates": [[[116.37, 39.89], [116.43, 39.89], [116.43, 39.93], [116.37, 39.93], [116.37, 39.89]]],
    }
    intersect_codes = {c.space_code for c in engine.cover_geometry(geometry, requested_grid_level=2, cover_mode="intersect")}
    minimal_addresses = list(engine.cover_geometry(geometry, requested_grid_level=2, cover_mode="minimal"))
    expanded_minimal = _expand_mgrs_to_level(engine, minimal_addresses, target_level=2)

    assert expanded_minimal.issubset(intersect_codes)
    assert len(minimal_addresses) <= len(expanded_minimal)


def test_mgrs_neighbors_k1_non_empty():
    engine = MGRSEngine()
    address = engine.locate_space_code(lon=116.391, lat=39.907, requested_grid_level=5)
    neighbors = engine.neighbors(address, k=1)

    assert len(neighbors) > 0
    assert address.space_code not in {n.space_code for n in neighbors}
    assert all(n.grid_level == 5 for n in neighbors)


def test_mgrs_neighbors_k_validation():
    engine = MGRSEngine()
    address = engine.locate_space_code(lon=116.391, lat=39.907, requested_grid_level=5)
    with pytest.raises(ValidationError):
        engine.neighbors(address, k=0)


def test_mgrs_parent_and_children_roundtrip():
    engine = MGRSEngine()
    address = engine.locate_space_code(lon=116.391, lat=39.907, requested_grid_level=3)
    parent = engine.parent(address)
    children = engine.children(parent, target_grid_level=3)

    assert parent.space_code == address.space_code[:-2]
    assert len(children) == 100
    assert address.space_code in {c.space_code for c in children}


def test_mgrs_parent_validation():
    engine = MGRSEngine()
    address = engine.locate_space_code(lon=116.391, lat=39.907, requested_grid_level=0)
    with pytest.raises(ValidationError):
        engine.parent(address)


def test_mgrs_children_validation():
    engine = MGRSEngine()
    address = engine.locate_space_code(lon=116.391, lat=39.907, requested_grid_level=3)
    with pytest.raises(ValidationError):
        engine.children(address, target_grid_level=3)
    with pytest.raises(ValidationError):
        engine.children(address, target_grid_level=2)


def test_mgrs_cover_geometry_compact_matches_full_cover():
    engine = MGRSEngine()
    geometry = {
        "type": "Polygon",
        "coordinates": [[[116.385, 39.903], [116.397, 39.903], [116.397, 39.911], [116.385, 39.911], [116.385, 39.903]]],
    }

    full = engine.cover_geometry(geometry, requested_grid_level=3, cover_mode="intersect")
    compact = engine.cover_geometry_compact(geometry, requested_grid_level=3, cover_mode="intersect")

    assert {cell.space_code for cell in compact} == {cell.space_code for cell in full}
    assert {cell.space_code: cell.bbox for cell in compact} == {
        cell.space_code: cell.bbox for cell in full
    }
