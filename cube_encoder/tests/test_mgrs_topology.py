"""Tests for MGRS cross-domain topology: clipped geometry, neighbors, domain intersection."""
from __future__ import annotations

import pytest
from shapely.geometry import shape

from grid_core.app.engines.mgrs.address import parent_space_code
from grid_core.app.engines.mgrs_engine import MGRSEngine

# ---------------------------------------------------------------------------
# Clipped geometry
# ---------------------------------------------------------------------------


def test_utm_cell_geometry_is_valid_and_non_empty() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(10.0, 50.0, 2)
    geom_dict = engine.code_to_geometry(address)
    geom = shape(geom_dict)
    assert geom.is_valid
    assert not geom.is_empty
    assert geom.area > 0.0


def test_utm_ups_boundary_cell_clipped_to_valid_domain() -> None:
    """A cell near the UTM/UPS boundary must be clipped to its valid domain."""
    engine = MGRSEngine()
    address = engine.locate_space_code(0.0, 83.0, 0)
    geom_dict = engine.code_to_geometry(address)
    geom = shape(geom_dict)
    domain_geom = shape(engine.domain_geometry(address))
    assert geom.is_valid
    assert not geom.is_empty
    # Clipped cell must lie within the valid domain
    assert domain_geom.covers(geom) or geom.intersection(domain_geom).equals(geom)


def test_ups_north_cell_geometry_is_valid() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(0.0, 88.0, 0)
    geom = shape(engine.code_to_geometry(address))
    assert geom.is_valid
    assert not geom.is_empty


def test_ups_south_cell_geometry_is_valid() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(0.0, -88.0, 0)
    geom = shape(engine.code_to_geometry(address))
    assert geom.is_valid
    assert not geom.is_empty


def test_cell_geometry_is_polygon_type() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(116.391, 39.907, 2)
    geom_dict = engine.code_to_geometry(address)
    assert geom_dict["type"] in ("Polygon", "MultiPolygon")


def test_bbox_dimensions_are_positive() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(116.391, 39.907, 3)
    bbox = engine.code_to_bbox(address)
    assert len(bbox) == 4
    assert bbox[0] < bbox[2]
    assert bbox[1] < bbox[3]


def test_center_is_inside_bbox() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(10.0, 52.0, 2)
    bbox = engine.code_to_bbox(address)
    center = engine.code_to_center(address)
    assert bbox[0] <= center[0] <= bbox[2]
    assert bbox[1] <= center[1] <= bbox[3]


# ---------------------------------------------------------------------------
# Cross-domain neighbors
# ---------------------------------------------------------------------------


def test_cross_zone_neighbors_use_standard_space_code_identity() -> None:
    engine = MGRSEngine()
    # Near a zone boundary (lon≈6 is between zone 32 and standard zone for that lat)
    address = engine.locate_space_code(5.999999, 0.0, 2)
    neighbors = engine.neighbors(address, k=1)
    assert len(neighbors) > 0
    for nb in neighbors:
        assert nb.space_code
        assert nb.topology_code is None
        assert nb.space_code != address.space_code


def test_neighbor_space_codes_are_unique() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(10.0, 52.0, 2)
    neighbors = engine.neighbors(address, k=1)
    space_codes = [nb.space_code for nb in neighbors]
    assert len(space_codes) == len(set(space_codes))


def test_neighbors_do_not_include_source() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(116.391, 39.907, 3)
    neighbors = engine.neighbors(address, k=1)
    assert address.space_code not in {nb.space_code for nb in neighbors}


def test_neighbors_k_invalid_raises() -> None:
    from grid_core.app.core.exceptions import ValidationError
    engine = MGRSEngine()
    address = engine.locate_space_code(10.0, 52.0, 2)
    with pytest.raises(ValidationError):
        engine.neighbors(address, k=0)


def test_neighbor_count_is_not_necessarily_eight() -> None:
    """MGRS neighbors are geometry-based; irregular counts are valid."""
    engine = MGRSEngine()
    # A regular mid-zone cell should have neighbors
    address = engine.locate_space_code(10.0, 52.0, 2)
    neighbors = engine.neighbors(address, k=1)
    assert len(neighbors) > 0  # Must have some, but not necessarily 8


# ---------------------------------------------------------------------------
# Parent / children with standard identities
# ---------------------------------------------------------------------------


def test_parent_returns_standard_mgrs_address() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(10.0, 52.0, 3)
    parent = engine.parent(address)
    assert parent.grid_type == "mgrs"
    assert parent.grid_level == 2
    assert parent.space_code == "32UNC6861"
    assert parent.topology_code is None


def test_children_return_standard_mgrs_addresses() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(10.0, 52.0, 2)
    children = engine.children(address, 3)
    assert len(children) == 100
    for child in children:
        assert child.grid_type == "mgrs"
        assert child.grid_level == 3
        assert parent_space_code(child.space_code) == address.space_code
        assert child.topology_code is None


def test_children_at_latitude_band_boundary_all_have_valid_geometry() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(10.0, 47.99, 0)

    children = engine.children(address, 1)

    assert address.space_code == "32TNU"
    assert 0 < len(children) < 100
    assert all(child.topology_code is None for child in children)
    assert all(shape(engine.code_to_geometry(child)).area > 0 for child in children)


def test_children_target_level_must_be_greater() -> None:
    from grid_core.app.core.exceptions import ValidationError
    engine = MGRSEngine()
    address = engine.locate_space_code(10.0, 52.0, 3)
    with pytest.raises(ValidationError):
        engine.children(address, 3)  # same level
    with pytest.raises(ValidationError):
        engine.children(address, 2)  # lower level


# ---------------------------------------------------------------------------
# Minimal coarsening does not merge across domains
# ---------------------------------------------------------------------------


def test_minimal_coarsening_preserves_domain_boundary() -> None:
    """Minimal cover keeps standard MGRS identities unique."""
    engine = MGRSEngine()
    # Small geometry that stays within one UTM zone
    geometry = {
        "type": "Polygon",
        "coordinates": [[[10.0, 52.0], [10.1, 52.0], [10.1, 52.1], [10.0, 52.1], [10.0, 52.0]]],
    }
    cells = engine.cover_geometry(geometry, 0, "minimal")
    assert len({cell.space_code for cell in cells}) == len(cells)
    assert all(cell.topology_code is None for cell in cells)


def test_minimal_coarsens_complete_valid_child_set_at_band_boundary() -> None:
    engine = MGRSEngine()
    parent = engine.locate_space_code(10.0, 47.99, 0)
    parent_geometry = engine.code_to_geometry(parent)

    cells = engine.cover_geometry(parent_geometry, 1, "minimal")

    assert any(cell.grid_level == 0 and cell.space_code == parent.space_code for cell in cells)


# ---------------------------------------------------------------------------
# Cover boundary-only contact excluded
# ---------------------------------------------------------------------------


def test_cover_intersect_excludes_boundary_only_contact() -> None:
    """Cells that only touch the AOI boundary (area=0) must not be selected.

    A tiny polygon fully inside one cell selects exactly that cell.
    A point has area=0 and the area-intersection rule produces no results;
    this test uses a proper tiny polygon to verify single-cell containment.
    """
    engine = MGRSEngine()
    # Precision-2 MGRS cells are ~1 km; use a tiny 10 m polygon well inside one cell
    geometry = {
        "type": "Polygon",
        "coordinates": [[[10.0001, 52.0001], [10.0002, 52.0001],
                          [10.0002, 52.0002], [10.0001, 52.0002],
                          [10.0001, 52.0001]]],
    }
    cells = engine.cover_geometry(geometry, 3, "intersect")
    # A tiny polygon must select the single containing cell (and only that cell)
    assert len(cells) == 1
