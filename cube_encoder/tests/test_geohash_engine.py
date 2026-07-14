"""Tests for GeohashEngine — TDD step 1 (these fail before the engine exists)."""
from __future__ import annotations

import pytest

from grid_core.app.engines.geohash_engine import GeohashEngine
from grid_core.app.models.grid_address import GridAddress

# ---------------------------------------------------------------------------
# Known-vector tests from the plan
# ---------------------------------------------------------------------------


def test_geohash_known_vector_and_boundary_normalization() -> None:
    engine = GeohashEngine()
    assert engine.locate_space_code(-5.6, 42.6, 5).space_code == "ezs42"
    assert (
        engine.locate_space_code(180.0, 0.0, 3).space_code
        == engine.locate_space_code(-180.0, 0.0, 3).space_code
    )


def test_geohash_parent_and_children_are_standard_base32() -> None:
    engine = GeohashEngine()
    address = engine.locate_space_code(-5.6, 42.6, 5)
    parent = engine.parent(address)
    children = engine.children(parent, target_grid_level=5)
    assert parent.space_code == "ezs4"
    assert len(children) == 32
    assert address in children
    assert all(child.topology_code is None and child.grid_level == 5 for child in children)


# ---------------------------------------------------------------------------
# GridAddress fields
# ---------------------------------------------------------------------------


def test_locate_returns_grid_address_fields() -> None:
    engine = GeohashEngine()
    addr = engine.locate_space_code(116.39, 39.91, 6)
    assert addr.grid_type == "geohash"
    assert addr.grid_level == 6
    assert len(addr.space_code) == 6
    assert addr.topology_code is None


# ---------------------------------------------------------------------------
# Precision 1 and 12 round-trip
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("precision", [1, 6, 12])
def test_roundtrip_at_level(precision: int) -> None:
    engine = GeohashEngine()
    addr = engine.locate_space_code(10.0, 20.0, precision)
    assert len(addr.space_code) == precision
    bbox = engine.code_to_bbox(addr)
    lon_min, lat_min, lon_max, lat_max = bbox
    center_lon = (lon_min + lon_max) / 2
    center_lat = (lat_min + lat_max) / 2
    addr2 = engine.locate_space_code(center_lon, center_lat, precision)
    assert addr2.space_code == addr.space_code


# ---------------------------------------------------------------------------
# Antimeridian
# ---------------------------------------------------------------------------


def test_antimeridian_lon_180_normalizes_to_minus_180() -> None:
    engine = GeohashEngine()
    a1 = engine.locate_space_code(180.0, 0.0, 6)
    a2 = engine.locate_space_code(-180.0, 0.0, 6)
    assert a1.space_code == a2.space_code


# ---------------------------------------------------------------------------
# Poles
# ---------------------------------------------------------------------------


def test_north_pole_does_not_raise() -> None:
    engine = GeohashEngine()
    addr = engine.locate_space_code(0.0, 90.0, 5)
    assert addr.space_code  # just ensure it encodes


def test_south_pole_does_not_raise() -> None:
    engine = GeohashEngine()
    addr = engine.locate_space_code(0.0, -90.0, 5)
    assert addr.space_code


# ---------------------------------------------------------------------------
# locate_point returns GridCell
# ---------------------------------------------------------------------------


def test_locate_point_returns_grid_cell() -> None:
    from grid_core.app.models.grid_cell import GridCell

    engine = GeohashEngine()
    cell = engine.locate_point(116.39, 39.91, 6)
    assert isinstance(cell, GridCell)
    assert cell.grid_type == "geohash"
    assert cell.grid_level == 6
    assert len(cell.center) == 2
    assert len(cell.bbox) == 4


# ---------------------------------------------------------------------------
# code_to_geometry
# ---------------------------------------------------------------------------


def test_code_to_geometry_returns_polygon() -> None:
    engine = GeohashEngine()
    addr = engine.locate_space_code(10.0, 20.0, 5)
    geom = engine.code_to_geometry(addr)
    assert geom["type"] == "Polygon"
    coords = geom["coordinates"][0]
    assert len(coords) == 5  # closed ring


# ---------------------------------------------------------------------------
# code_to_bbox
# ---------------------------------------------------------------------------


def test_code_to_bbox_matches_geometry() -> None:
    engine = GeohashEngine()
    addr = engine.locate_space_code(10.0, 20.0, 5)
    bbox = engine.code_to_bbox(addr)
    assert len(bbox) == 4
    lon_min, lat_min, lon_max, lat_max = bbox
    assert lon_min < lon_max
    assert lat_min < lat_max


# ---------------------------------------------------------------------------
# Parent/children consistency
# ---------------------------------------------------------------------------


def test_parent_reduces_precision_by_one() -> None:
    engine = GeohashEngine()
    addr = engine.locate_space_code(10.0, 20.0, 6)
    parent = engine.parent(addr)
    assert parent.grid_level == 5
    assert addr.space_code.startswith(parent.space_code)


def test_children_count_is_32() -> None:
    engine = GeohashEngine()
    addr = engine.locate_space_code(10.0, 20.0, 4)
    children = engine.children(addr, target_grid_level=5)
    assert len(children) == 32
    assert all(c.grid_level == 5 for c in children)
    assert all(c.space_code.startswith(addr.space_code) for c in children)


def test_multi_level_children() -> None:
    engine = GeohashEngine()
    addr = engine.locate_space_code(10.0, 20.0, 3)
    children = engine.children(addr, target_grid_level=5)
    # 32^2 = 1024
    assert len(children) == 1024
    assert all(c.grid_level == 5 for c in children)


# ---------------------------------------------------------------------------
# Neighbors
# ---------------------------------------------------------------------------


def test_neighbors_count_is_8() -> None:
    engine = GeohashEngine()
    addr = engine.locate_space_code(10.0, 20.0, 5)
    nbrs = engine.neighbors(addr, k=1)
    assert len(nbrs) == 8
    assert all(n.grid_level == 5 for n in nbrs)
    assert all(n.topology_code is None for n in nbrs)


def test_neighbors_do_not_include_self() -> None:
    engine = GeohashEngine()
    addr = engine.locate_space_code(10.0, 20.0, 5)
    nbrs = engine.neighbors(addr, k=1)
    assert addr not in nbrs


def test_neighbor_symmetry() -> None:
    """If B is a neighbor of A, then A is a neighbor of B."""
    engine = GeohashEngine()
    addr = engine.locate_space_code(10.0, 20.0, 5)
    nbrs = engine.neighbors(addr, k=1)
    for nbr in nbrs:
        nbr_of_nbr = engine.neighbors(nbr, k=1)
        nbr_codes = {n.space_code for n in nbr_of_nbr}
        assert addr.space_code in nbr_codes


# ---------------------------------------------------------------------------
# Cover: intersect
# ---------------------------------------------------------------------------


def _small_bbox_geom() -> dict:
    return {
        "type": "Polygon",
        "coordinates": [[
            [10.0, 20.0],
            [10.5, 20.0],
            [10.5, 20.5],
            [10.0, 20.5],
            [10.0, 20.0],
        ]],
    }


def test_cover_intersect_returns_grid_cells() -> None:
    from grid_core.app.models.grid_cell import GridCell

    engine = GeohashEngine()
    cells = engine.cover_geometry(_small_bbox_geom(), 5, "intersect")
    assert cells
    assert all(isinstance(c, GridCell) for c in cells)
    assert all(c.grid_type == "geohash" for c in cells)
    assert all(c.grid_level == 5 for c in cells)


def test_cover_contain_subset_of_intersect() -> None:
    engine = GeohashEngine()
    intersect = engine.cover_geometry(_small_bbox_geom(), 5, "intersect")
    contain = engine.cover_geometry(_small_bbox_geom(), 5, "contain")
    intersect_codes = {c.space_code for c in intersect}
    contain_codes = {c.space_code for c in contain}
    assert contain_codes.issubset(intersect_codes)


def test_cover_minimal_returns_compact_cells() -> None:
    from grid_core.app.models.compact_grid_cell import CompactGridCell

    engine = GeohashEngine()
    cells = engine.cover_geometry_compact(_small_bbox_geom(), 5, "minimal")
    assert cells
    assert all(isinstance(c, CompactGridCell) for c in cells)


def test_cover_minimal_has_mixed_levels_for_large_parent() -> None:
    """For a geometry that fully covers a level-4 cell, minimal should return
    that cell at level 4 (coarser than the requested level 5)."""
    engine = GeohashEngine()
    # Use the exact bbox of a level-4 cell as the AOI — the AOI covers the
    # parent cell exactly, so all 32 children are selected and then compacted.
    addr4 = engine.locate_space_code(10.0, 20.0, 4)
    bbox = engine.code_to_bbox(addr4)
    lon_min, lat_min, lon_max, lat_max = bbox
    # Expand bbox slightly so the parent cell is fully covered by the AOI
    geom = {
        "type": "Polygon",
        "coordinates": [[
            [lon_min - 1e-9, lat_min - 1e-9],
            [lon_max + 1e-9, lat_min - 1e-9],
            [lon_max + 1e-9, lat_max + 1e-9],
            [lon_min - 1e-9, lat_max + 1e-9],
            [lon_min - 1e-9, lat_min - 1e-9],
        ]],
    }
    cells = engine.cover_geometry_compact(geom, 5, "minimal")
    # Should contain at least one cell at level 4 (parent replaces all 32 children)
    levels = {c.grid_level for c in cells}
    assert 4 in levels


def test_cover_boundary_contact_excluded_from_intersect() -> None:
    """A cell that only touches the AOI boundary should not be in the intersect cover."""
    engine = GeohashEngine()
    # Use exact boundary of a level-5 cell
    addr = engine.locate_space_code(10.0, 20.0, 5)
    bbox = engine.code_to_bbox(addr)
    lon_min, lat_min, lon_max, lat_max = bbox
    # AOI that only touches addr at its eastern edge
    geom = {
        "type": "Polygon",
        "coordinates": [[
            [lon_max, lat_min],
            [lon_max + 0.1, lat_min],
            [lon_max + 0.1, lat_max],
            [lon_max, lat_max],
            [lon_max, lat_min],
        ]],
    }
    cells = engine.cover_geometry(geom, 5, "intersect")
    cell_codes = {c.space_code for c in cells}
    # The touching cell should not be included (boundary-only contact)
    assert addr.space_code not in cell_codes


# ---------------------------------------------------------------------------
# Antimeridian polygon cover
# ---------------------------------------------------------------------------


def test_cover_antimeridian_polygon() -> None:
    """Cover should handle polygons that cross the antimeridian."""
    engine = GeohashEngine()
    geom = {
        "type": "Polygon",
        "coordinates": [[
            [179.5, 10.0],
            [180.0, 10.0],
            [180.0, 10.5],
            [179.5, 10.5],
            [179.5, 10.0],
        ]],
    }
    cells = engine.cover_geometry(geom, 4, "intersect")
    assert cells  # should return cells near antimeridian


# ---------------------------------------------------------------------------
# Uppercase/invalid Base32 rejection
# ---------------------------------------------------------------------------


def test_invalid_uppercase_in_space_code_raises() -> None:
    engine = GeohashEngine()
    with pytest.raises(Exception):
        engine.code_to_geometry(GridAddress(grid_type="geohash", grid_level=5, space_code="EZZZZ"))


def test_invalid_base32_char_raises() -> None:
    engine = GeohashEngine()
    with pytest.raises(Exception):
        engine.code_to_geometry(GridAddress(grid_type="geohash", grid_level=5, space_code="aaaal"))  # 'l' is invalid
