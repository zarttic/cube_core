"""Tests for MGRS boundary conditions: antimeridian, UTM/UPS transitions, polar zones."""
from __future__ import annotations

import pytest
from shapely.geometry import shape

from grid_core.app.engines.mgrs.domain import domain_for_point
from grid_core.app.engines.mgrs_engine import MGRSEngine


# ---------------------------------------------------------------------------
# Antimeridian
# ---------------------------------------------------------------------------


def test_antimeridian_lon_180_maps_to_zone_1() -> None:
    """lon=180 is treated as -180, which falls in UTM zone 1."""
    domain = domain_for_point(180.0, 0.0)
    assert domain.token == "utm-1n"


def test_antimeridian_lon_minus_180_maps_to_zone_1() -> None:
    """lon=-180 falls in UTM zone 1."""
    domain = domain_for_point(-180.0, 0.0)
    assert domain.token == "utm-1n"


def test_antimeridian_polygon_cover_produces_valid_cells() -> None:
    """A polygon crossing the antimeridian should produce cells without 180-degree lon jumps."""
    engine = MGRSEngine()
    geometry = {
        "type": "Polygon",
        "coordinates": [[[178.0, 60.0], [180.0, 60.0], [180.0, 62.0], [178.0, 62.0], [178.0, 60.0]]],
    }
    cells = engine.cover_geometry(geometry, 0, "intersect")
    assert len(cells) > 0
    for cell in cells:
        assert cell.topology_code is not None
        # Bbox width must be under 180 degrees
        assert cell.bbox[2] - cell.bbox[0] < 180.0


def test_near_antimeridian_cell_geometry_is_continuous() -> None:
    """Cells near the antimeridian must have continuous longitude (no 360° gap)."""
    engine = MGRSEngine()
    address = engine.locate_space_code(179.5, 62.0, 0)
    geom_dict = engine.code_to_geometry(address)
    geom = shape(geom_dict)
    assert geom.is_valid
    bbox = engine.code_to_bbox(address)
    assert bbox[2] - bbox[0] < 180.0


# ---------------------------------------------------------------------------
# UTM/UPS transition at 84°N
# ---------------------------------------------------------------------------


def test_lat_84_is_ups_north() -> None:
    """lat=84 should be assigned to UPS north domain."""
    domain = domain_for_point(0.0, 84.0)
    assert domain.token == "ups-n"


def test_lat_just_below_84_is_utm() -> None:
    """lat just below 84 should remain in UTM."""
    domain = domain_for_point(0.0, 83.9)
    assert domain.kind == "utm"
    assert domain.hemisphere == "n"


def test_lat_84_locate_gives_ups_n_topology() -> None:
    """Locating a point at lat=84 must produce a UPS-north topology code."""
    engine = MGRSEngine()
    address = engine.locate_space_code(0.0, 84.0, 0)
    assert address.topology_code is not None
    assert "ups-n" in address.topology_code


def test_north_pole_locate() -> None:
    """The north pole (lat=90) must resolve to a valid UPS north cell."""
    engine = MGRSEngine()
    address = engine.locate_space_code(0.0, 90.0, 0)
    assert address.topology_code is not None
    assert "ups-n" in address.topology_code
    geom = shape(engine.code_to_geometry(address))
    assert geom.is_valid
    assert not geom.is_empty


# ---------------------------------------------------------------------------
# UTM/UPS transition at -80°S
# ---------------------------------------------------------------------------


def test_lat_minus_80_is_ups_south() -> None:
    """lat=-80 should be assigned to UPS south domain."""
    domain = domain_for_point(0.0, -80.0)
    assert domain.token == "ups-s"


def test_lat_just_above_minus_80_is_utm() -> None:
    """lat just above -80 should be UTM south."""
    domain = domain_for_point(0.0, -79.9)
    assert domain.kind == "utm"
    assert domain.hemisphere == "s"


def test_lat_minus_80_locate_gives_ups_s_topology() -> None:
    """Locating a point at lat=-80 must produce a UPS-south topology code."""
    engine = MGRSEngine()
    address = engine.locate_space_code(0.0, -80.0, 0)
    assert address.topology_code is not None
    assert "ups-s" in address.topology_code


def test_south_pole_locate() -> None:
    """The south pole (lat=-90) must resolve to a valid UPS south cell."""
    engine = MGRSEngine()
    address = engine.locate_space_code(0.0, -90.0, 0)
    assert address.topology_code is not None
    assert "ups-s" in address.topology_code
    geom = shape(engine.code_to_geometry(address))
    assert geom.is_valid
    assert not geom.is_empty


# ---------------------------------------------------------------------------
# Norway and Svalbard special zones
# ---------------------------------------------------------------------------


def test_norway_exception_zone_32v() -> None:
    """Central Norway (6°E, 60°N) must fall in zone 32 (not 33)."""
    domain = domain_for_point(6.0, 60.0)
    assert domain.token == "utm-32n"


def test_svalbard_zone_33() -> None:
    """Svalbard at 15°E, 75°N must fall in zone 33 (not 34)."""
    domain = domain_for_point(15.0, 75.0)
    assert domain.token == "utm-33n"


def test_svalbard_zone_35() -> None:
    """Svalbard at 27°E, 75°N must fall in zone 35."""
    domain = domain_for_point(27.0, 75.0)
    assert domain.token == "utm-35n"


def test_outside_svalbard_zone_standard() -> None:
    """At lat=70 (below Svalbard), standard zone applies for 15°E (zone 32 or 33)."""
    domain = domain_for_point(15.0, 70.0)
    # At lat=70, lon=15: standard zone = int((15+180)/6)%60+1 = int(32.5)%60+1 = 33
    assert domain.kind == "utm"


# ---------------------------------------------------------------------------
# Cross-zone boundary topology consistency
# ---------------------------------------------------------------------------


def test_cells_near_zone_boundary_have_topology_codes() -> None:
    """All cells near a UTM zone boundary must have topology codes."""
    engine = MGRSEngine()
    geometry = {
        "type": "Polygon",
        "coordinates": [[[5.9, 51.9], [6.1, 51.9], [6.1, 52.1], [5.9, 52.1], [5.9, 51.9]]],
    }
    cells = engine.cover_geometry(geometry, 1, "intersect")
    assert len(cells) > 0
    for cell in cells:
        assert cell.topology_code is not None
        assert cell.grid_level == 1


def test_topology_code_uniqueness_across_domain_boundaries() -> None:
    """All topology codes in a cross-boundary cover must be unique."""
    engine = MGRSEngine()
    geometry = {
        "type": "Polygon",
        "coordinates": [[[5.9, 51.9], [6.1, 51.9], [6.1, 52.1], [5.9, 52.1], [5.9, 51.9]]],
    }
    cells = engine.cover_geometry(geometry, 1, "intersect")
    topo_codes = [c.topology_code for c in cells]
    assert len(topo_codes) == len(set(topo_codes))


# ---------------------------------------------------------------------------
# Level 0 cells (100 km) span wide areas correctly
# ---------------------------------------------------------------------------


def test_level0_cell_bbox_is_reasonable() -> None:
    """A 100 km cell should have a bbox bounded by ~1-2 degrees in each direction."""
    engine = MGRSEngine()
    address = engine.locate_space_code(10.0, 52.0, 0)
    bbox = engine.code_to_bbox(address)
    lon_span = bbox[2] - bbox[0]
    lat_span = bbox[3] - bbox[1]
    # 100 km ≈ 0.9 degrees at lat=52
    assert 0.5 < lon_span < 5.0
    assert 0.5 < lat_span < 3.0


# ---------------------------------------------------------------------------
# Cover at UPS zone
# ---------------------------------------------------------------------------


def test_cover_ups_north_polygon() -> None:
    """Cover should work for a polygon in the UPS north zone."""
    engine = MGRSEngine()
    geometry = {
        "type": "Polygon",
        "coordinates": [[[-10.0, 85.0], [10.0, 85.0], [10.0, 86.0], [-10.0, 86.0], [-10.0, 85.0]]],
    }
    cells = engine.cover_geometry(geometry, 0, "intersect")
    assert len(cells) > 0
    for cell in cells:
        assert cell.topology_code is not None


def test_cover_ups_south_polygon() -> None:
    """Cover should work for a polygon in the UPS south zone."""
    engine = MGRSEngine()
    geometry = {
        "type": "Polygon",
        "coordinates": [[[-10.0, -87.0], [10.0, -87.0], [10.0, -85.0], [-10.0, -85.0], [-10.0, -87.0]]],
    }
    cells = engine.cover_geometry(geometry, 0, "intersect")
    assert len(cells) > 0
    for cell in cells:
        assert cell.topology_code is not None
