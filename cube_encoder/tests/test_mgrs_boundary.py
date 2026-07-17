"""Tests for MGRS boundary conditions: antimeridian, UTM/UPS transitions, polar zones."""
from __future__ import annotations

from itertools import combinations

import pytest
from shapely.geometry import box, shape
from shapely.strtree import STRtree

from grid_core.app.engines.mgrs.domain import domain_for_point
from grid_core.app.engines.mgrs.geometry import _projected_ring_to_wgs84
from grid_core.app.engines.mgrs_engine import MGRSEngine

# ---------------------------------------------------------------------------
# Antimeridian
# ---------------------------------------------------------------------------


def test_projected_ring_transforms_all_vertices_in_one_batch() -> None:
    class RecordingTransformer:
        def __init__(self) -> None:
            self.calls = []

        def transform(self, xs, ys):
            self.calls.append((tuple(xs), tuple(ys)))
            return tuple(x + 0.5 for x in xs), tuple(y - 0.5 for y in ys)

    transformer = RecordingTransformer()
    result = _projected_ring_to_wgs84([(1.0, 2.0), (3.0, 4.0)], transformer)

    assert result == [(1.5, 1.5), (3.5, 3.5)]
    assert transformer.calls == [((1.0, 3.0), (2.0, 4.0))]


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
        assert cell.topology_code is None
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


def test_lat_84_locate_returns_standard_mgrs_code() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(0.0, 84.0, 0)
    assert address.space_code
    assert address.topology_code is None


def test_north_pole_locate() -> None:
    """The north pole (lat=90) must resolve to a valid UPS north cell."""
    engine = MGRSEngine()
    address = engine.locate_space_code(0.0, 90.0, 0)
    assert address.space_code.startswith(("Y", "Z"))
    assert address.topology_code is None
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


def test_lat_minus_80_locate_returns_standard_mgrs_code() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(0.0, -80.0, 0)
    assert address.space_code
    assert address.topology_code is None


def test_south_pole_locate() -> None:
    """The south pole (lat=-90) must resolve to a valid UPS south cell."""
    engine = MGRSEngine()
    address = engine.locate_space_code(0.0, -90.0, 0)
    assert address.space_code.startswith(("A", "B"))
    assert address.topology_code is None
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
# Cross-zone standard MGRS consistency
# ---------------------------------------------------------------------------


def test_cells_near_zone_boundary_use_standard_codes() -> None:
    engine = MGRSEngine()
    geometry = {
        "type": "Polygon",
        "coordinates": [[[5.9, 51.9], [6.1, 51.9], [6.1, 52.1], [5.9, 52.1], [5.9, 51.9]]],
    }
    cells = engine.cover_geometry(geometry, 1, "intersect")
    assert len(cells) > 0
    for cell in cells:
        assert cell.topology_code is None
        assert cell.grid_level == 1


@pytest.mark.parametrize(
    "coordinates",
    [
        [[[5.9, 51.9], [6.1, 51.9], [6.1, 52.1], [5.9, 52.1], [5.9, 51.9]]],
        [[[5.9, 55.9], [6.1, 55.9], [6.1, 56.1], [5.9, 56.1], [5.9, 55.9]]],
    ],
)
def test_cross_domain_cover_has_unique_non_overlapping_cells(coordinates: list) -> None:
    engine = MGRSEngine()
    geometry = {
        "type": "Polygon",
        "coordinates": coordinates,
    }
    cells = engine.cover_geometry(geometry, 1, "intersect")
    assert len({cell.space_code for cell in cells}) == len(cells)
    geometries = [shape(cell.geometry) for cell in cells]
    for left, right in combinations(geometries, 2):
        assert left.intersection(right).area <= 1e-12


def test_shandong_level1_cover_has_no_extended_or_overlapping_cells() -> None:
    """Regression for the two-scene Shandong 10 km preview across zones 50/51."""
    engine = MGRSEngine()
    aoi = box(
        114.75737732592705,
        33.85704125649375,
        122.77491413824946,
        38.50352140009955,
    )
    cells = engine.cover_geometry(aoi.__geo_interface__, 1, "intersect")

    assert len(cells) == 3919
    assert len({cell.space_code for cell in cells}) == len(cells)
    assert all(cell.topology_code is None for cell in cells)

    geometries = [shape(cell.geometry) for cell in cells]
    tree = STRtree(geometries)
    for index, geometry in enumerate(geometries):
        for candidate in tree.query(geometry):
            candidate_index = int(candidate)
            if candidate_index > index:
                assert geometry.intersection(geometries[candidate_index]).area <= 1e-12


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
        assert cell.topology_code is None


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
        assert cell.topology_code is None
