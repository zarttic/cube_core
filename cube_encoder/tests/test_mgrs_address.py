"""Tests for standard MGRS UTM/UPS addressing and domain assignment.

Covers:
- UTM zones (standard, Norway exception, Svalbard exception)
- UPS polar zones (north, south)
- space_code canonical format (uppercase, no spaces, 2*level digits)
- legacy topology_code parsing compatibility
- Level 0-5 precision
- Domain assignment boundaries
- Antimeridian handling (lon=180 → utm-1)
"""
from __future__ import annotations

import pytest

from grid_core.app.engines.mgrs.address import (
    build_topology_code,
    canonicalize_mgrs,
    parent_space_code,
    parse_topology_code,
    precision_from_code,
)
from grid_core.app.engines.mgrs.domain import domain_for_point
from grid_core.app.engines.mgrs_engine import MGRSEngine

# ---------------------------------------------------------------------------
# Precision / level basics
# ---------------------------------------------------------------------------


def test_mgrs_precision_equals_grid_level() -> None:
    """Grid level must equal MGRS numeric precision (0-5)."""
    engine = MGRSEngine()
    address = engine.locate_space_code(2.2945, 48.8582, 5)
    assert address.grid_level == 5
    assert address.space_code == canonicalize_mgrs(address.space_code)
    # Precision 5 = 10 digit suffix
    # Only trailing digits count toward precision
    from grid_core.app.engines.mgrs.address import suffix_digit_count
    assert suffix_digit_count(address.space_code) == 10
    assert address.topology_code is None


def test_mgrs_level_three_canonical_vector() -> None:
    """Canonical level-3 identity vector: lon=2.2888, lat=49.2105 -> '31UDQ482511'."""
    engine = MGRSEngine()
    # Use the exact center of 31UDQ482511 (from mgrs.toLatLon)
    address = engine.locate_space_code(2.28878903, 49.21050080, 3)
    assert address.space_code == "31UDQ482511"
    # Exactly 6 trailing digits for precision 3
    assert address.space_code[-6:].isdigit()
    assert len(address.space_code[-6:]) == 6
    assert address.topology_code is None


def test_mgrs_level_zero_no_digits() -> None:
    """Precision 0 = 100 km cell has no numeric digits after the 100-km square ID."""
    engine = MGRSEngine()
    address = engine.locate_space_code(2.2945, 48.8582, 0)
    assert address.grid_level == 0
    assert address.space_code == "31UDQ"
    from grid_core.app.engines.mgrs.address import suffix_digit_count
    assert suffix_digit_count(address.space_code) == 0


@pytest.mark.parametrize("level", range(6))
def test_all_levels_have_correct_digit_count(level: int) -> None:
    """For every precision 0-5, space_code suffix must have exactly 2*level digits."""
    engine = MGRSEngine()
    address = engine.locate_space_code(116.391, 39.907, level)
    from grid_core.app.engines.mgrs.address import suffix_digit_count
    assert suffix_digit_count(address.space_code) == 2 * level
    assert address.grid_level == level


# ---------------------------------------------------------------------------
# Topology code format and parsing
# ---------------------------------------------------------------------------


def test_standard_mgrs_does_not_emit_topology_code() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(2.2945, 48.8582, 3)
    assert address.space_code == "31UDQ482119"
    assert address.topology_code is None


def test_standard_mgrs_levels_use_only_space_code() -> None:
    engine = MGRSEngine()
    for level in range(6):
        address = engine.locate_space_code(10.0, 52.0, level)
        assert address.space_code
        assert address.topology_code is None


def test_topology_code_build_and_parse_roundtrip() -> None:
    """build_topology_code / parse_topology_code must be inverses."""
    code = "31UDQ482511"
    topo = build_topology_code("utm-31n", 3, code)
    parsed = parse_topology_code(topo)
    assert parsed.domain_token == "utm-31n"
    assert parsed.level == 3
    assert parsed.space_code == code


def test_standard_ups_north_uses_only_space_code() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(0.0, 85.0, 0)
    assert address.space_code.startswith(("Y", "Z"))
    assert address.topology_code is None


def test_standard_ups_south_uses_only_space_code() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(0.0, -85.0, 0)
    assert address.space_code.startswith(("A", "B"))
    assert address.topology_code is None


# ---------------------------------------------------------------------------
# Domain assignment
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("lon", "lat", "expected_domain"),
    [
        (-180.0, 0.0, "utm-1n"),    # lon=180 → lon=-180 → zone 1
        (180.0, 0.0, "utm-1n"),     # lon=180 treated as -180
        (0.0, 84.0, "ups-n"),       # lat=84 → UPS north
        (0.0, -80.0, "ups-s"),      # lat=-80 → UPS south
        (0.0, 0.0, "utm-31n"),      # equator, Greenwich meridian
        (116.391, 39.907, "utm-50n"),  # Beijing
        (2.2945, 48.8582, "utm-31n"),  # Paris
    ],
)
def test_half_open_domain_assignment(lon: float, lat: float, expected_domain: str) -> None:
    assert domain_for_point(lon, lat).token == expected_domain


def test_norway_exception_zone_32() -> None:
    """Norway exception: 3°E–12°E at 56°N–64°N should be zone 32."""
    domain = domain_for_point(6.0, 60.0)
    assert domain.token == "utm-32n"


def test_svalbard_exception_zone_33() -> None:
    """Svalbard exception: 9°E–21°E at 72°N–84°N should be zone 33."""
    domain = domain_for_point(15.0, 75.0)
    assert domain.token == "utm-33n"


def test_utm_southern_hemisphere() -> None:
    """Points south of equator get 's' hemisphere."""
    domain = domain_for_point(151.0, -33.87)  # Sydney
    assert domain.hemisphere == "s"
    assert domain.token.endswith("s")


# ---------------------------------------------------------------------------
# Grid level validation
# ---------------------------------------------------------------------------


def test_level_validation_rejects_negative() -> None:
    from grid_core.app.core.exceptions import ValidationError
    engine = MGRSEngine()
    with pytest.raises(ValidationError):
        engine.locate_point(0.0, 0.0, -1)


def test_level_validation_rejects_above_five() -> None:
    from grid_core.app.core.exceptions import ValidationError
    engine = MGRSEngine()
    with pytest.raises(ValidationError):
        engine.locate_point(0.0, 0.0, 6)


def test_level_zero_is_valid() -> None:
    engine = MGRSEngine()
    cell = engine.locate_point(116.391, 39.907, 0)
    assert cell.grid_level == 0
    assert cell.grid_type == "mgrs"


# ---------------------------------------------------------------------------
# locate_point returns full GridCell
# ---------------------------------------------------------------------------


def test_locate_point_returns_gridcell_with_all_fields() -> None:
    engine = MGRSEngine()
    cell = engine.locate_point(116.391, 39.907, 3)
    assert cell.grid_type == "mgrs"
    assert cell.grid_level == 3
    assert cell.space_code
    assert cell.topology_code is None
    assert len(cell.center) == 2
    assert len(cell.bbox) == 4
    assert cell.bbox[0] < cell.bbox[2]
    assert cell.bbox[1] < cell.bbox[3]


def test_locate_point_beijing_level5() -> None:
    engine = MGRSEngine()
    cell = engine.locate_point(116.391, 39.907, 5)
    assert cell.space_code.startswith("50S")
    assert cell.grid_level == 5


# ---------------------------------------------------------------------------
# UPS polar locate
# ---------------------------------------------------------------------------


def test_ups_north_locate_level0() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(0.0, 88.0, 0)
    assert address.space_code.startswith(("Y", "Z"))
    assert address.topology_code is None
    assert address.grid_level == 0


def test_ups_south_locate_level0() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(0.0, -88.0, 0)
    assert address.space_code.startswith(("A", "B"))
    assert address.topology_code is None
    assert address.grid_level == 0


# ---------------------------------------------------------------------------
# Canonicalization
# ---------------------------------------------------------------------------


def test_canonicalize_strips_whitespace_and_uppercases() -> None:
    assert canonicalize_mgrs(" 31u dq 48 25 11 ") == "31UDQ482511"


def test_precision_from_code_correct_values() -> None:
    assert precision_from_code("31UDQ") == 0
    assert precision_from_code("31UDQ48") == 1
    assert precision_from_code("31UDQ4825") == 2
    assert precision_from_code("31UDQ482511") == 3
    assert precision_from_code("31UDQ48254811") == 4
    assert precision_from_code("31UDQ4825481134") == 5


def test_precision_from_code_invalid_odd_digits() -> None:
    from grid_core.app.core.exceptions import ValidationError
    # Construct a string with artificially odd trailing digits (1 digit suffix)
    with pytest.raises(ValidationError):
        precision_from_code("31UDQ4")  # 1 trailing digit → odd → ValidationError


# ---------------------------------------------------------------------------
# Cover geometry basic
# ---------------------------------------------------------------------------


def test_cover_intersect_returns_cells() -> None:
    engine = MGRSEngine()
    geometry = {
        "type": "Polygon",
        "coordinates": [[[116.385, 39.903], [116.397, 39.903], [116.397, 39.911],
                          [116.385, 39.911], [116.385, 39.903]]],
    }
    cells = engine.cover_geometry(geometry, 2, "intersect")
    assert len(cells) > 0
    for cell in cells:
        assert cell.topology_code is None
        assert cell.grid_level == 2


def test_cover_contain_is_subset_of_intersect() -> None:
    engine = MGRSEngine()
    geometry = {
        "type": "Polygon",
        "coordinates": [[[116.37, 39.89], [116.43, 39.89], [116.43, 39.93],
                          [116.37, 39.93], [116.37, 39.89]]],
    }
    intersect = {c.space_code for c in engine.cover_geometry(geometry, 2, "intersect")}
    contain = {c.space_code for c in engine.cover_geometry(geometry, 2, "contain")}
    assert contain.issubset(intersect)


def test_cover_invalid_mode_raises() -> None:
    from grid_core.app.core.exceptions import ValidationError
    engine = MGRSEngine()
    geometry = {"type": "Point", "coordinates": [0.0, 0.0]}
    with pytest.raises(ValidationError):
        engine.cover_geometry(geometry, 2, "bad_mode")


# ---------------------------------------------------------------------------
# Parent / children hierarchy
# ---------------------------------------------------------------------------


def test_parent_decreases_precision() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(116.391, 39.907, 3)
    parent = engine.parent(address)
    assert parent.grid_level == 2
    assert parent.space_code == "50SMK4717"


def test_children_increases_precision() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(116.391, 39.907, 2)
    children = engine.children(address, 3)
    assert len(children) == 100
    for child in children:
        assert child.grid_level == 3
        assert parent_space_code(child.space_code) == address.space_code


def test_parent_of_precision_zero_raises() -> None:
    from grid_core.app.core.exceptions import ValidationError
    engine = MGRSEngine()
    address = engine.locate_space_code(116.391, 39.907, 0)
    with pytest.raises(ValidationError):
        engine.parent(address)


# ---------------------------------------------------------------------------
# Neighbors
# ---------------------------------------------------------------------------


def test_neighbors_k1_non_empty() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(116.391, 39.907, 3)
    nbs = engine.neighbors(address, k=1)
    assert len(nbs) > 0
    for nb in nbs:
        assert nb.space_code != address.space_code
        assert nb.topology_code is None


def test_neighbor_space_codes_are_unique() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(116.391, 39.907, 3)
    nbs = engine.neighbors(address, k=1)
    space_codes = {nb.space_code for nb in nbs}
    assert len(space_codes) == len(nbs)
