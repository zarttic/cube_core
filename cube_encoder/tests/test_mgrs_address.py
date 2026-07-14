"""Tests for MGRS UTM/UPS addressing, domain assignment, and topology codes.

Covers:
- UTM zones (standard, Norway exception, Svalbard exception)
- UPS polar zones (north, south)
- space_code canonical format (uppercase, no spaces, 2*level digits)
- topology_code format and round-trip parsing
- Level 0-5 precision
- Domain assignment boundaries
- Antimeridian handling (lon=180 → utm-1)
"""
from __future__ import annotations

import pytest

from grid_core.app.engines.mgrs.address import (
    build_topology_code,
    canonicalize_mgrs,
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
    assert parse_topology_code(address.topology_code).level == 5


def test_mgrs_level_three_canonical_vector() -> None:
    """Canonical level-3 M2 identity vector: lon=2.2888, lat=49.2105 → '31UDQ482511'."""
    engine = MGRSEngine()
    # Use the exact center of 31UDQ482511 (from mgrs.toLatLon)
    address = engine.locate_space_code(2.28878903, 49.21050080, 3)
    assert address.space_code == "31UDQ482511"
    # Exactly 6 trailing digits for precision 3
    assert address.space_code[-6:].isdigit()
    assert len(address.space_code[-6:]) == 6
    assert address.topology_code == "mgrs-topo-v1:utm-31n:3:31UDQ482511"


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


def test_topology_code_format_utm() -> None:
    """topology_code must be 'mgrs-topo-v1:utm-<zone><ns>:<level>:<space_code>'."""
    engine = MGRSEngine()
    address = engine.locate_space_code(2.2945, 48.8582, 3)
    assert address.topology_code is not None
    parsed = parse_topology_code(address.topology_code)
    assert parsed.domain_token == "utm-31n"
    assert parsed.level == 3
    assert parsed.space_code == address.space_code


def test_topology_code_repeats_exact_space_code() -> None:
    """topology_code must contain the exact canonical space_code at the end."""
    engine = MGRSEngine()
    for level in range(6):
        address = engine.locate_space_code(10.0, 52.0, level)
        parsed = parse_topology_code(address.topology_code)
        assert parsed.space_code == address.space_code


def test_topology_code_build_and_parse_roundtrip() -> None:
    """build_topology_code / parse_topology_code must be inverses."""
    code = "31UDQ482511"
    topo = build_topology_code("utm-31n", 3, code)
    parsed = parse_topology_code(topo)
    assert parsed.domain_token == "utm-31n"
    assert parsed.level == 3
    assert parsed.space_code == code


def test_topology_code_ups_north() -> None:
    """UPS north cells must have 'ups-n' in topology_code."""
    engine = MGRSEngine()
    address = engine.locate_space_code(0.0, 85.0, 0)
    assert address.topology_code is not None
    assert "ups-n" in address.topology_code


def test_topology_code_ups_south() -> None:
    """UPS south cells must have 'ups-s' in topology_code."""
    engine = MGRSEngine()
    address = engine.locate_space_code(0.0, -85.0, 0)
    assert address.topology_code is not None
    assert "ups-s" in address.topology_code


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
    assert cell.topology_code
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
    assert "ups-n" in address.topology_code
    assert address.grid_level == 0


def test_ups_south_locate_level0() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(0.0, -88.0, 0)
    assert "ups-s" in address.topology_code
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
        assert cell.topology_code is not None
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
    assert parent.space_code == address.space_code[:-2]


def test_children_increases_precision() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(116.391, 39.907, 2)
    children = engine.children(address, 3)
    assert len(children) == 100
    for child in children:
        assert child.grid_level == 3
        assert child.space_code.startswith(address.space_code)


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
        assert nb.topology_code is not None
        assert nb.topology_code != address.topology_code


def test_neighbors_topology_codes_are_unique() -> None:
    engine = MGRSEngine()
    address = engine.locate_space_code(116.391, 39.907, 3)
    nbs = engine.neighbors(address, k=1)
    topo_set = {nb.topology_code for nb in nbs}
    assert len(topo_set) == len(nbs)
