"""Integration tests for CubeEncoderSDK across the three frozen grid types.

These exercise the SDK facade end-to-end (locate/cover/neighbors/parent/
children/code_to_geometry/generate_st_code) rather than individual engines,
to guard against drift between the SDK client and the underlying services.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from grid_core.app.core.enums import BoundaryType, CoverMode, GridType, TimeGranularity
from grid_core.app.models.grid_address import GridAddress
from grid_core.app.models.grid_cell import GridCell
from grid_core.sdk.client import CubeEncoderSDK

POINT = [116.391, 39.907]

# A representative requested_grid_level per grid type, within LEVEL_RANGES.
LEVELS = {
    GridType.GEOHASH: 5,
    GridType.MGRS: 3,
    GridType.ISEA4H: 3,
}

BBOX_GEOMETRY = {
    "type": "Polygon",
    "coordinates": [[
        [116.385, 39.903],
        [116.397, 39.903],
        [116.397, 39.911],
        [116.385, 39.911],
        [116.385, 39.903],
    ]],
}


@pytest.fixture(scope="module")
def sdk() -> CubeEncoderSDK:
    return CubeEncoderSDK()


@pytest.mark.parametrize("grid_type", [GridType.GEOHASH, GridType.MGRS, GridType.ISEA4H])
def test_locate_returns_grid_cell(sdk: CubeEncoderSDK, grid_type: GridType) -> None:
    level = LEVELS[grid_type]
    cell = sdk.locate(grid_type=grid_type, requested_grid_level=level, point=POINT)
    assert isinstance(cell, GridCell)
    assert cell.grid_type == grid_type.value
    assert cell.grid_level == level
    assert len(cell.center) == 2
    assert len(cell.bbox) == 4


@pytest.mark.parametrize("grid_type", [GridType.GEOHASH, GridType.MGRS, GridType.ISEA4H])
def test_locate_space_code_matches_full_locate_address(sdk: CubeEncoderSDK, grid_type: GridType) -> None:
    level = LEVELS[grid_type]
    address = sdk.locate_space_code(grid_type=grid_type, requested_grid_level=level, point=POINT)
    cell = sdk.locate(grid_type=grid_type, requested_grid_level=level, point=POINT)

    assert isinstance(address, GridAddress)
    assert address.grid_type == cell.grid_type
    assert address.grid_level == cell.grid_level
    assert address.space_code == cell.space_code


@pytest.mark.parametrize("grid_type", [GridType.GEOHASH, GridType.MGRS, GridType.ISEA4H])
def test_locate_space_codes_matches_single_lookup(sdk: CubeEncoderSDK, grid_type: GridType) -> None:
    level = LEVELS[grid_type]
    points = [POINT, [POINT[0] + 0.01, POINT[1] + 0.01]]

    addresses = sdk.locate_space_codes(grid_type=grid_type, requested_grid_level=level, points=points)

    assert [address.space_code for address in addresses] == [
        sdk.locate_space_code(grid_type=grid_type, requested_grid_level=level, point=point).space_code
        for point in points
    ]


@pytest.mark.parametrize("grid_type", [GridType.GEOHASH, GridType.MGRS, GridType.ISEA4H])
def test_locate_space_codes_accepts_empty_points(sdk: CubeEncoderSDK, grid_type: GridType) -> None:
    assert sdk.locate_space_codes(grid_type=grid_type, requested_grid_level=LEVELS[grid_type], points=[]) == []


@pytest.mark.parametrize("grid_type", [GridType.GEOHASH, GridType.MGRS, GridType.ISEA4H])
def test_generate_st_codes_matches_single_generation(sdk: CubeEncoderSDK, grid_type: GridType) -> None:
    level = LEVELS[grid_type]
    points = [POINT, [POINT[0] + 0.01, POINT[1] + 0.01]]
    addresses = sdk.locate_space_codes(grid_type=grid_type, requested_grid_level=level, points=points)
    timestamps = [datetime(2026, 4, 24, 3, 4, 5, tzinfo=timezone.utc), datetime(2026, 4, 25, tzinfo=timezone.utc)]

    st_codes = sdk.generate_st_codes(
        grid_type=grid_type,
        grid_level=level,
        space_codes=[address.space_code for address in addresses],
        timestamps=timestamps,
        time_granularity=TimeGranularity.DAY,
    )

    assert st_codes == [
        sdk.generate_st_code(address, timestamp, TimeGranularity.DAY).st_code
        for address, timestamp in zip(addresses, timestamps)
    ]


@pytest.mark.parametrize("grid_type", [GridType.GEOHASH, GridType.MGRS, GridType.ISEA4H])
def test_cover_returns_grid_cells(sdk: CubeEncoderSDK, grid_type: GridType) -> None:
    level = LEVELS[grid_type]
    cells = sdk.cover(
        grid_type=grid_type,
        requested_grid_level=level,
        cover_mode=CoverMode.INTERSECT,
        boundary_type=BoundaryType.POLYGON,
        geometry=BBOX_GEOMETRY,
    )
    assert cells
    assert all(isinstance(c, GridCell) for c in cells)
    assert all(c.grid_type == grid_type.value for c in cells)
    assert all(c.grid_level == level for c in cells)


@pytest.mark.parametrize("grid_type", [GridType.GEOHASH, GridType.MGRS, GridType.ISEA4H])
def test_cover_compact_returns_compact_cells(sdk: CubeEncoderSDK, grid_type: GridType) -> None:
    from grid_core.app.models.compact_grid_cell import CompactGridCell

    level = LEVELS[grid_type]
    cells = sdk.cover_compact(
        grid_type=grid_type,
        requested_grid_level=level,
        cover_mode=CoverMode.MINIMAL,
        geometry=BBOX_GEOMETRY,
    )
    assert cells
    assert all(isinstance(c, CompactGridCell) for c in cells)
    assert all(c.grid_type == grid_type.value for c in cells)


@pytest.mark.parametrize("grid_type", [GridType.GEOHASH, GridType.MGRS, GridType.ISEA4H])
def test_neighbors_returns_addresses(sdk: CubeEncoderSDK, grid_type: GridType) -> None:
    level = LEVELS[grid_type]
    cell = sdk.locate(grid_type=grid_type, requested_grid_level=level, point=POINT)
    address = GridAddress(
        grid_type=cell.grid_type,
        grid_level=cell.grid_level,
        space_code=cell.space_code,
        topology_code=cell.topology_code,
    )
    neighbors = sdk.neighbors(address, k=1)
    assert neighbors
    assert all(isinstance(n, GridAddress) for n in neighbors)
    assert all(n.grid_type == grid_type.value for n in neighbors)


@pytest.mark.parametrize("grid_type", [GridType.GEOHASH, GridType.MGRS, GridType.ISEA4H])
def test_parent_and_children_roundtrip(sdk: CubeEncoderSDK, grid_type: GridType) -> None:
    level = LEVELS[grid_type]
    cell = sdk.locate(grid_type=grid_type, requested_grid_level=level, point=POINT)
    address = GridAddress(
        grid_type=cell.grid_type,
        grid_level=cell.grid_level,
        space_code=cell.space_code,
        topology_code=cell.topology_code,
    )

    parent = sdk.parent(address)
    assert isinstance(parent, GridAddress)
    assert parent.grid_type == grid_type.value
    assert parent.grid_level == level - 1

    children = sdk.children(parent, target_grid_level=level)
    assert children
    assert all(isinstance(c, GridAddress) for c in children)
    assert all(c.grid_level == level for c in children)
    child_codes = {c.space_code for c in children}
    assert address.space_code in child_codes


@pytest.mark.parametrize("grid_type", [GridType.GEOHASH, GridType.MGRS, GridType.ISEA4H])
def test_code_to_geometry_and_bbox(sdk: CubeEncoderSDK, grid_type: GridType) -> None:
    level = LEVELS[grid_type]
    cell = sdk.locate(grid_type=grid_type, requested_grid_level=level, point=POINT)
    address = GridAddress(
        grid_type=cell.grid_type,
        grid_level=cell.grid_level,
        space_code=cell.space_code,
        topology_code=cell.topology_code,
    )

    geometry = sdk.code_to_geometry(address, boundary_type=BoundaryType.POLYGON)
    assert geometry["type"] == "Polygon"
    ring = geometry["coordinates"][0]
    assert len(ring) >= 4
    assert ring[0] == ring[-1]

    bbox = sdk.code_to_bbox(address)
    assert len(bbox) == 4
    assert bbox[0] <= bbox[2]
    assert bbox[1] <= bbox[3]


def test_codes_to_geometries_dedup_key(sdk: CubeEncoderSDK) -> None:
    """codes_to_geometries keys by topology_code or grid_type:grid_level:space_code."""
    level = LEVELS[GridType.MGRS]
    cell = sdk.locate(grid_type=GridType.MGRS, requested_grid_level=level, point=POINT)
    address = GridAddress(
        grid_type=cell.grid_type,
        grid_level=cell.grid_level,
        space_code=cell.space_code,
        topology_code=cell.topology_code,
    )
    result = sdk.codes_to_geometries([address, address], boundary_type=BoundaryType.POLYGON)
    assert len(result) == 1
    expected_key = address.topology_code or f"{address.grid_type}:{address.grid_level}:{address.space_code}"
    assert expected_key in result


@pytest.mark.parametrize("grid_type", [GridType.GEOHASH, GridType.MGRS, GridType.ISEA4H])
def test_generate_and_parse_st_code_roundtrip(sdk: CubeEncoderSDK, grid_type: GridType) -> None:
    level = LEVELS[grid_type]
    cell = sdk.locate(grid_type=grid_type, requested_grid_level=level, point=POINT)
    address = GridAddress(
        grid_type=cell.grid_type,
        grid_level=cell.grid_level,
        space_code=cell.space_code,
        topology_code=cell.topology_code,
    )
    timestamp = datetime(2024, 3, 15, 12, 30, tzinfo=timezone.utc)

    st_code = sdk.generate_st_code(address, timestamp, time_granularity=TimeGranularity.MINUTE)
    assert st_code.grid_type == grid_type.value
    assert st_code.grid_level == level
    assert st_code.space_code == cell.space_code

    parsed = sdk.parse_st_code(st_code.st_code)
    assert parsed.grid_type == grid_type.value
    assert parsed.grid_level == level
    assert parsed.space_code == cell.space_code
    assert parsed.time_code == st_code.time_code
