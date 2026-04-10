from datetime import datetime, timezone

import h3
from s2sphere import CellId

from grid_core.sdk import CubeEncoderSDK


def test_sdk_locate_and_cover_mgrs_minimal():
    sdk = CubeEncoderSDK()
    located = sdk.locate(grid_type="mgrs", level=3, point=[116.391, 39.907])
    assert located.grid_type == "mgrs"
    assert located.space_code

    intersect = sdk.cover(
        grid_type="mgrs",
        level=2,
        cover_mode="intersect",
        boundary_type="polygon",
        bbox=[116.37, 39.89, 116.43, 39.93],
    )
    minimal = sdk.cover(
        grid_type="mgrs",
        level=2,
        cover_mode="minimal",
        boundary_type="polygon",
        bbox=[116.37, 39.89, 116.43, 39.93],
    )

    intersect_codes = {cell.space_code for cell in intersect}
    minimal_codes = {cell.space_code for cell in minimal}
    assert minimal_codes.issubset(intersect_codes)


def test_sdk_topology_roundtrip():
    sdk = CubeEncoderSDK()
    located = sdk.locate(grid_type="geohash", level=7, point=[116.391, 39.907])
    code = located.space_code

    parent_code = sdk.parent(grid_type="geohash", code=code)
    assert CellId.from_token(parent_code).level() == 6

    child_codes = sdk.children(grid_type="geohash", code=parent_code, target_level=7)
    assert len(child_codes) == 4
    assert code in child_codes

    neighbor_codes = sdk.neighbors(grid_type="geohash", code=code, k=1)
    assert len(neighbor_codes) > 0

    geom = sdk.code_to_geometry(grid_type="geohash", code=code, boundary_type="polygon")
    assert geom["type"] == "Polygon"

    geom_map = sdk.codes_to_geometries(grid_type="geohash", codes=neighbor_codes[:3], boundary_type="polygon")
    assert len(geom_map) == 3


def test_sdk_st_code_generate_parse_batch():
    sdk = CubeEncoderSDK()
    g1 = sdk.locate(grid_type="geohash", level=7, point=[116.391, 39.907]).space_code
    g2 = sdk.locate(grid_type="geohash", level=7, point=[116.392, 39.908]).space_code
    st = sdk.generate_st_code(
        grid_type="isea4h",
        level=7,
        space_code=h3.latlng_to_cell(39.907, 116.391, 7),
        timestamp=datetime(2026, 3, 9, 15, 30, 0, tzinfo=timezone.utc),
        time_granularity="minute",
        version="v1",
    )
    assert st.st_code.startswith("hx:7:")

    parsed = sdk.parse_st_code(st.st_code)
    assert parsed.grid_type == "isea4h"
    assert parsed.level == 7

    batch = sdk.batch_generate_st_codes(
        grid_type="geohash",
        level=7,
        time_granularity="minute",
        version="v1",
        items=[
            {"space_code": g1, "timestamp": datetime(2026, 3, 9, 15, 30, 0, tzinfo=timezone.utc)},
            {"space_code": g2, "timestamp": datetime(2026, 3, 9, 15, 31, 0, tzinfo=timezone.utc)},
        ],
    )
    assert len(batch) == 2
    assert batch[0] == f"gh:7:{g1}:202603091530:v1"
