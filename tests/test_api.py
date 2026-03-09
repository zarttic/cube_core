from datetime import datetime, timezone

from grid_core.app.api.code import batch_generate_st, generate_st, parse_st
from grid_core.app.api.grid import cover, locate
from grid_core.app.api.topology import children, parent
from grid_core.app.main import health
from grid_core.app.models.request import (
    ChildrenRequest,
    CoverRequest,
    LocateRequest,
    ParentRequest,
    STCodeBatchGenerateRequest,
    STCodeGenerateRequest,
    STCodeParseRequest,
)


def test_health():
    body = health()
    assert body["status"] == "ok"


def test_locate_endpoint_function():
    req = LocateRequest(grid_type="geohash", level=7, point=[116.391, 39.907])
    resp = locate(req)
    assert resp.cell.grid_type == "geohash"
    assert len(resp.cell.space_code) == 7


def test_st_code_functions():
    gen_req = STCodeGenerateRequest(
        grid_type="geohash",
        level=7,
        space_code="wtw3sjq",
        timestamp=datetime(2026, 3, 9, 15, 30, 0, tzinfo=timezone.utc),
        time_granularity="minute",
        version="v1",
    )
    gen_resp = generate_st(gen_req)
    assert gen_resp.st_code == "gh:7:wtw3sjq:202603091530:v1"

    parse_resp = parse_st(STCodeParseRequest(st_code=gen_resp.st_code))
    assert parse_resp.grid_type == "geohash"
    assert parse_resp.level == 7


def test_st_code_batch_function():
    req = STCodeBatchGenerateRequest(
        grid_type="geohash",
        level=7,
        time_granularity="minute",
        version="v1",
        items=[
            {"space_code": "wtw3sjq", "timestamp": "2026-03-09T15:30:00Z"},
            {"space_code": "wtw3sjr", "timestamp": "2026-03-09T15:31:00Z"},
        ],
    )
    resp = batch_generate_st(req)
    assert resp.statistics["count"] == 2
    assert resp.st_codes[0] == "gh:7:wtw3sjq:202603091530:v1"


def test_cover_with_bbox_input():
    req = CoverRequest(grid_type="geohash", level=6, cover_mode="intersect", bbox=[116.38, 39.90, 116.40, 39.91])
    resp = cover(req)
    assert resp.grid_type == "geohash"
    assert resp.statistics["cell_count"] > 0


def test_topology_parent_children_functions():
    parent_resp = parent(ParentRequest(grid_type="geohash", code="wtw3sjq"))
    assert parent_resp.parent_code == "wtw3sj"

    children_resp = children(ChildrenRequest(grid_type="geohash", code="wtw3sj", target_level=7))
    assert len(children_resp.child_codes) == 32
