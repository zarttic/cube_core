import pytest
from fastapi.responses import HTMLResponse

from grid_core.app.api.demo import map_page, sdk_cover, sdk_locate
from grid_core.app.core.exceptions import NotImplementedCapabilityError
from grid_core.app.models.request import CoverRequest, LocateRequest


def test_demo_map_page_loads_html():
    resp = map_page()
    assert isinstance(resp, HTMLResponse)
    assert "Grid Visualizer" in resp.body.decode("utf-8")


def test_demo_sdk_locate_geohash_works():
    resp = sdk_locate(LocateRequest(grid_type="geohash", level=7, point=[116.391, 39.907]))
    assert resp.cell.grid_type == "geohash"
    assert len(resp.cell.space_code) == 7


def test_demo_sdk_cover_geohash_works():
    resp = sdk_cover(
        CoverRequest(
            grid_type="geohash",
            level=6,
            cover_mode="intersect",
            boundary_type="polygon",
            bbox=[116.385, 39.903, 116.397, 39.911],
        )
    )
    assert resp.grid_type == "geohash"
    assert resp.statistics["cell_count"] > 0


def test_demo_sdk_locate_isea4h_returns_not_implemented():
    with pytest.raises(NotImplementedCapabilityError):
        sdk_locate(LocateRequest(grid_type="isea4h", level=7, point=[116.391, 39.907]))
