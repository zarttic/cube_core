import h3
from fastapi.responses import HTMLResponse

from grid_core.app.api.demo import (
    demo_home,
    demo_index_page,
    demo_script,
    demo_styles,
    encoding_html_page,
    encoding_page,
    map_page,
    partition_html_page,
    partition_page,
    sdk_children,
    sdk_code_to_geometry,
    sdk_codes_to_geometries,
    sdk_cover,
    sdk_locate,
    sdk_neighbors,
    sdk_parent,
)
from grid_core.app.models.request import (
    BatchCodeToGeometryRequest,
    ChildrenRequest,
    CodeToGeometryRequest,
    CoverRequest,
    LocateRequest,
    NeighborsRequest,
    ParentRequest,
)


def test_demo_map_page_loads_html():
    resp = map_page()
    assert isinstance(resp, HTMLResponse)
    html = resp.body.decode("utf-8")
    assert "全球离散格网与数据剖分系统" in html
    assert "encoding.html" in html
    assert "partition.html" in html


def test_demo_static_pages_and_assets_load():
    assert "格网模型与时空编码系统" in encoding_page().body.decode("utf-8")
    assert "分析就绪数据剖分系统" in partition_page().body.decode("utf-8")
    assert "全球离散格网与数据剖分系统" in demo_home().body.decode("utf-8")
    assert "全球离散格网与数据剖分系统" in demo_index_page().body.decode("utf-8")
    assert "document.addEventListener" in demo_script().body.decode("utf-8")
    assert ".site-header" in demo_styles().body.decode("utf-8")
    assert "格网模型与时空编码系统" in encoding_html_page().body.decode("utf-8")
    assert "分析就绪数据剖分系统" in partition_html_page().body.decode("utf-8")


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


def test_demo_sdk_cover_geohash_with_geometry_works():
    resp = sdk_cover(
        CoverRequest(
            grid_type="geohash",
            level=6,
            cover_mode="intersect",
            boundary_type="polygon",
            geometry={
                "type": "Polygon",
                "coordinates": [[[116.385, 39.903], [116.397, 39.903], [116.397, 39.911], [116.385, 39.911], [116.385, 39.903]]],
            },
        )
    )
    assert resp.grid_type == "geohash"
    assert resp.statistics["cell_count"] > 0


def test_demo_sdk_locate_isea4h_works():
    resp = sdk_locate(LocateRequest(grid_type="isea4h", level=7, point=[116.391, 39.907]))
    assert resp.cell.grid_type == "isea4h"
    assert h3.is_valid_cell(resp.cell.space_code)


def test_demo_sdk_topology_geohash_roundtrip():
    located = sdk_locate(LocateRequest(grid_type="geohash", level=7, point=[116.391, 39.907]))
    code = located.cell.space_code

    parent_resp = sdk_parent(ParentRequest(grid_type="geohash", code=code))
    assert parent_resp.parent_code == code[:-1]

    children_resp = sdk_children(ChildrenRequest(grid_type="geohash", code=parent_resp.parent_code, target_level=7))
    assert code in children_resp.child_codes

    neighbors_resp = sdk_neighbors(NeighborsRequest(grid_type="geohash", code=code, k=1))
    assert neighbors_resp.statistics["count"] > 0

    geometry_resp = sdk_code_to_geometry(
        CodeToGeometryRequest(grid_type="geohash", code=code, boundary_type="polygon")
    )
    assert geometry_resp.geometry["type"] == "Polygon"

    geometry_batch = sdk_codes_to_geometries(
        BatchCodeToGeometryRequest(grid_type="geohash", codes=neighbors_resp.result_codes[:3], boundary_type="polygon")
    )
    assert geometry_batch.statistics["count"] == 3
