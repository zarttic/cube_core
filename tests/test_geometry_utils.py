from grid_core.app.utils.geometry import bbox_to_polygon


def test_bbox_to_polygon_normal_bbox_returns_polygon():
    geom = bbox_to_polygon([100.0, 20.0, 101.0, 21.0])
    assert geom.geom_type == "Polygon"


def test_bbox_to_polygon_dateline_crossing_returns_multipolygon():
    geom = bbox_to_polygon([170.0, -10.0, -170.0, 10.0])
    assert geom.geom_type == "MultiPolygon"
    assert len(list(geom.geoms)) == 2
