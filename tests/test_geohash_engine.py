from grid_core.app.engines.geohash_engine import GeohashEngine
from grid_core.app.utils import geohash_utils


def test_locate_point_returns_valid_cell():
    engine = GeohashEngine()
    cell = engine.locate_point(lon=116.391, lat=39.907, level=7)

    assert len(cell.space_code) == 7
    assert cell.grid_type == "geohash"
    assert cell.level == 7
    assert len(cell.bbox) == 4
    assert len(cell.center) == 2


def test_neighbors_k1_non_empty():
    engine = GeohashEngine()
    codes = engine.neighbors("wtw3sjq", k=1)
    assert len(codes) > 0
    assert "wtw3sjq" not in codes


def test_cover_intersect_polygon_non_empty():
    engine = GeohashEngine()
    polygon = {
        "type": "Polygon",
        "coordinates": [
            [
                [116.38, 39.90],
                [116.40, 39.90],
                [116.40, 39.91],
                [116.38, 39.91],
                [116.38, 39.90],
            ]
        ],
    }
    cells = engine.cover_geometry(polygon, level=6, cover_mode="intersect")
    assert len(cells) > 0


def test_cover_contain_returns_only_fully_contained_cells():
    engine = GeohashEngine()
    code = geohash_utils.encode(116.391, 39.907, precision=6)
    min_lon, min_lat, max_lon, max_lat = geohash_utils.decode_bbox(code)
    polygon = {
        "type": "Polygon",
        "coordinates": [
            [
                [min_lon, min_lat],
                [max_lon, min_lat],
                [max_lon, max_lat],
                [min_lon, max_lat],
                [min_lon, min_lat],
            ]
        ],
    }

    cells = engine.cover_geometry(polygon, level=6, cover_mode="contain")

    assert {cell.space_code for cell in cells} == {code}


def test_cover_minimal_is_subset_of_intersect():
    engine = GeohashEngine()
    polygon = {
        "type": "Polygon",
        "coordinates": [
            [
                [116.38, 39.90],
                [116.40, 39.90],
                [116.40, 39.91],
                [116.38, 39.91],
                [116.38, 39.90],
            ]
        ],
    }

    intersect_codes = {c.space_code for c in engine.cover_geometry(polygon, level=6, cover_mode="intersect")}
    minimal_codes = {c.space_code for c in engine.cover_geometry(polygon, level=6, cover_mode="minimal")}

    assert minimal_codes.issubset(intersect_codes)
    assert len(minimal_codes) > 0
