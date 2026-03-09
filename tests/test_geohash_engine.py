from grid_core.app.engines.geohash_engine import GeohashEngine


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
