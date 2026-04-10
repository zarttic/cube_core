from s2sphere import CellId

from grid_core.app.engines.geohash_engine import GeohashEngine
from grid_core.app.utils.geometry import bbox_to_polygon


def _expand_s2(engine: GeohashEngine, codes: set[str], target_level: int) -> set[str]:
    out: set[str] = set()
    for code in codes:
        cid = CellId.from_token(code)
        if cid.level() == target_level:
            out.add(code)
            continue
        out.update(engine.children(code, target_level))
    return out


def test_locate_point_returns_valid_cell():
    engine = GeohashEngine()
    cell = engine.locate_point(lon=116.391, lat=39.907, level=7)

    cid = CellId.from_token(cell.space_code)
    assert cid.is_valid()
    assert cid.level() == 7
    assert cell.grid_type == "geohash"
    assert cell.level == 7
    assert len(cell.bbox) == 4
    assert len(cell.center) == 2


def test_neighbors_k1_non_empty():
    engine = GeohashEngine()
    code = engine.locate_point(lon=116.391, lat=39.907, level=7).space_code
    codes = engine.neighbors(code, k=1)
    assert len(codes) > 0
    assert code not in codes


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
    code = engine.locate_point(lon=116.391, lat=39.907, level=6).space_code
    min_lon, min_lat, max_lon, max_lat = engine.code_to_bbox(code)
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


def test_cover_minimal_expanded_is_subset_of_intersect():
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
    minimal_cells = engine.cover_geometry(polygon, level=6, cover_mode="minimal")
    minimal_codes = {c.space_code for c in minimal_cells}
    expanded_minimal = _expand_s2(engine, minimal_codes, target_level=6)

    assert expanded_minimal.issubset(intersect_codes)
    assert len(minimal_codes) > 0
    assert len(minimal_codes) <= len(expanded_minimal)


def test_cover_intersect_dateline_crossing_bbox_polygon():
    engine = GeohashEngine()
    geometry = bbox_to_polygon([170.0, -10.0, -170.0, 10.0]).__geo_interface__

    cells = engine.cover_geometry(geometry, level=3, cover_mode="intersect")

    assert len(cells) > 0
    assert all(-90.0 <= cell.center[1] <= 90.0 for cell in cells)
