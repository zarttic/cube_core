from __future__ import annotations

import random

import h3

from grid_core.app.engines.geohash_engine import GeohashEngine
from grid_core.app.engines.isea4h_engine import ISEA4HEngine
from grid_core.app.engines.mgrs_engine import MGRSEngine
from grid_core.app.utils import geohash_utils
from grid_core.app.utils.geometry import bbox_to_polygon


def _expand_geohash(codes: set[str], target_level: int) -> set[str]:
    out: set[str] = set()
    for code in codes:
        if len(code) == target_level:
            out.add(code)
            continue
        frontier = [code]
        while frontier:
            cur = frontier.pop()
            if len(cur) == target_level:
                out.add(cur)
            else:
                frontier.extend(f"{cur}{ch}" for ch in geohash_utils.BASE32)
    return out


def _expand_mgrs(engine: MGRSEngine, codes: set[str], target_level: int) -> set[str]:
    out: set[str] = set()
    for code in codes:
        precision = engine._precision_from_code(code)
        if precision == target_level:
            out.add(code)
        else:
            out.update(engine.children(code, target_level))
    return out


def test_geohash_cover_random_bbox_stability():
    engine = GeohashEngine()
    rng = random.Random(20260311)

    for _ in range(50):
        lat0 = rng.uniform(-80.0, 80.0)
        lat1 = min(89.0, lat0 + rng.uniform(0.3, 5.0))

        if rng.random() < 0.3:
            left = rng.uniform(150.0, 179.0)
            right = rng.uniform(-179.0, -150.0)
            bbox = [left, lat0, right, lat1]
        else:
            lon0 = rng.uniform(-175.0, 170.0)
            lon1 = min(179.0, lon0 + rng.uniform(0.3, 6.0))
            bbox = [lon0, lat0, lon1, lat1]

        geometry = bbox_to_polygon(bbox).__geo_interface__
        intersect_codes = {c.space_code for c in engine.cover_geometry(geometry, level=4, cover_mode="intersect")}
        minimal_codes = {c.space_code for c in engine.cover_geometry(geometry, level=4, cover_mode="minimal")}
        expanded_minimal = _expand_geohash(minimal_codes, target_level=4)

        assert expanded_minimal.issubset(intersect_codes)


def test_mgrs_cover_zone_boundary_stability():
    engine = MGRSEngine()
    longitudes = [5.9, 6.1, 11.9, 12.1, 17.9, 18.1, 177.5, -177.5]

    for lon in longitudes:
        bbox = [lon - 0.05, 39.85, lon + 0.05, 39.95]
        geometry = bbox_to_polygon(bbox).__geo_interface__
        intersect_codes = {c.space_code for c in engine.cover_geometry(geometry, level=2, cover_mode="intersect")}
        minimal_codes = {c.space_code for c in engine.cover_geometry(geometry, level=2, cover_mode="minimal")}
        expanded_minimal = _expand_mgrs(engine, minimal_codes, target_level=2)

        assert expanded_minimal.issubset(intersect_codes)


def test_isea4h_polar_cover_stability():
    engine = ISEA4HEngine()
    geometry = {
        "type": "Polygon",
        "coordinates": [[[-30.0, 84.0], [30.0, 84.0], [30.0, 88.0], [-30.0, 88.0], [-30.0, 84.0]]],
    }

    intersect_codes = {c.space_code for c in engine.cover_geometry(geometry, level=5, cover_mode="intersect")}
    minimal_codes = {c.space_code for c in engine.cover_geometry(geometry, level=5, cover_mode="minimal")}

    expanded_minimal: set[str] = set()
    for code in minimal_codes:
        level = h3.get_resolution(code)
        if level == 5:
            expanded_minimal.add(code)
        else:
            expanded_minimal.update(h3.cell_to_children(code, 5))

    assert expanded_minimal.issubset(intersect_codes)
