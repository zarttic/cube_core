"""Differential test for the ISEA4H ring fast path and the shared-ring cell build.

``_continuous_ring`` used to try all six rotations of the cell boundary and pick the
narrowest normalised ring.  A cell that does not straddle the antimeridian normalises
to the same ring from every rotation, so it now normalises once; the six-candidate
search is kept for the cells that really cross the date line.  ``_make_cell`` also
built its own ring for the bbox on top of the one its geometry used -- it now shares
one ring.

Both changes must be invisible: same ring polygon, same WGS84 shape, same bbox.
"""

from __future__ import annotations

from shapely.geometry import Polygon, shape

from grid_core.app.engines.isea4h.addressing import cell_count
from grid_core.app.engines.isea4h.geometry import cell_boundary_polygon
from grid_core.app.engines.isea4h_engine import (
    ISEA4HEngine,
    _cell_bbox,
    _cell_shape,
    _continuous_ring,
    _shape_from_ring,
    _wrap_longitude,
)
from grid_core.app.utils.geometry import normalize_ring_longitudes


def _legacy_continuous_ring(seqnum: int, res: int) -> list[list[float]]:
    """The original six-rotation search, kept verbatim as the oracle."""
    corners = cell_boundary_polygon(seqnum, res)
    candidates: list[list[list[float]]] = []
    for start in range(len(corners)):
        ring = normalize_ring_longitudes(corners[start:] + corners[:start])
        if abs(ring[-1][0] - ring[0][0]) <= 180.0 + 1e-6:
            candidates.append(ring)
    if not candidates:
        return normalize_ring_longitudes(corners)
    return min(candidates, key=lambda ring: max(point[0] for point in ring) - min(point[0] for point in ring))


def _legacy_bbox(ring: list[list[float]], shape) -> list[float]:
    min_lon = min(point[0] for point in ring)
    max_lon = max(point[0] for point in ring)
    min_lat = min(point[1] for point in ring)
    max_lat = max(point[1] for point in ring)
    if max_lon - min_lon <= 180.0 + 1e-6:
        return [_wrap_longitude(min_lon), min_lat, _wrap_longitude(max_lon), max_lat]
    return list(shape.bounds)


def _cases() -> list[tuple[int, int]]:
    cases: list[tuple[int, int]] = []
    for res in range(0, 4):
        cases.extend((seqnum, res) for seqnum in range(1, cell_count(res) + 1))
    cases.extend((seqnum, 4) for seqnum in range(1, cell_count(4) + 1, 7))
    for res in (5, 6):
        cases.extend((seqnum, res) for seqnum in range(1, cell_count(res) + 1, 101))
    engine = ISEA4HEngine()
    for res in (4, 5, 6):
        for lon in (179.99, -179.99, 170.0, -170.0, 0.0):
            for lat in (0.0, 60.0, 84.5, -84.5, 89.0, -89.0):
                cases.append((int(engine.locate_space_code(lon, lat, res).space_code), res))
    return cases


def test_ring_fast_path_matches_the_six_rotation_search() -> None:
    cases = _cases()
    assert len(cases) > 1500

    dateline_cells = 0
    for seqnum, res in cases:
        corners = cell_boundary_polygon(seqnum, res)
        longitudes = [point[0] for point in corners]
        if max(longitudes) - min(longitudes) > 180.0:
            dateline_cells += 1  # exercises the retained slow path

        new_ring = _continuous_ring(seqnum, res)
        old_ring = _legacy_continuous_ring(seqnum, res)
        assert Polygon(new_ring).normalize() == Polygon(old_ring).normalize(), (seqnum, res)

        new_shape = _cell_shape(seqnum, res)
        old_shape = _shape_from_ring(old_ring)
        assert new_shape.equals(old_shape), (seqnum, res)
        assert _cell_bbox(seqnum, res) == _legacy_bbox(old_ring, old_shape), (seqnum, res)

    assert dateline_cells > 0, "the sample must include cells that cross the date line"


def test_make_cell_keeps_the_same_bbox_and_geometry() -> None:
    engine = ISEA4HEngine()
    for lon, lat, level in (
        (116.391, 39.907, 6),
        (179.99, 0.0, 6),
        (-179.99, 0.0, 6),
        (0.0, 84.5, 4),
        (0.0, -84.5, 4),
        (-10.0, 89.2, 2),
    ):
        cell = engine.locate_point(lon, lat, level)
        seqnum = int(cell.space_code)
        old_ring = _legacy_continuous_ring(seqnum, level)
        old_shape = _shape_from_ring(old_ring)
        assert cell.bbox == _legacy_bbox(old_ring, old_shape)
        assert shape(cell.geometry).area > 0
