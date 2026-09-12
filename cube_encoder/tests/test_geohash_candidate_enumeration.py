"""Differential test for the arithmetic geohash candidate enumeration.

``_cells_for_bbox`` used to walk the cell lattice from the lower-left seed with one
decode-shift-encode round trip per cell.  It now computes the (longitude index,
latitude index) rectangle arithmetically, which is ~2x faster but must return the
exact same candidate set -- including the half-open cell edges, the antimeridian
clamp and the polar clamp.  The original walk is kept here as the oracle.
"""

from __future__ import annotations

from grid_core.app.engines.geohash_engine import (
    _cells_and_boxes_for_bbox,
    _cells_for_bbox,
    _decode_bbox,
    _encode,
    _neighbor_in_direction,
)

# (lon_min, lat_min, lon_max, lat_max, precision)
CASES: tuple[tuple[float, float, float, float, int], ...] = (
    (116.0, 39.5, 117.0, 40.5, 6),
    (114.75, 33.85, 122.77, 38.50, 5),
    (116.391, 39.907, 116.3912, 39.9072, 7),
    (-180.0, -90.0, 180.0, 90.0, 1),
    (-180.0, -90.0, 180.0, 90.0, 2),
    (179.9, -1.0, 180.0, 1.0, 6),
    (-180.0, -1.0, -179.9, 1.0, 6),
    (0.0, 84.0, 0.1, 84.1, 5),
    (0.0, -84.1, 0.1, -84.0, 5),
    (-90.0, -90.0, -89.99, -89.99, 4),
    (0.0, 0.0, 0.0, 0.0, 3),
    (10.0, 10.0, 5.0, 5.0, 3),
    (-0.010986328125, 0.0, 0.0, 0.010986328125, 6),
    (116.40673828125, 39.90234375, 116.417724609375, 39.913330078125, 6),
)


def _walk_enumeration(lon_min: float, lat_min: float, lon_max: float, lat_max: float, precision: int) -> set[str]:
    """The original seed-and-walk enumeration, kept verbatim as the oracle."""
    seed = _encode(
        max(-180.0, min(lon_min, 179.9999999)),
        max(-90.0, lat_min),
        precision,
    )
    result_codes: set[str] = set()
    row_start = seed
    while True:
        row_code = row_start
        while True:
            result_codes.add(row_code)
            rb = _decode_bbox(row_code)
            if rb[2] >= lon_max:
                break
            nxt = _neighbor_in_direction(row_code, "right")
            if nxt == row_code:
                break
            row_code = nxt
        rb_start = _decode_bbox(row_start)
        if rb_start[3] >= lat_max:
            break
        nxt_row = _neighbor_in_direction(row_start, "top")
        if nxt_row == row_start:
            break
        row_start = nxt_row
    return result_codes


def test_cells_for_bbox_matches_the_walk_enumeration() -> None:
    for lon_min, lat_min, lon_max, lat_max, precision in CASES:
        expected = _walk_enumeration(lon_min, lat_min, lon_max, lat_max, precision)
        actual = set(_cells_for_bbox(lon_min, lat_min, lon_max, lat_max, precision))
        assert actual == expected, (
            f"bbox={(lon_min, lat_min, lon_max, lat_max)} precision={precision}: "
            f"missing={sorted(expected - actual)[:5]} extra={sorted(actual - expected)[:5]}"
        )


def test_out_of_range_bbox_terminates_and_stays_in_range() -> None:
    """A bbox reaching past lon 180 must terminate.

    The seed-and-walk enumeration never satisfied ``cell.lon_max >= 180.5`` and its
    right-neighbour step wrapped around the antimeridian, so it looped forever.  The
    arithmetic range clamps instead; ``aoi.bounds`` from Shapely never exceeds 180,
    so this only guards against a hand-built bbox.
    """
    cells = _cells_for_bbox(179.99, -0.01, 180.5, 0.01, 7)

    assert cells
    for code in cells:
        lon_min, _, lon_max, _ = _decode_bbox(code)
        assert -180.0 <= lon_min < lon_max <= 180.0


def test_cell_bbox_enumerates_exactly_that_cell() -> None:
    for code in ("9fzk", "wx4g0b", "0", "zzzzzz", "u4pruydqqvj"):
        lon_min, lat_min, lon_max, lat_max = _decode_bbox(code)
        cells = _cells_for_bbox(lon_min, lat_min, lon_max, lat_max, len(code))
        assert cells == [code]


def test_cells_for_bbox_covers_the_requested_span() -> None:
    """Every enumerated cell must overlap the bbox, and every overlapped candidate must appear."""
    from shapely.geometry import box as shapely_box

    for lon_min, lat_min, lon_max, lat_max, precision in (
        (116.0, 39.5, 117.0, 40.5, 5),
        (179.5, -2.0, 180.0, 2.0, 5),
        (-1.0, -1.0, 1.0, 1.0, 4),
    ):
        aoi = shapely_box(lon_min, lat_min, lon_max, lat_max)
        cells = set(_cells_for_bbox(lon_min, lat_min, lon_max, lat_max, precision))
        outer = shapely_box(
            max(-180.0, lon_min) - 1e-6,
            max(-90.0, lat_min) - 1e-6,
            min(180.0, lon_max) + 1e-6,
            min(90.0, lat_max) + 1e-6,
        )
        for code in cells:
            assert shapely_box(*_decode_bbox(code)).intersects(outer)
        # No cell outside the returned set overlaps the bbox.
        step = 360.0 / (1 << ((precision * 5 + 1) // 2))
        lon = max(-180.0, lon_min)
        while lon < min(180.0, lon_max):
            lat = max(-90.0, lat_min)
            while lat < min(90.0, lat_max):
                code = _encode(lon + step / 8.0, lat + step / 8.0, precision)
                assert shapely_box(*_decode_bbox(code)).intersects(aoi)
                assert code in cells
                lat += 180.0 / (1 << ((precision * 5) // 2))
            lon += step


def test_arithmetic_bboxes_are_bit_identical_to_the_bisection_decoder() -> None:
    """The candidate boxes must match ``_decode_bbox`` exactly, not approximately.

    ``_cover_codes`` feeds these boxes straight into the intersection/area test, so a
    one-ULP difference could flip a boundary-touching decision.  All cell edges are
    dyadic values, so exact equality is achievable and must hold.
    """
    checked = 0
    for *bbox, precision in (
        (116.0, 39.5, 117.0, 40.5, 6),
        (114.75, 33.85, 122.77, 38.50, 4),
        (-180.0, -90.0, 180.0, 90.0, 3),
        (179.9, -1.0, 180.0, 1.0, 5),
        (-180.0, -90.0, -179.0, -89.0, 7),
    ):
        codes, boxes = _cells_and_boxes_for_bbox(*bbox, precision)
        assert len(codes) == len(boxes)
        for code, candidate_box in zip(codes, boxes):
            assert _decode_bbox(code) == candidate_box
            checked += 1
    assert checked > 10_000
