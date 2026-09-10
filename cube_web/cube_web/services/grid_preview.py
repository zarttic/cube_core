"""Display-only geometry helpers for the grid preview map.

The production MGRS cover remains the standard UTM/UPS cell cover.  A map
preview that crosses two UTM zones can look discontinuous because each zone
has its own projected 100 km lattice.  This module builds a regular display
lattice in one UTM CRS without changing the production cell identities or
their geometries.
"""

from __future__ import annotations

import math
import re
from collections import Counter
from typing import Sequence

from grid_core.sdk import GridCell
from pyproj import CRS, Transformer
from pyproj.exceptions import CRSError
from shapely.geometry import Polygon, box, mapping, shape
from shapely.ops import transform
from shapely.validation import make_valid

_MGRS_ZONE_RE = re.compile(r"^(?P<zone>\d{1,2})(?P<band>[C-HJ-NP-X])")
_NORTHERN_BANDS = frozenset("NPQRSTUVWX")
_EDGE_SEGMENTS = 8
_MAX_PREVIEW_CELLS = 20_000


def build_continuous_mgrs_preview(
    cells: Sequence[GridCell],
    *,
    requested_grid_level: int,
    target_geometry: dict,
    preview_crs: str | None = None,
) -> list[GridCell]:
    """Build a regular, display-only MGRS preview for one AOI.

    ``cells`` are intentionally not modified.  The returned cells are only
    used by the web map and carry ``preview_only`` metadata so callers can
    distinguish their synthetic display geometry from the real MGRS cover.
    If the AOI cannot be represented by a supported UTM CRS, an empty list is
    returned and the caller should render the native cells instead.
    """
    if not cells or not 0 <= requested_grid_level <= 5:
        return []

    target = shape(target_geometry)
    if target.is_empty:
        return []

    resolved = _resolve_utm_crs(cells, preview_crs)
    if resolved is None:
        return []
    target_crs, zone, hemisphere = resolved

    try:
        forward = Transformer.from_crs("EPSG:4326", target_crs, always_xy=True)
        inverse = Transformer.from_crs(target_crs, "EPSG:4326", always_xy=True)
        projected_target = transform(forward.transform, target)
    except (CRSError, TypeError, ValueError):
        return []
    if projected_target.is_empty:
        return []

    # The source footprint is a geographic rectangle in the current web
    # preview flow.  Use its projected extent as the selection envelope so
    # corner cells are not dropped merely because the rectangle is slightly
    # curved after projection; the display lattice must remain continuous.
    projected_selection = box(*projected_target.bounds)

    cell_size = 100_000.0 / (10 ** requested_grid_level)
    min_x, min_y, max_x, max_y = projected_target.bounds
    start_x = math.floor(min_x / cell_size) * cell_size
    start_y = math.floor(min_y / cell_size) * cell_size
    column_count = max(0, math.ceil((max_x - start_x) / cell_size - 1e-12))
    row_count = max(0, math.ceil((max_y - start_y) / cell_size - 1e-12))
    if not column_count or not row_count or column_count * row_count > _MAX_PREVIEW_CELLS:
        return []

    preview_cells: list[GridCell] = []
    for column in range(column_count):
        easting = start_x + column * cell_size
        for row in range(row_count):
            northing = start_y + row * cell_size
            projected_square = _projected_square(easting, northing, cell_size)
            if projected_square.intersection(projected_selection).area <= 0.0:
                continue
            display_geometry = _to_wgs84(projected_square, inverse)
            if display_geometry.is_empty:
                continue
            if not display_geometry.is_valid:
                display_geometry = make_valid(display_geometry)
            center = display_geometry.centroid
            preview_cells.append(
                GridCell(
                    grid_type="mgrs",
                    grid_level=requested_grid_level,
                    space_code=f"preview-{zone:02d}-{int(easting)}-{int(northing)}",
                    topology_code=None,
                    center=[float(center.x), float(center.y)],
                    bbox=[float(value) for value in display_geometry.bounds],
                    geometry=dict(mapping(display_geometry)),
                    metadata={
                        "preview_only": True,
                        "preview_projection": target_crs.to_string(),
                        "preview_hemisphere": hemisphere,
                        "preview_cell_size_m": cell_size,
                    },
                )
            )
    return preview_cells


def _resolve_utm_crs(
    cells: Sequence[GridCell], preview_crs: str | None
) -> tuple[CRS, int, str] | None:
    if preview_crs:
        try:
            candidate = CRS.from_user_input(preview_crs)
            epsg = candidate.to_epsg()
        except (CRSError, TypeError, ValueError):
            epsg = None
            candidate = None
        if epsg is not None and 32601 <= epsg <= 32660:
            return candidate, epsg - 32600, "north"  # type: ignore[return-value]
        if epsg is not None and 32701 <= epsg <= 32760:
            return candidate, epsg - 32700, "south"  # type: ignore[return-value]
        return None

    parsed = [_parse_mgrs_code(cell.space_code) for cell in cells]
    parsed = [item for item in parsed if item is not None]
    if not parsed:
        return None
    zone, band = Counter(parsed).most_common(1)[0][0]
    epsg = 32600 + zone if band in _NORTHERN_BANDS else 32700 + zone
    return CRS.from_epsg(epsg), zone, "north" if epsg < 32700 else "south"


def _parse_mgrs_code(code: str) -> tuple[int, str] | None:
    match = _MGRS_ZONE_RE.match(str(code).replace(" ", "").upper())
    if not match:
        return None
    zone = int(match.group("zone"))
    if not 1 <= zone <= 60:
        return None
    return zone, match.group("band")


def _projected_square(easting: float, northing: float, size: float) -> Polygon:
    corners = [
        (easting, northing),
        (easting + size, northing),
        (easting + size, northing + size),
        (easting, northing + size),
    ]
    ring: list[tuple[float, float]] = []
    for index, (x0, y0) in enumerate(corners):
        x1, y1 = corners[(index + 1) % len(corners)]
        for segment in range(_EDGE_SEGMENTS):
            fraction = segment / _EDGE_SEGMENTS
            ring.append((x0 + (x1 - x0) * fraction, y0 + (y1 - y0) * fraction))
    ring.append(corners[0])
    return Polygon(ring)


def _to_wgs84(projected_square: Polygon, inverse: Transformer):
    return transform(inverse.transform, projected_square)
