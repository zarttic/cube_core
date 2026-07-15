"""ISEA4H geometry: cell centres and boundary polygons.

Ported from DGGRID's DgIDGGBase::setAddVertices. Cell corners are the six
hexagon vertices in the resolution grid's continuous frame, converted through
Q2DD -> vertex2DD -> projTri -> geo. For the twelve pentagon cells (each quad's
(0,0) address, plus the two pole caps) the "nokeep" sub-triangle corner is
dropped, yielding five corners.

All public outputs are WGS84 (lon_deg, lat_deg) in degrees. No I/O; never
imports h3 or calls dggrid.
"""
from __future__ import annotations

import math

from grid_core.app.engines.isea4h.addressing import (
    _VERT_TABLE,
    _compute_subtriangle,
    _inv_quantify,
    is_pole_quad,
    q2dd_to_geo,
    q2di_to_geo,
    seqnum_to_q2di,
)
from grid_core.app.engines.isea4h.constants import HEX_R

_R2D = 180.0 / math.pi
_DGGRID_NORTH_POLE_LON = -179.1061274
_DGGRID_SOUTH_POLE_LON = -157.3603104

# Hexagon corner offsets in the continuous (ccRF) frame, matching
# DgHexC1Grid2D::setAddVertices (r = HEX_R = 1/sqrt(3)).
_R2 = HEX_R / 2.0
_HEX_CORNERS: tuple[tuple[float, float], ...] = (
    (0.0, HEX_R),
    (-0.5, _R2),
    (-0.5, -_R2),
    (0.0, -HEX_R),
    (0.5, -_R2),
    (0.5, _R2),
)


def _norm_lon(lon_deg: float) -> float:
    return ((lon_deg + 180.0) % 360.0) - 180.0


def cell_center(seqnum: int, res: int) -> tuple[float, float]:
    """Return the (lon_deg, lat_deg) centre of the ISEA4H cell *seqnum*."""
    quad, i, j = seqnum_to_q2di(seqnum, res)
    lat, lon = q2di_to_geo(quad, i, j, res)
    if abs(abs(lat) - math.pi / 2.0) < 1e-9:
        # DGGRID assigns deterministic longitudes to the geographic poles.
        lon = math.radians(_DGGRID_NORTH_POLE_LON if lat > 0.0 else _DGGRID_SOUTH_POLE_LON)
    return (_norm_lon(lon * _R2D), lat * _R2D)


def is_pentagon(seqnum: int, res: int) -> bool:
    """Return True iff the cell is one of the 12 pentagons (icosa vertices)."""
    quad, i, j = seqnum_to_q2di(seqnum, res)
    return is_pole_quad(quad) or (i == 0 and j == 0)


def cell_boundary_polygon(seqnum: int, res: int) -> list[tuple[float, float]]:
    """Return the cell boundary as a list of (lon_deg, lat_deg) corners.

    Hexagons return 6 corners, pentagons return 5. The ring is not explicitly
    closed (no repeated first point). Corners are ordered as DGGRID emits them.
    """
    quad, i, j = seqnum_to_q2di(seqnum, res)
    mag = 2 ** res
    # centre of the cell in the resolution grid's continuous frame
    ccx, ccy = _inv_quantify(i, j)

    is_vertex_cell = (i == 0 and j == 0) or is_pole_quad(quad)

    corners: list[tuple[float, float]] = []
    for dx, dy in _HEX_CORNERS:
        # corner in ccRF, then scale to Q2DD (quad edge length 1.0)
        qx = (ccx + dx) / mag
        qy = (ccy + dy) / mag
        if is_vertex_cell:
            # drop the corner that falls in the quad's "nokeep" sub-triangle
            st = _compute_subtriangle(qx, qy)
            if not _VERT_TABLE[quad][st][4]:
                continue
        lat, lon = q2dd_to_geo(quad, qx, qy)
        corners.append((_norm_lon(lon * _R2D), lat * _R2D))
    return corners
