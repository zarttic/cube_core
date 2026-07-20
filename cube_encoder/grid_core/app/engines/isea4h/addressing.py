"""ISEA4H addressing: DGGRID GLOBAL_SEQUENCE (SEQNUM) <-> Q2DI, and the full
geodetic <-> Q2DI pipeline.

This is a direct port of the DGGRID converter chain for aperture-4 HEXAGON
(Class I) grids:

  geo -> projTri        (DgProjISEA, see projection.py)
      -> vertex2DD       (DgProjTriToVertex2DD / DgQ2DDtoVertex2DDConverter)
      -> Q2DD            (DgVertex2DDToQ2DDConverter)
      -> Q2DI            (DgQ2DDtoIConverter: quantify + overage folding)

and the inverse. SEQNUM ordering is DgBoundedIDGG::seqNumAddress:

  quad 0            -> seqnum 1                       (north pole)
  quad 1..10        -> seqnum 2 + (q-1)*4**res + i*mag + j
  quad 11           -> seqnum 10*4**res + 2           (south pole)

where mag = 2**res. The poles sit in the middle of the sequence, not at the
ends, exactly as DGGRID's GLOBAL_SEQUENCE numbers them.

Angles are radians internally. No I/O; never imports h3 or calls dggrid.
"""
from __future__ import annotations

import math

from grid_core.app.engines.isea4h.constants import (
    HEX_R,
    M_HALF,
    M_SIN60,
    M_SQRT3,
)
from grid_core.app.engines.isea4h.projection import sllxys, snyder_fwd, snyder_inv, which_icosa_tris

# ---------------------------------------------------------------------------
# Lookup tables (ported verbatim from DgIDGGutil.cpp)
# ---------------------------------------------------------------------------
# vertTable_[vertNum][subTri] -> DgVertTriVals(quadNum, triNum, subTri, keep,
#                                               trans, rot60)
# We store (triNum, trans_x, trans_y, rot60, keep). triNum is the *projection*
# triangle (2nd constructor arg); vertNum == quad index is the row.
_M = M_HALF
_S = M_SIN60
_VERT_TABLE: tuple[tuple[tuple[int, float, float, int, bool], ...], ...] = (
    (  # vert 0
        (1, -_M, -_S, 3, True), (0, -1.0, 0.0, 2, True), (4, -_M, _S, 1, True),
        (-1, -_M, _S, 1, False), (3, 1.0, 0.0, -1, True), (2, _M, -_S, -2, True),
    ),
    (  # vert 1
        (0, 0.0, 0.0, 1, True), (5, -_M, -_S, 4, True), (14, -_M, _S, 1, True),
        (-1, -_M, _S, 1, False), (9, 0.0, 0.0, 3, True), (4, 1.0, 0.0, 0, True),
    ),
    (  # vert 2
        (1, 0.0, 0.0, 1, True), (6, -_M, -_S, 4, True), (10, -_M, _S, 1, True),
        (-1, -_M, _S, 1, False), (5, 0.0, 0.0, 3, True), (0, 1.0, 0.0, 0, True),
    ),
    (  # vert 3
        (2, 0.0, 0.0, 1, True), (7, -_M, -_S, 4, True), (11, -_M, _S, 1, True),
        (-1, -_M, _S, 1, False), (6, 0.0, 0.0, 3, True), (1, 1.0, 0.0, 0, True),
    ),
    (  # vert 4
        (3, 0.0, 0.0, 1, True), (8, -_M, -_S, 4, True), (12, -_M, _S, 1, True),
        (-1, -_M, _S, 1, False), (7, 0.0, 0.0, 3, True), (2, 1.0, 0.0, 0, True),
    ),
    (  # vert 5
        (4, 0.0, 0.0, 1, True), (9, -_M, -_S, 4, True), (13, -_M, _S, 1, True),
        (-1, -_M, _S, 1, False), (8, 0.0, 0.0, 3, True), (3, 1.0, 0.0, 0, True),
    ),
    (  # vert 6
        (10, 0.0, 0.0, 1, True), (15, -_M, -_S, 4, True), (19, 0.0, 0.0, -1, True),
        (14, -_M, _S, 2, True), (-1, -_M, _S, 1, False), (5, _M, -_S, 4, True),
    ),
    (  # vert 7
        (11, 0.0, 0.0, 1, True), (16, -_M, -_S, 4, True), (15, 0.0, 0.0, -1, True),
        (10, -_M, _S, 2, True), (-1, -_M, _S, 1, False), (6, _M, -_S, 4, True),
    ),
    (  # vert 8
        (12, 0.0, 0.0, 1, True), (17, -_M, -_S, 4, True), (16, 0.0, 0.0, -1, True),
        (11, -_M, _S, 2, True), (-1, -_M, _S, 1, False), (7, _M, -_S, 4, True),
    ),
    (  # vert 9
        (13, 0.0, 0.0, 1, True), (18, -_M, -_S, 4, True), (17, 0.0, 0.0, -1, True),
        (12, -_M, _S, 2, True), (-1, -_M, _S, 1, False), (8, _M, -_S, 4, True),
    ),
    (  # vert 10
        (14, 0.0, 0.0, 1, True), (19, -_M, -_S, 4, True), (18, 0.0, 0.0, -1, True),
        (13, -_M, _S, 2, True), (-1, -_M, _S, 1, False), (9, _M, -_S, 4, True),
    ),
    (  # vert 11
        (17, -_M, -_S, 3, True), (18, -1.0, 0.0, 2, True), (19, -_M, _S, 1, True),
        (15, _M, _S, 0, True), (-1, 0.0, 0.0, 0, False), (16, _M, -_S, -2, True),
    ),
)

# triTable_[triNum] -> (quadNum, trans_x, trans_y, rot60); used geo->cell.
_TRI_TABLE: tuple[tuple[int, float, float, int], ...] = (
    (1, 0.0, 0.0, 1), (2, 0.0, 0.0, 1), (3, 0.0, 0.0, 1), (4, 0.0, 0.0, 1),
    (5, 0.0, 0.0, 1),
    (1, -_M, -_S, 4), (2, -_M, -_S, 4), (3, -_M, -_S, 4), (4, -_M, -_S, 4),
    (5, -_M, -_S, 4),
    (6, 0.0, 0.0, 1), (7, 0.0, 0.0, 1), (8, 0.0, 0.0, 1), (9, 0.0, 0.0, 1),
    (10, 0.0, 0.0, 1),
    (6, -_M, -_S, 4), (7, -_M, -_S, 4), (8, -_M, -_S, 4), (9, -_M, -_S, 4),
    (10, -_M, -_S, 4),
)

# edgeTable_[quad] -> (isType0, loneVert, upQuad, downQuad, rightQuad, leftQuad)
_EDGE_TABLE: tuple[tuple[bool, int, int, int, int, int], ...] = (
    (True, 0, 0, 0, 0, 0), (True, 0, 2, 10, 6, 5), (True, 0, 3, 6, 7, 1),
    (True, 0, 4, 7, 8, 2), (True, 0, 5, 8, 9, 3), (True, 0, 1, 9, 10, 4),
    (False, 11, 2, 10, 7, 1), (False, 11, 3, 6, 8, 2), (False, 11, 4, 7, 9, 3),
    (False, 11, 5, 8, 10, 4), (False, 11, 1, 9, 6, 5), (False, 11, 0, 0, 0, 0),
)


# ---------------------------------------------------------------------------
# SEQNUM <-> Q2DI
# ---------------------------------------------------------------------------

def cell_count(res: int) -> int:
    """Total number of ISEA4H cells at *res*: ``10 * 4**res + 2``."""
    return 10 * 4 ** res + 2


def offset_per_quad(res: int) -> int:
    """Cells per diamond quad at *res*: ``4**res``."""
    return 4 ** res


def intra_quad_index(i: int, j: int, res: int) -> int:
    """Row-major linear index of (i, j) within a diamond quad at *res*.

    ``index = i * mag + j`` where ``mag = 2**res``; the inverse of the
    intra-quad decomposition used by :func:`q2di_to_seqnum`.
    """
    return i * (2 ** res) + j


def q2di_to_seqnum(quad: int, i: int, j: int, res: int) -> int:
    """Convert Q2DI (quad, i, j) at *res* to DGGRID GLOBAL_SEQUENCE number."""
    if quad == 0:
        return 1
    if quad == 11:
        return cell_count(res)
    mag = 2 ** res
    return 2 + (quad - 1) * offset_per_quad(res) + i * mag + j


def seqnum_to_q2di(seqnum: int, res: int) -> tuple[int, int, int]:
    """Convert DGGRID GLOBAL_SEQUENCE *seqnum* at *res* to (quad, i, j)."""
    n = cell_count(res)
    if seqnum == 1:
        return (0, 0, 0)
    if seqnum == n:
        return (11, 0, 0)
    off = offset_per_quad(res)
    mag = 2 ** res
    idx = seqnum - 2
    quad = idx // off + 1
    intra = idx % off
    return (quad, intra // mag, intra % mag)


def validate_seqnum(seqnum: int, res: int) -> bool:
    """Return True iff *seqnum* is a valid DGGRID SEQNUM at *res*."""
    return 1 <= seqnum <= cell_count(res)


# ---------------------------------------------------------------------------
# Hex grid quantize / inverse-quantize (DgHexC1Grid2D)
# ---------------------------------------------------------------------------

def _rotate(x: float, y: float, degrees: float) -> tuple[float, float]:
    """Rotate (x, y) counter-clockwise by *degrees* (DgDVec2D::rotate)."""
    a = degrees * math.pi / 180.0
    c = math.cos(a)
    s = math.sin(a)
    return (x * c - y * s, x * s + y * c)


def _compute_subtriangle(x: float, y: float) -> int:
    """Port of DgQ2DDtoVertex2DDConverter::compute_subtriangle."""
    tol = 1e-15
    xs = M_SQRT3 * x
    xpp = xs + tol
    xmp = -xs + tol
    xpm = xs - tol
    xmm = -xs - tol
    if y >= xmm and y > xpp:
        return 0
    if (abs(y) <= tol and abs(x) <= tol) or (y <= xpp and y >= -tol):
        return 1
    if y < -tol and y > xmp:
        return 2
    if y <= xmp and y < xpm:
        return 3
    if y >= xpm and y < -tol:
        return 4
    if y >= -tol and y < xmm:
        return 5
    raise ValueError(f"value out of hex: ({x}, {y})")


def _inv_quantify(i: int, j: int) -> tuple[float, float]:
    """Port of DgHexC1Grid2D::invQuantify: (i, j) -> ccRF (x, y)."""
    return (i - 0.5 * j, j * 1.5 * HEX_R)


def _quantify(px: float, py: float) -> tuple[int, int]:
    """Port of DgHexC1Grid2D::quantify: ccRF (x, y) -> (i, j)."""
    a1 = abs(px)
    a2 = abs(py)
    x2 = a2 / M_SIN60
    x1 = a1 + x2 / 2.0
    m1 = int(x1)
    m2 = int(x2)
    r1 = x1 - m1
    r2 = x2 - m2
    ai = 0
    aj = 0
    if r1 < 0.5:
        if r1 < 1.0 / 3.0:
            if r2 < (1.0 + r1) / 2.0:
                ai, aj = m1, m2
            else:
                ai, aj = m1, m2 + 1
        else:
            aj = m2 if r2 < (1.0 - r1) else m2 + 1
            ai = m1 + 1 if (1.0 - r1) <= r2 < (2.0 * r1) else m1
    else:
        if r1 < 2.0 / 3.0:
            aj = m2 if r2 < (1.0 - r1) else m2 + 1
            ai = m1 if (2.0 * r1 - 1.0) < r2 < (1.0 - r1) else m1 + 1
        else:
            if r2 < (r1 / 2.0):
                ai, aj = m1 + 1, m2
            else:
                ai, aj = m1 + 1, m2 + 1
    # fold across the axes if necessary
    if px < 0.0:
        if (aj % 2) == 0:
            axisi = aj // 2
            diff = ai - axisi
            ai = ai - 2 * diff
        else:
            axisi = (aj + 1) // 2
            diff = ai - axisi
            ai = ai - (2 * diff + 1)
    if py < 0.0:
        ai = ai - (2 * aj + 1) // 2
        aj = -1 * aj
    return (ai, aj)


# ---------------------------------------------------------------------------
# Quad overage resolution
# ---------------------------------------------------------------------------

def _resolve_overage(q: int, i: int, j: int, num_d: int) -> tuple[int, int, int]:
    """Port of DgQ2DDtoIConverter overage handling for hex grids.

    Fold an (i, j) that ran off quad *q*'s [0, num_d-1]^2 range onto the
    correct neighbouring quad. *num_d* == mag == 2**res.
    """
    max_ij = num_d - 1
    top_edge = max_ij + 1
    for _ in range(12):
        under_i = i < 0
        under_j = j < 0
        over_i = i > max_ij
        over_j = j > max_ij
        num_over = under_i + under_j + over_i + over_j
        if not num_over:
            return (q, i, j)
        is_type0, lone_vert, up_q, down_q, right_q, left_q = _EDGE_TABLE[q]
        if over_i and over_j:
            q = up_q if is_type0 else right_q
            i, j = 0, 0
        elif under_i:
            q = left_q
            if is_type0:
                i, j = top_edge - j + i, top_edge + i
            else:
                i = top_edge + i
        elif under_j:
            q = down_q
            if is_type0:
                i, j = i, top_edge + j
            else:
                i, j = top_edge + j, (top_edge - i) + j
        elif over_i:
            if is_type0:
                q = right_q
                i = i - top_edge
            else:
                if j == 0:
                    q = lone_vert
                    i, j = 0, 0
                else:
                    q = right_q
                    i_over = i - top_edge
                    i, j = (top_edge - j) + i_over, i_over
        elif over_j:
            if is_type0:
                if i == 0:
                    q = lone_vert
                    i, j = 0, 0
                else:
                    q = up_q
                    j_over = j - top_edge
                    i, j = j_over, top_edge - i + j_over
            else:
                q = up_q
                j = j - top_edge
    raise ValueError("overage resolution did not converge")


def q2dix_to_q2di(q: int, i: int, j: int, num_d: int) -> tuple[int, int, int]:
    """Port of DgBoundedIDGG::q2dixToQ2di (hex grid branch).

    Take a (quad, i, j) that may lie off its quad (e.g. a neighbour or an
    edge child) and return the canonical (quad, i, j). *num_d* == mag.
    """
    for _ in range(10):
        origin = (0, 0)
        offset = (0, 0)
        good = False
        if q == 0:
            if i > 0:
                q = 2
                origin = (0, num_d)
                offset = (j, -i + j)
            elif j > 0:
                q = 3
                origin = (0, num_d)
                offset = (-i + j, -i)
            elif i < 0:
                q = 5
                origin = (0, num_d)
                offset = (-j, i - j)
            elif j < 0:
                q = 1
                origin = (0, num_d)
                offset = (i, j)
            else:
                good = True
        elif q <= 5:
            tbl = {1: (5, 2, 6, 10), 2: (1, 3, 7, 6), 3: (2, 4, 8, 7),
                   4: (3, 5, 9, 8), 5: (4, 1, 10, 9)}
            a, b, c, d = tbl[q]
            if i == 0 and j == num_d:
                q, i, j, good = 0, 0, 0, True
            elif i >= num_d:
                q = c
                origin = (-num_d, 0)
                offset = (i, j)
            elif j >= num_d:
                q = b
                origin = (-num_d, 0)
                offset = (j, -i + j)
            elif i < 0:
                q = a
                origin = (num_d, num_d)
                offset = (i - j, i)
            elif j < 0:
                q = d
                origin = (0, num_d)
                offset = (i, j)
            else:
                good = True
        elif q <= 10:
            tbl = {6: (1, 2, 7, 10), 7: (2, 3, 8, 6), 8: (3, 4, 9, 7),
                   9: (4, 5, 10, 8), 10: (5, 1, 6, 9)}
            a, b, c, d = tbl[q]
            if i == num_d and j == 0:
                q, i, j, good = 11, 0, 0, True
            elif i < 0:
                q = a
                origin = (num_d, 0)
                offset = (i, j)
            elif j >= num_d:
                q = b
                origin = (0, -num_d)
                offset = (i, j)
            elif i >= num_d:
                q = c
                origin = (0, -num_d)
                offset = (i - j, i)
            elif j < 0:
                q = d
                origin = (num_d, num_d)
                offset = (j, -i + j)
            else:
                good = True
        else:  # q == 11
            if i > 0:
                q = 7
                origin = (num_d, 0)
                offset = (-j, i - j)
            elif j > 0:
                q = 6
                origin = (num_d, 0)
                offset = (i - j, i)
            elif i < 0:
                q = 10
                origin = (num_d, 0)
                offset = (i, j)
            elif j < 0:
                q = 9
                origin = (num_d, 0)
                offset = (-i + j, -i)
            else:
                good = True
        if good:
            return (q, i, j)
        i = origin[0] + offset[0]
        j = origin[1] + offset[1]
    raise ValueError("q2dixToQ2di did not converge")


# ---------------------------------------------------------------------------
# geodetic <-> Q2DI (full pipeline)
# ---------------------------------------------------------------------------

def geo_to_q2di(lat: float, lon: float, res: int) -> tuple[int, int, int]:
    """(lat, lon) radians -> canonical Q2DI (quad, i, j) at *res*."""
    tri, x, y = snyder_fwd(lat, lon)
    return _projected_tri_to_q2di(tri, x, y, res)


def _projected_tri_to_q2di(tri: int, x: float, y: float, res: int) -> tuple[int, int, int]:
    quad, tx, ty, rot60 = _TRI_TABLE[tri]
    # projTri -> vertex2DD: rotate(rot60*60); -= trans
    px, py = _rotate(x, y, rot60 * 60.0)
    px -= tx
    py -= ty
    # vertex2DD -> Q2DD is identity coords; Q2DD -> Q2DI: scale to ccRF, quantify
    mag = 2 ** res
    i, j = _quantify(px * mag, py * mag)
    return _resolve_overage(quad, i, j, mag)


def geo_to_q2dis(latitudes: list[float], longitudes: list[float], res: int) -> list[tuple[int, int, int]]:
    """Batch geodetic lookup, accelerating only the independent face selection."""
    if not latitudes:
        return []
    triangles = which_icosa_tris(latitudes, longitudes)
    return [
        _projected_tri_to_q2di(triangle, *projected_point, res)
        for triangle, projected_point in zip(triangles, sllxys(latitudes, longitudes, triangles))
    ]


def q2di_to_geo(quad: int, i: int, j: int, res: int) -> tuple[float, float]:
    """Canonical Q2DI (quad, i, j) at *res* -> cell centre (lat, lon) radians."""
    from grid_core.app.engines.isea4h.constants import ICOVERTS

    if quad == 0:
        return ICOVERTS[0]
    if quad == 11:
        return ICOVERTS[11]
    mag = 2 ** res
    cx, cy = _inv_quantify(i, j)
    qx = cx / mag
    qy = cy / mag
    return q2dd_to_geo(quad, qx, qy)


def q2dd_to_geo(quad: int, qx: float, qy: float) -> tuple[float, float]:
    """Q2DD (quad, x, y) -> (lat, lon) radians via vertTable + snyder_inv."""
    st = _compute_subtriangle(qx, qy)
    tri_num, tx, ty, rot60, _keep = _VERT_TABLE[quad][st]
    # vertex2DD -> projTri: += trans; rotate(-rot60*60)
    px = qx + tx
    py = qy + ty
    px, py = _rotate(px, py, -rot60 * 60.0)
    return snyder_inv(tri_num, px, py)


def is_pole_quad(quad: int) -> bool:
    """Return True for the two pole caps (quad 0 north, quad 11 south)."""
    return quad == 0 or quad == 11


def locate_cell(lon_deg: float, lat_deg: float, res: int) -> tuple[int, int, int]:
    """Locate the Q2DI (quad, i, j) containing a WGS84 (lon, lat) point.

    Degrees in; canonical Q2DI out. Thin wrapper over :func:`geo_to_q2di`.
    """
    return geo_to_q2di(lat_deg * math.pi / 180.0, lon_deg * math.pi / 180.0, res)


def locate_cells(points: list[list[float]], res: int) -> list[tuple[int, int, int]]:
    """Locate multiple WGS84 degree points at one resolution."""
    latitudes = [float(point[1]) * math.pi / 180.0 for point in points]
    longitudes = [float(point[0]) * math.pi / 180.0 for point in points]
    return geo_to_q2dis(latitudes, longitudes, res)


def locate_seqnum(lon_deg: float, lat_deg: float, res: int) -> int:
    """Locate the SEQNUM of the cell containing a WGS84 (lon, lat) point."""
    quad, i, j = locate_cell(lon_deg, lat_deg, res)
    return q2di_to_seqnum(quad, i, j, res)
