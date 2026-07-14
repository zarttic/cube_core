"""ISEA4H topology: neighbours, parent, children.

Ported from DGGRID's DgIDGGS4H (aperture-4 hexagon hierarchy) and
DgHexC1Grid2D neighbour offsets, with quad folding via
DgBoundedIDGG::q2dixToQ2di.

  - :func:`cell_neighbors` uses the six CCW hex neighbour offsets, folded
    onto their canonical quad; duplicates (at the 12 pentagons) collapse to
    five distinct neighbours.
  - :func:`cell_children` returns the interior child (cell centre lowered to
    res+1) plus the boundary edge-midpoint children, exactly as
    ``DgIDGGS4H::setAddAllChildren``.
  - :func:`cell_parent` inverts children: the parent is the res-1 cell whose
    children include *seqnum*.

All operations take and return SEQNUM integers. No I/O; never imports h3 or
calls dggrid.
"""
from __future__ import annotations

from grid_core.app.engines.isea4h.addressing import (
    _inv_quantify,
    _quantify,
    q2di_to_seqnum,
    q2dix_to_q2di,
    seqnum_to_q2di,
    validate_seqnum,
)
from grid_core.app.engines.isea4h.constants import HEX_R

# CCW hex neighbour offsets, from DgHexC1Grid2D::setAddNeighbors.
_HEX_NBR: tuple[tuple[int, int], ...] = (
    (1, 0), (1, 1), (0, 1), (-1, 0), (-1, -1), (0, -1),
)

# Hexagon corner offsets in the continuous (ccRF) frame
# (DgHexC1Grid2D::setAddVertices, r = HEX_R).
_R2 = HEX_R / 2.0
_HEX_CORNERS: tuple[tuple[float, float], ...] = (
    (0.0, HEX_R),
    (-0.5, _R2),
    (-0.5, -_R2),
    (0.0, -HEX_R),
    (0.5, -_R2),
    (0.5, _R2),
)


def cell_neighbors(seqnum: int, res: int) -> list[int]:
    """Return the SEQNUM neighbours of *seqnum* at *res* (sorted).

    Six for hexagons, five for the twelve pentagons.
    """
    if not validate_seqnum(seqnum, res):
        raise ValueError(f"invalid seqnum {seqnum} at res {res}")
    quad, i, j = seqnum_to_q2di(seqnum, res)
    num_d = 2 ** res
    out: list[int] = []
    seen: set[int] = set()
    for di, dj in _HEX_NBR:
        nq, ni, nj = q2dix_to_q2di(quad, i + di, j + dj, num_d)
        ns = q2di_to_seqnum(nq, ni, nj, res)
        if ns != seqnum and ns not in seen:
            seen.add(ns)
            out.append(ns)
    return sorted(out)


def cell_children(seqnum: int, res: int, target_res: int | None = None) -> list[int]:
    """Return the SEQNUM children of *seqnum* at res+1 (sorted).

    *target_res*, if given, must equal ``res + 1`` (aperture-4 children are
    defined one resolution finer). Six children for pentagons, seven for
    hexagons, matching DGGRID's TEXT children output.
    """
    if target_res is not None and target_res != res + 1:
        raise ValueError("ISEA4H children are defined only for res+1")
    if not validate_seqnum(seqnum, res):
        raise ValueError(f"invalid seqnum {seqnum} at res {res}")

    quad, i, j = seqnum_to_q2di(seqnum, res)
    cres = res + 1
    cmag = 2 ** cres
    mag = 2 ** res
    out: list[int] = []
    seen: set[int] = set()

    def _add(q: int, ci: int, cj: int) -> None:
        cs = q2di_to_seqnum(q, ci, cj, cres)
        if cs not in seen:
            seen.add(cs)
            out.append(cs)

    # interior child: cell centre lowered to res+1
    ccx, ccy = _inv_quantify(i, j)
    qx, qy = ccx / mag, ccy / mag
    ii, jj = _quantify(qx * cmag, qy * cmag)
    q, ii, jj = q2dix_to_q2di(quad, ii, jj, cmag)
    _add(q, ii, jj)

    # boundary children: the six edge midpoints of the parent hexagon
    verts = [((ccx + dx) / mag, (ccy + dy) / mag) for dx, dy in _HEX_CORNERS]
    nv = len(verts)
    for k in range(nv):
        ax, ay = verts[k]
        bx, by = verts[(k + 1) % nv]
        mx = (ax + bx) / 2.0
        my = (ay + by) / 2.0
        mi, mj = _quantify(mx * cmag, my * cmag)
        q2, mi, mj = q2dix_to_q2di(quad, mi, mj, cmag)
        _add(q2, mi, mj)

    return sorted(out)


def cell_parent(seqnum: int, res: int) -> int:
    """Return the SEQNUM of the parent cell at res-1.

    Consistent with :func:`cell_children`: the parent is the unique res-1 cell
    whose children list contains *seqnum*.
    """
    if res <= 0:
        raise ValueError("resolution 0 cells have no parent")
    if not validate_seqnum(seqnum, res):
        raise ValueError(f"invalid seqnum {seqnum} at res {res}")

    pres = res - 1
    # An interior child shares the parent's centre; boundary children are
    # shared. Find the parent by testing the candidate parents that can reach
    # this cell. The cell's own quad centre at pres is the primary candidate;
    # search its pres-neighbourhood for the parent whose children include us.
    from grid_core.app.engines.isea4h.addressing import cell_count

    # Direct candidate: lower the cell centre to pres.
    quad, i, j = seqnum_to_q2di(seqnum, res)
    mag = 2 ** res
    pmag = 2 ** pres
    ccx, ccy = _inv_quantify(i, j)
    qx, qy = ccx / mag, ccy / mag
    pi, pj = _quantify(qx * pmag, qy * pmag)
    pq, pi, pj = q2dix_to_q2di(quad, pi, pj, pmag)
    candidates = [q2di_to_seqnum(pq, pi, pj, pres)]
    # plus that candidate's neighbourhood, since boundary children belong to
    # an adjacent parent
    for extra in cell_neighbors(candidates[0], pres):
        candidates.append(extra)

    for cand in candidates:
        if seqnum in cell_children(cand, pres):
            return cand

    # Fallback: exhaustive (never expected, but keeps parent total).
    for cand in range(1, cell_count(pres) + 1):
        if seqnum in cell_children(cand, pres):
            return cand
    raise ValueError(f"no parent found for seqnum {seqnum} at res {res}")
