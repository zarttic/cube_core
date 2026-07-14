"""DGGRID v8.44 ISEA4H constants and icosahedron construction.

This is a direct port of DGGRID's icosahedron setup and ISEA projection
constants. Orientation matches the DGGRID default:

  dggs_vert0_lon: 11.25
  dggs_vert0_lat: 58.28252559
  dggs_vert0_azimuth: 0.0
  Aperture: 4 (PURE), Topology: HEXAGON, Projection: ISEA
  proj_datum WGS84_AUTHALIC_SPHERE

The icosahedron is built exactly as ``DgSphIcosa::ico12verts`` does: 11
vertices are placed in a pole-centred frame and rotated onto the sphere via
``coordtrans`` so that vertex 0 lands at (11.25, 58.28252559). The 20
triangle centres (``TRICEN``) and per-triangle azimuth offsets (``DAZH``)
are then precomputed, matching ``DgProjTriRF.cpp``.

All internal angles are radians. Public geodetic APIs use degrees.
"""
from __future__ import annotations

import math

# ---------------------------------------------------------------------------
# Sphere
# ---------------------------------------------------------------------------
SPHERE_RADIUS_KM: float = 6371.007180918475

# ---------------------------------------------------------------------------
# DGGRID orientation vertex 0
# ---------------------------------------------------------------------------
VERT0_LON: float = 11.25          # degrees east
VERT0_LAT: float = 58.28252559    # degrees north
VERT0_AZ: float = 0.0             # azimuth degrees

# ---------------------------------------------------------------------------
# Aperture / topology
# ---------------------------------------------------------------------------
QUAD_COUNT: int = 10
APERTURE: int = 4

# ---------------------------------------------------------------------------
# ISEA projection constants (from DgProjISEA.cpp)
# ---------------------------------------------------------------------------
_D2R = math.pi / 180.0
_R2D = 180.0 / math.pi

R1 = 0.9103832815
R1S = R1 * R1
DH = 37.37736814 * _D2R
GH = 36.0 * _D2R
COT30 = 1.0 / math.tan(30.0 * _D2R)
TAN_DH = math.tan(DH)
COS_DH = math.cos(DH)
SIN_GH = math.sin(GH)
COS_GH = math.cos(GH)
ORIGIN_X_OFF = 0.6022955029
ORIGIN_Y_OFF = 0.3477354707
ICOSA_EDGE = 2.0 * ORIGIN_X_OFF
PRECISION = 0.0000000000005

M_PI = math.pi
M_2PI = 2.0 * math.pi
M_PI_2 = math.pi / 2.0

# Hex grid geometry constants (from DgConstants.h)
M_HALF = 0.5
M_SIN60 = 0.8660254037844386467637231707529361834714
M_SQRT3 = 1.7320508075688772935274463415058723669428
M_1_SQRT3 = 0.5773502691896257645091487805019574556476
# hex grid radius r() used by DgHexC1Grid2D (M_1_SQRT3)
HEX_R = M_1_SQRT3

# ---------------------------------------------------------------------------
# Icosahedron triangle -> vertex index table (from DgSphIcosa::ico12verts)
# ---------------------------------------------------------------------------
TRI_VERTS: tuple[tuple[int, int, int], ...] = (
    (0, 1, 2), (0, 2, 3), (0, 3, 4), (0, 4, 5), (0, 5, 1),
    (6, 2, 1), (7, 3, 2), (8, 4, 3), (9, 5, 4), (10, 1, 5),
    (2, 6, 7), (3, 7, 8), (4, 8, 9), (5, 9, 10), (1, 10, 6),
    (11, 7, 6), (11, 8, 7), (11, 9, 8), (11, 10, 9), (11, 6, 10),
)


# ---------------------------------------------------------------------------
# Icosahedron construction (port of DgSphIcosa::ico12verts + DgProjTriRF)
# All coordinates below are (lat, lon) in radians, matching the C++ GeoCoord.
# ---------------------------------------------------------------------------

def _llxyz(lat: float, lon: float) -> tuple[float, float, float]:
    x = math.cos(lat) * math.cos(lon)
    y = math.cos(lat) * math.sin(lon)
    z = math.sin(lat)
    eps = 1e-15
    if abs(x) < eps:
        x = 0.0
    if abs(y) < eps:
        y = 0.0
    if abs(z) < eps:
        z = 0.0
    return (x, y, z)


def _xyzll(v: tuple[float, float, float]) -> tuple[float, float]:
    x, y, z = v
    z = max(-1.0, min(1.0, z))
    lat = math.asin(z)
    if lat in (M_PI_2, -M_PI_2):
        lon = 0.0
    else:
        lon = math.atan2(y, x)
    return (lat, lon)


def _vec_normalize(v: tuple[float, float, float]) -> tuple[float, float, float]:
    n = math.sqrt(v[0] ** 2 + v[1] ** 2 + v[2] ** 2)
    return (v[0] / n, v[1] / n, v[2] / n)


def _sph_tricenpoint(pts: list[tuple[float, float]]) -> tuple[float, float]:
    p = [_llxyz(la, lo) for (la, lo) in pts]
    cp = (
        (p[0][0] + p[1][0] + p[2][0]) / 3.0,
        (p[0][1] + p[1][1] + p[2][1]) / 3.0,
        (p[0][2] + p[1][2] + p[2][2]) / 3.0,
    )
    return _xyzll(_vec_normalize(cp))


def _coordtrans(
    new_np: tuple[float, float], pt_old: tuple[float, float], lon0: float
) -> tuple[float, float]:
    """Port of coordtrans() in DgProjTriRF.cpp. (lat, lon) radians."""
    np_lat, np_lon = new_np
    p_lat, p_lon = pt_old
    cos_new_lat = (
        math.sin(np_lat) * math.sin(p_lat)
        + math.cos(np_lat) * math.cos(p_lat) * math.cos(np_lon - p_lon)
    )
    cos_new_lat = max(-1.0, min(1.0, cos_new_lat))
    new_lat = math.acos(cos_new_lat)
    if abs(new_lat - 0.0) < PRECISION * 100000:
        new_lon = 0.0
    elif abs(new_lat - M_PI) < PRECISION * 100000:
        new_lon = 0.0
    else:
        cos_new_lon = (
            math.sin(p_lat) * math.cos(np_lat)
            - math.cos(p_lat) * math.sin(np_lat) * math.cos(np_lon - p_lon)
        ) / math.sin(new_lat)
        cos_new_lon = max(-1.0, min(1.0, cos_new_lon))
        new_lon = math.acos(cos_new_lon)
        if 0 <= (p_lon - np_lon) < M_PI:
            new_lon = -new_lon + lon0
        else:
            new_lon = new_lon + lon0
        if new_lon > M_PI:
            new_lon -= 2 * M_PI
        if new_lon < -M_PI:
            new_lon += 2 * M_PI
    new_lat = M_PI / 2 - new_lat
    return (new_lat, new_lon)


def _build_icosa() -> tuple[
    list[tuple[float, float]],
    list[list[tuple[float, float]]],
    list[tuple[float, float]],
    list[float],
]:
    """Return (icoverts, icotri, tricen, dazh) with (lat, lon) radians."""
    vert0_lat = VERT0_LAT * _D2R
    vert0_lon = VERT0_LON * _D2R
    az = VERT0_AZ * _D2R

    icoverts: list[tuple[float, float]] = [(0.0, 0.0)] * 12
    vertsnew: list[tuple[float, float]] = [(0.0, 0.0)] * 12
    newnpold = (vert0_lat, 0.0)
    for i in range(1, 6):
        lat = 26.565051177 * _D2R
        lon = -az + 72 * (i - 1) * _D2R
        if lon > M_PI - PRECISION:
            lon -= 2 * M_PI
        if lon < -(M_PI + PRECISION):
            lon += 2 * M_PI
        vertsnew[i] = (lat, lon)
        lat2 = -26.565051177 * _D2R
        lon2 = -az + (36.0 + 72.0 * (i - 1)) * _D2R
        if lon2 > M_PI - PRECISION:
            lon2 -= 2 * M_PI
        if lon2 < -(M_PI + PRECISION):
            lon2 += 2 * M_PI
        vertsnew[i + 5] = (lat2, lon2)
    vertsnew[11] = (-90.0 * _D2R, 0.0)
    icoverts[0] = (vert0_lat, vert0_lon)
    for i in range(1, 12):
        icoverts[i] = _coordtrans(newnpold, vertsnew[i], vert0_lon)

    icotri: list[list[tuple[float, float]]] = []
    tricen: list[tuple[float, float]] = []
    dazh: list[float] = []
    for i in range(20):
        tri = [icoverts[TRI_VERTS[i][j]] for j in range(3)]
        icotri.append(tri)
        clat, clon = _sph_tricenpoint(tri)
        tricen.append((clat, clon))
        t0lat, t0lon = tri[0]
        da = math.atan2(
            math.cos(t0lat) * math.sin(t0lon - clon),
            math.cos(clat) * math.sin(t0lat)
            - math.sin(clat) * math.cos(t0lat) * math.cos(t0lon - clon),
        )
        dazh.append(da)
    return icoverts, icotri, tricen, dazh


ICOVERTS, ICOTRI, TRICEN, DAZH = _build_icosa()

assert len(ICOVERTS) == 12
assert len(TRICEN) == 20
assert len(DAZH) == 20

# ---------------------------------------------------------------------------
# Derived public views
# ---------------------------------------------------------------------------
# North pole is DGGRID GLOBAL_SEQUENCE seqnum 1 (quad 0). The south pole is the
# last seqnum (cell_count(res)); both sit at fixed quad caps. Note: the poles
# are NOT the icosahedron vertices - they are the two pole caps of the quad
# layout. See addressing.py.
POLE_SEQNUM_N: int = 1

# The 12 icosahedron vertices as WGS84 (lon_deg, lat_deg), derived from the
# radian (lat, lon) ICOVERTS built above. These are the centres of the twelve
# res-0 pentagons.
ICOSAHEDRON_VERTICES: tuple[tuple[float, float], ...] = tuple(
    (((lon * _R2D + 180.0) % 360.0) - 180.0, lat * _R2D) for (lat, lon) in ICOVERTS
)
assert len(ICOSAHEDRON_VERTICES) == 12

