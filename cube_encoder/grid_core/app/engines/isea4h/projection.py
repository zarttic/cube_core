"""DGGRID v8.44 ISEA (Icosahedral Snyder Equal Area) projection port.

Direct translation of the C functions in ``DgProjISEA.cpp``:
  - :func:`which_icosa_tri` ports ``DgSphIcosa::whichIcosaTri``
  - :func:`sllxy` ports ``sllxy`` (sphere -> plane forward)
  - :func:`snyder_fwd` ports ``snyderFwd``
  - :func:`snyder_inv` ports ``snyderInv`` (plane -> sphere inverse)

All angles are radians internally, matching the C++ GeoCoord convention.
Plane coordinates (x, y) are the DgProjTriCoord values: the raw projected
point divided by ``ICOSA_EDGE`` (so a triangle edge has unit length).

This module performs no I/O and never imports h3 or calls dggrid.
"""
from __future__ import annotations

import math

from grid_core.app.engines.isea4h.constants import (
    COS_DH,
    COS_GH,
    COT30,
    DAZH,
    ICOSA_EDGE,
    M_2PI,
    M_PI,
    M_PI_2,
    ORIGIN_X_OFF,
    ORIGIN_Y_OFF,
    PRECISION,
    R1,
    R1S,
    SIN_GH,
    TAN_DH,
    TRICEN,
)

_D2R = math.pi / 180.0
_120 = 120.0 * _D2R
_240 = 240.0 * _D2R
_180 = 180.0 * _D2R


def _gc_dist(a: tuple[float, float], b: tuple[float, float]) -> float:
    """Great-circle central angle between two (lat, lon) radian points."""
    la1, lo1 = a
    la2, lo2 = b
    v = math.sin(la1) * math.sin(la2) + math.cos(la1) * math.cos(la2) * math.cos(lo1 - lo2)
    v = max(-1.0, min(1.0, v))
    return math.acos(v)


def which_icosa_tri(lat: float, lon: float) -> int:
    """Return the icosahedron triangle (0-19) whose centre is nearest.

    Port of ``DgSphIcosa::whichIcosaTri``.
    """
    min_face = 0
    min_dist = _gc_dist(TRICEN[0], (lat, lon))
    for i in range(1, 20):
        d = _gc_dist(TRICEN[i], (lat, lon))
        if d < min_dist:
            min_dist = d
            min_face = i
    return min_face


def sllxy(lat: float, lon: float, n_tri: int) -> tuple[float, float]:
    """ISEA forward projection sphere -> plane for triangle ``n_tri``.

    Port of ``sllxy`` in DgProjISEA.cpp. Returns (x, y) in DgProjTriCoord
    units (divided by ICOSA_EDGE).
    """
    clat, clon = TRICEN[n_tri]
    sin_lat_c = math.sin(clat)
    cos_lat_c = math.cos(clat)
    cos_lat = math.cos(lat)
    sin_lat = math.sin(lat)
    dazh = DAZH[n_tri]

    tmp = sin_lat_c * sin_lat + cos_lat_c * cos_lat * math.cos(lon - clon)
    tmp = max(-1.0, min(1.0, tmp))
    z = math.acos(tmp)

    azh = math.atan2(
        cos_lat * math.sin(lon - clon),
        cos_lat_c * sin_lat - sin_lat_c * cos_lat * math.cos(lon - clon),
    ) - dazh

    if azh < 0.0:
        azh += M_2PI
    azh0 = azh
    if _120 <= azh <= _240:
        azh -= _120
    if azh > _240:
        azh -= _240

    cos_azh = math.cos(azh)
    sin_azh = math.sin(azh)

    dz = math.atan2(TAN_DH, cos_azh + COT30 * sin_azh)
    h = math.acos(sin_azh * SIN_GH * COS_DH - cos_azh * COS_GH)
    ag = azh + (36.0 * _D2R) + h - _180
    azh1 = math.atan2(2.0 * ag, R1S * TAN_DH * TAN_DH - 2.0 * ag * COT30)
    fh = TAN_DH / (2.0 * (math.cos(azh1) + COT30 * math.sin(azh1)) * math.sin(dz / 2.0))
    ph = 2.0 * R1 * fh * math.sin(z / 2.0)

    if _120 <= azh0 < _240:
        azh1 += _120
    if azh0 >= _240:
        azh1 += _240

    x = (ph * math.sin(azh1) + ORIGIN_X_OFF) / ICOSA_EDGE
    y = (ph * math.cos(azh1) + ORIGIN_Y_OFF) / ICOSA_EDGE
    return (x, y)


def snyder_fwd(lat: float, lon: float) -> tuple[int, float, float]:
    """Port of ``snyderFwd``: (lat, lon) radians -> (triangle, x, y)."""
    tri = which_icosa_tri(lat, lon)
    x, y = sllxy(lat, lon, tri)
    return tri, x, y


def locate_cell(lon_deg: float, lat_deg: float, res: int) -> tuple[int, int, int]:
    """Locate the Q2DI (quad, i, j) for a WGS84 (lon, lat) degree point.

    Thin re-export of :func:`grid_core.app.engines.isea4h.addressing.locate_cell`.
    Imported lazily to avoid an import cycle (addressing imports projection).
    """
    from grid_core.app.engines.isea4h.addressing import locate_cell as _lc

    return _lc(lon_deg, lat_deg, res)


def snyder_inv(tri: int, x: float, y: float) -> tuple[float, float]:
    """Port of ``snyderInv``: (triangle, x, y) -> (lat, lon) radians."""
    clat, clon = TRICEN[tri]
    sin_lat_c = math.sin(clat)
    cos_lat_c = math.cos(clat)
    ddazh = DAZH[tri]

    px = x * ICOSA_EDGE - ORIGIN_X_OFF
    py = y * ICOSA_EDGE - ORIGIN_Y_OFF

    if abs(px) < PRECISION and abs(py) < PRECISION:
        return (clat, clon)

    ph = math.sqrt(px * px + py * py)
    azh1 = math.atan2(px, py)
    if azh1 < 0.0:
        azh1 += M_2PI
    azh0 = azh1
    if _120 < azh1 <= _240:
        azh1 -= _120
    if azh1 > _240:
        azh1 -= _240
    azh = azh1

    if abs(azh1) > PRECISION:
        agh = R1S * TAN_DH * TAN_DH / (2.0 * (1.0 / math.tan(azh1) + COT30))
        dazh = 1.0
        while abs(dazh) > PRECISION:
            h = math.acos(math.sin(azh) * SIN_GH * COS_DH - math.cos(azh) * COS_GH)
            fazh = agh - azh - (36.0 * _D2R) - h + M_PI
            flazh = (
                (math.cos(azh) * SIN_GH * COS_DH + math.sin(azh) * COS_GH) / math.sin(h)
            ) - 1.0
            dazh = -fazh / flazh
            azh += dazh
    else:
        azh = azh1 = 0.0

    dz = math.atan2(TAN_DH, math.cos(azh) + COT30 * math.sin(azh))
    fh = TAN_DH / (2.0 * (math.cos(azh1) + COT30 * math.sin(azh1)) * math.sin(dz / 2.0))
    z = 2.0 * math.asin(ph / (2.0 * R1 * fh))

    if _120 <= azh0 < _240:
        azh += _120
    if azh0 >= _240:
        azh += _240

    azh += ddazh
    while azh <= -M_PI:
        azh += M_2PI
    while azh > M_PI:
        azh -= M_2PI

    sin_lat = sin_lat_c * math.cos(z) + cos_lat_c * math.sin(z) * math.cos(azh)
    sin_lat = max(-1.0, min(1.0, sin_lat))
    lat = math.asin(sin_lat)

    if abs(abs(lat) - M_PI_2) < 1e-9:
        lat = M_PI_2 if lat > 0.0 else -M_PI_2
        lon = 0.0
    else:
        sin_lon = math.sin(azh) * math.sin(z) / math.cos(lat)
        cos_lon = (math.cos(z) - sin_lat_c * math.sin(lat)) / cos_lat_c / math.cos(lat)
        sin_lon = max(-1.0, min(1.0, sin_lon))
        cos_lon = max(-1.0, min(1.0, cos_lon))
        lon = clon + math.atan2(sin_lon, cos_lon)
        if lon <= -M_PI:
            lon += M_2PI
        if lon >= M_PI:
            lon -= M_2PI
    return (lat, lon)
