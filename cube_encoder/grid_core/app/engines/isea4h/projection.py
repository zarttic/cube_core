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

# The nearest face has the largest dot product with the point's unit vector.
# Keeping the triangle centres in Cartesian form avoids 20 angular-distance
# calculations for every point location.
_TRICEN_CARTESIAN = tuple(
    (
        math.cos(lat) * math.cos(lon),
        math.cos(lat) * math.sin(lon),
        math.sin(lat),
    )
    for lat, lon in TRICEN
)
_TRICEN_FORWARD = tuple(
    (math.sin(lat), math.cos(lat), lon, DAZH[index])
    for index, (lat, lon) in enumerate(TRICEN)
)


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
    cos_lat = math.cos(lat)
    px = cos_lat * math.cos(lon)
    py = cos_lat * math.sin(lon)
    pz = math.sin(lat)
    best_face = 0
    cx, cy, cz = _TRICEN_CARTESIAN[0]
    best_dot = px * cx + py * cy + pz * cz
    for index, (cx, cy, cz) in enumerate(_TRICEN_CARTESIAN[1:], start=1):
        dot = px * cx + py * cy + pz * cz
        if dot > best_dot:
            best_dot = dot
            best_face = index
    return best_face


def which_icosa_tris(latitudes: list[float], longitudes: list[float]) -> list[int]:
    """Vectorized nearest-face lookup with a scalar fallback for SDK-only installs."""
    if len(latitudes) != len(longitudes):
        raise ValueError("latitudes and longitudes must have the same length")
    if not latitudes:
        return []
    try:
        import numpy as np
    except ModuleNotFoundError:
        return [which_icosa_tri(lat, lon) for lat, lon in zip(latitudes, longitudes)]

    latitude_values = np.asarray(latitudes, dtype=float)
    longitude_values = np.asarray(longitudes, dtype=float)
    cos_latitudes = np.cos(latitude_values)
    points = np.column_stack(
        (
            cos_latitudes * np.cos(longitude_values),
            cos_latitudes * np.sin(longitude_values),
            np.sin(latitude_values),
        )
    )
    centres = np.asarray(_TRICEN_CARTESIAN)
    return [int(index) for index in np.argmax(points @ centres.T, axis=1)]


def sllxy(lat: float, lon: float, n_tri: int) -> tuple[float, float]:
    """ISEA forward projection sphere -> plane for triangle ``n_tri``.

    Port of ``sllxy`` in DgProjISEA.cpp. Returns (x, y) in DgProjTriCoord
    units (divided by ICOSA_EDGE).
    """
    sin_lat_c, cos_lat_c, clon, dazh = _TRICEN_FORWARD[n_tri]
    cos_lat = math.cos(lat)
    sin_lat = math.sin(lat)
    delta_lon = lon - clon
    cos_delta_lon = math.cos(delta_lon)

    tmp = sin_lat_c * sin_lat + cos_lat_c * cos_lat * cos_delta_lon
    tmp = max(-1.0, min(1.0, tmp))
    z = math.acos(tmp)

    azh = math.atan2(
        cos_lat * math.sin(delta_lon),
        cos_lat_c * sin_lat - sin_lat_c * cos_lat * cos_delta_lon,
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


def sllxys(
    latitudes: list[float],
    longitudes: list[float],
    triangles: list[int],
) -> list[tuple[float, float]]:
    """Batch ISEA forward projection with a scalar fallback for SDK-only installs."""
    if not (len(latitudes) == len(longitudes) == len(triangles)):
        raise ValueError("latitudes, longitudes and triangles must have the same length")
    if not latitudes:
        return []
    try:
        import numpy as np
    except ModuleNotFoundError:
        return [sllxy(lat, lon, triangle) for lat, lon, triangle in zip(latitudes, longitudes, triangles)]

    latitude_values = np.asarray(latitudes, dtype=float)
    longitude_values = np.asarray(longitudes, dtype=float)
    triangle_values = np.asarray(triangles, dtype=int)
    triangle_parameters = np.asarray(_TRICEN_FORWARD)[triangle_values]
    sin_lat_c, cos_lat_c, center_longitudes, dazhs = triangle_parameters.T
    cos_lat = np.cos(latitude_values)
    sin_lat = np.sin(latitude_values)
    delta_lon = longitude_values - center_longitudes
    cos_delta_lon = np.cos(delta_lon)
    tmp = np.clip(sin_lat_c * sin_lat + cos_lat_c * cos_lat * cos_delta_lon, -1.0, 1.0)
    z = np.arccos(tmp)
    azh = np.arctan2(
        cos_lat * np.sin(delta_lon),
        cos_lat_c * sin_lat - sin_lat_c * cos_lat * cos_delta_lon,
    ) - dazhs
    azh = np.where(azh < 0.0, azh + M_2PI, azh)
    azh0 = azh
    azh = np.where((_120 <= azh) & (azh <= _240), azh - _120, azh)
    azh = np.where(azh > _240, azh - _240, azh)

    cos_azh = np.cos(azh)
    sin_azh = np.sin(azh)
    dz = np.arctan2(TAN_DH, cos_azh + COT30 * sin_azh)
    h = np.arccos(sin_azh * SIN_GH * COS_DH - cos_azh * COS_GH)
    ag = azh + (36.0 * _D2R) + h - _180
    azh1 = np.arctan2(2.0 * ag, R1S * TAN_DH * TAN_DH - 2.0 * ag * COT30)
    fh = TAN_DH / (2.0 * (np.cos(azh1) + COT30 * np.sin(azh1)) * np.sin(dz / 2.0))
    ph = 2.0 * R1 * fh * np.sin(z / 2.0)
    azh1 = np.where((_120 <= azh0) & (azh0 < _240), azh1 + _120, azh1)
    azh1 = np.where(azh0 >= _240, azh1 + _240, azh1)
    xs = (ph * np.sin(azh1) + ORIGIN_X_OFF) / ICOSA_EDGE
    ys = (ph * np.cos(azh1) + ORIGIN_Y_OFF) / ICOSA_EDGE
    return list(zip(xs.tolist(), ys.tolist()))


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
