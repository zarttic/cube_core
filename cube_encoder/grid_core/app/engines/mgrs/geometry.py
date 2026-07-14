"""Clipped MGRS cell geometry: project square → densify → inverse-project → clip to domain."""
from __future__ import annotations

from functools import lru_cache

import mgrs as mgrs_lib
from pyproj import Transformer
from shapely.geometry import MultiPolygon, Polygon, mapping, shape
from shapely.ops import unary_union
from shapely.validation import make_valid

from grid_core.app.core.exceptions import ValidationError
from grid_core.app.engines.mgrs.domain import GridDomain, domain_polygon
from grid_core.app.engines.mgrs.projection import cell_size_metres, projected_to_wgs84, wgs84_to_projected
from grid_core.app.utils.geometry import normalize_ring_longitudes

_converter = mgrs_lib.MGRS()

# Number of densification segments per projected edge (controls inverse-projection accuracy)
_EDGE_SEGMENTS = 8


def _densify_projected_edge(
    x0: float, y0: float, x1: float, y1: float, segments: int
) -> list[tuple[float, float]]:
    """Return intermediate + end projected points along one edge."""
    pts = []
    for i in range(1, segments + 1):
        t = i / segments
        pts.append((x0 + (x1 - x0) * t, y0 + (y1 - y0) * t))
    return pts


def _projected_ring_to_wgs84(
    ring_xy: list[tuple[float, float]],
    transformer: Transformer,
) -> list[tuple[float, float]]:
    """Convert a densified projected ring to WGS84 (lon, lat) tuples."""
    wgs = []
    for x, y in ring_xy:
        lon, lat = transformer.transform(x, y)
        wgs.append((lon, lat))
    return wgs


def _build_cell_projected_ring(
    easting: float,
    northing: float,
    size_m: float,
    segments: int = _EDGE_SEGMENTS,
) -> list[tuple[float, float]]:
    """Build a densified projected ring for one MGRS cell square (closed ring)."""
    corners = [
        (easting, northing),
        (easting + size_m, northing),
        (easting + size_m, northing + size_m),
        (easting, northing + size_m),
    ]
    ring: list[tuple[float, float]] = []
    n = len(corners)
    for i in range(n):
        x0, y0 = corners[i]
        x1, y1 = corners[(i + 1) % n]
        ring.append((x0, y0))
        ring.extend(_densify_projected_edge(x0, y0, x1, y1, segments)[:-1])
    return ring


def _ring_to_shapely(wgs_ring: list[tuple[float, float]]) -> Polygon:
    """Convert a WGS84 ring to a Shapely polygon, normalizing longitude continuity."""
    normalized = normalize_ring_longitudes(wgs_ring)
    coords = [(float(lon), float(lat)) for lon, lat in normalized]
    return Polygon(coords)


def _utm_raw_geometry(
    zone: int,
    hemisphere: str,
    easting: float,
    northing: float,
    precision: int,
) -> Polygon:
    """Build raw (pre-clip) geometry for a UTM MGRS cell."""
    size_m = cell_size_metres(precision)
    domain = GridDomain(kind="utm", zone=zone, hemisphere=hemisphere.lower())  # type: ignore[arg-type]
    transformer = projected_to_wgs84(domain)
    ring_xy = _build_cell_projected_ring(easting, northing, size_m)
    wgs_ring = _projected_ring_to_wgs84(ring_xy, transformer)
    poly = _ring_to_shapely(wgs_ring)
    if not poly.is_valid:
        poly = make_valid(poly)
    return poly


def _ups_raw_geometry(code: str, precision: int) -> Polygon:
    """Build raw (pre-clip) geometry for a UPS MGRS cell."""
    first_char = code[0].upper()
    if first_char in ("Y", "Z"):
        hemisphere: str = "n"
    elif first_char in ("A", "B"):
        hemisphere = "s"
    else:
        raise ValidationError(f"Cannot determine UPS hemisphere from code: {code!r}")

    domain = GridDomain(kind="ups", zone=None, hemisphere=hemisphere)  # type: ignore[arg-type]
    transformer_proj = projected_to_wgs84(domain)
    transformer_inv = wgs84_to_projected(domain)

    try:
        lat, lon = _converter.toLatLon(code)
    except Exception as exc:
        raise ValidationError(f"Cannot decode UPS MGRS code: {code!r}") from exc

    easting, northing = transformer_inv.transform(lon, lat)
    size_m = cell_size_metres(precision)
    ring_xy = _build_cell_projected_ring(easting, northing, size_m)
    wgs_ring = _projected_ring_to_wgs84(ring_xy, transformer_proj)
    poly = _ring_to_shapely(wgs_ring)
    if not poly.is_valid:
        poly = make_valid(poly)
    return poly


def cell_geometry_clipped(
    code: str, precision: int, domain: GridDomain
) -> Polygon | MultiPolygon:
    """Return WGS84 cell geometry clipped to the valid domain polygon.

    Raises ValidationError if the code cannot be decoded or the clipped result is empty.
    """
    if domain.kind == "utm":
        try:
            zone, hemisphere, easting, northing = _converter.MGRSToUTM(code)
        except Exception as exc:
            raise ValidationError(f"Cannot decode UTM MGRS code: {code!r}") from exc
        raw = _utm_raw_geometry(zone, hemisphere, easting, northing, precision)
    else:
        raw = _ups_raw_geometry(code, precision)

    valid_domain = domain_polygon(domain)
    clipped = raw.intersection(valid_domain)
    if clipped.is_empty:
        raise ValidationError(
            f"MGRS cell {code!r} has no area within domain {domain.token!r}"
        )
    if not clipped.is_valid:
        clipped = make_valid(clipped)

    if isinstance(clipped, (Polygon, MultiPolygon)):
        return clipped

    # GeometryCollection from edge-only intersections — extract polygonal parts
    polys = [
        g
        for g in clipped.geoms
        if isinstance(g, (Polygon, MultiPolygon)) and not g.is_empty
    ]
    if not polys:
        raise ValidationError(
            f"MGRS cell {code!r} clips to empty geometry in domain {domain.token!r}"
        )
    return unary_union(polys)


def cell_geometry_to_geojson(geom: Polygon | MultiPolygon) -> dict:
    """Convert a Shapely geometry to a GeoJSON-compatible dict."""
    return dict(mapping(geom))


def cell_bbox(geom: Polygon | MultiPolygon) -> list[float]:
    """Return [min_lon, min_lat, max_lon, max_lat] bounding box."""
    minx, miny, maxx, maxy = geom.bounds
    return [float(minx), float(miny), float(maxx), float(maxy)]


def cell_center(geom: Polygon | MultiPolygon) -> list[float]:
    """Return [lon, lat] representative point."""
    pt = geom.representative_point()
    return [float(pt.x), float(pt.y)]
