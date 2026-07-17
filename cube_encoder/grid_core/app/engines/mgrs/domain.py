"""UTM/UPS domain assignment and valid-domain polygon construction."""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Literal

from shapely.geometry import MultiPolygon, Polygon, box
from shapely.ops import unary_union

# -------------------------------------------------------------------------
# Latitude thresholds (half-open boundary policy)
# - UPS north : lat >= 84
# - UPS south : lat <= -80
# - UTM       : -80 < lat < 84
# -------------------------------------------------------------------------
UPS_NORTH_LAT = 84.0
UPS_SOUTH_LAT = -80.0


@dataclass(frozen=True)
class GridDomain:
    kind: Literal["utm", "ups"]
    zone: int | None          # 1-60 for UTM, None for UPS
    hemisphere: Literal["n", "s"]

    @property
    def token(self) -> str:
        if self.kind == "ups":
            return f"ups-{self.hemisphere}"
        return f"utm-{self.zone}{self.hemisphere}"

    @property
    def epsg(self) -> int:
        """Return the appropriate projected CRS EPSG code."""
        if self.kind == "ups":
            return 32661 if self.hemisphere == "n" else 32761
        base = 32600 if self.hemisphere == "n" else 32700
        assert self.zone is not None
        return base + self.zone


def _normalize_lon(lon: float) -> float:
    """Map lon=180 → -180; keep everything else unchanged."""
    return -180.0 if lon == 180.0 else lon


def _utm_zone_for_lon_lat(lon: float, lat: float) -> int:
    """Return the UTM zone number for (lon, lat), applying Norway/Svalbard exceptions."""
    # Standard 6-degree zones
    zone = int((lon + 180.0) / 6.0) % 60 + 1

    # Norway exception: zone 32V covers 3°E–12°E at 56°N–64°N
    if 56.0 <= lat < 64.0 and 3.0 <= lon < 12.0:
        return 32

    # Svalbard exceptions at 72°N–84°N
    if 72.0 <= lat < 84.0:
        if 0.0 <= lon < 9.0:
            return 31
        if 9.0 <= lon < 21.0:
            return 33
        if 21.0 <= lon < 33.0:
            return 35
        if 33.0 <= lon < 42.0:
            return 37

    return zone


def domain_for_point(lon: float, lat: float) -> GridDomain:
    """Return the canonical UTM/UPS domain for a WGS84 point.

    Boundary policy (half-open):
    - lat >= 84  → UPS north
    - lat <= -80 → UPS south
    - otherwise  → UTM, with lon=180 treated as -180
    """
    if lat >= UPS_NORTH_LAT:
        return GridDomain(kind="ups", zone=None, hemisphere="n")
    if lat <= UPS_SOUTH_LAT:
        return GridDomain(kind="ups", zone=None, hemisphere="s")

    norm_lon = _normalize_lon(lon)
    hemisphere: Literal["n", "s"] = "n" if lat >= 0.0 else "s"
    zone = _utm_zone_for_lon_lat(norm_lon, lat)
    return GridDomain(kind="utm", zone=zone, hemisphere=hemisphere)


@lru_cache(maxsize=256)
def domain_polygon(domain: GridDomain) -> Polygon | MultiPolygon:
    """Return the WGS84 valid-domain polygon for a UTM/UPS domain.

    UTM: 6-degree-wide (with Norway/Svalbard exceptions) strip from -80 to 84.
    UPS north: full disk above 84°N as a longitude-spanning polygon.
    UPS south: full disk below -80°S as a longitude-spanning polygon.

    All polygons are in EPSG:4326 [lon, lat] coordinate order.
    Antimeridian-crossing zones are split into two halves joined as MultiPolygon.
    """
    if domain.kind == "ups":
        if domain.hemisphere == "n":
            return box(-180.0, UPS_NORTH_LAT, 180.0, 90.0)
        else:
            return box(-180.0, -90.0, 180.0, UPS_SOUTH_LAT)

    # UTM domain polygon
    zone = domain.zone
    assert zone is not None
    lat_south = UPS_SOUTH_LAT    # -80
    lat_north = UPS_NORTH_LAT    # 84

    lon_west = -180.0 + (zone - 1) * 6.0
    domain_geometry = box(lon_west, lat_south, lon_west + 6.0, lat_north)

    # Standard MGRS uses mutually exclusive Norway and Svalbard exceptions.
    # Reassign only the documented latitude bands instead of widening a zone
    # across the full UTM latitude range.
    exceptions = (
        (box(3.0, 56.0, 12.0, 64.0), 32),
        (box(0.0, 72.0, 9.0, 84.0), 31),
        (box(9.0, 72.0, 21.0, 84.0), 33),
        (box(21.0, 72.0, 33.0, 84.0), 35),
        (box(33.0, 72.0, 42.0, 84.0), 37),
    )
    additions = []
    for exception_geometry, owner_zone in exceptions:
        domain_geometry = domain_geometry.difference(exception_geometry)
        if zone == owner_zone:
            additions.append(exception_geometry)
    if additions:
        domain_geometry = unary_union([domain_geometry, *additions])
    return domain_geometry

def all_utm_domains() -> list[GridDomain]:
    """Return all 120 standard UTM domains (60 zones × north/south)."""
    domains = []
    for zone in range(1, 61):
        for hemi in ("n", "s"):
            domains.append(GridDomain(kind="utm", zone=zone, hemisphere=hemi))  # type: ignore[arg-type]
    return domains
