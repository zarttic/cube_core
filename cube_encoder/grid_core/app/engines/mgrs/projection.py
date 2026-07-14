"""Projection adapters for UTM/UPS domains using pyproj."""
from __future__ import annotations

from functools import lru_cache

from pyproj import Transformer

from grid_core.app.engines.mgrs.domain import GridDomain


@lru_cache(maxsize=256)
def projected_to_wgs84(domain: GridDomain) -> Transformer:
    """Return a cached pyproj Transformer from the domain's CRS to WGS84."""
    return Transformer.from_crs(f"EPSG:{domain.epsg}", "EPSG:4326", always_xy=True)


@lru_cache(maxsize=256)
def wgs84_to_projected(domain: GridDomain) -> Transformer:
    """Return a cached pyproj Transformer from WGS84 to the domain's projected CRS."""
    return Transformer.from_crs("EPSG:4326", f"EPSG:{domain.epsg}", always_xy=True)


def cell_size_metres(precision: int) -> float:
    """Return the MGRS cell edge length in metres for the given precision (0-5)."""
    return 10.0 ** (5 - precision)
