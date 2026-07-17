"""Cross-domain MGRS topology: neighbors, parent, children via geometry intersection."""
from __future__ import annotations

import mgrs as mgrs_lib

from grid_core.app.core.exceptions import ValidationError
from grid_core.app.engines.mgrs.address import (
    canonicalize_mgrs,
    direct_child_space_codes,
    parent_space_code,
    precision_from_code,
)
from grid_core.app.engines.mgrs.domain import GridDomain
from grid_core.app.engines.mgrs.geometry import (
    cell_geometry_clipped,
)
from grid_core.app.engines.mgrs.projection import cell_size_metres, projected_to_wgs84
from grid_core.app.models.grid_address import GridAddress

_converter = mgrs_lib.MGRS()


def address_for_code(code: str, domain: GridDomain) -> GridAddress:
    """Build a GridAddress for a canonical MGRS code in the given domain."""
    canonical = canonicalize_mgrs(code)
    precision = precision_from_code(canonical)
    return GridAddress(
        grid_type="mgrs",
        grid_level=precision,
        space_code=canonical,
        topology_code=None,
    )


def neighbors_for_address(address: GridAddress, k: int = 1) -> list[GridAddress]:
    """Return k-ring MGRS neighbors via geometry boundary intersection.

    Two cells are neighbors only when their clipped boundaries share a line
    segment whose geodesic length exceeds the tolerance (not just a point).
    Cross-domain cells are included; irregular counts are valid.
    """
    if k < 1:
        raise ValidationError("k must be >= 1")

    precision = address.grid_level
    code = address.space_code
    domain = _domain_for_address(code)

    try:
        source_geom = cell_geometry_clipped(code, precision, domain)
    except ValidationError:
        return []

    size_m = cell_size_metres(precision)
    nominal_edge_m = size_m
    tolerance_m = max(1e-6, nominal_edge_m * 1e-8)

    # Convert tolerance from metres to approximate degrees
    tolerance_deg = tolerance_m / 111_000.0

    # Enumerate candidate cells: expand bounding box by k cells in each direction
    candidate_codes: set[str] = set()
    _collect_utm_candidates(code, precision, domain, k, candidate_codes)

    results: dict[str, GridAddress] = {}
    for cand_code in candidate_codes:
        if cand_code == code:
            continue
        try:
            cand_domain = _domain_for_address(cand_code)
            cand_geom = cell_geometry_clipped(cand_code, precision, cand_domain)
        except ValidationError:
            continue

        shared = source_geom.intersection(cand_geom)
        if shared.is_empty:
            continue

        # Must share a line (not just a point) — check that shared length > tolerance
        if shared.length < tolerance_deg:
            continue

        addr = address_for_code(cand_code, cand_domain)
        results.setdefault(addr.space_code, addr)

    return sorted(results.values(), key=lambda a: a.space_code)


def _collect_utm_candidates(
    code: str,
    precision: int,
    domain: GridDomain,
    k: int,
    out: set[str],
) -> None:
    """Enumerate candidate neighbor codes via UTM offset arithmetic."""
    if domain.kind != "utm":
        _collect_ups_candidates(code, precision, domain, k, out)
        return

    try:
        zone, hemisphere, easting, northing = _converter.MGRSToUTM(code)
    except Exception:
        return

    size_m = cell_size_metres(precision)
    for dx in range(-k, k + 1):
        for dy in range(-k, k + 1):
            if dx == 0 and dy == 0:
                continue
            cand_e = easting + dx * size_m
            cand_n = northing + dy * size_m
            # Try same zone first
            for cand_zone in _candidate_zones(zone, dx):
                for cand_hemi in _candidate_hemispheres(hemisphere, northing, dy, size_m):
                    try:
                        cand_code = _converter.UTMToMGRS(
                            cand_zone,
                            cand_hemi,
                            cand_e,
                            cand_n,
                            MGRSPrecision=precision,
                        )
                        if precision_from_code(cand_code) == precision:
                            out.add(cand_code)
                    except Exception:
                        pass

    # Also try the k=1 grid in adjacent UTM zones via lon/lat conversion
    transformer = projected_to_wgs84(domain)
    for dx in range(-k, k + 1):
        for dy in range(-k, k + 1):
            cand_e = easting + dx * size_m + size_m / 2
            cand_n = northing + dy * size_m + size_m / 2
            try:
                lon, lat = transformer.transform(cand_e, cand_n)
                if -90.0 < lat < 90.0:
                    mgrs_code = _converter.toMGRS(lat, lon, MGRSPrecision=precision)
                    canonical = mgrs_code.replace(" ", "").upper()
                    out.add(canonical)
            except Exception:
                pass


def _collect_ups_candidates(
    code: str,
    precision: int,
    domain: GridDomain,
    k: int,
    out: set[str],
) -> None:
    """Enumerate candidate UPS neighbor codes via center lat/lon and grid offsets."""
    from grid_core.app.engines.mgrs.projection import wgs84_to_projected

    try:
        lat, lon = _converter.toLatLon(code)
    except Exception:
        return

    transformer_inv = wgs84_to_projected(domain)
    transformer_fwd = projected_to_wgs84(domain)
    size_m = cell_size_metres(precision)

    try:
        cx, cy = transformer_inv.transform(lon, lat)
    except Exception:
        return

    for dx in range(-k, k + 1):
        for dy in range(-k, k + 1):
            if dx == 0 and dy == 0:
                continue
            try:
                cand_x = cx + dx * size_m
                cand_y = cy + dy * size_m
                cand_lon, cand_lat = transformer_fwd.transform(cand_x, cand_y)
                if -90.0 <= cand_lat <= 90.0:
                    cand_code = _converter.toMGRS(cand_lat, cand_lon, MGRSPrecision=precision)
                    canonical = cand_code.replace(" ", "").upper()
                    out.add(canonical)
            except Exception:
                pass


def _candidate_zones(zone: int, dx: int) -> list[int]:
    """Return UTM zone candidates given a horizontal offset."""
    zones = [zone]
    if dx > 0 and zone < 60:
        zones.append(zone + 1)
    elif dx < 0 and zone > 1:
        zones.append(zone - 1)
    # Handle antimeridian wrap
    if zone == 60 and dx > 0:
        zones.append(1)
    if zone == 1 and dx < 0:
        zones.append(60)
    return zones


def _candidate_hemispheres(
    hemisphere: str, northing: float, dy: int, size_m: float
) -> list[str]:
    """Return hemisphere candidates given a vertical offset."""
    hemis = [hemisphere]
    # Near the equator, cells may straddle hemispheres
    if hemisphere == "N" and dy < 0 and northing + dy * size_m < 0:
        hemis.append("S")
    elif hemisphere == "S" and dy > 0 and northing + dy * size_m > 0:
        hemis.append("N")
    return hemis


def parent_address(address: GridAddress) -> GridAddress:
    """Return the parent MGRS cell at precision - 1.

    Raises ValidationError if already at precision 0.
    """
    precision = address.grid_level
    if precision <= 0:
        raise ValidationError("MGRS precision 0 has no parent")
    parent_code = parent_space_code(address.space_code)
    domain = _domain_for_address(parent_code)
    return address_for_code(parent_code, domain)


def children_addresses(address: GridAddress, target_level: int) -> list[GridAddress]:
    """Return all child MGRS addresses at target_level > current level.

    Children are the 100 direct numeric sub-divisions (10×10 grid) applied
    iteratively for multi-level descent.
    """
    current_level = address.grid_level
    if target_level <= current_level:
        raise ValidationError(
            f"target_level {target_level} must be greater than current MGRS level {current_level}"
        )
    if target_level > 5:
        raise ValidationError(f"target_level {target_level} exceeds maximum MGRS precision 5")

    codes = [address.space_code]
    for _ in range(target_level - current_level):
        codes = [child for prefix in codes for child in direct_child_space_codes(prefix)]

    result = []
    for code in codes:
        try:
            domain = _domain_for_address(code)
            cell_geometry_clipped(code, target_level, domain)
            result.append(address_for_code(code, domain))
        except ValidationError:
            pass
    return result


def _domain_for_address(code: str) -> GridDomain:
    """Derive the fixed UTM/UPS domain encoded by a standard MGRS code."""
    try:
        zone, hemisphere, _, _ = _converter.MGRSToUTM(code)
        return GridDomain(kind="utm", zone=zone, hemisphere=hemisphere.lower())  # type: ignore[arg-type]
    except Exception:
        pass

    first = canonicalize_mgrs(code)[0]
    if first in {"Y", "Z"}:
        return GridDomain(kind="ups", zone=None, hemisphere="n")
    if first in {"A", "B"}:
        return GridDomain(kind="ups", zone=None, hemisphere="s")
    raise ValidationError(f"Cannot determine domain for MGRS code: {code!r}")
