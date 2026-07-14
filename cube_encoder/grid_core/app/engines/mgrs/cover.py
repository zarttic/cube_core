"""Domain-split MGRS cover: intersect AOI with UTM/UPS domain polygons, enumerate cells."""
from __future__ import annotations

from collections import deque

import mgrs as mgrs_lib
from shapely.geometry import MultiPolygon, Polygon, shape
from shapely.ops import unary_union
from shapely.validation import make_valid

from grid_core.app.core.enums import CoverMode
from grid_core.app.core.exceptions import ValidationError
from grid_core.app.engines.mgrs.address import (
    build_topology_code,
    canonicalize_mgrs,
    precision_from_code,
)
from grid_core.app.engines.mgrs.domain import (
    GridDomain,
    domain_for_point,
    domain_polygon,
)
from grid_core.app.engines.mgrs.geometry import (
    cell_bbox,
    cell_center,
    cell_geometry_clipped,
    cell_geometry_to_geojson,
)
from grid_core.app.engines.mgrs.topology import address_for_code, _domain_for_address
from grid_core.app.models.compact_grid_cell import CompactGridCell
from grid_core.app.models.grid_address import GridAddress
from grid_core.app.models.grid_cell import GridCell
from grid_core.app.utils.geometry import to_shapely, wrapped_geometry_variants

_converter = mgrs_lib.MGRS()

_MAX_CELLS = 20_000


def cover_geometry(
    geometry: dict,
    precision: int,
    cover_mode: str,
) -> list[CompactGridCell]:
    """Cover a GeoJSON geometry at the given MGRS precision.

    Returns CompactGridCell list with topology_code set.
    Raises ValidationError for unsupported modes or excessive output.
    """
    if cover_mode not in {CoverMode.INTERSECT.value, CoverMode.CONTAIN.value, CoverMode.MINIMAL.value}:
        raise ValidationError(f"MGRS cover does not support cover_mode={cover_mode!r}")

    aoi = to_shapely(geometry)
    if not aoi.is_valid:
        aoi = make_valid(aoi)

    selected: dict[str, tuple[GridAddress, CompactGridCell]] = {}

    # Seed candidate codes from representative points in the AOI
    seed_codes = _seed_codes(aoi, precision)
    if not seed_codes:
        return []

    visited: set[str] = set()
    queue: deque[str] = deque(sorted(seed_codes))

    while queue:
        code = queue.popleft()
        if code in visited:
            continue
        visited.add(code)

        # Determine domain for this candidate
        try:
            domain = _domain_for_address(code)
            clipped = cell_geometry_clipped(code, precision, domain)
        except ValidationError:
            continue

        # Check intersection with AOI (handle antimeridian variants)
        aoi_variants = wrapped_geometry_variants(aoi)
        intersects = any(clipped.intersects(v) for v in aoi_variants)
        if not intersects:
            continue

        # Selection rules
        if cover_mode in {CoverMode.INTERSECT.value, CoverMode.MINIMAL.value}:
            # Intersect: positive area intersection (exclude boundary-only contact)
            for aoi_v in aoi_variants:
                inter = clipped.intersection(aoi_v)
                if not inter.is_empty and inter.area > 0.0:
                    _add_cell(code, precision, domain, clipped, selected)
                    break
        else:  # CONTAIN
            # Contain: AOI must fully cover the clipped cell
            for aoi_v in aoi_variants:
                if aoi_v.covers(clipped):
                    _add_cell(code, precision, domain, clipped, selected)
                    break

        if len(selected) > _MAX_CELLS:
            raise ValidationError(f"MGRS cover result too large (> {_MAX_CELLS} cells)")

        # Expand to neighbors using the UTM offset approach
        for nb_code in _neighbor_codes(code, precision, domain):
            if nb_code not in visited:
                queue.append(nb_code)

    if cover_mode == CoverMode.MINIMAL.value:
        selected = _coarsen_minimal(selected, aoi, precision)

    return [cell for _, cell in sorted(selected.values(), key=lambda x: x[0].topology_code or "")]


def _add_cell(
    code: str,
    precision: int,
    domain: GridDomain,
    clipped: Polygon | MultiPolygon,
    selected: dict,
) -> None:
    topo = build_topology_code(domain.token, precision, code)
    if topo in selected:
        return
    addr = GridAddress(
        grid_type="mgrs",
        grid_level=precision,
        space_code=code,
        topology_code=topo,
    )
    compact = CompactGridCell(
        grid_type="mgrs",
        grid_level=precision,
        space_code=code,
        topology_code=topo,
        bbox=cell_bbox(clipped),
    )
    selected[topo] = (addr, compact)


def _seed_codes(aoi: Polygon | MultiPolygon, precision: int) -> set[str]:
    """Get seed MGRS codes from representative points in the AOI."""
    codes: set[str] = set()
    geoms = list(aoi.geoms) if hasattr(aoi, "geoms") else [aoi]
    for geom in geoms:
        rp = geom.representative_point()
        _try_add_code(float(rp.x), float(rp.y), precision, codes)
        minx, miny, maxx, maxy = geom.bounds
        midx = (minx + maxx) / 2
        midy = (miny + maxy) / 2
        for lon, lat in [
            (minx, miny), (minx, maxy), (maxx, miny), (maxx, maxy),
            (midx, miny), (midx, maxy), (minx, midy), (maxx, midy),
            (midx, midy),
        ]:
            if -90.0 < lat < 90.0:
                _try_add_code(lon, lat, precision, codes)
    return codes


def _try_add_code(lon: float, lat: float, precision: int, out: set[str]) -> None:
    """Try to encode lat/lon to MGRS and add to the set."""
    try:
        code = _converter.toMGRS(lat, lon, MGRSPrecision=precision)
        out.add(code.replace(" ", "").upper())
    except Exception:
        pass


def _neighbor_codes(code: str, precision: int, domain: GridDomain) -> list[str]:
    """Return immediate neighbor candidate codes (k=1 grid expansion)."""
    from grid_core.app.engines.mgrs.projection import cell_size_metres, projected_to_wgs84

    candidates: set[str] = set()
    size_m = cell_size_metres(precision)

    if domain.kind == "utm":
        try:
            zone, hemisphere, easting, northing = _converter.MGRSToUTM(code)
        except Exception:
            return []

        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                if dx == 0 and dy == 0:
                    continue
                try:
                    cand = _converter.UTMToMGRS(
                        zone, hemisphere,
                        easting + dx * size_m,
                        northing + dy * size_m,
                        MGRSPrecision=precision,
                    )
                    candidates.add(cand.replace(" ", "").upper())
                except Exception:
                    pass

        # Cross-zone: also try via lat/lon
        fwd = projected_to_wgs84(domain)
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                if dx == 0 and dy == 0:
                    continue
                try:
                    lon, lat = fwd.transform(
                        easting + dx * size_m + size_m / 2,
                        northing + dy * size_m + size_m / 2,
                    )
                    if -90.0 < lat < 90.0:
                        cand = _converter.toMGRS(lat, lon, MGRSPrecision=precision)
                        candidates.add(cand.replace(" ", "").upper())
                except Exception:
                    pass
    else:
        # UPS: offset via polar projection
        from grid_core.app.engines.mgrs.projection import wgs84_to_projected
        try:
            lat, lon = _converter.toLatLon(code)
        except Exception:
            return []
        try:
            inv = wgs84_to_projected(domain)
            fwd = projected_to_wgs84(domain)
            cx, cy = inv.transform(lon, lat)
        except Exception:
            return []
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                if dx == 0 and dy == 0:
                    continue
                try:
                    cand_lon, cand_lat = fwd.transform(cx + dx * size_m, cy + dy * size_m)
                    if -90.0 <= cand_lat <= 90.0:
                        cand = _converter.toMGRS(cand_lat, cand_lon, MGRSPrecision=precision)
                        candidates.add(cand.replace(" ", "").upper())
                except Exception:
                    pass

    return list(candidates)


def _coarsen_minimal(
    selected: dict,
    aoi: Polygon | MultiPolygon,
    precision: int,
) -> dict:
    """Coarsen the selected set by replacing complete 100-child groups with their parent.

    Only merges cells within the same domain (same domain token in topology_code).
    """
    if precision == 0:
        return selected

    out = dict(selected)
    changed = True
    while changed:
        changed = False
        grouped: dict[tuple[str, str], set[str]] = {}  # (domain_token, parent_code) -> set of topo keys

        for topo_key, (addr, _) in out.items():
            if addr.grid_level <= 0:
                continue
            parent_code = addr.space_code[:-2]
            domain_token = (addr.topology_code or "").split(":")[1] if addr.topology_code else ""
            key = (domain_token, parent_code)
            grouped.setdefault(key, set()).add(topo_key)

        for (domain_token, parent_code), topo_keys in grouped.items():
            if len(topo_keys) != 100:
                continue
            # All 100 children in same domain → replace with parent
            try:
                parent_domain = _domain_for_address(parent_code)
                if parent_domain.token != domain_token:
                    continue
                parent_clipped = cell_geometry_clipped(parent_code, precision - 1, parent_domain)
            except ValidationError:
                continue

            # Verify parent is actually covered by AOI
            aoi_variants = wrapped_geometry_variants(aoi)
            parent_covered = any(v.covers(parent_clipped) for v in aoi_variants)
            if not parent_covered:
                continue

            # Replace children with parent
            for topo_key in topo_keys:
                del out[topo_key]
            _add_cell(parent_code, precision - 1, parent_domain, parent_clipped, out)
            changed = True

    return out
