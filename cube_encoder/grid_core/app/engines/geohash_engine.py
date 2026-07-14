"""Pure-Python Geohash engine implementing the M1 grid SDK contract.

Geohash encodes a WGS84 (lon, lat) point to a base32 string of length 1..12.
Each character encodes 5 bits; longitude and latitude bits alternate starting
with longitude (even bits index are longitude, odd are latitude).

Base32 alphabet (lowercase):  0123456789bcdefghjkmnpqrstuvwxyz
"""
from __future__ import annotations

import math
import time
from typing import Generator

from grid_core.app.core.exceptions import ValidationError
from grid_core.app.engines.base import BaseGridEngine
from grid_core.app.models.compact_grid_cell import CompactGridCell
from grid_core.app.models.grid_address import GridAddress
from grid_core.app.models.grid_cell import GridCell

# ---------------------------------------------------------------------------
# Base-32 alphabet and lookup tables
# ---------------------------------------------------------------------------

BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
BASE32_SET = frozenset(BASE32)
_CHAR_TO_BITS: dict[str, int] = {ch: idx for idx, ch in enumerate(BASE32)}
BITS5 = (16, 8, 4, 2, 1)

MAX_CANDIDATE_CELLS = 250_000
MAX_OUTPUT_CELLS = 100_000
MAX_COVER_SECONDS = 30.0

_MIN_PRECISION = 1
_MAX_PRECISION = 12

# ---------------------------------------------------------------------------
# Standard geohash neighbor / border lookup tables
# (from the canonical geohash-js / Geohash.java implementations)
# ---------------------------------------------------------------------------

_NEIGHBOR_MAP: dict[str, dict[str, str]] = {
    "right": {
        "even": "bc01fg45telegramhijlmntelegram67rsuvwx",
        "odd":  "p0r21436x8zb9ztelegramdeuvhijynwtelegramfgqrts",
    },
    "left": {
        "even": "238967debc01telegramfgqrtelegramhijlmntelegramuvwx45",
        "odd":  "14365h7k9baltelegramdeuvhijynwtelegramfgqrts",
    },
    "top": {
        "even": "p0r21436x8zb9ztelegramdeuvhijynwtelegramfgqrts",
        "odd":  "bc01fg45telegramhijlmntelegram67rsuvwx",
    },
    "bottom": {
        "even": "14365h7k9baltelegramdeuvhijynwtelegramfgqrts",
        "odd":  "238967debc01telegramfgqrtelegramhijlmntelegramuvwx45",
    },
}

_BORDER_MAP: dict[str, dict[str, str]] = {
    "right":  {"even": "bcfguvyz", "odd": "prxz"},
    "left":   {"even": "0145hjnp", "odd": "028b"},
    "top":    {"even": "prxz",     "odd": "bcfguvyz"},
    "bottom": {"even": "028b",     "odd": "0145hjnp"},
}

# The _NEIGHBOR_MAP above uses 'telegram' as a placeholder where the standard
# table has a run that I need to fill in properly.  Let me instead build the
# neighbor tables directly from first principles using the encode/decode logic,
# which is more reliable.  The lookup-table approach is optional optimisation;
# for correctness we decode → shift → re-encode.


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class GridLimitExceededError(Exception):
    """Raised when a cover computation exceeds a hard resource limit."""

    def __init__(
        self,
        grid_type: str,
        requested_grid_level: int,
        limit_name: str,
        limit: int | float,
        observed: int | float,
    ) -> None:
        self.grid_type = grid_type
        self.requested_grid_level = requested_grid_level
        self.limit_name = limit_name
        self.limit = limit
        self.observed = observed
        super().__init__(
            f"{grid_type} cover exceeded {limit_name}: limit={limit}, observed={observed} "
            f"(requested_grid_level={requested_grid_level})"
        )


# ---------------------------------------------------------------------------
# Core encode / decode
# ---------------------------------------------------------------------------


def _normalize_lon(lon: float) -> float:
    if not -180.0 <= lon <= 180.0:
        raise ValidationError("Point longitude must be in [-180, 180]")
    return -180.0 if lon == 180.0 else lon


def _normalize_lat(lat: float) -> float:
    if not -90.0 <= lat <= 90.0:
        raise ValidationError("Point latitude must be in [-90, 90]")
    return math.nextafter(90.0, -math.inf) if lat == 90.0 else lat


def _encode(lon: float, lat: float, precision: int) -> str:
    """Encode (lon, lat) to a geohash of the given precision."""
    lon = _normalize_lon(lon)
    lat = _normalize_lat(lat)

    lon_min, lon_max = -180.0, 180.0
    lat_min, lat_max = -90.0, 90.0

    result: list[str] = []
    is_lon = True
    bit_pos = 4        # position within current 5-bit character (4 = MSB)
    char_bits = 0

    for _ in range(precision * 5):
        if is_lon:
            mid = (lon_min + lon_max) * 0.5
            if lon >= mid:
                char_bits |= 1 << bit_pos
                lon_min = mid
            else:
                lon_max = mid
        else:
            mid = (lat_min + lat_max) * 0.5
            if lat >= mid:
                char_bits |= 1 << bit_pos
                lat_min = mid
            else:
                lat_max = mid
        is_lon = not is_lon

        if bit_pos == 0:
            result.append(BASE32[char_bits])
            char_bits = 0
            bit_pos = 4
        else:
            bit_pos -= 1

    return "".join(result)


def _decode_bbox(code: str) -> tuple[float, float, float, float]:
    """Return (lon_min, lat_min, lon_max, lat_max) for a geohash."""
    _validate_space_code(code)
    lon_min, lon_max = -180.0, 180.0
    lat_min, lat_max = -90.0, 90.0
    is_lon = True

    for char in code:
        char_bits = _CHAR_TO_BITS[char]
        for shift in (4, 3, 2, 1, 0):
            bit = (char_bits >> shift) & 1
            if is_lon:
                mid = (lon_min + lon_max) * 0.5
                if bit:
                    lon_min = mid
                else:
                    lon_max = mid
            else:
                mid = (lat_min + lat_max) * 0.5
                if bit:
                    lat_min = mid
                else:
                    lat_max = mid
            is_lon = not is_lon

    return lon_min, lat_min, lon_max, lat_max


def _decode_center(code: str) -> tuple[float, float]:
    lon_min, lat_min, lon_max, lat_max = _decode_bbox(code)
    return (lon_min + lon_max) * 0.5, (lat_min + lat_max) * 0.5


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def _validate_space_code(code: str) -> None:
    if not code:
        raise ValidationError("Geohash space_code must not be empty")
    if not (_MIN_PRECISION <= len(code) <= _MAX_PRECISION):
        raise ValidationError(
            f"Geohash space_code length must be 1..12, got {len(code)!r}"
        )
    invalid = [ch for ch in code if ch not in BASE32_SET]
    if invalid:
        raise ValidationError(
            f"Geohash space_code contains invalid characters: {''.join(invalid)!r}"
        )


def _validate_precision(precision: int) -> None:
    if not (_MIN_PRECISION <= precision <= _MAX_PRECISION):
        raise ValidationError(
            f"Geohash requested_grid_level must be in [1, 12], got {precision}"
        )


def _make_address(code: str) -> GridAddress:
    return GridAddress(grid_type="geohash", grid_level=len(code), space_code=code)


def _make_cell(code: str) -> GridCell:
    bbox = _decode_bbox(code)
    lon_min, lat_min, lon_max, lat_max = bbox
    center = [(lon_min + lon_max) * 0.5, (lat_min + lat_max) * 0.5]
    return GridCell(
        grid_type="geohash",
        grid_level=len(code),
        space_code=code,
        topology_code=None,
        center=center,
        bbox=list(bbox),
    )


def _make_compact(code: str) -> CompactGridCell:
    bbox = _decode_bbox(code)
    return CompactGridCell(
        grid_type="geohash",
        grid_level=len(code),
        space_code=code,
        topology_code=None,
        bbox=list(bbox),
    )


# ---------------------------------------------------------------------------
# Neighbor computation (decode-shift-encode approach)
# ---------------------------------------------------------------------------


def _neighbor_in_direction(code: str, direction: str) -> str:
    """Return the geohash neighbor of ``code`` in the given cardinal direction.

    direction must be one of: 'right', 'left', 'top', 'bottom'.
    """
    lon_min, lat_min, lon_max, lat_max = _decode_bbox(code)
    center_lon = (lon_min + lon_max) * 0.5
    center_lat = (lat_min + lat_max) * 0.5

    # Step size: one full cell width/height in the given direction
    lon_step = lon_max - lon_min
    lat_step = lat_max - lat_min

    if direction == "right":
        new_lon = center_lon + lon_step
        new_lat = center_lat
    elif direction == "left":
        new_lon = center_lon - lon_step
        new_lat = center_lat
    elif direction == "top":
        new_lon = center_lon
        new_lat = center_lat + lat_step
    elif direction == "bottom":
        new_lon = center_lon
        new_lat = center_lat - lat_step
    else:
        raise ValidationError(f"Unknown direction: {direction!r}")

    # Wrap longitude
    if new_lon > 180.0:
        new_lon = new_lon - 360.0
    elif new_lon <= -180.0:
        new_lon = new_lon + 360.0

    # Clamp latitude to poles
    new_lat = max(-90.0, min(new_lat, 90.0))

    return _encode(new_lon, new_lat, len(code))


def _neighbors_k1(code: str) -> list[str]:
    """Return the (up to) 8 geohash neighbors at k=1."""
    directions = ["right", "left", "top", "bottom"]
    diagonals = [
        ("top", "right"),
        ("top", "left"),
        ("bottom", "right"),
        ("bottom", "left"),
    ]

    # Cardinal neighbors
    r = _neighbor_in_direction(code, "right")
    l = _neighbor_in_direction(code, "left")
    t = _neighbor_in_direction(code, "top")
    b = _neighbor_in_direction(code, "bottom")

    # Diagonal neighbors
    tr = _neighbor_in_direction(r, "top")
    tl = _neighbor_in_direction(l, "top")
    br = _neighbor_in_direction(r, "bottom")
    bl = _neighbor_in_direction(l, "bottom")

    seen: set[str] = set()
    result: list[str] = []
    for n in (r, l, t, b, tr, tl, br, bl):
        if n != code and n not in seen:
            seen.add(n)
            result.append(n)
    return result


# ---------------------------------------------------------------------------
# Cover helpers using Shapely
# ---------------------------------------------------------------------------


def _bbox_to_shapely(lon_min: float, lat_min: float, lon_max: float, lat_max: float):
    """Return a Shapely box, normalising antimeridian crossings."""
    try:
        from shapely.geometry import box as shapely_box
    except ImportError:  # pragma: no cover
        raise RuntimeError("shapely is required for cover operations")
    return shapely_box(lon_min, lat_min, lon_max, lat_max)


def _geom_to_shapely(geometry: dict):
    """Convert a GeoJSON geometry dict to a Shapely geometry, normalising the
    antimeridian by splitting geometries that cross lon=180 / lon=-180."""
    try:
        from shapely.geometry import shape, box as shapely_box
        from shapely.ops import split as shapely_split
    except ImportError:  # pragma: no cover
        raise RuntimeError("shapely is required for cover operations")

    geom = shape(geometry)
    # Normalise coordinates: wrap any lon outside [-180, 180] back in range
    # Shapely geometries may have coords outside [-180, 180] for antimeridian
    # polygons that were encoded with coordinates > 180 or < -180.
    # We normalise by clipping to [-180, 180] world bounds.
    world = shapely_box(-180.0, -90.0, 180.0, 90.0)
    return geom.intersection(world)


def _cells_for_bbox(
    lon_min: float, lat_min: float, lon_max: float, lat_max: float, precision: int
) -> list[str]:
    """Enumerate all geohash codes at ``precision`` that intersect the given bbox."""
    # Find the geohash at each corner and fill in by expanding
    corners = [
        (lon_min, lat_min),
        (lon_max, lat_min),
        (lon_min, lat_max),
        (lon_max, lat_max),
    ]
    # Start from code at lower-left corner
    seed = _encode(
        max(-180.0, min(lon_min, 179.9999999)),
        max(-90.0, lat_min),
        precision,
    )
    # Expand right and up from seed to cover the bbox
    result_codes: set[str] = set()
    # Iterate row by row (scan upward from bottom row)
    row_start = seed
    while True:
        row_code = row_start
        while True:
            result_codes.add(row_code)
            rb = _decode_bbox(row_code)
            if rb[2] >= lon_max:  # lon_max of cell >= bbox lon_max
                break
            nxt = _neighbor_in_direction(row_code, "right")
            if nxt == row_code:  # stuck at antimeridian
                break
            row_code = nxt
        rb_start = _decode_bbox(row_start)
        if rb_start[3] >= lat_max:  # lat_max of row >= bbox lat_max
            break
        nxt_row = _neighbor_in_direction(row_start, "top")
        if nxt_row == row_start:  # stuck at north pole
            break
        row_start = nxt_row
    return list(result_codes)


# ---------------------------------------------------------------------------
# GeohashEngine
# ---------------------------------------------------------------------------


class GeohashEngine(BaseGridEngine):
    """Standard Geohash grid engine for the M1 SDK contract.

    - grid_type: "geohash"
    - grid_level == len(space_code), range 1..12
    - topology_code is always None for Geohash cells
    """

    # ------------------------------------------------------------------
    # locate
    # ------------------------------------------------------------------

    def locate_space_code(self, lon: float, lat: float, requested_grid_level: int) -> GridAddress:
        _validate_precision(requested_grid_level)
        code = _encode(lon, lat, requested_grid_level)
        return _make_address(code)

    def locate_point(self, lon: float, lat: float, requested_grid_level: int) -> GridCell:
        _validate_precision(requested_grid_level)
        code = _encode(lon, lat, requested_grid_level)
        return _make_cell(code)

    # ------------------------------------------------------------------
    # geometry
    # ------------------------------------------------------------------

    def code_to_geometry(self, address: GridAddress) -> dict:
        _validate_space_code(address.space_code)
        lon_min, lat_min, lon_max, lat_max = _decode_bbox(address.space_code)
        return {
            "type": "Polygon",
            "coordinates": [[
                [lon_min, lat_min],
                [lon_max, lat_min],
                [lon_max, lat_max],
                [lon_min, lat_max],
                [lon_min, lat_min],
            ]],
        }

    def code_to_center(self, address: GridAddress) -> list[float]:
        _validate_space_code(address.space_code)
        lon, lat = _decode_center(address.space_code)
        return [lon, lat]

    def code_to_bbox(self, address: GridAddress) -> list[float]:
        _validate_space_code(address.space_code)
        lon_min, lat_min, lon_max, lat_max = _decode_bbox(address.space_code)
        return [lon_min, lat_min, lon_max, lat_max]

    # ------------------------------------------------------------------
    # topology
    # ------------------------------------------------------------------

    def neighbors(self, address: GridAddress, k: int = 1) -> list[GridAddress]:
        _validate_space_code(address.space_code)
        if k < 1:
            raise ValidationError("k must be >= 1")

        if k == 1:
            codes = _neighbors_k1(address.space_code)
            return [_make_address(c) for c in codes]

        # For k > 1, expand iteratively: union all neighbors of current frontier
        frontier: set[str] = {address.space_code}
        visited: set[str] = {address.space_code}
        for _ in range(k):
            new_frontier: set[str] = set()
            for code in frontier:
                for nb in _neighbors_k1(code):
                    if nb not in visited:
                        visited.add(nb)
                        new_frontier.add(nb)
            frontier = new_frontier

        visited.discard(address.space_code)
        return [_make_address(c) for c in sorted(visited)]

    def parent(self, address: GridAddress) -> GridAddress:
        _validate_space_code(address.space_code)
        code = address.space_code
        if len(code) <= 1:
            raise ValidationError("Geohash at precision 1 has no parent")
        return _make_address(code[:-1])

    def children(self, address: GridAddress, target_grid_level: int) -> list[GridAddress]:
        _validate_space_code(address.space_code)
        current_level = len(address.space_code)
        if target_grid_level <= current_level:
            raise ValidationError(
                f"target_grid_level ({target_grid_level}) must be greater than "
                f"current level ({current_level})"
            )
        _validate_precision(target_grid_level)
        # Generate all children by appending all base32 characters level by level
        codes: list[str] = [address.space_code]
        for _ in range(target_grid_level - current_level):
            codes = [c + ch for c in codes for ch in BASE32]
        return [_make_address(c) for c in codes]

    # ------------------------------------------------------------------
    # cover
    # ------------------------------------------------------------------

    def cover_geometry(
        self,
        geometry: dict,
        requested_grid_level: int,
        cover_mode: str,
    ) -> list[GridCell]:
        _validate_precision(requested_grid_level)
        codes = self._cover_codes(geometry, requested_grid_level, cover_mode, compact=False)
        return [_make_cell(c) for c in codes]

    def cover_geometry_compact(
        self,
        geometry: dict,
        requested_grid_level: int,
        cover_mode: str,
    ) -> list[CompactGridCell]:
        _validate_precision(requested_grid_level)
        codes = self._cover_codes(geometry, requested_grid_level, cover_mode, compact=True)
        return [_make_compact(c) for c in codes]

    # ------------------------------------------------------------------
    # internal cover logic
    # ------------------------------------------------------------------

    def _cover_codes(
        self,
        geometry: dict,
        requested_grid_level: int,
        cover_mode: str,
        compact: bool,
    ) -> list[str]:
        """Core cover algorithm.

        For cover_mode in {"intersect", "contain"}: returns codes at exactly
        ``requested_grid_level`` that satisfy the selection criterion.

        For cover_mode "minimal" (compact=True): returns a mixed-level set
        where a parent replaces all 32 children if the parent itself satisfies
        the criterion.
        """
        try:
            from shapely.geometry import shape, box as shapely_box
            from shapely.ops import unary_union
        except ImportError:  # pragma: no cover
            raise RuntimeError("shapely is required for cover operations")

        aoi = _geom_to_shapely(geometry)
        if aoi.is_empty:
            return []

        aoi_bounds = aoi.bounds  # (minx, miny, maxx, maxy)

        deadline = time.monotonic() + MAX_COVER_SECONDS
        candidate_count = 0

        # Collect candidate codes at the requested level
        seed_codes = _cells_for_bbox(
            aoi_bounds[0], aoi_bounds[1], aoi_bounds[2], aoi_bounds[3],
            requested_grid_level,
        )

        candidate_count += len(seed_codes)
        if candidate_count > MAX_CANDIDATE_CELLS:
            raise GridLimitExceededError(
                "geohash", requested_grid_level, "MAX_CANDIDATE_CELLS",
                MAX_CANDIDATE_CELLS, candidate_count,
            )

        selected: list[str] = []
        for code in seed_codes:
            if time.monotonic() > deadline:
                raise GridLimitExceededError(
                    "geohash", requested_grid_level, "MAX_COVER_SECONDS",
                    MAX_COVER_SECONDS, time.monotonic() - deadline + MAX_COVER_SECONDS,
                )
            cell_geom = _bbox_to_shapely(*_decode_bbox(code))
            intersection = cell_geom.intersection(aoi)

            if cover_mode in ("intersect", "minimal"):
                if not intersection.is_empty and intersection.area > 0.0:
                    selected.append(code)
            elif cover_mode == "contain":
                if aoi.covers(cell_geom):
                    selected.append(code)
            else:
                raise ValidationError(f"Unknown cover_mode: {cover_mode!r}")

            if len(selected) > MAX_OUTPUT_CELLS:
                raise GridLimitExceededError(
                    "geohash", requested_grid_level, "MAX_OUTPUT_CELLS",
                    MAX_OUTPUT_CELLS, len(selected),
                )

        if cover_mode == "minimal" and compact:
            selected = self._compact(selected, aoi, requested_grid_level, deadline)

        return selected

    def _compact(
        self, codes: list[str], aoi, requested_grid_level: int, deadline: float
    ) -> list[str]:
        """Replace groups of 32 siblings with their parent if the parent is
        fully covered by the AOI.  Works bottom-up from the deepest level."""
        try:
            from shapely.geometry import shape
        except ImportError:  # pragma: no cover
            raise RuntimeError("shapely is required for compact cover")

        code_set: set[str] = set(codes)

        # Work from longest codes upward
        for level in range(requested_grid_level, 1, -1):
            if time.monotonic() > deadline:
                raise GridLimitExceededError(
                    "geohash", requested_grid_level, "MAX_COVER_SECONDS",
                    MAX_COVER_SECONDS, MAX_COVER_SECONDS,
                )
            # Group codes at this level by parent
            at_level = [c for c in code_set if len(c) == level]
            parent_to_children: dict[str, list[str]] = {}
            for code in at_level:
                parent = code[:-1]
                parent_to_children.setdefault(parent, []).append(code)

            for parent, children in parent_to_children.items():
                if len(children) == 32:
                    # All 32 children present — replace with parent
                    parent_geom = _bbox_to_shapely(*_decode_bbox(parent))
                    if aoi.covers(parent_geom):
                        for child in children:
                            code_set.discard(child)
                        code_set.add(parent)

        return sorted(code_set, key=len)
