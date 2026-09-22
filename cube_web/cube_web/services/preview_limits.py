"""Hard limits for the interactive grid preview endpoints.

Why this exists
---------------
``/v1/grid/cover`` and ``/v1/topology/children`` materialise every grid cell in
memory before answering, and neither had a bound.  Measured on the production web
node (2026-09-20): a 1°x1° geohash L7 cover is 531,441 cells / 1.8 GB peak, and a
3°x3° request extrapolates to ~4.8M cells / ~16 GB -- the same magnitude as the
2026-09-17 incident where a host process consumed 16 GB of anonymous memory in 90
seconds and the machine had to be hard reset.

These limits guard the *preview* path only.  Partition runs go through
``cube_split`` and are already bounded per worker by the shard planner
(``cube_split/jobs/logical_sharding.py`` keeps a chunk under
``TARGET_ROWS_PER_CHUNK`` rows), so a preview-sized cap must never be applied
there: a legitimate fine-level partition of a large AOI is far above any
browser-renderable cell count.
"""

from __future__ import annotations

import logging
import math
from typing import Any, Iterable

from fastapi import HTTPException

logger = logging.getLogger(__name__)

DEFAULT_MAX_PREVIEW_CELLS = 10_000
DEFAULT_MAX_PREVIEW_CHILDREN = 10_000

MAX_CELLS_ENV = "CUBE_WEB_PREVIEW_MAX_CELLS"
MAX_CHILDREN_ENV = "CUBE_WEB_PREVIEW_MAX_CHILDREN"

_LIMIT_CODE = "preview_limit_exceeded"


def _positive_env_int(name: str, default: int) -> int:
    from cube_split import runtime_config

    raw = runtime_config.env_text(name, "")
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def grid_type_value(grid_type: Any) -> str:
    """Grid type as the SDK's string value (``str(GridType.GEOHASH)`` is not "geohash")."""
    return str(getattr(grid_type, "value", grid_type))


def preview_max_cells() -> int:
    return _positive_env_int(MAX_CELLS_ENV, DEFAULT_MAX_PREVIEW_CELLS)


def preview_max_children() -> int:
    return _positive_env_int(MAX_CHILDREN_ENV, DEFAULT_MAX_PREVIEW_CHILDREN)


def _limit_error(*, message: str, limit: int, estimated: int, unit: str) -> HTTPException:
    return HTTPException(
        status_code=413,
        detail={
            "code": _LIMIT_CODE,
            "message": message,
            "limit": limit,
            "estimated": estimated,
            "unit": unit,
        },
    )


def _numbers(value: Any) -> Iterable[float]:
    """Yield every number in a GeoJSON-like coordinate structure."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        yield float(value)
        return
    if isinstance(value, dict):
        for item in value.values():
            yield from _numbers(item)
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            yield from _numbers(item)


def _bbox_from_geometry(geometry: Any) -> list[float] | None:
    """Bounding box of a GeoJSON geometry, or ``None`` when it is unusable."""
    if not isinstance(geometry, dict):
        return None
    coordinates = geometry.get("coordinates")
    if coordinates is None:
        return None
    values = list(_numbers(coordinates))
    if len(values) < 4:
        return None
    lons, lats = values[0::2], values[1::2]
    if not lons or not lats:
        return None
    return [min(lons), min(lats), max(lons), max(lats)]


def resolve_request_bbox(bbox: Any, geometry: Any) -> list[float] | None:
    """The extent a cover request will actually cover."""
    if _explicit_bbox(bbox):
        west, south, east, north = (float(value) for value in list(bbox)[:4])
        if east > west and north > south:
            return [west, south, east, north]
        return None
    return _bbox_from_geometry(geometry)


def _explicit_bbox(bbox: Any) -> bool:
    """True when the request carried its own usable bbox (not just a geometry)."""
    if not isinstance(bbox, (list, tuple)) or len(bbox) < 4:
        return False
    try:
        west, south, east, north = (float(value) for value in list(bbox)[:4])
    except (TypeError, ValueError):
        return False
    return east > west and north > south


def estimate_cover_cells(
    sdk: Any, *, grid_type: str, grid_level: int, bbox: list[float] | None,
) -> int | None:
    """Estimate how many cells a cover will materialise, or ``None`` if unknown.

    Cell size is measured with the SDK at the extent centre (cells shrink with
    latitude for geohash/MGRS), mirroring ``cube_split``'s shard planner.  The
    estimate only needs to be good enough to keep a runaway request away from the
    machine; the exact count is checked again after the cover returns.
    """
    if not bbox:
        return None
    west, south, east, north = bbox
    span_x, span_y = east - west, north - south
    if span_x <= 0 or span_y <= 0:
        return None
    lon, lat = (west + east) / 2, (south + north) / 2
    probes = [(lon, lat), (west, south), (east, north), (west, north), (east, south)]
    for probe_lon, probe_lat in probes:
        try:
            cell = sdk.locate(grid_type=grid_type, requested_grid_level=int(grid_level),
                              point=[probe_lon, probe_lat])
        except Exception:  # noqa: BLE001 - domain boundaries are expected to raise
            continue
        cell_bbox = list(getattr(cell, "bbox", None) or [])
        if len(cell_bbox) < 4:
            continue
        width = abs(float(cell_bbox[2]) - float(cell_bbox[0]))
        height = abs(float(cell_bbox[3]) - float(cell_bbox[1]))
        if width <= 0 or height <= 0:
            continue
        return max(1, math.ceil(span_x / width) * math.ceil(span_y / height))
    return None


def ensure_preview_cover_within_limit(
    sdk: Any, *, grid_type: str, grid_level: int, bbox: Any, geometry: Any,
) -> int | None:
    """Reject a cover preview that would materialise too many cells.

    Only used when the extent cannot be subdivided safely (geometry-only requests);
    bbox requests are truncated by :func:`cover_preview_cells` instead.
    """
    limit = preview_max_cells()
    extent = resolve_request_bbox(bbox, geometry)
    estimated = estimate_cover_cells(
        sdk, grid_type=grid_type_value(grid_type), grid_level=int(grid_level), bbox=extent,
    )
    if estimated is None:
        logger.warning("preview cover size could not be estimated grid_type=%s level=%s", grid_type, grid_level)
        return None
    if estimated > limit:
        raise _limit_error(
            limit=limit,
            estimated=estimated,
            unit="cells",
            message=(
                f"预览需要约 {estimated:,} 个格元，超过预览上限 {limit:,} 个："
                f"请缩小范围或选择更粗的格网层级。"
            ),
        )
    return estimated


def truncation_notice(limit: int) -> str:
    return f"最多显示 {limit:,} 个格网，多余格网已截断"


def _strip_bboxes(extent: list[float], *, estimate: int, limit: int) -> Iterable[list[float]]:
    """Split the extent into horizontal strips that each stay under ``limit`` cells."""
    west, south, east, north = extent
    span_y = north - south
    strips = max(1, math.ceil(estimate / max(1, limit)))
    height = span_y / strips
    for index in range(strips):
        strip_south = south + index * height
        strip_north = north if index == strips - 1 else min(north, strip_south + height)
        if strip_north <= strip_south:
            continue
        yield [west, strip_south, east, strip_north]


def cover_preview_cells(
    sdk: Any,
    *,
    grid_type: Any,
    grid_level: int,
    cover_mode: Any,
    boundary_type: Any,
    bbox: Any,
    geometry: Any,
    crs: Any,
) -> tuple[list[Any], bool]:
    """Cover for display, never materialising more than the preview limit.

    Returns ``(cells, truncated)``.  A request whose extent is far larger than the
    limit is covered strip by strip in row order and stops at the limit, so the
    memory a single request can allocate stays bounded by ``limit`` cells instead of
    the whole extent (measured 3.5 KB per cell).
    """
    limit = preview_max_cells()
    extent = resolve_request_bbox(bbox, geometry)
    estimate = estimate_cover_cells(
        sdk, grid_type=grid_type_value(grid_type), grid_level=int(grid_level), bbox=extent,
    )

    def _cover(target_bbox: Any) -> list[Any]:
        return sdk.cover(
            grid_type=grid_type,
            requested_grid_level=int(grid_level),
            cover_mode=cover_mode,
            boundary_type=boundary_type,
            geometry=geometry if target_bbox is None else None,
            bbox=target_bbox,
            crs=crs,
        )

    if estimate is None or estimate <= limit:
        cells = _cover(bbox)
        if len(cells) > limit:
            return cells[:limit], True
        return cells, False

    if not _explicit_bbox(bbox):
        # Geometry-only request: subdividing by bbox would silently cover cells
        # outside the requested polygon, so keep the hard rejection here.
        raise _limit_error(
            limit=limit,
            estimated=estimate,
            unit="cells",
            message=(
                f"预览需要约 {estimate:,} 个格元，超过预览上限 {limit:,} 个："
                f"请缩小范围或选择更粗的格网层级。"
            ),
        )

    cells: list[Any] = []
    for strip in _strip_bboxes(extent, estimate=estimate, limit=limit):
        cells.extend(_cover(strip))
        if len(cells) >= limit:
            break
    return cells[:limit], True


def ensure_preview_children_within_limit(
    sdk: Any, *, address: Any, target_grid_level: int,
) -> int:
    """Reject a ``children`` preview whose result would explode exponentially.

    Children grow by the grid's branching factor per level (geohash 32, MGRS 100,
    ISEA4H 4), so one level of children gives the exact factor to extrapolate with.
    """
    limit = preview_max_children()
    current_level = int(getattr(address, "grid_level", 0) or 0)
    gap = int(target_grid_level) - current_level
    if gap <= 0:
        return 0  # let the engine raise its own "must be greater" validation error
    try:
        first_level = sdk.children(address, current_level + 1)
    except Exception:  # noqa: BLE001 - the engine reports its own errors
        return 0
    factor = max(1, len(first_level))
    total = factor ** gap
    if total > limit:
        raise _limit_error(
            limit=limit,
            estimated=total,
            unit="children",
            message=(
                f"该层级跨度会产生 {total:,} 个子格元，超过预览上限 {limit:,} 个："
                f"请缩小层级跨度或分批查询。"
            ),
        )
    return total
