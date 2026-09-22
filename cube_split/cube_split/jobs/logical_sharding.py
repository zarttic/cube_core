"""Shard planning for logical (Geohash/MGRS/ISEA4H) partitions.

Why: the shard grid used to be a fixed ``CUBE_LOGICAL_SHARD_DEGREES`` (default 1°),
with one chunk per ``SHARDS_PER_TASK`` (default 16) shards.  For a coarse grid over a
large extent that is wildly mis-sized -- a global MGRS L0 request splits the world into
64,800 one-degree shards, producing thousands of chunk objects that each hold a handful
of rows (measured on 2026-09-19: 557k rows spread over 4,050 chunks, ~137 rows each).

This planner estimates how many grid cells the extent will produce and derives the
shard size from a *chunk budget* instead:

    rows_per_cell = 1 (grid cell) + 2 x bands (tile + index)
    chunks        = clamp(ceil(rows / target_rows_per_chunk), min_chunks, max_chunks)
    shards        = chunks x shards_per_task
    shard size    = extent / (shards_x, shards_y)   (aspect-preserving grid)

So the number of chunk objects is bounded by ``max_chunks`` while each chunk stays
within ``target_rows_per_chunk``.  ``CUBE_LOGICAL_SHARD_MODE=legacy`` keeps the old
fixed-degree behaviour for comparison or rollback.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Iterator

from cube_split import runtime_config

AUTO_MODE = "auto"
LEGACY_MODE = "legacy"
VALID_MODES = (AUTO_MODE, LEGACY_MODE)

DEFAULT_MODE = AUTO_MODE
DEFAULT_SHARD_DEGREES = 1.0
DEFAULT_SHARDS_PER_TASK = 16
DEFAULT_TARGET_ROWS_PER_CHUNK = 20_000
DEFAULT_MAX_CHUNKS = 512
# A single chunk may exceed the target, but never this multiple of it: an absurd AOI
# (e.g. a global fine grid) buys more chunks instead of one giant in-memory chunk.
DEFAULT_MAX_ROWS_PER_CHUNK_FACTOR = 10
DEFAULT_MIN_CHUNKS_FACTOR = 2


@dataclass(frozen=True)
class LogicalShardPlan:
    mode: str
    shard_width: float
    shard_height: float
    shards_x: int
    shards_y: int
    shard_count: int
    chunk_count: int
    estimated_cells: int
    estimated_rows: int
    rows_per_chunk: int
    cells_per_shard: float
    reason: str
    details: dict[str, Any] = field(default_factory=dict)


def shard_mode() -> str:
    raw = str(runtime_config.env_text("CUBE_LOGICAL_SHARD_MODE", DEFAULT_MODE) or "").strip().lower()
    return raw if raw in VALID_MODES else AUTO_MODE


def _env_int(name: str, default: int) -> int:
    raw = runtime_config.env_text(name, "")
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def _env_float(name: str, default: float) -> float:
    raw = runtime_config.env_text(name, "")
    try:
        value = float(str(raw).strip())
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def _measurement_points(lon: float, lat: float) -> list[tuple[float, float]]:
    """Candidate probe points for the cell-size measurement.

    A single point can sit exactly on a grid/domain boundary (the antimeridian, the
    equator, a UTM zone edge) where locating raises, so try a few nearby points and
    finish with two mid-latitude fallbacks.
    """
    candidates = [
        (lon, lat),
        (lon, _clamp_lat(lat + 0.5)),
        (_wrap_lon(lon + 0.5), lat),
        (_wrap_lon(lon + 0.7), _clamp_lat(lat + 0.7)),
        (13.5, 43.5),
        (100.5, 30.5),
    ]
    seen: set[tuple[float, float]] = set()
    unique: list[tuple[float, float]] = []
    for candidate in candidates:
        key = (round(candidate[0], 6), round(candidate[1], 6))
        if key in seen:
            continue
        seen.add(key)
        unique.append(key)
    return unique


def _wrap_lon(lon: float) -> float:
    wrapped = math.fmod(lon + 180.0, 360.0)
    if wrapped < 0:
        wrapped += 360.0
    return wrapped - 180.0


def _clamp_lat(lat: float) -> float:
    return max(-89.9, min(89.9, lat))


def measure_cell_degrees(sdk: Any, *, grid_type: str, grid_level: int, lon: float, lat: float) -> tuple[float, float]:
    """Cell size in degrees at a reference point (the extent centre).

    Cells shrink with latitude for MGRS/geohash, so measuring at the centre gives a
    usable average for planning.  Never returns a degenerate size: if every probe
    point fails (domain boundaries, polar cells) the analytic size is used.
    """
    for probe_lon, probe_lat in _measurement_points(lon, lat):
        try:
            cell = sdk.locate(grid_type=grid_type, requested_grid_level=int(grid_level), point=[probe_lon, probe_lat])
        except Exception:  # noqa: BLE001 - a boundary point is expected to raise
            continue
        bbox = list(getattr(cell, "bbox", None) or [])
        if len(bbox) < 4:
            continue
        width = abs(float(bbox[2]) - float(bbox[0]))
        height = abs(float(bbox[3]) - float(bbox[1]))
        if width > 1e-9 and height > 1e-9:
            return min(width, 360.0), min(height, 180.0)
    fallback_w, fallback_h = _analytic_cell_degrees(grid_type, int(grid_level), lat)
    return min(fallback_w, 360.0), min(fallback_h, 180.0)


def _analytic_cell_degrees(grid_type: str, grid_level: int, lat: float) -> tuple[float, float]:
    """Rough cell size when no cell could be measured (planning only)."""
    level = max(0, int(grid_level))
    kind = str(grid_type or "").strip().lower()
    cos_lat = max(math.cos(math.radians(_clamp_lat(lat))), 0.05)
    if kind == "mgrs":
        size_km = 100.0 / (10**level)
        return size_km / (111.32 * cos_lat), size_km / 110.57
    if kind == "geohash":
        bits = 5 * level + 1
        return 360.0 / (2 ** math.ceil(bits / 2)), 180.0 / (2 ** (bits // 2))
    if kind == "isea4h":
        cells = 10 * (4**level) + 2
        side_km = math.sqrt(510_072_000.0 / cells)
        return side_km / (111.32 * cos_lat), side_km / 110.57
    return 360.0 / (2 ** level), 180.0 / (2 ** level)


def plan_logical_shards(
    *,
    sdk: Any,
    bbox: list[float] | tuple[float, float, float, float],
    grid_type: str,
    grid_level: int,
    bands: int,
    shards_per_task: int = DEFAULT_SHARDS_PER_TASK,
    parallelism: int = 1,
    target_rows_per_chunk: int | None = None,
    max_chunks: int | None = None,
    mode: str | None = None,
) -> LogicalShardPlan:
    """Decide the shard grid for one logical partition extent."""
    west, south, east, north = (float(value) for value in bbox)
    span_x = max(east - west, 1e-9)
    span_y = max(north - south, 1e-9)
    per_task = max(1, int(shards_per_task))
    resolved_mode = (mode or shard_mode()).strip().lower()
    rows_per_cell = 1 + 2 * max(1, int(bands))

    if resolved_mode == LEGACY_MODE:
        degrees = _env_float("CUBE_LOGICAL_SHARD_DEGREES", DEFAULT_SHARD_DEGREES)
        if degrees > 10:
            degrees = DEFAULT_SHARD_DEGREES
        shards_x = max(1, math.ceil(span_x / degrees))
        shards_y = max(1, math.ceil(span_y / degrees))
        cell_x, cell_y = measure_cell_degrees(sdk, grid_type=grid_type, grid_level=grid_level, lon=(west + east) / 2, lat=(south + north) / 2)
        cells = _estimate_cells(span_x, span_y, cell_x, cell_y)
        return LogicalShardPlan(
            mode=LEGACY_MODE,
            shard_width=degrees,
            shard_height=degrees,
            shards_x=shards_x,
            shards_y=shards_y,
            shard_count=shards_x * shards_y,
            chunk_count=math.ceil(shards_x * shards_y / per_task),
            estimated_cells=cells,
            estimated_rows=cells * rows_per_cell,
            rows_per_chunk=int(round(cells * rows_per_cell / max(1, math.ceil(shards_x * shards_y / per_task)))),
            cells_per_shard=round(cells / max(1, shards_x * shards_y), 2),
            reason=f"legacy fixed {degrees:g}° shards",
        )

    cell_x, cell_y = measure_cell_degrees(
        sdk, grid_type=grid_type, grid_level=grid_level, lon=(west + east) / 2, lat=(south + north) / 2
    )
    cells = _estimate_cells(span_x, span_y, cell_x, cell_y)
    rows = cells * rows_per_cell
    target_rows = target_rows_per_chunk or _env_int("CUBE_LOGICAL_TARGET_ROWS_PER_CHUNK", DEFAULT_TARGET_ROWS_PER_CHUNK)
    chunk_cap = max_chunks or _env_int("CUBE_LOGICAL_MAX_CHUNKS", DEFAULT_MAX_CHUNKS)
    max_rows_per_chunk = DEFAULT_MAX_ROWS_PER_CHUNK_FACTOR * target_rows
    min_chunks = max(1, min(chunk_cap, DEFAULT_MIN_CHUNKS_FACTOR * max(1, int(parallelism))))

    chunks_by_rows = max(1, math.ceil(rows / max(1, target_rows)))
    chunks_by_memory = max(1, math.ceil(rows / max(1, max_rows_per_chunk)))
    chunks = max(chunks_by_rows, chunks_by_memory, min_chunks)
    reason = f"target_rows_per_chunk={target_rows}"
    if chunks > chunk_cap:
        if chunks_by_memory > chunk_cap:
            # Soft cap: keeping a chunk within the hard row budget matters more than the
            # object budget, so exceed CUBE_LOGICAL_MAX_CHUNKS and say so.
            reason = f"{reason}, exceeding CUBE_LOGICAL_MAX_CHUNKS={chunk_cap} to stay under {max_rows_per_chunk} rows/chunk"
        else:
            chunks = chunk_cap
            reason = f"{reason}, capped by CUBE_LOGICAL_MAX_CHUNKS={chunk_cap}"
    shard_count = chunks * per_task

    # Aspect-preserving shard grid that never exceeds the resolved shard budget
    # (rounding up would push the chunk count past the memory/cap decision above).
    budget = shard_count
    aspect = span_x / span_y
    shards_x = max(1, int(math.floor(math.sqrt(budget * aspect))))
    shards_y = max(1, int(math.floor(budget / shards_x)))
    while (shards_x + 1) * shards_y <= budget:
        shards_x += 1
    shard_w = span_x / shards_x
    shard_h = span_y / shards_y
    # A shard finer than a cell wastes chunks; a shard far coarser than the cell grid
    # only makes each task bigger, so align to the cell size when it is the tighter limit.
    if shard_w < cell_x:
        shards_x = max(1, math.floor(span_x / cell_x))
        shard_w = span_x / shards_x
    if shard_h < cell_y:
        shards_y = max(1, math.floor(span_y / cell_y))
        shard_h = span_y / shards_y
    shard_count = shards_x * shards_y
    chunk_count = math.ceil(shard_count / per_task)
    if chunk_count > chunks and shards_x * shards_y > per_task:
        # The cell-alignment clamp can only grow the grid when cells are tiny; keep the budget.
        keep = max(1, chunks * per_task)
        while shards_x * shards_y > keep:
            if shards_x >= shards_y and shards_x > 1:
                shards_x -= 1
            elif shards_y > 1:
                shards_y -= 1
            else:
                break
        shard_w = span_x / shards_x
        shard_h = span_y / shards_y
        shard_count = shards_x * shards_y
        chunk_count = math.ceil(shard_count / per_task)
    # Keep the parallelism floor after the aspect-factorisation (it can undershoot).
    if min_chunks > chunk_count and chunk_cap >= min_chunks:
        wanted_shards = min_chunks * per_task
        while shards_x * shards_y < wanted_shards and shards_x * shards_y < chunks * per_task:
            if shards_x <= shards_y:
                shards_x += 1
            else:
                shards_y += 1
            if span_x / shards_x < cell_x and span_y / shards_y < cell_y:
                break
        shard_w = span_x / shards_x
        shard_h = span_y / shards_y
        shard_count = shards_x * shards_y
        chunk_count = math.ceil(shard_count / per_task)

    return LogicalShardPlan(
        mode=AUTO_MODE,
        shard_width=shard_w,
        shard_height=shard_h,
        shards_x=shards_x,
        shards_y=shards_y,
        shard_count=shard_count,
        chunk_count=chunk_count,
        estimated_cells=cells,
        estimated_rows=rows,
        rows_per_chunk=int(round(rows / max(1, chunk_count))),
        cells_per_shard=round(cells / max(1, shard_count), 2),
        reason=reason,
        details={"cell_width_deg": round(cell_x, 6), "cell_height_deg": round(cell_y, 6), "bands": max(1, int(bands))},
    )


def iter_shards(
    bbox: list[float] | tuple[float, float, float, float], shard_width: float, shard_height: float
) -> Iterator[tuple[float, float, float, float]]:
    """Tile the bbox with (width x height) shards; the union is exactly the bbox."""
    west, south, east, north = (float(value) for value in bbox)
    if not -180 <= west <= 180 or not -180 <= east <= 180 or not -90 <= south <= 90 or not -90 <= north <= 90 or west > east or south > north:
        raise ValueError("logical chunk source bbox must be a non-wrapping WGS84 bbox")
    width = max(float(shard_width), 1e-9)
    height = max(float(shard_height), 1e-9)
    lat = south
    while lat < north:
        next_lat = min(north, lat + height)
        lon = west
        while lon < east:
            next_lon = min(east, lon + width)
            yield lon, lat, next_lon, next_lat
            lon = next_lon
        lat = next_lat


def existing_cells_estimate(sdk: Any, *, grid_type: str, grid_level: int, bbox: list[float] | tuple[float, float, float, float]) -> int:
    """Cheap cell count for the whole extent (same measure the planner uses)."""
    west, south, east, north = (float(value) for value in bbox)
    cell_x, cell_y = measure_cell_degrees(
        sdk, grid_type=grid_type, grid_level=grid_level, lon=(west + east) / 2, lat=(south + north) / 2
    )
    return _estimate_cells(max(east - west, 1e-9), max(north - south, 1e-9), cell_x, cell_y)


def _estimate_cells(span_x: float, span_y: float, cell_x: float, cell_y: float) -> int:
    return max(1, int(math.ceil(span_x / cell_x) * math.ceil(span_y / cell_y)))
