from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

DEFAULT_LOGICAL_GRID_LEVEL = 5
DEFAULT_ISEA4H_GRID_LEVEL = 4

_RESOLUTION_NUMBER_RE = re.compile(r"(\d+(?:\.\d+)?)")
_RESOLUTION_KEYS = (
    "resolution",
    "resolution_m",
    "spatial_resolution",
    "spatial_resolution_m",
    "ground_resolution",
    "pixel_size",
    "pixel_size_m",
    "gsd",
    "gsd_m",
)

_LOGICAL_RESOLUTION_LEVELS = (
    (10.0, 6),
    (30.0, DEFAULT_LOGICAL_GRID_LEVEL),
    (250.0, 4),
    (1000.0, 3),
)
_ISEA4H_RESOLUTION_LEVELS = (
    (10.0, DEFAULT_ISEA4H_GRID_LEVEL),
    (30.0, DEFAULT_ISEA4H_GRID_LEVEL),
    (250.0, DEFAULT_ISEA4H_GRID_LEVEL),
    (1000.0, DEFAULT_ISEA4H_GRID_LEVEL),
)


def default_grid_level_for_grid_type(grid_type: str | None) -> int:
    return DEFAULT_ISEA4H_GRID_LEVEL if str(grid_type or "").lower() == "isea4h" else DEFAULT_LOGICAL_GRID_LEVEL


def default_grid_level_for_resolution(
    resolution: Any,
    *,
    grid_type: str | None = None,
    fallback: int | None = None,
) -> int:
    parsed = _parse_resolution(resolution)
    if parsed is None:
        return fallback if fallback is not None else default_grid_level_for_grid_type(grid_type)
    table = _ISEA4H_RESOLUTION_LEVELS if str(grid_type or "").lower() == "isea4h" else _LOGICAL_RESOLUTION_LEVELS
    for max_resolution, level in table:
        if parsed <= max_resolution:
            return level
    return DEFAULT_ISEA4H_GRID_LEVEL if str(grid_type or "").lower() == "isea4h" else 2


def default_grid_level_from_assets(
    assets: Iterable[Any] | None,
    *,
    grid_type: str | None = None,
    fallback: int | None = None,
) -> int:
    resolutions = [
        resolution
        for asset in assets or []
        if isinstance(asset, dict)
        for resolution in [_asset_resolution(asset)]
        if resolution is not None
    ]
    if not resolutions:
        return fallback if fallback is not None else default_grid_level_for_grid_type(grid_type)
    return default_grid_level_for_resolution(min(resolutions), grid_type=grid_type, fallback=fallback)


def apply_resolution_grid_defaults(
    payload: dict[str, Any],
    *,
    data_type: str,
    fallback_grid_level: int | None = None,
) -> dict[str, Any]:
    if data_type == "carbon":
        return payload
    payload.setdefault("grid_type", "geohash")
    grid_level = payload.get("grid_level")
    if grid_level is None or grid_level == "":
        payload["grid_level"] = default_grid_level_from_assets(
            payload.get("selected_assets") if isinstance(payload.get("selected_assets"), list) else [],
            grid_type=str(payload.get("grid_type") or "geohash"),
            fallback=fallback_grid_level,
        )
        payload.setdefault("grid_level_mode", "auto")
    return payload


def _asset_resolution(asset: dict[str, Any]) -> float | None:
    values = [_parse_resolution(asset.get(key)) for key in _RESOLUTION_KEYS if key in asset]
    values.extend(_parse_resolution(asset.get(key)) for key in ("resolution_x", "resolution_y", "pixel_size_x", "pixel_size_y"))
    values = [value for value in values if value is not None]
    return min(values) if values else None


def _parse_resolution(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if value > 0 else None
    if isinstance(value, str):
        match = _RESOLUTION_NUMBER_RE.search(value)
        if not match:
            return None
        parsed = float(match.group(1))
        return parsed if parsed > 0 else None
    if isinstance(value, dict):
        values = [_parse_resolution(value.get(key)) for key in _RESOLUTION_KEYS if key in value]
        values.extend(_parse_resolution(value.get(key)) for key in ("x", "y", "width", "height"))
        values = [item for item in values if item is not None]
        return min(values) if values else None
    if isinstance(value, Iterable):
        values = [_parse_resolution(item) for item in value]
        values = [item for item in values if item is not None]
        return min(values) if values else None
    return None
