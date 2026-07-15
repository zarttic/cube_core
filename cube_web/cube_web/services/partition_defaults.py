from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

DEFAULT_LOGICAL_GRID_LEVEL = 5
DEFAULT_ISEA4H_GRID_LEVEL = 6
DEFAULT_ENTITY_GRID_LEVEL = DEFAULT_ISEA4H_GRID_LEVEL

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


def normalize_partition_method(partition_method: Any, *, grid_type: str | None = None) -> str:
    method = str(partition_method or "").strip().lower()
    expected = "entity" if str(grid_type or "").lower() == "isea4h" else "logical"
    if method and method not in {"logical", "entity"}:
        raise ValueError("partition_method must be one of: logical, entity")
    if method and method != expected:
        raise ValueError(f"{grid_type} requires partition_method={expected}")
    return method or expected


def default_grid_level_for_grid_type(grid_type: str | None) -> int:
    return DEFAULT_ISEA4H_GRID_LEVEL if str(grid_type or "").lower() == "isea4h" else DEFAULT_LOGICAL_GRID_LEVEL


def default_grid_level_for_partition(
    grid_type: str | None,
    partition_method: Any,
) -> int:
    method = normalize_partition_method(partition_method, grid_type=grid_type)
    if method == "entity":
        return DEFAULT_ENTITY_GRID_LEVEL
    return default_grid_level_for_grid_type(grid_type)


def default_grid_level_for_resolution(
    resolution: Any,
    *,
    grid_type: str | None = None,
    partition_method: Any = None,
    fallback: int | None = None,
) -> int:
    method = normalize_partition_method(partition_method, grid_type=grid_type)
    if method == "entity":
        return fallback if fallback is not None else DEFAULT_ENTITY_GRID_LEVEL
    if str(grid_type or "").lower() == "mgrs":
        return fallback if fallback is not None else default_grid_level_for_grid_type(grid_type)
    parsed = _parse_resolution(resolution)
    if parsed is None:
        return fallback if fallback is not None else default_grid_level_for_partition(grid_type, method)
    if parsed < 10:
        return 8
    if parsed <= 30:
        return 7
    return 6


def default_grid_level_from_assets(
    assets: Iterable[Any] | None,
    *,
    grid_type: str | None = None,
    partition_method: Any = None,
    fallback: int | None = None,
) -> int:
    resolutions: list[float] = [
        resolution
        for asset in assets or []
        if isinstance(asset, dict)
        for resolution in [_asset_resolution(asset)]
        if resolution is not None
    ]
    if not resolutions:
        return fallback if fallback is not None else default_grid_level_for_partition(grid_type, partition_method)
    return default_grid_level_for_resolution(
        min(resolutions),
        grid_type=grid_type,
        partition_method=partition_method,
        fallback=fallback,
    )


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
            partition_method=payload.get("partition_method"),
            fallback=fallback_grid_level,
        )
    return payload


def _asset_resolution(asset: dict[str, Any]) -> float | None:
    values = [_parse_resolution(asset.get(key)) for key in _RESOLUTION_KEYS if key in asset]
    values.extend(_parse_resolution(asset.get(key)) for key in ("resolution_x", "resolution_y", "pixel_size_x", "pixel_size_y"))
    parsed_values: list[float] = [value for value in values if value is not None]
    return min(parsed_values) if parsed_values else None


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
        parsed_values: list[float] = [item for item in values if item is not None]
        return min(parsed_values) if parsed_values else None
    if isinstance(value, Iterable):
        values = [_parse_resolution(item) for item in value]
        parsed_values: list[float] = [item for item in values if item is not None]
        return min(parsed_values) if parsed_values else None
    return None
