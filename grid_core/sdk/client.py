from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from grid_core.app.core.enums import BoundaryType, CoverMode, GridType, TimeGranularity
from grid_core.app.models.compact_grid_cell import CompactGridCell
from grid_core.app.models.grid_cell import GridCell
from grid_core.app.models.st_code import STCode
from grid_core.app.services.code_service import CodeService
from grid_core.app.services.grid_service import GridService
from grid_core.app.services.topology_service import TopologyService


def _parse_enum(value: str | Enum, enum_cls: type[Enum]) -> Enum:
    if isinstance(value, enum_cls):
        return value
    return enum_cls(value)


class CubeEncoderSDK:
    """Local Python SDK facade over grid/topology/ST-code capabilities."""

    def __init__(self) -> None:
        self._grid = GridService()
        self._topology = TopologyService()
        self._code = CodeService()

    def locate(self, grid_type: str | GridType, level: int, point: list[float]) -> GridCell:
        parsed_grid_type = _parse_enum(grid_type, GridType)
        return self._grid.locate(grid_type=parsed_grid_type, level=level, point=point)

    def cover(
        self,
        grid_type: str | GridType,
        level: int,
        cover_mode: str | CoverMode,
        boundary_type: str | BoundaryType,
        geometry: dict[str, Any] | None = None,
        bbox: list[float] | None = None,
        crs: str = "EPSG:4326",
    ) -> list[GridCell]:
        parsed_grid_type = _parse_enum(grid_type, GridType)
        parsed_cover_mode = _parse_enum(cover_mode, CoverMode)
        parsed_boundary_type = _parse_enum(boundary_type, BoundaryType)
        return self._grid.cover(
            grid_type=parsed_grid_type,
            level=level,
            geometry=geometry,
            bbox=bbox,
            cover_mode=parsed_cover_mode.value,
            boundary_type=parsed_boundary_type,
            crs=crs,
        )

    def cover_compact(
        self,
        grid_type: str | GridType,
        level: int,
        cover_mode: str | CoverMode,
        geometry: dict[str, Any] | None = None,
        bbox: list[float] | None = None,
        crs: str = "EPSG:4326",
    ) -> list[CompactGridCell]:
        parsed_grid_type = _parse_enum(grid_type, GridType)
        parsed_cover_mode = _parse_enum(cover_mode, CoverMode)
        return self._grid.cover_compact(
            grid_type=parsed_grid_type,
            level=level,
            geometry=geometry,
            bbox=bbox,
            cover_mode=parsed_cover_mode.value,
            crs=crs,
        )

    def neighbors(self, grid_type: str | GridType, code: str, k: int = 1) -> list[str]:
        parsed_grid_type = _parse_enum(grid_type, GridType)
        return self._topology.neighbors(grid_type=parsed_grid_type, code=code, k=k)

    def parent(self, grid_type: str | GridType, code: str) -> str:
        parsed_grid_type = _parse_enum(grid_type, GridType)
        return self._topology.parent(grid_type=parsed_grid_type, code=code)

    def children(self, grid_type: str | GridType, code: str, target_level: int) -> list[str]:
        parsed_grid_type = _parse_enum(grid_type, GridType)
        return self._topology.children(grid_type=parsed_grid_type, code=code, target_level=target_level)

    def code_to_geometry(
        self,
        grid_type: str | GridType,
        code: str,
        boundary_type: str | BoundaryType = BoundaryType.POLYGON,
    ) -> dict[str, Any]:
        parsed_grid_type = _parse_enum(grid_type, GridType)
        parsed_boundary_type = _parse_enum(boundary_type, BoundaryType)
        return self._topology.code_to_geometry(
            grid_type=parsed_grid_type,
            code=code,
            boundary_type=parsed_boundary_type,
        )

    def code_to_bbox(self, grid_type: str | GridType, code: str) -> list[float]:
        parsed_grid_type = _parse_enum(grid_type, GridType)
        return self._topology.code_to_bbox(
            grid_type=parsed_grid_type,
            code=code,
        )

    def codes_to_geometries(
        self,
        grid_type: str | GridType,
        codes: list[str],
        boundary_type: str | BoundaryType = BoundaryType.POLYGON,
    ) -> dict[str, dict[str, Any]]:
        parsed_grid_type = _parse_enum(grid_type, GridType)
        parsed_boundary_type = _parse_enum(boundary_type, BoundaryType)
        return self._topology.codes_to_geometries(
            grid_type=parsed_grid_type,
            codes=codes,
            boundary_type=parsed_boundary_type,
        )

    def generate_st_code(
        self,
        grid_type: str | GridType,
        level: int,
        space_code: str,
        timestamp: datetime,
        time_granularity: str | TimeGranularity = TimeGranularity.MINUTE,
        version: str = "v1",
    ) -> STCode:
        parsed_grid_type = _parse_enum(grid_type, GridType)
        parsed_time_granularity = _parse_enum(time_granularity, TimeGranularity)
        return self._code.generate_st_code(
            grid_type=parsed_grid_type,
            level=level,
            space_code=space_code,
            timestamp=timestamp,
            time_granularity=parsed_time_granularity,
            version=version,
        )

    def batch_generate_st_codes(
        self,
        grid_type: str | GridType,
        level: int,
        items: list[dict[str, Any]],
        time_granularity: str | TimeGranularity = TimeGranularity.MINUTE,
        version: str = "v1",
    ) -> list[str]:
        parsed_grid_type = _parse_enum(grid_type, GridType)
        parsed_time_granularity = _parse_enum(time_granularity, TimeGranularity)
        return self._code.batch_generate_st_codes(
            grid_type=parsed_grid_type,
            level=level,
            items=items,
            time_granularity=parsed_time_granularity,
            version=version,
        )

    def parse_st_code(self, st_code: str) -> STCode:
        return self._code.parse_st_code(st_code=st_code)
