"""CubeEncoderSDK: the frozen M1 SDK facade for CubeEncoder capabilities.

Method signatures are final and consumed by cube_split and cube_web.
Internally this delegates to the same GridService / TopologyService /
CodeService used by the encoder's own FastAPI routes, so the SDK and the
HTTP API can never drift apart.
"""
from __future__ import annotations

from datetime import datetime

from grid_core.app.core.enums import BoundaryType, CoverMode, GridType, TimeGranularity
from grid_core.app.models.compact_grid_cell import CompactGridCell
from grid_core.app.models.grid_address import GridAddress
from grid_core.app.models.grid_cell import GridCell
from grid_core.app.models.st_code import STCode
from grid_core.app.services.code_service import CodeService
from grid_core.app.services.grid_service import GridService
from grid_core.app.services.topology_service import TopologyService


class CubeEncoderSDK:
    """Typed SDK facade over grid/topology/ST-code capabilities.

    All signatures are frozen by M1 contract.  Callers (cube_split, cube_web)
    depend on these exact parameter names; do not rename them.

    ``codes_to_geometries`` keys its result by
    ``topology_code or f"{grid_type}:{grid_level}:{space_code}"``
    so distinct cross-domain MGRS cells cannot collide.
    """

    def __init__(self) -> None:
        self._grid_service = GridService()
        self._topology_service = TopologyService()
        self._code_service = CodeService()

    @staticmethod
    def _as_grid_type(grid_type: str | GridType) -> GridType:
        return grid_type if isinstance(grid_type, GridType) else GridType(grid_type)

    @staticmethod
    def _as_boundary_type(boundary_type: str | BoundaryType) -> BoundaryType:
        return boundary_type if isinstance(boundary_type, BoundaryType) else BoundaryType(boundary_type)

    @staticmethod
    def _as_cover_mode(cover_mode: str | CoverMode) -> CoverMode:
        return cover_mode if isinstance(cover_mode, CoverMode) else CoverMode(cover_mode)

    @staticmethod
    def _as_time_granularity(time_granularity: str | TimeGranularity) -> TimeGranularity:
        return time_granularity if isinstance(time_granularity, TimeGranularity) else TimeGranularity(time_granularity)

    def locate(
        self,
        grid_type: str | GridType,
        requested_grid_level: int,
        point: list[float],
    ) -> GridCell:
        return self._grid_service.locate(
            grid_type=self._as_grid_type(grid_type),
            requested_grid_level=requested_grid_level,
            point=point,
        )

    def cover(
        self,
        grid_type: str | GridType,
        requested_grid_level: int,
        cover_mode: str | CoverMode,
        boundary_type: str | BoundaryType,
        geometry: dict[str, object] | None = None,
        bbox: list[float] | None = None,
        crs: str = "EPSG:4326",
    ) -> list[GridCell]:
        return self._grid_service.cover(
            grid_type=self._as_grid_type(grid_type),
            requested_grid_level=requested_grid_level,
            geometry=geometry,
            bbox=bbox,
            cover_mode=self._as_cover_mode(cover_mode).value,
            boundary_type=self._as_boundary_type(boundary_type),
            crs=crs,
        )

    def cover_compact(
        self,
        grid_type: str | GridType,
        requested_grid_level: int,
        cover_mode: str | CoverMode,
        geometry: dict[str, object] | None = None,
        bbox: list[float] | None = None,
        crs: str = "EPSG:4326",
    ) -> list[CompactGridCell]:
        return self._grid_service.cover_compact(
            grid_type=self._as_grid_type(grid_type),
            requested_grid_level=requested_grid_level,
            geometry=geometry,
            bbox=bbox,
            cover_mode=self._as_cover_mode(cover_mode).value,
            crs=crs,
        )

    def neighbors(self, address: GridAddress, k: int = 1) -> list[GridAddress]:
        return self._topology_service.neighbors(address, k=k)

    def parent(self, address: GridAddress) -> GridAddress:
        return self._topology_service.parent(address)

    def children(self, address: GridAddress, target_grid_level: int) -> list[GridAddress]:
        return self._topology_service.children(address, target_grid_level)

    def code_to_geometry(
        self,
        address: GridAddress,
        boundary_type: str | BoundaryType = BoundaryType.POLYGON,
    ) -> dict[str, object]:
        return self._topology_service.code_to_geometry(address, self._as_boundary_type(boundary_type))

    def code_to_bbox(self, address: GridAddress) -> list[float]:
        return self._topology_service.code_to_bbox(address)

    def codes_to_geometries(
        self,
        addresses: list[GridAddress],
        boundary_type: str | BoundaryType = BoundaryType.POLYGON,
    ) -> dict[str, dict[str, object]]:
        return self._topology_service.codes_to_geometries(addresses, self._as_boundary_type(boundary_type))

    def generate_st_code(
        self,
        address: GridAddress,
        timestamp: datetime,
        time_granularity: str | TimeGranularity = TimeGranularity.MINUTE,
    ) -> STCode:
        return self._code_service.generate_st_code(
            address=address,
            timestamp=timestamp,
            time_granularity=self._as_time_granularity(time_granularity),
        )

    def parse_st_code(self, st_code: str) -> STCode:
        return self._code_service.parse_st_code(st_code)
