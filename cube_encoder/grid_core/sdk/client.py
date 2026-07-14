"""Frozen M1 SDK contract stubs for CubeEncoderSDK.

Method signatures are final and consumed by cube_split and cube_web.
Implementations raise NotImplementedError until each engine replacement
task (Tasks 2–7) is complete and the facade is wired (Task 8).
"""
from __future__ import annotations

from datetime import datetime

from grid_core.app.core.enums import BoundaryType, CoverMode, GridType, TimeGranularity
from grid_core.app.models.compact_grid_cell import CompactGridCell
from grid_core.app.models.grid_address import GridAddress
from grid_core.app.models.grid_cell import GridCell
from grid_core.app.models.st_code import STCode


class CubeEncoderSDK:
    """Typed SDK facade over grid/topology/ST-code capabilities.

    All signatures are frozen by M1 contract.  Callers (cube_split, cube_web)
    depend on these exact parameter names; do not rename them.

    ``codes_to_geometries`` keys its result by
    ``topology_code or f"{grid_type}:{grid_level}:{space_code}"``
    so distinct cross-domain MGRS cells cannot collide.
    """

    def locate(
        self,
        grid_type: str | GridType,
        requested_grid_level: int,
        point: list[float],
    ) -> GridCell:
        raise NotImplementedError

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
        raise NotImplementedError

    def cover_compact(
        self,
        grid_type: str | GridType,
        requested_grid_level: int,
        cover_mode: str | CoverMode,
        geometry: dict[str, object] | None = None,
        bbox: list[float] | None = None,
        crs: str = "EPSG:4326",
    ) -> list[CompactGridCell]:
        raise NotImplementedError

    def neighbors(self, address: GridAddress, k: int = 1) -> list[GridAddress]:
        raise NotImplementedError

    def parent(self, address: GridAddress) -> GridAddress:
        raise NotImplementedError

    def children(self, address: GridAddress, target_grid_level: int) -> list[GridAddress]:
        raise NotImplementedError

    def code_to_geometry(
        self,
        address: GridAddress,
        boundary_type: str | BoundaryType = BoundaryType.POLYGON,
    ) -> dict[str, object]:
        raise NotImplementedError

    def code_to_bbox(self, address: GridAddress) -> list[float]:
        raise NotImplementedError

    def codes_to_geometries(
        self,
        addresses: list[GridAddress],
        boundary_type: str | BoundaryType = BoundaryType.POLYGON,
    ) -> dict[str, dict[str, object]]:
        raise NotImplementedError

    def generate_st_code(
        self,
        address: GridAddress,
        timestamp: datetime,
        time_granularity: str | TimeGranularity = TimeGranularity.MINUTE,
    ) -> STCode:
        raise NotImplementedError
