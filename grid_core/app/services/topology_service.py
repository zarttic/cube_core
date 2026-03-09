from __future__ import annotations

from grid_core.app.core.enums import BoundaryType, GridType
from grid_core.app.engines.registry import GridEngineRegistry


class TopologyService:
    def __init__(self) -> None:
        self._registry = GridEngineRegistry()

    def neighbors(self, grid_type: GridType, code: str, k: int = 1) -> list[str]:
        engine = self._registry.get_engine(grid_type)
        return engine.neighbors(code, k=k)

    def code_to_geometry(self, grid_type: GridType, code: str, boundary_type: BoundaryType) -> dict:
        engine = self._registry.get_engine(grid_type)
        if boundary_type == BoundaryType.BBOX:
            return {
                "type": "BBox",
                "bbox": engine.code_to_bbox(code),
            }
        return engine.code_to_geometry(code)

    def codes_to_geometries(self, grid_type: GridType, codes: list[str], boundary_type: BoundaryType) -> dict[str, dict]:
        return {code: self.code_to_geometry(grid_type, code, boundary_type) for code in codes}

    def parent(self, grid_type: GridType, code: str) -> str:
        engine = self._registry.get_engine(grid_type)
        return engine.parent(code)

    def children(self, grid_type: GridType, code: str, target_level: int) -> list[str]:
        engine = self._registry.get_engine(grid_type)
        return engine.children(code, target_level)
