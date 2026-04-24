from __future__ import annotations

from functools import lru_cache

from grid_core.app.core.enums import BoundaryType, GridType
from grid_core.app.engines.registry import GridEngineRegistry


class TopologyService:
    def __init__(self) -> None:
        self._registry = GridEngineRegistry()

    def neighbors(self, grid_type: GridType, code: str, k: int = 1) -> list[str]:
        engine = self._registry.get_engine(grid_type)
        return engine.neighbors(code, k=k)

    def code_to_geometry(self, grid_type: GridType, code: str, boundary_type: BoundaryType) -> dict:
        if boundary_type == BoundaryType.BBOX:
            return {
                "type": "BBox",
                "bbox": self.code_to_bbox(grid_type, code),
            }
        engine = self._registry.get_engine(grid_type)
        return engine.code_to_geometry(code)

    def codes_to_geometries(self, grid_type: GridType, codes: list[str], boundary_type: BoundaryType) -> dict[str, dict]:
        unique_codes = list(dict.fromkeys(codes))
        return {code: self.code_to_geometry(grid_type, code, boundary_type) for code in unique_codes}

    def parent(self, grid_type: GridType, code: str) -> str:
        engine = self._registry.get_engine(grid_type)
        return engine.parent(code)

    def children(self, grid_type: GridType, code: str, target_level: int) -> list[str]:
        engine = self._registry.get_engine(grid_type)
        return engine.children(code, target_level)

    def code_to_bbox(self, grid_type: GridType, code: str) -> list[float]:
        return list(self._code_to_bbox_cached(grid_type.value, code))

    @lru_cache(maxsize=200000)
    def _code_to_bbox_cached(self, grid_type: str, code: str) -> tuple[float, float, float, float]:
        parsed_grid_type = GridType(grid_type)
        engine = self._registry.get_engine(parsed_grid_type)
        bbox = engine.code_to_bbox(code)
        return float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3])
