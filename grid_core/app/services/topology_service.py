from __future__ import annotations

from grid_core.app.core.enums import BoundaryType, GridType
from grid_core.app.core.exceptions import ValidationError
from grid_core.app.engines.geohash_engine import GeohashEngine


class TopologyService:
    def __init__(self) -> None:
        self._geohash = GeohashEngine()

    def neighbors(self, grid_type: GridType, code: str, k: int = 1) -> list[str]:
        if grid_type != GridType.GEOHASH:
            raise ValidationError(f"Unsupported grid_type in MVP: {grid_type}")
        return self._geohash.neighbors(code, k=k)

    def code_to_geometry(self, grid_type: GridType, code: str, boundary_type: BoundaryType) -> dict:
        if grid_type != GridType.GEOHASH:
            raise ValidationError(f"Unsupported grid_type in MVP: {grid_type}")
        if boundary_type == BoundaryType.BBOX:
            return {
                "type": "BBox",
                "bbox": self._geohash.code_to_bbox(code),
            }
        return self._geohash.code_to_geometry(code)

    def parent(self, grid_type: GridType, code: str) -> str:
        if grid_type != GridType.GEOHASH:
            raise ValidationError(f"Unsupported grid_type in MVP: {grid_type}")
        return self._geohash.parent(code)

    def children(self, grid_type: GridType, code: str, target_level: int) -> list[str]:
        if grid_type != GridType.GEOHASH:
            raise ValidationError(f"Unsupported grid_type in MVP: {grid_type}")
        return self._geohash.children(code, target_level)
