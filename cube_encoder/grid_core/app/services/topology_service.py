from __future__ import annotations

from grid_core.app.core.enums import BoundaryType, GridType
from grid_core.app.engines.registry import GridEngineRegistry
from grid_core.app.models.grid_address import GridAddress


class TopologyService:
    def __init__(self) -> None:
        self._registry = GridEngineRegistry()

    def neighbors(self, address: GridAddress, k: int = 1) -> list[GridAddress]:
        engine = self._registry.get_engine(GridType(address.grid_type))
        return engine.neighbors(address, k=k)

    def code_to_geometry(self, address: GridAddress, boundary_type: BoundaryType) -> dict:
        if boundary_type == BoundaryType.BBOX:
            return {
                "type": "BBox",
                "bbox": self.code_to_bbox(address),
            }
        engine = self._registry.get_engine(GridType(address.grid_type))
        return engine.code_to_geometry(address)

    def codes_to_geometries(self, addresses: list[GridAddress], boundary_type: BoundaryType) -> dict[str, dict]:
        result: dict[str, dict] = {}
        for address in addresses:
            key = address.topology_code or f"{address.grid_type}:{address.grid_level}:{address.space_code}"
            if key in result:
                continue
            result[key] = self.code_to_geometry(address, boundary_type)
        return result

    def parent(self, address: GridAddress) -> GridAddress:
        engine = self._registry.get_engine(GridType(address.grid_type))
        return engine.parent(address)

    def children(self, address: GridAddress, target_grid_level: int) -> list[GridAddress]:
        engine = self._registry.get_engine(GridType(address.grid_type))
        return engine.children(address, target_grid_level)

    def code_to_bbox(self, address: GridAddress) -> list[float]:
        engine = self._registry.get_engine(GridType(address.grid_type))
        bbox = engine.code_to_bbox(address)
        return [float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3])]
