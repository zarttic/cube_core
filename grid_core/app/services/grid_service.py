from __future__ import annotations

from grid_core.app.core.enums import BoundaryType, GridType
from grid_core.app.core.exceptions import ValidationError
from grid_core.app.engines.registry import GridEngineRegistry
from grid_core.app.models.grid_cell import GridCell
from grid_core.app.utils.geometry import bbox_to_polygon


class GridService:
    def __init__(self) -> None:
        self._registry = GridEngineRegistry()

    def locate(self, grid_type: GridType, level: int, point: list[float]) -> GridCell:
        engine = self._registry.get_engine(grid_type)
        lon, lat = point
        return engine.locate_point(lon=lon, lat=lat, level=level)

    def cover(
        self,
        grid_type: GridType,
        level: int,
        geometry: dict | None,
        bbox: list[float] | None,
        cover_mode: str,
        boundary_type: BoundaryType,
        crs: str,
    ) -> list[GridCell]:
        engine = self._registry.get_engine(grid_type)
        if crs != "EPSG:4326":
            raise ValidationError("Only EPSG:4326 is supported in MVP")
        target_geometry = geometry
        if target_geometry is None and bbox is not None:
            target_geometry = bbox_to_polygon(bbox).__geo_interface__
        if target_geometry is None:
            raise ValidationError("Either geometry or bbox must be provided")

        cells = engine.cover_geometry(geometry=target_geometry, level=level, cover_mode=cover_mode)

        if boundary_type == BoundaryType.BBOX:
            for cell in cells:
                cell.geometry = None
        return cells
