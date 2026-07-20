from __future__ import annotations

from grid_core.app.core.enums import BoundaryType, GridType
from grid_core.app.core.exceptions import ValidationError
from grid_core.app.engines.registry import GridEngineRegistry
from grid_core.app.models.compact_grid_cell import CompactGridCell
from grid_core.app.models.grid_address import GridAddress
from grid_core.app.models.grid_cell import GridCell
from grid_core.app.utils.geometry import bbox_to_polygon, point_from_coords


class GridService:
    def __init__(self) -> None:
        self._registry = GridEngineRegistry()

    def locate(self, grid_type: GridType, requested_grid_level: int, point: list[float]) -> GridCell:
        engine = self._registry.get_engine(grid_type)
        pt = point_from_coords(point)
        lon, lat = float(pt.x), float(pt.y)
        return engine.locate_point(lon=lon, lat=lat, requested_grid_level=requested_grid_level)

    def locate_space_code(self, grid_type: GridType, requested_grid_level: int, point: list[float]) -> GridAddress:
        engine = self._registry.get_engine(grid_type)
        if len(point) != 2:
            raise ValidationError("Point must be [lon, lat]")
        lon, lat = float(point[0]), float(point[1])
        if lon < -180.0 or lon > 180.0:
            raise ValidationError("Point longitude must be in [-180, 180]")
        if lat < -90.0 or lat > 90.0:
            raise ValidationError("Point latitude must be in [-90, 90]")
        return engine.locate_space_code(lon=lon, lat=lat, requested_grid_level=requested_grid_level)

    def locate_space_codes(
        self,
        grid_type: GridType,
        requested_grid_level: int,
        points: list[list[float]],
    ) -> list[GridAddress]:
        engine = self._registry.get_engine(grid_type)
        normalized_points: list[list[float]] = []
        for point in points:
            if len(point) != 2:
                raise ValidationError("Point must be [lon, lat]")
            lon, lat = float(point[0]), float(point[1])
            if lon < -180.0 or lon > 180.0:
                raise ValidationError("Point longitude must be in [-180, 180]")
            if lat < -90.0 or lat > 90.0:
                raise ValidationError("Point latitude must be in [-90, 90]")
            normalized_points.append([lon, lat])
        locate_many = getattr(engine, "locate_space_codes", None)
        if locate_many is not None:
            return locate_many(normalized_points, requested_grid_level)
        return [
            engine.locate_space_code(lon=point[0], lat=point[1], requested_grid_level=requested_grid_level)
            for point in normalized_points
        ]

    def cover(
        self,
        grid_type: GridType,
        requested_grid_level: int,
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

        if boundary_type == BoundaryType.BBOX:
            compact_cells = engine.cover_geometry_compact(
                geometry=target_geometry, requested_grid_level=requested_grid_level, cover_mode=cover_mode
            )
            return [
                GridCell(
                    grid_type=cell.grid_type,
                    grid_level=cell.grid_level,
                    space_code=cell.space_code,
                    topology_code=cell.topology_code,
                    center=[
                        (cell.bbox[0] + cell.bbox[2]) / 2.0,
                        (cell.bbox[1] + cell.bbox[3]) / 2.0,
                    ],
                    bbox=cell.bbox,
                    geometry=None,
                    metadata={},
                )
                for cell in compact_cells
            ]

        cells = engine.cover_geometry(geometry=target_geometry, requested_grid_level=requested_grid_level, cover_mode=cover_mode)
        return cells

    def cover_compact(
        self,
        grid_type: GridType,
        requested_grid_level: int,
        geometry: dict | None,
        bbox: list[float] | None,
        cover_mode: str,
        crs: str,
    ) -> list[CompactGridCell]:
        engine = self._registry.get_engine(grid_type)
        if crs != "EPSG:4326":
            raise ValidationError("Only EPSG:4326 is supported in MVP")
        target_geometry = geometry
        if target_geometry is None and bbox is not None:
            target_geometry = bbox_to_polygon(bbox).__geo_interface__
        if target_geometry is None:
            raise ValidationError("Either geometry or bbox must be provided")
        return engine.cover_geometry_compact(
            geometry=target_geometry, requested_grid_level=requested_grid_level, cover_mode=cover_mode
        )
