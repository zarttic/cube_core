from __future__ import annotations

from functools import lru_cache

import h3
from shapely.geometry import Polygon

from grid_core.app.core.enums import CoverMode
from grid_core.app.core.exceptions import ValidationError
from grid_core.app.models.compact_grid_cell import CompactGridCell
from grid_core.app.models.grid_cell import GridCell
from grid_core.app.utils.geometry import to_shapely


class ISEA4HEngine:
    grid_type = "isea4h"

    def locate_point(self, lon: float, lat: float, level: int) -> GridCell:
        self._validate_level(level)
        code = h3.latlng_to_cell(lat, lon, level)
        return self._build_cell(code, level)

    def cover_geometry(self, geometry: dict, level: int, cover_mode: str) -> list[GridCell]:
        compact_cells, boundary_cache = self._cover_geometry_core(geometry=geometry, level=level, cover_mode=cover_mode)
        return [
            self._build_cell(cell.space_code, cell.level, boundary=boundary_cache.get(cell.space_code))
            for cell in compact_cells
        ]

    def cover_geometry_compact(self, geometry: dict, level: int, cover_mode: str) -> list[CompactGridCell]:
        compact_cells, _ = self._cover_geometry_core(geometry=geometry, level=level, cover_mode=cover_mode)
        return compact_cells

    def _cover_geometry_core(
        self,
        geometry: dict,
        level: int,
        cover_mode: str,
    ) -> tuple[list[CompactGridCell], dict[str, list[list[float]]]]:
        self._validate_level(level)
        if cover_mode not in {CoverMode.INTERSECT.value, CoverMode.CONTAIN.value, CoverMode.MINIMAL.value}:
            raise ValidationError(f"Unsupported cover_mode: {cover_mode}")

        shp = to_shapely(geometry)
        candidate_codes = set(h3.geo_to_cells(geometry, level))
        selected: set[str] = set()
        boundary_cache: dict[str, list[list[float]]] = {}
        for code in candidate_codes:
            boundary = self._boundary_lnglat(code)
            boundary_cache[code] = boundary
            cell_poly = Polygon(boundary)
            if cover_mode in {CoverMode.INTERSECT.value, CoverMode.MINIMAL.value} and cell_poly.intersects(shp):
                selected.add(code)
            elif cover_mode == CoverMode.CONTAIN.value and shp.covers(cell_poly):
                selected.add(code)
        if cover_mode == CoverMode.MINIMAL.value:
            selected = self._coarsen_minimal(selected)
        compact_cells = [
            CompactGridCell(
                space_code=code,
                level=h3.get_resolution(code),
                bbox=self._bbox_from_boundary(boundary_cache.get(code) or self._boundary_lnglat(code)),
            )
            for code in sorted(selected)
        ]
        return compact_cells, boundary_cache

    def code_to_geometry(self, code: str) -> dict:
        self._validate_code(code)
        coords = self._boundary_lnglat(code)
        return {"type": "Polygon", "coordinates": [coords]}

    def code_to_center(self, code: str) -> list[float]:
        self._validate_code(code)
        lat, lon = h3.cell_to_latlng(code)
        return [lon, lat]

    def code_to_bbox(self, code: str) -> list[float]:
        self._validate_code(code)
        ring = self._boundary_lnglat(code)
        lons = [p[0] for p in ring]
        lats = [p[1] for p in ring]
        return [min(lons), min(lats), max(lons), max(lats)]

    def neighbors(self, code: str, k: int = 1) -> list[str]:
        self._validate_code(code)
        if k < 1:
            raise ValidationError("k must be >= 1")
        out = set(h3.grid_disk(code, k))
        out.discard(code)
        return sorted(out)

    def parent(self, code: str) -> str:
        self._validate_code(code)
        level = h3.get_resolution(code)
        if level <= 1:
            raise ValidationError("Root ISEA4H level has no parent")
        return h3.cell_to_parent(code, level - 1)

    def children(self, code: str, target_level: int) -> list[str]:
        self._validate_code(code)
        self._validate_level(target_level)
        level = h3.get_resolution(code)
        if target_level <= level:
            raise ValidationError("target_level must be greater than current level")
        return sorted(h3.cell_to_children(code, target_level))

    def _build_cell(self, code: str, level: int, boundary: list[list[float]] | None = None) -> GridCell:
        boundary = boundary or self._boundary_lnglat(code)
        lons = [p[0] for p in boundary]
        lats = [p[1] for p in boundary]
        bbox = [min(lons), min(lats), max(lons), max(lats)]
        lat, lon = h3.cell_to_latlng(code)
        geometry = {"type": "Polygon", "coordinates": [boundary]}
        return GridCell(
            grid_type=self.grid_type,
            level=level,
            cell_id=code,
            space_code=code,
            center=[lon, lat],
            bbox=bbox,
            geometry=geometry,
            metadata={"precision": level, "zone": None, "facet": "h3"},
        )

    @lru_cache(maxsize=50000)
    def _boundary_cached(self, code: str) -> tuple[tuple[float, float], ...]:
        ring = tuple((lon, lat) for lat, lon in h3.cell_to_boundary(code))
        if ring and ring[0] != ring[-1]:
            ring = ring + (ring[0],)
        return ring

    @staticmethod
    def _ring_to_list(ring: tuple[tuple[float, float], ...]) -> list[list[float]]:
        return [[lon, lat] for lon, lat in ring]

    def _boundary_lnglat(self, code: str) -> list[list[float]]:
        return self._ring_to_list(self._boundary_cached(code))

    @staticmethod
    def _bbox_from_boundary(boundary: list[list[float]]) -> list[float]:
        lons = [p[0] for p in boundary]
        lats = [p[1] for p in boundary]
        return [min(lons), min(lats), max(lons), max(lats)]

    @staticmethod
    def _validate_level(level: int) -> None:
        if level < 1 or level > 12:
            raise ValidationError("ISEA4H level must be in [1, 12] for MVP")

    @staticmethod
    def _validate_code(code: str) -> None:
        if not h3.is_valid_cell(code):
            raise ValidationError("Invalid ISEA4H code")

    @staticmethod
    def _coarsen_minimal(codes: set[str]) -> set[str]:
        out = set(codes)
        changed = True
        while changed:
            changed = False
            grouped: dict[str, set[str]] = {}
            for code in out:
                level = h3.get_resolution(code)
                if level <= 1:
                    continue
                parent_code = h3.cell_to_parent(code, level - 1)
                grouped.setdefault(parent_code, set()).add(code)

            for parent_code, child_codes in grouped.items():
                child_level = h3.get_resolution(parent_code) + 1
                expected_children = set(h3.cell_to_children(parent_code, child_level))
                if expected_children == child_codes:
                    out.difference_update(child_codes)
                    out.add(parent_code)
                    changed = True
        return out
