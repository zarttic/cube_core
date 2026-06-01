from __future__ import annotations

import math
from functools import lru_cache

from shapely.geometry import box
from shapely.prepared import prep

from grid_core.app.core.enums import CoverMode
from grid_core.app.core.exceptions import ValidationError
from grid_core.app.models.compact_grid_cell import CompactGridCell
from grid_core.app.models.grid_cell import GridCell
from grid_core.app.utils.geometry import to_shapely


class TileMatrixEngine:
    grid_type = "tile_matrix"

    def locate_point(self, lon: float, lat: float, level: int) -> GridCell:
        self._validate_level(level)
        self._validate_lon_lat(lon, lat)
        code = self.locate_space_code(lon=lon, lat=lat, level=level)
        return self._build_cell(code)

    def locate_space_code(self, lon: float, lat: float, level: int) -> str:
        self._validate_level(level)
        self._validate_lon_lat(lon, lat)
        tile_size = self._tile_size(level)
        matrix_width, matrix_height = self._matrix_size(level)
        x = min(matrix_width - 1, max(0, int(math.floor((lon + 180.0) / tile_size))))
        y = min(matrix_height - 1, max(0, int(math.floor((90.0 - lat) / tile_size))))
        return self._format_code(level, x, y)

    def cover_geometry(self, geometry: dict, level: int, cover_mode: str) -> list[GridCell]:
        compact_cells = self.cover_geometry_compact(geometry=geometry, level=level, cover_mode=cover_mode)
        return [self._build_cell(cell.space_code) for cell in compact_cells]

    def cover_geometry_compact(self, geometry: dict, level: int, cover_mode: str) -> list[CompactGridCell]:
        self._validate_level(level)
        if cover_mode not in {CoverMode.INTERSECT.value, CoverMode.CONTAIN.value, CoverMode.MINIMAL.value}:
            raise ValidationError(f"Unsupported cover_mode: {cover_mode}")

        shp = to_shapely(geometry)
        prepared_shp = prep(shp)
        candidate_codes = self._candidate_codes_for_bounds(shp.bounds, level)
        selected: set[str] = set()
        for code in candidate_codes:
            cell_poly = box(*self.code_to_bbox(code))
            if cover_mode in {CoverMode.INTERSECT.value, CoverMode.MINIMAL.value} and prepared_shp.intersects(cell_poly):
                selected.add(code)
            elif cover_mode == CoverMode.CONTAIN.value and shp.covers(cell_poly):
                selected.add(code)

        if cover_mode == CoverMode.MINIMAL.value:
            selected = self._coarsen_minimal(selected)

        return [
            CompactGridCell(space_code=code, level=self._level_from_code(code), bbox=self.code_to_bbox(code))
            for code in sorted(selected, key=self._sort_key)
        ]

    def code_to_geometry(self, code: str) -> dict:
        bbox_values = self.code_to_bbox(code)
        min_lon, min_lat, max_lon, max_lat = bbox_values
        ring = [
            [min_lon, min_lat],
            [max_lon, min_lat],
            [max_lon, max_lat],
            [min_lon, max_lat],
            [min_lon, min_lat],
        ]
        return {"type": "Polygon", "coordinates": [ring]}

    def code_to_center(self, code: str) -> list[float]:
        min_lon, min_lat, max_lon, max_lat = self.code_to_bbox(code)
        return [(min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0]

    def code_to_bbox(self, code: str) -> list[float]:
        return list(self._code_to_bbox_cached(code))

    def neighbors(self, code: str, k: int = 1) -> list[str]:
        if k < 1:
            raise ValidationError("k must be >= 1")
        level, x, y = self._parse_code(code)
        matrix_width, matrix_height = self._matrix_size(level)
        out: list[str] = []
        for ny in range(max(0, y - k), min(matrix_height - 1, y + k) + 1):
            for nx in range(max(0, x - k), min(matrix_width - 1, x + k) + 1):
                if nx == x and ny == y:
                    continue
                out.append(self._format_code(level, nx, ny))
        return out

    def parent(self, code: str) -> str:
        level, x, y = self._parse_code(code)
        if level <= self._level_min():
            raise ValidationError("Root tile_matrix level has no parent")
        return self._format_code(level - 1, x // 2, y // 2)

    def children(self, code: str, target_level: int) -> list[str]:
        self._validate_level(target_level)
        level, x, y = self._parse_code(code)
        if target_level <= level:
            raise ValidationError("target_level must be greater than current tile_matrix level")
        scale = 2 ** (target_level - level)
        out: list[str] = []
        for child_y in range(y * scale, (y + 1) * scale):
            for child_x in range(x * scale, (x + 1) * scale):
                out.append(self._format_code(target_level, child_x, child_y))
        return out

    def _build_cell(self, code: str) -> GridCell:
        level, x, y = self._parse_code(code)
        bbox_values = self.code_to_bbox(code)
        return GridCell(
            grid_type=self.grid_type,
            level=level,
            cell_id=code,
            space_code=code,
            center=self.code_to_center(code),
            bbox=bbox_values,
            geometry=self.code_to_geometry(code),
            metadata={"tile_matrix_set": "WorldCRS84Quad", "x": x, "y": y, "facet": "planar"},
        )

    def _candidate_codes_for_bounds(self, bounds: tuple[float, float, float, float], level: int) -> list[str]:
        min_lon, min_lat, max_lon, max_lat = bounds
        min_lon = max(-180.0, float(min_lon))
        max_lon = min(180.0, float(max_lon))
        min_lat = max(-90.0, float(min_lat))
        max_lat = min(90.0, float(max_lat))
        if min_lon >= max_lon or min_lat >= max_lat:
            return []

        tile_size = self._tile_size(level)
        matrix_width, matrix_height = self._matrix_size(level)
        x_min = min(matrix_width - 1, max(0, int(math.floor((min_lon + 180.0) / tile_size))))
        x_max = min(matrix_width - 1, max(0, int(math.floor(((max_lon + 180.0) - 1e-12) / tile_size))))
        y_min = min(matrix_height - 1, max(0, int(math.floor((90.0 - max_lat) / tile_size))))
        y_max = min(matrix_height - 1, max(0, int(math.floor(((90.0 - min_lat) - 1e-12) / tile_size))))

        candidate_count = (x_max - x_min + 1) * (y_max - y_min + 1)
        if candidate_count > 1_000_000:
            raise ValidationError("tile_matrix cover result too large for MVP")
        return [
            self._format_code(level, x, y)
            for y in range(y_min, y_max + 1)
            for x in range(x_min, x_max + 1)
        ]

    @lru_cache(maxsize=200000)
    def _code_to_bbox_cached(self, code: str) -> tuple[float, float, float, float]:
        level, x, y = self._parse_code(code)
        tile_size = self._tile_size(level)
        min_lon = -180.0 + x * tile_size
        max_lon = min_lon + tile_size
        max_lat = 90.0 - y * tile_size
        min_lat = max_lat - tile_size
        return min_lon, min_lat, max_lon, max_lat

    @classmethod
    def _coarsen_minimal(cls, codes: set[str]) -> set[str]:
        out = set(codes)
        changed = True
        while changed:
            changed = False
            grouped: dict[str, set[str]] = {}
            for code in out:
                level, x, y = cls._parse_code(code)
                if level <= cls._level_min():
                    continue
                parent_code = cls._format_code(level - 1, x // 2, y // 2)
                grouped.setdefault(parent_code, set()).add(code)
            for parent_code, child_codes in grouped.items():
                parent_level, parent_x, parent_y = cls._parse_code(parent_code)
                expected = {
                    cls._format_code(parent_level + 1, parent_x * 2 + dx, parent_y * 2 + dy)
                    for dy in range(2)
                    for dx in range(2)
                }
                if child_codes == expected:
                    out.difference_update(child_codes)
                    out.add(parent_code)
                    changed = True
        return out

    @staticmethod
    def _format_code(level: int, x: int, y: int) -> str:
        return f"{level}/{x}/{y}"

    @classmethod
    def _parse_code(cls, code: str) -> tuple[int, int, int]:
        try:
            level_text, x_text, y_text = str(code).split("/")
            level = int(level_text)
            x = int(x_text)
            y = int(y_text)
        except Exception as exc:
            raise ValidationError("Invalid tile_matrix code") from exc
        cls._validate_level(level)
        matrix_width, matrix_height = cls._matrix_size(level)
        if x < 0 or x >= matrix_width or y < 0 or y >= matrix_height:
            raise ValidationError("tile_matrix code row or column out of range")
        return level, x, y

    @classmethod
    def _level_from_code(cls, code: str) -> int:
        level, _, _ = cls._parse_code(code)
        return level

    @staticmethod
    def _sort_key(code: str) -> tuple[int, int, int]:
        level, x, y = TileMatrixEngine._parse_code(code)
        return level, y, x

    @staticmethod
    def _level_min() -> int:
        return 1

    @staticmethod
    def _level_max() -> int:
        return 12

    @classmethod
    def _validate_level(cls, level: int) -> None:
        if level < cls._level_min() or level > cls._level_max():
            raise ValidationError(f"tile_matrix level must be in [{cls._level_min()}, {cls._level_max()}]")

    @staticmethod
    def _validate_lon_lat(lon: float, lat: float) -> None:
        if lon < -180.0 or lon > 180.0:
            raise ValidationError("Point longitude must be in [-180, 180]")
        if lat < -90.0 or lat > 90.0:
            raise ValidationError("Point latitude must be in [-90, 90]")

    @staticmethod
    def _tile_size(level: int) -> float:
        return 180.0 / (2**level)

    @staticmethod
    def _matrix_size(level: int) -> tuple[int, int]:
        return 2 ** (level + 1), 2**level
