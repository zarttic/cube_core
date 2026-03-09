from __future__ import annotations

from shapely.geometry import box

from grid_core.app.core.enums import CoverMode
from grid_core.app.core.exceptions import ValidationError
from grid_core.app.models.grid_cell import GridCell
from grid_core.app.utils.geometry import to_shapely


class ISEA4HEngine:
    grid_type = "isea4h"

    def locate_point(self, lon: float, lat: float, level: int) -> GridCell:
        self._validate_level(level)
        ix, iy = self._point_to_index(lon, lat, level)
        code = self._encode(level, ix, iy)
        return self._build_cell(code, level)

    def cover_geometry(self, geometry: dict, level: int, cover_mode: str) -> list[GridCell]:
        self._validate_level(level)
        if cover_mode not in {CoverMode.INTERSECT.value, CoverMode.CONTAIN.value, CoverMode.MINIMAL.value}:
            raise ValidationError(f"Unsupported cover_mode: {cover_mode}")

        shp = to_shapely(geometry)
        lon_step, lat_step, lon_bins, lat_bins = self._grid_step(level)
        geoms = list(shp.geoms) if hasattr(shp, "geoms") else [shp]
        candidates: set[tuple[int, int]] = set()
        for geom in geoms:
            min_lon, min_lat, max_lon, max_lat = geom.bounds
            min_ix = max(0, int((min_lon + 180.0) // lon_step) - 1)
            max_ix = min(lon_bins - 1, int((max_lon + 180.0) // lon_step) + 1)
            min_iy = max(0, int((min_lat + 90.0) // lat_step) - 1)
            max_iy = min(lat_bins - 1, int((max_lat + 90.0) // lat_step) + 1)
            for ix in range(min_ix, max_ix + 1):
                for iy in range(min_iy, max_iy + 1):
                    candidates.add((ix, iy))

        selected: set[str] = set()
        for ix, iy in sorted(candidates):
            code = self._encode(level, ix, iy)
            cell_poly = box(*self.code_to_bbox(code))
            if cover_mode == CoverMode.INTERSECT.value and cell_poly.intersects(shp):
                selected.add(code)
            elif cover_mode == CoverMode.CONTAIN.value and shp.covers(cell_poly):
                selected.add(code)
            elif cover_mode == CoverMode.MINIMAL.value and cell_poly.intersects(shp):
                selected.add(code)

        return [self._build_cell(code, level) for code in sorted(selected)]

    def code_to_geometry(self, code: str) -> dict:
        min_lon, min_lat, max_lon, max_lat = self.code_to_bbox(code)
        return {
            "type": "Polygon",
            "coordinates": [
                [
                    [min_lon, min_lat],
                    [max_lon, min_lat],
                    [max_lon, max_lat],
                    [min_lon, max_lat],
                    [min_lon, min_lat],
                ]
            ],
        }

    def code_to_center(self, code: str) -> list[float]:
        min_lon, min_lat, max_lon, max_lat = self.code_to_bbox(code)
        return [(min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0]

    def code_to_bbox(self, code: str) -> list[float]:
        level, ix, iy = self._decode(code)
        lon_step, lat_step, _, _ = self._grid_step(level)
        min_lon = -180.0 + ix * lon_step
        max_lon = min_lon + lon_step
        min_lat = -90.0 + iy * lat_step
        max_lat = min_lat + lat_step
        return [min_lon, min_lat, max_lon, max_lat]

    def neighbors(self, code: str, k: int = 1) -> list[str]:
        if k < 1:
            raise ValidationError("k must be >= 1")
        level, ix, iy = self._decode(code)
        _, _, lon_bins, lat_bins = self._grid_step(level)
        out: set[str] = set()
        for dx in range(-k, k + 1):
            for dy in range(-k, k + 1):
                if dx == 0 and dy == 0:
                    continue
                nx = ix + dx
                ny = iy + dy
                if 0 <= nx < lon_bins and 0 <= ny < lat_bins:
                    out.add(self._encode(level, nx, ny))
        return sorted(out)

    def parent(self, code: str) -> str:
        level, ix, iy = self._decode(code)
        if level <= 1:
            raise ValidationError("Root ISEA4H level has no parent")
        return self._encode(level - 1, ix // 2, iy // 2)

    def children(self, code: str, target_level: int) -> list[str]:
        self._validate_level(target_level)
        level, ix, iy = self._decode(code)
        if target_level <= level:
            raise ValidationError("target_level must be greater than current level")

        out = [(ix, iy)]
        for _ in range(target_level - level):
            out = [(x * 2 + dx, y * 2 + dy) for x, y in out for dx in range(2) for dy in range(2)]
        return [self._encode(target_level, x, y) for x, y in out]

    def _build_cell(self, code: str, level: int) -> GridCell:
        bbox = self.code_to_bbox(code)
        return GridCell(
            grid_type=self.grid_type,
            level=level,
            cell_id=code,
            space_code=code,
            center=self.code_to_center(code),
            bbox=bbox,
            geometry=self.code_to_geometry(code),
            metadata={"precision": level, "zone": None, "facet": None},
        )

    @staticmethod
    def _grid_step(level: int) -> tuple[float, float, int, int]:
        lon_bins = 2 ** (level + 1)
        lat_bins = 2**level
        return 360.0 / lon_bins, 180.0 / lat_bins, lon_bins, lat_bins

    def _point_to_index(self, lon: float, lat: float, level: int) -> tuple[int, int]:
        lon_step, lat_step, lon_bins, lat_bins = self._grid_step(level)
        lon_norm = min(max(lon, -180.0), 180.0 - 1e-12)
        lat_norm = min(max(lat, -90.0), 90.0 - 1e-12)
        ix = int((lon_norm + 180.0) // lon_step)
        iy = int((lat_norm + 90.0) // lat_step)
        return min(ix, lon_bins - 1), min(iy, lat_bins - 1)

    @staticmethod
    def _encode(level: int, ix: int, iy: int) -> str:
        return f"HX{level}-{ix}-{iy}"

    def _decode(self, code: str) -> tuple[int, int, int]:
        parts = code.split("-")
        if len(parts) != 3 or not parts[0].startswith("HX"):
            raise ValidationError("Invalid ISEA4H code")
        try:
            level = int(parts[0][2:])
            ix = int(parts[1])
            iy = int(parts[2])
        except ValueError as exc:
            raise ValidationError("Invalid ISEA4H code") from exc
        self._validate_level(level)
        _, _, lon_bins, lat_bins = self._grid_step(level)
        if ix < 0 or iy < 0 or ix >= lon_bins or iy >= lat_bins:
            raise ValidationError("Invalid ISEA4H code index")
        return level, ix, iy

    @staticmethod
    def _validate_level(level: int) -> None:
        if level < 1 or level > 12:
            raise ValidationError("ISEA4H level must be in [1, 12] for MVP")
