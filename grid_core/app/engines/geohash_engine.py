from __future__ import annotations

from shapely.geometry import box

from grid_core.app.core.enums import CoverMode
from grid_core.app.core.exceptions import ValidationError
from grid_core.app.models.grid_cell import GridCell
from grid_core.app.utils import geohash_utils
from grid_core.app.utils.geometry import to_shapely


class GeohashEngine:
    grid_type = "geohash"

    def locate_point(self, lon: float, lat: float, level: int) -> GridCell:
        self._validate_level(level)
        code = geohash_utils.encode(lon, lat, precision=level)
        return self._build_cell(code, level)

    def cover_geometry(self, geometry: dict, level: int, cover_mode: str) -> list[GridCell]:
        self._validate_level(level)
        if cover_mode not in {CoverMode.INTERSECT.value, CoverMode.CONTAIN.value, CoverMode.MINIMAL.value}:
            raise ValidationError(f"Unsupported cover_mode: {cover_mode}")

        shp = to_shapely(geometry)
        lon_step, lat_step = geohash_utils.cell_size(level)
        lon_bits, lat_bits = geohash_utils.bits_for_precision(level)
        lon_bins = 2**lon_bits
        lat_bins = 2**lat_bits

        geoms = list(shp.geoms) if hasattr(shp, "geoms") else [shp]
        candidate_indices: set[tuple[int, int]] = set()
        for geom in geoms:
            min_lon, min_lat, max_lon, max_lat = geom.bounds
            min_ix = max(0, int((min_lon + 180.0) // lon_step) - 1)
            max_ix = min(lon_bins - 1, int((max_lon + 180.0) // lon_step) + 1)
            min_iy = max(0, int((min_lat + 90.0) // lat_step) - 1)
            max_iy = min(lat_bins - 1, int((max_lat + 90.0) // lat_step) + 1)
            for ix in range(min_ix, max_ix + 1):
                for iy in range(min_iy, max_iy + 1):
                    candidate_indices.add((ix, iy))

        selected_codes: set[str] = set()
        for ix, iy in sorted(candidate_indices):
            code = geohash_utils.from_grid_index(ix, iy, level)
            bbox = geohash_utils.decode_bbox(code)
            cell_poly = box(*bbox)
            if cover_mode == CoverMode.INTERSECT.value and cell_poly.intersects(shp):
                selected_codes.add(code)
            elif cover_mode == CoverMode.CONTAIN.value and shp.covers(cell_poly):
                selected_codes.add(code)
            elif cover_mode == CoverMode.MINIMAL.value and cell_poly.intersects(shp):
                selected_codes.add(code)

        return [self._build_cell(code, level) for code in sorted(selected_codes)]

    def code_to_geometry(self, code: str) -> dict:
        return geohash_utils.polygon_from_bbox(geohash_utils.decode_bbox(code))

    def code_to_center(self, code: str) -> list[float]:
        lon, lat = geohash_utils.decode_center(code)
        return [lon, lat]

    def code_to_bbox(self, code: str) -> list[float]:
        return list(geohash_utils.decode_bbox(code))

    def neighbors(self, code: str, k: int = 1) -> list[str]:
        if k < 1:
            raise ValidationError("k must be >= 1")
        return geohash_utils.neighbors(code, k=k)

    def parent(self, code: str) -> str:
        if len(code) <= 1:
            raise ValidationError("Root geohash has no parent")
        return code[:-1]

    def children(self, code: str, target_level: int) -> list[str]:
        current = len(code)
        if target_level <= current:
            raise ValidationError("target_level must be greater than code length")
        out = [code]
        for _ in range(target_level - current):
            out = [prefix + ch for prefix in out for ch in geohash_utils.BASE32]
        return out

    def _build_cell(self, code: str, level: int) -> GridCell:
        bbox = list(geohash_utils.decode_bbox(code))
        center = list(geohash_utils.decode_center(code))
        return GridCell(
            grid_type=self.grid_type,
            level=level,
            cell_id=code,
            space_code=code,
            center=center,
            bbox=bbox,
            geometry=geohash_utils.polygon_from_bbox(tuple(bbox)),
            metadata={"precision": level, "zone": None, "facet": None},
        )

    @staticmethod
    def _validate_level(level: int) -> None:
        if level < 1 or level > 12:
            raise ValidationError("level must be in [1, 12]")
