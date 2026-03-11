from __future__ import annotations

from collections import deque
from functools import lru_cache

import mgrs
from shapely.geometry import box

from grid_core.app.core.enums import CoverMode
from grid_core.app.core.exceptions import ValidationError
from grid_core.app.models.grid_cell import GridCell
from grid_core.app.utils.geometry import to_shapely


class MGRSEngine:
    grid_type = "mgrs"

    def __init__(self) -> None:
        self._converter = mgrs.MGRS()

    def locate_point(self, lon: float, lat: float, level: int) -> GridCell:
        precision = self._validate_level(level)
        code = self._converter.toMGRS(lat, lon, MGRSPrecision=precision)
        return self._build_cell(code=code, level=level)

    def cover_geometry(self, geometry: dict, level: int, cover_mode: str):
        precision = self._validate_level(level)
        if cover_mode not in {CoverMode.INTERSECT.value, CoverMode.CONTAIN.value, CoverMode.MINIMAL.value}:
            raise ValidationError("MGRS cover supports only intersect/contain/minimal mode in MVP")

        shp = to_shapely(geometry)
        seeds = self._seed_points(shp)
        seed_codes = {
            self.locate_point(lon=lon, lat=lat, level=precision).space_code for lon, lat in seeds if -90.0 <= lat <= 90.0
        }
        if not seed_codes:
            return []

        selected: set[str] = set()
        visited: set[str] = set()
        queue = deque(sorted(seed_codes))

        while queue:
            code = queue.popleft()
            if code in visited:
                continue
            visited.add(code)

            cell_poly = box(*self.code_to_bbox(code))
            intersects = cell_poly.intersects(shp)
            if not intersects:
                continue

            if cover_mode in {CoverMode.INTERSECT.value, CoverMode.MINIMAL.value} or shp.covers(cell_poly):
                selected.add(code)

            if len(selected) > 20000:
                raise ValidationError("MGRS cover result too large for MVP")

            for neighbor in self.neighbors(code, k=1):
                if neighbor not in visited:
                    queue.append(neighbor)

        return [self._build_cell(code=code, level=precision) for code in sorted(selected)]

    def code_to_geometry(self, code: str):
        return self._geometry_from_bbox(self.code_to_bbox(code))

    def code_to_center(self, code: str):
        return self._center_from_bbox(self.code_to_bbox(code))

    def code_to_bbox(self, code: str):
        return list(self._code_to_bbox_cached(code))

    @lru_cache(maxsize=50000)
    def _code_to_bbox_cached(self, code: str) -> tuple[float, float, float, float]:
        precision = self._precision_from_code(code)
        zone, hemisphere, easting, northing = self._converter.MGRSToUTM(code)
        cell_size_m = 10 ** (5 - precision)

        sw_code = self._converter.UTMToMGRS(zone, hemisphere, easting, northing, MGRSPrecision=5)
        ne_code = self._converter.UTMToMGRS(
            zone,
            hemisphere,
            easting + cell_size_m,
            northing + cell_size_m,
            MGRSPrecision=5,
        )
        sw_lat, sw_lon = self._converter.toLatLon(sw_code)
        ne_lat, ne_lon = self._converter.toLatLon(ne_code)

        return sw_lon, sw_lat, ne_lon, ne_lat

    def neighbors(self, code: str, k: int = 1):
        return list(self._neighbors_cached(code, k))

    @lru_cache(maxsize=50000)
    def _neighbors_cached(self, code: str, k: int) -> tuple[str, ...]:
        if k < 1:
            raise ValidationError("k must be >= 1")
        precision = self._precision_from_code(code)
        zone, hemisphere, easting, northing = self._parse_utm(code)
        cell_size_m = 10 ** (5 - precision)

        neighbors: set[str] = set()
        for dx in range(-k, k + 1):
            for dy in range(-k, k + 1):
                if dx == 0 and dy == 0:
                    continue
                try:
                    neighbor = self._converter.UTMToMGRS(
                        zone,
                        hemisphere,
                        easting + dx * cell_size_m,
                        northing + dy * cell_size_m,
                        MGRSPrecision=precision,
                    )
                except Exception:
                    continue
                if self._precision_from_code(neighbor) == precision:
                    neighbors.add(neighbor)
        return tuple(sorted(neighbors))

    def parent(self, code: str):
        precision = self._precision_from_code(code)
        if precision <= 1:
            raise ValidationError("Root MGRS level has no parent")
        return code[:-2]

    def children(self, code: str, target_level: int):
        target_precision = self._validate_level(target_level)
        current_precision = self._precision_from_code(code)
        if target_precision <= current_precision:
            raise ValidationError("target_level must be greater than current MGRS level")

        out = [code]
        for _ in range(target_precision - current_precision):
            out = [f"{prefix}{easting}{northing}" for prefix in out for easting in range(10) for northing in range(10)]
        return out

    def _build_cell(self, code: str, level: int) -> GridCell:
        bbox = self.code_to_bbox(code)
        return GridCell(
            grid_type=self.grid_type,
            level=level,
            cell_id=code,
            space_code=code,
            center=self._center_from_bbox(bbox),
            bbox=bbox,
            geometry=self._geometry_from_bbox(bbox),
            metadata={"precision": self._precision_from_code(code), "zone": code[:3], "facet": None},
        )

    @staticmethod
    def _validate_level(level: int) -> int:
        if level < 1 or level > 5:
            raise ValidationError("MGRS level must be in [1, 5] for MVP")
        return level

    @staticmethod
    def _precision_from_code(code: str) -> int:
        if len(code) < 5:
            raise ValidationError("Invalid MGRS code")
        suffix_len = len(code) - 5
        if suffix_len % 2 != 0:
            raise ValidationError("Invalid MGRS precision digits")
        precision = suffix_len // 2
        if precision < 0 or precision > 5:
            raise ValidationError("Invalid MGRS precision")
        return precision

    def _parse_utm(self, code: str) -> tuple[int, str, float, float]:
        try:
            return self._converter.MGRSToUTM(code)
        except Exception as exc:
            raise ValidationError("Invalid MGRS code") from exc

    @staticmethod
    def _center_from_bbox(bbox: list[float] | tuple[float, float, float, float]) -> list[float]:
        min_lon, min_lat, max_lon, max_lat = bbox
        return [(min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0]

    @staticmethod
    def _geometry_from_bbox(bbox: list[float] | tuple[float, float, float, float]) -> dict:
        min_lon, min_lat, max_lon, max_lat = bbox
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

    @staticmethod
    def _seed_points(shp) -> list[tuple[float, float]]:
        points: set[tuple[float, float]] = set()
        geoms = list(shp.geoms) if hasattr(shp, "geoms") else [shp]

        for geom in geoms:
            rp = geom.representative_point()
            points.add((float(rp.x), float(rp.y)))

            min_lon, min_lat, max_lon, max_lat = geom.bounds
            mid_lon = (min_lon + max_lon) / 2.0
            mid_lat = (min_lat + max_lat) / 2.0
            points.update(
                {
                    (min_lon, min_lat),
                    (min_lon, max_lat),
                    (max_lon, min_lat),
                    (max_lon, max_lat),
                    (mid_lon, min_lat),
                    (mid_lon, max_lat),
                    (min_lon, mid_lat),
                    (max_lon, mid_lat),
                    (mid_lon, mid_lat),
                }
            )

        return sorted(points)
