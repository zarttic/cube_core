from __future__ import annotations

from collections import deque
from functools import lru_cache

import mgrs
from pyproj import Transformer
from shapely import affinity
from shapely.geometry import Polygon

from grid_core.app.core.enums import CoverMode
from grid_core.app.core.exceptions import ValidationError
from grid_core.app.models.compact_grid_cell import CompactGridCell
from grid_core.app.models.grid_cell import GridCell
from grid_core.app.utils.geometry import to_shapely


class MGRSEngine:
    grid_type = "mgrs"

    def __init__(self) -> None:
        self._converter = mgrs.MGRS()

    def locate_point(self, lon: float, lat: float, level: int) -> GridCell:
        app_level = self._validate_level(level)
        code = self.locate_space_code(lon=lon, lat=lat, level=app_level)
        return self._build_cell(code=code, level=app_level)

    def locate_space_code(self, lon: float, lat: float, level: int) -> str:
        app_level = self._validate_level(level)
        precision = self._to_precision(app_level)
        return self._converter.toMGRS(lat, lon, MGRSPrecision=precision)

    def cover_geometry(self, geometry: dict, level: int, cover_mode: str):
        compact_cells = self.cover_geometry_compact(geometry=geometry, level=level, cover_mode=cover_mode)
        return [self._build_cell(code=cell.space_code, level=cell.level) for cell in compact_cells]

    def cover_geometry_compact(self, geometry: dict, level: int, cover_mode: str) -> list[CompactGridCell]:
        app_level = self._validate_level(level)
        if cover_mode not in {CoverMode.INTERSECT.value, CoverMode.CONTAIN.value, CoverMode.MINIMAL.value}:
            raise ValidationError("MGRS cover supports only intersect/contain/minimal mode in MVP")

        shp = to_shapely(geometry)
        seeds = self._seed_points(shp)
        seed_codes = {
            self.locate_point(lon=lon, lat=lat, level=app_level).space_code for lon, lat in seeds if -90.0 <= lat <= 90.0
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

            cell_poly = self._geometry_shape(code)
            intersects = any(cell_poly.intersects(target_geom) for target_geom in self._wrapped_geometry_variants(shp))
            if not intersects:
                continue

            if cover_mode in {CoverMode.INTERSECT.value, CoverMode.MINIMAL.value} or any(
                target_geom.covers(cell_poly) for target_geom in self._wrapped_geometry_variants(shp)
            ):
                selected.add(code)

            if len(selected) > 20000:
                raise ValidationError("MGRS cover result too large for MVP")

            for neighbor in self.neighbors(code, k=1):
                if neighbor not in visited:
                    queue.append(neighbor)

        if cover_mode == CoverMode.MINIMAL.value:
            selected = self._coarsen_minimal(selected)

        return [
            CompactGridCell(space_code=code, level=self._level_from_code(code), bbox=self.code_to_bbox(code))
            for code in sorted(selected)
        ]

    def code_to_geometry(self, code: str):
        return self._geometry_from_corners(self._code_to_corners(code))

    def code_to_center(self, code: str):
        return self._center_from_bbox(self.code_to_bbox(code))

    def code_to_bbox(self, code: str):
        return list(self._code_to_bbox_cached(code))

    @lru_cache(maxsize=50000)
    def _code_to_bbox_cached(self, code: str) -> tuple[float, float, float, float]:
        return self._bbox_from_coords(self._code_to_corners(code))

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
        level = self._level_from_code(code)
        if level <= self._level_min():
            raise ValidationError("Root MGRS level has no parent")
        return code[:-2]

    def children(self, code: str, target_level: int):
        target_app_level = self._validate_level(target_level)
        current_app_level = self._level_from_code(code)
        if target_app_level <= current_app_level:
            raise ValidationError("target_level must be greater than current MGRS level")

        out = [code]
        for _ in range(target_app_level - current_app_level):
            out = [f"{prefix}{easting}{northing}" for prefix in out for easting in range(10) for northing in range(10)]
        return out

    def _build_cell(self, code: str, level: int) -> GridCell:
        bbox = self.code_to_bbox(code)
        geometry = self.code_to_geometry(code)
        return GridCell(
            grid_type=self.grid_type,
            level=level,
            cell_id=code,
            space_code=code,
            center=self._center_from_bbox(bbox),
            bbox=bbox,
            geometry=geometry,
            metadata={"precision": self._precision_from_code(code), "zone": code[:3], "facet": None},
        )

    @staticmethod
    def _level_min() -> int:
        return 1

    @staticmethod
    def _level_max() -> int:
        # MGRSPrecision supports 0..5, we expose it as app level 1..6.
        return 6

    @classmethod
    def _validate_level(cls, level: int) -> int:
        if level < cls._level_min() or level > cls._level_max():
            raise ValidationError(f"MGRS level must be in [{cls._level_min()}, {cls._level_max()}] for MVP")
        return level

    @classmethod
    def _to_precision(cls, level: int) -> int:
        # app level 1..6 -> MGRSPrecision 0..5
        return cls._validate_level(level) - 1

    @classmethod
    def _to_level(cls, precision: int) -> int:
        if precision < 0 or precision > 5:
            raise ValidationError("Invalid MGRS precision")
        return precision + 1

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

    @classmethod
    def _level_from_code(cls, code: str) -> int:
        return cls._to_level(cls._precision_from_code(code))

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
    def _geometry_from_corners(
        corners: list[list[float]] | tuple[tuple[float, float], ...],
    ) -> dict:
        return {
            "type": "Polygon",
            "coordinates": [
                [
                    [float(lon), float(lat)] for lon, lat in [*corners, corners[0]]
                ]
            ],
        }

    @lru_cache(maxsize=50000)
    def _code_to_corners(self, code: str) -> tuple[tuple[float, float], ...]:
        precision = self._precision_from_code(code)
        zone, hemisphere, easting, northing = self._converter.MGRSToUTM(code)
        cell_size_m = 10 ** (5 - precision)
        transformer = self._utm_to_lonlat(zone, hemisphere)
        corners = [
            transformer.transform(easting, northing),
            transformer.transform(easting + cell_size_m, northing),
            transformer.transform(easting + cell_size_m, northing + cell_size_m),
            transformer.transform(easting, northing + cell_size_m),
        ]
        normalized = self._normalize_longitudes(corners)
        return tuple((float(lon), float(lat)) for lon, lat in normalized)

    @staticmethod
    def _bbox_from_coords(
        corners: list[list[float]] | tuple[tuple[float, float], ...],
    ) -> tuple[float, float, float, float]:
        lons = [float(lon) for lon, _ in corners]
        lats = [float(lat) for _, lat in corners]
        return min(lons), min(lats), max(lons), max(lats)

    @staticmethod
    def _normalize_longitudes(corners: list[tuple[float, float]]) -> list[list[float]]:
        if not corners:
            return []
        normalized = [[float(corners[0][0]), float(corners[0][1])]]
        previous_lon = normalized[0][0]
        for lon, lat in corners[1:]:
            current_lon = float(lon)
            while current_lon - previous_lon > 180.0:
                current_lon -= 360.0
            while current_lon - previous_lon < -180.0:
                current_lon += 360.0
            normalized.append([current_lon, float(lat)])
            previous_lon = current_lon
        mean_lon = sum(lon for lon, _ in normalized) / len(normalized)
        shift = 0.0
        while mean_lon < -180.0:
            shift += 360.0
            mean_lon += 360.0
        while mean_lon > 180.0:
            shift -= 360.0
            mean_lon -= 360.0
        if shift:
            normalized = [[lon + shift, lat] for lon, lat in normalized]
        return normalized

    @staticmethod
    @lru_cache(maxsize=120)
    def _utm_to_lonlat(zone: int, hemisphere: str) -> Transformer:
        epsg = 32600 + zone if hemisphere == "N" else 32700 + zone
        return Transformer.from_crs(f"EPSG:{epsg}", "EPSG:4326", always_xy=True)

    def _geometry_shape(self, code: str) -> Polygon:
        corners = self._code_to_corners(code)
        return Polygon(corners)

    @staticmethod
    def _wrapped_geometry_variants(geometry) -> tuple:
        return (
            geometry,
            affinity.translate(geometry, xoff=360.0),
            affinity.translate(geometry, xoff=-360.0),
        )

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

    def _coarsen_minimal(self, codes: set[str]) -> set[str]:
        out = set(codes)
        changed = True
        while changed:
            changed = False
            grouped: dict[str, set[str]] = {}
            for code in out:
                precision = self._precision_from_code(code)
                if precision <= 0:
                    continue
                parent_code = code[:-2]
                grouped.setdefault(parent_code, set()).add(code)

            for parent_code, child_codes in grouped.items():
                if len(child_codes) != 100:
                    continue
                child_level = self._level_from_code(parent_code) + 1
                expected_children = set(self.children(parent_code, child_level))
                if expected_children == child_codes:
                    out.difference_update(child_codes)
                    out.add(parent_code)
                    changed = True
        return out
