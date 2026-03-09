from __future__ import annotations

import mgrs

from grid_core.app.core.exceptions import NotImplementedCapabilityError, ValidationError
from grid_core.app.models.grid_cell import GridCell


class MGRSEngine:
    grid_type = "mgrs"

    def __init__(self) -> None:
        self._converter = mgrs.MGRS()

    def locate_point(self, lon: float, lat: float, level: int) -> GridCell:
        precision = self._validate_level(level)
        code = self._converter.toMGRS(lat, lon, MGRSPrecision=precision)
        return self._build_cell(code=code, level=level)

    def cover_geometry(self, geometry: dict, level: int, cover_mode: str):
        raise NotImplementedCapabilityError("MGRS cover geometry is not implemented yet")

    def code_to_geometry(self, code: str):
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

    def code_to_center(self, code: str):
        min_lon, min_lat, max_lon, max_lat = self.code_to_bbox(code)
        return [(min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0]

    def code_to_bbox(self, code: str):
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

        return [sw_lon, sw_lat, ne_lon, ne_lat]

    def neighbors(self, code: str, k: int = 1):
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
        return sorted(neighbors)

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
        center = self.code_to_center(code)
        return GridCell(
            grid_type=self.grid_type,
            level=level,
            cell_id=code,
            space_code=code,
            center=center,
            bbox=bbox,
            geometry=self.code_to_geometry(code),
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
