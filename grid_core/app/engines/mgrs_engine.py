from __future__ import annotations

from grid_core.app.core.exceptions import NotImplementedCapabilityError


class MGRSEngine:
    grid_type = "mgrs"

    def locate_point(self, lon: float, lat: float, level: int):
        raise NotImplementedCapabilityError("MGRS engine is not implemented yet")

    def cover_geometry(self, geometry: dict, level: int, cover_mode: str):
        raise NotImplementedCapabilityError("MGRS engine is not implemented yet")

    def code_to_geometry(self, code: str):
        raise NotImplementedCapabilityError("MGRS engine is not implemented yet")

    def code_to_center(self, code: str):
        raise NotImplementedCapabilityError("MGRS engine is not implemented yet")

    def code_to_bbox(self, code: str):
        raise NotImplementedCapabilityError("MGRS engine is not implemented yet")

    def neighbors(self, code: str, k: int = 1):
        raise NotImplementedCapabilityError("MGRS engine is not implemented yet")

    def parent(self, code: str):
        raise NotImplementedCapabilityError("MGRS engine is not implemented yet")

    def children(self, code: str, target_level: int):
        raise NotImplementedCapabilityError("MGRS engine is not implemented yet")
