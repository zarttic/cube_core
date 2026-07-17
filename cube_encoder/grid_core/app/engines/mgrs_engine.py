"""MGRS UTM/UPS grid engine: implements BaseGridEngine with GridAddress protocol."""
from __future__ import annotations

import mgrs as mgrs_lib

from grid_core.app.core.exceptions import ValidationError
from grid_core.app.engines.base import BaseGridEngine
from grid_core.app.engines.mgrs.address import canonicalize_mgrs
from grid_core.app.engines.mgrs.cover import cover_geometry as _cover_geometry
from grid_core.app.engines.mgrs.geometry import (
    cell_bbox,
    cell_center,
    cell_geometry_clipped,
    cell_geometry_to_geojson,
)
from grid_core.app.engines.mgrs.topology import (
    _domain_for_address,
    children_addresses,
    neighbors_for_address,
    parent_address,
)
from grid_core.app.models.compact_grid_cell import CompactGridCell
from grid_core.app.models.grid_address import GridAddress
from grid_core.app.models.grid_cell import GridCell

_converter = mgrs_lib.MGRS()

_PRECISION_MIN = 0
_PRECISION_MAX = 5


class MGRSEngine(BaseGridEngine):
    """Standard MGRS grid engine with full UTM/UPS support.

    Grid level == MGRS numeric precision (0-5):
      0 = 100 km cell
      1 = 10 km cell
      2 = 1 km cell
      3 = 100 m cell
      4 = 10 m cell
      5 = 1 m cell
    """

    grid_type = "mgrs"

    # ------------------------------------------------------------------
    # Public API — location
    # ------------------------------------------------------------------

    def locate_point(self, lon: float, lat: float, requested_grid_level: int) -> GridCell:
        precision = self._validate_precision(requested_grid_level)
        address = self.locate_space_code(lon, lat, precision)
        return self._address_to_cell(address)

    def locate_space_code(self, lon: float, lat: float, requested_grid_level: int) -> GridAddress:
        precision = self._validate_precision(requested_grid_level)
        try:
            code = _converter.toMGRS(lat, lon, MGRSPrecision=precision)
        except Exception as exc:
            raise ValidationError(f"Cannot encode ({lon}, {lat}) at precision {precision}") from exc
        canonical = canonicalize_mgrs(code)
        return GridAddress(
            grid_type=self.grid_type,
            grid_level=precision,
            space_code=canonical,
            topology_code=None,
        )

    # ------------------------------------------------------------------
    # Public API — cover
    # ------------------------------------------------------------------

    def cover_geometry(
        self, geometry: dict, requested_grid_level: int, cover_mode: str
    ) -> list[GridCell]:
        compact = self.cover_geometry_compact(geometry, requested_grid_level, cover_mode)
        return [self._compact_to_cell(c) for c in compact]

    def cover_geometry_compact(
        self, geometry: dict, requested_grid_level: int, cover_mode: str
    ) -> list[CompactGridCell]:
        precision = self._validate_precision(requested_grid_level)
        return _cover_geometry(geometry, precision, cover_mode)

    # ------------------------------------------------------------------
    # Public API — geometry
    # ------------------------------------------------------------------

    def code_to_geometry(self, address: GridAddress) -> dict:
        geom = self._clipped_geom(address)
        return cell_geometry_to_geojson(geom)

    def code_to_center(self, address: GridAddress) -> list[float]:
        geom = self._clipped_geom(address)
        return cell_center(geom)

    def code_to_bbox(self, address: GridAddress) -> list[float]:
        geom = self._clipped_geom(address)
        return cell_bbox(geom)

    # ------------------------------------------------------------------
    # Public API — topology
    # ------------------------------------------------------------------

    def neighbors(self, address: GridAddress, k: int = 1) -> list[GridAddress]:
        return neighbors_for_address(address, k)

    def parent(self, address: GridAddress) -> GridAddress:
        return parent_address(address)

    def children(self, address: GridAddress, target_grid_level: int) -> list[GridAddress]:
        return children_addresses(address, target_grid_level)

    # ------------------------------------------------------------------
    # Domain helpers (exposed for tests)
    # ------------------------------------------------------------------

    def domain_geometry(self, address: GridAddress) -> dict:
        """Return the WGS84 valid-domain polygon for the given address's domain."""
        from shapely.geometry import mapping

        from grid_core.app.engines.mgrs.domain import domain_polygon
        domain = _domain_for_address(address.space_code)
        return dict(mapping(domain_polygon(domain)))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_precision(level: int) -> int:
        if level < _PRECISION_MIN or level > _PRECISION_MAX:
            raise ValidationError(
                f"MGRS requested_grid_level must be in [{_PRECISION_MIN}, {_PRECISION_MAX}]"
            )
        return level

    def _clipped_geom(self, address: GridAddress):
        precision = address.grid_level
        code = address.space_code
        domain = _domain_for_address(code)
        return cell_geometry_clipped(code, precision, domain)

    def _address_to_cell(self, address: GridAddress) -> GridCell:
        geom = self._clipped_geom(address)
        return GridCell(
            grid_type=self.grid_type,
            grid_level=address.grid_level,
            space_code=address.space_code,
            topology_code=address.topology_code,
            center=cell_center(geom),
            bbox=cell_bbox(geom),
            geometry=cell_geometry_to_geojson(geom),
            metadata={
                "precision": address.grid_level,
                "domain": _domain_for_address(address.space_code).token,
            },
        )

    def _compact_to_cell(self, compact: CompactGridCell) -> GridCell:
        addr = GridAddress(
            grid_type=compact.grid_type,
            grid_level=compact.grid_level,
            space_code=compact.space_code,
            topology_code=compact.topology_code,
        )
        return self._address_to_cell(addr)
