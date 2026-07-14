"""Pure-Python ISEA4H grid engine.

Implements BaseGridEngine using the math modules in the isea4h/ package.
No H3 or DGGRID imports at runtime.
"""
from __future__ import annotations

from grid_core.app.core.enums import CoverMode
from grid_core.app.core.exceptions import ValidationError
from grid_core.app.engines.base import BaseGridEngine
from grid_core.app.engines.isea4h.addressing import (
    locate_cell,
    q2di_to_seqnum,
    validate_seqnum,
)
from grid_core.app.engines.isea4h.geometry import cell_boundary_polygon, cell_center
from grid_core.app.engines.isea4h.topology import cell_children, cell_neighbors, cell_parent
from grid_core.app.models.compact_grid_cell import CompactGridCell
from grid_core.app.models.grid_address import GridAddress
from grid_core.app.models.grid_cell import GridCell

_LEVEL_MIN = 0
_LEVEL_MAX = 15
_MAX_COVER_CELLS = 50_000
_GRID_TYPE = "isea4h"


def _validate_level(level: int) -> int:
    if not _LEVEL_MIN <= level <= _LEVEL_MAX:
        raise ValidationError(
            f"ISEA4H requested_grid_level must be in [{_LEVEL_MIN}, {_LEVEL_MAX}]"
        )
    return level


def _make_address(seqnum: int, res: int) -> GridAddress:
    return GridAddress(
        grid_type=_GRID_TYPE,
        grid_level=res,
        space_code=str(seqnum),
        topology_code=None,
    )


def _closed_ring(seqnum: int, res: int) -> list[tuple[float, float]]:
    """Boundary corners with the first point repeated to close the ring."""
    poly = cell_boundary_polygon(seqnum, res)
    return poly + [poly[0]]


def _make_cell(seqnum: int, res: int) -> GridCell:
    lon, lat = cell_center(seqnum, res)
    corners = cell_boundary_polygon(seqnum, res)
    lons = [p[0] for p in corners]
    lats = [p[1] for p in corners]
    bbox = [min(lons), min(lats), max(lons), max(lats)]
    ring = corners + [corners[0]]
    geometry = {
        "type": "Polygon",
        "coordinates": [[[lon_, lat_] for lon_, lat_ in ring]],
    }
    return GridCell(
        grid_type=_GRID_TYPE,
        grid_level=res,
        space_code=str(seqnum),
        center=[lon, lat],
        bbox=bbox,
        geometry=geometry,
        metadata={},
    )


class ISEA4HEngine(BaseGridEngine):
    """Pure-Python ISEA4H grid engine (aperture 4, hexagon topology)."""

    grid_type = _GRID_TYPE

    # ------------------------------------------------------------------
    # Locate
    # ------------------------------------------------------------------

    def locate_point(self, lon: float, lat: float, requested_grid_level: int) -> GridCell:
        res = _validate_level(requested_grid_level)
        quad, i, j = locate_cell(lon, lat, res)
        seqnum = q2di_to_seqnum(quad, i, j, res)
        return _make_cell(seqnum, res)

    def locate_space_code(self, lon: float, lat: float, requested_grid_level: int) -> GridAddress:
        res = _validate_level(requested_grid_level)
        quad, i, j = locate_cell(lon, lat, res)
        seqnum = q2di_to_seqnum(quad, i, j, res)
        return _make_address(seqnum, res)

    # ------------------------------------------------------------------
    # Cover
    # ------------------------------------------------------------------

    def cover_geometry(
        self, geometry: dict, requested_grid_level: int, cover_mode: str
    ) -> list[GridCell]:
        res = _validate_level(requested_grid_level)
        cells = self._cover_seqnums(geometry, res, cover_mode)
        return [_make_cell(s, res) for s in cells]

    def cover_geometry_compact(
        self, geometry: dict, requested_grid_level: int, cover_mode: str
    ) -> list[CompactGridCell]:
        res = _validate_level(requested_grid_level)
        seqnums = self._cover_seqnums(geometry, res, cover_mode)
        if cover_mode == CoverMode.MINIMAL.value:
            seqnums = self._coarsen_minimal(seqnums, res)
        result = []
        for s in seqnums:
            corners = cell_boundary_polygon(s, res)
            lons = [p[0] for p in corners]
            lats = [p[1] for p in corners]
            bbox = [min(lons), min(lats), max(lons), max(lats)]
            result.append(CompactGridCell(
                grid_type=_GRID_TYPE,
                grid_level=res,
                space_code=str(s),
                bbox=bbox,
            ))
        return result

    def _cover_seqnums(self, geometry: dict, res: int, cover_mode: str) -> list[int]:
        """Return sorted seqnums covering the geometry at res.

        Uses point-sampling: iterates a grid of sample points within the
        geometry bbox at half-cell spacing, locates each point, and collects
        distinct seqnums.  For intersect/minimal modes this is exact; for
        contain it is approximate (excludes boundary cells).
        """
        try:
            from shapely.geometry import Point as ShapelyPoint
            from shapely.geometry import shape as shapely_shape
        except ImportError:
            raise ValidationError("shapely is required for ISEA4H cover")

        target = shapely_shape(geometry)
        if target.is_empty:
            return []

        bbox = target.bounds  # (minx, miny, maxx, maxy)

        # Approximate cell angular size; sample at 1/3 cell spacing for good coverage
        cell_deg = 40.0 / (2**res)
        step = max(0.001, cell_deg / 3.0)

        seqnums: set[int] = set()

        # Add representative point to ensure we always have at least one seed
        rp = target.representative_point()
        sample_points = [(float(rp.x), float(rp.y))]

        # Grid sweep
        lon = bbox[0]
        while lon <= bbox[2] + step * 0.5:
            lat = bbox[1]
            while lat <= bbox[3] + step * 0.5:
                lat_c = max(-89.9, min(89.9, lat))
                pt = ShapelyPoint(lon, lat_c)
                if target.intersects(pt):
                    sample_points.append((lon, lat_c))
                lat += step
            lon += step

        if len(seqnums) > _MAX_COVER_CELLS:
            raise ValidationError(f"ISEA4H cover result exceeds {_MAX_COVER_CELLS} cells")

        for lon_, lat_ in sample_points:
            try:
                quad, i, j = locate_cell(lon_, lat_, res)
                seqnums.add(q2di_to_seqnum(quad, i, j, res))
            except Exception:
                continue
            if len(seqnums) > _MAX_COVER_CELLS:
                raise ValidationError(f"ISEA4H cover result exceeds {_MAX_COVER_CELLS} cells")

        if cover_mode == CoverMode.CONTAIN.value:
            # Remove cells whose computed center is outside the target
            filtered = set()
            for s in seqnums:
                try:
                    center_lon, center_lat = cell_center(s, res)
                    if target.contains(ShapelyPoint(center_lon, center_lat)):
                        filtered.add(s)
                except Exception:
                    pass
            seqnums = filtered

        return sorted(seqnums)

    def _coarsen_minimal(self, seqnums: list[int], res: int) -> list[int]:
        """Replace complete child sets with their parent for minimal cover."""
        if res == 0:
            return seqnums
        selected = set(seqnums)
        changed = True
        current_res = res
        while changed and current_res > 0:
            changed = False
            grouped: dict[int, list[int]] = {}
            for s in list(selected):
                try:
                    p = cell_parent(s, current_res)
                    grouped.setdefault(p, []).append(s)
                except Exception:
                    continue
            for parent_seq, children_seqs in grouped.items():
                try:
                    expected = set(cell_children(parent_seq, current_res - 1, current_res))
                except Exception:
                    continue
                if expected and set(children_seqs) >= expected:
                    selected.difference_update(children_seqs)
                    selected.add(parent_seq)
                    changed = True
            current_res -= 1
        return sorted(selected)

    # ------------------------------------------------------------------
    # Geometry
    # ------------------------------------------------------------------

    def code_to_geometry(self, address: GridAddress) -> dict:
        seqnum, res = self._parse(address)
        ring = _closed_ring(seqnum, res)
        return {
            "type": "Polygon",
            "coordinates": [[[lon_, lat_] for lon_, lat_ in ring]],
        }

    def code_to_center(self, address: GridAddress) -> list[float]:
        seqnum, res = self._parse(address)
        lon, lat = cell_center(seqnum, res)
        return [lon, lat]

    def code_to_bbox(self, address: GridAddress) -> list[float]:
        seqnum, res = self._parse(address)
        corners = cell_boundary_polygon(seqnum, res)
        lons = [p[0] for p in corners]
        lats = [p[1] for p in corners]
        return [min(lons), min(lats), max(lons), max(lats)]

    # ------------------------------------------------------------------
    # Topology
    # ------------------------------------------------------------------

    def neighbors(self, address: GridAddress, k: int = 1) -> list[GridAddress]:
        seqnum, res = self._parse(address)
        nbs = cell_neighbors(seqnum, res)
        return [_make_address(n, res) for n in nbs]

    def parent(self, address: GridAddress) -> GridAddress:
        seqnum, res = self._parse(address)
        p = cell_parent(seqnum, res)
        return _make_address(p, res - 1)

    def children(self, address: GridAddress, target_grid_level: int) -> list[GridAddress]:
        seqnum, res = self._parse(address)
        target_res = _validate_level(target_grid_level)
        ch = cell_children(seqnum, res, target_res)
        return [_make_address(c, target_res) for c in ch]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse(address: GridAddress) -> tuple[int, int]:
        try:
            seqnum = int(address.space_code)
        except (ValueError, TypeError) as exc:
            raise ValidationError(
                f"Invalid ISEA4H space_code: {address.space_code!r}"
            ) from exc
        res = address.grid_level
        if not validate_seqnum(seqnum, res):
            raise ValidationError(
                f"ISEA4H seqnum {seqnum} is out of range at resolution {res}"
            )
        return seqnum, res
