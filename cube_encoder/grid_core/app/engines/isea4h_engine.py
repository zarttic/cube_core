"""Pure-Python ISEA4H grid engine.

Implements BaseGridEngine using the math modules in the isea4h/ package.
No H3 or DGGRID imports at runtime.
"""
from __future__ import annotations

import time

from shapely import affinity
from shapely.geometry import Polygon, box, mapping
from shapely.ops import unary_union
from shapely.validation import make_valid

from grid_core.app.core.enums import CoverMode
from grid_core.app.core.exceptions import ValidationError
from grid_core.app.engines.base import BaseGridEngine
from grid_core.app.engines.isea4h.addressing import (
    cell_count,
    locate_cell,
    q2di_to_seqnum,
    validate_seqnum,
)
from grid_core.app.engines.isea4h.geometry import cell_boundary_polygon, cell_center
from grid_core.app.engines.isea4h.topology import cell_children, cell_neighbors, cell_parent
from grid_core.app.models.compact_grid_cell import CompactGridCell
from grid_core.app.models.grid_address import GridAddress
from grid_core.app.models.grid_cell import GridCell
from grid_core.app.utils.geometry import normalize_ring_longitudes

_LEVEL_MIN = 0
_LEVEL_MAX = 15
_MAX_CANDIDATE_CELLS = 500_000
_MAX_OUTPUT_CELLS = 100_000
_MAX_COVER_SECONDS = 30.0
_GRID_TYPE = "isea4h"
_COVER_INDEX_CACHE: dict[int, tuple[object, list[object]]] = {}
_WGS84_BOUNDS = box(-180.0, -90.0, 180.0, 90.0)


def _validate_level(level: int) -> int:
    if not _LEVEL_MIN <= level <= _LEVEL_MAX:
        raise ValidationError(
            f"ISEA4H requested_grid_level must be in [{_LEVEL_MIN}, {_LEVEL_MAX}]"
        )
    return level


def _unwrap_geojson_longitudes(geometry: dict) -> dict:
    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates")
    if geometry_type == "Polygon" and isinstance(coordinates, list):
        return {
            **geometry,
            "coordinates": [
                normalize_ring_longitudes([(float(point[0]), float(point[1])) for point in ring])
                for ring in coordinates
            ],
        }
    if geometry_type == "MultiPolygon" and isinstance(coordinates, list):
        normalized_polygons = [
            [normalize_ring_longitudes([(float(point[0]), float(point[1])) for point in ring]) for ring in polygon]
            for polygon in coordinates
        ]
        if normalized_polygons:
            anchor = sum(point[0] for point in normalized_polygons[0][0]) / len(normalized_polygons[0][0])
            for polygon in normalized_polygons[1:]:
                center = sum(point[0] for point in polygon[0]) / len(polygon[0])
                offset = round((anchor - center) / 360.0) * 360.0
                if offset:
                    for ring in polygon:
                        for point in ring:
                            point[0] += offset
        return {
            **geometry,
            "coordinates": normalized_polygons,
        }
    return geometry


def _make_address(seqnum: int, res: int) -> GridAddress:
    return GridAddress(
        grid_type=_GRID_TYPE,
        grid_level=res,
        space_code=str(seqnum),
        topology_code=None,
    )


def _continuous_ring(seqnum: int, res: int) -> list[list[float]]:
    """Return a local, antimeridian-continuous cell boundary."""
    corners = cell_boundary_polygon(seqnum, res)
    candidates: list[list[list[float]]] = []
    for start in range(len(corners)):
        ring = normalize_ring_longitudes(corners[start:] + corners[:start])
        if abs(ring[-1][0] - ring[0][0]) <= 180.0 + 1e-6:
            candidates.append(ring)
    if not candidates:
        return normalize_ring_longitudes(corners)
    return min(candidates, key=lambda ring: max(point[0] for point in ring) - min(point[0] for point in ring))


def _closed_ring(seqnum: int, res: int) -> list[list[float]]:
    """Boundary corners with the first point repeated to close the ring."""
    ring = _continuous_ring(seqnum, res)
    return ring + [ring[0]]


def _to_wgs84_shape(geometry: object):
    if not geometry.is_valid:
        geometry = make_valid(geometry)
    pieces = [
        affinity.translate(geometry, xoff=360.0 * offset).intersection(_WGS84_BOUNDS)
        for offset in (-1, 0, 1)
    ]
    return make_valid(unary_union([piece for piece in pieces if not piece.is_empty]))


def _cell_shape(seqnum: int, res: int):
    return _to_wgs84_shape(Polygon(_closed_ring(seqnum, res)))


def _cell_geometry(seqnum: int, res: int) -> dict:
    return dict(mapping(_cell_shape(seqnum, res)))


def _wrap_longitude(lon: float) -> float:
    wrapped = (lon + 180.0) % 360.0 - 180.0
    return 180.0 if wrapped == -180.0 and lon > 0.0 else wrapped


def _cell_bbox(seqnum: int, res: int) -> list[float]:
    ring = _continuous_ring(seqnum, res)
    min_lon = min(point[0] for point in ring)
    max_lon = max(point[0] for point in ring)
    min_lat = min(point[1] for point in ring)
    max_lat = max(point[1] for point in ring)
    if max_lon - min_lon <= 180.0 + 1e-6:
        return [_wrap_longitude(min_lon), min_lat, _wrap_longitude(max_lon), max_lat]
    return list(_cell_shape(seqnum, res).bounds)


def _make_cell(seqnum: int, res: int) -> GridCell:
    lon, lat = cell_center(seqnum, res)
    geometry = _cell_shape(seqnum, res)
    return GridCell(
        grid_type=_GRID_TYPE,
        grid_level=res,
        space_code=str(seqnum),
        center=[lon, lat],
        bbox=_cell_bbox(seqnum, res),
        geometry=dict(mapping(geometry)),
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
        cells = self._cover_cells(geometry, res, cover_mode)
        return [_make_cell(seqnum, cell_res) for seqnum, cell_res in cells]

    def cover_geometry_compact(
        self, geometry: dict, requested_grid_level: int, cover_mode: str
    ) -> list[CompactGridCell]:
        res = _validate_level(requested_grid_level)
        cells = self._cover_cells(geometry, res, cover_mode)
        result: list[CompactGridCell] = []
        for seqnum, cell_res in cells:
            bbox = _cell_bbox(seqnum, cell_res)
            result.append(CompactGridCell(
                grid_type=_GRID_TYPE,
                grid_level=cell_res,
                space_code=str(seqnum),
                bbox=bbox,
            ))
        return result

    def _cover_cells(self, geometry: dict, res: int, cover_mode: str) -> list[tuple[int, int]]:
        """Return an exact positive-area cover while preserving cell levels."""
        try:
            from shapely import affinity
            from shapely.geometry import shape as shapely_shape
            from shapely.ops import unary_union
            from shapely.strtree import STRtree
        except ImportError:
            raise ValidationError("shapely is required for ISEA4H cover")

        if cover_mode not in {mode.value for mode in CoverMode}:
            raise ValidationError(f"ISEA4H cover does not support cover_mode={cover_mode!r}")
        target = shapely_shape(_unwrap_geojson_longitudes(geometry))
        target = _to_wgs84_shape(target)
        if target.is_empty:
            return []
        def longitude_variants(value: object) -> tuple[object, ...]:
            return tuple(affinity.translate(value, xoff=360.0 * offset) for offset in range(-2, 3))

        target_variants = (target,)
        target_query_variants = longitude_variants(target)
        min_lon, min_lat, max_lon, max_lat = target.bounds
        is_global_target = min_lon <= -180.0 and min_lat <= -90.0 and max_lon >= 180.0 and max_lat >= 90.0
        if cell_count(res) > _MAX_CANDIDATE_CELLS:
            raise ValidationError(
                f"ISEA4H cover exceeded MAX_CANDIDATE_CELLS: "
                f"limit={_MAX_CANDIDATE_CELLS}, observed={cell_count(res)}"
            )
        deadline = time.monotonic() + _MAX_COVER_SECONDS
        output: list[tuple[int, int]] = []

        def index_for(level: int) -> tuple[object, list[object]]:
            cached = _COVER_INDEX_CACHE.get(level)
            if cached is None:
                cells: list[object] = []
                for seqnum in range(1, cell_count(level) + 1):
                    if time.monotonic() > deadline:
                        raise ValidationError(f"ISEA4H cover exceeded MAX_COVER_SECONDS: limit={_MAX_COVER_SECONDS}")
                    cells.append(_cell_shape(seqnum, level))
                cached = (STRtree(cells), cells)
                _COVER_INDEX_CACHE[level] = cached
            return cached

        def candidate_indexes_for(index: object) -> set[int]:
            return {
                int(candidate_index)
                for target_variant in target_query_variants
                for candidate_index in index.query(target_variant)
            }

        def covers_area(container: object, contained: object) -> bool:
            return contained.difference(container).area <= 1e-12

        index, cells = index_for(res)
        candidate_indexes = candidate_indexes_for(index)
        if cover_mode == CoverMode.MINIMAL.value and is_global_target:
            return [(seqnum, 0) for seqnum in range(1, cell_count(0) + 1)]

        selected_parents: list[tuple[int, int, object]] = []
        selected_by_target_variant: list[list[object]] = [[] for _ in target_variants]
        if cover_mode == CoverMode.MINIMAL.value:
            for current_res in range(res):
                parent_index, parent_cells = index_for(current_res)
                for candidate_index in sorted(candidate_indexes_for(parent_index)):
                    if time.monotonic() > deadline:
                        raise ValidationError(f"ISEA4H cover exceeded MAX_COVER_SECONDS: limit={_MAX_COVER_SECONDS}")
                    parent = parent_cells[candidate_index]
                    parent_variants = longitude_variants(parent)
                    covered_variants = [
                        (target_index, parent_variant)
                        for target_index, target_variant in enumerate(target_variants)
                        for parent_variant in parent_variants
                        if covers_area(target_variant, parent_variant)
                    ]
                    if not covered_variants:
                        continue
                    if any(
                        covers_area(unary_union(selected_by_target_variant[target_index]), parent_variant)
                        for target_index, parent_variant in covered_variants
                        if selected_by_target_variant[target_index]
                    ):
                        continue
                    selected_parents.append((candidate_index + 1, current_res, parent))
                    for target_index, parent_variant in covered_variants:
                        selected_by_target_variant[target_index].append(parent_variant)

        remaining_by_target_variant = [
            target_variant.difference(unary_union(selected_by_target_variant[target_index]))
            if selected_by_target_variant[target_index]
            else target_variant
            for target_index, target_variant in enumerate(target_variants)
        ]

        for candidate_index in sorted(candidate_indexes):
            if time.monotonic() > deadline:
                raise ValidationError(f"ISEA4H cover exceeded MAX_COVER_SECONDS: limit={_MAX_COVER_SECONDS}")
            cell = cells[candidate_index]
            cell_variants = longitude_variants(cell)
            intersects = any(
                cell_variant.intersection(target_variant).area > 0.0
                for cell_variant in cell_variants
                for target_variant in target_variants
            )
            if not intersects:
                continue
            contained = is_global_target or any(
                covers_area(target_variant, cell_variant)
                for cell_variant in cell_variants
                for target_variant in target_variants
            )
            covered_by_parent = False
            if cover_mode == CoverMode.MINIMAL.value and selected_parents:
                covered_by_parent = not any(
                    cell_variant.intersection(remaining).area > 1e-12
                    for cell_variant in cell_variants
                    for remaining in remaining_by_target_variant
                )
            if (cover_mode != CoverMode.CONTAIN.value or contained) and not covered_by_parent:
                output.append((candidate_index + 1, res))
            if len(output) > _MAX_OUTPUT_CELLS:
                raise ValidationError(
                    f"ISEA4H cover exceeded MAX_OUTPUT_CELLS: "
                    f"limit={_MAX_OUTPUT_CELLS}, observed={len(output)}"
                )

        output.extend((seqnum, current_res) for seqnum, current_res, _ in selected_parents)
        return sorted(output, key=lambda cell: (cell[1], cell[0]))

    # ------------------------------------------------------------------
    # Geometry
    # ------------------------------------------------------------------

    def code_to_geometry(self, address: GridAddress) -> dict:
        seqnum, res = self._parse(address)
        return _cell_geometry(seqnum, res)

    def code_to_center(self, address: GridAddress) -> list[float]:
        seqnum, res = self._parse(address)
        lon, lat = cell_center(seqnum, res)
        return [lon, lat]

    def code_to_bbox(self, address: GridAddress) -> list[float]:
        seqnum, res = self._parse(address)
        return _cell_bbox(seqnum, res)

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
