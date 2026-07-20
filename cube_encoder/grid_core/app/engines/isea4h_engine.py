"""Pure-Python ISEA4H grid engine.

Implements BaseGridEngine using the math modules in the isea4h/ package.
No H3 or DGGRID imports at runtime.
"""
from __future__ import annotations

import time
from collections import deque
from collections.abc import Iterator

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
    locate_cells,
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
_WGS84_BOUNDS = box(-180.0, -90.0, 180.0, 90.0)
_INDEXED_LEVEL_MAX_CELLS = 200_000
_COVER_INDEX_CACHE: dict[int, tuple[object, list[object]]] = {}


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

    def locate_space_codes(self, points: list[list[float]], requested_grid_level: int) -> list[GridAddress]:
        res = _validate_level(requested_grid_level)
        return [_make_address(q2di_to_seqnum(quad, i, j, res), res) for quad, i, j in locate_cells(points, res)]

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
            from shapely.geometry import shape as shapely_shape
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

        is_global_target = _WGS84_BOUNDS.difference(target).area <= 1e-12
        if cover_mode == CoverMode.MINIMAL.value and is_global_target:
            return [(seqnum, 0) for seqnum in range(1, cell_count(0) + 1)]

        deadline = time.monotonic() + _MAX_COVER_SECONDS
        visited_count = 0
        shape_cache: dict[tuple[int, int], object] = {}

        def cached_cell_shape(seqnum: int, level: int):
            key = (seqnum, level)
            cached = shape_cache.get(key)
            if cached is None:
                cached = _cell_shape(seqnum, level)
                shape_cache[key] = cached
            return cached

        def indexed_candidates(search_target: object, level: int) -> set[int] | None:
            """Return spatial-index candidates for levels that fit in memory.

            Neighbor walking is fast for a small local AOI, but it can miss a
            cell when a path crosses an ISEA4H quad edge or a pole.  The
            bounded index makes the default production levels exhaustive while
            retaining the local walk for very fine levels.
            """
            if cell_count(level) > _INDEXED_LEVEL_MAX_CELLS:
                return None
            try:
                from shapely.strtree import STRtree
            except ImportError:  # pragma: no cover - Shapely is a runtime dependency
                return None
            cached_index = _COVER_INDEX_CACHE.get(level)
            if cached_index is None:
                envelopes = [
                    box(*_cell_bbox(seqnum, level))
                    for seqnum in range(1, cell_count(level) + 1)
                ]
                cached_index = (STRtree(envelopes), envelopes)
                _COVER_INDEX_CACHE[level] = cached_index
            tree, _envelopes = cached_index
            candidates: set[int] = set()
            for variant in longitude_variants(search_target):
                for index in tree.query(variant):
                    candidates.add(int(index) + 1)
            return candidates

        def component_seeds(search_target: object, level: int) -> set[int]:
            pending = [search_target]
            seeds: set[int] = set()
            while pending:
                component = pending.pop()
                if component.is_empty:
                    continue
                if component.geom_type in {"Polygon", "LineString", "Point"}:
                    point = component.representative_point()
                    quad, i, j = locate_cell(point.x, point.y, level)
                    seeds.add(q2di_to_seqnum(quad, i, j, level))
                    continue
                pending.extend(component.geoms)
            return seeds

        def intersects_area(cell: object, search_variants: tuple[object, ...]) -> bool:
            return any(
                cell_variant.intersection(search_variant).area > 0.0
                for cell_variant in longitude_variants(cell)
                for search_variant in search_variants
            )

        def intersecting_cells(search_target: object, level: int) -> Iterator[tuple[int, object]]:
            nonlocal visited_count
            search_variants = longitude_variants(search_target)
            candidates = indexed_candidates(search_target, level)
            if candidates is not None:
                for seqnum in sorted(candidates):
                    visited_count += 1
                    if visited_count > _MAX_CANDIDATE_CELLS:
                        raise ValidationError(
                            f"ISEA4H cover exceeded MAX_CANDIDATE_CELLS: "
                            f"limit={_MAX_CANDIDATE_CELLS}, observed={visited_count}"
                        )
                    if time.monotonic() > deadline:
                        raise ValidationError(f"ISEA4H cover exceeded MAX_COVER_SECONDS: limit={_MAX_COVER_SECONDS}")
                    cell = cached_cell_shape(seqnum, level)
                    if intersects_area(cell, search_variants):
                        yield seqnum, cell
                return

            seeds = component_seeds(search_target, level)
            queue = deque(seeds)
            for seed in seeds:
                queue.extend(cell_neighbors(seed, level))
            visited: set[int] = set()
            while queue:
                seqnum = queue.popleft()
                if seqnum in visited:
                    continue
                visited.add(seqnum)
                visited_count += 1
                if visited_count > _MAX_CANDIDATE_CELLS:
                    raise ValidationError(
                        f"ISEA4H cover exceeded MAX_CANDIDATE_CELLS: "
                        f"limit={_MAX_CANDIDATE_CELLS}, observed={visited_count}"
                    )
                if time.monotonic() > deadline:
                    raise ValidationError(f"ISEA4H cover exceeded MAX_COVER_SECONDS: limit={_MAX_COVER_SECONDS}")
                cell = cached_cell_shape(seqnum, level)
                if not intersects_area(cell, search_variants):
                    continue
                queue.extend(cell_neighbors(seqnum, level))
                yield seqnum, cell

        def covers_area(container: object, contained: object) -> bool:
            return contained.difference(container).area <= 1e-12

        output: list[tuple[int, int]] = []

        def append_output(seqnum: int, level: int) -> None:
            output.append((seqnum, level))
            if len(output) > _MAX_OUTPUT_CELLS:
                raise ValidationError(
                    f"ISEA4H cover exceeded MAX_OUTPUT_CELLS: "
                    f"limit={_MAX_OUTPUT_CELLS}, observed={len(output)}"
                )

        remaining = target
        if cover_mode == CoverMode.MINIMAL.value:
            for current_res in range(res):
                selected_shapes: list[object] = []
                for seqnum, cell in intersecting_cells(remaining, current_res):
                    if covers_area(remaining, cell):
                        append_output(seqnum, current_res)
                        selected_shapes.append(cell)
                if selected_shapes:
                    remaining = make_valid(remaining.difference(unary_union(selected_shapes)))
                    if remaining.is_empty or remaining.area <= 1e-12:
                        remaining = Polygon()
                        break

        if not remaining.is_empty:
            for seqnum, cell in intersecting_cells(remaining, res):
                if cover_mode != CoverMode.CONTAIN.value or covers_area(target, cell):
                    append_output(seqnum, res)

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
