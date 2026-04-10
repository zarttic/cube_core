from __future__ import annotations

from collections import deque

from s2sphere import Cell, CellId, LatLng, LatLngRect, RegionCoverer
from shapely.geometry import Polygon
from shapely.prepared import prep

from grid_core.app.core.enums import CoverMode
from grid_core.app.core.exceptions import ValidationError
from grid_core.app.models.grid_cell import GridCell
from grid_core.app.utils.geometry import to_shapely


class GeohashEngine:
    # Keep the external grid_type string unchanged for API compatibility.
    grid_type = "geohash"

    def locate_point(self, lon: float, lat: float, level: int) -> GridCell:
        self._validate_level(level)
        code = CellId.from_lat_lng(LatLng.from_degrees(lat, lon)).parent(level).to_token()
        return self._build_cell(code, level)

    def cover_geometry(self, geometry: dict, level: int, cover_mode: str) -> list[GridCell]:
        self._validate_level(level)
        if cover_mode not in {CoverMode.INTERSECT.value, CoverMode.CONTAIN.value, CoverMode.MINIMAL.value}:
            raise ValidationError(f"Unsupported cover_mode: {cover_mode}")

        shp = to_shapely(geometry)
        prepared_shp = prep(shp)
        geoms = list(shp.geoms) if hasattr(shp, "geoms") else [shp]

        coverer = RegionCoverer()
        coverer.min_level = level
        coverer.max_level = level
        coverer.max_cells = 1_000_000

        candidate_codes: set[str] = set()
        for geom in geoms:
            min_lon, min_lat, max_lon, max_lat = geom.bounds
            rect = LatLngRect.from_point_pair(
                LatLng.from_degrees(min_lat, min_lon),
                LatLng.from_degrees(max_lat, max_lon),
            )
            for cid in coverer.get_covering(rect):
                candidate_codes.add(cid.to_token())

        selected: set[str] = set()
        boundary_cache: dict[str, list[list[float]]] = {}
        for code in sorted(candidate_codes):
            boundary = self._boundary_lnglat(code)
            boundary_cache[code] = boundary
            cell_poly = Polygon(boundary)
            if cover_mode in {CoverMode.INTERSECT.value, CoverMode.MINIMAL.value} and prepared_shp.intersects(cell_poly):
                selected.add(code)
            elif cover_mode == CoverMode.CONTAIN.value and shp.covers(cell_poly):
                selected.add(code)

        if cover_mode == CoverMode.MINIMAL.value:
            selected = self._coarsen_minimal(selected)

        return [self._build_cell(code, self._cell_id_from_code(code).level(), boundary=boundary_cache.get(code)) for code in sorted(selected)]

    def code_to_geometry(self, code: str) -> dict:
        self._validate_code(code)
        return {"type": "Polygon", "coordinates": [self._boundary_lnglat(code)]}

    def code_to_center(self, code: str) -> list[float]:
        cid = self._cell_id_from_code(code)
        ll = LatLng.from_point(cid.to_point())
        return [ll.lng().degrees, ll.lat().degrees]

    def code_to_bbox(self, code: str) -> list[float]:
        ring = self._boundary_lnglat(code)
        lons = [p[0] for p in ring]
        lats = [p[1] for p in ring]
        return [min(lons), min(lats), max(lons), max(lats)]

    def neighbors(self, code: str, k: int = 1) -> list[str]:
        if k < 1:
            raise ValidationError("k must be >= 1")

        cid = self._cell_id_from_code(code)
        level = cid.level()
        visited = {cid.id()}
        out: set[str] = set()
        queue = deque([(cid, 0)])

        while queue:
            current, depth = queue.popleft()
            if depth >= k:
                continue
            for nbr in current.get_edge_neighbors():
                if nbr.id() in visited:
                    continue
                visited.add(nbr.id())
                nbr_at_level = nbr.parent(level)
                out.add(nbr_at_level.to_token())
                queue.append((nbr_at_level, depth + 1))

        out.discard(code)
        return sorted(out)

    def parent(self, code: str) -> str:
        cid = self._cell_id_from_code(code)
        if cid.level() <= 1:
            raise ValidationError("Root geohash level has no parent")
        return cid.parent(cid.level() - 1).to_token()

    def children(self, code: str, target_level: int) -> list[str]:
        self._validate_level(target_level)
        cid = self._cell_id_from_code(code)
        current = cid.level()
        if target_level <= current:
            raise ValidationError("target_level must be greater than current level")

        frontier = [cid]
        for lvl in range(current + 1, target_level + 1):
            next_frontier: list[CellId] = []
            for cur in frontier:
                next_frontier.extend(cur.children(lvl))
            frontier = next_frontier
        return sorted(c.to_token() for c in frontier)

    def _build_cell(self, code: str, level: int, boundary: list[list[float]] | None = None) -> GridCell:
        boundary = boundary or self._boundary_lnglat(code)
        lons = [p[0] for p in boundary]
        lats = [p[1] for p in boundary]
        bbox = [min(lons), min(lats), max(lons), max(lats)]
        center = self.code_to_center(code)
        return GridCell(
            grid_type=self.grid_type,
            level=level,
            cell_id=code,
            space_code=code,
            center=center,
            bbox=bbox,
            geometry={"type": "Polygon", "coordinates": [boundary]},
            metadata={"precision": level, "zone": None, "facet": "s2"},
        )

    def _boundary_lnglat(self, code: str) -> list[list[float]]:
        cid = self._cell_id_from_code(code)
        cell = Cell(cid)
        ring: list[list[float]] = []
        for i in range(4):
            ll = LatLng.from_point(cell.get_vertex(i))
            ring.append([ll.lng().degrees, ll.lat().degrees])
        ring.append(ring[0])
        return ring

    def _coarsen_minimal(self, codes: set[str]) -> set[str]:
        out = set(codes)
        changed = True
        while changed:
            changed = False
            grouped: dict[str, set[str]] = {}
            for code in out:
                cid = self._cell_id_from_code(code)
                if cid.level() <= 1:
                    continue
                parent_code = cid.parent(cid.level() - 1).to_token()
                grouped.setdefault(parent_code, set()).add(code)

            for parent_code, child_codes in grouped.items():
                parent = self._cell_id_from_code(parent_code)
                expected_children = {c.to_token() for c in parent.children(parent.level() + 1)}
                if expected_children == child_codes:
                    out.difference_update(child_codes)
                    out.add(parent_code)
                    changed = True
        return out

    @staticmethod
    def _cell_id_from_code(code: str) -> CellId:
        try:
            cid = CellId.from_token(code)
        except Exception as exc:
            raise ValidationError("Invalid geohash space_code") from exc
        if not cid.is_valid():
            raise ValidationError("Invalid geohash space_code")
        return cid

    @classmethod
    def _validate_code(cls, code: str) -> None:
        cls._cell_id_from_code(code)

    @staticmethod
    def _validate_level(level: int) -> None:
        if level < 1 or level > 12:
            raise ValidationError("level must be in [1, 12]")
