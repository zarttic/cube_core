from __future__ import annotations

from shapely.geometry import Polygon, box
from shapely.validation import make_valid

from grid_core.app.engines.isea4h.addressing import cell_count
from grid_core.app.engines.isea4h.geometry import cell_boundary_polygon, cell_center
from grid_core.app.engines.isea4h_engine import ISEA4HEngine
from grid_core.app.models.grid_address import GridAddress
from grid_core.app.utils.geometry import normalize_ring_longitudes, wrapped_geometry_variants


def _exact_intersections(aoi, resolution: int) -> set[str]:
    target_variants = wrapped_geometry_variants(aoi)
    result: set[str] = set()
    for seqnum in range(1, cell_count(resolution) + 1):
        cell = Polygon(normalize_ring_longitudes(cell_boundary_polygon(seqnum, resolution)))
        if not cell.is_valid:
            cell = make_valid(cell)
        if any(
            cell_variant.intersection(target_variant).area > 0.0
            for cell_variant in wrapped_geometry_variants(cell)
            for target_variant in target_variants
        ):
            result.add(str(seqnum))
    return result


def test_intersect_keeps_all_cells_touching_a_vertex_with_positive_area() -> None:
    engine = ISEA4HEngine()
    resolution = 3
    seed = engine.locate_space_code(116.391, 39.907, resolution)
    lon, lat = cell_boundary_polygon(int(seed.space_code), resolution)[0]
    aoi = box(lon - 1e-4, lat - 1e-4, lon + 1e-4, lat + 1e-4)

    covered = engine.cover_geometry(aoi.__geo_interface__, resolution, "intersect")

    assert {cell.space_code for cell in covered} == _exact_intersections(aoi, resolution)


def test_contain_requires_the_complete_cell_geometry() -> None:
    engine = ISEA4HEngine()
    resolution = 3
    seed = engine.locate_space_code(116.391, 39.907, resolution)
    lon, lat = cell_center(int(seed.space_code), resolution)
    aoi = box(lon - 1e-3, lat - 1e-3, lon + 1e-3, lat + 1e-3)

    assert engine.cover_geometry(aoi.__geo_interface__, resolution, "contain") == []


def test_intersect_descends_through_an_antimeridian_root_cell() -> None:
    engine = ISEA4HEngine()
    aoi = box(-160.0, -35.0, -159.0, -34.0)

    covered = engine.cover_geometry(aoi.__geo_interface__, 1, "intersect")

    assert {cell.space_code for cell in covered} == _exact_intersections(aoi, 1) == {"24"}


def test_intersect_keeps_south_polar_cells_without_parent_pruning() -> None:
    engine = ISEA4HEngine()
    aoi = box(50.0, -70.0, 51.0, -69.0)

    covered = engine.cover_geometry(aoi.__geo_interface__, 1, "intersect")

    assert {cell.space_code for cell in covered} == _exact_intersections(aoi, 1) == {"32", "33"}


def test_minimal_replaces_a_fully_covered_parent_cell() -> None:
    engine = ISEA4HEngine()
    for resolution in (0, 1):
        for seqnum in range(1, cell_count(resolution) + 1):
            parent = GridAddress(grid_type="isea4h", grid_level=resolution, space_code=str(seqnum))

            covered = engine.cover_geometry_compact(
                engine.code_to_geometry(parent), resolution + 1, "minimal"
            )

            assert {(cell.space_code, cell.grid_level) for cell in covered} == {(str(seqnum), resolution)}


def test_minimal_preserves_a_parent_cell_actual_resolution() -> None:
    engine = ISEA4HEngine()
    world = box(-180.0, -90.0, 180.0, 90.0)

    covered = engine.cover_geometry_compact(world.__geo_interface__, 1, "minimal")

    assert len(covered) == cell_count(0)
    assert {(cell.space_code, cell.grid_level) for cell in covered} == {
        (str(seqnum), 0) for seqnum in range(1, cell_count(0) + 1)
    }
