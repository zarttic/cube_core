from __future__ import annotations

import pytest
from shapely.geometry import MultiPolygon, Polygon, box, shape
from shapely.validation import make_valid

from grid_core.app.core.exceptions import ValidationError
from grid_core.app.engines import isea4h_engine
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


def test_minimal_world_with_a_hole_does_not_use_the_global_shortcut() -> None:
    engine = ISEA4HEngine()
    world = box(-180.0, -90.0, 180.0, 90.0)
    aoi = world.difference(box(110.0, 30.0, 120.0, 40.0))

    covered = engine.cover_geometry_compact(aoi.__geo_interface__, 1, "minimal")

    assert any(cell.grid_level == 1 for cell in covered)
    for cell in covered:
        if cell.grid_level == 0:
            address = GridAddress(grid_type="isea4h", grid_level=0, space_code=cell.space_code)
            assert shape(engine.code_to_geometry(address)).difference(aoi).area <= 1e-12


def test_small_aoi_cover_does_not_enumerate_the_global_target_level() -> None:
    engine = ISEA4HEngine()

    for resolution in (8, 15):
        seed = engine.locate_space_code(116.391, 39.907, resolution)
        lon, lat = cell_center(int(seed.space_code), resolution)
        aoi = box(lon - 1e-6, lat - 1e-6, lon + 1e-6, lat + 1e-6)

        covered = engine.cover_geometry(aoi.__geo_interface__, resolution, "intersect")

        assert {cell.space_code for cell in covered} == {seed.space_code}


def test_candidate_limit_counts_only_cells_visited_for_the_aoi(monkeypatch) -> None:
    engine = ISEA4HEngine()
    resolution = 8
    seed = engine.locate_space_code(116.391, 39.907, resolution)
    lon, lat = cell_center(int(seed.space_code), resolution)
    aoi = box(lon - 1e-6, lat - 1e-6, lon + 1e-6, lat + 1e-6)
    monkeypatch.setattr(isea4h_engine, "_MAX_CANDIDATE_CELLS", 8)

    covered = engine.cover_geometry(aoi.__geo_interface__, resolution, "intersect")

    assert {cell.space_code for cell in covered} == {seed.space_code}


def test_output_limit_fails_when_the_first_matching_cell_is_appended(monkeypatch) -> None:
    engine = ISEA4HEngine()
    resolution = 8
    seed = engine.locate_space_code(116.391, 39.907, resolution)
    lon, lat = cell_center(int(seed.space_code), resolution)
    aoi = box(lon - 1e-6, lat - 1e-6, lon + 1e-6, lat + 1e-6)
    original_cell_shape = isea4h_engine._cell_shape
    built_cells: list[tuple[int, int]] = []

    def tracking_cell_shape(seqnum: int, level: int):
        built_cells.append((seqnum, level))
        return original_cell_shape(seqnum, level)

    monkeypatch.setattr(isea4h_engine, "_cell_shape", tracking_cell_shape)
    monkeypatch.setattr(isea4h_engine, "_MAX_OUTPUT_CELLS", 0)

    with pytest.raises(ValidationError, match=r"MAX_OUTPUT_CELLS: limit=0, observed=1"):
        engine.cover_geometry(aoi.__geo_interface__, resolution, "intersect")

    assert built_cells == [(int(seed.space_code), resolution)]


def test_disconnected_multipolygon_cover_matches_each_component() -> None:
    engine = ISEA4HEngine()
    resolution = 5
    components = [box(116.2, 39.8, 116.4, 40.0), box(151.1, -33.95, 151.3, -33.75)]
    combined = MultiPolygon(components)

    actual = {cell.space_code for cell in engine.cover_geometry(combined.__geo_interface__, resolution, "intersect")}
    expected = {
        cell.space_code
        for component in components
        for cell in engine.cover_geometry(component.__geo_interface__, resolution, "intersect")
    }

    assert actual == expected


def test_polygon_hole_excludes_cells_inside_the_hole() -> None:
    engine = ISEA4HEngine()
    resolution = 5
    shell = [(115.0, 39.0), (118.0, 39.0), (118.0, 42.0), (115.0, 42.0), (115.0, 39.0)]
    hole = [(115.8, 39.8), (117.2, 39.8), (117.2, 41.2), (115.8, 41.2), (115.8, 39.8)]
    aoi = Polygon(shell, [hole])

    actual = {cell.space_code for cell in engine.cover_geometry(aoi.__geo_interface__, resolution, "intersect")}

    assert actual == _exact_intersections(aoi, resolution)


def test_high_resolution_antimeridian_polygon_matches_split_components() -> None:
    engine = ISEA4HEngine()
    resolution = 8
    crossing = {
        "type": "Polygon",
        "coordinates": [[
            [179.95, 10.0],
            [-179.95, 10.0],
            [-179.95, 10.1],
            [179.95, 10.1],
            [179.95, 10.0],
        ]],
    }
    split_components = [box(179.95, 10.0, 180.0, 10.1), box(-180.0, 10.0, -179.95, 10.1)]

    actual = {cell.space_code for cell in engine.cover_geometry(crossing, resolution, "intersect")}
    expected = {
        cell.space_code
        for component in split_components
        for cell in engine.cover_geometry(component.__geo_interface__, resolution, "intersect")
    }

    assert actual == expected


def test_high_resolution_contain_accepts_an_exact_cell_geometry() -> None:
    engine = ISEA4HEngine()
    resolution = 8
    seed = engine.locate_space_code(116.391, 39.907, resolution)

    covered = engine.cover_geometry(engine.code_to_geometry(seed), resolution, "contain")

    assert seed.space_code in {cell.space_code for cell in covered}


def test_high_resolution_polar_cover_stays_local() -> None:
    engine = ISEA4HEngine()
    aoi = box(-10.0, 89.0, 10.0, 89.5)

    covered = engine.cover_geometry(aoi.__geo_interface__, 8, "intersect")

    assert {cell.space_code for cell in covered} == {"132", "133", "134", "388", "389", "390"}
