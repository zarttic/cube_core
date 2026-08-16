import pytest

from cube_web.services.partition_defaults import default_grid_level_for_resolution


def test_geohash_resolution_recommendations_coarsen_large_pixels() -> None:
    assert default_grid_level_for_resolution(499.999, grid_type="geohash") == 4
    assert default_grid_level_for_resolution(500, grid_type="geohash") == 3
    assert default_grid_level_for_resolution(999.999, grid_type="geohash") == 3
    assert default_grid_level_for_resolution(1000, grid_type="geohash") == 2
    assert default_grid_level_for_resolution(1000.001, grid_type="geohash") == 2


@pytest.mark.parametrize(
    ("resolution", "expected_level"),
    [(1, 6), (10, 6), (30, 5), (100, 4), (1000, 3), (1000.001, 2)],
)
def test_isea4h_resolution_recommendation_follows_production_mapping(
    resolution: float,
    expected_level: int,
) -> None:
    assert default_grid_level_for_resolution(resolution, grid_type="isea4h") == expected_level
