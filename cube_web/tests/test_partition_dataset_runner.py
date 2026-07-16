import pytest

from cube_web.services.partition_dataset_runner import _consume_observation_budget, _record_asset_cell


def test_carbon_unique_cells_enforce_the_same_per_asset_cap_as_cog_cover() -> None:
    cells: set[tuple[str, int, str | None]] = set()
    _record_asset_cell(cells, ("u4pr", 5, None), max_cells_per_asset=1)
    _record_asset_cell(cells, ("u4pr", 5, None), max_cells_per_asset=1)
    with pytest.raises(RuntimeError, match=r"2 > 1"):
        _record_asset_cell(cells, ("u4ps", 5, None), max_cells_per_asset=1)


def test_carbon_observation_budget_is_shared_across_assets() -> None:
    remaining = 100
    remaining = _consume_observation_budget(remaining, 60)
    assert remaining == 40
    remaining = _consume_observation_budget(remaining, 40)
    assert remaining == 0
    assert _consume_observation_budget(None, 1000) is None
