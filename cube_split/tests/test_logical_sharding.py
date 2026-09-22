"""Shard planning for logical partitions: coverage, budget and mode behaviour."""

from __future__ import annotations

import math

import pytest

from cube_split.jobs import logical_sharding
from cube_split.jobs.logical_sharding import iter_shards, plan_logical_shards

GLOBAL = [0.0, -90.0, 360.0, 90.0]


class _FakeSDK:
    """Fixed cell size per grid/level so the planner maths is deterministic."""

    def __init__(self, cell_degrees: float = 0.9) -> None:
        self.cell_degrees = cell_degrees

    def locate(self, *, grid_type: str, requested_grid_level: int, point: list[float]):
        size = self.cell_degrees
        return type("Cell", (), {"bbox": [point[0], point[1], point[0] + size, point[1] + size]})()


@pytest.fixture
def env(monkeypatch):
    values: dict[str, str] = {}

    def env_text(name: str, default: str = "") -> str:
        return values.get(name, default)

    monkeypatch.setattr(logical_sharding.runtime_config, "env_text", env_text)
    return values


def _bbox_area(bbox) -> float:
    west, south, east, north = bbox
    return (east - west) * (north - south)


def test_shards_tile_the_bbox_exactly() -> None:
    bbox = [-10.0, 20.0, 5.0, 30.0]
    shards = list(iter_shards(bbox, 3.0, 2.5))

    assert sum(_bbox_area(shard) for shard in shards) == pytest.approx(_bbox_area(bbox))
    assert min(shard[0] for shard in shards) == bbox[0]
    assert min(shard[1] for shard in shards) == bbox[1]
    assert max(shard[2] for shard in shards) == bbox[2]
    assert max(shard[3] for shard in shards) == bbox[3]


def test_shards_reject_invalid_bbox() -> None:
    with pytest.raises(ValueError, match="WGS84"):
        list(iter_shards([10.0, 0.0, -10.0, 1.0], 1.0, 1.0))


def test_legacy_mode_keeps_fixed_degree_shards(env) -> None:
    env["CUBE_LOGICAL_SHARD_DEGREES"] = "1"
    plan = plan_logical_shards(
        sdk=_FakeSDK(), bbox=GLOBAL, grid_type="mgrs", grid_level=0, bands=1, shards_per_task=16, mode="legacy"
    )

    assert plan.mode == "legacy"
    assert (plan.shards_x, plan.shards_y) == (360, 180)
    assert plan.shard_count == 360 * 180
    assert plan.chunk_count == math.ceil(360 * 180 / 16)  # 4,050 — the measured bad case


def test_auto_mode_sizes_shards_from_the_cell_estimate(env) -> None:
    plan = plan_logical_shards(
        sdk=_FakeSDK(), bbox=GLOBAL, grid_type="mgrs", grid_level=0, bands=1, shards_per_task=16, parallelism=4
    )

    assert plan.mode == "auto"
    assert plan.estimated_cells == 400 * 200          # ceil(360/0.9) x ceil(180/0.9)
    assert plan.estimated_rows == plan.estimated_cells * 3
    assert plan.chunk_count < 100                      # vs 4,050 in legacy mode
    assert plan.rows_per_chunk <= 2 * logical_sharding.DEFAULT_TARGET_ROWS_PER_CHUNK
    assert plan.shard_width >= 0.9 and plan.shard_height >= 0.9


def test_chunk_cap_is_respected_when_rows_fit(env) -> None:
    plan = plan_logical_shards(
        sdk=_FakeSDK(cell_degrees=0.05), bbox=[0.0, 0.0, 40.0, 20.0], grid_type="geohash", grid_level=6, bands=1,
        shards_per_task=16, parallelism=4, max_chunks=64,
    )

    assert plan.chunk_count <= 64
    # 分片网格按长宽比取整，只要求不超过预算（chunk x shards_per_task）
    assert plan.shard_count <= plan.chunk_count * 16
    assert plan.shards_x * plan.shards_y == plan.shard_count


def test_row_budget_overrides_the_chunk_cap_for_huge_extents(env) -> None:
    # 全球 x 细格元：行数爆炸时宁可超过对象数上限，也不做出单 chunk 上百万行的巨型 chunk
    plan = plan_logical_shards(
        sdk=_FakeSDK(cell_degrees=0.005), bbox=GLOBAL, grid_type="geohash", grid_level=7, bands=1,
        shards_per_task=16, parallelism=4, max_chunks=64,
    )

    assert plan.chunk_count > 64
    assert "exceeding" in plan.reason
    assert plan.rows_per_chunk <= 10 * logical_sharding.DEFAULT_TARGET_ROWS_PER_CHUNK
    assert plan.shards_x * plan.shards_y == plan.shard_count


def test_parallelism_floor_keeps_enough_shards(env) -> None:
    # 20°x20° AOI at a 0.9° cell: rows are few, so the floor (2 x parallelism) decides.
    plan = plan_logical_shards(
        sdk=_FakeSDK(), bbox=[0.0, 0.0, 20.0, 20.0], grid_type="mgrs", grid_level=0, bands=1,
        shards_per_task=16, parallelism=8,
    )

    assert plan.chunk_count >= 16  # 2 x parallelism
    assert plan.shard_count >= plan.chunk_count


def test_cells_larger_than_the_extent_collapse_to_one_chunk(env) -> None:
    plan = plan_logical_shards(
        sdk=_FakeSDK(cell_degrees=5.0), bbox=[0.0, 0.0, 1.0, 1.0], grid_type="mgrs", grid_level=0, bands=1,
        shards_per_task=16, parallelism=8,
    )

    assert plan.chunk_count == 1   # the extent holds a single cell — nothing to parallelise
    assert plan.estimated_cells == 1


def test_small_extent_collapses_to_one_chunk(env) -> None:
    plan = plan_logical_shards(
        sdk=_FakeSDK(), bbox=[116.0, 39.0, 117.2, 40.0], grid_type="mgrs", grid_level=0, bands=1,
        shards_per_task=16, parallelism=4,
    )

    assert plan.chunk_count == 1
    assert plan.shard_count <= 16


def test_multi_band_extents_get_more_chunks(env) -> None:
    one_band = plan_logical_shards(
        sdk=_FakeSDK(), bbox=GLOBAL, grid_type="mgrs", grid_level=0, bands=1, shards_per_task=16, parallelism=4
    )
    four_bands = plan_logical_shards(
        sdk=_FakeSDK(), bbox=GLOBAL, grid_type="mgrs", grid_level=0, bands=4, shards_per_task=16, parallelism=4
    )

    assert four_bands.estimated_rows == one_band.estimated_rows // 3 * 9  # 1 + 2*4 rows per cell
    assert four_bands.chunk_count >= one_band.chunk_count


def test_unknown_mode_falls_back_to_auto(env) -> None:
    env["CUBE_LOGICAL_SHARD_MODE"] = "garbage"

    assert logical_sharding.shard_mode() == logical_sharding.AUTO_MODE


def test_mode_read_from_env(env) -> None:
    env["CUBE_LOGICAL_SHARD_MODE"] = "legacy"

    assert logical_sharding.shard_mode() == logical_sharding.LEGACY_MODE


def test_degenerate_cell_size_does_not_divide_by_zero(monkeypatch, env) -> None:
    class _FlatSDK:
        def locate(self, **_kwargs: object):
            return type("Cell", (), {"bbox": [0.0, 0.0, 0.0, 0.0]})()

    plan = plan_logical_shards(
        sdk=_FlatSDK(), bbox=[0.0, 0.0, 1.0, 1.0], grid_type="mgrs", grid_level=0, bands=1, shards_per_task=16
    )

    assert plan.chunk_count >= 1
    assert plan.shard_width > 0 and plan.shard_height > 0
