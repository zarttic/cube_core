from grid_core.spark_jobs.logical_partition_job import (
    _bbox_intersection,
    _bbox_intersects,
    _group_tasks_for_local_processing,
    _should_skip_sort_within_partitions,
    _should_skip_verify,
    _should_skip_grid_task_count,
    _resolve_cover_execution,
    _resolve_execution_engine,
    _resolve_shuffle_partitions,
)


def test_bbox_intersects_detects_overlap_without_shapely():
    assert _bbox_intersects([0.0, 0.0, 2.0, 2.0], [1.0, 1.0, 3.0, 3.0]) is True
    assert _bbox_intersects([0.0, 0.0, 1.0, 1.0], [1.1, 1.1, 2.0, 2.0]) is False


def test_bbox_intersection_returns_overlap_bounds():
    assert _bbox_intersection([0.0, 0.0, 2.0, 2.0], [1.0, 1.0, 3.0, 3.0]) == (1.0, 1.0, 2.0, 2.0)
    assert _bbox_intersection([0.0, 0.0, 1.0, 1.0], [2.0, 2.0, 3.0, 3.0]) is None


def test_small_run_heuristics_reduce_spark_overhead():
    assert _resolve_cover_execution(asset_count=8, cover_execution="spark", optimize_small_runs=True) == "driver"
    assert _resolve_cover_execution(asset_count=80, cover_execution="spark", optimize_small_runs=True) == "spark"
    assert _resolve_shuffle_partitions(asset_count=8, requested_repartition=0, optimize_small_runs=True) == 2
    assert _should_skip_sort_within_partitions(asset_count=8, optimize_small_runs=True) is True


def test_timing_mode_skips_optional_metrics():
    assert _should_skip_verify(skip_verify=False, timing_mode=True) is True
    assert _should_skip_verify(skip_verify=False, timing_mode=False) is False
    assert _should_skip_grid_task_count(skip_task_count=False, timing_mode=True) is True
    assert _should_skip_grid_task_count(skip_task_count=False, timing_mode=False) is False


def test_small_run_can_switch_to_local_execution_engine():
    assert _resolve_execution_engine(execution_engine="spark", optimize_small_runs=False, asset_count=8) == "spark"
    assert _resolve_execution_engine(execution_engine="local", optimize_small_runs=False, asset_count=8) == "local"
    assert _resolve_execution_engine(execution_engine="auto", optimize_small_runs=True, asset_count=8) == "local"
    assert _resolve_execution_engine(execution_engine="auto", optimize_small_runs=True, asset_count=80) == "spark"


def test_group_tasks_for_local_processing_sorts_by_asset_and_space_code():
    tasks = [
        {"asset_path": "/b.tif", "space_code": "z", "band": "b"},
        {"asset_path": "/a.tif", "space_code": "y", "band": "a"},
        {"asset_path": "/a.tif", "space_code": "x", "band": "a"},
    ]
    groups = _group_tasks_for_local_processing(tasks)

    assert [group[0]["asset_path"] for group in groups] == ["/a.tif", "/b.tif"]
    assert [row["space_code"] for row in groups[0]] == ["x", "y"]
