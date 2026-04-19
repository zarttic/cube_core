from grid_core.ray_jobs.logical_partition_job import (
    _chunk_tasks_for_ray,
    _resolve_ray_chunk_size,
    _resolve_ray_parallelism,
)
from grid_core.spark_jobs.logical_partition_job import _prepare_task_rows_for_partitioning


def test_chunk_tasks_for_ray_preserves_order_and_chunk_size():
    tasks = [{"id": i} for i in range(7)]
    chunks = _chunk_tasks_for_ray(tasks, chunk_size=3)

    assert [[row["id"] for row in chunk] for chunk in chunks] == [[0, 1, 2], [3, 4, 5], [6]]


def test_resolve_ray_parallelism_caps_by_task_count():
    assert _resolve_ray_parallelism(task_group_count=1, requested_parallelism=8) == 1
    assert _resolve_ray_parallelism(task_group_count=4, requested_parallelism=8) == 4
    assert _resolve_ray_parallelism(task_group_count=4, requested_parallelism=2) == 2


def test_resolve_ray_parallelism_auto_uses_task_count_ceiling():
    assert _resolve_ray_parallelism(task_group_count=8, requested_parallelism=0) == 8


def test_resolve_ray_chunk_size_auto_prefers_full_fanout_for_small_runs():
    assert _resolve_ray_chunk_size(task_group_count=8, parallelism=8, requested_chunk_size=0) == 1
    assert _resolve_ray_chunk_size(task_group_count=8, parallelism=4, requested_chunk_size=0) == 1


def test_resolve_ray_chunk_size_manual_value_is_preserved():
    assert _resolve_ray_chunk_size(task_group_count=8, parallelism=4, requested_chunk_size=2) == 2


def test_prepare_task_rows_for_partitioning_adds_prefix_and_time_bucket():
    rows = _prepare_task_rows_for_partitioning(
        [
            {
                "space_code": "wtw3sjq6",
                "acq_time": "2021-03-12T00:00:00Z",
            }
        ],
        partition_prefix_len=3,
        time_granularity="day",
    )

    assert rows[0]["space_code_prefix"] == "wtw"
    assert rows[0]["time_bucket"] == "20210312"
