from cube_split.partition_timing import finish_partition_timing, partition_timing_from_workers


def test_partition_timing_starts_after_the_latest_worker_started() -> None:
    marker = partition_timing_from_workers(
        {
            "workers": [
                {"scope": "logical_worker", "started_at": "2026-08-25T10:00:01Z"},
                {"scope": "logical_worker", "started_at": "2026-08-25T10:00:03Z"},
            ],
            "driver": {"scope": "logical_batch_driver", "started_at": "2026-08-25T09:59:50Z"},
        }
    )

    assert marker is not None
    assert marker["started_at"] == "2026-08-25T10:00:03Z"
    assert marker["worker_count"] == 2
    assert marker["elapsed_sec"] is None

    completed = finish_partition_timing(marker, "2026-08-25T10:00:13Z")

    assert completed is not None
    assert completed["finished_at"] == "2026-08-25T10:00:13Z"
    assert completed["elapsed_sec"] == 10.0


def test_partition_timing_does_not_invent_a_start_without_worker_records() -> None:
    assert partition_timing_from_workers({"driver": {"scope": "raster_driver"}}) is None


def test_partition_timing_reads_workers_from_combined_scene_units() -> None:
    marker = partition_timing_from_workers(
        {
            "units": [
                {"workers": [{"scope": "logical_worker", "started_at": "2026-08-25T10:00:01Z"}]},
                {"workers": [{"scope": "carbon_worker", "started_at": "2026-08-25T10:00:02Z"}]},
            ]
        }
    )

    assert marker is not None
    assert marker["started_at"] == "2026-08-25T10:00:02Z"
    assert marker["worker_count"] == 2
    assert marker["worker_scopes"] == ["carbon_worker", "logical_worker"]
