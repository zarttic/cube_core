import sys
from types import SimpleNamespace

import pytest

import cube_web.services.partition_dataset_runner as runner_module
from cube_web.services.partition_dataset_runner import (
    ENTITY_RAY_PARALLELISM,
    NormalizedPartitionDatasetRunner,
    _carbon_index_attributes,
    _consume_observation_budget,
    _entity_cells_by_shard,
    _entity_cells_for_shard,
    _entity_cover_cells,
    _entity_minio_parallel_uploads,
    _entity_planning_shards,
    _entity_ray_parallelism,
    _group_entity_payloads,
    _logical_shards,
    _normalize_wgs84_bbox,
    _ray_init_runtime_env,
    _record_asset_cell,
    _run_dataset_on_ray,
    _sha256_file,
    _sha256_file_cached,
    _source_band_index,
    _time_bucket,
    _wait_for_ray_batch,
    _wait_for_ray_result,
)


class _FakeRay:
    def __init__(self) -> None:
        self.cancelled: list[tuple[object, bool]] = []

    def cancel(self, ref, *, force: bool) -> None:
        self.cancelled.append((ref, force))

    def wait(self, *_args, **_kwargs):
        return [], []

    def get(self, ref):
        return {"ref": ref}


class _FakeBatchWaitRay:
    def __init__(self) -> None:
        self.calls: list[tuple[list[object], int, dict[str, object]]] = []
        self.responses = [
            (["first", "second", "third"], ["fourth"]),
        ]

    def wait(self, refs, *, num_returns: int, **kwargs):
        self.calls.append((list(refs), num_returns, kwargs))
        return self.responses.pop(0)


def test_time_bucket_uses_utc_date() -> None:
    assert _time_bucket("2026-07-21T07:15:25+08:00", "day") == "20260720"


def test_normalize_wgs84_bbox_clamps_raster_edges() -> None:
    assert _normalize_wgs84_bbox([-180.0044, -90.0022, 180.0044, 90.0022]) == [-180.0, -90.0, 180.0, 90.0]


def test_sha256_file_streams_content_and_reports_size(tmp_path) -> None:
    from hashlib import sha256

    payload = b"cube-source" * 200_000
    path = tmp_path / "source.tif"
    path.write_bytes(payload)

    digest, size = _sha256_file(path)

    assert digest == sha256(payload).hexdigest()
    assert size == len(payload)


def test_sha256_file_cache_reuses_unchanged_file(tmp_path) -> None:
    path = tmp_path / "source.tif"
    path.write_bytes(b"source-content")

    first = _sha256_file_cached(path)
    second = _sha256_file_cached(path)

    assert first[2] is False
    assert second == (first[0], first[1], True)


def test_logical_shards_cover_registered_boundary_without_cog_access() -> None:
    assert _logical_shards([100.0, 20.0, 101.0, 21.0], 0.5) == [
        (100.0, 20.0, 100.5, 20.5),
        (100.5, 20.0, 101.0, 20.5),
        (100.0, 20.5, 100.5, 21.0),
        (100.5, 20.5, 101.0, 21.0),
    ]


def test_entity_planning_uses_fixed_sixteen_spatial_shards() -> None:
    shards = _entity_planning_shards([100.0, 20.0, 104.0, 24.0])

    assert ENTITY_RAY_PARALLELISM == 16
    assert len(shards) == 16
    assert shards[0] == [99.98, 19.98, 101.02, 21.02]
    assert shards[-1] == [102.98, 22.98, 104.02, 24.02]


def test_entity_ray_parallelism_env_override(monkeypatch) -> None:
    monkeypatch.delenv("CUBE_ENTITY_RAY_PARALLELISM", raising=False)
    assert _entity_ray_parallelism() == 16


def test_entity_minio_parallel_uploads_env_override(monkeypatch) -> None:
    monkeypatch.delenv("CUBE_ENTITY_MINIO_PARALLEL_UPLOADS", raising=False)
    assert _entity_minio_parallel_uploads() == 3

    monkeypatch.setenv("CUBE_ENTITY_MINIO_PARALLEL_UPLOADS", "1")
    assert _entity_minio_parallel_uploads() == 1

    monkeypatch.setenv("CUBE_ENTITY_MINIO_PARALLEL_UPLOADS", "99")
    assert _entity_minio_parallel_uploads() == 8

    monkeypatch.setenv("CUBE_ENTITY_RAY_PARALLELISM", "25")
    assert _entity_ray_parallelism() == 25

    monkeypatch.setenv("CUBE_ENTITY_RAY_PARALLELISM", "0")
    assert _entity_ray_parallelism() == 1

    monkeypatch.setenv("CUBE_ENTITY_RAY_PARALLELISM", "not-a-number")
    assert _entity_ray_parallelism() == 16


def test_entity_planning_resolves_parallelism_at_execution_time(monkeypatch) -> None:
    monkeypatch.setenv("CUBE_ENTITY_RAY_PARALLELISM", "25")

    shards = _entity_planning_shards([100.0, 20.0, 104.0, 24.0])

    assert len(shards) == 25


def test_entity_cover_is_cached_and_cells_are_filtered_by_shard(monkeypatch) -> None:
    calls = 0

    class FakeSDK:
        def cover(self, **_kwargs):
            nonlocal calls
            calls += 1
            from grid_core.app.models.grid_cell import GridCell

            return [
                GridCell(grid_type="isea4h", grid_level=6, space_code="1", bbox=[100.0, 20.0, 101.0, 21.0], center=[100.5, 20.5]),
                GridCell(grid_type="isea4h", grid_level=6, space_code="2", bbox=[102.0, 22.0, 103.0, 23.0], center=[102.5, 22.5]),
            ]

    monkeypatch.setattr("grid_core.sdk.CubeEncoderSDK", FakeSDK)
    with runner_module._ENTITY_COVER_CACHE_LOCK:
        runner_module._ENTITY_COVER_CACHE.clear()
    from cube_split.partition_timing import TimingRecorder

    timing = TimingRecorder("test")
    first = _entity_cover_cells(
        grid_type="isea4h", requested_grid_level=6, cover_mode="intersect", asset_bbox=[100.0, 20.0, 103.0, 23.0], timing=timing,
    )
    second = _entity_cover_cells(
        grid_type="isea4h", requested_grid_level=6, cover_mode="intersect", asset_bbox=[100.0, 20.0, 103.0, 23.0], timing=timing,
    )

    assert calls == 1
    assert first == second
    assert [cell["space_code"] for cell in _entity_cells_for_shard(first, [99.9, 19.9, 101.1, 21.1])] == ["1"]


def test_entity_cells_are_assigned_to_one_overlapping_shard() -> None:
    cells = [
        {"space_code": "left", "bbox": [100.0, 20.0, 101.0, 21.0], "center": [100.5, 20.5]},
        {"space_code": "right", "bbox": [101.0, 20.0, 102.0, 21.0], "center": [101.5, 20.5]},
    ]
    shards = [[99.9, 19.9, 101.1, 21.1], [100.9, 19.9, 102.1, 21.1]]

    assigned = _entity_cells_by_shard(cells, shards)

    assert [[cell["space_code"] for cell in group] for group in assigned] == [["left"], ["right"]]


def test_entity_payloads_group_same_source_bands_and_keep_unit_order(monkeypatch) -> None:
    monkeypatch.setenv("CUBE_ENTITY_BANDS_PER_TASK", "2")
    asset = {
        "source_asset_id": "asset-1",
        "cog_uri": "s3://cube/source/scene.tif",
        "checksum": "a" * 64,
        "bbox": [100.0, 20.0, 101.0, 21.0],
        "time_start": "2026-01-01T00:00:00Z",
        "time_end": "2026-01-01T00:00:00Z",
    }

    def payload(band_code: str) -> dict[str, object]:
        return {
            "dataset": {
                "dataset_id": "dataset-1",
                "assets": [asset],
                "bands": [{"source_asset_id": "asset-1", "band_code": band_code}],
            },
            "task_id": "task-1",
            "output_version": "version-1",
            "grid_type": "isea4h",
            "requested_grid_level": 6,
            "cover_mode": "intersect",
            "max_cells_per_asset": 0,
            "time_granularity": "day",
            "max_observations": None,
            "ray_address": "ray-address",
        }

    groups = _group_entity_payloads([payload("B01"), payload("B02")])

    assert len(groups) == 1
    assert groups[0]["payload_indexes"] == [0, 1]
    assert [band["band_code"] for band in groups[0]["payload"]["dataset"]["bands"]] == ["B01", "B02"]
    assert groups[0]["payload"]["batch_group_unit_count"] == 2


def test_entity_units_use_one_batch_driver_and_keep_input_order(monkeypatch) -> None:
    observed_payloads = []

    def fake_entity_batch(payloads, _runtime_env, _cancellation_check):
        observed_payloads.extend(payloads)
        return [
            {"result": {"dataset_id": payload["dataset"]["dataset_id"]}}
            for payload in payloads
        ]

    runner = NormalizedPartitionDatasetRunner()
    monkeypatch.setattr(runner, "_ray_execution_context", lambda: ("ray-address", {"env_vars": {}}))
    monkeypatch.setattr(runner, "_verify_assets_exist", lambda _payloads: None)
    monkeypatch.setattr(runner_module, "_run_entity_dataset_batch_on_ray", fake_entity_batch)
    first = SimpleNamespace(data_type="optical", model_dump=lambda **_kwargs: {"dataset_id": "first", "data_type": "optical"})
    second = SimpleNamespace(data_type="radar", model_dump=lambda **_kwargs: {"dataset_id": "second", "data_type": "radar"})

    outcomes = runner.run_datasets(
        runs=[
            {"dataset": first, "task_id": "task", "output_version": "v1", "grid_type": "isea4h", "requested_grid_level": 6, "cover_mode": "intersect", "max_cells_per_asset": 0, "time_granularity": "day", "max_observations": None},
            {"dataset": second, "task_id": "task", "output_version": "v2", "grid_type": "isea4h", "requested_grid_level": 6, "cover_mode": "intersect", "max_cells_per_asset": 0, "time_granularity": "day", "max_observations": None},
        ],
    )

    assert [payload["dataset"]["dataset_id"] for payload in observed_payloads] == ["first", "second"]
    assert [outcome["result"]["dataset_id"] for outcome in outcomes] == ["first", "second"]


def test_ray_job_driver_does_not_reapply_runtime_env(monkeypatch) -> None:
    runtime_env = {"working_dir": "gcs://package", "env_vars": {"PYTHONPATH": "."}}
    monkeypatch.setenv("CUBE_WEB_RAY_JOB_DRIVER", "1")
    assert _ray_init_runtime_env(runtime_env) is None
    monkeypatch.delenv("CUBE_WEB_RAY_JOB_DRIVER")
    assert _ray_init_runtime_env(runtime_env) == runtime_env


def test_carbon_unique_cells_are_not_limited_per_asset() -> None:
    cells: set[tuple[str, int, str | None]] = set()
    _record_asset_cell(cells, ("u4pr", 5, None), max_cells_per_asset=1)
    _record_asset_cell(cells, ("u4pr", 5, None), max_cells_per_asset=1)
    _record_asset_cell(cells, ("u4ps", 5, None), max_cells_per_asset=1)

    assert cells == {("u4pr", 5, None), ("u4ps", 5, None)}


def test_ray_wait_cancels_active_remote_task() -> None:
    ray = _FakeRay()

    with pytest.raises(Exception, match="Partition task cancelled"):
        _wait_for_ray_result(ray, "ray-ref", lambda: True)

    assert ray.cancelled == [("ray-ref", True)]


def test_ray_batch_wait_blocks_for_one_parallel_window() -> None:
    ray = _FakeBatchWaitRay()

    ready, pending = _wait_for_ray_batch(ray, ["first", "second", "third", "fourth"], 3)

    assert ready == ["first", "second", "third"]
    assert pending == ["fourth"]
    assert ray.calls == [(["first", "second", "third", "fourth"], 3, {})]


def test_dataset_ray_task_uses_configured_node_resource(monkeypatch) -> None:
    class FakeRemoteTask:
        def __init__(self) -> None:
            self.options_value = None

        def options(self, **kwargs):
            self.options_value = kwargs
            return self

        def remote(self, _payload):
            return "task-ref"

    class FakeRay:
        def __init__(self) -> None:
            self.task = FakeRemoteTask()

        def remote(self, _function):
            return self.task

        def is_initialized(self) -> bool:
            return True

        def get(self, ref):
            assert ref == "task-ref"
            return {"status": "completed"}

    ray = FakeRay()
    monkeypatch.setitem(sys.modules, "ray", ray)
    monkeypatch.setenv("RAY_ACTOR_NODE_RESOURCE", "node:10.3.100.180")

    result = _run_dataset_on_ray({"ray_address": "10.3.100.182:6379"}, None)

    assert result["status"] == "completed"
    assert result["timings"]["driver"]["scope"] == "raster_driver"
    assert ray.task.options_value == {"resources": {"node:10.3.100.180": 0.001}}


def test_ray_client_uses_node_local_credentials_without_runtime_env(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(runner_module.runtime_config, "require_ray_address", lambda: "ray://10.3.100.182:10001")
    monkeypatch.setattr(
        runner_module,
        "_run_dataset_on_ray",
        lambda payload, runtime_env, cancellation_check: captured.update(payload=payload, runtime_env=runtime_env) or {"status": "completed"},
    )
    monkeypatch.setattr(
        "cube_split.jobs.ray_logical_partition_job._ray_runtime_env_from_env",
        lambda: {"env_vars": {"CUBE_WEB_POSTGRES_DSN": "must-not-be-forwarded"}},
    )
    dataset = SimpleNamespace(data_type="optical", model_dump=lambda **_kwargs: {"dataset_id": "dataset-1"})

    result = runner_module.NormalizedPartitionDatasetRunner().run_dataset(
        dataset=dataset,
        task_id="task-1",
        output_version="version-1",
        grid_type="quadtree",
        requested_grid_level=1,
        cover_mode="covers",
    )

    assert result["status"] == "completed"
    assert result["timings"]["source_preflight"]["scope"] == "source_preflight"
    assert captured["runtime_env"] == {"env_vars": {}}
    assert captured["payload"] == {
        "dataset": {"dataset_id": "dataset-1"}, "task_id": "task-1", "output_version": "version-1",
        "grid_type": "quadtree", "requested_grid_level": 1, "cover_mode": "covers",
        "time_granularity": "day", "max_cells_per_asset": 0, "max_observations": None,
        "ray_address": "ray://10.3.100.182:10001",
    }


def test_batch_runner_submits_all_logical_units_to_one_ray_queue(monkeypatch) -> None:
    captured: dict[str, object] = {}
    runner = NormalizedPartitionDatasetRunner()
    monkeypatch.setattr(runner, "_ray_execution_context", lambda: ("ray-address", {"env_vars": {}}))
    monkeypatch.setattr(
        "cube_split.jobs.ray_logical_chunk_job.run_logical_chunk_jobs",
        lambda payloads, runtime_env, cancellation_check: captured.update(
            payloads=payloads, runtime_env=runtime_env, cancellation_check=cancellation_check,
        ) or [{"result": {"dataset_id": payload["dataset"]["dataset_id"]}} for payload in payloads],
    )
    first = SimpleNamespace(data_type="optical", model_dump=lambda **_kwargs: {"dataset_id": "first", "data_type": "optical"})
    second = SimpleNamespace(data_type="optical", model_dump=lambda **_kwargs: {"dataset_id": "second", "data_type": "optical"})

    outcomes = runner.run_datasets(
        runs=[
            {"dataset": first, "task_id": "task", "output_version": "v1", "grid_type": "geohash", "requested_grid_level": 5, "cover_mode": "intersect", "max_cells_per_asset": 0, "time_granularity": "day", "max_observations": None, "scene_id": "scene-first", "band_unit_id": "unit-first"},
            {"dataset": second, "task_id": "task", "output_version": "v2", "grid_type": "mgrs", "requested_grid_level": 3, "cover_mode": "intersect", "max_cells_per_asset": 0, "time_granularity": "day", "max_observations": None},
        ],
    )

    assert [payload["dataset"]["dataset_id"] for payload in captured["payloads"]] == ["first", "second"]
    assert captured["runtime_env"] == {"env_vars": {}}
    assert "scene_id" not in captured["payloads"][0]
    assert "band_unit_id" not in captured["payloads"][0]
    assert [outcome["result"]["dataset_id"] for outcome in outcomes] == ["first", "second"]
    assert outcomes[0]["result"]["timings"]["source_preflight"]["scope"] == "source_preflight"
    assert outcomes[1]["result"]["timings"]["source_preflight"]["scope"] == "source_preflight"
    assert outcomes[0]["result"]["timings"]["source_preflight"]["attributes"]["dataset_id"] == "first"
    assert outcomes[1]["result"]["timings"]["source_preflight"]["attributes"]["dataset_id"] == "second"


def test_carbon_observation_budget_is_shared_across_assets() -> None:
    remaining = 100
    remaining = _consume_observation_budget(remaining, 60)
    assert remaining == 40
    remaining = _consume_observation_budget(remaining, 40)
    assert remaining == 0
    assert _consume_observation_budget(None, 1000) is None


def test_carbon_index_attributes_preserve_normalized_observation_fields() -> None:
    footprint = {"type": "Point", "coordinates": [116.3, 39.9]}
    row = {
        "satellite": "OCO2",
        "observation_id": "obs-1",
        "xco2": 410.25,
        "quality_flag": "0",
        "center_lon": 116.3,
        "center_lat": 39.9,
        "footprint_geojson": footprint,
        "source_index": None,
        "metadata_json": '{"orbit": 42}',
        "product_type": "xco2",
    }

    assert _carbon_index_attributes(row, source_index=7) == {
        "satellite": "OCO2",
        "observation_id": "obs-1",
        "xco2": 410.25,
        "quality_flag": "0",
        "center_lon": 116.3,
        "center_lat": 39.9,
        "footprint_geojson": footprint,
        "source_index": 7,
        "metadata_json": '{"orbit": 42}',
        "product_type": "xco2",
    }


def test_source_band_index_prefers_loader_metadata_and_validates_bounds() -> None:
    assert _source_band_index({"display_order": 0, "attributes": {"source_band_index": 3}}, 4) == 3
    assert _source_band_index({"display_order": 1, "attributes": {}}, 4) == 2
    with pytest.raises(ValueError, match="outside raster band count"):
        _source_band_index({"display_order": 4, "attributes": {}}, 4)
