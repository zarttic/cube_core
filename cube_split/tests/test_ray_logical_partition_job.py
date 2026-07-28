from __future__ import annotations

from argparse import Namespace
from pathlib import Path

from cube_split.jobs.cancellation import shutdown_ray_if_needed
from cube_split.jobs.logical_chunk_codec import compress_logical_chunk, logical_chunk_id, serialize_logical_chunk_rows
from cube_split.jobs.ray_logical_partition_job import (
    _chunk_task_groups_by_actor,
    _chunk_tasks_for_ray,
    _ray_actor_options_from_env,
    _ray_runtime_env_from_env,
    _resolve_ray_actor_parallelism,
    _resolve_ray_chunk_size,
    _resolve_ray_parallelism,
    _should_run_ingest,
    parse_args,
)
from cube_split.jobs.ray_partition_core import _group_tasks_for_local_processing, _prepare_task_rows_for_partitioning
from cube_split.jobs.ray_logical_chunk_job import _time_bucket, logical_output_id, run_logical_chunk_jobs
from cube_web.services.partition_contracts import OutputIdentity, make_output_id


class _FakeObjectRef:
    def __init__(self, value):
        self.value = value


class _FakeRemoteMethod:
    def __init__(self, func):
        self._func = func

    def remote(self, *args, **kwargs):
        return _FakeObjectRef(self._func(*args, **kwargs))


class _FakeActorHandle:
    def __init__(self, actor_cls):
        self._instance = actor_cls()

    def __getattr__(self, name):
        return _FakeRemoteMethod(getattr(self._instance, name))


class _FakeActorClass:
    def __init__(self, actor_cls):
        self._actor_cls = actor_cls

    def options(self, **kwargs):
        return self

    def remote(self):
        return _FakeActorHandle(self._actor_cls)


class _FakeRay:
    def __init__(self):
        self.shutdown_calls = 0
        self._initialized = False

    def remote(self, actor_cls):
        return _FakeActorClass(actor_cls)

    def init(self, **kwargs):
        self._initialized = True
        return None

    def wait(self, pending, num_returns=1, timeout=1.0):
        return pending[:num_returns], pending[num_returns:]

    def get(self, ref):
        return ref.value

    def shutdown(self):
        self.shutdown_calls += 1
        self._initialized = False

    def is_initialized(self):
        return self._initialized


class _FakeLogicalChunkRemote:
    def __init__(self, func, events):
        self._func = func
        self._events = events

    def options(self, **kwargs):
        assert kwargs == {"num_cpus": 1}
        return self

    def remote(self, value):
        self._events.append(("submit", value["name"]))
        return _FakeObjectRef(self._func(value))


class _FakeLogicalChunkRay(_FakeRay):
    def __init__(self, events):
        super().__init__()
        self._events = events

    def remote(self, func):
        return _FakeLogicalChunkRemote(func, self._events)


def test_chunk_tasks_for_ray_preserves_order_and_chunk_size():
    tasks = [{"id": i} for i in range(7)]
    chunks = _chunk_tasks_for_ray(tasks, chunk_size=3)

    assert [[row["id"] for row in chunk] for chunk in chunks] == [[0, 1, 2], [3, 4, 5], [6]]


def test_logical_chunk_scheduler_consumes_dataset_iterators_lazily_and_round_robin(monkeypatch):
    events = []
    fake_ray = _FakeLogicalChunkRay(events)

    def logical_values(payload):
        for index in range(3):
            name = f"{payload['dataset']['dataset_id']}:{index}"
            events.append(("yield", name))
            yield {"name": name}

    monkeypatch.setitem(__import__("sys").modules, "ray", fake_ray)
    monkeypatch.setattr("cube_split.jobs.ray_logical_chunk_job._logical_task_values", logical_values)
    monkeypatch.setattr("cube_split.jobs.ray_logical_chunk_job._logical_task_limit", lambda: 2)
    monkeypatch.setattr("cube_split.jobs.ray_logical_chunk_job._plan_logical_chunk", lambda value: {"chunk_id": value["name"]})

    payloads = [
        {
            "dataset": {"dataset_id": dataset_id},
            "task_id": f"task-{dataset_id}",
            "output_version": "v1",
            "grid_type": "isea4h",
            "requested_grid_level": 5,
            "ray_address": "auto",
        }
        for dataset_id in ("dataset-a", "dataset-b")
    ]

    outcomes = run_logical_chunk_jobs(payloads, runtime_env=None)

    assert events[:4] == [
        ("yield", "dataset-a:0"),
        ("submit", "dataset-a:0"),
        ("yield", "dataset-b:0"),
        ("submit", "dataset-b:0"),
    ]
    assert [chunk["chunk_id"] for chunk in outcomes[0]["result"]["chunks"]] == ["dataset-a:0", "dataset-a:1", "dataset-a:2"]
    assert [chunk["chunk_id"] for chunk in outcomes[1]["result"]["chunks"]] == ["dataset-b:0", "dataset-b:1", "dataset-b:2"]


def test_chunk_task_groups_by_actor_keeps_asset_groups_together():
    groups = [
        [{"asset_path": "/source/a.tif", "space_code": "35f4"}],
        [{"asset_path": "/source/a.tif", "space_code": "35f5"}],
        [{"asset_path": "/source/b.tif", "space_code": "36a1"}],
        [{"asset_path": "/source/a.tif", "space_code": "35f6"}],
        [{"asset_path": "/source/b.tif", "space_code": "36a2"}],
    ]

    chunks_by_actor = _chunk_task_groups_by_actor(groups, parallelism=2, chunk_size=1)

    actor_by_asset: dict[str, int] = {}
    for actor_idx, actor_chunks in enumerate(chunks_by_actor):
        for chunk in actor_chunks:
            for group in chunk:
                asset_path = group[0]["asset_path"]
                actor_by_asset.setdefault(asset_path, actor_idx)
                assert actor_by_asset[asset_path] == actor_idx
    assert actor_by_asset == {"/source/a.tif": 0, "/source/b.tif": 1}


def test_resolve_ray_parallelism_caps_by_task_count():
    assert _resolve_ray_parallelism(task_group_count=1, requested_parallelism=8) == 1
    assert _resolve_ray_parallelism(task_group_count=4, requested_parallelism=8) == 4
    assert _resolve_ray_parallelism(task_group_count=4, requested_parallelism=2) == 2


def test_resolve_ray_actor_parallelism_caps_by_asset_count():
    groups = [
        [{"asset_path": "/source/a.tif"}],
        [{"asset_path": "/source/a.tif"}],
        [{"asset_path": "/source/b.tif"}],
    ]

    assert _resolve_ray_actor_parallelism(groups, requested_parallelism=8) == 2


def test_ray_actor_options_reads_runtime_config(monkeypatch):
    monkeypatch.setattr(
        "cube_split.jobs.ray_logical_partition_job.runtime_config.env_text",
        lambda name: "node:10.3.100.180" if name == "RAY_ACTOR_NODE_RESOURCE" else "",
    )

    assert _ray_actor_options_from_env() == {"resources": {"node:10.3.100.180": 0.001}}


def test_logical_chunk_identity_matches_partition_contract():
    identity = OutputIdentity(
        dataset_id="dataset-a", output_version="version-a", source_asset_id="asset-a", band_code="B01",
        grid_type="geohash", grid_level=6, space_code="wxyz", topology_code=None,
        time_bucket="20260701", window_identity="logical-reference",
    )
    assert logical_output_id(
        dataset_id="dataset-a", output_version="version-a", source_asset_id="asset-a", band_code="B01",
        grid_type="geohash", grid_level=6, space_code="wxyz", topology_code=None,
        time_bucket="20260701", window_identity="logical-reference",
    ) == make_output_id(identity)


def test_logical_chunk_time_bucket_uses_utc_date():
    assert _time_bucket("2026-07-21T07:15:25+08:00", "day") == "20260720"


def test_logical_chunk_compression_is_deterministic():
    content = b'{"kind":"indexes","row":{"output_id":"output-1"}}\n'
    compressed = compress_logical_chunk(content)

    assert compressed == compress_logical_chunk(content)
    assert compressed[4:8] == b"\0\0\0\0"


def test_logical_chunk_serialization_is_independent_of_input_order():
    rows = [
        {"kind": "tiles", "row": {"output_id": "tile-2", "band_code": "B02"}},
        {"kind": "grid_cells", "row": {"output_id": "cell-1", "space_code": "35f"}},
        {"kind": "tiles", "row": {"output_id": "tile-1", "band_code": "B01"}},
    ]

    assert serialize_logical_chunk_rows(rows) == serialize_logical_chunk_rows(list(reversed(rows)))


def test_logical_chunk_identity_includes_selected_bands():
    band_1 = {"source_asset_id": "asset-1", "band_code": "B01", "attributes": {"band_unit_id": "unit-1"}}
    band_2 = {"source_asset_id": "asset-1", "band_code": "B02", "attributes": {"band_unit_id": "unit-2"}}
    common = {"dataset_id": "dataset-1", "output_version": "version-1", "shard_id": "asset-1:0"}

    assert logical_chunk_id(**common, bands=[band_1, band_2]) == logical_chunk_id(**common, bands=[band_2, band_1])
    assert logical_chunk_id(**common, bands=[band_1]) != logical_chunk_id(**common, bands=[band_2])


def test_parse_args_allows_mgrs_grid_type(monkeypatch):
    monkeypatch.setattr("sys.argv", ["ray_logical_partition_job.py", "--grid-type", "mgrs"])
    args = parse_args()
    assert args.grid_type == "mgrs"


def test_parse_args_uses_production_grid_contract_without_conversion_options(monkeypatch):
    monkeypatch.setattr("sys.argv", ["ray_logical_partition_job.py", "--grid-type", "geohash"])
    args = parse_args()
    assert args.grid_type == "geohash"
    assert args.max_cells_per_asset == 0
    assert not hasattr(args, "cog_workers")
    assert not hasattr(args, "target_crs")
    assert not hasattr(args, "timing_mode")


def test_resolve_ray_parallelism_auto_uses_task_count_ceiling(monkeypatch):
    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job.os.cpu_count", lambda: 64)

    assert _resolve_ray_parallelism(task_group_count=8, requested_parallelism=0) == 8


def test_shutdown_ray_if_needed_skips_already_initialized_ray():
    fake_ray = _FakeRay()
    fake_ray._initialized = True

    shutdown_ray_if_needed(fake_ray, already_initialized=True)

    assert fake_ray.shutdown_calls == 0


def test_ray_runtime_env_passes_source_cache_dir(monkeypatch):
    monkeypatch.setenv("CUBE_SOURCE_CACHE_DIR", "/data/cube_split_source_cache")

    runtime_env = _ray_runtime_env_from_env()

    assert runtime_env is not None
    assert runtime_env["env_vars"]["CUBE_SOURCE_CACHE_DIR"] == "/data/cube_split_source_cache"
    assert ".codegraph/**" in runtime_env["excludes"]
    assert ".run/**" in runtime_env["excludes"]
    assert ".tmp/**" in runtime_env["excludes"]
    assert "cube_web/frontend/test-results/**" in runtime_env["excludes"]


def test_resolve_ray_chunk_size_auto_prefers_full_fanout_for_small_runs():
    assert _resolve_ray_chunk_size(task_group_count=8, parallelism=8, requested_chunk_size=0) == 1
    assert _resolve_ray_chunk_size(task_group_count=8, parallelism=4, requested_chunk_size=0) == 1


def test_resolve_ray_chunk_size_manual_value_is_preserved():
    assert _resolve_ray_chunk_size(task_group_count=8, parallelism=4, requested_chunk_size=2) == 2


def test_prepare_task_rows_for_partitioning_adds_prefix_and_time_bucket():
    rows = _prepare_task_rows_for_partitioning(
        [
            {
                "space_code": "35f04",
                "acq_time": "2026-07-21T07:15:25+08:00",
            }
        ],
        partition_prefix_len=3,
        time_granularity="day",
    )

    assert rows[0]["space_code_prefix"] == "35f"
    assert rows[0]["time_bucket"] == "20260720"


def test_group_tasks_can_split_single_asset_by_space_prefix():
    tasks = [
        {"asset_path": "/source/a.tif", "space_code": "35f04", "space_code_prefix": "35f"},
        {"asset_path": "/source/a.tif", "space_code": "35f05", "space_code_prefix": "35f"},
        {"asset_path": "/source/a.tif", "space_code": "36a01", "space_code_prefix": "36a"},
    ]

    grouped = _group_tasks_for_local_processing(tasks, split_by_space_prefix=True)

    assert [[row["space_code"] for row in group] for group in grouped] == [["35f04", "35f05"], ["36a01"]]


def test_should_run_ingest_uses_metadata_backend_and_explicit_override():
    assert _should_run_ingest(Namespace(metadata_backend="none")) is False
    assert _should_run_ingest(Namespace(metadata_backend="postgres")) is True
    assert _should_run_ingest(Namespace(metadata_backend="sqlite")) is True
    assert _should_run_ingest(Namespace(metadata_backend="postgres", ingest_enabled=False)) is False


def test_logical_partition_does_not_shutdown_shared_ray(monkeypatch, tmp_path: Path):
    fake_ray = _FakeRay()
    fake_ray._initialized = True

    shutdown_ray_if_needed(fake_ray, already_initialized=True)

    assert fake_ray.shutdown_calls == 0
