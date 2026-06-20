from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

from cube_split.jobs.ray_logical_partition_job import (
    _chunk_tasks_for_ray,
    _ray_runtime_env_from_env,
    _resolve_ray_chunk_size,
    _resolve_ray_parallelism,
    _should_run_ingest,
    run_logical_partition,
)
from cube_split.jobs.ray_partition_core import _group_tasks_for_local_processing, _prepare_task_rows_for_partitioning


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

    def remote(self, actor_cls):
        return _FakeActorClass(actor_cls)

    def init(self, **kwargs):
        return None

    def wait(self, pending, num_returns=1, timeout=1.0):
        return pending[:num_returns], pending[num_returns:]

    def get(self, ref):
        return ref.value

    def shutdown(self):
        self.shutdown_calls += 1


def test_chunk_tasks_for_ray_preserves_order_and_chunk_size():
    tasks = [{"id": i} for i in range(7)]
    chunks = _chunk_tasks_for_ray(tasks, chunk_size=3)

    assert [[row["id"] for row in chunk] for chunk in chunks] == [[0, 1, 2], [3, 4, 5], [6]]


def test_resolve_ray_parallelism_caps_by_task_count():
    assert _resolve_ray_parallelism(task_group_count=1, requested_parallelism=8) == 1
    assert _resolve_ray_parallelism(task_group_count=4, requested_parallelism=8) == 4
    assert _resolve_ray_parallelism(task_group_count=4, requested_parallelism=2) == 2


def test_resolve_ray_parallelism_auto_uses_task_count_ceiling(monkeypatch):
    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job.os.cpu_count", lambda: 64)

    assert _resolve_ray_parallelism(task_group_count=8, requested_parallelism=0) == 8


def test_ray_runtime_env_passes_source_cache_dir(monkeypatch):
    monkeypatch.setenv("CUBE_SOURCE_CACHE_DIR", "/data/cube_split_source_cache")

    runtime_env = _ray_runtime_env_from_env()

    assert runtime_env is not None
    assert runtime_env["env_vars"]["CUBE_SOURCE_CACHE_DIR"] == "/data/cube_split_source_cache"


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
                "acq_time": "2021-03-12T00:00:00Z",
            }
        ],
        partition_prefix_len=3,
        time_granularity="day",
    )

    assert rows[0]["space_code_prefix"] == "35f"
    assert rows[0]["time_bucket"] == "20210312"


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


def test_logical_partition_runs_ingest_after_rows_are_written(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    cog_dir = tmp_path / "cog"
    input_dir.mkdir()
    captured: dict[str, object] = {}

    source_asset = Namespace(path=str(input_dir / "source.tif"))
    cog_asset = Namespace(path=str(cog_dir / "source_cog.tif"))

    def fake_build_manifest(*args, **kwargs):
        return [source_asset]

    def fake_convert_assets_to_cog(*args, **kwargs):
        cog_dir.mkdir(parents=True)
        (cog_dir / "source_cog.tif").write_bytes(b"cog")
        return [cog_asset]

    def fake_build_grid_tasks_driver(*args, **kwargs):
        return [
            {
                "scene_id": "SCENE_A",
                "band": "b04",
                "asset_path": str(cog_dir / "source_cog.tif"),
                "acq_time": "2026-04-21T00:00:00Z",
                "grid_type": "s2",
                "grid_level": 5,
                "space_code": "35f4",
                "cell_min_lon": 116.1,
                "cell_min_lat": 39.8,
                "cell_max_lon": 116.2,
                "cell_max_lat": 39.9,
                "window_col_off": 0,
                "window_row_off": 0,
                "window_width": 16,
                "window_height": 16,
                "cover_mode": "intersect",
            }
        ]

    def fake_process_local_task_group(group, time_granularity, include_sample_mean=False):
        row = dict(group[0])
        row["st_code"] = "s2:5:35f4:20260421"
        row["time_bucket"] = "20260421"
        row["space_code_prefix"] = "35f"
        row["intersect_min_lon"] = row["cell_min_lon"]
        row["intersect_min_lat"] = row["cell_min_lat"]
        row["intersect_max_lon"] = row["cell_max_lon"]
        row["intersect_max_lat"] = row["cell_max_lat"]
        row["sample_mean_band1"] = None
        return [row]

    def fake_run_ingest(args):
        captured["run_dir"] = args.run_dir
        captured["job_id"] = args.job_id
        captured["metadata_backend"] = args.metadata_backend
        captured["asset_storage_backend"] = args.asset_storage_backend
        captured["minio_bucket"] = args.minio_bucket
        rows_path = Path(args.run_dir) / "index_rows.jsonl"
        assert rows_path.exists()
        return {
            "input_rows": 1,
            "raw_asset_rows": 1,
            "cube_fact_rows": 1,
            "metadata_backend": args.metadata_backend,
            "asset_storage_backend": args.asset_storage_backend,
        }

    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job.build_manifest", fake_build_manifest)
    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job.convert_assets_to_cog", fake_convert_assets_to_cog)
    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job.build_grid_tasks_driver", fake_build_grid_tasks_driver)
    monkeypatch.setattr("cube_split.jobs.ray_partition_core._process_local_task_group", fake_process_local_task_group)
    monkeypatch.setattr("cube_split.ingest.ray_ingest_job.run_ingest", fake_run_ingest)

    report = run_logical_partition(
        Namespace(
            input_dir=str(input_dir),
            manifest_path="",
            product_family="auto",
            output_dir=str(output_dir),
            cog_input_dir=str(cog_dir),
            cog_overwrite=True,
            cog_workers=1,
            cog_compress="LZW",
            cog_predictor=2,
            cog_level=0,
            cog_num_threads="ALL_CPUS",
            target_crs="EPSG:4326",
            grid_type="s2",
            grid_level=5,
            cover_mode="intersect",
            time_granularity="day",
            max_cells_per_asset=20000,
            ray_parallelism=1,
            ray_address="",
            chunk_size=1,
            partition_backend="thread",
            partition_prefix_len=3,
            timing_mode=False,
            skip_verify=False,
            sample_mean=False,
            job_id="",
            dataset="landsat8",
            sensor="L8",
            asset_version="v1",
            cube_version="cube-v1",
            quality_rule="best_quality_wins",
            metadata_backend="postgres",
            postgres_dsn="postgresql://postgres:postgres@127.0.0.1:5432/cube",
            db_path="",
            asset_storage_backend="minio",
            minio_endpoint="10.3.100.179:9000",
            minio_access_key="access",
            minio_secret_key="secret",
            minio_bucket="cube",
            minio_prefix="cube/raw",
            minio_secure=False,
            minio_upload_workers=2,
            cog_output_root=str(tmp_path / "local_cog"),
            cog_materialize_mode="copy",
        )
    )

    assert report["ingest_enabled"] is True
    assert report["metadata_backend"] == "postgres"
    assert report["asset_storage_backend"] == "minio"
    assert report["ingest_stats"]["input_rows"] == 1
    assert captured["job_id"] == Path(captured["run_dir"]).name
    assert captured["minio_bucket"] == "cube"


def test_logical_partition_ray_worker_uses_local_cog_before_upload(monkeypatch, tmp_path: Path):
    fake_ray = _FakeRay()
    source_asset = SimpleNamespace(
        path="/source/scene_a.tif",
        scene_id="SCENE_A",
        band="b04",
        acq_time="2026-04-21T00:00:00Z",
        product_family="other",
        sensor="optical_mosaic",
        bbox=None,
        corners=[[116.1, 39.9], [116.2, 39.9], [116.2, 39.8], [116.1, 39.8]],
        resolution=30,
    )
    worker_cog_path = tmp_path / "worker" / "scene_a_cog.tif"
    calls: list[tuple[str, object]] = []

    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job._load_ray", lambda: fake_ray)
    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job._ray_runtime_env_from_env", lambda: {"env_vars": {}})
    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job._prepend_sys_paths", lambda paths: None)
    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job._ray_project_roots", lambda: [str(tmp_path)])
    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job._ray_actor_options_from_env", lambda: {})
    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job.build_manifest", lambda *args, **kwargs: [source_asset])
    monkeypatch.setattr(
        "cube_split.jobs.ray_logical_partition_job.build_grid_tasks_driver",
        lambda **kwargs: [
            {
                "scene_id": "SCENE_A",
                "band": "b04",
                "asset_path": source_asset.path,
                "acq_time": "2026-04-21T00:00:00Z",
                "grid_type": "s2",
                "grid_level": 5,
                "space_code": "35f4",
                "cell_min_lon": 116.1,
                "cell_min_lat": 39.8,
                "cell_max_lon": 116.2,
                "cell_max_lat": 39.9,
                "window_col_off": 0,
                "window_row_off": 0,
                "window_width": 16,
                "window_height": 16,
                "cover_mode": "intersect",
            },
            {
                "scene_id": "SCENE_A",
                "band": "b04",
                "asset_path": source_asset.path,
                "acq_time": "2026-04-21T00:00:00Z",
                "grid_type": "s2",
                "grid_level": 5,
                "space_code": "35f5",
                "cell_min_lon": 116.2,
                "cell_min_lat": 39.8,
                "cell_max_lon": 116.3,
                "cell_max_lat": 39.9,
                "window_col_off": 0,
                "window_row_off": 0,
                "window_width": 16,
                "window_height": 16,
                "cover_mode": "intersect",
            },
        ],
    )
    monkeypatch.setattr(
        "cube_split.jobs.ray_logical_partition_job._group_tasks_for_local_processing",
        lambda task_rows, **kwargs: [[task_rows[0]], [task_rows[1]]],
    )
    monkeypatch.setattr(
        "cube_split.jobs.ray_logical_partition_job.asset_record_to_dict",
        lambda asset: {
            "scene_id": asset.scene_id,
            "band": asset.band,
            "path": asset.path,
            "acq_time": asset.acq_time,
            "product_family": asset.product_family,
            "sensor": asset.sensor,
            "bbox": asset.bbox,
            "corners": asset.corners,
            "resolution": asset.resolution,
        },
    )
    monkeypatch.setattr(
        "cube_split.jobs.ray_partition_core.asset_record_from_dict",
        lambda row: SimpleNamespace(**row),
    )

    def fake_convert_asset_to_cog(asset, **kwargs):
        worker_cog_path.parent.mkdir(parents=True, exist_ok=True)
        worker_cog_path.write_bytes(b"cog")
        calls.append(("convert", asset.path))
        return SimpleNamespace(
            scene_id=asset.scene_id,
            band=asset.band,
            path=worker_cog_path,
            acq_time=asset.acq_time,
            product_family=asset.product_family,
            sensor=asset.sensor,
            bbox=asset.bbox,
            corners=asset.corners,
            resolution=asset.resolution,
        )

    def fake_process_local_task_group(group, time_granularity, include_sample_mean=False):
        calls.append(("process", str(group[0]["asset_path"])))
        row = dict(group[0])
        row["st_code"] = f"{row['space_code']}:{row['time_bucket']}"
        row["sample_mean_band1"] = None
        row["intersect_min_lon"] = row["cell_min_lon"]
        row["intersect_min_lat"] = row["cell_min_lat"]
        row["intersect_max_lon"] = row["cell_max_lon"]
        row["intersect_max_lat"] = row["cell_max_lat"]
        return [row]

    def fake_upload_cog_to_minio(asset, local_path, options):
        calls.append(("upload", str(local_path)))
        return f"s3://cube/cube/raw/{Path(local_path).name}"

    monkeypatch.setattr("cube_split.jobs.ray_partition_core.convert_asset_to_cog", fake_convert_asset_to_cog)
    monkeypatch.setattr("cube_split.jobs.ray_partition_core._process_local_task_group", fake_process_local_task_group)
    monkeypatch.setattr("cube_split.jobs.ray_partition_core.upload_cog_to_minio", fake_upload_cog_to_minio)

    report = run_logical_partition(
        Namespace(
            input_dir=str(tmp_path),
            manifest_path="",
            product_family="auto",
            output_dir=str(tmp_path / "output"),
            cog_input_dir=str(tmp_path / "cog"),
            cog_overwrite=True,
            cog_workers=1,
            cog_compress="LZW",
            cog_predictor=2,
            cog_level=0,
            cog_num_threads="ALL_CPUS",
            target_crs="EPSG:4326",
            grid_type="s2",
            grid_level=5,
            cover_mode="intersect",
            time_granularity="day",
            max_cells_per_asset=20000,
            ray_parallelism=1,
            ray_address="10.3.100.182:6379",
            chunk_size=2,
            partition_backend="ray",
            partition_prefix_len=3,
            timing_mode=False,
            skip_verify=False,
            sample_mean=False,
            job_id="",
            data_type="optical",
            dataset="demo_optical",
            sensor="optical_mosaic",
            asset_version="v1",
            cube_version="v1",
            quality_rule="best_quality_wins",
            metadata_backend="none",
            postgres_dsn="postgresql://postgres:postgres@127.0.0.1:5432/cube",
            db_path="",
            asset_storage_backend="local",
            minio_endpoint="10.3.100.179:9000",
            minio_access_key="access",
            minio_secret_key="secret",
            minio_bucket="cube",
            minio_prefix="cube/raw",
            minio_secure=False,
            minio_upload_workers=2,
            cog_output_root=str(tmp_path / "local_cog"),
            cog_materialize_mode="copy",
        )
    )

    assert [name for name, _ in calls] == ["convert", "process", "process", "upload"]
    assert calls[1][1] == str(worker_cog_path)
    assert calls[2][1] == str(worker_cog_path)
    assert calls[3][1] == str(worker_cog_path)
    assert report["execution_engine"] == "ray"
    assert report["partition_backend_used"] == "ray"
    assert report["total_index_rows"] == 2
    rows_path = Path(report["run_dir"]) / "index_rows.jsonl"
    rows = [line for line in rows_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert all("s3://cube/cube/raw/scene_a_cog.tif" in row for row in rows)
    assert fake_ray.shutdown_calls == 1


def test_logical_partition_ray_actor_reuses_local_cog_across_chunks(monkeypatch, tmp_path: Path):
    fake_ray = _FakeRay()
    source_asset = SimpleNamespace(
        path="/source/scene_a.tif",
        scene_id="SCENE_A",
        band="b04",
        acq_time="2026-04-21T00:00:00Z",
        product_family="other",
        sensor="optical_mosaic",
        bbox=None,
        corners=[[116.1, 39.9], [116.2, 39.9], [116.2, 39.8], [116.1, 39.8]],
        resolution=30,
    )
    worker_cog_path = tmp_path / "worker" / "scene_a_cog.tif"
    calls: list[tuple[str, object]] = []

    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job._load_ray", lambda: fake_ray)
    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job._ray_runtime_env_from_env", lambda: {"env_vars": {}})
    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job._prepend_sys_paths", lambda paths: None)
    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job._ray_project_roots", lambda: [str(tmp_path)])
    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job._ray_actor_options_from_env", lambda: {})
    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job.build_manifest", lambda *args, **kwargs: [source_asset])
    monkeypatch.setattr(
        "cube_split.jobs.ray_logical_partition_job.build_grid_tasks_driver",
        lambda **kwargs: [
            {
                "scene_id": "SCENE_A",
                "band": "b04",
                "asset_path": source_asset.path,
                "acq_time": "2026-04-21T00:00:00Z",
                "grid_type": "s2",
                "grid_level": 5,
                "space_code": "35f4",
                "cell_min_lon": 116.1,
                "cell_min_lat": 39.8,
                "cell_max_lon": 116.2,
                "cell_max_lat": 39.9,
                "cover_mode": "intersect",
            },
            {
                "scene_id": "SCENE_A",
                "band": "b04",
                "asset_path": source_asset.path,
                "acq_time": "2026-04-21T00:00:00Z",
                "grid_type": "s2",
                "grid_level": 5,
                "space_code": "36a1",
                "cell_min_lon": 116.2,
                "cell_min_lat": 39.8,
                "cell_max_lon": 116.3,
                "cell_max_lat": 39.9,
                "cover_mode": "intersect",
            },
        ],
    )
    monkeypatch.setattr(
        "cube_split.jobs.ray_logical_partition_job.asset_record_to_dict",
        lambda asset: {
            "scene_id": asset.scene_id,
            "band": asset.band,
            "path": asset.path,
            "acq_time": asset.acq_time,
            "product_family": asset.product_family,
            "sensor": asset.sensor,
            "bbox": asset.bbox,
            "corners": asset.corners,
            "resolution": asset.resolution,
        },
    )
    monkeypatch.setattr("cube_split.jobs.ray_partition_core.asset_record_from_dict", lambda row: SimpleNamespace(**row))

    def fake_convert_asset_to_cog(asset, **kwargs):
        worker_cog_path.parent.mkdir(parents=True, exist_ok=True)
        worker_cog_path.write_bytes(b"cog")
        calls.append(("convert", asset.path))
        return SimpleNamespace(
            scene_id=asset.scene_id,
            band=asset.band,
            path=worker_cog_path,
            acq_time=asset.acq_time,
            product_family=asset.product_family,
            sensor=asset.sensor,
            bbox=asset.bbox,
            corners=asset.corners,
            resolution=asset.resolution,
        )

    def fake_process_local_task_group(group, time_granularity, include_sample_mean=False):
        calls.append(("process", str(group[0]["asset_path"])))
        row = dict(group[0])
        row["st_code"] = f"{row['space_code']}:{row['time_bucket']}"
        row["sample_mean_band1"] = None
        row["intersect_min_lon"] = row["cell_min_lon"]
        row["intersect_min_lat"] = row["cell_min_lat"]
        row["intersect_max_lon"] = row["cell_max_lon"]
        row["intersect_max_lat"] = row["cell_max_lat"]
        return [row]

    def fake_upload_cog_to_minio(asset, local_path, options):
        calls.append(("upload", str(local_path)))
        return f"s3://cube/cube/raw/{Path(local_path).name}"

    monkeypatch.setattr("cube_split.jobs.ray_partition_core.convert_asset_to_cog", fake_convert_asset_to_cog)
    monkeypatch.setattr("cube_split.jobs.ray_partition_core._process_local_task_group", fake_process_local_task_group)
    monkeypatch.setattr("cube_split.jobs.ray_partition_core.upload_cog_to_minio", fake_upload_cog_to_minio)

    report = run_logical_partition(
        Namespace(
            input_dir=str(tmp_path),
            manifest_path="",
            product_family="auto",
            output_dir=str(tmp_path / "output"),
            cog_input_dir=str(tmp_path / "cog"),
            cog_overwrite=True,
            cog_workers=1,
            cog_compress="LZW",
            cog_predictor=2,
            cog_level=0,
            cog_num_threads="ALL_CPUS",
            target_crs="EPSG:4326",
            grid_type="s2",
            grid_level=5,
            cover_mode="intersect",
            time_granularity="day",
            max_cells_per_asset=20000,
            ray_parallelism=1,
            ray_address="10.3.100.182:6379",
            chunk_size=1,
            partition_backend="ray",
            partition_prefix_len=3,
            timing_mode=False,
            skip_verify=False,
            sample_mean=False,
            job_id="",
            data_type="optical",
            dataset="demo_optical",
            sensor="optical_mosaic",
            asset_version="v1",
            cube_version="v1",
            quality_rule="best_quality_wins",
            metadata_backend="none",
            postgres_dsn="postgresql://postgres:postgres@127.0.0.1:5432/cube",
            db_path="",
            asset_storage_backend="local",
            minio_endpoint="10.3.100.179:9000",
            minio_access_key="access",
            minio_secret_key="secret",
            minio_bucket="cube",
            minio_prefix="cube/raw",
            minio_secure=False,
            minio_upload_workers=2,
            cog_output_root=str(tmp_path / "local_cog"),
            cog_materialize_mode="copy",
        )
    )

    assert [name for name, _ in calls] == ["convert", "process", "upload", "process"]
    assert calls.count(("convert", source_asset.path)) == 1
    assert calls.count(("upload", str(worker_cog_path))) == 1
    assert report["chunk_size"] == 1
    assert report["total_index_rows"] == 2
