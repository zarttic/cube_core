from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

import cube_split.jobs.entity_partition_job as entity_partition_job
from cube_split.jobs.entity_partition_job import run_entity_partition


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
        self.kill_calls = 0

    def remote(self, actor_cls):
        return _FakeActorClass(actor_cls)

    def init(self, **kwargs):
        return None

    def wait(self, pending, num_returns=1, timeout=1.0):
        return pending[:num_returns], pending[num_returns:]

    def get(self, ref):
        if isinstance(ref, list):
            return [item.value for item in ref]
        return ref.value

    def kill(self, actor, no_restart=True):
        self.kill_calls += 1

    def shutdown(self):
        self.shutdown_calls += 1


def _write_tif(path: Path) -> None:
    transform = from_origin(116.0, 40.0, 0.001, 0.001)
    data = np.ones((1, 32, 32), dtype=np.uint8)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=32,
        height=32,
        count=1,
        dtype=data.dtype,
        crs="EPSG:4326",
        transform=transform,
    ) as ds:
        ds.write(data)


def test_entity_partition_writes_one_hex_file_per_band(tmp_path: Path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    source = input_dir / "demo_scene_blue.tif"
    _write_tif(source)

    report = run_entity_partition(
        SimpleNamespace(
            input_dir=str(input_dir),
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
            grid_level=0,
            target_pixels_per_hex_edge=768,
            cover_mode="intersect",
            time_granularity="day",
            max_cells_per_asset=20000,
            partition_prefix_len=3,
            partition_backend="thread",
            ray_address="",
            ray_parallelism=0,
            chunk_size=0,
            asset_storage_backend="local",
            metadata_backend="none",
        )
    )

    run_dir = Path(report["run_dir"])
    entity_rows_path = run_dir / "entity_index_rows.jsonl"
    rows = [json.loads(line) for line in entity_rows_path.read_text(encoding="utf-8").splitlines()]

    assert report["status"] == "completed"
    assert report["partition_type"] == "entity"
    assert report["data_type"] == "optical"
    assert report["grid_type"] == "isea4h"
    assert report["requested_grid_level"] is None
    assert report["grid_level"] == report["inferred_grid_level"]
    assert report["rows"] == len(rows)
    assert report["entity_tile_count"] == len(rows) >= 1
    assert (run_dir / "index_rows.jsonl").exists()

    row = rows[0]
    tile_path = Path(row["output_path"])
    assert row["partition_type"] == "entity"
    assert row["data_type"] == "optical"
    assert row["asset_path"] == str(tile_path)
    assert row["source_asset_path"] == str(source.resolve())
    assert "entity_tiles/optical" in tile_path.as_posix()
    assert row["space_code"]
    assert row["st_code"].startswith("hx:")
    assert row["st_time_granularity"] == "day"
    assert tile_path.exists()
    with rasterio.open(tile_path) as ds:
        assert ds.count == 1
        assert ds.nodata == 0
        assert ds.width > 0
        assert ds.height > 0


def test_entity_writer_preserves_original_source_asset_path(tmp_path: Path):
    run_dir = tmp_path / "run"
    source = tmp_path / "source.tif"
    worker_source = tmp_path / "worker" / "source.tif"
    worker_source.parent.mkdir()
    _write_tif(worker_source)
    cell = entity_partition_job.CubeEncoderSDK().locate(
        grid_type="isea4h",
        level=1,
        point=[116.016, 39.984],
    )

    tasks = [
        {
            "scene_id": "scene-a",
            "band": "b04",
            "asset_path": str(worker_source),
            "source_asset_path": str(source),
            "acq_time": "2026-04-21T00:00:00Z",
            "grid_type": "isea4h",
            "grid_level": 1,
            "space_code": cell.space_code,
            "cover_mode": "intersect",
            "cell_min_lon": float(cell.bbox[0]),
            "cell_min_lat": float(cell.bbox[1]),
            "cell_max_lon": float(cell.bbox[2]),
            "cell_max_lat": float(cell.bbox[3]),
        }
    ]

    rows = entity_partition_job._write_entity_tiles(
        tasks,
        run_dir=run_dir,
        time_granularity="day",
        partition_prefix_len=3,
        data_type="optical",
    )

    assert rows
    assert rows[0]["source_asset_path"] == str(source)
    assert rows[0]["asset_path"] != rows[0]["source_asset_path"]


def test_entity_task_grouping_batches_by_asset_and_space_prefix():
    tasks = [
        {"asset_path": "/source/a.tif", "space_code": "811aaa"},
        {"asset_path": "/source/a.tif", "space_code": "811bbb"},
        {"asset_path": "/source/a.tif", "space_code": "812aaa"},
        {"asset_path": "/source/b.tif", "space_code": "811ccc"},
        {"asset_path": "/source/a.tif", "space_code": "811ccc"},
    ]

    groups = entity_partition_job._group_tasks_for_parallel_processing(
        tasks,
        partition_prefix_len=3,
        max_tasks_per_group=2,
    )

    assert [[task["space_code"] for task in group] for group in groups] == [
        ["811aaa", "811bbb"],
        ["811ccc"],
        ["812aaa"],
        ["811ccc"],
    ]
    assert [group[0]["asset_path"] for group in groups] == [
        "/source/a.tif",
        "/source/a.tif",
        "/source/a.tif",
        "/source/b.tif",
    ]


def test_entity_auto_parallelism_uses_split_groups_after_prefix_batching(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    source = input_dir / "demo_scene_parallel.tif"
    _write_tif(source)
    captured: dict[str, object] = {}

    def fake_build_grid_tasks_driver(**kwargs):
        asset = kwargs["assets"][0]
        return [
            {
                "scene_id": asset.scene_id,
                "band": asset.band,
                "asset_path": asset.path,
                "acq_time": asset.acq_time,
                "grid_type": "isea4h",
                "grid_level": kwargs["grid_level"],
                "space_code": f"811{i:012x}",
                "cover_mode": "intersect",
                "cell_min_lon": 0.0,
                "cell_min_lat": 0.0,
                "cell_max_lon": 1.0,
                "cell_max_lat": 1.0,
            }
            for i in range(130)
        ]

    def fake_ray_writer(task_chunks, run_dir, time_granularity, partition_prefix_len, parallelism, ray_address, **kwargs):
        _ = time_granularity, partition_prefix_len, ray_address, kwargs
        captured["parallelism"] = parallelism
        captured["task_chunk_sizes"] = [sum(len(group) for group in chunk) for chunk in task_chunks]
        captured["tile_upload_options"] = kwargs.get("tile_upload_options")
        rows = []
        for chunk in task_chunks:
            for group in chunk:
                for task in group:
                    tile_uri = f"s3://entity-bucket/cube/entity/{task['space_code']}.tif"
                    rows.append(
                        {
                            "partition_type": "entity",
                            "data_type": "optical",
                            "scene_id": task["scene_id"],
                            "band": task["band"],
                            "asset_path": tile_uri,
                            "source_asset_path": task["asset_path"],
                            "output_path": tile_uri,
                            "acq_time": task["acq_time"],
                            "grid_type": "isea4h",
                            "grid_level": task["grid_level"],
                            "space_code": task["space_code"],
                            "space_code_prefix": task["space_code"][:3],
                            "st_code": f"hx:{task['grid_level']}:{task['space_code']}:19700101",
                            "time_bucket": "19700101",
                            "cover_mode": task["cover_mode"],
                            "cell_min_lon": task["cell_min_lon"],
                            "cell_min_lat": task["cell_min_lat"],
                            "cell_max_lon": task["cell_max_lon"],
                            "cell_max_lat": task["cell_max_lat"],
                            "window_col_off": 0,
                            "window_row_off": 0,
                            "window_width": 1,
                            "window_height": 1,
                            "nodata": 0,
                            "valid_pixel_ratio": 1.0,
                        }
                    )
        return rows, 0.1, 0.2, 0.3, 0.4, 0.5

    monkeypatch.setattr("cube_split.jobs.ray_logical_partition_job.os.cpu_count", lambda: 8)
    monkeypatch.setattr(entity_partition_job, "build_grid_tasks_driver", fake_build_grid_tasks_driver)
    monkeypatch.setattr(entity_partition_job, "_write_entity_tile_chunks_ray", fake_ray_writer)

    report = run_entity_partition(
        SimpleNamespace(
            input_dir=str(input_dir),
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
            grid_level=1,
            target_pixels_per_hex_edge=768,
            cover_mode="intersect",
            time_granularity="day",
            max_cells_per_asset=20000,
            partition_prefix_len=3,
            partition_backend="ray",
            ray_address="10.3.100.182:6379",
            ray_parallelism=0,
            chunk_size=0,
            asset_storage_backend="local",
            metadata_backend="none",
            minio_endpoint="10.3.100.179:9000",
            minio_access_key="access",
            minio_secret_key="secret",
            minio_bucket="entity-bucket",
            minio_prefix="cube/entity",
            minio_secure=False,
            minio_upload_workers=2,
        )
    )

    assert report["grid_task_count"] == 130
    assert report["task_group_count"] == 8
    assert report["ray_parallelism"] == 8
    assert report["asset_storage_backend"] == "minio"
    assert report["uploaded_tile_count"] == 130
    assert captured["parallelism"] == 8
    assert captured["task_chunk_sizes"] == [17, 17, 17, 17, 17, 17, 17, 11]
    assert captured["tile_upload_options"]["minio_bucket"] == "entity-bucket"


def test_entity_partition_minio_updates_rows_and_postgres_metadata(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    _write_tif(input_dir / "demo_scene_green.tif")
    captured: dict[str, object] = {}

    def fake_upload(rows, args):
        captured["upload_rows"] = len(rows)
        return {row["asset_path"]: f"s3://entity-bucket/{Path(row['asset_path']).name}" for row in rows}

    def fake_metadata(rows, args, run_dir):
        captured["metadata_rows"] = len(rows)
        captured["metadata_asset_path"] = rows[0]["asset_path"]
        captured["metadata_local_asset_path"] = rows[0]["local_asset_path"]
        return {"entity_tile_rows": len(rows)}

    monkeypatch.setattr(entity_partition_job, "_upload_entity_tiles_to_minio", fake_upload)
    monkeypatch.setattr(entity_partition_job, "_write_entity_metadata_postgres", fake_metadata)

    report = run_entity_partition(
        SimpleNamespace(
            input_dir=str(input_dir),
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
            grid_level=0,
            target_pixels_per_hex_edge=768,
            cover_mode="intersect",
            time_granularity="day",
            max_cells_per_asset=20000,
            partition_prefix_len=3,
            partition_backend="thread",
            ray_address="",
            ray_parallelism=0,
            chunk_size=0,
            dataset="demo_optical",
            sensor="optical_mosaic",
            asset_version="v1",
            asset_storage_backend="minio",
            metadata_backend="postgres",
            postgres_dsn="postgresql://example",
            minio_endpoint="10.3.100.179:9000",
            minio_access_key="access",
            minio_secret_key="secret",
            minio_bucket="entity-bucket",
            minio_prefix="cube/entity",
            minio_secure=False,
            minio_upload_workers=2,
        )
    )

    rows_path = Path(report["rows_path"])
    rows = [json.loads(line) for line in rows_path.read_text(encoding="utf-8").splitlines()]

    assert report["asset_storage_backend"] == "minio"
    assert report["metadata_backend"] == "postgres"
    assert report["uploaded_tile_count"] == len(rows)
    assert report["metadata_rows"] == len(rows)
    assert rows[0]["asset_path"].startswith("s3://entity-bucket/")
    assert rows[0]["output_path"] == rows[0]["asset_path"]
    assert Path(rows[0]["local_asset_path"]).exists()
    assert captured["upload_rows"] == len(rows)
    assert captured["metadata_rows"] == len(rows)
    assert str(captured["metadata_asset_path"]).startswith("s3://entity-bucket/")
    assert Path(str(captured["metadata_local_asset_path"])).exists()


def test_entity_tile_minio_upload_keys_include_space_code(monkeypatch, tmp_path: Path):
    captured_keys: list[str] = []

    class FakeStat:
        size = 4

    class FakeMinio:
        def __init__(self, *args, **kwargs):
            pass

        def bucket_exists(self, _bucket):
            return True

        def stat_object(self, _bucket, key):
            captured_keys.append(key)
            return FakeStat()

        def fput_object(self, _bucket, _key, _path):
            raise AssertionError("stat hit should skip upload")

    monkeypatch.setattr("minio.Minio", FakeMinio)
    first = tmp_path / "cell-a" / "sr_band2.tif"
    second = tmp_path / "cell-b" / "sr_band2.tif"
    first.parent.mkdir()
    second.parent.mkdir()
    first.write_bytes(b"data")
    second.write_bytes(b"data")
    rows = [
        {
            "asset_path": str(first),
            "scene_id": "scene-a",
            "band": "sr_band2",
            "acq_time": "2020-08-01T00:00:00Z",
            "grid_level": 6,
            "space_code": "86283082fffffff",
        },
        {
            "asset_path": str(second),
            "scene_id": "scene-a",
            "band": "sr_band2",
            "acq_time": "2020-08-01T00:00:00Z",
            "grid_level": 6,
            "space_code": "862830837ffffff",
        },
    ]

    uploaded = entity_partition_job._upload_entity_tiles_to_minio(
        rows,
        SimpleNamespace(
            dataset="demo_optical",
            sensor="optical_mosaic",
            asset_version="v1",
            minio_endpoint="127.0.0.1:9000",
            minio_access_key="access",
            minio_secret_key="secret",
            minio_bucket="entity-bucket",
            minio_prefix="cube/entity",
            minio_secure=False,
            minio_upload_workers=1,
        ),
    )

    assert len(uploaded) == 2
    assert uploaded[str(first)] != uploaded[str(second)]
    assert "space_code=86283082fffffff" in uploaded[str(first)]
    assert "space_code=862830837ffffff" in uploaded[str(second)]
    assert len(set(captured_keys)) == 2


def test_entity_partition_ingest_disabled_skips_minio_and_postgres(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    _write_tif(input_dir / "demo_scene_red.tif")

    def fail_upload(rows, args):
        raise AssertionError("MinIO upload should not run when ingest is disabled")

    def fail_metadata(rows, args, run_dir):
        raise AssertionError("PostgreSQL metadata write should not run when ingest is disabled")

    monkeypatch.setattr(entity_partition_job, "_upload_entity_tiles_to_minio", fail_upload)
    monkeypatch.setattr(entity_partition_job, "_write_entity_metadata_postgres", fail_metadata)

    report = run_entity_partition(
        SimpleNamespace(
            input_dir=str(input_dir),
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
            grid_level=0,
            target_pixels_per_hex_edge=768,
            cover_mode="intersect",
            time_granularity="day",
            max_cells_per_asset=20000,
            partition_prefix_len=3,
            partition_backend="thread",
            ray_address="",
            ray_parallelism=0,
            chunk_size=0,
            dataset="demo_optical",
            sensor="optical_mosaic",
            asset_version="v1",
            asset_storage_backend="minio",
            metadata_backend="postgres",
            postgres_dsn="postgresql://example",
            minio_endpoint="10.3.100.179:9000",
            minio_access_key="access",
            minio_secret_key="secret",
            minio_bucket="entity-bucket",
            minio_prefix="cube/entity",
            minio_secure=False,
            minio_upload_workers=2,
            ingest_enabled=False,
        )
    )

    assert report["ingest_enabled"] is False
    assert report["asset_storage_backend"] == "local"
    assert report["metadata_backend"] == "none"
    assert report["uploaded_tile_count"] == 0
    assert report["metadata_rows"] == 0


def test_entity_partition_ray_ingest_disabled_still_uses_minio_tiles(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    _write_tif(input_dir / "demo_scene_ray.tif")
    captured: dict[str, object] = {}

    def fake_ray_writer(task_chunks, run_dir, time_granularity, partition_prefix_len, parallelism, ray_address, **kwargs):
        _ = run_dir, time_granularity, partition_prefix_len, parallelism, ray_address
        captured["tile_upload_options"] = kwargs.get("tile_upload_options")
        rows = []
        for chunk in task_chunks:
            for group in chunk:
                for task in group:
                    tile_uri = f"s3://entity-bucket/cube/entity/{task['space_code']}.tif"
                    rows.append(
                        {
                            "partition_type": "entity",
                            "data_type": "optical",
                            "scene_id": task["scene_id"],
                            "band": task["band"],
                            "asset_path": tile_uri,
                            "source_asset_path": task["asset_path"],
                            "output_path": tile_uri,
                            "acq_time": task["acq_time"],
                            "grid_type": "isea4h",
                            "grid_level": task["grid_level"],
                            "space_code": task["space_code"],
                            "space_code_prefix": task["space_code"][:3],
                            "st_code": f"hx:{task['grid_level']}:{task['space_code']}:19700101",
                            "time_bucket": "19700101",
                            "cover_mode": task["cover_mode"],
                            "cell_min_lon": task["cell_min_lon"],
                            "cell_min_lat": task["cell_min_lat"],
                            "cell_max_lon": task["cell_max_lon"],
                            "cell_max_lat": task["cell_max_lat"],
                            "window_col_off": 0,
                            "window_row_off": 0,
                            "window_width": 1,
                            "window_height": 1,
                            "nodata": 0,
                            "valid_pixel_ratio": 1.0,
                        }
                    )
        return rows, 0.1, 0.2, 0.3, 0.4, 0.5

    def fail_upload(rows, args):
        raise AssertionError("Ray worker should upload entity tiles before returning rows")

    def fail_metadata(rows, args, run_dir):
        raise AssertionError("PostgreSQL metadata write should not run when ingest is disabled")

    monkeypatch.setattr(entity_partition_job, "_write_entity_tile_chunks_ray", fake_ray_writer)
    monkeypatch.setattr(entity_partition_job, "_upload_entity_tiles_to_minio", fail_upload)
    monkeypatch.setattr(entity_partition_job, "_write_entity_metadata_postgres", fail_metadata)

    report = run_entity_partition(
        SimpleNamespace(
            input_dir=str(input_dir),
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
            grid_level=0,
            target_pixels_per_hex_edge=768,
            cover_mode="intersect",
            time_granularity="day",
            max_cells_per_asset=20000,
            partition_prefix_len=3,
            partition_backend="ray",
            ray_address="10.3.100.182:6379",
            ray_parallelism=0,
            chunk_size=0,
            dataset="demo_optical",
            sensor="optical_mosaic",
            asset_version="v1",
            asset_storage_backend="local",
            metadata_backend="postgres",
            postgres_dsn="postgresql://example",
            minio_endpoint="10.3.100.179:9000",
            minio_access_key="access",
            minio_secret_key="secret",
            minio_bucket="entity-bucket",
            minio_prefix="cube/entity",
            minio_secure=False,
            minio_upload_workers=2,
            ingest_enabled=False,
        )
    )

    rows = [json.loads(line) for line in Path(report["rows_path"]).read_text(encoding="utf-8").splitlines()]
    assert report["ingest_enabled"] is False
    assert report["asset_storage_backend"] == "minio"
    assert report["metadata_backend"] == "none"
    assert report["uploaded_tile_count"] == len(rows)
    assert all(row["asset_path"].startswith("s3://entity-bucket/") for row in rows)
    assert captured["tile_upload_options"]["minio_bucket"] == "entity-bucket"


def test_entity_partition_ray_requires_minio_tile_output(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    _write_tif(input_dir / "demo_scene_missing_minio.tif")

    def fail_ray_writer(*args, **kwargs):
        raise AssertionError("Ray writer should not run without MinIO tile output settings")

    monkeypatch.setattr(entity_partition_job, "_write_entity_tile_chunks_ray", fail_ray_writer)

    with pytest.raises(ValueError, match="Ray entity partition requires minio"):
        run_entity_partition(
            SimpleNamespace(
                input_dir=str(input_dir),
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
                grid_level=0,
                target_pixels_per_hex_edge=768,
                cover_mode="intersect",
                time_granularity="day",
                max_cells_per_asset=20000,
                partition_prefix_len=3,
                partition_backend="ray",
                ray_address="10.3.100.182:6379",
                ray_parallelism=0,
                chunk_size=0,
                dataset="demo_optical",
                sensor="optical_mosaic",
                asset_version="v1",
                asset_storage_backend="local",
                metadata_backend="none",
                minio_endpoint="",
                minio_access_key="",
                minio_secret_key="",
                minio_bucket="",
                minio_prefix="cube/entity",
                minio_secure=False,
                minio_upload_workers=2,
            )
        )


def test_write_entity_metadata_postgres_reports_probe_metrics(monkeypatch, tmp_path: Path):
    captured: list[entity_partition_job.TileProbeMetric] = []
    calls: list[tuple[str, object]] = []
    rows = [
        {
            "scene_id": "scene-a",
            "band": "sr_band2",
            "acq_time": "2020-08-01T00:00:00Z",
            "grid_type": "isea4h",
            "grid_level": 6,
            "space_code": "86283082fffffff",
            "space_code_prefix": "862",
            "st_code": "hx:6:86283082fffffff:20200801",
            "time_bucket": "20200801",
            "entity_tile_uri": "s3://entity-bucket/cube/entity/fake.tif",
            "local_asset_path": str(tmp_path / "fake.tif"),
            "asset_path": "s3://entity-bucket/cube/entity/fake.tif",
            "source_asset_path": "s3://cube/cube/source/optocal/a.tif",
            "cover_mode": "intersect",
            "cell_min_lon": 116.0,
            "cell_min_lat": 39.0,
            "cell_max_lon": 117.0,
            "cell_max_lat": 40.0,
            "window_width": 256,
            "window_height": 256,
            "nodata": 0,
            "valid_pixel_ratio": 1.0,
            "partition_type": "entity",
            "data_type": "optical",
            "window_col_off": 0,
            "window_row_off": 0,
        }
    ]

    def fake_report_tile_metrics(metrics):
        captured.extend(list(metrics))

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, *_exc_info):
            return False

        def execute(self, sql, params=None):
            _ = params
            calls.append(("execute", sql))
            if "SELECT retry_count" in sql:
                self._row = None

        def executemany(self, sql, values):
            calls.append(("executemany", len(values)))

        def fetchone(self):
            return getattr(self, "_row", None)

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_exc_info):
            return False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            calls.append(("commit", None))

    class FakePsycopg:
        @staticmethod
        def connect(dsn, client_encoding="UTF8"):
            _ = client_encoding
            calls.append(("connect", dsn))
            return FakeConnection()

    monkeypatch.setitem(sys.modules, "psycopg", FakePsycopg)
    monkeypatch.setattr(entity_partition_job, "report_tile_metrics", fake_report_tile_metrics)

    stats = entity_partition_job._write_entity_metadata_postgres(
        rows,
        SimpleNamespace(
            postgres_dsn="postgresql://example",
            dataset="demo_optical",
            sensor="optical_mosaic",
            asset_version="v1",
            job_id="entity-job-probe",
            asset_storage_backend="minio",
        ),
        tmp_path / "run_probe",
    )

    assert stats["entity_tile_rows"] == 1
    assert ("connect", "postgresql://example") in calls
    assert ("executemany", 1) in calls
    assert len(captured) == 1
    assert captured[0].task_name == "cube.partition.entity.ingest.optical"
    assert captured[0].method_name == "merge.rs_entity_tile_asset"
    assert captured[0].attributes["cube.scene_id"] == "scene-a"
    assert captured[0].attributes["cube.space_code"] == "86283082fffffff"
    assert captured[0].attributes["cube.target_table"] == "rs_entity_tile_asset"


def test_entity_partition_does_not_limit_cover_cells(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    source = input_dir / "demo_scene_unlimited.tif"
    _write_tif(source)
    captured: dict[str, object] = {}

    def fake_build_grid_tasks_driver(**kwargs):
        captured["max_cells_per_asset"] = kwargs["max_cells_per_asset"]
        asset = kwargs["assets"][0]
        return [
            {
                "scene_id": asset.scene_id,
                "band": asset.band,
                "asset_path": asset.path,
                "acq_time": asset.acq_time,
                "grid_type": "isea4h",
                "grid_level": 1,
                "space_code": "811ffffffffffff",
                "space_code_prefix": "811",
                "time_bucket": "19700101",
                "cover_mode": "intersect",
                "cell_min_lon": 0.0,
                "cell_min_lat": 0.0,
                "cell_max_lon": 1.0,
                "cell_max_lat": 1.0,
            },
            {
                "scene_id": asset.scene_id,
                "band": asset.band,
                "asset_path": asset.path,
                "acq_time": asset.acq_time,
                "grid_type": "isea4h",
                "grid_level": 1,
                "space_code": "812ffffffffffff",
                "space_code_prefix": "812",
                "time_bucket": "19700101",
                "cover_mode": "intersect",
                "cell_min_lon": 1.0,
                "cell_min_lat": 0.0,
                "cell_max_lon": 2.0,
                "cell_max_lat": 1.0,
            },
        ]

    def fake_writer(task_chunks, run_dir, time_granularity, partition_prefix_len, workers, data_type="optical"):
        _ = time_granularity, partition_prefix_len, workers
        rows = []
        for chunk in task_chunks:
            for group in chunk:
                task = group[0]
                rows.append(
                    {
                        "partition_type": "entity",
                        "data_type": data_type,
                        **task,
                        "asset_path": str((run_dir / f"{task['space_code']}.tif").resolve()),
                        "source_asset_path": task["asset_path"],
                        "output_path": str((run_dir / f"{task['space_code']}.tif").resolve()),
                "st_code": f"hx:1:{task['space_code']}:19700101",
                "window_col_off": 0,
                "window_row_off": 0,
                        "window_width": 1,
                        "window_height": 1,
                        "nodata": 0,
                        "valid_pixel_ratio": 1.0,
                    }
                )
        return rows

    monkeypatch.setattr(entity_partition_job, "build_grid_tasks_driver", fake_build_grid_tasks_driver)
    monkeypatch.setattr(entity_partition_job, "_write_entity_tile_chunks_thread", fake_writer)

    report = run_entity_partition(
        SimpleNamespace(
            input_dir=str(input_dir),
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
            grid_level=1,
            target_pixels_per_hex_edge=768,
            cover_mode="intersect",
            time_granularity="day",
            max_cells_per_asset=1,
            partition_prefix_len=3,
            partition_backend="thread",
            ray_address="",
            ray_parallelism=0,
            chunk_size=0,
            asset_storage_backend="local",
            metadata_backend="none",
        )
    )

    assert captured["max_cells_per_asset"] == 0
    assert report["grid_task_count"] == 2
    assert report["entity_tile_count"] == 2


def test_entity_partition_passes_data_type_to_manifest_and_writer(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    asset_path = input_dir / "product_1980.tif"
    captured: dict[str, object] = {}

    def fake_build_manifest(input_dir_arg, product_family, data_type, manifest_path):
        captured["manifest_input_dir"] = input_dir_arg
        captured["manifest_data_type"] = data_type
        captured["manifest_product_family"] = product_family
        captured["manifest_path"] = manifest_path
        return [
            entity_partition_job.AssetRecord(
                scene_id="product_1980",
                band="product_value",
                path=str(asset_path),
                acq_time="1980-01-01T00:00:00Z",
                product_family="product",
                sensor="data_product",
                resolution=30.0,
            )
        ]

    def fake_build_grid_tasks_driver(**kwargs):
        asset = kwargs["assets"][0]
        return [
            {
                "scene_id": asset.scene_id,
                "band": asset.band,
                "asset_path": asset.path,
                "acq_time": asset.acq_time,
                "grid_type": "isea4h",
                "grid_level": kwargs["grid_level"],
                "space_code": "832830fffffffff",
                "cover_mode": "intersect",
                "cell_min_lon": 100.0,
                "cell_min_lat": 20.0,
                "cell_max_lon": 101.0,
                "cell_max_lat": 21.0,
            }
        ]

    def fake_writer(task_chunks, run_dir, time_granularity, partition_prefix_len, workers, data_type="optical"):
        _ = time_granularity, partition_prefix_len, workers
        captured["writer_data_type"] = data_type
        task = task_chunks[0][0][0]
        tile_path = run_dir / "entity_tiles" / data_type / task["scene_id"] / "fake.tif"
        return [
            {
                "partition_type": "entity",
                "data_type": data_type,
                "scene_id": task["scene_id"],
                "band": task["band"],
                "asset_path": str(tile_path.resolve()),
                "source_asset_path": task["asset_path"],
                "output_path": str(tile_path.resolve()),
                "acq_time": task["acq_time"],
                "grid_type": "isea4h",
                "grid_level": task["grid_level"],
                "space_code": task["space_code"],
                "space_code_prefix": "832",
                "st_code": "hx:6:832830fffffffff:1980",
                "time_bucket": "1980",
                "st_time_granularity": "day",
                "cover_mode": task["cover_mode"],
                "cell_min_lon": task["cell_min_lon"],
                "cell_min_lat": task["cell_min_lat"],
                "cell_max_lon": task["cell_max_lon"],
                "cell_max_lat": task["cell_max_lat"],
                "window_col_off": 0,
                "window_row_off": 0,
                "window_width": 1,
                "window_height": 1,
                "nodata": 0,
                "valid_pixel_ratio": 1.0,
            }
        ]

    monkeypatch.setattr(entity_partition_job, "build_manifest", fake_build_manifest)
    monkeypatch.setattr(entity_partition_job, "build_grid_tasks_driver", fake_build_grid_tasks_driver)
    monkeypatch.setattr(entity_partition_job, "_write_entity_tile_chunks_thread", fake_writer)

    report = run_entity_partition(
        SimpleNamespace(
            input_dir=str(input_dir),
            manifest_path="",
            product_family="product",
            data_type="product",
            output_dir=str(tmp_path / "output"),
            cog_input_dir=str(tmp_path / "cog"),
            cog_overwrite=True,
            cog_workers=1,
            cog_compress="LZW",
            cog_predictor=2,
            cog_level=0,
            cog_num_threads="ALL_CPUS",
            target_crs="EPSG:4326",
            grid_level=6,
            target_pixels_per_hex_edge=768,
            cover_mode="intersect",
            time_granularity="year",
            max_cells_per_asset=20000,
            partition_prefix_len=3,
            partition_backend="thread",
            ray_address="",
            ray_parallelism=0,
            chunk_size=0,
            asset_storage_backend="local",
            metadata_backend="none",
        )
    )

    rows = [json.loads(line) for line in Path(report["rows_path"]).read_text(encoding="utf-8").splitlines()]
    assert captured["manifest_input_dir"] == input_dir
    assert captured["manifest_data_type"] == "product"
    assert captured["manifest_product_family"] == "product"
    assert captured["manifest_path"] is None
    assert captured["writer_data_type"] == "product"
    assert report["data_type"] == "product"
    assert rows[0]["data_type"] == "product"
    assert rows[0]["time_bucket"] == "1980"
    assert rows[0]["st_time_granularity"] == "day"


def test_entity_partition_dispatches_ray_backend(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    _write_tif(input_dir / "demo_scene_nir.tif")
    captured: dict[str, object] = {}

    def fake_ray_writer(task_chunks, run_dir, time_granularity, partition_prefix_len, parallelism, ray_address, **kwargs):
        _ = time_granularity, partition_prefix_len
        captured["task_chunk_count"] = len(task_chunks)
        captured["parallelism"] = parallelism
        captured["ray_address"] = ray_address
        captured["assets_by_path"] = kwargs.get("assets_by_path")
        captured["tile_upload_options"] = kwargs.get("tile_upload_options")
        tile_uri = "s3://entity-bucket/cube/entity/fake.tif"
        return (
            [
                {
                    "partition_type": "entity",
                    "scene_id": "demo_scene_nir",
                    "band": "demo_scene_nir",
                    "asset_path": tile_uri,
                    "source_asset_path": str((input_dir / "demo_scene_nir.tif").resolve()),
                    "output_path": tile_uri,
                    "acq_time": "1970-01-01T00:00:00Z",
                    "grid_type": "isea4h",
                    "grid_level": 4,
                    "space_code": "842a107ffffffff",
                    "space_code_prefix": "842",
                    "st_code": "hx:4:842a107ffffffff:19700101",
                    "time_bucket": "19700101",
                    "cover_mode": "intersect",
                    "cell_min_lon": 0.0,
                    "cell_min_lat": 0.0,
                    "cell_max_lon": 1.0,
                    "cell_max_lat": 1.0,
                    "window_col_off": 0,
                    "window_row_off": 0,
                    "window_width": 1,
                    "window_height": 1,
                    "nodata": 0,
                    "valid_pixel_ratio": 1.0,
                }
            ],
            0.25,
            0.2,
            0.3,
            0.4,
            0.5,
        )

    monkeypatch.setattr(entity_partition_job, "_write_entity_tile_chunks_ray", fake_ray_writer)

    report = run_entity_partition(
        SimpleNamespace(
            input_dir=str(input_dir),
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
            grid_level=0,
            target_pixels_per_hex_edge=768,
            cover_mode="intersect",
            time_granularity="day",
            max_cells_per_asset=20000,
            partition_prefix_len=3,
            partition_backend="ray",
            ray_address="10.3.100.182:6379",
            ray_parallelism=0,
            chunk_size=1,
            asset_storage_backend="local",
            metadata_backend="none",
            minio_endpoint="10.3.100.179:9000",
            minio_access_key="access",
            minio_secret_key="secret",
            minio_bucket="entity-bucket",
            minio_prefix="cube/entity",
            minio_secure=False,
            minio_upload_workers=2,
        )
    )

    assert report["execution_engine"] == "ray"
    assert report["partition_backend_used"] == "ray"
    assert report["ray_parallelism"] == 1
    assert report["ray_address"] == "10.3.100.182:6379"
    assert report["ray_init_elapsed_sec"] == 0.25
    assert report["source_prepare_elapsed_sec"] == 0.2
    assert report["partition_elapsed_sec"] == 0.3
    assert report["source_prepare_worker_elapsed_sec"] == 0.4
    assert report["worker_partition_elapsed_sec"] == 0.5
    assert captured["parallelism"] == 1
    assert captured["ray_address"] == "10.3.100.182:6379"
    assert captured["assets_by_path"] is None
    assert captured["tile_upload_options"]["minio_bucket"] == "entity-bucket"
    assert report["asset_storage_backend"] == "minio"
    assert report["uploaded_tile_count"] == 1


def test_entity_ray_worker_reads_source_without_intermediate_cog(monkeypatch, tmp_path: Path):
    fake_ray = _FakeRay()
    calls: list[tuple[str, object]] = []

    monkeypatch.setattr(entity_partition_job, "_load_ray", lambda: fake_ray)
    monkeypatch.setattr(entity_partition_job, "_ray_runtime_env_from_env", lambda: {"env_vars": {}})
    monkeypatch.setattr(entity_partition_job, "_prepend_sys_paths", lambda paths: None)
    monkeypatch.setattr(entity_partition_job, "_ray_project_roots", lambda: [str(tmp_path)])
    monkeypatch.setattr(entity_partition_job, "_ray_actor_options_from_env", lambda: {})
    def fake_convert_asset_to_cog(*args, **kwargs):
        raise AssertionError("entity ray worker should not convert source assets to intermediate COG")

    def fake_upload_cog_to_minio(*args, **kwargs):
        raise AssertionError("entity ray worker should not upload intermediate COG before tile writing")

    def fake_upload_tiles(rows, args):
        calls.append(("tile_upload_bucket", args.minio_bucket))
        return {row["asset_path"]: f"s3://{args.minio_bucket}/entity/{Path(row['asset_path']).name}" for row in rows}

    def fake_writer(tasks, run_dir, time_granularity, partition_prefix_len, data_type="optical", source_options=None):
        _ = time_granularity, partition_prefix_len
        calls.append(("writer_assets", [task["asset_path"] for task in tasks]))
        calls.append(("source_options", source_options))
        return [
            {
                "partition_type": "entity",
                "data_type": data_type,
                "scene_id": task["scene_id"],
                "band": task["band"],
                "asset_path": str((run_dir / f"{task['space_code']}.tif").resolve()),
                "source_asset_path": task.get("source_asset_path", task["asset_path"]),
                "output_path": str((run_dir / f"{task['space_code']}.tif").resolve()),
                "acq_time": task["acq_time"],
                "grid_type": "isea4h",
                "grid_level": task["grid_level"],
                "space_code": task["space_code"],
                "space_code_prefix": task["space_code"][:3],
                "st_code": f"hx:{task['grid_level']}:{task['space_code']}:20260421",
                "time_bucket": "20260421",
                "cover_mode": task["cover_mode"],
                "cell_min_lon": task["cell_min_lon"],
                "cell_min_lat": task["cell_min_lat"],
                "cell_max_lon": task["cell_max_lon"],
                "cell_max_lat": task["cell_max_lat"],
                "window_col_off": 0,
                "window_row_off": 0,
                "window_width": 1,
                "window_height": 1,
                "nodata": 0,
                "valid_pixel_ratio": 1.0,
            }
            for task in tasks
        ]

    monkeypatch.setattr("cube_split.jobs.ray_partition_core.convert_asset_to_cog", fake_convert_asset_to_cog)
    monkeypatch.setattr("cube_split.jobs.ray_partition_core.upload_cog_to_minio", fake_upload_cog_to_minio)
    monkeypatch.setattr(entity_partition_job, "_write_entity_tiles", fake_writer)
    monkeypatch.setattr(entity_partition_job, "_upload_entity_tiles_to_minio", fake_upload_tiles)
    monkeypatch.setattr(entity_partition_job, "resolve_asset_source_path", lambda source_uri, options=None: source_uri)

    source_path = "/source/scene.tif"
    task_a = {
        "scene_id": "scene-a",
        "band": "b04",
        "asset_path": source_path,
        "acq_time": "2026-04-21T00:00:00Z",
        "grid_type": "isea4h",
        "grid_level": 1,
        "space_code": "811ffffffffffff",
        "cover_mode": "intersect",
        "cell_min_lon": 0.0,
        "cell_min_lat": 0.0,
        "cell_max_lon": 1.0,
        "cell_max_lat": 1.0,
    }
    task_b = {**task_a, "space_code": "812ffffffffffff"}

    rows, ray_init_elapsed, source_prepare_elapsed, partition_elapsed, source_prepare_worker_elapsed, worker_partition_elapsed = entity_partition_job._write_entity_tile_chunks_ray(
        task_chunks=[[[task_a]], [[task_b]]],
        run_dir=tmp_path / "run",
        time_granularity="day",
        partition_prefix_len=3,
        parallelism=1,
        ray_address="10.3.100.182:6379",
        data_type="optical",
        source_options={"endpoint": "10.3.100.179:9000"},
        tile_upload_options={
            "minio_bucket": "cube",
            "minio_endpoint": "10.3.100.179:9000",
            "minio_access_key": "access",
            "minio_secret_key": "secret",
        },
    )

    assert ray_init_elapsed >= 0
    assert source_prepare_elapsed >= 0
    assert partition_elapsed >= 0
    assert source_prepare_worker_elapsed >= 0
    assert worker_partition_elapsed >= 0
    assert calls == [
        ("writer_assets", [source_path, source_path]),
        ("source_options", None),
        ("tile_upload_bucket", "cube"),
    ]
    assert [row["asset_path"] for row in rows] == ["s3://cube/entity/811ffffffffffff.tif", "s3://cube/entity/812ffffffffffff.tif"]
    assert [row["source_asset_path"] for row in rows] == [source_path, source_path]
    assert all("local_asset_path" not in row for row in rows)
    assert fake_ray.kill_calls == 1
    assert fake_ray.shutdown_calls == 1


def test_entity_ray_prepare_sources_only_actor_assigned_assets(monkeypatch, tmp_path: Path):
    fake_ray = _FakeRay()
    prepared: list[list[str]] = []

    monkeypatch.setattr(entity_partition_job, "_load_ray", lambda: fake_ray)
    monkeypatch.setattr(entity_partition_job, "_ray_runtime_env_from_env", lambda: {"env_vars": {}})
    monkeypatch.setattr(entity_partition_job, "_prepend_sys_paths", lambda paths: None)
    monkeypatch.setattr(entity_partition_job, "_ray_project_roots", lambda: [str(tmp_path)])
    monkeypatch.setattr(entity_partition_job, "_ray_actor_options_from_env", lambda: {})
    monkeypatch.setattr(entity_partition_job, "_upload_entity_tiles_to_minio", lambda rows, args: {row["asset_path"]: row["asset_path"] for row in rows})

    def fake_resolve(source_uri, options=None):
        prepared.append([source_uri])
        return f"/cache/{Path(source_uri).name}"

    def fake_writer(tasks, run_dir, time_granularity, partition_prefix_len, data_type="optical", source_options=None):
        _ = run_dir, time_granularity, partition_prefix_len, data_type, source_options
        return []

    monkeypatch.setattr("cube_split.jobs.ray_partition_core.resolve_asset_source_path", fake_resolve)
    monkeypatch.setattr(entity_partition_job, "_write_entity_tiles", fake_writer)

    def task(source_path: str, space_code: str):
        return {
            "scene_id": Path(source_path).stem,
            "band": "b1",
            "asset_path": source_path,
            "acq_time": "2026-04-21T00:00:00Z",
            "grid_type": "isea4h",
            "grid_level": 1,
            "space_code": space_code,
            "cover_mode": "intersect",
            "cell_min_lon": 0.0,
            "cell_min_lat": 0.0,
            "cell_max_lon": 1.0,
            "cell_max_lat": 1.0,
        }

    entity_partition_job._write_entity_tile_chunks_ray(
        task_chunks=[
            [[task("s3://cube/source/a.tif", "811ffffffffffff")]],
            [[task("s3://cube/source/b.tif", "812ffffffffffff")]],
        ],
        run_dir=tmp_path / "run",
        time_granularity="day",
        partition_prefix_len=3,
        parallelism=2,
        ray_address="10.3.100.182:6379",
        data_type="product",
        source_options={"endpoint": "10.3.100.179:9000"},
        tile_upload_options=None,
    )

    assert sorted(prepared) == [["s3://cube/source/a.tif"], ["s3://cube/source/b.tif"]]
    assert fake_ray.kill_calls == 2
    assert fake_ray.shutdown_calls == 1


def test_entity_actor_assignment_keeps_asset_affinity_when_parallelism_allows():
    def group(asset_path: str, idx: int):
        return [{"asset_path": asset_path, "space_code": f"81{idx:02x}fffffffffff"}]

    task_chunks = [[[group(f"s3://cube/source/asset_{asset_idx}.tif", group_idx)[0]] for group_idx in range(8)] for asset_idx in range(5)]

    assigned = entity_partition_job._assign_entity_task_groups_to_actors(task_chunks, parallelism=16)

    assert len(assigned) == 16
    for actor_groups in assigned:
        actor_assets = {group[0]["asset_path"] for group in actor_groups}
        assert len(actor_assets) == 1
    assert sorted({group[0]["asset_path"] for actor_groups in assigned for group in actor_groups}) == [
        f"s3://cube/source/asset_{idx}.tif" for idx in range(5)
    ]


def test_entity_ray_initial_cancellation_cleans_up_actors(monkeypatch, tmp_path: Path):
    fake_ray = _FakeRay()

    monkeypatch.setattr(entity_partition_job, "_load_ray", lambda: fake_ray)
    monkeypatch.setattr(entity_partition_job, "_ray_runtime_env_from_env", lambda: {"env_vars": {}})
    monkeypatch.setattr(entity_partition_job, "_prepend_sys_paths", lambda paths: None)
    monkeypatch.setattr(entity_partition_job, "_ray_project_roots", lambda: [str(tmp_path)])
    monkeypatch.setattr(entity_partition_job, "_ray_actor_options_from_env", lambda: {})

    task = {
        "scene_id": "scene-a",
        "band": "b04",
        "asset_path": "/source/scene.tif",
        "acq_time": "2026-04-21T00:00:00Z",
        "grid_type": "isea4h",
        "grid_level": 1,
        "space_code": "811ffffffffffff",
        "cover_mode": "intersect",
        "cell_min_lon": 0.0,
        "cell_min_lat": 0.0,
        "cell_max_lon": 1.0,
        "cell_max_lat": 1.0,
    }

    def cancelled():
        return True

    with pytest.raises(entity_partition_job.PartitionCancelledError):
        entity_partition_job._write_entity_tile_chunks_ray(
            task_chunks=[[[task]]],
            run_dir=tmp_path / "run",
            time_granularity="day",
            partition_prefix_len=3,
            parallelism=2,
            ray_address="10.3.100.182:6379",
            data_type="optical",
            source_options=None,
            tile_upload_options=None,
            cancellation_check=cancelled,
        )

    assert fake_ray.kill_calls == 1
    assert fake_ray.shutdown_calls == 1
