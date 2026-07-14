from __future__ import annotations

import json
import sqlite3
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

import cube_split.ingest.product_ingest_job as product_ingest_job
from cube_split.ingest.product_ingest_job import run_product_ingest
from cube_split.jobs.product_partition_job import (
    _partition_groups_ray,
    _prepare_product_task_rows,
    _process_group_chunk,
    parse_args,
    run_product_partition,
)
from cube_split.partition.product_products import parse_product_asset
from cube_split.quality.product_quality import run_quality_check


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


def _write_product_tif(path: Path, *, crs: str = "EPSG:4326") -> None:
    data = np.arange(100, dtype=np.float32).reshape(1, 10, 10)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=10,
        height=10,
        count=1,
        dtype=data.dtype,
        crs=crs,
        transform=from_origin(100.0, 25.0, 0.01, 0.01),
        nodata=-9999.0,
    ) as ds:
        ds.write(data)


def _stub_product_source_asset_upload(monkeypatch) -> None:
    monkeypatch.setattr(
        "cube_split.jobs.product_partition_job.upload_source_assets_to_minio",
        lambda assets, *, prefix, options=None: list(assets),
    )


def test_product_parse_args_allows_mgrs_grid_type(monkeypatch):
    monkeypatch.setattr("sys.argv", ["product_partition_job.py", "--grid-type", "mgrs"])
    args = parse_args()
    assert args.grid_type == "mgrs"


def test_product_parse_args_defaults_to_geohash_and_keeps_source_crs_by_default(monkeypatch):
    monkeypatch.setattr("sys.argv", ["product_partition_job.py"])
    args = parse_args()
    assert args.grid_type == "geohash"
    assert args.target_crs == ""
    assert args.max_cells_per_asset == 0


def test_product_partition_rejects_entity_grid_from_direct_namespace(tmp_path: Path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    with pytest.raises(ValueError, match="geohash, mgrs"):
        run_product_partition(Namespace(input_dir=str(input_dir), output_dir=str(tmp_path / "output"), grid_type="isea4h"))


def _product_row(asset_path: Path, year: int = 1980) -> dict:
    return {
        "scene_id": f"dianzhong_ecological_security_{year}",
        "band": "product_value",
        "asset_path": str(asset_path),
        "acq_time": f"{year}-01-01T00:00:00Z",
        "grid_type": "geohash",
        "grid_level": 5,
        "space_code": "372c",
        "space_code_prefix": "372",
        "st_code": f"gh:5:372c:{year}",
        "time_bucket": str(year),
        "cover_mode": "intersect",
        "cell_min_lon": 100.0,
        "cell_min_lat": 24.9,
        "cell_max_lon": 100.1,
        "cell_max_lat": 25.0,
        "window_col_off": 0,
        "window_row_off": 0,
        "window_width": 8,
        "window_height": 8,
        "intersect_min_lon": 100.0,
        "intersect_min_lat": 24.9,
        "intersect_max_lon": 100.1,
        "intersect_max_lat": 25.0,
        "sample_mean_band1": 10.0,
    }


def test_parse_product_asset_extracts_year_and_product_identity(tmp_path: Path):
    tif = tmp_path / "1980-2020年滇中地区30米生态安全评价数据集（第一版）_2010年.tif"
    tif.write_bytes(b"fake")

    record = parse_product_asset(tif)

    assert record.scene_id == "dianzhong_ecological_security_2010"
    assert record.band == "product_value"
    assert record.product_family == "product"
    assert record.sensor == "data_product"
    assert record.acq_time.isoformat().startswith("2010-01-01T00:00:00")


def test_prepare_product_task_rows_keeps_year_bucket_and_day_st_code_input():
    rows = _prepare_product_task_rows(
        [
            {
                "space_code": "372c",
                "acq_time": "2010-01-01T00:00:00Z",
            }
        ],
        partition_prefix_len=3,
    )

    assert rows[0]["space_code_prefix"] == "372"
    assert rows[0]["time_bucket"] == "2010"
    assert rows[0]["st_time_granularity"] == "day"


def test_process_group_chunk_batches_rows_for_one_dataset_open(monkeypatch):
    captured: dict[str, object] = {}

    def fake_process_local_task_group(rows, time_granularity, include_sample_mean=False):
        captured["rows"] = rows
        captured["time_granularity"] = time_granularity
        captured["include_sample_mean"] = include_sample_mean
        return list(rows)

    monkeypatch.setattr("cube_split.jobs.product_partition_job._process_local_task_group", fake_process_local_task_group)

    rows = _process_group_chunk(
        [
            [{"asset_path": "/source/product_2020.tif", "space_code": "35f4"}],
            [{"asset_path": "/source/product_2020.tif", "space_code": "35f5"}],
        ],
        include_sample_mean=True,
    )

    assert [row["space_code"] for row in rows] == ["35f4", "35f5"]
    assert captured["rows"] == rows
    assert captured["time_granularity"] == "day"
    assert captured["include_sample_mean"] is True


def test_product_partition_disables_cog_predictor_for_64bit_product_assets(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    cog_dir = tmp_path / "cog"
    input_dir.mkdir()
    tif = input_dir / "1980-2020年滇中地区30米生态安全评价数据集（第一版）_1980年.tif"
    _write_product_tif(tif)
    captured = {}

    def fake_convert_assets_to_cog(assets, **kwargs):
        captured["predictor"] = kwargs["predictor"]
        return assets

    def fake_build_grid_tasks_driver(**kwargs):
        return [
            {
                "scene_id": "dianzhong_ecological_security_1980",
                "band": "product_value",
                "asset_path": str(tif),
                "acq_time": "1980-01-01T00:00:00Z",
                "grid_type": "geohash",
                "grid_level": 5,
                "space_code": "372c",
                "st_code": "gh:5:372c:19800101",
                "cell_min_lon": 100.0,
                "cell_min_lat": 24.9,
                "cell_max_lon": 100.1,
                "cell_max_lat": 25.0,
                "window_col_off": 0,
                "window_row_off": 0,
                "window_width": 8,
                "window_height": 8,
            }
        ]

    def fake_process_local_task_group(group, time_granularity, include_sample_mean=False):
        return group

    monkeypatch.setattr("cube_split.jobs.product_partition_job.convert_assets_to_cog", fake_convert_assets_to_cog)
    monkeypatch.setattr("cube_split.jobs.product_partition_job.build_grid_tasks_driver", fake_build_grid_tasks_driver)
    monkeypatch.setattr("cube_split.jobs.product_partition_job._process_local_task_group", fake_process_local_task_group)

    result = run_product_partition(
        Namespace(
            input_dir=str(input_dir),
            output_dir=str(output_dir),
            cog_input_dir=str(cog_dir),
            target_crs="EPSG:4326",
            grid_type="geohash",
            grid_level=5,
            cover_mode="intersect",
            max_cells_per_asset=20000,
            partition_prefix_len=3,
            cog_overwrite=True,
            cog_workers=1,
            partition_workers=1,
            partition_backend="thread",
            ray_address="",
            ray_parallelism=0,
            chunk_size=0,
            sample_mean=False,
        )
    )

    assert result["status"] == "completed"
    assert captured["predictor"] == 0


def test_product_partition_dispatches_ray_backend(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    cog_dir = tmp_path / "cog"
    input_dir.mkdir()
    tif = input_dir / "1980-2020年滇中地区30米生态安全评价数据集（第一版）_1980年.tif"
    _write_product_tif(tif)
    captured = {}

    def fake_convert_assets_to_cog(assets, **kwargs):
        return assets

    def fake_build_grid_tasks_driver(**kwargs):
        return [_product_row(tif, 1980)]

    def fake_partition_groups_ray(
        task_chunks,
        parallelism,
        ray_address,
        include_sample_mean,
        assets_by_path,
        cog_input_dir,
        cog_overwrite,
        cog_options,
        target_crs,
        source_options,
        cog_upload_options,
    ):
        captured["task_chunk_count"] = len(task_chunks)
        captured["parallelism"] = parallelism
        captured["ray_address"] = ray_address
        captured["include_sample_mean"] = include_sample_mean
        captured["assets_by_path"] = assets_by_path
        captured["cog_input_dir"] = cog_input_dir
        captured["cog_overwrite"] = cog_overwrite
        captured["cog_options"] = cog_options
        captured["target_crs"] = target_crs
        captured["source_options"] = source_options
        captured["cog_upload_options"] = cog_upload_options
        return [_product_row(tif, 1980)], 0.25

    monkeypatch.setattr("cube_split.jobs.product_partition_job.convert_assets_to_cog", fake_convert_assets_to_cog)
    monkeypatch.setattr("cube_split.jobs.product_partition_job.build_grid_tasks_driver", fake_build_grid_tasks_driver)
    monkeypatch.setattr("cube_split.jobs.product_partition_job._partition_groups_ray", fake_partition_groups_ray)
    _stub_product_source_asset_upload(monkeypatch)

    result = run_product_partition(
        Namespace(
            input_dir=str(input_dir),
            manifest_path="",
            output_dir=str(output_dir),
            cog_input_dir=str(cog_dir),
            target_crs="EPSG:4326",
            grid_type="geohash",
            grid_level=5,
            cover_mode="intersect",
            max_cells_per_asset=20000,
            partition_prefix_len=3,
            cog_overwrite=True,
            cog_workers=1,
            partition_workers=1,
            partition_backend="ray",
            ray_address="10.3.100.182:6379",
            ray_parallelism=4,
            chunk_size=1,
            sample_mean=True,
            metadata_backend="none",
            asset_storage_backend="local",
            minio_endpoint="10.3.100.179:9000",
            minio_access_key="access",
            minio_secret_key="secret",
            minio_bucket="cube",
            minio_prefix="cube/product",
            minio_secure=False,
            dataset="dianzhong_ecological_security",
            asset_version="v1",
        )
    )

    assert result["execution_engine"] == "ray"
    assert result["partition_backend_used"] == "ray"
    assert result["ray_parallelism"] == 1
    assert result["ray_address"] == "10.3.100.182:6379"
    assert result["ray_init_elapsed_sec"] == 0.25
    assert result["asset_storage_backend"] == "minio"
    assert result["minio_bucket"] == "cube"
    assert result["minio_prefix"] == "cube/product"
    assert captured["parallelism"] == 1
    assert captured["include_sample_mean"] is True
    assert str(tif.resolve()) in captured["assets_by_path"]
    assert captured["cog_input_dir"] == str(cog_dir)
    assert captured["cog_overwrite"] is True
    assert captured["cog_options"]["PREDICTOR"] == "2"
    assert captured["target_crs"] == "EPSG:4326"
    assert captured["source_options"]["endpoint"] == "10.3.100.179:9000"
    assert captured["source_options"]["access_key"] == "access"
    assert captured["cog_upload_options"]["bucket"] == "cube"
    assert captured["cog_upload_options"]["prefix"] == "cube/product"
    assert captured["cog_upload_options"]["dataset"] == "dianzhong_ecological_security"


def test_product_partition_ray_worker_uses_local_cog_before_upload(monkeypatch, tmp_path: Path):
    fake_ray = _FakeRay()
    local_cog_path = tmp_path / "worker" / "product_cog.tif"
    calls: list[tuple[str, object]] = []

    monkeypatch.setattr("cube_split.jobs.product_partition_job._load_ray", lambda: fake_ray)
    monkeypatch.setattr("cube_split.jobs.product_partition_job._ray_runtime_env_from_env", lambda: {"env_vars": {}})
    monkeypatch.setattr("cube_split.jobs.product_partition_job._prepend_sys_paths", lambda paths: None)
    monkeypatch.setattr("cube_split.jobs.product_partition_job._ray_project_roots", lambda: [str(tmp_path)])
    monkeypatch.setattr("cube_split.jobs.product_partition_job._ray_actor_options_from_env", lambda: {})
    monkeypatch.setattr(
        "cube_split.jobs.ray_partition_core.asset_record_from_dict",
        lambda row: SimpleNamespace(**row),
    )

    def fake_convert_asset_to_cog(asset, **kwargs):
        local_cog_path.parent.mkdir(parents=True, exist_ok=True)
        local_cog_path.write_bytes(b"cog")
        timing = kwargs.get("timing")
        if timing is not None:
            timing["source_resolve_elapsed_sec"] = timing.get("source_resolve_elapsed_sec", 0.0) + 1.25
            timing["cog_write_elapsed_sec"] = timing.get("cog_write_elapsed_sec", 0.0) + 2.5
            timing["cog_write_count"] = timing.get("cog_write_count", 0.0) + 1.0
        calls.append(("convert", asset.path))
        return SimpleNamespace(
            scene_id=asset.scene_id,
            band=asset.band,
            path=local_cog_path,
            acq_time=asset.acq_time,
            product_family=asset.product_family,
            sensor=asset.sensor,
            bbox=asset.bbox,
            corners=asset.corners,
            resolution=asset.resolution,
        )

    def fake_process_group_chunk(chunk, include_sample_mean):
        calls.append(("process", [group[0]["asset_path"] for group in chunk]))
        return [{"scene_id": group[0]["scene_id"], "asset_path": group[0]["asset_path"]} for group in chunk]

    def fake_upload_cog_to_minio(asset, local_path, options):
        calls.append(("upload", str(local_path)))
        return f"s3://cube/cube/product/{Path(local_path).name}"

    monkeypatch.setattr("cube_split.jobs.ray_partition_core.convert_asset_to_cog", fake_convert_asset_to_cog)
    monkeypatch.setattr("cube_split.jobs.product_partition_job._process_group_chunk", fake_process_group_chunk)
    monkeypatch.setattr("cube_split.jobs.ray_partition_core.upload_cog_to_minio", fake_upload_cog_to_minio)

    rows, ray_init_elapsed, stats = _partition_groups_ray(
        task_chunks=[
            [
                [{"scene_id": "product-2020", "asset_path": "/source/product_2020.tif"}],
                [{"scene_id": "product-2020-b", "asset_path": "/source/product_2020.tif"}],
            ]
        ],
        parallelism=1,
        ray_address="10.3.100.182:6379",
        include_sample_mean=False,
        assets_by_path={
            "/source/product_2020.tif": {
                "scene_id": "product-2020",
                "band": "product_value",
                "path": "/source/product_2020.tif",
                "acq_time": "2020-01-01T00:00:00Z",
                "product_family": "product",
                "sensor": "data_product",
                "bbox": None,
                "corners": [[100.0, 25.0], [100.1, 25.0], [100.1, 24.9], [100.0, 24.9]],
                "resolution": 30,
            }
        },
        cog_input_dir=str(tmp_path / "cog"),
        cog_overwrite=True,
        cog_options={"COMPRESS": "LZW"},
        target_crs="EPSG:4326",
        source_options={"endpoint": "10.3.100.179:9000"},
        cog_upload_options={"bucket": "cube", "prefix": "cube/product"},
    )

    assert ray_init_elapsed >= 0
    assert [name for name, _ in calls] == ["convert", "process", "upload"]
    assert calls[1][1] == [str(local_cog_path), str(local_cog_path)]
    assert calls[2][1] == str(local_cog_path)
    assert [row["asset_path"] for row in rows] == [
        "s3://cube/cube/product/product_cog.tif",
        "s3://cube/cube/product/product_cog.tif",
    ]
    assert stats["source_resolve_elapsed_sec"] == 1.25
    assert stats["cog_write_elapsed_sec"] == 2.5
    assert stats["cog_write_count"] == 1.0
    assert stats["cog_cache_hit_count"] == 1.0
    assert stats["cog_upload_count"] == 1.0
    assert stats["partition_rows_elapsed_sec"] >= 0
    assert fake_ray.shutdown_calls == 1


def test_product_partition_ray_actor_reuses_local_cog_across_chunks(monkeypatch, tmp_path: Path):
    fake_ray = _FakeRay()
    local_cog_path = tmp_path / "worker" / "product_cog.tif"
    calls: list[tuple[str, object]] = []

    monkeypatch.setattr("cube_split.jobs.product_partition_job._load_ray", lambda: fake_ray)
    monkeypatch.setattr("cube_split.jobs.product_partition_job._ray_runtime_env_from_env", lambda: {"env_vars": {}})
    monkeypatch.setattr("cube_split.jobs.product_partition_job._prepend_sys_paths", lambda paths: None)
    monkeypatch.setattr("cube_split.jobs.product_partition_job._ray_project_roots", lambda: [str(tmp_path)])
    monkeypatch.setattr("cube_split.jobs.product_partition_job._ray_actor_options_from_env", lambda: {})
    monkeypatch.setattr("cube_split.jobs.ray_partition_core.asset_record_from_dict", lambda row: SimpleNamespace(**row))

    def fake_convert_asset_to_cog(asset, **kwargs):
        local_cog_path.parent.mkdir(parents=True, exist_ok=True)
        local_cog_path.write_bytes(b"cog")
        calls.append(("convert", asset.path))
        return SimpleNamespace(
            scene_id=asset.scene_id,
            band=asset.band,
            path=local_cog_path,
            acq_time=asset.acq_time,
            product_family=asset.product_family,
            sensor=asset.sensor,
            bbox=asset.bbox,
            corners=asset.corners,
            resolution=asset.resolution,
        )

    def fake_process_group_chunk(chunk, include_sample_mean):
        calls.append(("process", [group[0]["asset_path"] for group in chunk]))
        return [{"scene_id": group[0]["scene_id"], "asset_path": group[0]["asset_path"]} for group in chunk]

    def fake_upload_cog_to_minio(asset, local_path, options):
        calls.append(("upload", str(local_path)))
        return f"s3://cube/cube/product/{Path(local_path).name}"

    monkeypatch.setattr("cube_split.jobs.ray_partition_core.convert_asset_to_cog", fake_convert_asset_to_cog)
    monkeypatch.setattr("cube_split.jobs.product_partition_job._process_group_chunk", fake_process_group_chunk)
    monkeypatch.setattr("cube_split.jobs.ray_partition_core.upload_cog_to_minio", fake_upload_cog_to_minio)

    rows, _, stats = _partition_groups_ray(
        task_chunks=[
            [[{"scene_id": "product-2020", "asset_path": "/source/product_2020.tif"}]],
            [[{"scene_id": "product-2020-b", "asset_path": "/source/product_2020.tif"}]],
        ],
        parallelism=2,
        ray_address="10.3.100.182:6379",
        include_sample_mean=False,
        assets_by_path={
            "/source/product_2020.tif": {
                "scene_id": "product-2020",
                "band": "product_value",
                "path": "/source/product_2020.tif",
                "acq_time": "2020-01-01T00:00:00Z",
                "product_family": "product",
                "sensor": "data_product",
                "bbox": None,
                "corners": [[100.0, 25.0], [100.1, 25.0], [100.1, 24.9], [100.0, 24.9]],
                "resolution": 30,
            }
        },
        cog_input_dir=str(tmp_path / "cog"),
        cog_overwrite=True,
        cog_options={"COMPRESS": "LZW"},
        target_crs="EPSG:4326",
        source_options={"endpoint": "10.3.100.179:9000"},
        cog_upload_options={"bucket": "cube", "prefix": "cube/product"},
    )

    assert [name for name, _ in calls] == ["convert", "process", "upload", "process"]
    assert calls.count(("convert", "/source/product_2020.tif")) == 1
    assert calls.count(("upload", str(local_cog_path))) == 1
    assert stats["cog_cache_hit_count"] == 1.0
    assert stats["cog_upload_count"] == 1.0
    assert [row["asset_path"] for row in rows] == [
        "s3://cube/cube/product/product_cog.tif",
        "s3://cube/cube/product/product_cog.tif",
    ]


def test_product_partition_runs_ingest_after_rows_are_written(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    cog_dir = tmp_path / "cog"
    input_dir.mkdir()
    tif = input_dir / "1980-2020年滇中地区30米生态安全评价数据集（第一版）_1980年.tif"
    _write_product_tif(tif)
    captured = {}

    def fake_convert_assets_to_cog(assets, **kwargs):
        return assets

    def fake_build_grid_tasks_driver(**kwargs):
        return [_product_row(tif, 1980)]

    def fake_process_local_task_group(group, time_granularity, include_sample_mean=False):
        return group

    def fake_run_product_ingest(args):
        captured["run_dir"] = args.run_dir
        captured["job_id"] = args.job_id
        captured["metadata_backend"] = args.metadata_backend
        captured["asset_storage_backend"] = args.asset_storage_backend
        captured["minio_bucket"] = args.minio_bucket
        assert (Path(args.run_dir) / "index_rows.jsonl").exists()
        return {
            "input_rows": 1,
            "product_asset_rows": 1,
            "product_fact_rows": 1,
            "metadata_backend": args.metadata_backend,
            "asset_storage_backend": args.asset_storage_backend,
        }

    monkeypatch.setattr("cube_split.jobs.product_partition_job.convert_assets_to_cog", fake_convert_assets_to_cog)
    monkeypatch.setattr("cube_split.jobs.product_partition_job.build_grid_tasks_driver", fake_build_grid_tasks_driver)
    monkeypatch.setattr("cube_split.jobs.product_partition_job._process_local_task_group", fake_process_local_task_group)
    monkeypatch.setattr("cube_split.ingest.product_ingest_job.run_product_ingest", fake_run_product_ingest)

    result = run_product_partition(
        Namespace(
            input_dir=str(input_dir),
            manifest_path="",
            output_dir=str(output_dir),
            cog_input_dir=str(cog_dir),
            target_crs="EPSG:4326",
            grid_type="geohash",
            grid_level=5,
            cover_mode="intersect",
            max_cells_per_asset=20000,
            partition_prefix_len=3,
            cog_overwrite=True,
            cog_workers=1,
            partition_workers=1,
            partition_backend="thread",
            ray_address="",
            ray_parallelism=0,
            chunk_size=0,
            sample_mean=False,
            job_id="",
            dataset="dianzhong_ecological_security",
            product_name="滇中地区30米生态安全评价数据集",
            asset_version="v1",
            cube_version="product_v1",
            metadata_backend="postgres",
            postgres_dsn="postgresql://postgres:postgres@127.0.0.1:5432/cube",
            db_path="",
            asset_storage_backend="minio",
            minio_endpoint="10.3.100.179:9000",
            minio_access_key="access",
            minio_secret_key="secret",
            minio_bucket="cube",
            minio_prefix="cube/product",
            minio_secure=False,
            minio_upload_workers=2,
            cog_output_root=str(tmp_path / "product_cog_store"),
            cog_materialize_mode="copy",
        )
    )

    assert result["ingest_enabled"] is True
    assert result["ingest_stats"]["input_rows"] == 1
    assert captured["job_id"] == Path(captured["run_dir"]).name
    assert captured["asset_storage_backend"] == "minio"
    assert captured["minio_bucket"] == "cube"


def test_product_partition_ray_forces_minio_ingest_contract(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    cog_dir = tmp_path / "cog"
    input_dir.mkdir()
    tif = input_dir / "1980-2020年滇中地区30米生态安全评价数据集（第一版）_1980年.tif"
    _write_product_tif(tif)
    captured: dict[str, object] = {}

    monkeypatch.setattr("cube_split.jobs.product_partition_job.build_grid_tasks_driver", lambda **kwargs: [_product_row(tif, 1980)])
    monkeypatch.setattr(
        "cube_split.jobs.product_partition_job._partition_groups_ray",
        lambda **kwargs: ([
            {**_product_row(tif, 1980), "asset_path": "s3://cube/cube/product/demo_scene.tif"}
        ], 0.1),
    )
    _stub_product_source_asset_upload(monkeypatch)

    def fake_run_product_ingest(args):
        captured["asset_storage_backend"] = args.asset_storage_backend
        captured["minio_bucket"] = args.minio_bucket
        captured["run_dir"] = args.run_dir
        return {"input_rows": 1, "product_asset_rows": 1, "product_fact_rows": 1}

    monkeypatch.setattr("cube_split.ingest.product_ingest_job.run_product_ingest", fake_run_product_ingest)

    result = run_product_partition(
        Namespace(
            input_dir=str(input_dir),
            manifest_path="",
            output_dir=str(output_dir),
            cog_input_dir=str(cog_dir),
            target_crs="EPSG:4326",
            grid_type="geohash",
            grid_level=5,
            cover_mode="intersect",
            max_cells_per_asset=20000,
            partition_prefix_len=3,
            cog_overwrite=True,
            cog_workers=1,
            partition_workers=1,
            partition_backend="ray",
            ray_address="10.3.100.182:6379",
            ray_parallelism=1,
            chunk_size=1,
            sample_mean=False,
            job_id="",
            dataset="dianzhong_ecological_security",
            product_name="滇中地区30米生态安全评价数据集",
            asset_version="v1",
            cube_version="product_v1",
            metadata_backend="sqlite",
            postgres_dsn="",
            db_path=str(tmp_path / "product.db"),
            asset_storage_backend="local",
            minio_endpoint="10.3.100.179:9000",
            minio_access_key="access",
            minio_secret_key="secret",
            minio_bucket="cube",
            minio_prefix="cube/product",
            minio_secure=False,
            minio_upload_workers=1,
            cog_output_root=str(tmp_path / "product_cog_store"),
            cog_materialize_mode="copy",
        )
    )

    assert result["asset_storage_backend"] == "minio"
    assert captured["asset_storage_backend"] == "minio"
    assert captured["minio_bucket"] == "cube"


def test_product_partition_fills_product_name_from_source_metadata(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    cog_dir = tmp_path / "cog"
    input_dir.mkdir()
    tif = input_dir / "1980-2020年滇中地区30米生态安全评价数据集（第一版）_1980年.tif"
    _write_product_tif(tif)
    captured: dict[str, object] = {}

    monkeypatch.setattr("cube_split.jobs.product_partition_job.build_grid_tasks_driver", lambda **kwargs: [_product_row(tif, 1980)])
    monkeypatch.setattr(
        "cube_split.jobs.product_partition_job._partition_groups_ray",
        lambda **kwargs: ([
            {**_product_row(tif, 1980), "asset_path": "s3://cube/cube/product/demo_scene.tif"}
        ], 0.1),
    )
    _stub_product_source_asset_upload(monkeypatch)

    def fake_run_product_ingest(args):
        captured["product_name"] = args.product_name
        return {"input_rows": 1, "product_asset_rows": 1, "product_fact_rows": 1}

    monkeypatch.setattr("cube_split.ingest.product_ingest_job.run_product_ingest", fake_run_product_ingest)

    run_product_partition(
        Namespace(
            input_dir=str(input_dir),
            manifest_path="",
            output_dir=str(output_dir),
            cog_input_dir=str(cog_dir),
            target_crs="EPSG:4326",
            grid_type="geohash",
            grid_level=5,
            cover_mode="intersect",
            max_cells_per_asset=20000,
            partition_prefix_len=3,
            cog_overwrite=True,
            cog_workers=1,
            partition_workers=1,
            partition_backend="ray",
            ray_address="10.3.100.182:6379",
            ray_parallelism=1,
            chunk_size=1,
            sample_mean=False,
            job_id="",
            dataset="dianzhong_ecological_security",
            product_name="",
            asset_version="v1",
            cube_version="product_v1",
            metadata_backend="sqlite",
            postgres_dsn="",
            db_path=str(tmp_path / "product.db"),
            asset_storage_backend="local",
            minio_endpoint="10.3.100.179:9000",
            minio_access_key="access",
            minio_secret_key="secret",
            minio_bucket="cube",
            minio_prefix="cube/product",
            minio_secure=False,
            minio_upload_workers=1,
            cog_output_root=str(tmp_path / "product_cog_store"),
            cog_materialize_mode="copy",
        )
    )

    assert captured["product_name"] == "1980-2020年滇中地区30米生态安全评价数据集（第一版）"


def test_product_quality_passes_complete_years(tmp_path: Path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    rows = []
    for year in (1980, 1990, 2000, 2010, 2020):
        asset = tmp_path / f"product_{year}_cog.tif"
        _write_product_tif(asset)
        rows.append(_product_row(asset, year=year))
    (run_dir / "index_rows.jsonl").write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )

    report = run_quality_check(Namespace(run_dir=str(run_dir), target_crs="EPSG:4326"))

    assert report["status"] == "PASS"
    assert report["summary"]["index_rows"] == 5
    assert report["summary"]["product_years"] == [1980, 1990, 2000, 2010, 2020]
    report_path = run_dir / "quality_report.json"
    assert report["report_path"] == str(report_path.resolve())
    stored_report = json.loads(report_path.read_text(encoding="utf-8"))
    assert stored_report["data_type"] == "product"
    assert stored_report["summary"]["product_years"] == [1980, 1990, 2000, 2010, 2020]


def test_product_quality_passes_selected_single_year_by_default(tmp_path: Path):
    asset = tmp_path / "product_1980_cog.tif"
    _write_product_tif(asset)
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "index_rows.jsonl").write_text(json.dumps(_product_row(asset, 1980)) + "\n", encoding="utf-8")

    report = run_quality_check(Namespace(run_dir=str(run_dir), target_crs="EPSG:4326"))

    assert report["status"] == "PASS"
    year_check = next(check for check in report["checks"] if check["name"] == "product_years")
    assert year_check["status"] == "PASS"
    assert year_check["metrics"]["present_years"] == [1980]


def test_product_quality_warns_missing_explicit_expected_year(tmp_path: Path):
    asset = tmp_path / "product_1980_cog.tif"
    _write_product_tif(asset)
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "index_rows.jsonl").write_text(json.dumps(_product_row(asset, 1980)) + "\n", encoding="utf-8")

    report = run_quality_check(Namespace(run_dir=str(run_dir), target_crs="EPSG:4326", expected_years="1980,1990,2000,2010,2020"))

    assert report["status"] == "WARN"
    year_check = next(check for check in report["checks"] if check["name"] == "product_years")
    assert year_check["metrics"]["missing_years"] == [1990, 2000, 2010, 2020]


def test_run_product_ingest_creates_product_tables(tmp_path: Path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    asset = tmp_path / "product_1980_cog.tif"
    _write_product_tif(asset)
    (run_dir / "index_rows.jsonl").write_text(json.dumps(_product_row(asset, 1980)) + "\n", encoding="utf-8")
    db_path = tmp_path / "product_ingest.db"

    stats = run_product_ingest(
        Namespace(
            run_dir=str(run_dir),
            job_id="product-job-1",
            dataset="dianzhong_ecological_security",
            product_name="滇中地区30米生态安全评价数据集",
            asset_version="v1",
            cube_version="product_v1",
            metadata_backend="sqlite",
            db_path=str(db_path),
            asset_storage_backend="local",
            cog_output_root=str(tmp_path / "product_cog_store"),
            cog_materialize_mode="copy",
            postgres_dsn="",
            minio_endpoint="",
            minio_access_key="",
            minio_secret_key="",
            minio_bucket="",
            minio_prefix="cube/product",
            minio_secure=False,
            minio_upload_workers=1,
        )
    )

    assert stats["input_rows"] == 1
    assert stats["product_asset_rows"] == 1
    assert stats["product_fact_rows"] == 1

    conn = sqlite3.connect(str(db_path))
    try:
        assets = conn.execute("SELECT dataset, product_name, product_year FROM rs_product_asset").fetchall()
        facts = conn.execute("SELECT product_year, product_band, value_ref_uri FROM rs_product_cell_fact").fetchall()
    finally:
        conn.close()

    assert assets == [("dianzhong_ecological_security", "滇中地区30米生态安全评价数据集", 1980)]
    assert facts[0][0] == 1980
    assert facts[0][1] == "product_value"
    assert "#window=" in facts[0][2]


def test_product_postgres_upserts_batch_merge_rows() -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple]] = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            self.calls.append((sql, tuple(params)))

    class FakeConn:
        def __init__(self) -> None:
            self.cursor_obj = FakeCursor()

        def cursor(self):
            return self.cursor_obj

    asset_records = [
        product_ingest_job.ProductAssetRecord("dataset", "product", f"scene-{idx}", 1980, "2026-04-21T00:00:00Z", f"s3://cube/p{idx}.tif", "v1", "job")
        for idx in range(3)
    ]
    fact_records = [
        product_ingest_job.ProductFactRecord(
            "dataset",
            "product",
            1980,
            "product_value",
            "geohash",
            7,
            f"35f0{idx}",
            "1980",
            f"gh:7:35f0{idx}:1980",
            116.1,
            39.8,
            116.2,
            39.9,
            f"s3://cube/p{idx}.tif#window=0,0,256,256",
            1.0,
            "product_v1",
            "job",
        )
        for idx in range(3)
    ]
    asset_conn = FakeConn()
    fact_conn = FakeConn()

    product_ingest_job.upsert_product_assets_postgres(asset_conn, asset_records, batch_size=2)
    product_ingest_job.upsert_product_facts_postgres(fact_conn, fact_records, batch_size=2)

    assert len(asset_conn.cursor_obj.calls) == 2
    assert len(fact_conn.cursor_obj.calls) == 2
    assert "VALUES" in asset_conn.cursor_obj.calls[0][0]
    assert "VALUES" in fact_conn.cursor_obj.calls[0][0]
    assert len(asset_conn.cursor_obj.calls[0][1]) == 16
    assert len(asset_conn.cursor_obj.calls[1][1]) == 8
    assert len(fact_conn.cursor_obj.calls[0][1]) == 34
    assert len(fact_conn.cursor_obj.calls[1][1]) == 17


def test_run_product_ingest_reports_probe_metrics(monkeypatch, tmp_path: Path):
    run_dir = tmp_path / "run_probe"
    run_dir.mkdir()
    asset = tmp_path / "product_1980_probe.tif"
    _write_product_tif(asset)
    (run_dir / "index_rows.jsonl").write_text(json.dumps(_product_row(asset, 1980)) + "\n", encoding="utf-8")
    captured: list[product_ingest_job.TileProbeMetric] = []

    def fake_report_tile_metrics(metrics):
        captured.extend(list(metrics))

    monkeypatch.setattr(product_ingest_job, "report_tile_metrics", fake_report_tile_metrics)

    run_product_ingest(
        Namespace(
            run_dir=str(run_dir),
            job_id="product-job-probe",
            dataset="dianzhong_ecological_security",
            product_name="滇中地区30米生态安全评价数据集",
            asset_version="v1",
            cube_version="product_v1",
            metadata_backend="sqlite",
            db_path=str(tmp_path / "product_probe.db"),
            asset_storage_backend="local",
            cog_output_root=str(tmp_path / "product_cog_store"),
            cog_materialize_mode="copy",
            postgres_dsn="",
            minio_endpoint="",
            minio_access_key="",
            minio_secret_key="",
            minio_bucket="",
            minio_prefix="cube/product",
            minio_secure=False,
            minio_upload_workers=1,
        )
    )

    assert len(captured) == 1
    assert captured[0].task_name == "cube.partition.product.ingest"
    assert captured[0].method_name == "merge.rs_product_cell_fact"
    assert captured[0].attributes["cube.band"] == "product_value"
    assert captured[0].attributes["cube.product_year"] == 1980
    assert captured[0].attributes["cube.target_table"] == "rs_product_cell_fact"
