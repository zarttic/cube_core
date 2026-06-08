from __future__ import annotations

from argparse import Namespace
from pathlib import Path

from cube_split.jobs.ray_logical_partition_job import (
    _chunk_tasks_for_ray,
    _resolve_ray_chunk_size,
    _resolve_ray_parallelism,
    _should_run_ingest,
    run_logical_partition,
)
from cube_split.jobs.ray_partition_core import _prepare_task_rows_for_partitioning


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
