import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import rasterio
from rasterio.transform import from_origin

import cube_split.jobs.entity_partition_job as entity_partition_job
from cube_split.jobs.entity_partition_job import run_entity_partition


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

    assert report["partition_type"] == "entity"
    assert report["grid_type"] == "isea4h"
    assert report["requested_grid_level"] is None
    assert report["grid_level"] == report["inferred_grid_level"]
    assert report["entity_tile_count"] == len(rows) >= 1
    assert (run_dir / "index_rows.jsonl").exists()

    row = rows[0]
    tile_path = Path(row["output_path"])
    assert row["partition_type"] == "entity"
    assert row["asset_path"] == str(tile_path)
    assert row["source_asset_path"] == str(source.resolve())
    assert row["space_code"]
    assert row["st_code"].startswith("hx:")
    assert tile_path.exists()
    with rasterio.open(tile_path) as ds:
        assert ds.count == 1
        assert ds.nodata == 0
        assert ds.width > 0
        assert ds.height > 0


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
            minio_endpoint="10.136.1.14:9000",
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


def test_entity_partition_dispatches_ray_backend(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    _write_tif(input_dir / "demo_scene_nir.tif")
    captured: dict[str, object] = {}

    def fake_ray_writer(task_chunks, run_dir, time_granularity, partition_prefix_len, parallelism, ray_address):
        captured["task_chunk_count"] = len(task_chunks)
        captured["parallelism"] = parallelism
        captured["ray_address"] = ray_address
        return (
            [
                {
                    "partition_type": "entity",
                    "scene_id": "demo_scene_nir",
                    "band": "demo_scene_nir",
                    "asset_path": str((run_dir / "fake.tif").resolve()),
                    "source_asset_path": str((input_dir / "demo_scene_nir.tif").resolve()),
                    "output_path": str((run_dir / "fake.tif").resolve()),
                    "acq_time": "1970-01-01T00:00:00Z",
                    "grid_type": "isea4h",
                    "grid_level": 4,
                    "space_code": "842a107ffffffff",
                    "space_code_prefix": "842",
                    "st_code": "hx:4:842a107ffffffff:19700101:v1",
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
            ray_address="ray://10.136.1.13:10001",
            ray_parallelism=4,
            chunk_size=1,
            asset_storage_backend="local",
            metadata_backend="none",
        )
    )

    assert report["execution_engine"] == "ray"
    assert report["partition_backend_used"] == "ray"
    assert report["ray_parallelism"] == 1
    assert report["ray_address"] == "ray://10.136.1.13:10001"
    assert report["ray_init_elapsed_sec"] == 0.25
    assert captured["parallelism"] == 1
    assert captured["ray_address"] == "ray://10.136.1.13:10001"
