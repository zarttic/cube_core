from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from cube_split.jobs.ray_partition_core import (
    AssetRecord,
    _process_local_task_group,
    build_grid_tasks_driver,
    build_manifest,
    cache_source_cog,
    create_unique_run_dir,
)
from cube_split.partition.optical_products import get_optical_product_adapter, supported_optical_product_families


def _write_tif(path: Path) -> None:
    transform = from_origin(116.0, 40.0, 0.01, 0.01)
    data = np.ones((1, 8, 8), dtype=np.uint8)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=8,
        height=8,
        count=1,
        dtype=data.dtype,
        crs="EPSG:4326",
        transform=transform,
    ) as ds:
        ds.write(data)


def _write_projected_tif(path: Path) -> None:
    transform = from_origin(500000.0, 4100000.0, 10.0, 10.0)
    data = np.arange(64, dtype=np.uint8).reshape((1, 8, 8))
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=8,
        height=8,
        count=1,
        dtype=data.dtype,
        crs="EPSG:32650",
        transform=transform,
    ) as ds:
        ds.write(data)


def test_process_local_task_group_restores_remote_source_uri(monkeypatch):
    local_path = "/tmp/cube_split_source_cache/worker/source.tif"
    source_uri = "s3://cube/cube/source/optical/source.tif"
    monkeypatch.setattr(
        "cube_split.jobs.ray_partition_core.process_partition",
        lambda rows, _granularity, include_sample_mean=True: iter([{"asset_path": next(rows).asset_path}]),
    )

    rows = _process_local_task_group(
        [{"asset_path": local_path, "source_asset_path": source_uri}],
        "day",
    )

    assert rows == [{"asset_path": source_uri, "source_asset_path": source_uri}]


class RecordingMinio:
    def __init__(self) -> None:
        self.downloads: list[tuple[str, str, str]] = []
        self.uploads: list[tuple[object, ...]] = []

    def fget_object(self, bucket: str, key: str, target: str) -> None:
        self.downloads.append((bucket, key, target))
        Path(target).write_bytes(b"loader-owned-cog")

    def fput_object(self, *args: object) -> None:
        self.uploads.append(args)


def test_cache_source_cog_downloads_atomically_without_source_upload(tmp_path: Path) -> None:
    client = RecordingMinio()

    cached = cache_source_cog("s3://cube/loader/dataset/a.tif", tmp_path, client, "cube")

    assert cached.read_bytes() == b"loader-owned-cog"
    assert client.downloads == [("cube", "loader/dataset/a.tif", str(cached.with_suffix(".tif.part")))]
    assert client.uploads == []
    assert cached.suffix == ".tif"
    assert len(cached.parent.name) == 64


def test_build_manifest_supports_landsat_collection_filenames(tmp_path: Path):
    source = tmp_path / "LC09_L2SP_123033_20240424_20240425_02_T1_SR_B4.TIF"
    _write_tif(source)

    records = build_manifest(tmp_path)

    assert len(records) == 1
    assert records[0].scene_id == "LC09_L2SP_123033_20240424_20240425_02_T1"
    assert records[0].band == "sr_b4"
    assert records[0].acq_time == "2024-04-24T00:00:00Z"
    assert records[0].product_family == "landsat"
    assert records[0].sensor == "landsat9_oli_tirs"


def test_build_grid_tasks_driver_uses_sdk_geohash_cells_with_asset_bbox():
    tasks = build_grid_tasks_driver(
        [
            AssetRecord(
                scene_id="scene-a",
                band="b1",
                path="s3://cube/loader/scene-a.tif",
                acq_time="2026-03-09T00:00:00Z",
                bbox=[116.38, 39.90, 116.40, 39.91],
            )
        ],
        grid_type="geohash",
        grid_level=5,
        cover_mode="intersect",
        max_cells_per_asset=20000,
    )

    assert tasks
    assert all(task["grid_level"] == 5 and task["topology_code"] is None for task in tasks)
    assert all(task["cell_geom"]["type"] == "Polygon" for task in tasks)
    assert all(task["cell_geom"]["coordinates"][0][0] == task["cell_geom"]["coordinates"][0][-1] for task in tasks)


def test_build_grid_tasks_uses_standard_mgrs_identity():
    tasks = build_grid_tasks_driver(
        [
            AssetRecord(
                scene_id="scene-a",
                band="b1",
                path="s3://cube/loader/scene-a.tif",
                acq_time="2026-03-09T00:00:00Z",
                bbox=[116.38, 39.90, 116.40, 39.91],
            )
        ],
        grid_type="mgrs",
        grid_level=2,
        cover_mode="minimal",
        max_cells_per_asset=0,
    )

    assert tasks
    assert all(task["space_code"] and task["topology_code"] is None for task in tasks)


def test_build_manifest_supports_sentinel2_optical_filenames(tmp_path: Path):
    source = tmp_path / "T50TMK_20240424T030539_B08_10m.tif"
    _write_tif(source)

    records = build_manifest(tmp_path)

    assert len(records) == 1
    assert records[0].scene_id == "S2_T50TMK_20240424T030539"
    assert records[0].band == "b08_10m"
    assert records[0].acq_time == "2024-04-24T03:05:39Z"
    assert records[0].product_family == "sentinel2"
    assert records[0].sensor == "sentinel2_msi"


def test_build_manifest_supports_sentinel1_radar_dat_filenames(tmp_path: Path):
    source = tmp_path / "20180615_VV.dat"
    source.write_bytes(b"")

    records = build_manifest(tmp_path, data_type="radar")

    assert len(records) == 1
    assert records[0].scene_id == "S1_20180615"
    assert records[0].band == "vv"
    assert records[0].acq_time == "2018-06-15T00:00:00Z"
    assert records[0].product_family == "sentinel1"
    assert records[0].sensor == "sentinel1_sar"


def test_build_manifest_supports_landsat_l1tp_filenames(tmp_path: Path):
    source = tmp_path / "LO81200292021293BJC00_B1.TIF"
    _write_tif(source)

    records = build_manifest(tmp_path)

    assert len(records) == 1
    assert records[0].scene_id == "LO81200292021293BJC00"
    assert records[0].band == "b1"
    assert records[0].acq_time == "2021-10-20T00:00:00Z"
    assert records[0].product_family == "landsat"
    assert records[0].sensor == "landsat8_oli_tirs"


def test_build_manifest_l1tp_band_qa(tmp_path: Path):
    source = tmp_path / "LO81200292021293BJC00_BQA.TIF"
    _write_tif(source)

    records = build_manifest(tmp_path)

    assert len(records) == 1
    assert records[0].scene_id == "LO81200292021293BJC00"
    assert records[0].band == "bqa"


def test_build_manifest_l1tp_various_sensor_prefixes(tmp_path: Path):
    for prefix, expected_sensor in [
        ("LC8", "landsat8_oli_tirs"),
        ("LC9", "landsat9_oli_tirs"),
        ("LE7", "landsat7_etm"),
        ("LO9", "landsat9_oli_tirs"),
        ("LT5", "landsat5_tm"),
    ]:
        source = tmp_path / f"{prefix}1190292020200BJC00_B1.TIF"
        _write_tif(source)

    records = build_manifest(tmp_path, product_family="landsat_l1tp")

    assert len(records) == 5
    # files are sorted alphabetically by path
    assert records[0].sensor == "landsat8_oli_tirs"  # LC8
    assert records[1].sensor == "landsat9_oli_tirs"  # LC9
    assert records[2].sensor == "landsat7_etm"  # LE7
    assert records[3].sensor == "landsat9_oli_tirs"  # LO9
    assert records[4].sensor == "landsat5_tm"  # LT5


def test_supported_optical_product_families_are_registered():
    assert supported_optical_product_families() == ("landsat", "sentinel2", "other")
    assert get_optical_product_adapter("sentinel_optical").family == "sentinel2"


def test_build_manifest_can_use_explicit_product_family(tmp_path: Path):
    source = tmp_path / "T50TMK_20240424T030539_B08_10m.tif"
    _write_tif(source)

    records = build_manifest(tmp_path, product_family="sentinel2")

    assert len(records) == 1
    assert records[0].scene_id == "S2_T50TMK_20240424T030539"
    assert records[0].product_family == "sentinel2"


def test_build_manifest_rejects_unknown_product_family(tmp_path: Path):
    source = tmp_path / "T50TMK_20240424T030539_B08_10m.tif"
    _write_tif(source)

    with pytest.raises(ValueError, match="Unsupported optical product_family"):
        build_manifest(tmp_path, product_family="unknown-family")


def test_build_manifest_keeps_generic_tif_fallback_for_auto_detection(tmp_path: Path):
    source = tmp_path / "demo_scene_blue.tif"
    _write_tif(source)

    records = build_manifest(tmp_path)

    assert len(records) == 1
    assert records[0].scene_id == "demo_scene_blue"
    assert records[0].band == "demo_scene_blue"
    assert records[0].acq_time == "1970-01-01T00:00:00Z"
    assert records[0].product_family == "generic_tif"


def test_build_manifest_recurses_and_parses_shandong_mosaic_filenames(tmp_path: Path):
    nested = tmp_path / "Shandong_mosaic_2020Q3_sr_band4_cut"
    nested.mkdir()
    source = nested / "Shandong_mosaic_2020Q3_sr_band4_cut.tif"
    _write_tif(source)

    records = build_manifest(tmp_path)

    assert len(records) == 1
    assert records[0].scene_id == "Shandong_mosaic_2020Q3"
    assert records[0].band == "sr_band4"
    assert records[0].acq_time == "2020-07-01T00:00:00Z"
    assert records[0].product_family == "other"
    assert records[0].sensor == "optical_mosaic"


def test_build_manifest_supports_unified_manifest_jsonl(tmp_path: Path):
    source = tmp_path / "Shandong_mosaic_2020Q3_sr_band4_cut.tif"
    _write_tif(source)
    manifest = tmp_path / "manifest.jsonl"
    manifest.write_text(
        (
            '{"data_type":"optical","scene_id":"Shandong_mosaic_2020Q3","band":"sr_band4",'
            '"acq_time":"2020-07-01T00:00:00Z","source_uri":"Shandong_mosaic_2020Q3_sr_band4_cut.tif",'
            '"sensor":"optical_mosaic","product_family":"other","resolution":30,'
            '"corners":[[117.0,36.0],[117.2,36.0],[117.2,35.8],[117.0,35.8]]}\n'
        ),
        encoding="utf-8",
    )

    records = build_manifest(tmp_path, manifest_path=manifest)

    assert len(records) == 1
    assert records[0].scene_id == "Shandong_mosaic_2020Q3"
    assert records[0].band == "sr_band4"
    assert records[0].path == str(source.resolve())
    assert records[0].acq_time == "2020-07-01T00:00:00Z"
    assert records[0].product_family == "other"
    assert records[0].sensor == "optical_mosaic"
    assert records[0].bbox == [117.0, 35.8, 117.2, 36.0]
    assert records[0].corners == [[117.0, 36.0], [117.2, 36.0], [117.2, 35.8], [117.0, 35.8]]
    assert records[0].resolution == 30


def test_create_unique_run_dir_avoids_same_second_collisions(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("cube_split.jobs.ray_partition_core.time.strftime", lambda fmt: "run_20260630_150000")

    first = create_unique_run_dir(tmp_path)
    second = create_unique_run_dir(tmp_path)

    assert first.name == "run_20260630_150000"
    assert second.name == "run_20260630_150000_01"
    assert first.exists()
    assert second.exists()


def test_build_manifest_manifest_jsonl_requires_required_fields(tmp_path: Path):
    manifest = tmp_path / "manifest.jsonl"
    manifest.write_text('{"scene_id":"s1"}\n', encoding="utf-8")

    with pytest.raises(ValueError, match="required fields are source_uri, scene_id, acq_time"):
        build_manifest(tmp_path, manifest_path=manifest)


def test_build_manifest_supports_batch_manifest_json_with_assets(tmp_path: Path):
    source = tmp_path / "Shandong_mosaic_2020Q3_sr_band4_cut.tif"
    _write_tif(source)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        (
            '{"batch_id":"optical_batch_xx","data_type":"optical","assets":['
            '{"scene_id":"Shandong_mosaic_2020Q3","band":"sr_band4","acq_time":"2020-07-01T00:00:00Z",'
            '"source_uri":"Shandong_mosaic_2020Q3_sr_band4_cut.tif","sensor":"optical_mosaic",'
            '"product_family":"other","resolution":30,'
            '"corners":[[117.0,36.0],[117.2,36.0],[117.2,35.8],[117.0,35.8]]}'
            "]}"
        ),
        encoding="utf-8",
    )

    records = build_manifest(tmp_path, manifest_path=manifest)
    assert len(records) == 1
    assert records[0].scene_id == "Shandong_mosaic_2020Q3"
    assert records[0].bbox == [117.0, 35.8, 117.2, 36.0]
    assert records[0].resolution == 30


def test_build_manifest_manifest_requires_four_corners(tmp_path: Path):
    source = tmp_path / "Shandong_mosaic_2020Q3_sr_band4_cut.tif"
    _write_tif(source)
    manifest = tmp_path / "manifest.jsonl"
    manifest.write_text(
        (
            '{"scene_id":"Shandong_mosaic_2020Q3","band":"sr_band4","acq_time":"2020-07-01T00:00:00Z",'
            '"source_uri":"Shandong_mosaic_2020Q3_sr_band4_cut.tif","sensor":"optical_mosaic",'
            '"product_family":"other","resolution":30,"corners":[[117.0,36.0],[117.2,36.0]]}\n'
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="corners"):
        build_manifest(tmp_path, manifest_path=manifest)
