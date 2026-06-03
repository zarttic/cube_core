from __future__ import annotations

import os
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

import cube_split.jobs.ray_partition_core as ray_partition_core
from cube_split.jobs.ray_partition_core import AssetRecord, build_grid_tasks_driver, build_manifest, convert_assets_to_cog
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


def test_build_grid_tasks_driver_supports_tile_matrix_with_asset_bbox():
    tasks = build_grid_tasks_driver(
        assets=[
            AssetRecord(
                scene_id="scene-a",
                band="b1",
                path="/tmp/scene-a.tif",
                acq_time="2026-03-09T00:00:00Z",
                bbox=[116.38, 39.90, 116.40, 39.91],
            )
        ],
        grid_type="tile_matrix",
        grid_level=8,
        cover_mode="intersect",
        max_cells_per_asset=20000,
    )

    assert len(tasks) > 0
    assert {task["grid_type"] for task in tasks} == {"tile_matrix"}
    assert all(task["space_code"].count("/") == 2 for task in tasks)


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
    assert records[0].sensor == "landsat8_oli_tirs"   # LC8
    assert records[1].sensor == "landsat9_oli_tirs"   # LC9
    assert records[2].sensor == "landsat7_etm"        # LE7
    assert records[3].sensor == "landsat9_oli_tirs"   # LO9
    assert records[4].sensor == "landsat5_tm"         # LT5


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
            ']}'
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


def test_convert_assets_to_cog_creates_cog_files(tmp_path: Path):
    with rasterio.Env() as env:
        if "COG" not in env.drivers():
            pytest.skip("COG driver unavailable in current GDAL build")

    source = tmp_path / "LC08_L2SP_123033_20201225_02_T1_blue.TIF"
    transform = from_origin(116.0, 40.0, 0.01, 0.01)
    data = np.ones((1, 8, 8), dtype=np.uint8)
    with rasterio.open(
        source,
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

    assets = [
        AssetRecord(
            scene_id="LC08_L2SP_123033_20201225_02_T1",
            band="blue",
            path=str(source),
            acq_time="2020-12-25T00:00:00Z",
        )
    ]
    converted = convert_assets_to_cog(assets, cog_input_dir=tmp_path / "cog", overwrite=False)

    assert len(converted) == 1
    out_path = Path(converted[0].path)
    assert out_path.exists()
    assert out_path.name.endswith("_cog.tif")
    assert out_path != source
    with rasterio.open(out_path) as ds:
        assert str(ds.profile.get("compress", "")).lower() == "lzw"
        assert ds.overviews(1) == []


def test_convert_assets_to_cog_can_standardize_target_crs(tmp_path: Path):
    with rasterio.Env() as env:
        if "COG" not in env.drivers():
            pytest.skip("COG driver unavailable in current GDAL build")

    source = tmp_path / "Shandong_mosaic_2020Q3_sr_band4_cut.tif"
    transform = from_origin(500000.0, 4100000.0, 30.0, 30.0)
    data = np.ones((1, 16, 16), dtype=np.int16)
    with rasterio.open(
        source,
        "w",
        driver="GTiff",
        width=16,
        height=16,
        count=1,
        dtype=data.dtype,
        crs="EPSG:32650",
        transform=transform,
    ) as ds:
        ds.write(data)

    converted = convert_assets_to_cog(
        [
            AssetRecord(
                scene_id="Shandong_mosaic_2020Q3",
                band="sr_band4",
                path=str(source),
                acq_time="2020-07-01T00:00:00Z",
            )
        ],
        cog_input_dir=tmp_path / "cog",
        target_crs="EPSG:4326",
    )

    with rasterio.open(converted[0].path) as ds:
        assert ds.crs.to_string() == "EPSG:4326"
        assert ds.width > 0
        assert ds.height > 0


def test_download_s3_object_falls_back_to_user_writable_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    class FakeStat:
        size = 4

    class FakeMinioClient:
        def stat_object(self, _bucket, _key):
            return FakeStat()

        def fget_object(self, _bucket, _key, target):
            Path(target).write_bytes(b"data")

    cache_root = tmp_path / "readonly-cache"
    cache_root.mkdir()
    cache_root.chmod(0o555)
    monkeypatch.setattr(ray_partition_core, "_minio_client", lambda _options=None: FakeMinioClient())

    try:
        downloaded = ray_partition_core._download_s3_object("s3://cube/demo/source.tif", cache_root, {})
    finally:
        cache_root.chmod(0o755)

    fallback_root = Path(tempfile.gettempdir()) / f"{cache_root.name}_u{getattr(os, 'getuid', lambda: 0)()}"
    assert downloaded.exists()
    assert downloaded.read_bytes() == b"data"
    assert fallback_root in downloaded.parents


def test_download_s3_object_uses_unique_temp_files_for_concurrent_downloads(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    class FakeStat:
        size = 4

    barrier = threading.Barrier(2)
    targets: list[str] = []

    class FakeMinioClient:
        def stat_object(self, _bucket, _key):
            return FakeStat()

        def fget_object(self, _bucket, _key, target):
            targets.append(target)
            Path(target).write_bytes(b"data")
            barrier.wait(timeout=5)

    monkeypatch.setattr(ray_partition_core, "_minio_client", lambda _options=None: FakeMinioClient())

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda _idx: ray_partition_core._download_s3_object("s3://cube/demo/source.tif", tmp_path / "cache", {}),
                range(2),
            )
        )

    assert results[0] == results[1]
    assert results[0].read_bytes() == b"data"
    main_download_targets = [
        target
        for target in targets
        if Path(target).name.startswith(".source.tif.") and ".aux.xml." not in target and ".ovr." not in target
    ]
    assert len(set(main_download_targets)) == 2
