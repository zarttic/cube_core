import hashlib
import os
from pathlib import Path

import numpy as np
import pytest
import rasterio
from netCDF4 import Dataset
from rasterio.transform import from_origin

from cube_web.services.quality_object_reader import AssetInspection, QualityObjectReader


def _write_raster(path: Path, values: np.ndarray) -> None:
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=values.shape[1],
        height=values.shape[0],
        count=1,
        dtype=values.dtype,
        crs="EPSG:4326",
        transform=from_origin(116, 40, 0.01, 0.01),
    ) as dataset:
        dataset.write(values, 1)


def test_reader_opens_local_raster_and_returns_fixed_shape_pixel_statistics(tmp_path: Path) -> None:
    source = tmp_path / "source.tif"
    values = np.zeros((256, 256), dtype=np.uint16)
    values[64:192, 64:192] = 10
    _write_raster(source, values)

    result = QualityObjectReader().inspect(str(source), "cog", sample_pixels=True)

    assert result.crs == "EPSG:4326"
    assert result.sample_pixels == 128 * 128
    assert result.valid_pixels == 128 * 128
    assert 0 < result.nonzero_pixels < result.valid_pixels


def test_reader_samples_requested_band_from_multiband_raster(tmp_path: Path) -> None:
    source = tmp_path / "multiband.tif"
    with rasterio.open(
        source,
        "w",
        driver="GTiff",
        width=16,
        height=16,
        count=2,
        dtype="uint16",
        crs="EPSG:4326",
        transform=from_origin(116, 40, 0.01, 0.01),
    ) as dataset:
        dataset.write(np.zeros((16, 16), dtype=np.uint16), 1)
        dataset.write(np.full((16, 16), 10, dtype=np.uint16), 2)

    result = QualityObjectReader().inspect(
        str(source),
        "cog",
        sample_pixels=True,
        sample_band_index=2,
    )

    assert result.nonzero_pixels == result.valid_pixels == 16 * 16


def test_reader_rejects_requested_band_outside_raster(tmp_path: Path) -> None:
    source = tmp_path / "singleband.tif"
    _write_raster(source, np.ones((16, 16), dtype=np.uint16))

    with pytest.raises(ValueError, match="band index is out of range"):
        QualityObjectReader().inspect(str(source), "cog", sample_band_index=2)


def test_reader_rejects_missing_local_object(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        QualityObjectReader().inspect(str(tmp_path / "missing.tif"), "cog")


def test_reader_opens_local_netcdf_without_loading_observation_arrays(tmp_path: Path) -> None:
    source = tmp_path / "source.nc"
    with Dataset(source, mode="w") as dataset:
        dataset.createDimension("observation", 2)
        variable = dataset.createVariable("xco2", "f4", ("observation",))
        variable[:] = [410.0, 411.0]

    result = QualityObjectReader().inspect(str(source), "netcdf")

    assert result.sample_pixels is None


def test_reader_rejects_checksum_mismatch_before_opening(tmp_path: Path) -> None:
    source = tmp_path / "source.tif"
    _write_raster(source, np.ones((16, 16), dtype=np.uint16))

    with pytest.raises(ValueError, match="checksum mismatch"):
        QualityObjectReader().inspect(str(source), "cog", expected_checksum="0" * 64)


def test_reader_hashes_unchanged_file_only_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "source.tif"
    _write_raster(source, np.ones((16, 16), dtype=np.uint16))
    checksum = hashlib.sha256(source.read_bytes()).hexdigest()
    reader = QualityObjectReader()
    calls = 0
    original = reader._sha256_file

    def counted(path: Path) -> str:
        nonlocal calls
        calls += 1
        return original(path)

    monkeypatch.setattr(reader, "_sha256_file", counted)

    reader.inspect(str(source), "cog", expected_checksum=checksum)
    reader.inspect(str(source), "cog", sample_pixels=True, expected_checksum=checksum)

    assert calls == 1


def test_checksum_cache_rehashes_atomic_replacement_with_preserved_size_and_mtime(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"original")
    original_stat = source.stat()
    checksum = hashlib.sha256(source.read_bytes()).hexdigest()
    reader = QualityObjectReader()
    reader._verify_checksum(source, checksum)

    replacement = tmp_path / "replacement.bin"
    replacement.write_bytes(b"replaced")
    os.utime(replacement, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
    os.replace(replacement, source)

    with pytest.raises(ValueError, match="checksum mismatch"):
        reader._verify_checksum(source, checksum)


def test_hdf5_runtime_error_falls_back_to_gdal_subdatasets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "generic.h5"
    source.write_bytes(b"generic-hdf5-placeholder")

    def fail_netcdf(*_args, **_kwargs):
        raise RuntimeError("not a NetCDF-convention HDF5 file")

    class GenericHdf:
        count = 0
        subdatasets = ("HDF5:generic.h5://dataset",)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

    monkeypatch.setattr("netCDF4.Dataset", fail_netcdf)
    monkeypatch.setattr(
        "cube_web.services.quality_object_reader.rasterio.open",
        lambda *_args, **_kwargs: GenericHdf(),
    )

    result = QualityObjectReader().inspect(str(source), "hdf5")

    assert result == AssetInspection()
