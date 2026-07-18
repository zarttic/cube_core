from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import rasterio
from cube_split.jobs.ray_partition_core import resolve_asset_source_path


@dataclass(frozen=True)
class AssetInspection:
    crs: str | None = None
    sample_pixels: int | None = None
    valid_pixels: int | None = None
    nonzero_pixels: int | None = None


class QualityObjectReader:
    """Resolve source objects with the shared worker cache and inspect them locally."""

    def __init__(self, *, source_cache_dir: str = "/tmp/cube_split_source_cache/quality") -> None:
        self._resolve_options = {"source_cache_dir": source_cache_dir}
        self._verified_checksums: set[tuple[str, int, int, int, int, str]] = set()

    def inspect(
        self,
        uri: str,
        source_format: str,
        *,
        sample_pixels: bool = False,
        sample_band_index: int = 1,
        expected_checksum: str | None = None,
    ) -> AssetInspection:
        local_path = Path(resolve_asset_source_path(uri, self._resolve_options))
        if not local_path.is_file():
            raise FileNotFoundError(local_path)
        if expected_checksum is not None:
            self._verify_checksum(local_path, expected_checksum)
        if source_format == "cog":
            return self._inspect_raster(
                local_path,
                sample_pixels=sample_pixels,
                sample_band_index=sample_band_index,
            )
        if source_format in {"netcdf", "hdf5"}:
            self._inspect_scientific_container(local_path, source_format)
            return AssetInspection()
        raise ValueError(f"unsupported source format: {source_format}")

    def _verify_checksum(self, path: Path, expected_checksum: str) -> None:
        stat = path.stat()
        cache_key = (
            str(path.resolve()),
            stat.st_size,
            stat.st_mtime_ns,
            stat.st_ino,
            stat.st_ctime_ns,
            expected_checksum,
        )
        if cache_key in self._verified_checksums:
            return
        if self._sha256_file(path) != expected_checksum:
            raise ValueError("source object checksum mismatch")
        self._verified_checksums.add(cache_key)

    @staticmethod
    def _sha256_file(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _inspect_raster(
        path: Path,
        *,
        sample_pixels: bool,
        sample_band_index: int = 1,
    ) -> AssetInspection:
        with rasterio.open(path) as dataset:
            if dataset.width <= 0 or dataset.height <= 0 or dataset.count <= 0:
                raise ValueError("raster has no readable pixels")
            if sample_band_index < 1 or sample_band_index > dataset.count:
                raise ValueError(f"raster band index is out of range: {sample_band_index}")
            crs = dataset.crs.to_string() if dataset.crs else None
            if not sample_pixels:
                return AssetInspection(crs=crs)
            height = min(dataset.height, 128)
            width = min(dataset.width, 128)
            sample = dataset.read(sample_band_index, masked=True, out_shape=(1, height, width))
            valid_mask = ~np.ma.getmaskarray(sample)
            return AssetInspection(
                crs=crs,
                sample_pixels=int(sample.size),
                valid_pixels=int(valid_mask.sum()),
                nonzero_pixels=int(((np.ma.getdata(sample) != 0) & valid_mask).sum()),
            )

    @staticmethod
    def _inspect_scientific_container(path: Path, source_format: str) -> None:
        from netCDF4 import Dataset

        try:
            with Dataset(path, mode="r") as dataset:
                # Enumerating root metadata forces the container header to be
                # parsed without loading large observation arrays.
                tuple(dataset.dimensions)
                tuple(dataset.variables)
                return
        except (OSError, RuntimeError):
            if source_format != "hdf5":
                raise
        # Some valid HDF5 products do not use the NetCDF data model but are
        # still exposed by GDAL through raster bands or subdatasets.
        with rasterio.open(path) as dataset:
            if dataset.count <= 0 and not dataset.subdatasets:
                raise ValueError("HDF5 container has no readable datasets")


def quality_object_reader() -> QualityObjectReader:
    return QualityObjectReader()
