#!/usr/bin/env python3
"""Create a deterministic Landsat 8/9-sized 30 m mock scene.

The file models one WRS-2 scene on a common 30 m grid. It is intentionally a
synthetic raster: the dimensions, band count, dtype, CRS, transform and block
layout match the benchmark contract, while pixel values are deterministic
spatial texture rather than copied satellite observations.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
from rasterio.transform import from_origin


SCENE_WIDTH_M = 185_000
SCENE_HEIGHT_M = 180_000
RESOLUTION_M = 30
WIDTH = SCENE_WIDTH_M // RESOLUTION_M + (1 if SCENE_WIDTH_M % RESOLUTION_M else 0)
HEIGHT = SCENE_HEIGHT_M // RESOLUTION_M
BANDS = tuple(f"B{index}" for index in range(1, 12))
CRS = "EPSG:32649"
ORIGIN_X = 500_000.0
ORIGIN_Y = 4_000_000.0


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def block_values(rows: np.ndarray, cols: np.ndarray, band_index: int) -> np.ndarray:
    """Return reproducible reflectance-like uint16 texture for one block."""
    x = cols.astype(np.uint64)[None, :]
    y = rows.astype(np.uint64)[:, None]
    seed = 0x9E3779B1 * band_index + int(rows[0]) * 1_000_003 + int(cols[0]) * 97
    rng = np.random.default_rng(seed)
    coarse = ((x // 32) * 17 + (y // 32) * 29 + band_index * 131) % 2400
    noise = rng.integers(-900, 901, size=(len(rows), len(cols)), dtype=np.int16)
    values = np.clip(2600 + coarse.astype(np.int32) + noise.astype(np.int32), 0, 12_000)
    return np.asarray(values, dtype=np.uint16)


def build_scene(output: Path) -> dict[str, Any]:
    started = time.perf_counter()
    output.parent.mkdir(parents=True, exist_ok=True)
    profile = {
        "driver": "GTiff",
        "dtype": "uint16",
        "count": len(BANDS),
        "width": WIDTH,
        "height": HEIGHT,
        "crs": CRS,
        "transform": from_origin(ORIGIN_X, ORIGIN_Y, RESOLUTION_M, RESOLUTION_M),
        "nodata": 0,
        "tiled": True,
        "blockxsize": 512,
        "blockysize": 512,
        "compress": "deflate",
        "predictor": 2,
        "zlevel": 1,
        "BIGTIFF": "IF_SAFER",
    }
    with rasterio.open(output, "w", **profile) as destination:
        for band_index, band_name in enumerate(BANDS, start=1):
            destination.set_band_description(band_index, band_name)
            for _, window in destination.block_windows(band_index):
                rows = np.arange(
                    int(window.row_off), int(window.row_off + window.height), dtype=np.uint64
                )
                cols = np.arange(
                    int(window.col_off), int(window.col_off + window.width), dtype=np.uint64
                )
                destination.write(
                    block_values(rows, cols, band_index),
                    indexes=band_index,
                    window=window,
                )

    with rasterio.open(output) as source:
        if source.width != WIDTH or source.height != HEIGHT:
            raise RuntimeError(f"unexpected mock dimensions: {source.width}x{source.height}")
        if source.count != len(BANDS):
            raise RuntimeError(f"unexpected mock band count: {source.count}")
        resolution = (abs(float(source.transform.a)), abs(float(source.transform.e)))
        if resolution != (RESOLUTION_M, RESOLUTION_M):
            raise RuntimeError(f"unexpected mock resolution: {resolution}")
        if source.crs is None or source.crs.to_string() != CRS:
            raise RuntimeError(f"unexpected mock CRS: {source.crs}")
        if source.read(1, window=((0, 1), (0, 1))).size != 1:
            raise RuntimeError("mock source window read failed")
        bounds = [float(source.bounds.left), float(source.bounds.bottom), float(source.bounds.right), float(source.bounds.top)]

    metadata = {
        "mock": True,
        "contract": "Landsat 8/9 OLI/TIRS WRS-2 single scene on common 30 m grid",
        "scene_coverage_m": [SCENE_WIDTH_M, SCENE_HEIGHT_M],
        "width": WIDTH,
        "height": HEIGHT,
        "count": len(BANDS),
        "bands": list(BANDS),
        "resolution_m": RESOLUTION_M,
        "dtype": "uint16",
        "crs": CRS,
        "bounds_projected": bounds,
        "size_bytes": output.stat().st_size,
        "checksum": sha256_file(output),
        "build_elapsed_sec": round(time.perf_counter() - started, 6),
    }
    output.with_suffix(output.suffix + ".json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return metadata


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    metadata = build_scene(args.output)
    print(json.dumps({"output": str(args.output), **metadata}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
