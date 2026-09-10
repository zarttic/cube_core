#!/usr/bin/env python3
"""Prepare one real Sentinel-2 scene as a ten-band 10 m GeoTIFF.

The source bands are fetched from the Microsoft Planetary Computer STAC item.
Lower-resolution bands are resampled onto the native B02 10 m grid; no pixel
values are synthesized beyond that spatial resampling operation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
import requests
from rasterio.enums import Resampling
from rasterio.warp import reproject


STAC_SEARCH = "https://planetarycomputer.microsoft.com/api/stac/v1/search"
STAC_TOKEN = "https://planetarycomputer.microsoft.com/api/sas/v1/token/sentinel-2-l2a"
SCENE_BBOX = [112.0, 33.2, 113.4, 34.5]
SCENE_DATETIME = "2024-01-01/2024-12-31"
TARGET_TILE = "T49SFU"
BANDS = ("B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B09", "B11")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def select_item() -> dict[str, Any]:
    response = requests.post(
        STAC_SEARCH,
        json={
            "collections": ["sentinel-2-l2a"],
            "bbox": SCENE_BBOX,
            "datetime": SCENE_DATETIME,
            "limit": 100,
            "query": {"eo:cloud_cover": {"lt": 20}},
        },
        timeout=60,
    )
    response.raise_for_status()
    features = response.json().get("features") or []
    candidates = [
        feature for feature in features
        if TARGET_TILE in str(feature.get("id") or "")
        and all(band in (feature.get("assets") or {}) for band in BANDS)
    ]
    if not candidates:
        raise RuntimeError(f"no complete {TARGET_TILE} Sentinel-2 item found")
    return min(
        candidates,
        key=lambda feature: float(feature.get("properties", {}).get("eo:cloud_cover") or 100.0),
    )


def signed_asset_urls(item: dict[str, Any]) -> dict[str, str]:
    response = requests.get(STAC_TOKEN, timeout=60)
    response.raise_for_status()
    token = str(response.json().get("token") or "")
    if not token:
        raise RuntimeError("Planetary Computer SAS token is empty")
    assets = item.get("assets") or {}
    return {band: f"{assets[band]['href']}?{token}" for band in BANDS}


def download_one(band: str, url: str, directory: Path) -> Path:
    target = directory / f"{band}.tif"
    temporary = target.with_suffix(".download")
    with requests.get(url, stream=True, timeout=(30, 600)) as response:
        response.raise_for_status()
        with temporary.open("wb") as handle:
            for chunk in response.iter_content(8 * 1024 * 1024):
                if chunk:
                    handle.write(chunk)
    temporary.replace(target)
    return target


def build_multiband_scene(item: dict[str, Any], source_dir: Path, output_path: Path) -> dict[str, Any]:
    source_paths = {band: source_dir / f"{band}.tif" for band in BANDS}
    with rasterio.open(source_paths["B02"]) as reference:
        target_profile = reference.profile.copy()
        target_profile.update(
            driver="GTiff",
            count=len(BANDS),
            dtype=reference.dtypes[0],
            compress="deflate",
            predictor=2,
            tiled=True,
            blockxsize=512,
            blockysize=512,
            BIGTIFF="IF_SAFER",
            nodata=0,
        )
        target_transform = reference.transform
        target_crs = reference.crs
        width, height = reference.width, reference.height
        target_resolution = abs(float(reference.transform.a))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(output_path, "w", **target_profile) as output:
        for index, band in enumerate(BANDS, start=1):
            with rasterio.open(source_paths[band]) as source:
                values = np.zeros((height, width), dtype=target_profile["dtype"])
                source_resolution = abs(float(source.transform.a))
                resampling = Resampling.nearest if abs(source_resolution - target_resolution) < 1e-9 else Resampling.bilinear
                reproject(
                    source=rasterio.band(source, 1),
                    destination=values,
                    src_transform=source.transform,
                    src_crs=source.crs,
                    src_nodata=0,
                    dst_transform=target_transform,
                    dst_crs=target_crs,
                    dst_nodata=0,
                    resampling=resampling,
                )
                output.write(values, index)
                output.set_band_description(index, band)

    return {
        "item_id": item["id"],
        "item_datetime": item.get("properties", {}).get("datetime"),
        "cloud_cover": item.get("properties", {}).get("eo:cloud_cover"),
        "item_bbox": item.get("bbox"),
        "bands": list(BANDS),
        "target_grid": "B02 native 10 m grid",
        "width": width,
        "height": height,
        "count": len(BANDS),
        "crs": target_crs.to_string() if target_crs else None,
        "checksum": sha256_file(output_path),
        "size_bytes": output_path.stat().st_size,
        "source_resampling": {
            "nearest": ["B02", "B03", "B04", "B08"],
            "bilinear_to_10m": ["B05", "B06", "B07", "B8A", "B09", "B11"],
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--source-dir", required=True, type=Path)
    args = parser.parse_args()

    item = select_item()
    urls = signed_asset_urls(item)
    args.source_dir.mkdir(parents=True, exist_ok=True)
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(download_one, band, urls[band], args.source_dir) for band in BANDS]
        for future in as_completed(futures):
            future.result()
    metadata = build_multiband_scene(item, args.source_dir, args.output)
    metadata["source_assets"] = {band: urls[band].split("?", 1)[0] for band in BANDS}
    metadata_path = args.output.with_suffix(args.output.suffix + ".json")
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"output": str(args.output), "metadata": str(metadata_path), **{k: metadata[k] for k in ("item_id", "width", "height", "count", "size_bytes", "checksum")}}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
