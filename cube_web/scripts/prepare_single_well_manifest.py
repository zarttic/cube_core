#!/usr/bin/env python3
"""Prepare a replayable full-scene performance manifest from MinIO sources.

The preparation step is intentionally separate from partition timing.  Formal
performance runs preserve the complete raster scene and all of its bands,
uploading worker-readable COGs under a namespaced source prefix.  Crop mode is
kept for local debugging only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from uuid import uuid4

import rasterio
from minio import Minio
from rasterio.warp import transform_bounds
from rasterio.windows import Window
from rasterio.windows import transform as window_transform

REPO_ROOT = Path(__file__).resolve().parents[2]
for package_root in (REPO_ROOT / "cube_encoder", REPO_ROOT / "cube_split", REPO_ROOT / "cube_web"):
    if str(package_root) not in sys.path:
        sys.path.insert(0, str(package_root))

from cube_split import runtime_config  # noqa: E402


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def elapsed(started: float) -> float:
    return round(time.perf_counter() - started, 6)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_object(client: Minio, bucket: str, key: str) -> tuple[str, float, int]:
    started = time.perf_counter()
    response = client.get_object(bucket, key)
    digest = hashlib.sha256()
    total = 0
    try:
        for chunk in response.stream(1024 * 1024):
            digest.update(chunk)
            total += len(chunk)
    finally:
        response.close()
        response.release_conn()
    return digest.hexdigest(), elapsed(started), total


def download_object(client: Minio, bucket: str, key: str, path: Path) -> float:
    started = time.perf_counter()
    client.fget_object(bucket, key, str(path))
    return elapsed(started)


def upload_if_needed(client: Minio, bucket: str, key: str, path: Path, checksum: str) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        existing = client.stat_object(bucket, key)
    except Exception as exc:
        if getattr(exc, "code", None) not in {"NoSuchKey", "NoSuchObject", "ResourceNotFound"}:
            raise
        existing = None
    if existing is not None:
        metadata = {str(name).lower(): str(value) for name, value in (getattr(existing, "metadata", {}) or {}).items()}
        remote_checksum = metadata.get("checksum-sha256") or metadata.get("x-amz-meta-checksum-sha256")
        if int(getattr(existing, "size", -1)) != path.stat().st_size or remote_checksum != checksum:
            raise RuntimeError(f"prepared source collision: s3://{bucket}/{key}")
        return {"uploaded": False, "elapsed_sec": elapsed(started)}
    client.fput_object(
        bucket,
        key,
        str(path),
        content_type="image/tiff",
        metadata={"checksum-sha256": checksum},
    )
    return {"uploaded": True, "elapsed_sec": elapsed(started)}


def prepare_raster(
    source_path: Path,
    output_path: Path,
    *,
    band_indexes: list[int] | None = None,
    window_size: int | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    with rasterio.open(source_path) as source:
        indexes = list(band_indexes or source.indexes)
        if not indexes or any(index < 1 or index > source.count for index in indexes):
            raise ValueError(f"invalid source band indexes for {source_path}: {indexes}")
        if window_size is None:
            width = source.width
            height = source.height
            col_off = 0
            row_off = 0
        else:
            width = min(window_size, source.width)
            height = min(window_size, source.height)
            col_off = max(0, (source.width - width) // 2)
            row_off = max(0, (source.height - height) // 2)
        window = Window(col_off, row_off, width, height)
        profile = source.profile.copy()
        profile.update(
            driver="GTiff",
            count=len(indexes),
            width=width,
            height=height,
            transform=window_transform(window, source.transform),
            compress="deflate",
            predictor=3 if str(source.dtypes[indexes[0] - 1]).startswith("float") else 2,
            BIGTIFF="IF_SAFER",
        )
        if width >= 16 and height >= 16:
            profile.update(
                tiled=True,
                blockxsize=max(16, min(512, (width // 16) * 16)),
                blockysize=max(16, min(512, (height // 16) * 16)),
            )
        else:
            profile.update(tiled=False)
            profile.pop("blockxsize", None)
            profile.pop("blockysize", None)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(output_path, "w", **profile) as destination:
            for _, destination_window in destination.block_windows(1):
                source_window = Window(
                    window.col_off + destination_window.col_off,
                    window.row_off + destination_window.row_off,
                    destination_window.width,
                    destination_window.height,
                )
                destination.write(
                    source.read(indexes, window=source_window),
                    indexes=list(range(1, len(indexes) + 1)),
                    window=destination_window,
                )
    checksum = sha256_file(output_path)
    with rasterio.open(output_path) as cropped:
        bounds = cropped.bounds
        if cropped.crs and str(cropped.crs).upper() != "EPSG:4326":
            bbox = transform_bounds(
                cropped.crs,
                "EPSG:4326",
                bounds.left,
                bounds.bottom,
                bounds.right,
                bounds.top,
                densify_pts=21,
            )
        else:
            bbox = (bounds.left, bounds.bottom, bounds.right, bounds.top)
        metadata = {
            "width": cropped.width,
            "height": cropped.height,
            "count": cropped.count,
            "source_band_count": source.count,
            "source_band_indexes": indexes,
            "dtype": cropped.dtypes[0],
            "crs": cropped.crs.to_string() if cropped.crs else None,
            "bbox": [round(float(value), 10) for value in bbox],
            "source_window": {
                "col_off": int(col_off),
                "row_off": int(row_off),
                "width": int(width),
                "height": int(height),
                "band_indexes": indexes,
            },
            "checksum": checksum,
            "size_bytes": output_path.stat().st_size,
            "prepare_elapsed_sec": elapsed(started),
        }
    return metadata


def raster_asset(
    *,
    data_type: str,
    asset_id: str,
    scene_id: str,
    uri: str,
    metadata: dict[str, Any],
    time_start: str,
    bands: list[dict[str, Any]],
    scene_mode: str,
) -> dict[str, Any]:
    asset_bands = []
    for index, band in enumerate(bands):
        item = dict(band)
        item["source_asset_id"] = asset_id
        item.setdefault("display_order", index)
        item.setdefault("attributes", {})
        item["attributes"] = {
            **dict(item["attributes"]),
            "source_band_index": metadata["source_band_indexes"][index],
        }
        asset_bands.append(item)
    return {
        "asset_id": asset_id,
        "cog_uri": uri,
        "source_kind": "cog",
        "source_format": "cog",
        "checksum": metadata["checksum"],
        "bbox": metadata["bbox"],
        "crs": metadata["crs"],
        "time_start": time_start,
        "time_end": time_start,
        "attributes": {
            "preparation": scene_mode,
            "width": metadata["width"],
            "height": metadata["height"],
            "count": metadata["count"],
            "source_band_count": metadata["source_band_count"],
            "source_band_indexes": metadata["source_band_indexes"],
            "source_window": metadata["source_window"],
        },
        "bands": asset_bands,
    }


def dataset(dataset_id: str, title: str, data_type: str, scene_id: str, assets: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "dataset_id": dataset_id,
        "dataset_code": dataset_id,
        "dataset_title": title,
        "data_type": data_type,
        "scenes": [{"scene_id": scene_id, "scene_key": scene_id, "assets": assets}],
    }


def prepare(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any]]:
    settings = runtime_config.minio_settings()
    if not all((settings.endpoint, settings.access_key, settings.secret_key, settings.bucket)):
        raise RuntimeError("MinIO runtime configuration is incomplete")
    client = Minio(settings.endpoint, access_key=settings.access_key, secret_key=settings.secret_key, secure=settings.secure)
    token = args.token or f"{time.strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"
    optical_local_source = str(args.optical_local_source or os.getenv("CUBE_PERF_OPTICAL_LOCAL_SOURCE") or "").strip()
    source_keys = {
        "optical": str(args.optical_source or os.getenv("CUBE_PERF_OPTICAL_SOURCE") or "").strip(),
        "radar_vv": str(args.radar_vv_source or args.radar_source or os.getenv("CUBE_PERF_RADAR_VV_SOURCE") or os.getenv("CUBE_PERF_RADAR_SOURCE") or "").strip(),
        "radar_vv_header": str(args.radar_vv_header_source or args.radar_header_source or os.getenv("CUBE_PERF_RADAR_VV_HEADER_SOURCE") or os.getenv("CUBE_PERF_RADAR_HEADER_SOURCE") or "").strip(),
        "radar_vh": str(args.radar_vh_source or os.getenv("CUBE_PERF_RADAR_VH_SOURCE") or "").strip(),
        "radar_vh_header": str(args.radar_vh_header_source or os.getenv("CUBE_PERF_RADAR_VH_HEADER_SOURCE") or "").strip(),
        "product": str(args.product_source or os.getenv("CUBE_PERF_PRODUCT_SOURCE") or "").strip(),
        "carbon": str(args.carbon_source or os.getenv("CUBE_PERF_CARBON_SOURCE") or "").strip(),
    }
    missing_sources = [
        name for name, value in source_keys.items()
        if not value and name not in {"optical", "radar_vh", "radar_vh_header"}
    ]
    if not source_keys["optical"] and not optical_local_source:
        missing_sources.append("optical or optical_local_source")
    if bool(source_keys["radar_vh"]) != bool(source_keys["radar_vh_header"]):
        raise RuntimeError("radar VH source and header must be provided together")
    if missing_sources:
        raise RuntimeError(
            "source object keys or local optical source are required via CLI or CUBE_PERF_* environment variables: "
            + ", ".join(missing_sources)
        )
    scene_mode = str(args.scene_mode).lower()
    if scene_mode not in {"full", "crop"}:
        raise ValueError(f"unsupported scene mode: {scene_mode}")
    if scene_mode == "full" and not source_keys["radar_vh"]:
        raise RuntimeError("full-scene radar preparation requires both VV and VH sources")
    if args.expected_optical_bands < 1:
        raise RuntimeError("full-scene optical preparation requires at least one band")
    optical_band_codes = [
        item.strip() for item in str(args.optical_band_codes or "").split(",") if item.strip()
    ]
    if optical_band_codes and len(optical_band_codes) != args.expected_optical_bands:
        raise RuntimeError(
            "optical_band_codes must contain exactly expected_optical_bands entries"
        )
    if not optical_band_codes:
        optical_band_codes = [
            f"B{index:02d}" for index in range(1, args.expected_optical_bands + 1)
        ]
    window_size = args.window_size if scene_mode == "crop" else None
    if scene_mode == "crop" and (window_size is None or window_size < 1):
        raise ValueError("crop mode requires --window-size >= 1")
    output_dir = Path(args.output_dir) / token
    output_dir.mkdir(parents=True, exist_ok=False)
    source_prefix = f"cube/source/perf/single-well/{token}"
    started = time.perf_counter()
    records: list[dict[str, Any]] = []
    with TemporaryDirectory(prefix=f"single-well-prep-{token}-") as temp_dir:
        temp = Path(temp_dir)
        raster_specs: list[dict[str, Any]] = []
        if optical_local_source:
            optical_source: str | Path = Path(optical_local_source)
        else:
            optical_source = source_keys["optical"]
        raster_specs.append({
            "data_type": "optical",
            "role": "optical",
            "source": optical_source,
            "header": None,
            "filename": "optical_scene.tif",
            "time_start": args.optical_time_start,
            "asset_id": "optical-well-asset",
            "bands": [
                {"band_code": code, "band_name": code, "band_type": "spectral"}
                for code in optical_band_codes
            ],
        })
        raster_specs.append({
            "data_type": "radar",
            "role": "radar_vv",
            "source": source_keys["radar_vv"],
            "header": source_keys["radar_vv_header"],
            "filename": "radar_vv_scene.tif",
            "time_start": "2018-06-03T00:00:00Z",
            "asset_id": "radar-vv-well-asset",
            "bands": [{"band_code": "VV", "band_name": "VV", "band_type": "polarization"}],
        })
        if source_keys["radar_vh"]:
            raster_specs.append({
                "data_type": "radar",
                "role": "radar_vh",
                "source": source_keys["radar_vh"],
                "header": source_keys["radar_vh_header"],
                "filename": "radar_vh_scene.tif",
                "time_start": "2018-06-03T00:00:00Z",
                "asset_id": "radar-vh-well-asset",
                "bands": [{"band_code": "VH", "band_name": "VH", "band_type": "polarization"}],
            })
        raster_specs.append({
            "data_type": "product",
            "role": "product",
            "source": source_keys["product"],
            "header": None,
            "filename": "product_scene.tif",
            "time_start": "2020-01-01T00:00:00Z",
            "asset_id": "product-well-asset",
            "bands": [{"band_code": "VALUE", "band_name": "Product value", "band_type": "variable"}],
        })
        prepared_assets: dict[str, list[dict[str, Any]]] = {"optical": [], "radar": [], "product": []}
        for spec in raster_specs:
            source = spec["source"]
            if isinstance(source, Path):
                if not source.is_file():
                    raise FileNotFoundError(f"local optical source not found: {source}")
                source_path = source
                download_sec = 0.0
                source_uri = f"local:{source.name}"
            else:
                source_path = temp / Path(source).name
                download_sec = download_object(client, settings.bucket, source, source_path)
                source_uri = f"s3://{settings.bucket}/{source}"
            sidecar_download_sec = 0.0
            if spec["header"]:
                header_path = temp / Path(spec["header"]).name
                sidecar_download_sec = download_object(client, settings.bucket, spec["header"], header_path)
            output_path = temp / spec["filename"]
            requested_indexes = None if scene_mode == "full" else [1]
            metadata = prepare_raster(
                source_path,
                output_path,
                band_indexes=requested_indexes,
                window_size=window_size,
            )
            if spec["data_type"] == "optical" and scene_mode == "full" and metadata["count"] != args.expected_optical_bands:
                raise RuntimeError(
                    f"standard optical scene requires {args.expected_optical_bands} bands, "
                    f"observed {metadata['count']} in {source_path}"
                )
            if len(spec["bands"]) != metadata["count"]:
                if spec["data_type"] == "optical" and scene_mode == "full":
                    raise RuntimeError("optical band metadata does not match raster band count")
                spec["bands"] = [
                    {"band_code": f"B{index:02d}", "band_name": f"Band {index}", "band_type": "spectral"}
                    for index in metadata["source_band_indexes"]
                ]
            output_key = f"{source_prefix}/{spec['filename']}"
            upload = upload_if_needed(client, settings.bucket, output_key, output_path, metadata["checksum"])
            uri = f"s3://{settings.bucket}/{output_key}"
            asset = raster_asset(
                data_type=spec["data_type"],
                asset_id=spec["asset_id"],
                scene_id=f"{spec['data_type']}-well-scene",
                uri=uri,
                metadata=metadata,
                time_start=spec["time_start"],
                bands=spec["bands"],
                scene_mode=scene_mode,
            )
            prepared_assets[spec["data_type"]].append(asset)
            records.append({
                "data_type": spec["data_type"],
                "asset_role": spec["role"],
                "source_uri": source_uri,
                "source_size_bytes": source_path.stat().st_size,
                "source_download_elapsed_sec": download_sec,
                "sidecar_download_elapsed_sec": sidecar_download_sec,
                "output_uri": uri,
                "output": metadata,
                "upload": upload,
            })
        carbon_uri = f"s3://{settings.bucket}/{source_keys['carbon']}"
        carbon_stat_started = time.perf_counter()
        carbon_stat = client.stat_object(settings.bucket, source_keys["carbon"])
        carbon_stat_sec = elapsed(carbon_stat_started)
        carbon_checksum, carbon_checksum_sec, carbon_bytes = sha256_object(client, settings.bucket, source_keys["carbon"])
        carbon_asset_id = "carbon-well-asset"
        carbon_band = {
            "source_asset_id": carbon_asset_id,
            "band_code": "XCO2",
            "band_name": "XCO2",
            "band_type": "variable",
            "unit": "ppm",
            "display_order": 0,
        }
        carbon_asset = {
            "asset_id": carbon_asset_id,
            "source_uri": carbon_uri,
            "source_kind": "raw",
            "source_format": "netcdf",
            "checksum": carbon_checksum,
            "time_start": "2020-12-31T00:00:00Z",
            "time_end": "2020-12-31T00:00:00Z",
            "bands": [carbon_band],
        }
        records.append({
            "data_type": "carbon",
            "source_uri": carbon_uri,
            "source_size_bytes": int(getattr(carbon_stat, "size", 0) or 0),
            "source_stat_elapsed_sec": carbon_stat_sec,
            "source_checksum_elapsed_sec": carbon_checksum_sec,
            "checksum": carbon_checksum,
            "bytes_read": carbon_bytes,
        })
    manifest = {
        "schema_version": "single-well-performance-v1",
        "source_system": "single-well-performance-prepared",
        "well_id": args.well_id or f"single-well-{token}",
        "scene_mode": scene_mode,
        "expected_band_counts": {"optical": args.expected_optical_bands},
        "grid_levels": {"geohash": args.geohash_level, "mgrs": args.mgrs_level, "isea4h": args.isea4h_level},
        "max_cells_per_asset": 0,
        "carbon_max_observations": 1,
        "datasets": [
            dataset("optical-well", "Single well optical full scene", "optical", "optical-well-scene", prepared_assets["optical"]),
            dataset("radar-well", "Single well radar full scene", "radar", "radar-well-scene", prepared_assets["radar"]),
            dataset("product-well", "Single well product full scene", "product", "product-well-scene", prepared_assets["product"]),
            {
                "dataset_id": "carbon-well",
                "dataset_code": "carbon-well",
                "dataset_title": "Single well carbon observation",
                "data_type": "carbon",
                "product_type": "xco2",
                "scenes": [{"scene_id": "carbon-well-scene", "scene_key": "carbon-well-scene", "assets": [carbon_asset]}],
            },
        ],
    }
    report = {
        "schema_version": "single-well-preparation-v1",
        "prepared_at": now_iso(),
        "well_id": manifest["well_id"],
        "token": token,
        "source_prefix": f"s3://{settings.bucket}/{source_prefix}/",
        "source_keys": source_keys,
        "scene_mode": scene_mode,
        "window_size": window_size,
        "expected_optical_bands": args.expected_optical_bands,
        "grid_levels": manifest["grid_levels"],
        "records": records,
        "total_elapsed_sec": elapsed(started),
        "note": "Preparation and upload timings are excluded from partition threshold timings.",
    }
    manifest_path = output_dir / "single_well_manifest.json"
    report_path = output_dir / "preparation_report.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    report["manifest"] = str(manifest_path)
    report["report"] = str(report_path)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest, report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(REPO_ROOT / ".tmp" / "single_well_performance" / "prepared"))
    parser.add_argument("--token", default=None)
    parser.add_argument("--well-id", default=None)
    parser.add_argument("--scene-mode", choices=("full", "crop"), default="full")
    parser.add_argument("--window-size", type=int, default=None, help="Center crop size; only used with --scene-mode crop")
    parser.add_argument("--expected-optical-bands", type=int, default=4)
    parser.add_argument("--optical-band-codes", default="", help="Comma-separated output band codes")
    parser.add_argument("--optical-time-start", default="2020-07-01T00:00:00Z")
    parser.add_argument("--geohash-level", type=int, default=4)
    parser.add_argument("--mgrs-level", type=int, default=1)
    parser.add_argument("--isea4h-level", type=int, default=6)
    parser.add_argument("--optical-source", default=None)
    parser.add_argument("--optical-local-source", default=None, help="Local optical scene uploaded as a prepared MinIO COG")
    parser.add_argument("--radar-source", default=None)
    parser.add_argument("--radar-header-source", default=None)
    parser.add_argument("--radar-vv-source", default=None)
    parser.add_argument("--radar-vv-header-source", default=None)
    parser.add_argument("--radar-vh-source", default=None)
    parser.add_argument("--radar-vh-header-source", default=None)
    parser.add_argument("--product-source", default=None)
    parser.add_argument("--carbon-source", default=None)
    args = parser.parse_args()
    manifest, report = prepare(args)
    print(json.dumps({"manifest": report["manifest"], "preparation_report": report["report"], "well_id": manifest["well_id"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
