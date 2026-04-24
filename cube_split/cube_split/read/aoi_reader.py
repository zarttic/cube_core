from __future__ import annotations

import argparse
import os
from math import ceil
from pathlib import Path
from urllib.parse import urlparse

import numpy as np
import psycopg
import rasterio
from rasterio.transform import from_origin
from rasterio.windows import Window

from grid_core.sdk import CubeEncoderSDK


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read AOI by space-code index and build a multi-band GeoTIFF")
    parser.add_argument("--bbox", nargs=4, type=float, required=True, metavar=("MIN_LON", "MIN_LAT", "MAX_LON", "MAX_LAT"))
    parser.add_argument("--time-bucket", required=True, help="Time bucket such as 20260204")
    parser.add_argument("--bands", nargs="+", required=True, help="Band list such as sr_b2 sr_b3 sr_b4")
    parser.add_argument("--output", required=True, help="Output GeoTIFF path")
    parser.add_argument("--postgres-dsn", required=True, help="PostgreSQL DSN")
    parser.add_argument("--minio-endpoint", default="127.0.0.1:9000", help="MinIO endpoint host:port")
    parser.add_argument("--minio-access-key", default="minioadmin", help="MinIO access key")
    parser.add_argument("--minio-secret-key", default="minioadmin", help="MinIO secret key")
    parser.add_argument("--grid-type", default="geohash", choices=["geohash", "mgrs", "isea4h"])
    parser.add_argument("--grid-level", type=int, default=7)
    parser.add_argument("--cover-mode", default="intersect", choices=["intersect", "contain", "minimal"])
    parser.add_argument("--cube-version", default="v1")
    return parser.parse_args()


def _parse_s3_window(value_ref_uri: str) -> tuple[str, Window]:
    base, frag = value_ref_uri.split("#", 1)
    window_str = frag.split("window=", 1)[1]
    col_off, row_off, width, height = map(int, window_str.split(","))
    parsed = urlparse(base)
    vsi = f"/vsis3/{parsed.netloc}/{parsed.path.lstrip('/')}"
    return vsi, Window(col_off, row_off, width, height)


def _query_value_refs(
    postgres_dsn: str,
    space_codes: list[str],
    time_bucket: str,
    bands: list[str],
    grid_type: str,
    grid_level: int,
    cube_version: str,
) -> list[tuple[str, str, str]]:
    with psycopg.connect(postgres_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT space_code, band, value_ref_uri
                FROM rs_cube_cell_fact
                WHERE grid_type = %s
                  AND grid_level = %s
                  AND time_bucket = %s
                  AND cube_version = %s
                  AND band = ANY(%s)
                  AND space_code = ANY(%s)
                ORDER BY space_code, band
                """,
                (grid_type, grid_level, time_bucket, cube_version, bands, space_codes),
            )
            return cur.fetchall()


def read_aoi_rgb(
    *,
    bbox: list[float],
    time_bucket: str,
    bands: list[str],
    output: str,
    postgres_dsn: str,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
    grid_type: str = "geohash",
    grid_level: int = 7,
    cover_mode: str = "intersect",
    cube_version: str = "v1",
) -> Path:
    sdk = CubeEncoderSDK()
    cells = sdk.cover_compact(
        grid_type=grid_type,
        level=grid_level,
        cover_mode=cover_mode,
        bbox=bbox,
        crs="EPSG:4326",
    )
    space_codes = sorted({cell.space_code for cell in cells})
    rows = _query_value_refs(
        postgres_dsn=postgres_dsn,
        space_codes=space_codes,
        time_bucket=time_bucket,
        bands=bands,
        grid_type=grid_type,
        grid_level=grid_level,
        cube_version=cube_version,
    )
    if not rows:
        raise RuntimeError("No cube rows matched the AOI and filters")

    band_chunks: dict[str, list[dict[str, object]]] = {band: [] for band in bands}
    profile = None
    px_w = 0.0
    px_h = 0.0
    union_left = None
    union_bottom = None
    union_right = None
    union_top = None

    os.environ["AWS_ACCESS_KEY_ID"] = minio_access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = minio_secret_key
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_S3_ENDPOINT"] = minio_endpoint
    os.environ["AWS_HTTPS"] = "NO"
    os.environ["AWS_VIRTUAL_HOSTING"] = "FALSE"
    for space_code, band, value_ref_uri in rows:
        vsi, window = _parse_s3_window(value_ref_uri)
        with rasterio.open(vsi) as ds:
            arr = ds.read(1, window=window)
            transform = ds.window_transform(window)
            left, bottom, right, top = rasterio.transform.array_bounds(arr.shape[0], arr.shape[1], transform)
            band_chunks[band].append(
                {
                    "space_code": space_code,
                    "array": arr,
                    "bounds": (left, bottom, right, top),
                }
            )
            if profile is None:
                profile = ds.profile.copy()
                px_w = ds.transform.a
                px_h = abs(ds.transform.e)
            union_left = left if union_left is None else min(union_left, left)
            union_bottom = bottom if union_bottom is None else min(union_bottom, bottom)
            union_right = right if union_right is None else max(union_right, right)
            union_top = top if union_top is None else max(union_top, top)

    assert profile is not None
    assert union_left is not None and union_bottom is not None and union_right is not None and union_top is not None

    width = int(ceil((union_right - union_left) / px_w))
    height = int(ceil((union_top - union_bottom) / px_h))
    out_transform = from_origin(union_left, union_top, px_w, px_h)

    out_arrays: list[tuple[str, np.ndarray]] = []
    for band in bands:
        chunks = band_chunks.get(band, [])
        if not chunks:
            raise RuntimeError(f"Missing band data for {band}")
        canvas = np.zeros((height, width), dtype=chunks[0]["array"].dtype)
        for item in chunks:
            left, _, _, top = item["bounds"]
            arr = item["array"]
            col_off = int(round((left - union_left) / px_w))
            row_off = int(round((union_top - top) / px_h))
            arr_height, arr_width = arr.shape
            target = canvas[row_off : row_off + arr_height, col_off : col_off + arr_width]
            mask = arr != 0
            target[mask] = arr[mask]
        out_arrays.append((band, canvas))

    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    profile.update(
        driver="GTiff",
        count=len(bands),
        width=width,
        height=height,
        transform=out_transform,
        compress="deflate",
        tiled=True,
    )
    with rasterio.open(output_path, "w", **profile) as dst:
        for idx, (band, arr) in enumerate(out_arrays, start=1):
            dst.write(arr, idx)
            dst.set_band_description(idx, band)
    return output_path


def main() -> None:
    args = _parse_args()
    output_path = read_aoi_rgb(
        bbox=args.bbox,
        time_bucket=args.time_bucket,
        bands=args.bands,
        output=args.output,
        postgres_dsn=args.postgres_dsn,
        minio_endpoint=args.minio_endpoint,
        minio_access_key=args.minio_access_key,
        minio_secret_key=args.minio_secret_key,
        grid_type=args.grid_type,
        grid_level=args.grid_level,
        cover_mode=args.cover_mode,
        cube_version=args.cube_version,
    )
    print(output_path)


if __name__ == "__main__":
    main()
