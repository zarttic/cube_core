#!/usr/bin/env python3
"""Create external strict manifests and execute the non-skipping M1-to-M3 gate."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

REQUIRED = (
    "CUBE_WEB_POSTGRES_DSN",
    "CUBE_WEB_RAY_ADDRESS",
    "CUBE_WEB_MINIO_ENDPOINT",
    "CUBE_WEB_MINIO_ACCESS_KEY",
    "CUBE_WEB_MINIO_SECRET_KEY",
    "CUBE_WEB_MINIO_BUCKET",
)
DEFAULT_SOURCE = "s3://cube/cube/source/perf/dataset=perf_ray/sensor=optical_perf/acq_date=2020/07/01/scene_id=PERF_RAY_1/version=v1/optocal_readback_20200701_3584_b234.tif"


def _required_environment() -> dict[str, str]:
    values = {name: os.getenv(name, "").strip() for name in REQUIRED}
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise RuntimeError("M1-M3 chained gate missing required environment: " + ", ".join(missing))
    return values


def _manifest(source_uri: str, checksum: str, bbox: list[float], token: str, *, defects: int = 0) -> dict:
    declared = [{"error_code": "deterministic_defect", "message": f"manifest defect {index}", "field": "metadata"} for index in range(defects)]
    return {
        "batch_id": f"m1-m3-chain-manifest-batch-{token}",
        "grid_type": "geohash",
        "requested_grid_level": 1,
        "partition_method": "logical",
        "cover_mode": "intersect",
        "time_granularity": "day",
        "max_cells_per_asset": 20,
        "datasets": [{
            "dataset_id": f"m1-m3-chain-manifest-dataset-{token}",
            "dataset_code": f"M1M3-{token}",
            "dataset_title": "M1-M3 chained real input",
            "data_type": "optical",
            "assets": [{
                "source_asset_id": "source-1",
                "cog_uri": source_uri,
                "checksum": checksum,
                "bbox": bbox,
                "crs": "EPSG:4326",
                "time_start": "2015-03-01T00:00:00Z",
                "time_end": "2015-03-01T00:00:00Z",
                "attributes": {"m1_m3_expected_error_count": defects, "quality_metadata_defects": declared},
            }],
            "bands": [{"source_asset_id": "source-1", "band_code": "B3", "band_name": "Band 3", "band_type": "spectral", "display_order": 0}],
            "attributes": {"m1_m3_expected_error_count": defects},
        }],
    }


def main() -> int:
    values = _required_environment()
    source_uri = os.getenv("CUBE_M1_M3_CHAIN_SOURCE_URI", DEFAULT_SOURCE).strip()
    parsed = urlparse(source_uri)
    if parsed.scheme != "s3" or parsed.netloc != values["CUBE_WEB_MINIO_BUCKET"]:
        raise RuntimeError("CUBE_M1_M3_CHAIN_SOURCE_URI must be an s3 object in CUBE_WEB_MINIO_BUCKET")
    import rasterio
    from minio import Minio

    client = Minio(values["CUBE_WEB_MINIO_ENDPOINT"], access_key=values["CUBE_WEB_MINIO_ACCESS_KEY"], secret_key=values["CUBE_WEB_MINIO_SECRET_KEY"], secure=False)
    response = client.get_object(parsed.netloc, parsed.path.lstrip("/"))
    try:
        payload = response.read()
    finally:
        response.close()
        response.release_conn()
    checksum = sha256(payload).hexdigest()
    with rasterio.MemoryFile(payload) as memory:
        with memory.open() as dataset:
            if str(dataset.crs).upper() != "EPSG:4326":
                raise RuntimeError(f"M1-M3 source CRS must be EPSG:4326, got {dataset.crs}")
            bounds = dataset.bounds
            bbox = [bounds.left, bounds.bottom, bounds.right, bounds.top]
    with tempfile.TemporaryDirectory(prefix="cube-m1-m3-chain-") as directory:
        input_path, defect_path = Path(directory) / "input.json", Path(directory) / "defects.json"
        input_path.write_text(json.dumps(_manifest(source_uri, checksum, bbox, uuid4().hex)), encoding="utf-8")
        defect_path.write_text(json.dumps(_manifest(source_uri, checksum, bbox, uuid4().hex, defects=501)), encoding="utf-8")
        environment = os.environ.copy()
        environment.update({
            "CUBE_M1_M3_CHAIN_INPUT_MANIFEST": str(input_path),
            "CUBE_M1_M3_CHAIN_DEFECT_MANIFEST": str(defect_path),
            "RAY_ADDRESS": values["CUBE_WEB_RAY_ADDRESS"],
            "CUBE_WEB_AUTH_REQUIRED": "0",
        })
        command = [sys.executable, "-m", "pytest", "cube_web/tests/real/test_m1_m3_chained_real.py", "-v", "-m", "m1_m3_chain_real", "-rs"]
        completed = subprocess.run(command, env=environment, text=True, capture_output=True)
        print(completed.stdout, end="")
        print(completed.stderr, end="", file=sys.stderr)
        summary = completed.stdout + completed.stderr
        if completed.returncode or any(token in summary for token in (" skipped", "SKIPPED", " deselected", "xfailed", "XFAIL")):
            raise SystemExit(completed.returncode or 1)
    print(json.dumps({"gate": "m1-m3-chained-real", "status": "passed", "source": source_uri, "timestamp": datetime.now(timezone.utc).isoformat(), "scenarios": 1, "skipped": 0}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
