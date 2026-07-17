from __future__ import annotations

from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import unquote, urlparse


DATA_TYPES = ("optical", "radar", "product", "carbon")
SOURCE_PREFIX = "cube/source/"
REQUIRED_SOURCE_COUNTS = {"optical": 2, "radar": 1, "product": 1, "carbon": 1}


def classify_source_key(key: str) -> str | None:
    """Classify a MinIO source key without reading object contents."""
    text = unquote(key).casefold()
    if not text.startswith(SOURCE_PREFIX):
        return None
    if "/carbon/" in text:
        return "carbon"
    if "/radar/" in text or "/sar/" in text or "sentinel-1" in text or "sentinel1" in text:
        return "radar"
    if "/product/" in text:
        return "product"
    if "/optocal/" in text or "/optical/" in text:
        return "optical"
    return None


def collect_source_snapshot(
    client: Any,
    *,
    bucket: str,
    explicit_uris: dict[str, Iterable[str]] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """List and stat enough real objects for every M6 data type.

    The returned records deliberately exclude connection details and credentials.
    A SHA-256 mock identity is derived from stat metadata; it is not presented as
    the source object's content checksum.
    """
    candidates: dict[str, list[str]] = {data_type: [] for data_type in DATA_TYPES}
    for item in client.list_objects(bucket, prefix=SOURCE_PREFIX, recursive=True):
        key = str(getattr(item, "object_name", ""))
        data_type = classify_source_key(key)
        if data_type is not None and key not in candidates[data_type]:
            candidates[data_type].append(key)

    for data_type, uris in (explicit_uris or {}).items():
        if data_type not in candidates:
            raise ValueError(f"unsupported explicit source type: {data_type}")
        for uri in uris:
            key = source_key(uri, bucket=bucket)
            if key not in candidates[data_type]:
                candidates[data_type].insert(0, key)

    snapshot: dict[str, list[dict[str, Any]]] = {}
    for data_type in DATA_TYPES:
        required = REQUIRED_SOURCE_COUNTS[data_type]
        keys = candidates[data_type][:required]
        if len(keys) < required:
            raise RuntimeError(
                f"M6 source coverage requires {required} {data_type} object(s), found {len(keys)}; "
                f"provide CUBE_M6_{data_type.upper()}_SOURCE_URIS when source keys cannot be classified"
            )
        rows: list[dict[str, Any]] = []
        for key in keys:
            stat = client.stat_object(bucket, key)
            size = int(getattr(stat, "size", 0))
            if size <= 0:
                raise RuntimeError(f"M6 source object is empty: s3://{bucket}/{key}")
            etag = str(getattr(stat, "etag", "") or "")
            modified = getattr(stat, "last_modified", None)
            rows.append(
                {
                    "source_uri": f"s3://{bucket}/{key}",
                    "size": size,
                    "etag": etag,
                    "last_modified": _iso(modified),
                    "mock_identity_sha256": sha256(f"{bucket}\0{key}\0{size}\0{etag}".encode()).hexdigest(),
                }
            )
        snapshot[data_type] = rows
    return snapshot


def build_mock_manifest(snapshot: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    validate_snapshot(snapshot)
    optical_a, optical_b = snapshot["optical"][:2]
    radar = snapshot["radar"][0]
    product = snapshot["product"][0]
    carbon = snapshot["carbon"][0]

    scenes = [
        _scene(
            "scene-optical-shared",
            "dataset-optical",
            "optical",
            optical_a,
            assets=[
                _asset("asset-optical-red", optical_a, "B04", "spectral"),
                _asset("asset-optical-nir", optical_b, "B08", "spectral"),
            ],
        ),
        _scene(
            "scene-optical-duplicate",
            "dataset-optical",
            "optical",
            optical_a,
            checksum=optical_a["mock_identity_sha256"],
        ),
        _scene("scene-radar", "dataset-radar", "radar", radar, band="VV", band_type="polarization"),
        _scene("scene-product", "dataset-product", "product", product, band="value", band_type="variable"),
        _scene("scene-carbon", "dataset-carbon", "carbon", carbon, band="xco2", band_type="variable"),
    ]
    by_id = {scene["scene_id"]: scene for scene in scenes}
    batches = [
        {
            "load_batch_id": "load-batch-a",
            "scene_ids": ["scene-optical-shared", "scene-radar"],
            "datasets": ["dataset-optical", "dataset-radar"],
        },
        {
            "load_batch_id": "load-batch-b",
            "scene_ids": [
                "scene-optical-shared",
                "scene-optical-duplicate",
                "scene-product",
                "scene-carbon",
            ],
            "datasets": ["dataset-optical", "dataset-product", "dataset-carbon"],
        },
    ]
    memberships = [
        {
            "load_batch_id": batch["load_batch_id"],
            "scene_id": scene_id,
            "dataset_id": by_id[scene_id]["dataset_id"],
            "load_status": "duplicate" if scene_id == "scene-optical-duplicate" else "succeeded",
        }
        for batch in batches
        for scene_id in batch["scene_ids"]
    ]
    return {
        "manifest_version": "m6-mock-acceptance-v1",
        "source_snapshot": snapshot,
        "datasets": [
            {"dataset_id": "dataset-optical", "data_type": "optical"},
            {"dataset_id": "dataset-radar", "data_type": "radar"},
            {"dataset_id": "dataset-product", "data_type": "product"},
            {"dataset_id": "dataset-carbon", "data_type": "carbon"},
            {"dataset_id": "dataset-product-target", "data_type": "product"},
        ],
        "scenes": scenes,
        "load_batches": batches,
        "load_batch_scenes": memberships,
        "quality_decisions": {
            "dataset-optical": "pass",
            "dataset-radar": "warn",
            "dataset-product": "fail",
            "dataset-carbon": "pass",
        },
        "expected_coverage": {
            "single_batch_multiple_datasets": True,
            "dataset_across_batches": "dataset-optical",
            "scene_across_batches": "scene-optical-shared",
            "duplicate_checksum_scenes": ["scene-optical-shared", "scene-optical-duplicate"],
            "multi_asset_scene": "scene-optical-shared",
            "data_types": list(DATA_TYPES),
            "scene_outcomes": ["completed", "failed", "partial_failure", "retried", "cancelled"],
            "quality_decisions": ["pass", "warn", "fail"],
            "automatic_ingest_after_quality": True,
            "idempotent_ingest_redelivery": True,
            "publication_lifecycle": ["active", "withdrawn"],
            "scene_reassignment_provenance": True,
            "cell_geom_points": {"geohash": 5, "mgrs": 5, "isea4h": 7},
        },
    }


def validate_snapshot(snapshot: dict[str, list[dict[str, Any]]]) -> None:
    for data_type, required in REQUIRED_SOURCE_COUNTS.items():
        rows = snapshot.get(data_type) or []
        if len(rows) < required:
            raise ValueError(f"mock snapshot requires {required} {data_type} source object(s)")
        for row in rows[:required]:
            if not str(row.get("source_uri") or "").startswith("s3://"):
                raise ValueError(f"{data_type} source_uri must use s3://")
            if len(str(row.get("mock_identity_sha256") or "")) != 64:
                raise ValueError(f"{data_type} mock identity must be SHA-256")


def source_key(uri: str, *, bucket: str) -> str:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or parsed.netloc != bucket or not parsed.path.lstrip("/"):
        raise ValueError(f"source URI must be an object in s3://{bucket}/")
    return unquote(parsed.path.lstrip("/"))


def ensure_tmp_path(path: Path) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_relative_to(Path("/tmp")):
        raise ValueError("M6 mock artifacts must be written under /tmp")
    return resolved


def _scene(
    scene_id: str,
    dataset_id: str,
    data_type: str,
    source: dict[str, Any],
    *,
    band: str = "B01",
    band_type: str = "spectral",
    checksum: str | None = None,
    assets: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    resolved_assets = assets or [_asset(f"asset-{scene_id}", source, band, band_type)]
    return {
        "scene_id": scene_id,
        "scene_key": scene_id.removeprefix("scene-"),
        "dataset_id": dataset_id,
        "data_type": data_type,
        "checksum": checksum or source["mock_identity_sha256"],
        "acquisition_time": "2026-06-01T00:00:00Z",
        "bbox": [116.20, 39.80, 116.40, 40.00],
        "status": "loaded",
        "assets": resolved_assets,
    }


def _asset(asset_id: str, source: dict[str, Any], band: str, band_type: str) -> dict[str, Any]:
    return {
        "asset_id": asset_id,
        "source_uri": source["source_uri"],
        "checksum": source["mock_identity_sha256"],
        "bands": [{"band_code": band, "band_type": band_type}],
    }


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)
