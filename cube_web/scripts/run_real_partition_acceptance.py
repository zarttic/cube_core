#!/usr/bin/env python3
"""Run the production real-source acceptance chain.

The runner deliberately consumes a prepared manifest instead of downloading a
portal order itself.  A preparation job may turn a small portal download into
COG and upload it to MinIO; this script only exercises the production import,
partition, quality and ingest contracts.  It is safe by default: identifiers
are namespaced, cleanup is opt-in, and no credential is printed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parents[2]
for root in (REPO_ROOT / "cube_encoder", REPO_ROOT / "cube_split", REPO_ROOT / "cube_web"):
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

GRID_CASES = (
    ("geohash", "logical", 1),
    ("mgrs", "logical", 1),
    ("isea4h", "entity", 6),
)
EXPECTED_RUN_COUNT = len(GRID_CASES) + 3  # three grid runs, cancel probe, quality warn and fail probes
DATA_TYPES = ("optical", "radar", "product", "carbon")
TERMINAL_TASK_STATES = {"succeeded", "completed", "failed", "cancelled", "partial_failure"}


def load_prepared(path: str | Path = "/tmp/cube-real-acceptance-prepared.json") -> dict[str, Any]:
    """Load the COG preparation artifact without exposing runtime secrets."""
    manifest_path = Path(path)
    if not manifest_path.is_file():
        raise FileNotFoundError(f"prepared manifest not found: {manifest_path}")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("prepared manifest must be an object")
    return payload


def load_manifest(path: str | Path = "/tmp/cube-real-acceptance-prepared.json") -> dict[str, Any]:
    """Load an already-derived Dataset/Scene manifest (mainly for focused use)."""
    payload = load_prepared(path)
    if not isinstance(payload.get("datasets"), list):
        raise ValueError("derived manifest must contain a datasets list")
    seen: set[str] = set()
    types: set[str] = set()
    for dataset in payload["datasets"]:
        if not isinstance(dataset, dict):
            raise ValueError("datasets entries must be objects")
        data_type = str(dataset.get("data_type") or "")
        dataset_id = str(dataset.get("dataset_id") or "")
        if data_type not in DATA_TYPES or not dataset_id:
            raise ValueError("each dataset requires a supported data_type and dataset_id")
        if dataset_id in seen:
            raise ValueError(f"duplicate dataset_id: {dataset_id}")
        seen.add(dataset_id)
        types.add(data_type)
        if not isinstance(dataset.get("scenes"), list) or not dataset["scenes"]:
            raise ValueError(f"dataset {dataset_id} requires scenes")
    missing = set(DATA_TYPES) - types
    if missing:
        raise ValueError("manifest is missing data types: " + ", ".join(sorted(missing)))
    return payload


def discover_carbon_asset(client: Any, *, bucket: str, explicit_uri: str | None = None) -> dict[str, Any]:
    """Find, stat and SHA-256 verify one real carbon NC/NC4 object in MinIO."""
    if explicit_uri:
        prefix = f"s3://{bucket}/"
        if not explicit_uri.startswith(prefix):
            raise ValueError("explicit carbon URI must use the configured MinIO bucket")
        keys = [explicit_uri[len(prefix):]]
    else:
        keys = sorted(
            item.object_name
            for item in client.list_objects(bucket, prefix="cube/source/carbon/", recursive=True)
            if str(item.object_name).lower().endswith((".nc", ".nc4"))
        )
    if len(keys) != 1:
        raise RuntimeError(f"expected exactly one carbon NC/NC4 source, observed={len(keys)}; set CUBE_CARBON_SOURCE_URI")
    key = keys[0]
    stat = client.stat_object(bucket, key)
    if int(stat.size) <= 0:
        raise RuntimeError("carbon source object is empty")
    metadata = {str(name).lower(): str(value) for name, value in (getattr(stat, "metadata", {}) or {}).items()}
    expected = str(metadata.get("x-amz-meta-sha256") or metadata.get("x-amz-meta-checksum-sha256") or metadata.get("sha256") or "").lower()
    response = client.get_object(bucket, key)
    digest = hashlib.sha256()
    try:
        for chunk in response.stream(1024 * 1024):
            digest.update(chunk)
    finally:
        response.close()
        response.release_conn()
    checksum = digest.hexdigest()
    if expected and expected != checksum:
        raise RuntimeError("carbon source SHA-256 metadata mismatch")
    return {"s3_uri": f"s3://{bucket}/{key}", "sha256": checksum, "size_bytes": int(stat.size)}


def derive_manifest(prepared: dict[str, Any], carbon: dict[str, Any]) -> dict[str, Any]:
    """Convert prepared COG roles into one multi-dataset, multi-scene load batch."""
    assets = {str(item.get("role")): item for item in prepared.get("assets", []) if isinstance(item, dict)}
    required = {"optical", "radar_vv", "radar_vh", "product_smoke", "product_standard_window"}
    missing = required - assets.keys()
    if missing:
        raise ValueError("prepared COG roles missing: " + ", ".join(sorted(missing)))

    def raster_asset(role: str, asset_id: str, bands: list[dict[str, Any]]) -> dict[str, Any]:
        row = assets[role]
        native_resolution = max(float(value) for value in row.get("resolution_native") or [30])
        geographic = str(row.get("crs") or "").upper() == "EPSG:4326"
        return {
            "asset_id": asset_id,
            "source_uri": row["s3_uri"],
            "cog_uri": row["s3_uri"],
            "source_kind": "cog",
            "source_format": "cog",
            "checksum": row["sha256"],
            "acquisition_time": "2020-07-10T00:00:00Z",
            "bbox": row["bbox_wgs84"],
            "crs": row["crs"],
            "resolution": native_resolution,
            "resolution_unit": "degree" if geographic else "m",
            "bands": bands,
        }

    optical_bands = [
        {"band_code": "B02", "band_name": "Blue", "band_type": "spectral", "display_order": 0},
        {"band_code": "B03", "band_name": "Green", "band_type": "spectral", "display_order": 1},
        {"band_code": "B04", "band_name": "Red", "band_type": "spectral", "display_order": 2},
    ]
    product_scenes = []
    for index, role in enumerate(("product_smoke", "product_standard_window"), start=1):
        product_year = 2026 if role == "product_smoke" else 2020
        asset = raster_asset(
            role,
            f"product-asset-{index}",
            [{"band_code": "VALUE", "band_name": "产品值", "band_type": "variable"}],
        )
        asset["acquisition_time"] = f"{product_year}-01-01T00:00:00Z"
        asset["attributes"] = {"product_year": product_year}
        product_scenes.append({
            "scene_id": f"product-scene-{index}",
            "scene_key": f"product-scene-{index}",
            "assets": [asset],
        })
    return {
        "schema_version": "real-acceptance-v1",
        "source_system": "noda-minio-real-acceptance",
        "datasets": [
            {"dataset_id": "optical-standard", "dataset_title": "真实光学 COG 验收数据集", "data_type": "optical", "scenes": [{"scene_id": "optical-scene", "scene_key": "optical-scene", "assets": [raster_asset("optical", "optical-asset", optical_bands)]}]},
            {"dataset_id": "radar-standard", "dataset_title": "真实雷达 COG 验收数据集", "data_type": "radar", "scenes": [{"scene_id": "radar-scene", "scene_key": "radar-scene", "assets": [
                raster_asset("radar_vv", "radar-vv", [{"band_code": "VV", "band_name": "VV", "band_type": "polarization"}]),
                raster_asset("radar_vh", "radar-vh", [{"band_code": "VH", "band_name": "VH", "band_type": "polarization"}]),
            ]}]},
            {"dataset_id": "product-standard", "dataset_title": "真实信息产品 COG 验收数据集", "data_type": "product", "scenes": product_scenes},
            {"dataset_id": "carbon-standard", "dataset_title": "真实碳卫星 NC4 验收数据集", "data_type": "carbon", "scenes": [{"scene_id": "carbon-scene", "scene_key": "carbon-scene", "assets": [{
                "asset_id": "carbon-asset", "source_uri": carbon["s3_uri"], "source_kind": "observation", "source_format": "netcdf",
                "checksum": carbon["sha256"], "acquisition_time": "2018-01-01T00:00:00Z",
                "bands": [{"band_code": "XCO2", "band_name": "XCO2", "band_type": "variable", "unit": "ppm"}],
            }]}]},
        ],
    }


def namespace_manifest(manifest: dict[str, Any], prefix: str | None = None) -> dict[str, Any]:
    """Return a deep-copy manifest whose IDs cannot collide with production data."""
    token = prefix or f"real-accept-{uuid4().hex[:12]}"
    result = json.loads(json.dumps(manifest))
    result["load_batch_id"] = f"{token}-batch"
    result["batch_name"] = f"Real partition acceptance {token}"
    for dataset in result["datasets"]:
        old_dataset_id = str(dataset["dataset_id"])
        dataset["dataset_id"] = f"{token}-{old_dataset_id}"
        dataset["dataset_code"] = f"{token}-{dataset.get('dataset_code') or old_dataset_id}"
        for scene in dataset["scenes"]:
            scene["scene_id"] = f"{token}-{scene.get('scene_id') or scene.get('scene_key') or uuid4().hex[:8]}"
            scene["canonical_scene_id"] = scene["scene_id"]
            scene["scene_key"] = f"{token}-{scene.get('scene_key') or scene['scene_id']}"
            scene["identity_key"] = f"{token}:{scene.get('identity_key') or scene['scene_key']}"
            for asset in scene.get("assets", []):
                asset["asset_id"] = f"{token}-{asset.get('asset_id') or uuid4().hex[:8]}"
                for band in asset.get("bands", []):
                    band["band_code"] = f"{token}_{band.get('band_code') or 'band'}"
    return result


def import_payload(manifest: dict[str, Any]) -> dict[str, Any]:
    """Build the public loader payload consumed by ``POST /v1/partition/schemas/import``."""
    return {
        "schema_version": manifest.get("schema_version", "real-acceptance-v1"),
        "load_batch_id": manifest["load_batch_id"],
        "batch_name": manifest.get("batch_name", manifest["load_batch_id"]),
        "source_system": manifest.get("source_system", "noda-real-acceptance"),
        "loaded_at": manifest.get("loaded_at"),
        "datasets": manifest["datasets"],
    }


@dataclass
class HttpClient:
    base_url: str
    token: str | None = None
    timeout: float = 30.0

    def request(self, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        data = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(body, ensure_ascii=False).encode()
        request = urllib.request.Request(self.base_url.rstrip("/") + path, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")
            raise RuntimeError(f"HTTP {exc.code} {method} {path}: {detail[:500]}") from exc
        return json.loads(raw or b"{}")

    def request_bytes(self, method: str, path: str) -> tuple[bytes, dict[str, str]]:
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        request = urllib.request.Request(self.base_url.rstrip("/") + path, headers=headers, method=method)
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            return response.read(), dict(response.headers.items())


def build_grid_run_payloads(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    selections = []
    for dataset in manifest["datasets"]:
        selections.append({
            "dataset_id": dataset["dataset_id"],
            "data_type": dataset["data_type"],
            "scene_ids": [scene["scene_id"] for scene in dataset["scenes"]],
            "partition": {},
        })
    payloads: list[dict[str, Any]] = []
    for grid_type, partition_method, level in GRID_CASES:
        payloads.append({
            "partition_run_id": f"{manifest['load_batch_id']}-{grid_type}",
            "source_batch_ids": [manifest["load_batch_id"]],
            "datasets": [
                {
                    **{key: value for key, value in selection.items() if key != "data_type"},
                    "partition": {
                        "grid_type": grid_type,
                        "requested_grid_level": level,
                        "partition_method": partition_method,
                        "cover_mode": "intersect",
                        "time_granularity": "day",
                        "max_cells_per_asset": 0,
                        **({"max_observations": 50} if selection["data_type"] == "carbon" else {}),
                    },
                }
                for selection in selections
            ],
        })
    return payloads


def submit_grid_runs(client: HttpClient, manifest: dict[str, Any], *, wait: bool = True, poll_seconds: float = 2.0) -> list[dict[str, Any]]:
    """Submit all three required grid modes against the same multi-type batch."""
    results: list[dict[str, Any]] = []
    for payload in build_grid_run_payloads(manifest):
        result = client.request("POST", "/v1/partition/runs", payload)
        results.append(result)
        if wait:
            results[-1]["final"] = wait_for_task(client, str(result["task_id"]), poll_seconds=poll_seconds)
    return results


def verify_idempotent_submissions(client: HttpClient, manifest: dict[str, Any], original: list[dict[str, Any]]) -> None:
    for payload, first in zip(build_grid_run_payloads(manifest), original, strict=True):
        repeated = client.request("POST", "/v1/partition/runs", payload)
        if repeated.get("task_id") != first.get("task_id"):
            raise RuntimeError(f"partition run is not idempotent: {payload['partition_run_id']}")


def probe_task_cancellation(client: HttpClient, manifest: dict[str, Any], *, poll_seconds: float = 1.0) -> dict[str, Any]:
    dataset = next(item for item in manifest["datasets"] if item["data_type"] == "product")
    payload = {
        "partition_run_id": f"{manifest['load_batch_id']}-cancel-probe",
        "source_batch_ids": [manifest["load_batch_id"]],
        "datasets": [{
            "dataset_id": dataset["dataset_id"],
            "scene_ids": [dataset["scenes"][0]["scene_id"]],
            "partition": {"grid_type": "geohash", "requested_grid_level": 1, "partition_method": "logical", "cover_mode": "intersect", "time_granularity": "day", "max_cells_per_asset": 50},
        }],
    }
    submitted = client.request("POST", "/v1/partition/runs", payload)
    cancellation = client.request("POST", f"/v1/partition/tasks/{submitted['task_id']}/cancel")
    final = wait_for_task(client, str(submitted["task_id"]), poll_seconds=poll_seconds)
    if str(final.get("status")) not in {"cancelled", "completed", "succeeded"}:
        raise RuntimeError(f"cancel probe reached an invalid terminal state: {final.get('status')}")
    retried = None
    retry_final = None
    if str(final.get("status")) == "cancelled":
        retried = client.request("POST", f"/v1/partition/tasks/{submitted['task_id']}/retry")
        retry_final = wait_for_task(client, str(retried["task_id"]), poll_seconds=poll_seconds)
        if str(retry_final.get("status")) not in {"completed", "succeeded"}:
            raise RuntimeError(f"cancelled task retry failed: {retry_final.get('status')}")
    return {
        "submitted": submitted,
        "cancellation": cancellation,
        "final": final,
        "completed_before_cancel": str(final.get("status")) != "cancelled",
        "retried": retried,
        "retry_final": retry_final,
    }


def quality_probe_manifest(manifest: dict[str, Any], expected_status: str) -> dict[str, Any]:
    """Build an isolated product Dataset for a controlled Warn or Fail decision."""
    if expected_status not in {"warn", "fail"}:
        raise ValueError("quality probe status must be warn or fail")
    prefix = manifest["load_batch_id"].removesuffix("-batch")
    source = next(item for item in manifest["datasets"] if item["data_type"] == "product")["scenes"][0]
    asset = json.loads(json.dumps(source["assets"][0]))
    asset["asset_id"] = f"{prefix}-quality-{expected_status}-asset"
    attributes = dict(asset.get("attributes") or {})
    acquisition_time = str(asset.get("acquisition_time") or "2020-01-01T00:00:00Z")
    attributes.setdefault("product_year", int(acquisition_time[:4]))
    declared_finding = {
        "error_code": "acceptance_declared_defect",
        "message": f"controlled real acceptance quality {expected_status}",
        "field": "acceptance_probe",
    }
    if expected_status == "warn":
        declared_finding["severity"] = "warning"
    attributes["quality_metadata_defects"] = [declared_finding]
    if expected_status == "fail":
        attributes.pop("product_year", None)
    asset["attributes"] = attributes
    for band in asset.get("bands", []):
        band["band_code"] = f"{prefix}_QUALITY_{expected_status.upper()}_{band.get('band_code') or 'VALUE'}"
    dataset_id = f"{prefix}-quality-{expected_status}-product"
    scene_id = f"{prefix}-quality-{expected_status}-scene"
    batch_id = f"{prefix}-quality-{expected_status}-batch"
    return {
        "schema_version": "real-acceptance-v1",
        "source_system": "noda-minio-real-acceptance",
        "load_batch_id": batch_id,
        "batch_name": f"Controlled quality {expected_status} {prefix}",
        "datasets": [{
            "dataset_id": dataset_id,
            "dataset_code": dataset_id,
            "dataset_title": f"真实信息产品质检{expected_status}探针",
            "data_type": "product",
            "scenes": [{
                "scene_id": scene_id,
                "canonical_scene_id": scene_id,
                "scene_key": scene_id,
                "identity_key": f"{prefix}:quality-{expected_status}",
                "assets": [asset],
            }],
        }],
    }


def run_quality_probe(
    client: HttpClient,
    manifest: dict[str, Any],
    expected_status: str,
    *,
    poll_seconds: float = 2.0,
) -> dict[str, Any]:
    probe = quality_probe_manifest(manifest, expected_status)
    client.request("POST", "/v1/partition/schemas/import", import_payload(probe))
    dataset = probe["datasets"][0]
    scene = dataset["scenes"][0]
    payload = {
        "partition_run_id": f"{probe['load_batch_id']}-geohash",
        "source_batch_ids": [probe["load_batch_id"]],
        "datasets": [{
            "dataset_id": dataset["dataset_id"],
            "scene_ids": [scene["scene_id"]],
            "partition": {"grid_type": "geohash", "requested_grid_level": 1, "partition_method": "logical", "cover_mode": "intersect", "time_granularity": "day", "max_cells_per_asset": 50},
        }],
    }
    submitted = client.request("POST", "/v1/partition/runs", payload)
    final = wait_for_task(client, str(submitted["task_id"]), poll_seconds=poll_seconds)
    assert_partition_success([{**submitted, "final": final}])
    quality = client.request("POST", f"/v1/datasets/{dataset['dataset_id']}/quality-runs")
    quality_run_id = str(quality["quality_run_id"])
    deadline = time.monotonic() + 900
    while True:
        quality = client.request("GET", f"/v1/quality/records/{quality_run_id}")
        if str(quality.get("status")) in {"pass", "warn", "fail", "error", "cancelled"}:
            break
        if time.monotonic() >= deadline:
            raise TimeoutError(f"quality {expected_status} probe did not finish: {quality_run_id}")
        time.sleep(poll_seconds)
    error_count = int(quality.get("error_count") or 0)
    warning_count = int(quality.get("warning_count") or 0)
    expected_count = error_count if expected_status == "fail" else warning_count
    if quality.get("status") != expected_status or expected_count < 1:
        raise RuntimeError(f"quality {expected_status} probe reached {quality.get('status')}")
    exports = {}
    for export_format in ("json", "csv"):
        content, headers = client.request_bytes("GET", f"/v1/quality/records/{quality_run_id}/errors/export?format={export_format}")
        count = int(headers.get("X-Export-Count", headers.get("x-export-count", "-1")))
        if count != error_count + warning_count or not content:
            raise RuntimeError(f"quality {expected_status} {export_format} export is incomplete")
        exports[export_format] = count
    ingests = client.request("GET", f"/v1/ingest-runs?dataset_id={dataset['dataset_id']}&page_size=20")
    if any(item.get("status") == "completed" for item in ingests.get("items", [])):
        raise RuntimeError(f"quality {expected_status} probe incorrectly triggered completed ingest")
    return {
        "dataset_id": dataset["dataset_id"],
        "quality_run_id": quality_run_id,
        "quality_status": expected_status,
        "error_count": error_count,
        "warning_count": warning_count,
        "exports": exports,
        "completed_ingest_count": 0,
    }


def assert_partition_success(results: list[dict[str, Any]]) -> None:
    failures = []
    for result in results:
        final = result.get("final") or result
        if str(final.get("status") or "").lower() not in {"succeeded", "completed"}:
            failures.append({"task_id": result.get("task_id"), "status": final.get("status"), "error": final.get("error")})
        elif not _contains_ray_evidence(final.get("result")):
            failures.append({"task_id": result.get("task_id"), "status": final.get("status"), "error": "missing Ray execution evidence"})
    if failures:
        raise RuntimeError("partition acceptance failed: " + json.dumps(failures, ensure_ascii=False))


def _contains_ray_evidence(value: Any) -> bool:
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key) in {"backend", "execution_engine", "partition_backend"} and str(item).lower() == "ray":
                return True
            if str(key) == "ray_parallelism" and int(item or 0) > 0:
                return True
            if _contains_ray_evidence(item):
                return True
    elif isinstance(value, (list, tuple)):
        return any(_contains_ray_evidence(item) for item in value)
    return False


def collect_ray_evidence() -> dict[str, Any]:
    """Prove the configured production Ray cluster is reachable for this gate."""
    import ray
    from cube_split import runtime_config

    address = runtime_config.require_ray_address()
    started_here = not ray.is_initialized()
    if started_here:
        ray.init(address=address, ignore_reinit_error=True, include_dashboard=False, logging_level=40)
    try:
        nodes = [node for node in ray.nodes() if node.get("Alive")]
        resources = ray.cluster_resources()
        if not nodes or float(resources.get("CPU", 0)) <= 0:
            raise RuntimeError("configured Ray cluster has no live CPU resources")
        return {"backend": "ray", "live_nodes": len(nodes), "cpu": float(resources.get("CPU", 0))}
    finally:
        if started_here:
            ray.shutdown()


def run_quality_ingest_gate(
    client: HttpClient,
    manifest: dict[str, Any],
    *,
    poll_seconds: float = 2.0,
    timeout_seconds: float = 900.0,
) -> list[dict[str, Any]]:
    """Request quality, validate complete error export, then await auto-ingest."""
    reports: list[dict[str, Any]] = []
    for dataset in manifest["datasets"]:
        dataset_id = str(dataset["dataset_id"])
        quality = client.request("POST", f"/v1/datasets/{dataset_id}/quality-runs")
        quality_run_id = str(quality.get("quality_run_id") or "")
        if not quality_run_id:
            raise RuntimeError(f"quality request did not return an id: {dataset_id}")
        deadline = time.monotonic() + timeout_seconds
        while True:
            quality = client.request("GET", f"/v1/quality/records/{quality_run_id}")
            status = str(quality.get("status") or "").lower()
            if status in {"pass", "warn", "fail", "error", "cancelled"}:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(f"quality run did not finish: {quality_run_id}")
            time.sleep(poll_seconds)
        error_count = int(quality.get("error_count") or 0)
        if error_count:
            exported, headers = client.request_bytes("GET", f"/v1/quality/records/{quality_run_id}/errors/export?format=json")
            if int(headers.get("X-Export-Count", headers.get("x-export-count", "-1"))) != error_count:
                raise RuntimeError(f"quality error export count mismatch: {quality_run_id}")
            if not exported:
                raise RuntimeError(f"quality error export is empty: {quality_run_id}")
        if status not in {"pass", "warn"}:
            raise RuntimeError(f"quality gate rejected dataset {dataset_id}: {status}")
        ingest = _wait_for_dataset_ingest(client, dataset_id, deadline=deadline, poll_seconds=poll_seconds)
        reports.append({"dataset_id": dataset_id, "quality_run_id": quality_run_id, "quality_status": status, "ingest": ingest})
    return reports


def _wait_for_dataset_ingest(client: HttpClient, dataset_id: str, *, deadline: float, poll_seconds: float) -> dict[str, Any]:
    while True:
        page = client.request("GET", f"/v1/ingest-runs?dataset_id={dataset_id}&page_size=20")
        items = page.get("items") or []
        if items:
            latest = items[0]
            status = str(latest.get("status") or "").lower()
            if status in {"completed", "partial_failure", "failed", "cancelled"}:
                if status != "completed":
                    raise RuntimeError(f"ingest gate failed for {dataset_id}: {status}")
                return latest
        if time.monotonic() >= deadline:
            raise TimeoutError(f"auto-ingest did not finish: {dataset_id}")
        time.sleep(poll_seconds)


def verify_publication_lifecycle(
    client: HttpClient,
    dataset_id: str,
    *,
    poll_seconds: float = 2.0,
    timeout_seconds: float = 600.0,
) -> dict[str, Any]:
    created = client.request("POST", f"/v1/datasets/{dataset_id}/publish")
    publication_id = str(created.get("publication_id") or "")
    if not publication_id:
        raise RuntimeError("publication request did not return publication_id")
    active = _wait_for_publication(client, dataset_id, publication_id, {"active", "failed"}, poll_seconds, timeout_seconds)
    if active.get("status") != "active":
        raise RuntimeError(f"publication did not become active: {publication_id}")
    client.request("POST", f"/v1/datasets/{dataset_id}/publications/{publication_id}/withdraw", {"reason": "real acceptance cleanup"})
    withdrawn = _wait_for_publication(client, dataset_id, publication_id, {"withdrawn", "failed"}, poll_seconds, timeout_seconds)
    if withdrawn.get("status") != "withdrawn":
        raise RuntimeError(f"publication did not become withdrawn: {publication_id}")
    return {"publication_id": publication_id, "active": active, "withdrawn": withdrawn}


def _wait_for_publication(
    client: HttpClient,
    dataset_id: str,
    publication_id: str,
    terminal: set[str],
    poll_seconds: float,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while True:
        page = client.request("GET", f"/v1/datasets/{dataset_id}/publications?page_size=100")
        row = next((item for item in page.get("items", []) if str(item.get("publication_id")) == publication_id), None)
        if row is not None and str(row.get("status") or "") in terminal:
            return row
        if time.monotonic() >= deadline:
            raise TimeoutError(f"publication did not reach {sorted(terminal)}: {publication_id}")
        time.sleep(poll_seconds)


def wait_for_task(client: HttpClient, task_id: str, *, poll_seconds: float = 2.0, timeout_seconds: float = 1800.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while True:
        task = client.request("GET", f"/v1/partition/tasks/{task_id}")
        status = str(task.get("status") or "").lower()
        if status in TERMINAL_TASK_STATES:
            return task
        if time.monotonic() >= deadline:
            raise TimeoutError(f"partition task did not finish: {task_id}")
        time.sleep(poll_seconds)


def validate_manifest_contract(manifest: dict[str, Any]) -> dict[str, int]:
    counts = {data_type: 0 for data_type in DATA_TYPES}
    for dataset in manifest["datasets"]:
        counts[dataset["data_type"]] += len(dataset["scenes"])
        for scene in dataset["scenes"]:
            for asset in scene.get("assets", []):
                uri = str(asset.get("source_uri") or asset.get("cog_uri") or "")
                if not uri.startswith("s3://"):
                    raise ValueError(f"asset URI must be s3://: {uri[:80]}")
                if len(str(asset.get("checksum") or "")) != 64:
                    raise ValueError(f"asset checksum is required: {scene.get('scene_id')}")
    if sum(counts.values()) < 4:
        raise ValueError("real acceptance requires at least four scenes")
    return counts


def cleanup_sql(prefix: str) -> tuple[str, ...]:
    """Return parameterized cleanup statements; caller must execute explicitly."""
    if not prefix or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for char in prefix):
        raise ValueError("cleanup prefix must be a simple generated identifier")
    return (
        "DELETE FROM partition_publications WHERE dataset_id LIKE %s",
        "DELETE FROM partition_quality_warn_approvals WHERE dataset_id LIKE %s",
        "DELETE FROM partition_quality_errors WHERE quality_run_id IN (SELECT quality_run_id FROM partition_quality_runs WHERE dataset_id LIKE %s)",
        "DELETE FROM partition_quality_results WHERE quality_run_id IN (SELECT quality_run_id FROM partition_quality_runs WHERE dataset_id LIKE %s)",
        "DELETE FROM partition_quality_runs WHERE dataset_id LIKE %s",
        "DELETE FROM partition_domain_outbox WHERE dataset_id LIKE %s",
        "DELETE FROM ingest_run_scenes WHERE ingest_run_id IN (SELECT ingest_run_id FROM ingest_runs WHERE ingest_run_id LIKE %s)",
        "DELETE FROM ingest_runs WHERE ingest_run_id LIKE %s",
        "DELETE FROM partition_run_scenes WHERE partition_run_id LIKE %s",
        "DELETE FROM partition_runs WHERE partition_run_id LIKE %s",
        "DELETE FROM partition_indexes WHERE dataset_id LIKE %s",
        "DELETE FROM partition_tiles WHERE dataset_id LIKE %s",
        "DELETE FROM partition_grid_cells WHERE dataset_id LIKE %s",
        "DELETE FROM partition_output_versions WHERE dataset_id LIKE %s",
        "DELETE FROM partition_dataset_bands WHERE dataset_id LIKE %s",
        "DELETE FROM partition_dataset_assets WHERE dataset_id LIKE %s",
        "DELETE FROM partition_datasets WHERE dataset_id LIKE %s",
        "DELETE FROM partition_job_attempts WHERE batch_id LIKE %s",
        "DELETE FROM partition_assets WHERE batch_id LIKE %s",
        "DELETE FROM partition_batches WHERE batch_id LIKE %s",
        "DELETE FROM load_batch_scenes WHERE load_batch_id LIKE %s",
        "DELETE FROM load_batches WHERE load_batch_id LIKE %s",
        "DELETE FROM scene_bands WHERE scene_id LIKE %s",
        "DELETE FROM scene_assets WHERE scene_id LIKE %s",
        "DELETE FROM scenes WHERE scene_id LIKE %s",
        "DELETE FROM datasets WHERE dataset_id LIKE %s",
    )


def inspect_database(dsn: str, prefix: str) -> dict[str, int]:
    """Read-only structural checks scoped to this runner's generated prefix."""
    if not prefix or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for char in prefix):
        raise ValueError("inspection prefix must be a simple generated identifier")
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - production dependency
        raise RuntimeError("psycopg is required for OpenGauss acceptance inspection") from exc
    like = prefix + "%"
    queries = {
        "datasets": "SELECT count(*) FROM datasets WHERE dataset_id LIKE %s",
        "scenes": "SELECT count(*) FROM scenes WHERE dataset_id LIKE %s",
        "partition_runs": "SELECT count(*) FROM partition_runs WHERE partition_run_id LIKE %s",
        "ingest_runs": "SELECT count(*) FROM ingest_runs WHERE dataset_id LIKE %s",
        "scene_orphans": "SELECT count(*) FROM scenes s LEFT JOIN datasets d ON d.dataset_id=s.dataset_id WHERE s.dataset_id LIKE %s AND d.dataset_id IS NULL",
        "batch_scene_orphans": "SELECT count(*) FROM load_batch_scenes lbs LEFT JOIN scenes s ON s.scene_id=lbs.scene_id WHERE lbs.load_batch_id LIKE %s AND s.scene_id IS NULL",
        "partition_scene_orphans": "SELECT count(*) FROM partition_run_scenes prs LEFT JOIN scenes s ON s.scene_id=prs.scene_id WHERE prs.partition_run_id LIKE %s AND s.scene_id IS NULL",
        "ingest_scene_orphans": "SELECT count(*) FROM ingest_run_scenes irs JOIN ingest_runs ir ON ir.ingest_run_id=irs.ingest_run_id LEFT JOIN scenes s ON s.scene_id=irs.scene_id WHERE ir.dataset_id LIKE %s AND s.scene_id IS NULL",
        "partition_datasets": "SELECT count(*) FROM partition_datasets WHERE dataset_id LIKE %s",
        "output_versions": "SELECT count(*) FROM partition_output_versions WHERE dataset_id LIKE %s",
        "grid_cells": "SELECT count(*) FROM partition_grid_cells WHERE dataset_id LIKE %s",
        "tiles": "SELECT count(*) FROM partition_tiles WHERE dataset_id LIKE %s",
        "indexes": "SELECT count(*) FROM partition_indexes WHERE dataset_id LIKE %s",
        "quality_runs": "SELECT count(*) FROM partition_quality_runs WHERE dataset_id LIKE %s",
        "completed_scene_without_output": "SELECT count(*) FROM partition_run_scenes WHERE partition_run_id LIKE %s AND status='completed' AND output_version IS NULL",
    }
    counts: dict[str, int] = {}
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            for name, sql in queries.items():
                cursor.execute(sql, (like,))
                counts[name] = int(cursor.fetchone()[0])
    if counts["datasets"] < 6 or counts["scenes"] < 7 or counts["partition_runs"] != EXPECTED_RUN_COUNT:
        raise RuntimeError(f"incomplete acceptance rows: {counts}")
    orphan_keys = ("scene_orphans", "batch_scene_orphans", "partition_scene_orphans", "ingest_scene_orphans", "completed_scene_without_output")
    if any(counts[key] for key in orphan_keys):
        raise RuntimeError(f"acceptance created orphan rows: {counts}")
    for required in ("partition_datasets", "output_versions", "grid_cells", "tiles", "indexes", "quality_runs"):
        if counts[required] == 0:
            raise RuntimeError(f"partition bridge table has no acceptance rows: {required}")
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", default="/tmp/cube-real-acceptance-prepared.json")
    parser.add_argument("--base-url", default=os.getenv("CUBE_WEB_ACCEPTANCE_BASE_URL", "http://127.0.0.1:50039"))
    parser.add_argument("--token", default=os.getenv("CUBE_WEB_ACCEPTANCE_TOKEN"))
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--no-wait", action="store_true")
    args = parser.parse_args(argv)
    prepared = load_prepared(args.manifest)
    if isinstance(prepared.get("datasets"), list):
        derived = prepared
    else:
        from cube_split import runtime_config
        from minio import Minio

        settings = runtime_config.minio_settings()
        if not all((settings.endpoint, settings.bucket, settings.access_key, settings.secret_key)):
            raise RuntimeError("MinIO runtime configuration is incomplete")
        minio = Minio(settings.endpoint, access_key=settings.access_key, secret_key=settings.secret_key, secure=settings.secure)
        carbon = discover_carbon_asset(minio, bucket=settings.bucket, explicit_uri=os.getenv("CUBE_CARBON_SOURCE_URI"))
        derived = derive_manifest(prepared, carbon)
    manifest = namespace_manifest(derived)
    counts = validate_manifest_contract(manifest)
    path = Path("/tmp") / f"{manifest['load_batch_id']}.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.prepare_only:
        print(json.dumps({"status": "prepared", "manifest": str(path), "scene_counts": counts}, ensure_ascii=False))
        return 0
    client = HttpClient(args.base_url, args.token)
    imported = client.request("POST", "/v1/partition/schemas/import", import_payload(manifest))
    runs = submit_grid_runs(client, manifest, wait=not args.no_wait)
    downstream: list[dict[str, Any]] = []
    publication: dict[str, Any] = {}
    controls: dict[str, Any] = {}
    ray_evidence: dict[str, Any] = {}
    if not args.no_wait:
        assert_partition_success(runs)
        ray_evidence = collect_ray_evidence()
        verify_idempotent_submissions(client, manifest, runs)
        controls["cancel"] = probe_task_cancellation(client, manifest)
        downstream = run_quality_ingest_gate(client, manifest)
        controls["quality_warning"] = run_quality_probe(client, manifest, "warn")
        controls["quality_failure"] = run_quality_probe(client, manifest, "fail")
        publication = verify_publication_lifecycle(client, manifest["datasets"][0]["dataset_id"])
    database = {}
    if not args.no_wait:
        from cube_split import runtime_config
        database = inspect_database(runtime_config.postgres_dsn(), manifest["load_batch_id"].removesuffix("-batch"))
    print(json.dumps({"status": "submitted" if args.no_wait else "passed", "import": imported, "runs": runs, "ray_evidence": ray_evidence, "controls": controls, "quality_ingest": downstream, "publication": publication, "database": database, "scene_counts": counts}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
