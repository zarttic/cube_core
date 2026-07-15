#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib import error, request

TERMINAL_TASK_STATUSES = {"completed", "failed", "manual_required", "cancelled"}
TERMINAL_BATCH_STATUSES = {"succeeded", "failed", "manual_required", "cancelled", "archived"}


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _make_jwt(secret: str, *, username: str = "admin", role: str = "admin", ttl_seconds: int = 3600) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "sub": "1",
        "username": username,
        "role": role,
        "exp": int(time.time()) + ttl_seconds,
    }
    header_text = _b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_text = _b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signature = hmac.new(secret.encode("utf-8"), f"{header_text}.{payload_text}".encode("utf-8"), hashlib.sha256).digest()
    return f"{header_text}.{payload_text}.{_b64url(signature)}"


class ApiClient:
    def __init__(self, base_url: str, token: str | None) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token

    def request(self, method: str, path: str, body: Any | None = None, *, auth: bool = True) -> tuple[int, Any]:
        headers = {"Content-Type": "application/json"}
        if auth and self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        data = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
        req = request.Request(f"{self.base_url}{path}", method=method.upper(), headers=headers, data=data)
        try:
            with request.urlopen(req, timeout=60) as resp:
                raw = resp.read().decode("utf-8")
                return resp.status, json.loads(raw) if raw else None
        except error.HTTPError as exc:
            raw = exc.read().decode("utf-8")
            try:
                parsed = json.loads(raw) if raw else {"detail": exc.reason}
            except json.JSONDecodeError:
                parsed = {"detail": raw or exc.reason}
            return exc.code, parsed

    def wait_task(self, task_id: str, *, timeout_seconds: int = 240, interval_seconds: float = 1.0) -> dict[str, Any]:
        deadline = time.time() + timeout_seconds
        last_body: dict[str, Any] | None = None
        while time.time() < deadline:
            status, body = self.request("GET", f"/v1/partition/tasks/{task_id}")
            if status != 200:
                raise RuntimeError(f"task lookup failed: {task_id} -> {status} {body}")
            assert isinstance(body, dict)
            last_body = body
            if str(body.get("status") or "") in TERMINAL_TASK_STATUSES:
                return body
            time.sleep(interval_seconds)
        raise TimeoutError(f"task did not reach terminal state: {task_id} last={last_body}")

    def wait_batch(self, batch_id: str, *, timeout_seconds: int = 240, interval_seconds: float = 1.0) -> dict[str, Any]:
        deadline = time.time() + timeout_seconds
        last_body: dict[str, Any] | None = None
        while time.time() < deadline:
            status, body = self.request("GET", f"/v1/partition/batches/{batch_id}")
            if status != 200:
                raise RuntimeError(f"batch lookup failed: {batch_id} -> {status} {body}")
            assert isinstance(body, dict)
            last_body = body
            if str(body.get("status") or "") in TERMINAL_BATCH_STATUSES:
                return body
            time.sleep(interval_seconds)
        raise TimeoutError(f"batch did not reach terminal state: {batch_id} last={last_body}")


def _optical_asset(source_uri: str, scene_id: str, *, asset_id: str | None = None) -> dict[str, Any]:
    asset = {
        "source_uri": source_uri,
        "scene_id": scene_id,
        "acq_time": "2026-01-01T00:00:00Z",
        "bands": ["sr_band1"],
        "band": "sr_band1",
        "corners": [[116.0, 40.0], [116.096, 40.0], [116.096, 39.904], [116.0, 39.904]],
        "resolution": 30.0,
        "sensor": "optical_mosaic",
        "product_family": "other",
    }
    if asset_id:
        asset["asset_id"] = asset_id
    return asset


def _radar_asset(source_uri: str, scene_id: str) -> dict[str, Any]:
    return {
        "source_uri": source_uri,
        "scene_id": scene_id,
        "acq_time": "2026-01-01T00:00:00Z",
        "bands": ["vv"],
        "band": "vv",
        "corners": [[119.35, 32.45], [119.446, 32.45], [119.446, 32.354000000000006], [119.35, 32.354000000000006]],
        "resolution": 30.0,
        "sensor": "sentinel1_sar",
        "product_family": "sentinel1",
        "polarization": "vv",
    }


def _product_asset(source_uri: str, scene_id: str) -> dict[str, Any]:
    return {
        "source_uri": source_uri,
        "scene_id": scene_id,
        "acq_time": "2026-01-01T00:00:00Z",
        "bands": ["product_value"],
        "band": "product_value",
        "corners": [[100.0, 27.0], [105.0, 27.0], [105.0, 23.0], [100.0, 23.0]],
        "resolution": 10.0,
        "sensor": "data_product",
        "product_family": "product",
        "product_name": "test_product",
        "product_year": 2026,
    }


def _carbon_observation(source_uri: str, observation_id: str) -> dict[str, Any]:
    return {
        "source_uri": source_uri,
        "observation_id": observation_id,
        "acq_time": "2026-01-01T00:00:00Z",
        "resolution": 10,
        "sensor": "oco2",
        "product_family": "xco2",
        "lon": 100.0,
        "lat": 25.0,
    }


def _default_sources(smoke_summary: Path | None) -> dict[str, str]:
    if smoke_summary and smoke_summary.exists():
        summary = json.loads(smoke_summary.read_text(encoding="utf-8"))
        prefix = str(summary.get("prefix") or "").strip("/")
        carbon_source = str(summary.get("carbon_source_uri") or "").strip()
        if prefix and carbon_source:
            return {
                "optical": f"s3://cube/{prefix}/sources/optical/Shandong_mosaic_2026Q1_sr_band1.tif",
                "radar": f"s3://cube/{prefix}/sources/radar/20260101_VV.tif",
                "product": f"s3://cube/{prefix}/sources/product/smoke_product_2026.tif",
                "carbon": carbon_source,
            }
    return {
        "optical": "s3://cube/cube/smoke/all_partition_flows/20260704165521/sources/optical/Shandong_mosaic_2026Q1_sr_band1.tif",
        "radar": "s3://cube/cube/smoke/all_partition_flows/20260704165521/sources/radar/20260101_VV.tif",
        "product": "s3://cube/cube/smoke/all_partition_flows/20260704165521/sources/product/smoke_product_2026.tif",
        "carbon": "s3://cube/cube/smoke/all_partition_flows/20260704165521/sources/carbon/oco2_LtCO2_201231_B11014Ar_220729012824s.nc4",
    }


def _missing_variant(uri: str) -> str:
    if ".tif" in uri:
        return uri.rsplit("/", 1)[0] + "/missing-does-not-exist.tif"
    if ".nc4" in uri:
        return uri.rsplit("/", 1)[0] + "/missing-does-not-exist.nc4"
    return uri.rsplit("/", 1)[0] + "/missing-does-not-exist"


def _raster_runtime_payload(
    *,
    run_id: str,
    batch_id: str,
    data_type: str,
    grid_type: str,
    grid_level: int,
    sensor: str,
    dataset: str,
    time_granularity: str,
    selected_assets: list[dict[str, Any]],
    ray_address: str,
    minio_endpoint: str,
    minio_bucket: str,
    product_name: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "job_id": batch_id,
        "sensor": sensor,
        "dataset": dataset,
        "batch_id": batch_id,
        "batch_name": batch_id,
        "grid_type": grid_type,
        "chunk_size": 1,
        "cover_mode": "intersect",
        "grid_level": grid_level,
        "target_crs": "EPSG:4326",
        "cog_workers": 1,
        "ray_address": ray_address,
        "cube_version": f"cube-{run_id}",
        "minio_bucket": minio_bucket,
        "minio_prefix": f"cube/api_web_regression/{run_id}/outputs",
        "minio_secure": False,
        "asset_version": f"asset-{run_id}",
        "cog_overwrite": True,
        "minio_endpoint": minio_endpoint,
        "grid_level_mode": "manual",
        "ray_parallelism": 2,
        "selected_assets": selected_assets,
        "metadata_backend": "postgres",
        "time_granularity": time_granularity,
        "partition_backend": "ray",
        "max_cells_per_asset": 50,
        "minio_upload_workers": 2,
        "partition_prefix_len": 2,
        "asset_storage_backend": "minio",
    }
    if product_name:
        payload["product_name"] = product_name
    return payload


def _carbon_runtime_payload(
    *,
    batch_id: str,
    grid_type: str,
    grid_level: int,
    ray_address: str,
    source_uri: str | None = None,
    selected_observations: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "job_id": batch_id,
        "batch_id": batch_id,
        "batch_name": batch_id,
        "grid_type": grid_type,
        "grid_level": grid_level,
        "ray_address": ray_address,
        "product_type": "xco2",
        "ray_parallelism": 2,
        "max_observations": 1,
        "metadata_backend": "postgres",
        "time_granularity": "day",
        "partition_backend": "ray",
        "partition_workers": 2,
        "partition_chunk_size": 250,
    }
    if source_uri:
        payload["source_uri"] = source_uri
    if selected_observations:
        payload["selected_observations"] = selected_observations
    return payload


def _quality_fetch_case(client: ApiClient, data_type: str, report_id: str) -> dict[str, Any]:
    status, body = client.request("POST", f"/v1/quality/{data_type}/report", {"report_id": report_id})
    if status != 200:
        raise AssertionError(f"quality report fetch failed for {data_type} {report_id}: {status} {body}")
    return {
        "status_code": status,
        "report_status": None if not isinstance(body, dict) else body.get("status"),
        "report_id": report_id,
    }


def _run_direct_case(
    client: ApiClient,
    data_type: str,
    payload: dict[str, Any],
    *,
    expect_status: str,
    timeout_seconds: int = 90,
) -> dict[str, Any]:
    status_code, submit = client.request("POST", f"/v1/partition/{data_type}/tasks/run", payload)
    if status_code != 202:
        raise RuntimeError(f"submit failed for {data_type}: {status_code} {submit}")
    assert isinstance(submit, dict)
    task_id = str(submit["task_id"])
    task = client.wait_task(task_id, timeout_seconds=timeout_seconds)
    if str(task.get("status")) != expect_status:
        raise AssertionError(f"{data_type} task {task_id} expected {expect_status}, got {task.get('status')}")
    batch_id = (
        (task.get("result") or {}).get("batch_id")
        if isinstance(task.get("result"), dict)
        else payload.get("batch_id")
    )
    batch: dict[str, Any] | None = None
    if batch_id:
        batch_status_code, batch_body = client.request("GET", f"/v1/partition/batches/{batch_id}")
        if batch_status_code != 200:
            raise RuntimeError(f"batch fetch failed for {data_type} {batch_id}: {batch_status_code} {batch_body}")
        if isinstance(batch_body, dict):
            batch = batch_body
    result: dict[str, Any] = {
        "task_id": task_id,
        "submit_status": status_code,
        "status": task.get("status"),
        "batch_id": batch_id,
        "batch_status": None if batch is None else batch.get("status"),
    }
    if expect_status == "completed":
        task_result = task.get("result") or {}
        result.update(
            {
                "grid_type": task_result.get("grid_type"),
                "grid_level": task_result.get("grid_level"),
                "rows": task_result.get("rows"),
                "execution_engine": task_result.get("execution_engine"),
                "quality_report_id": task_result.get("quality_report_id"),
                "quality_status": (task_result.get("quality_report") or {}).get("status"),
            }
        )
        if not isinstance(result.get("rows"), int) or int(result["rows"]) <= 0:
            raise AssertionError(f"{data_type} expected rows > 0, got {result.get('rows')}")
        if result.get("execution_engine") != "ray":
            raise AssertionError(f"{data_type} expected execution_engine=ray, got {result.get('execution_engine')}")
        if result.get("quality_status") != "PASS":
            raise AssertionError(f"{data_type} expected quality PASS, got {result.get('quality_status')}")
        if result.get("batch_status") != "succeeded":
            raise AssertionError(f"{data_type} expected batch succeeded, got {result.get('batch_status')}")
        report_id = task_result.get("quality_report_id")
        if not report_id:
            raise AssertionError(f"{data_type} expected quality_report_id")
        if report_id:
            result["quality_fetch"] = _quality_fetch_case(client, data_type, str(report_id))
            if result["quality_fetch"].get("report_status") != "PASS":
                raise AssertionError(
                    f"{data_type} expected quality report PASS, got {result['quality_fetch'].get('report_status')}"
                )
    else:
        result["error_snippet"] = str(task.get("error") or "")[:300]
        batch_id = str(result["batch_id"] or payload.get("batch_id") or "")
        if batch_id:
            batch_status, batch = client.request("GET", f"/v1/partition/batches/{batch_id}")
            attempts_status, attempts = client.request("GET", f"/v1/partition/batches/{batch_id}/attempts")
            assets_status, assets = client.request("GET", f"/v1/partition/batches/{batch_id}/assets")
            result["batch_status_code"] = batch_status
            result["attempts_status_code"] = attempts_status
            result["assets_status_code"] = assets_status
            if isinstance(batch, dict):
                result["batch_status"] = batch.get("status")
            if isinstance(attempts, dict):
                top_attempt = (attempts.get("attempts") or [{}])[0]
                result["attempt_error_type"] = top_attempt.get("error_type")
            if isinstance(assets, dict):
                result["asset_statuses"] = {
                    str(item.get("asset_id")): str(item.get("status"))
                    for item in (assets.get("assets") or [])
                }
            if result.get("batch_status") != "manual_required":
                raise AssertionError(f"{data_type} batch expected manual_required, got {result.get('batch_status')}")
            if result.get("attempt_error_type") != "source_missing":
                raise AssertionError(
                    f"{data_type} attempt expected source_missing, got {result.get('attempt_error_type')}"
                )
    return result


def _run_mixed_batch_case(client: ApiClient, batch_id: str, good_source: str, missing_source: str) -> dict[str, Any]:
    import_payload = {
        "batch_id": batch_id,
        "batch_name": batch_id,
        "data_type": "optical",
        "assets": [
            _optical_asset(good_source, f"{batch_id}-good", asset_id="asset-good"),
            _optical_asset(missing_source, f"{batch_id}-missing", asset_id="asset-missing"),
        ],
    }
    import_status, import_body = client.request("POST", "/v1/partition/schemas/import", import_payload)
    if import_status != 200:
        raise RuntimeError(f"schema import failed for {batch_id}: {import_status} {import_body}")
    run_status, submit = client.request("POST", f"/v1/partition/batches/{batch_id}/run", {})
    if run_status != 202:
        raise RuntimeError(f"batch run failed for {batch_id}: {run_status} {submit}")
    assert isinstance(submit, dict)
    task = client.wait_task(str(submit["task_id"]))
    batch = client.wait_batch(batch_id)
    _, assets = client.request("GET", f"/v1/partition/batches/{batch_id}/assets")
    _, attempts = client.request("GET", f"/v1/partition/batches/{batch_id}/attempts")
    asset_statuses = {
        str(item.get("source_uri")): str(item.get("status"))
        for item in (assets.get("assets") or [])
    } if isinstance(assets, dict) else {}
    first_attempt = (attempts.get("attempts") or [{}])[0] if isinstance(attempts, dict) else {}
    if str(task.get("status")) != "manual_required":
        raise AssertionError(f"mixed batch task expected manual_required, got {task.get('status')}")
    if str(batch.get("status")) != "manual_required":
        raise AssertionError(f"mixed batch expected manual_required, got {batch.get('status')}")
    if asset_statuses != {good_source: "pending", missing_source: "manual_required"}:
        raise AssertionError(f"unexpected mixed batch asset statuses: {asset_statuses}")
    return {
        "task_id": submit["task_id"],
        "task_status": task.get("status"),
        "batch_status": batch.get("status"),
        "attempt_error_type": first_attempt.get("error_type"),
        "asset_statuses": asset_statuses,
    }


def _record_case(results: list[dict[str, Any]], name: str, fn) -> None:
    started = time.time()
    case: dict[str, Any] = {"name": name}
    try:
        case["status"] = "pass"
        case["details"] = fn()
    except Exception as exc:  # noqa: BLE001
        case["status"] = "fail"
        case["error"] = str(exc)
    case["elapsed_sec"] = round(time.time() - started, 3)
    results.append(case)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Web API partition regression against live infra.")
    parser.add_argument("--base-url", default="http://127.0.0.1:50039")
    parser.add_argument("--env-file", default=".cube_web.env")
    parser.add_argument("--smoke-summary", default="/tmp/cube_partition_flow_smoke_summary_20260704.json")
    parser.add_argument("--output", default=f"/tmp/cube_web_partition_regression_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    args = parser.parse_args()

    _load_env_file(Path(args.env_file))
    auth_secret = os.environ.get("CUBE_WEB_AUTH_JWT_SECRET_KEY")
    if not auth_secret:
        print("CUBE_WEB_AUTH_JWT_SECRET_KEY is required", file=sys.stderr)
        return 2

    token = _make_jwt(auth_secret)
    client = ApiClient(args.base_url, token)
    smoke_summary = Path(args.smoke_summary)
    sources = _default_sources(smoke_summary if smoke_summary.exists() else None)
    ray_address = os.environ.get("CUBE_WEB_RAY_ADDRESS", "").strip()
    minio_endpoint = os.environ.get("CUBE_WEB_MINIO_ENDPOINT", "").strip()
    minio_bucket = os.environ.get("CUBE_WEB_MINIO_BUCKET", "").strip()
    if not ray_address or not minio_endpoint or not minio_bucket:
        print("CUBE_WEB_RAY_ADDRESS, CUBE_WEB_MINIO_ENDPOINT, and CUBE_WEB_MINIO_BUCKET are required", file=sys.stderr)
        return 2

    now = datetime.now().strftime("%Y%m%d%H%M%S")
    run_id = f"webapi-regression-{now}"
    results: list[dict[str, Any]] = []

    _record_case(
        results,
        "auth_required",
        lambda: (
            lambda response: (
                {"status_code": response[0], "response": response[1]}
                if response[0] == 401
                else (_ for _ in ()).throw(AssertionError(f"expected 401, got {response[0]} {response[1]}"))
            )
        )(
            client.request(
                "POST",
                "/v1/partition/optical/tasks/run",
                {"batch_id": f"{run_id}-auth-missing"},
                auth=False,
            )
        ),
    )
    _record_case(
        results,
        "unknown_data_type",
        lambda: (
            lambda response: (
                {"status_code": response[0], "response": response[1]}
                if response[0] == 404
                else (_ for _ in ()).throw(AssertionError(f"expected 404, got {response[0]} {response[1]}"))
            )
        )(
            client.request(
                "POST",
                "/v1/partition/unknown/tasks/run",
                {"batch_id": f"{run_id}-unknown-type"},
            )
        ),
    )

    success_cases: list[tuple[str, str, dict[str, Any]]] = [
        (
            "optical_geohash_success",
            "optical",
            _raster_runtime_payload(
                run_id=run_id,
                batch_id=f"{run_id}-optical-geohash",
                data_type="optical",
                grid_type="geohash",
                grid_level=5,
                sensor="optical_mosaic",
                dataset=f"{run_id.replace('-', '_')}_optical",
                time_granularity="day",
                selected_assets=[_optical_asset(sources["optical"], "apiweb_optical_2026q1")],
                ray_address=ray_address,
                minio_endpoint=minio_endpoint,
                minio_bucket=minio_bucket,
            ),
        ),
        (
            "optical_mgrs_success",
            "optical",
            _raster_runtime_payload(
                run_id=run_id,
                batch_id=f"{run_id}-optical-mgrs",
                data_type="optical",
                grid_type="mgrs",
                grid_level=3,
                sensor="optical_mosaic",
                dataset=f"{run_id.replace('-', '_')}_optical",
                time_granularity="day",
                selected_assets=[_optical_asset(sources["optical"], "apiweb_optical_2026q1")],
                ray_address=ray_address,
                minio_endpoint=minio_endpoint,
                minio_bucket=minio_bucket,
            ),
        ),
        (
            "optical_isea4h_success",
            "optical",
            _raster_runtime_payload(
                run_id=run_id,
                batch_id=f"{run_id}-optical-isea4h",
                data_type="optical",
                grid_type="isea4h",
                grid_level=1,
                sensor="optical_mosaic",
                dataset=f"{run_id.replace('-', '_')}_optical",
                time_granularity="day",
                selected_assets=[_optical_asset(sources["optical"], "apiweb_optical_2026q1")],
                ray_address=ray_address,
                minio_endpoint=minio_endpoint,
                minio_bucket=minio_bucket,
            ),
        ),
        (
            "radar_geohash_success",
            "radar",
            _raster_runtime_payload(
                run_id=run_id,
                batch_id=f"{run_id}-radar-geohash",
                data_type="radar",
                grid_type="geohash",
                grid_level=5,
                sensor="sentinel1_sar",
                dataset=f"{run_id.replace('-', '_')}_radar",
                time_granularity="day",
                selected_assets=[_radar_asset(sources["radar"], "APIWEB_S1_20260101")],
                ray_address=ray_address,
                minio_endpoint=minio_endpoint,
                minio_bucket=minio_bucket,
            ),
        ),
        (
            "product_geohash_success",
            "product",
            _raster_runtime_payload(
                run_id=run_id,
                batch_id=f"{run_id}-product-geohash",
                data_type="product",
                grid_type="geohash",
                grid_level=5,
                sensor="data_product",
                dataset=f"{run_id.replace('-', '_')}_product",
                time_granularity="year",
                selected_assets=[_product_asset(sources["product"], "apiweb_product_2026")],
                ray_address=ray_address,
                minio_endpoint=minio_endpoint,
                minio_bucket=minio_bucket,
                product_name="apiweb_product",
            ),
        ),
        (
            "carbon_isea4h_success",
            "carbon",
            _carbon_runtime_payload(
                batch_id=f"{run_id}-carbon-isea4h",
                grid_type="isea4h",
                grid_level=5,
                ray_address=ray_address,
                source_uri=sources["carbon"],
            ),
        ),
    ]
    for name, data_type, payload in success_cases:
        _record_case(results, name, lambda dt=data_type, pl=payload: _run_direct_case(client, dt, pl, expect_status="completed"))

    failure_cases: list[tuple[str, str, dict[str, Any]]] = [
        (
            "optical_missing_source_direct",
            "optical",
            _raster_runtime_payload(
                run_id=run_id,
                batch_id=f"{run_id}-optical-missing",
                data_type="optical",
                grid_type="geohash",
                grid_level=5,
                sensor="optical_mosaic",
                dataset=f"{run_id.replace('-', '_')}_optical",
                time_granularity="day",
                selected_assets=[_optical_asset(_missing_variant(sources["optical"]), "apiweb_optical_missing")],
                ray_address=ray_address,
                minio_endpoint=minio_endpoint,
                minio_bucket=minio_bucket,
            ),
        ),
        (
            "radar_missing_source_direct",
            "radar",
            _raster_runtime_payload(
                run_id=run_id,
                batch_id=f"{run_id}-radar-missing",
                data_type="radar",
                grid_type="geohash",
                grid_level=5,
                sensor="sentinel1_sar",
                dataset=f"{run_id.replace('-', '_')}_radar",
                time_granularity="day",
                selected_assets=[_radar_asset(_missing_variant(sources["radar"]), "APIWEB_S1_MISSING")],
                ray_address=ray_address,
                minio_endpoint=minio_endpoint,
                minio_bucket=minio_bucket,
            ),
        ),
        (
            "product_missing_source_direct",
            "product",
            _raster_runtime_payload(
                run_id=run_id,
                batch_id=f"{run_id}-product-missing",
                data_type="product",
                grid_type="geohash",
                grid_level=5,
                sensor="data_product",
                dataset=f"{run_id.replace('-', '_')}_product",
                time_granularity="year",
                selected_assets=[_product_asset(_missing_variant(sources["product"]), "apiweb_product_missing")],
                ray_address=ray_address,
                minio_endpoint=minio_endpoint,
                minio_bucket=minio_bucket,
                product_name="apiweb_product",
            ),
        ),
        (
            "carbon_missing_source_direct",
            "carbon",
            _carbon_runtime_payload(
                batch_id=f"{run_id}-carbon-missing",
                grid_type="isea4h",
                grid_level=5,
                ray_address=ray_address,
                selected_observations=[_carbon_observation(_missing_variant(sources["carbon"]), f"{run_id}-carbon-missing")],
            ),
        ),
    ]
    for name, data_type, payload in failure_cases:
        _record_case(results, name, lambda dt=data_type, pl=payload: _run_direct_case(client, dt, pl, expect_status="manual_required"))

    _record_case(
        results,
        "optical_mixed_batch_partial_failure",
        lambda: _run_mixed_batch_case(
            client,
            f"{run_id}-optical-mixed-batch",
            sources["optical"],
            _missing_variant(sources["optical"]),
        ),
    )

    summary = {
        "run_id": run_id,
        "base_url": args.base_url,
        "generated_at": datetime.now().isoformat(),
        "sources": sources,
        "results": results,
        "passed": sum(1 for item in results if item["status"] == "pass"),
        "failed": sum(1 for item in results if item["status"] == "fail"),
    }

    output_path = Path(args.output)
    output_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(output_path)
    print(json.dumps({"passed": summary["passed"], "failed": summary["failed"]}, ensure_ascii=False))
    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
