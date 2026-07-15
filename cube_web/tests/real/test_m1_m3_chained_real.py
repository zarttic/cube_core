"""Non-skipping M1-to-M3 acceptance against configured OpenGauss, MinIO, and Ray."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
import rasterio
from fastapi.testclient import TestClient
from grid_core.app.models.grid_address import GridAddress
from grid_core.sdk import CubeEncoderSDK
from minio import Minio
from rasterio.features import geometry_window
from shapely.geometry import box, shape

from cube_web.app import create_app
from cube_web.routes.auth import Actor
from cube_web.services.partition_contracts import StrictPartitionRequest, make_output_version
from cube_web.services.partition_dataset_runner import NormalizedPartitionDatasetRunner
from cube_web.services.partition_domain_store import OpenGaussPartitionDomainStore, get_partition_domain_store, set_partition_domain_store
from cube_web.services.publication_service import PublishRequest, publish_dataset, withdraw_publication
from cube_web.services.quality_worker import claim_quality_runs, dispatch_quality_events, execute_quality_run

pytestmark = pytest.mark.m1_m3_chain_real

REQUIRED = (
    "CUBE_WEB_POSTGRES_DSN", "CUBE_WEB_RAY_ADDRESS", "CUBE_WEB_MINIO_ENDPOINT", "CUBE_WEB_MINIO_ACCESS_KEY",
    "CUBE_WEB_MINIO_SECRET_KEY", "CUBE_WEB_MINIO_BUCKET", "CUBE_M1_M3_CHAIN_INPUT_MANIFEST", "CUBE_M1_M3_CHAIN_DEFECT_MANIFEST",
)


def _object_key(uri: str, bucket: str) -> str:
    prefix = f"s3://{bucket}/"
    if not uri.startswith(prefix):
        pytest.fail(f"manifest COG is not in configured bucket: {uri}")
    return uri[len(prefix) :]


def _read_object(client: Minio, bucket: str, uri: str) -> bytes:
    response = client.get_object(bucket, _object_key(uri, bucket))
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


@pytest.fixture(scope="module")
def real_runtime() -> dict[str, object]:
    values = {name: os.getenv(name, "").strip() for name in REQUIRED}
    missing = [name for name, value in values.items() if not value]
    if missing:
        pytest.fail("M1-M3 chained gate missing required environment: " + ", ".join(missing))
    root = Path(__file__).resolve().parents[3]
    manifests: list[StrictPartitionRequest] = []
    for name in ("CUBE_M1_M3_CHAIN_INPUT_MANIFEST", "CUBE_M1_M3_CHAIN_DEFECT_MANIFEST"):
        path = Path(values[name]).resolve()
        if root == path or root in path.parents:
            pytest.fail(f"{name} must point outside the repository")
        try:
            manifests.append(StrictPartitionRequest.model_validate_json(path.read_text(encoding="utf-8")))
        except Exception as exc:
            pytest.fail(f"{name} is not a strict partition manifest: {exc}")
    request, defect_request = manifests
    if request.grid_type != "geohash" or request.partition_method != "logical" or request.requested_grid_level <= 0 or len(request.datasets) != 1:
        pytest.fail("chained input manifest must contain one positive-level geohash logical dataset")
    if int(defect_request.datasets[0].attributes.get("m1_m3_expected_error_count", 0)) != 501:
        pytest.fail("chained defect manifest must declare exactly 501 deterministic defects")
    client = Minio(values["CUBE_WEB_MINIO_ENDPOINT"], access_key=values["CUBE_WEB_MINIO_ACCESS_KEY"], secret_key=values["CUBE_WEB_MINIO_SECRET_KEY"], secure=False)
    bucket = values["CUBE_WEB_MINIO_BUCKET"]
    if not client.bucket_exists(bucket):
        pytest.fail(f"configured MinIO bucket does not exist: {bucket}")
    payloads: dict[str, bytes] = {}
    for label, manifest in (("request", request), ("defect_request", defect_request)):
        asset = manifest.datasets[0].assets[0]
        payload = _read_object(client, bucket, str(asset.cog_uri))
        if hashlib.sha256(payload).hexdigest() != asset.checksum:
            pytest.fail(f"manifest checksum does not match MinIO object: {asset.cog_uri}")
        payloads[label] = payload
    try:
        with psycopg.connect(values["CUBE_WEB_POSTGRES_DSN"], connect_timeout=5) as connection:
            assert connection.execute("SELECT 1").fetchone() == (1,)
    except Exception as exc:
        pytest.fail(f"OpenGauss probe failed: {exc}")
    import ray
    from cube_split.jobs.ray_logical_partition_job import _ray_runtime_env_from_env

    try:
        runtime_env = _ray_runtime_env_from_env() or {"env_vars": {}}
        env_vars = dict(runtime_env.get("env_vars") or {})
        env_vars.update({
            "CUBE_WEB_MINIO_ENDPOINT": values["CUBE_WEB_MINIO_ENDPOINT"], "CUBE_WEB_MINIO_ACCESS_KEY": values["CUBE_WEB_MINIO_ACCESS_KEY"],
            "CUBE_WEB_MINIO_SECRET_KEY": values["CUBE_WEB_MINIO_SECRET_KEY"], "CUBE_WEB_MINIO_BUCKET": bucket,
        })
        runtime_env["env_vars"] = env_vars
        if ray.is_initialized():
            ray.shutdown()
        ray.init(address=values["CUBE_WEB_RAY_ADDRESS"], include_dashboard=False, logging_level=40, runtime_env=runtime_env)
        if ray.cluster_resources().get("CPU", 0) <= 0:
            pytest.fail("Ray cluster has no CPU resources")
    except Exception as exc:
        pytest.fail(f"Ray probe failed: {exc}")
    return {"values": values, "request": request, "defect_request": defect_request, "payloads": payloads}


def _verify_m1_three_grid_coverage(payload: bytes) -> None:
    """Exercise the M1 SDK on the exact COG also consumed by the Ray runner."""
    sdk = CubeEncoderSDK()
    with rasterio.MemoryFile(payload) as memory:
        with memory.open() as dataset:
            if str(dataset.crs).upper() != "EPSG:4326":
                pytest.fail(f"chained real COG must use EPSG:4326, got {dataset.crs}")
            bounds = dataset.bounds
            width, height = bounds.right - bounds.left, bounds.top - bounds.bottom
            if width <= 0 or height <= 0:
                pytest.fail("chained real COG has invalid WGS84 bounds")
            aoi = box(bounds.left + width * 0.375, bounds.bottom + height * 0.375, bounds.left + width * 0.625, bounds.bottom + height * 0.625)
            for grid_type, level in (("geohash", 6), ("mgrs", 2), ("isea4h", 6)):
                cells = sdk.cover(grid_type=grid_type, requested_grid_level=level, cover_mode="intersect", boundary_type="polygon", geometry=aoi.__geo_interface__, crs="EPSG:4326")
                assert cells, grid_type
                windows = 0
                for cell in cells:
                    assert cell.grid_level <= level
                    assert (cell.topology_code is not None) if grid_type == "mgrs" else cell.topology_code is None
                    geometry = sdk.code_to_geometry(GridAddress(grid_type=cell.grid_type, grid_level=cell.grid_level, space_code=cell.space_code, topology_code=cell.topology_code))
                    overlap = shape(geometry).intersection(aoi)
                    if overlap.is_empty or overlap.area == 0:
                        continue
                    window = geometry_window(dataset, [overlap.__geo_interface__])
                    assert window.width > 0 and window.height > 0
                    assert window.col_off >= 0 and window.row_off >= 0
                    assert window.col_off + window.width <= dataset.width
                    assert window.row_off + window.height <= dataset.height
                    windows += 1
                assert windows > 0, grid_type


def _insert_attempt(values: dict[str, str], request: StrictPartitionRequest, task_id: str) -> None:
    with psycopg.connect(values["CUBE_WEB_POSTGRES_DSN"]) as connection:
        connection.execute("INSERT INTO partition_batches (batch_id,batch_name,data_type,source_schema,normalized_payload,status) VALUES (%s,%s,'optical','{}'::jsonb,'{}'::jsonb,'running')", (request.batch_id, request.batch_id))
        connection.execute("INSERT INTO partition_job_attempts (task_id,batch_id,asset_ids,operation,status,attempt_no,payload) VALUES (%s,%s,'{}'::text[],'run','running',1,'{}'::jsonb)", (task_id, request.batch_id))


def _cleanup(values: dict[str, str], dataset_id: str, task_id: str, batch_id: str) -> None:
    with psycopg.connect(values["CUBE_WEB_POSTGRES_DSN"]) as connection:
        for table in ("partition_quality_errors", "partition_quality_results", "partition_quality_warn_approvals", "partition_publications", "partition_quality_runs"):
            connection.execute(f"DELETE FROM {table} WHERE dataset_id = %s", (dataset_id,))
        for table in ("partition_domain_outbox", "partition_indexes", "partition_tiles", "partition_grid_cells", "partition_output_versions", "partition_datasets"):
            connection.execute(f"DELETE FROM {table} WHERE dataset_id = %s", (dataset_id,))
        connection.execute("DELETE FROM partition_job_attempts WHERE task_id = %s", (task_id,))
        connection.execute("DELETE FROM partition_batches WHERE batch_id = %s", (batch_id,))


def _create_real_output(real_runtime: dict[str, object], manifest_key: str):
    values = dict(real_runtime["values"])
    base = real_runtime[manifest_key]
    token = uuid4().hex
    payload = base.model_dump(mode="json")
    payload["batch_id"] = f"m1-m3-chain-batch-{token}"
    payload["datasets"][0].update({"dataset_id": f"m1-m3-chain-dataset-{token}", "dataset_code": f"M1M3-{token}", "dataset_title": f"M1-M3 chained real {token}"})
    request = StrictPartitionRequest.model_validate(payload)
    task_id = f"m1-m3-chain-task-{token}"
    output_version = make_output_version(request.datasets[0].dataset_id, task_id)
    store = OpenGaussPartitionDomainStore(dsn=values["CUBE_WEB_POSTGRES_DSN"])
    _insert_attempt(values, request, task_id)
    try:
        assert store.start_output(request, request.datasets[0], task_id) == output_version
        raw = NormalizedPartitionDatasetRunner().run_dataset(dataset=request.datasets[0], task_id=task_id, output_version=output_version, grid_type=request.grid_type, requested_grid_level=request.requested_grid_level, cover_mode=request.cover_mode, max_cells_per_asset=min(request.max_cells_per_asset or 20, 20), time_granularity=request.time_granularity)
        assert store.complete_output(raw)["status"] == "completed"
        with store.transaction() as tx:
            pending = tx.execute("SELECT count(*) FROM partition_domain_outbox WHERE dataset_id = %s AND output_version = %s AND event_type = 'output-version.completed' AND status = 'pending'", (request.datasets[0].dataset_id, output_version)).fetchone()[0]
        assert pending == 1
        assert dispatch_quality_events(worker_id="m1-m3-chain-dispatch") == 1
        with store.transaction() as tx:
            lease = next(item for item in claim_quality_runs(tx, worker_id="m1-m3-chain-quality") if item.quality_run_id)
        execute_quality_run(lease)
        return values, store, request.datasets[0].dataset_id, output_version, lease.quality_run_id, task_id, request.batch_id
    except Exception:
        _cleanup(values, request.datasets[0].dataset_id, task_id, request.batch_id)
        raise


@pytest.fixture
def real_output(real_runtime: dict[str, object]):
    previous = get_partition_domain_store()
    set_partition_domain_store(OpenGaussPartitionDomainStore(dsn=dict(real_runtime["values"])["CUBE_WEB_POSTGRES_DSN"]))
    _verify_m1_three_grid_coverage(real_runtime["payloads"]["request"])
    output = _create_real_output(real_runtime, "request")
    try:
        yield output
    finally:
        _cleanup(output[0], output[2], output[5], output[6])
        set_partition_domain_store(previous)


def test_m1_m2_m3_chain_real_acceptance(real_runtime: dict[str, object], real_output) -> None:
    values, store, dataset_id, output_version, quality_run_id, task_id, batch_id = real_output
    client = TestClient(create_app())
    record = client.get(f"/v1/quality/records/{quality_run_id}")
    results = client.get(f"/v1/quality/records/{quality_run_id}/results?page_size=500")
    errors = client.get(f"/v1/quality/records/{quality_run_id}/errors?page_size=500")
    assert record.status_code == results.status_code == errors.status_code == 200
    assert record.json()["dataset_id"] == dataset_id and record.json()["output_version"] == output_version
    assert results.json()["total"] > 0 and errors.json()["total"] == 0
    with store.transaction() as tx:
        status, complete = tx.execute("SELECT status, result_complete FROM partition_quality_runs WHERE quality_run_id = %s", (quality_run_id,)).fetchone()
    assert (status, complete) == ("pass", True)
    publication = publish_dataset(dataset_id, PublishRequest(), Actor("m1-m3-chain", "admin"))
    assert publication.status == "active"
    withdrawn = withdraw_publication(dataset_id, publication.publication_id, "M1-M3 chained real gate", Actor("m1-m3-chain", "admin"))
    assert withdrawn.status == "withdrawn"
    with store.transaction() as tx:
        history = tx.execute("SELECT status, withdrawn_at FROM partition_publications WHERE publication_id = %s", (publication.publication_id,)).fetchone()
    assert history[0] == "withdrawn" and history[1] is not None

    defect = _create_real_output(real_runtime, "defect_request")
    defect_values, defect_store, defect_dataset_id, _, defect_run_id, defect_task_id, defect_batch_id = defect
    try:
        with defect_store.transaction() as tx:
            full_count = tx.execute("SELECT count(*) FROM partition_quality_errors WHERE quality_run_id = %s", (defect_run_id,)).fetchone()[0]
            filtered_count = tx.execute("SELECT count(*) FROM partition_quality_errors WHERE quality_run_id = %s AND rule_code = 'declared_metadata_defects' AND error_code = 'deterministic_defect'", (defect_run_id,)).fetchone()[0]
        assert full_count == filtered_count == 501
        filters = "rule_code=declared_metadata_defects&error_code=deterministic_defect"
        for format in ("csv", "json"):
            for suffix, expected in (("", full_count), (f"&{filters}", filtered_count)):
                response = client.get(f"/v1/quality/records/{defect_run_id}/errors/export?format={format}{suffix}")
                assert response.status_code == 200 and response.headers["X-Export-Count"] == str(expected)
                assert ("filtered" in response.headers["content-disposition"].lower()) is bool(suffix)
                if format == "csv":
                    assert len(response.content.decode("utf-8-sig").splitlines()) - 1 == expected
                else:
                    assert len(response.json()) == expected
    finally:
        _cleanup(defect_values, defect_dataset_id, defect_task_id, defect_batch_id)
