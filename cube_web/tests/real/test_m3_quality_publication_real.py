"""Non-skipping M3 acceptance against configured OpenGauss, MinIO, and Ray."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient
from minio import Minio

from cube_web.app import create_app
from cube_web.routes.auth import Actor
from cube_web.services.partition_contracts import StrictPartitionRequest, make_output_version
from cube_web.services.partition_dataset_runner import NormalizedPartitionDatasetRunner
from cube_web.services.partition_domain_store import OpenGaussPartitionDomainStore, get_partition_domain_store, set_partition_domain_store
from cube_web.services.publication_service import PublishRequest, publish_dataset, withdraw_publication
from cube_web.services.quality_worker import claim_quality_runs, dispatch_quality_events, execute_quality_run

pytestmark = pytest.mark.m3_real

REQUIRED = (
    "CUBE_WEB_POSTGRES_DSN",
    "CUBE_WEB_RAY_ADDRESS",
    "CUBE_WEB_MINIO_ENDPOINT",
    "CUBE_WEB_MINIO_ACCESS_KEY",
    "CUBE_WEB_MINIO_SECRET_KEY",
    "CUBE_WEB_MINIO_BUCKET",
    "CUBE_M3_REAL_INPUT_MANIFEST",
    "CUBE_M3_REAL_DEFECT_MANIFEST",
)


@pytest.fixture(scope="module")
def real_runtime() -> dict[str, object]:
    values = {name: os.getenv(name, "").strip() for name in REQUIRED}
    missing = [name for name, value in values.items() if not value]
    if missing:
        pytest.fail("M3 real gate missing required environment: " + ", ".join(missing))
    root = Path(__file__).resolve().parents[3]
    manifests: list[StrictPartitionRequest] = []
    for name in ("CUBE_M3_REAL_INPUT_MANIFEST", "CUBE_M3_REAL_DEFECT_MANIFEST"):
        path = Path(values[name]).resolve()
        if root == path or root in path.parents:
            pytest.fail(f"{name} must point outside the repository")
        try:
            manifests.append(StrictPartitionRequest.model_validate_json(path.read_text(encoding="utf-8")))
        except Exception as exc:
            pytest.fail(f"{name} is not a strict partition manifest: {exc}")
    request, defect_request = manifests
    if request.grid_type != "geohash" or request.partition_method != "logical" or request.requested_grid_level <= 0 or len(request.datasets) != 1:
        pytest.fail("M3 input manifest must contain one positive-level geohash logical dataset")
    if int(defect_request.datasets[0].attributes.get("m3_expected_error_count", 0)) < 501:
        pytest.fail("M3 defect manifest must declare at least 501 deterministic defects")
    client = Minio(
        values["CUBE_WEB_MINIO_ENDPOINT"],
        access_key=values["CUBE_WEB_MINIO_ACCESS_KEY"],
        secret_key=values["CUBE_WEB_MINIO_SECRET_KEY"],
        secure=False,
    )
    bucket = values["CUBE_WEB_MINIO_BUCKET"]
    if not client.bucket_exists(bucket):
        pytest.fail(f"configured MinIO bucket does not exist: {bucket}")
    for manifest in manifests:
        for asset in manifest.datasets[0].assets:
            uri = str(asset.cog_uri)
            prefix = f"s3://{bucket}/"
            if not uri.startswith(prefix):
                pytest.fail(f"manifest COG is not in configured bucket: {uri}")
            response = client.get_object(bucket, uri[len(prefix) :])
            try:
                checksum = hashlib.sha256(response.read()).hexdigest()
            finally:
                response.close()
                response.release_conn()
            if checksum != asset.checksum:
                pytest.fail(f"manifest checksum does not match MinIO object: {uri}")
    try:
        with psycopg.connect(values["CUBE_WEB_POSTGRES_DSN"], connect_timeout=5) as connection:
            assert connection.execute("SELECT 1").fetchone() == (1,)
    except Exception as exc:
        pytest.fail(f"OpenGauss probe failed: {exc}")
    import ray

    try:
        from cube_split.jobs.ray_logical_partition_job import _ray_runtime_env_from_env

        runtime_env = _ray_runtime_env_from_env() or {"env_vars": {}}
        env_vars = dict(runtime_env.get("env_vars") or {})
        env_vars.update({
            "CUBE_WEB_MINIO_ENDPOINT": values["CUBE_WEB_MINIO_ENDPOINT"],
            "CUBE_WEB_MINIO_ACCESS_KEY": values["CUBE_WEB_MINIO_ACCESS_KEY"],
            "CUBE_WEB_MINIO_SECRET_KEY": values["CUBE_WEB_MINIO_SECRET_KEY"],
            "CUBE_WEB_MINIO_BUCKET": values["CUBE_WEB_MINIO_BUCKET"],
        })
        runtime_env["env_vars"] = env_vars
        if ray.is_initialized():
            ray.shutdown()
        ray.init(address=values["CUBE_WEB_RAY_ADDRESS"], include_dashboard=False, logging_level=40, runtime_env=runtime_env)
        if ray.cluster_resources().get("CPU", 0) <= 0:
            pytest.fail("Ray cluster has no CPU resources")
    except Exception as exc:
        pytest.fail(f"Ray probe failed: {exc}")
    return {"values": values, "request": request, "defect_request": defect_request, "client": client}


def _insert_attempt(values: dict[str, str], request: StrictPartitionRequest, task_id: str) -> None:
    with psycopg.connect(values["CUBE_WEB_POSTGRES_DSN"]) as connection:
        connection.execute(
            "INSERT INTO partition_batches (batch_id,batch_name,data_type,source_schema,normalized_payload,status) VALUES (%s,%s,'optical','{}'::jsonb,'{}'::jsonb,'running')",
            (request.batch_id, request.batch_id),
        )
        connection.execute(
            "INSERT INTO partition_job_attempts (task_id,batch_id,asset_ids,operation,status,attempt_no,payload) VALUES (%s,%s,'{}'::text[],'run','running',1,'{}'::jsonb)",
            (task_id, request.batch_id),
        )


def _cleanup(values: dict[str, str], dataset_id: str, task_id: str, batch_id: str) -> None:
    with psycopg.connect(values["CUBE_WEB_POSTGRES_DSN"]) as connection:
        for table in ("partition_quality_errors", "partition_quality_results", "partition_quality_warn_approvals", "partition_publications", "partition_quality_runs"):
            connection.execute(f"DELETE FROM {table} WHERE dataset_id = %s", (dataset_id,))
        connection.execute("DELETE FROM partition_domain_outbox WHERE dataset_id = %s", (dataset_id,))
        connection.execute("DELETE FROM partition_indexes WHERE dataset_id = %s", (dataset_id,))
        connection.execute("DELETE FROM partition_tiles WHERE dataset_id = %s", (dataset_id,))
        connection.execute("DELETE FROM partition_grid_cells WHERE dataset_id = %s", (dataset_id,))
        connection.execute("DELETE FROM partition_output_versions WHERE dataset_id = %s", (dataset_id,))
        connection.execute("DELETE FROM partition_datasets WHERE dataset_id = %s", (dataset_id,))
        connection.execute("DELETE FROM partition_job_attempts WHERE task_id = %s", (task_id,))
        connection.execute("DELETE FROM partition_batches WHERE batch_id = %s", (batch_id,))


def _create_real_output(real_runtime: dict[str, object], manifest_key: str):
    values = dict(real_runtime["values"])
    base = real_runtime[manifest_key]
    token = uuid4().hex
    payload = base.model_dump(mode="json")
    payload["batch_id"] = f"m3-real-batch-{token}"
    payload["datasets"][0]["dataset_id"] = f"m3-real-dataset-{token}"
    payload["datasets"][0]["dataset_code"] = f"M3-{token}"
    payload["datasets"][0]["dataset_title"] = f"M3 real {token}"
    request = StrictPartitionRequest.model_validate(payload)
    task_id = f"m3-real-task-{token}"
    output_version = make_output_version(request.datasets[0].dataset_id, task_id)
    store = OpenGaussPartitionDomainStore(dsn=values["CUBE_WEB_POSTGRES_DSN"])
    _insert_attempt(values, request, task_id)
    try:
        assert store.start_output(request, request.datasets[0], task_id) == output_version
        raw = NormalizedPartitionDatasetRunner().run_dataset(
            dataset=request.datasets[0], task_id=task_id, output_version=output_version,
            grid_type=request.grid_type, requested_grid_level=request.requested_grid_level,
            cover_mode=request.cover_mode, max_cells_per_asset=min(request.max_cells_per_asset or 20, 20), time_granularity=request.time_granularity,
        )
        committed = store.complete_output(raw)
        assert committed["status"] == "completed"
        assert dispatch_quality_events(worker_id="m3-real-dispatch") == 1
        with store.transaction() as tx:
            lease = next(item for item in claim_quality_runs(tx, worker_id="m3-real-quality") if item.quality_run_id)
        execute_quality_run(lease)
        return values, store, request.datasets[0].dataset_id, output_version, lease.quality_run_id, task_id, request.batch_id
    except Exception:
        _cleanup(values, request.datasets[0].dataset_id, task_id, request.batch_id)
        raise


@pytest.fixture
def real_output(real_runtime: dict[str, object]):
    previous = get_partition_domain_store()
    set_partition_domain_store(OpenGaussPartitionDomainStore(dsn=dict(real_runtime["values"])["CUBE_WEB_POSTGRES_DSN"]))
    output = _create_real_output(real_runtime, "request")
    try:
        yield output
    finally:
        _cleanup(output[0], output[2], output[5], output[6])
        set_partition_domain_store(previous)


def test_real_dependencies_are_required(real_runtime: dict[str, object]) -> None:
    assert set(REQUIRED) <= set(real_runtime["values"])


def test_real_ray_quality_exports_and_publication_history(real_output, real_runtime: dict[str, object]) -> None:
    values, store, dataset_id, output_version, automatic_run_id, task_id, batch_id = real_output
    with store.transaction() as tx:
        row = tx.execute("SELECT status, result_complete FROM partition_quality_runs WHERE quality_run_id = %s", (automatic_run_id,)).fetchone()
        assert row == ("pass", True)
    publication = publish_dataset(dataset_id, PublishRequest(), Actor("m3-real", "admin"))
    assert publication.status == "active"
    withdrawn = withdraw_publication(dataset_id, publication.publication_id, "M3 real gate", Actor("m3-real", "admin"))
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
        client = TestClient(create_app())
        filters = "rule_code=declared_metadata_defects&error_code=deterministic_defect"
        for format in ("csv", "json"):
            for suffix, expected in (("", full_count), (f"&{filters}", filtered_count)):
                response = client.get(f"/v1/quality/records/{defect_run_id}/errors/export?format={format}{suffix}")
                assert response.status_code == 200
                assert response.headers["X-Export-Count"] == str(expected)
                assert ("filtered" in response.headers["content-disposition"].lower()) is bool(suffix)
                if format == "csv":
                    assert len(response.content.decode("utf-8-sig").splitlines()) - 1 == expected
                else:
                    assert len(response.json()) == expected
    finally:
        _cleanup(defect_values, defect_dataset_id, defect_task_id, defect_batch_id)
