"""Exactly six non-skipping M5 scenarios against OpenGauss, MinIO, and Ray."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import unquote, urlparse
from uuid import uuid4

import psycopg
import pytest
from cube_web.routes.auth import Actor
from cube_web.services.partition_contracts import StrictPartitionRequest
from cube_web.services.partition_dataset_runner import NormalizedPartitionDatasetRunner
from cube_web.services.partition_domain_store import OpenGaussPartitionDomainStore, get_partition_domain_store, set_partition_domain_store
from cube_web.services.partition_job_store import PostgresPartitionJobStore
from cube_web.services.partition_service import PartitionService
from cube_web.services.partition_workflow import PartitionWorkflowService
from cube_web.services.quality_contracts import QualityErrorFilter
from cube_web.services.quality_repository import count_quality_errors
from cube_web.services.quality_worker import claim_quality_runs, dispatch_quality_events, execute_quality_run
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient
from minio import Minio

from cube_split.scripts.run_m5_real_acceptance import CASE_IDS, validate_real_manifests

pytestmark = pytest.mark.m5_real
PUBLICATION_STATES = {"publishing", "active", "withdrawing", "failed", "withdrawn"}
DATASET_PUBLICATION_STATES = {"unpublished", "publishing", "active", "withdrawing", "failed", "withdrawn"}


def _source_key(uri: str, bucket: str) -> str:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or parsed.netloc != bucket or not parsed.path.lstrip("/"):
        pytest.fail("real acceptance source does not target the configured bucket")
    return unquote(parsed.path).lstrip("/")


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")).hexdigest()


def _redacted_prefix(uri: str) -> str:
    parsed = urlparse(uri)
    return f"s3://{parsed.netloc}/<source:{hashlib.sha256(uri.encode('utf-8')).hexdigest()[:16]}>"


def _asset_uri(asset: Any) -> str:
    return str(asset.source_uri or asset.cog_uri)


def _mutate_request(request: StrictPartitionRequest, *, label: str) -> StrictPartitionRequest:
    token = uuid4().hex
    payload = request.model_dump(mode="json")
    payload["batch_id"] = f"m5-{label}-batch-{token}"
    for index, dataset in enumerate(payload["datasets"]):
        dataset["dataset_id"] = f"m5-{label}-dataset-{index}-{token}"
        dataset["dataset_code"] = f"M5-{label[:8]}-{index}-{token[:8]}"
        dataset["dataset_title"] = f"M5 {label} {index}"
    return StrictPartitionRequest.model_validate(payload)


def _cleanup(values: dict[str, str], request: StrictPartitionRequest, task_id: str, client: Minio) -> None:
    dataset_ids = [dataset.dataset_id for dataset in request.datasets]
    generated: list[str] = []
    for attempt in range(3):
        try:
            with psycopg.connect(values["CUBE_WEB_POSTGRES_DSN"]) as connection:
                rows = connection.execute(
                    "SELECT tile_uri FROM partition_tiles WHERE dataset_id = ANY(%s) AND tile_kind = 'entity_file'", (dataset_ids,)
                ).fetchall()
                generated = [str(row[0]) for row in rows]
                for table in ("partition_quality_errors", "partition_quality_results", "partition_quality_warn_approvals", "partition_publications", "partition_quality_runs"):
                    connection.execute(f"DELETE FROM {table} WHERE dataset_id = ANY(%s)", (dataset_ids,))
                for table in (
                    "partition_domain_outbox",
                    "partition_indexes",
                    "partition_tiles",
                    "partition_grid_cells",
                    "partition_output_versions",
                    "partition_datasets",
                ):
                    connection.execute(f"DELETE FROM {table} WHERE dataset_id = ANY(%s)", (dataset_ids,))
                connection.execute("DELETE FROM partition_job_attempts WHERE task_id = %s", (task_id,))
                connection.execute("DELETE FROM partition_batches WHERE batch_id = %s", (request.batch_id,))
            break
        except psycopg.Error:
            if attempt == 2:
                raise
            time.sleep(0.5 * (attempt + 1))
    owned_prefixes = {
        f"s3://{values['CUBE_WEB_MINIO_BUCKET']}/partition/{dataset_id}/versions/" for dataset_id in dataset_ids
    }
    for uri in generated:
        if not any(uri.startswith(prefix) for prefix in owned_prefixes):
            pytest.fail("M5 cleanup refused an entity URI outside the test-owned partition prefix")
        try:
            client.remove_object(values["CUBE_WEB_MINIO_BUCKET"], _source_key(uri, values["CUBE_WEB_MINIO_BUCKET"]))
        except Exception as exc:  # Cleanup of generated entity outputs must not hide a gate result.
            print(f"M5 generated output cleanup warning: {exc}")


def _run_request(runtime: dict[str, Any], request: StrictPartitionRequest) -> tuple[dict[str, Any], str]:
    """Exercise the actual M2 strict route and poll its task API to completion."""
    data_types = {dataset.data_type for dataset in request.datasets}
    path = "/v1/partition/tasks/run" if len(data_types) > 1 else f"/v1/partition/{next(iter(data_types))}/tasks/run"
    response = runtime["client_api"].post(path, json=request.model_dump(mode="json"))
    assert response.status_code == 202, response.text
    task_id = str(response.json()["task_id"])
    deadline = time.monotonic() + 300
    while True:
        current = runtime["client_api"].get(f"/v1/partition/tasks/{task_id}")
        assert current.status_code == 200, current.text
        task = current.json()
        if task["status"] in {"completed", "failed", "cancelled"}:
            return (task["result"] if task["status"] == "completed" else task), task_id
        assert time.monotonic() < deadline, f"M2 strict task did not finish: {task_id}"
        time.sleep(0.25)


def _quality_for_dataset(runtime: dict[str, Any], dataset_id: str):
    dispatch_quality_events(worker_id="m5-real-dispatch", limit=100)
    with runtime["store"].transaction() as tx:
        row = tx.execute(
            "SELECT quality_run_id FROM partition_quality_runs WHERE dataset_id = %s ORDER BY quality_sequence DESC LIMIT 1", (dataset_id,)
        ).fetchone()
    assert row is not None
    quality_run_id = row[0]
    with runtime["store"].transaction() as tx:
        leases = claim_quality_runs(tx, worker_id="m5-real-quality", limit=100)
    lease = next((item for item in leases if item.quality_run_id == quality_run_id), None)
    assert lease is not None
    execute_quality_run(lease)
    with runtime["store"].transaction() as tx:
        run = tx.execute(
            "SELECT status,result_complete,error_count,warning_count FROM partition_quality_runs WHERE quality_run_id = %s", (quality_run_id,)
        ).fetchone()
    assert run is not None and bool(run[1])
    response = runtime["client_api"].get(f"/v1/quality/records/{quality_run_id}")
    assert response.status_code == 200, response.text
    assert response.json()["quality_run_id"] == str(quality_run_id)
    return quality_run_id, {"status": run[0], "error_count": int(run[2]), "warning_count": int(run[3])}


def _error_identity(row: dict[str, Any]) -> tuple[str, str, str | None, str | None, str | None, str | None, str | None, str, str, str]:
    def optional(value: Any) -> str | None:
        return None if value in {None, ""} else str(value)

    return (
        str(row["quality_error_id"]),
        row["rule_code"],
        optional(row.get("source_asset_id")),
        optional(row.get("tile_id")),
        optional(row.get("index_id")),
        optional(row.get("output_id")),
        optional(row.get("field")),
        row["error_code"],
        row["message"],
        json.dumps(row.get("context") or {}, sort_keys=True, separators=(",", ":")),
    )


def _api_errors(runtime: dict[str, Any], quality_run_id: Any, *, rule_code: str | None = None) -> tuple[set[tuple[Any, ...]], int]:
    page = 1
    rows: list[dict[str, Any]] = []
    total: int | None = None
    while True:
        parameters = {"page": page, "page_size": 2, "sort_by": "quality_error_id", "sort_order": "asc"}
        if rule_code:
            parameters["rule_code"] = rule_code
        response = runtime["client_api"].get(f"/v1/quality/records/{quality_run_id}/errors", params=parameters)
        assert response.status_code == 200, response.text
        payload = response.json()
        total = int(payload["total"])
        rows.extend(payload["items"])
        if len(rows) >= total:
            break
        page += 1
    return {_error_identity(row) for row in rows}, int(total or 0)


def _output_facts(store: OpenGaussPartitionDomainStore, dataset_id: str, output_version: str) -> dict[str, Any]:
    output = store.get_output_version(dataset_id, output_version)
    dataset = store.get_dataset(dataset_id)
    assert output is not None and dataset is not None
    facts = {
        "tiles": store.count_tiles(dataset_id, output_version),
        "indexes": store.count_indexes(dataset_id, output_version),
        "grid_cells": store.count_grid_cells(dataset_id, output_version),
    }
    assert output["status"] == "completed"
    assert dataset["current_output_version"] == output_version
    assert facts == {"tiles": int(output["tile_count"]), "indexes": int(output["index_count"]), "grid_cells": int(output["grid_cell_count"])}
    assert all(count > 0 for count in facts.values())
    return facts


@pytest.fixture(scope="module")
def runtime() -> dict[str, Any]:
    input_manifest = os.getenv("CUBE_M5_REAL_INPUT_MANIFEST", "").strip()
    defect_manifest = os.getenv("CUBE_M5_REAL_DEFECT_MANIFEST", "").strip()
    if not input_manifest or not defect_manifest:
        pytest.fail("M5 real gate requires both external M5 manifest paths")
    try:
        metadata = validate_real_manifests(input_manifest=Path(input_manifest), defect_manifest=Path(defect_manifest))
    except Exception as exc:
        pytest.fail(f"M5 real manifest validation failed: {exc}")
    values = {name: os.environ[name] for name in ("CUBE_WEB_POSTGRES_DSN", "CUBE_WEB_RAY_ADDRESS", "CUBE_WEB_MINIO_ENDPOINT", "CUBE_WEB_MINIO_ACCESS_KEY", "CUBE_WEB_MINIO_SECRET_KEY", "CUBE_WEB_MINIO_BUCKET")}
    try:
        with psycopg.connect(values["CUBE_WEB_POSTGRES_DSN"], connect_timeout=5) as connection:
            assert connection.execute("SELECT 1").fetchone() == (1,)
        import ray

        from cube_split.jobs.ray_logical_partition_job import _ray_runtime_env_from_env

        runtime_env = _ray_runtime_env_from_env() or {"env_vars": {}}
        env_vars = dict(runtime_env.get("env_vars") or {})
        env_vars.update({key: values[key] for key in ("CUBE_WEB_MINIO_ENDPOINT", "CUBE_WEB_MINIO_ACCESS_KEY", "CUBE_WEB_MINIO_SECRET_KEY", "CUBE_WEB_MINIO_BUCKET")})
        runtime_env["env_vars"] = env_vars
        if ray.is_initialized():
            ray.shutdown()
        ray.init(address=values["CUBE_WEB_RAY_ADDRESS"], include_dashboard=False, logging_level=40, runtime_env=runtime_env)
        assert ray.cluster_resources().get("CPU", 0) > 0
    except Exception as exc:
        pytest.fail(f"M5 real infrastructure probe failed: {exc}")
    store = OpenGaussPartitionDomainStore(dsn=values["CUBE_WEB_POSTGRES_DSN"])
    previous = get_partition_domain_store()
    set_partition_domain_store(store)
    client = Minio(values["CUBE_WEB_MINIO_ENDPOINT"], access_key=values["CUBE_WEB_MINIO_ACCESS_KEY"], secret_key=values["CUBE_WEB_MINIO_SECRET_KEY"], secure=False)
    service = PartitionService({})
    workflow = PartitionWorkflowService(
        service,
        store=PostgresPartitionJobStore(values["CUBE_WEB_POSTGRES_DSN"]),
        domain_store=store,
        runner=NormalizedPartitionDatasetRunner(),
    )
    from cube_web.routes.partition import create_partition_router

    app = FastAPI()
    api = APIRouter(prefix="/v1")
    api.include_router(create_partition_router(service=service, workflow=workflow))
    from cube_web.routes.partition_datasets import create_partition_datasets_router
    from cube_web.routes.quality import create_quality_router

    api.include_router(create_partition_datasets_router())
    api.include_router(create_quality_router())
    app.include_router(api)

    @app.middleware("http")
    async def local_actor(request, call_next):
        request.state.actor = Actor("m5-real", "admin")
        return await call_next(request)
    try:
        yield {
            "values": values,
            "store": store,
            "client": client,
            "client_api": TestClient(app),
            "input": {case_id: StrictPartitionRequest.model_validate(payload) for case_id, payload in metadata["input_requests"].items()},
            "defects": {case_id: StrictPartitionRequest.model_validate(payload) for case_id, payload in metadata["defect_requests"].items()},
        }
    finally:
        set_partition_domain_store(previous)


@pytest.fixture(scope="module", autouse=True)
def scenario_summary() -> Iterator[list[dict[str, Any]]]:
    records: list[dict[str, Any]] = []
    yield records
    expected = list(CASE_IDS)
    if [record["id"] for record in records] != expected:
        pytest.fail("M5 scenario summary is incomplete or out of immutable order")
    destination = os.getenv("CUBE_M5_REAL_SCENARIO_SUMMARY", "").strip()
    if not destination:
        pytest.fail("M5 runner did not supply CUBE_M5_REAL_SCENARIO_SUMMARY")
    path = Path(destination)
    path.write_text(json.dumps({"scenarios": records}, sort_keys=True), encoding="utf-8")


@contextmanager
def _case(records: list[dict[str, Any]], case_id: str, request: StrictPartitionRequest, task_id: str = "") -> Iterator[dict[str, Any]]:
    started = time.monotonic()
    details: dict[str, Any] = {"id": case_id, "requested_grid_level": request.requested_grid_level, "status": "passed"}
    try:
        yield details
    except Exception:
        details["status"] = "failed"
        raise
    finally:
        details["elapsed_ms"] = int((time.monotonic() - started) * 1000)
        details.setdefault("ray_job_id", task_id or None)
        details["input_digest"] = _digest(request.model_dump(mode="json"))
        details["source_prefixes"] = sorted({_redacted_prefix(_asset_uri(asset)) for dataset in request.datasets for asset in dataset.assets})
        records.append(details)


def test_geohash_logical_single_dataset(runtime: dict[str, Any], scenario_summary: list[dict[str, Any]]) -> None:
    request = _mutate_request(runtime["input"]["geohash_logical_single_dataset"], label="geohash")
    result, task_id = _run_request(runtime, request)
    try:
        with _case(scenario_summary, "geohash_logical_single_dataset", request, task_id) as record:
            assert result["status"] == "completed" and len(result["datasets"]) == 1
            item = result["datasets"][0]
            facts = _output_facts(runtime["store"], item["dataset_id"], item["output_version"])
            tiles = runtime["store"].list_tiles(item["dataset_id"], item["output_version"], limit=200, offset=0, sort_by="output_id", sort_order="asc")
            assert all(tile["tile_kind"] == "logical_reference" and tile["tile_uri"] == str(request.datasets[0].assets[0].cog_uri) for tile in tiles)
            record.update({"result_levels": sorted({int(tile["grid_level"]) for tile in tiles}), "open_gauss_counts": facts, "assertion_count": 8, "output_digest": _digest(item)})
    finally:
        _cleanup(runtime["values"], request, task_id, runtime["client"])


def test_mgrs_cross_zone_boundary_logical(runtime: dict[str, Any], scenario_summary: list[dict[str, Any]]) -> None:
    request = _mutate_request(runtime["input"]["mgrs_cross_zone_boundary_logical"], label="mgrs")
    result, task_id = _run_request(runtime, request)
    try:
        with _case(scenario_summary, "mgrs_cross_zone_boundary_logical", request, task_id) as record:
            assert result["status"] == "completed"
            item = result["datasets"][0]
            facts = _output_facts(runtime["store"], item["dataset_id"], item["output_version"])
            indexes = runtime["store"].list_indexes(item["dataset_id"], item["output_version"], limit=200, offset=0, sort_by="output_id", sort_order="asc")
            assert all(row["topology_code"] is None for row in indexes)
            actual_utm_domains = {
                f"utm-{re.match('[0-9]{1,2}', str(row['space_code'])).group()}n"
                for row in indexes
            }
            expected_domains = set(request.datasets[0].attributes["m5_expected_mgrs_domains"])
            assert actual_utm_domains == expected_domains
            assert all(str(row["space_code"]).strip() for row in indexes)
            assert all(row["window_width"] > 0 and row["window_height"] > 0 for row in indexes)
            record.update({"result_levels": sorted({int(row["grid_level"]) for row in indexes}), "open_gauss_counts": facts, "assertion_count": 8, "output_digest": _digest(result)})
    finally:
        _cleanup(runtime["values"], request, task_id, runtime["client"])


def test_isea4h_low_resolution_entity(runtime: dict[str, Any], scenario_summary: list[dict[str, Any]]) -> None:
    request = _mutate_request(runtime["input"]["isea4h_low_resolution_entity"], label="isea4h")
    result, task_id = _run_request(runtime, request)
    try:
        with _case(scenario_summary, "isea4h_low_resolution_entity", request, task_id) as record:
            assert result["status"] == "completed"
            item = result["datasets"][0]
            facts = _output_facts(runtime["store"], item["dataset_id"], item["output_version"])
            tiles = runtime["store"].list_tiles(item["dataset_id"], item["output_version"], limit=200, offset=0, sort_by="output_id", sort_order="asc")
            cells = runtime["store"].list_grid_cells(item["dataset_id"], item["output_version"], limit=200, offset=0, sort_by="output_id", sort_order="asc")
            assert all(tile["tile_kind"] == "entity_file" for tile in tiles)
            for tile in tiles:
                stat = runtime["client"].stat_object(runtime["values"]["CUBE_WEB_MINIO_BUCKET"], _source_key(str(tile["tile_uri"]), runtime["values"]["CUBE_WEB_MINIO_BUCKET"]))
                assert stat.size == tile["byte_size"]
            for cell in cells:
                level, code = int(cell["grid_level"]), str(cell["space_code"])
                assert 0 <= level <= 15 and code.isascii() and code.isdecimal() and not code.startswith("0")
                assert 1 <= int(code) <= 10 * 4**level + 2
            record.update({"result_levels": sorted({int(cell["grid_level"]) for cell in cells}), "open_gauss_counts": facts, "assertion_count": 10, "output_digest": _digest(result)})
    finally:
        _cleanup(runtime["values"], request, task_id, runtime["client"])


def test_batch_two_datasets_sibling_partial_failure(runtime: dict[str, Any], scenario_summary: list[dict[str, Any]]) -> None:
    request = _mutate_request(runtime["defects"]["batch_two_datasets_sibling_partial_failure"], label="partial")
    task_id = ""
    success_task_id = ""
    success_base = _mutate_request(runtime["defects"]["batch_two_datasets_sibling_partial_failure"], label="mixed-success")
    valid_asset = success_base.datasets[0].assets[0]
    repaired_asset = success_base.datasets[1].assets[0].model_copy(
        update={
            "cog_uri": valid_asset.cog_uri,
            "source_uri": valid_asset.source_uri,
            "checksum": valid_asset.checksum,
            "bbox": valid_asset.bbox,
            "crs": valid_asset.crs,
        }
    )
    repaired_dataset = success_base.datasets[1].model_copy(update={"assets": (repaired_asset,)})
    success_request = success_base.model_copy(update={"datasets": (success_base.datasets[0], repaired_dataset)})
    try:
        with _case(scenario_summary, "batch_two_datasets_sibling_partial_failure", request, task_id) as record:
            assert len(request.datasets) == 2 and {dataset.data_type for dataset in request.datasets} == {"optical", "radar"}
            success, success_task_id = _run_request(runtime, success_request)
            assert success["status"] == "completed" and len(success["datasets"]) == 2
            success_facts = {
                item["dataset_id"]: _output_facts(runtime["store"], item["dataset_id"], item["output_version"])
                for item in success["datasets"]
            }
            success_grids = {
                dataset.dataset_id: dataset.partition.grid_type
                for dataset in success_request.datasets
                if dataset.partition is not None
            }
            assert len(set(success_grids.values())) == 2
            assert set(success_facts) == set(success_grids)
            result, task_id = _run_request(runtime, request)
            record["ray_job_id"] = task_id
            assert result["status"] == "partial_failure"
            completed = next(item for item in result["datasets"] if item["status"] == "completed")
            failed = next(item for item in result["datasets"] if item["status"] == "failed")
            facts = _output_facts(runtime["store"], completed["dataset_id"], completed["output_version"])
            failed_dataset = runtime["store"].get_dataset(failed["dataset_id"])
            failed_output = runtime["store"].get_output_version(failed["dataset_id"], failed["output_version"])
            assert failed_dataset is not None and failed_dataset["current_output_version"] is None
            assert failed_output is not None and failed_output["status"] == "failed"
            assert runtime["store"].count_tiles(failed["dataset_id"], failed["output_version"]) == 0
            completed_dataset = next(dataset for dataset in request.datasets if dataset.dataset_id == completed["dataset_id"])
            failed_dataset_input = next(dataset for dataset in request.datasets if dataset.dataset_id == failed["dataset_id"])
            assert completed_dataset.partition is not None and failed_dataset_input.partition is not None
            assert completed_dataset.partition.grid_type != failed_dataset_input.partition.grid_type
            record.update({
                "result_levels": [completed_dataset.partition.requested_grid_level],
                "effective_grids": sorted(success_grids.values()),
                "successful_dataset_counts": success_facts,
                "open_gauss_counts": facts,
                "failed_sibling_persisted_rows": 0,
                "assertion_count": 16,
                "output_digest": _digest({"success": success, "partial_failure": result}),
            })
    finally:
        _cleanup(runtime["values"], request, task_id, runtime["client"])
        _cleanup(runtime["values"], success_request, success_task_id, runtime["client"])


def test_quality_fail_complete_exports(runtime: dict[str, Any], scenario_summary: list[dict[str, Any]]) -> None:
    request = _mutate_request(runtime["defects"]["quality_fail_complete_exports"], label="quality")
    result, task_id = _run_request(runtime, request)
    try:
        with _case(scenario_summary, "quality_fail_complete_exports", request, task_id) as record:
            assert result["status"] == "completed"
            item = result["datasets"][0]
            assets = runtime["store"].list_assets(
                item["dataset_id"], item["output_version"], limit=20, offset=0, sort_by="source_asset_id", sort_order="asc"
            )
            tiles = runtime["store"].list_tiles(
                item["dataset_id"], item["output_version"], limit=200, offset=0, sort_by="output_id", sort_order="asc"
            )
            assert assets and all(asset["source_kind"] == "raw" for asset in assets)
            assert all(asset["source_format"] in {"netcdf", "hdf5"} and asset["cog_uri"] is None for asset in assets)
            assert all(tile["tile_kind"] == "logical_reference" for tile in tiles)
            assert {tile["tile_uri"] for tile in tiles} == {asset["source_uri"] for asset in assets}
            quality_run_id, quality = _quality_for_dataset(runtime, item["dataset_id"])
            assert quality["status"] == "fail" and quality["error_count"] > 0
            with runtime["store"].transaction() as tx:
                db_count = count_quality_errors(tx, quality_run_id=quality_run_id, filters=QualityErrorFilter())
            full, full_total = _api_errors(runtime, quality_run_id)
            assert full_total == len(full) == db_count > 0
            rule_code = sorted({row[1] for row in full})[0]
            filtered_expected = {row for row in full if row[1] == rule_code}
            assert filtered_expected and filtered_expected != full
            filtered, filtered_total = _api_errors(runtime, quality_run_id, rule_code=rule_code)
            assert filtered_total == len(filtered) == len(filtered_expected)
            parsed: dict[tuple[str, str], set[tuple[Any, ...]]] = {}
            for format in ("csv", "json"):
                for label, parameters in (("full", {}), ("filtered", {"rule_code": rule_code})):
                    response = runtime["client_api"].get(
                        f"/v1/quality/records/{quality_run_id}/errors/export", params={"format": format, **parameters}
                    )
                    assert response.status_code == 200 and "page" not in str(response.request.url)
                    assert int(response.headers["X-Export-Count"]) == (db_count if label == "full" else len(filtered_expected))
                    rows = list(csv.DictReader(io.StringIO(response.content.decode("utf-8-sig")))) if format == "csv" else response.json()
                    for row in rows:
                        if isinstance(row.get("context"), str):
                            row["context"] = json.loads(row["context"])
                    parsed[(format, label)] = {_error_identity(row) for row in rows}
            assert parsed[("csv", "full")] == parsed[("json", "full")] == full
            assert parsed[("csv", "filtered")] == parsed[("json", "filtered")] == filtered_expected
            record.update({"result_levels": [request.requested_grid_level], "source_contract": {"kind": "raw", "formats": sorted({asset["source_format"] for asset in assets}), "cog_uri": None, "tile_kind": "logical_reference"}, "open_gauss_counts": {"quality_errors": db_count}, "quality_run_id": str(quality_run_id), "assertion_count": 17, "output_digest": _digest(result)})
    finally:
        _cleanup(runtime["values"], request, task_id, runtime["client"])


def test_pass_warn_publish_withdraw_reconciliation(runtime: dict[str, Any], scenario_summary: list[dict[str, Any]]) -> None:
    request = _mutate_request(runtime["input"]["pass_warn_publish_withdraw_reconciliation"], label="publication")
    assert len(request.datasets) == 2
    result, task_id = _run_request(runtime, request)
    try:
        with _case(scenario_summary, "pass_warn_publish_withdraw_reconciliation", request, task_id) as record:
            assert result["status"] == "completed"
            runs = {item["dataset_id"]: _quality_for_dataset(runtime, item["dataset_id"]) for item in result["datasets"]}
            pass_dataset = next(dataset_id for dataset_id, (_run_id, quality) in runs.items() if quality["status"] == "pass")
            warn_dataset = next(dataset_id for dataset_id, (_run_id, quality) in runs.items() if quality["status"] == "warn")
            pass_run = runs[pass_dataset][0]
            warn_run = runs[warn_dataset][0]
            pass_response = runtime["client_api"].post(f"/v1/partition/datasets/{pass_dataset}/publish", json={})
            assert pass_response.status_code == 201, pass_response.text
            pass_publication = pass_response.json()
            assert pass_publication["status"] == "active"
            warn_response = runtime["client_api"].post(f"/v1/partition/datasets/{warn_dataset}/publish", json={})
            assert warn_response.status_code == 409
            approval = runtime["client_api"].post(
                f"/v1/partition/datasets/{warn_dataset}/quality-runs/{warn_run}/warn-approvals",
                json={"reason": "M5 real Warn authorization"},
            )
            assert approval.status_code == 201, approval.text
            warn_response = runtime["client_api"].post(f"/v1/partition/datasets/{warn_dataset}/publish", json={})
            assert warn_response.status_code == 201, warn_response.text
            warn_publication = warn_response.json()
            assert warn_publication["status"] == "active"
            for dataset_id, publication in ((pass_dataset, pass_publication), (warn_dataset, warn_publication)):
                assert publication["status"] in PUBLICATION_STATES
                dataset = runtime["store"].get_dataset(dataset_id)
                assert dataset is not None and dataset["publish_status"] == "active" and dataset["publish_status"] in DATASET_PUBLICATION_STATES
                withdrawal_url = f"/v1/partition/datasets/{dataset_id}/publications/{publication['publication_id']}/withdraw"
                withdrawn = runtime["client_api"].post(withdrawal_url, json={"reason": "M5 reconciliation"})
                assert withdrawn.status_code == 200 and withdrawn.json()["status"] == "withdrawn"
                assert runtime["client_api"].post(withdrawal_url, json={"reason": "M5 reconciliation"}).json()["status"] == "withdrawn"
                history = runtime["store"].list_publications(dataset_id, limit=10, offset=0, sort_by="requested_at", sort_order="asc")
                assert len(history) == 1 and history[0]["status"] == "withdrawn"
                dataset = runtime["store"].get_dataset(dataset_id)
                assert dataset is not None and dataset["publish_status"] == "withdrawn"
            record.update({"result_levels": [request.requested_grid_level], "open_gauss_counts": {"publications": 2}, "quality_run_ids": [str(pass_run), str(warn_run)], "assertion_count": 17, "output_digest": _digest(result)})
    finally:
        _cleanup(runtime["values"], request, task_id, runtime["client"])
