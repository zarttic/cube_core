"""Non-skipping M2 acceptance against configured OpenGauss, MinIO, and Ray."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import pytest

from cube_web.services.partition_contracts import PartitionDatasetResult, StrictPartitionRequest, make_output_version
from cube_web.services.partition_dataset_runner import NormalizedPartitionDatasetRunner
from cube_web.services.partition_domain_store import OpenGaussPartitionDomainStore
from cube_web.services.partition_job_store import InMemoryPartitionJobStore
from cube_web.services.partition_object_store import PartitionObjectStore
from cube_web.services.partition_service import PartitionService
from cube_web.services.partition_workflow import PartitionWorkflowService

pytestmark = pytest.mark.m2_real

REQUIRED = (
    "CUBE_WEB_POSTGRES_DSN",
    "CUBE_WEB_M2_DATABASE_NAME",
    "CUBE_WEB_MINIO_ENDPOINT",
    "CUBE_WEB_MINIO_ACCESS_KEY",
    "CUBE_WEB_MINIO_SECRET_KEY",
    "CUBE_WEB_MINIO_BUCKET",
    "RAY_ADDRESS",
    "CUBE_WEB_M2_GEOHASH_COG_URI",
    "CUBE_WEB_M2_ISEA4H_COG_URI",
)


def _require_real_environment() -> dict[str, str]:
    values = {name: os.getenv(name, "").strip() for name in REQUIRED}
    missing = [name for name, value in values.items() if not value]
    if missing:
        pytest.fail("M2 real gate missing required environment: " + ", ".join(missing))
    return values


def _source_key(uri: str, bucket: str) -> str:
    parsed = urlparse(uri)
    assert parsed.scheme == "s3" and parsed.netloc == bucket and parsed.path.lstrip("/")
    return parsed.path.lstrip("/")


def _ray_source_metadata(values: dict[str, str], source_uri: str, *, entity_key: str | None = None) -> dict[str, object]:
    """Run the same cache/Rasterio source path used by M2 workers on real Ray."""
    import ray

    @ray.remote
    def read_source(config: dict[str, str], uri: str, output_key: str | None) -> dict[str, object]:
        from hashlib import sha256
        from io import BytesIO

        import rasterio
        from minio import Minio
        from rasterio.io import MemoryFile
        from rasterio.windows import Window

        parsed = urlparse(uri)
        client = Minio(
            config["CUBE_WEB_MINIO_ENDPOINT"],
            access_key=config["CUBE_WEB_MINIO_ACCESS_KEY"],
            secret_key=config["CUBE_WEB_MINIO_SECRET_KEY"],
            secure=False,
        )
        cache_root = Path("/tmp/cube_split_source_cache")
        cache_root.mkdir(parents=True, exist_ok=True)
        cache_path = cache_root / sha256(uri.encode("utf-8")).hexdigest()
        if not cache_path.exists():
            response = client.get_object(parsed.netloc, parsed.path.lstrip("/"))
            try:
                cache_path.write_bytes(response.read())
            finally:
                response.close()
                response.release_conn()
        with rasterio.open(cache_path) as source:
            bounds = source.bounds
            metadata: dict[str, object] = {
                "bbox": [bounds.left, bounds.bottom, bounds.right, bounds.top],
                "crs": str(source.crs),
                "width": source.width,
                "height": source.height,
            }
            if output_key is not None:
                window = Window(0, 0, min(32, source.width), min(32, source.height))
                profile = source.profile.copy()
                profile.update(driver="GTiff", width=int(window.width), height=int(window.height), transform=source.window_transform(window))
                with MemoryFile() as memory:
                    with memory.open(**profile) as destination:
                        destination.write(source.read(window=window))
                    tile = memory.read()
                checksum = sha256(tile).hexdigest()
                client.put_object(
                    parsed.netloc,
                    output_key,
                    BytesIO(tile),
                    len(tile),
                    content_type="image/tiff",
                    metadata={"checksum-sha256": checksum},
                )
                metadata.update({"tile_uri": f"s3://{parsed.netloc}/{output_key}", "checksum": checksum, "byte_size": len(tile)})
        return metadata

    ray.init(address=values["RAY_ADDRESS"], ignore_reinit_error=True, include_dashboard=False, logging_level=40)
    return ray.get(read_source.remote(values, source_uri, entity_key))


@pytest.fixture(scope="module")
def real_values() -> dict[str, str]:
    values = _require_real_environment()
    import psycopg
    from minio import Minio

    with psycopg.connect(values["CUBE_WEB_POSTGRES_DSN"]) as connection:
        row = connection.execute("SELECT current_database()").fetchone()
        assert row is not None and row[0] == values["CUBE_WEB_M2_DATABASE_NAME"]
    client = Minio(
        values["CUBE_WEB_MINIO_ENDPOINT"],
        access_key=values["CUBE_WEB_MINIO_ACCESS_KEY"],
        secret_key=values["CUBE_WEB_MINIO_SECRET_KEY"],
        secure=False,
    )
    for variable in ("CUBE_WEB_M2_GEOHASH_COG_URI", "CUBE_WEB_M2_ISEA4H_COG_URI"):
        client.stat_object(values["CUBE_WEB_MINIO_BUCKET"], _source_key(values[variable], values["CUBE_WEB_MINIO_BUCKET"]))
    import ray
    from cube_split.jobs.ray_logical_partition_job import _ray_runtime_env_from_env

    if ray.is_initialized():
        ray.shutdown()
    ray_runtime_env = _ray_runtime_env_from_env() or {"env_vars": {}}
    env_vars = dict(ray_runtime_env.get("env_vars") or {})
    env_vars.update({
        "CUBE_WEB_MINIO_ENDPOINT": values["CUBE_WEB_MINIO_ENDPOINT"],
        "CUBE_WEB_MINIO_ACCESS_KEY": values["CUBE_WEB_MINIO_ACCESS_KEY"],
        "CUBE_WEB_MINIO_SECRET_KEY": values["CUBE_WEB_MINIO_SECRET_KEY"],
        "CUBE_WEB_MINIO_BUCKET": values["CUBE_WEB_MINIO_BUCKET"],
    })
    ray_runtime_env["env_vars"] = env_vars
    ray.init(
        address=values["RAY_ADDRESS"],
        ignore_reinit_error=True,
        include_dashboard=False,
        logging_level=40,
        runtime_env=ray_runtime_env,
    )

    @ray.remote
    def clear_m2_source_cache() -> None:
        import shutil

        shutil.rmtree("/tmp/cube_split_source_cache", ignore_errors=True)

    ray.get(clear_m2_source_cache.remote())
    return values


@pytest.fixture
def minio(real_values: dict[str, str]):
    from minio import Minio

    return Minio(
        real_values["CUBE_WEB_MINIO_ENDPOINT"],
        access_key=real_values["CUBE_WEB_MINIO_ACCESS_KEY"],
        secret_key=real_values["CUBE_WEB_MINIO_SECRET_KEY"],
        secure=False,
    )


def _new_ids(label: str) -> tuple[str, str, str]:
    token = uuid4().hex
    return (f"m2-real-{label}-batch-{token}", f"m2-real-{label}-task-{token}", f"m2-real-{label}-dataset-{token}")


def _insert_active_attempt(values: dict[str, str], batch_id: str, task_id: str, data_type: str) -> None:
    import psycopg

    with psycopg.connect(values["CUBE_WEB_POSTGRES_DSN"]) as connection:
        connection.execute(
            """MERGE INTO partition_batches target
               USING (SELECT %s AS batch_id, %s AS batch_name, %s AS data_type, %s::jsonb AS source_schema, %s::jsonb AS normalized_payload) source
               ON (target.batch_id = source.batch_id)
               WHEN NOT MATCHED THEN INSERT (batch_id,batch_name,data_type,source_schema,normalized_payload,status)
                    VALUES (source.batch_id,source.batch_name,source.data_type,source.source_schema,source.normalized_payload,'running')""",
            (batch_id, batch_id, data_type, "{}", "{}"),
        )
        connection.execute(
            """MERGE INTO partition_job_attempts target
               USING (SELECT %s AS task_id, %s AS batch_id, %s::text[] AS asset_ids, %s AS operation, %s AS status, %s AS attempt_no, %s::jsonb AS payload) source
               ON (target.task_id = source.task_id)
               WHEN NOT MATCHED THEN INSERT (task_id,batch_id,asset_ids,operation,status,attempt_no,payload)
                    VALUES (source.task_id,source.batch_id,source.asset_ids,source.operation,source.status,source.attempt_no,source.payload)""",
            (task_id, batch_id, [], "run", "running", 1, "{}"),
        )
        connection.commit()


def _source_checksum(values: dict[str, str], source_uri: str) -> str:

    from minio import Minio

    client = Minio(values["CUBE_WEB_MINIO_ENDPOINT"], access_key=values["CUBE_WEB_MINIO_ACCESS_KEY"], secret_key=values["CUBE_WEB_MINIO_SECRET_KEY"], secure=False)
    response = client.get_object(values["CUBE_WEB_MINIO_BUCKET"], _source_key(source_uri, values["CUBE_WEB_MINIO_BUCKET"]))
    try:
        return sha256(response.read()).hexdigest()
    finally:
        response.close()
        response.release_conn()


def _request(batch_id: str, dataset_id: str, source_uri: str, *, checksum: str, grid_type: str = "geohash") -> StrictPartitionRequest:
    method = "entity" if grid_type == "isea4h" else "logical"
    return StrictPartitionRequest.model_validate(
        {
            "batch_id": batch_id,
            "grid_type": grid_type,
            "requested_grid_level": 1 if grid_type == "isea4h" else 3,
            "partition_method": method,
            "max_cells_per_asset": 100,
            "datasets": [
                {
                    "dataset_id": dataset_id,
                    "dataset_code": dataset_id,
                    "dataset_title": dataset_id,
                    "data_type": "optical",
                    "assets": [
                        {
                            "source_asset_id": "asset-1",
                            "cog_uri": source_uri,
                            "checksum": checksum,
                            "bbox": [100.0, 20.0, 101.0, 21.0],
                            "crs": "EPSG:4326",
                            "time_start": "2026-07-01T00:00:00Z",
                            "time_end": "2026-07-01T00:01:00Z",
                        }
                    ],
                    "bands": [
                        {"source_asset_id": "asset-1", "band_code": "B01", "band_name": "B01", "band_type": "spectral", "display_order": 0}
                    ],
                }
            ],
        }
    )


def _result(request: StrictPartitionRequest, dataset_id: str, task_id: str, metadata: dict[str, object]) -> PartitionDatasetResult:
    version = make_output_version(dataset_id, task_id)
    entity = request.grid_type == "isea4h"
    prefix = f"partition/{dataset_id}/versions/{version}/"
    common = {"source_asset_id": "asset-1", "band_code": "B01", "grid_type": request.grid_type, "grid_level": request.requested_grid_level,
              "space_code": "1" if entity else "w", "topology_code": None, "time_bucket": "2026-07-01"}
    tile_uri = str(metadata.get("tile_uri") or request.datasets[0].assets[0].cog_uri)
    tile = {**common, "output_id": f"tile-{version}", "tile_uri": tile_uri, "tile_kind": "entity_file" if entity else "logical_reference"}
    if entity:
        tile.update({"checksum": metadata["checksum"], "byte_size": metadata["byte_size"], "width": 32, "height": 32})
    index = {**common, "output_id": f"index-{version}", "tile_output_id": tile["output_id"] if entity else None, "st_code": f"i4h:{request.requested_grid_level}:1:20260701" if entity else "gh:5:w:20260701",
             "value_ref_uri": tile_uri, "window_col_off": None if entity else 0, "window_row_off": None if entity else 0,
             "window_width": None if entity else 1, "window_height": None if entity else 1}
    cell = {"output_id": f"cell-{version}", "grid_type": request.grid_type, "grid_level": request.requested_grid_level, "space_code": common["space_code"], "topology_code": None}
    return PartitionDatasetResult.model_validate({"dataset_id": dataset_id, "task_id": task_id, "output_version": version, "grid_type": request.grid_type,
        "requested_grid_level": request.requested_grid_level, "partition_method": request.partition_method, "object_prefix": prefix,
        "tiles": [tile], "indexes": [index], "grid_cells": [cell]})


def _complete_real_dataset(values: dict[str, str], source_uri: str, *, grid_type: str, label: str) -> tuple[OpenGaussPartitionDomainStore, StrictPartitionRequest, PartitionDatasetResult]:
    batch_id, task_id, dataset_id = _new_ids(label)
    _insert_active_attempt(values, batch_id, task_id, "optical")
    request = _request(batch_id, dataset_id, source_uri, checksum=_source_checksum(values, source_uri), grid_type=grid_type)
    store = OpenGaussPartitionDomainStore(dsn=values["CUBE_WEB_POSTGRES_DSN"])
    workflow = PartitionWorkflowService(
        PartitionService({}), store=InMemoryPartitionJobStore(), domain_store=store, runner=NormalizedPartitionDatasetRunner()
    )
    workflow_result = workflow.run(task_id=task_id, request=request)
    assert workflow_result["status"] == "completed", workflow_result
    version = make_output_version(dataset_id, task_id)
    output = store.get_output_version(dataset_id, version)
    assert output is not None
    result = PartitionDatasetResult.model_validate({
        "dataset_id": dataset_id, "task_id": task_id, "output_version": version, "grid_type": grid_type,
        "requested_grid_level": request.requested_grid_level, "partition_method": request.partition_method,
        "object_prefix": output["object_prefix"],
        "tiles": store.list_tiles(dataset_id, version, limit=200, offset=0, sort_by="output_id", sort_order="asc"),
        "indexes": store.list_indexes(dataset_id, version, limit=200, offset=0, sort_by="output_id", sort_order="asc"),
        "grid_cells": store.list_grid_cells(dataset_id, version, limit=200, offset=0, sort_by="output_id", sort_order="asc"),
    })
    return store, request, result


def test_m2_real_marker_registered_by_root_config(pytestconfig) -> None:
    assert any(marker.startswith("m2_real:") for marker in pytestconfig.getini("markers"))


def test_real_geohash_logical_dataset(real_values: dict[str, str], minio) -> None:
    store, request, result = _complete_real_dataset(real_values, real_values["CUBE_WEB_M2_GEOHASH_COG_URI"], grid_type="geohash", label="geohash")
    dataset = store.get_dataset(result.dataset_id)
    assert dataset and dataset["current_output_version"] == result.output_version and dataset["partition_status"] == "completed"
    assert store.count_tiles(result.dataset_id) == len(result.tiles) > 0
    assert store.count_indexes(result.dataset_id) == len(result.indexes) == len(result.tiles)
    output = store.get_output_version(result.dataset_id, result.output_version)
    assert output is not None and output["status"] == "completed"
    event = store.claim_outbox("m2-real-geohash", limit=1)
    assert len(event) == 1
    store.acknowledge_outbox(str(event[0]["event_id"]))
    assert result.tiles[0]["tile_uri"] == str(request.datasets[0].assets[0].cog_uri)


def test_real_isea4h_entity_dataset(real_values: dict[str, str], minio) -> None:
    store, _request_value, result = _complete_real_dataset(real_values, real_values["CUBE_WEB_M2_ISEA4H_COG_URI"], grid_type="isea4h", label="isea4h")
    tile = store.list_tiles(result.dataset_id, result.output_version, limit=10, offset=0, sort_by="output_id", sort_order="asc")[0]
    key = _source_key(str(tile["tile_uri"]), real_values["CUBE_WEB_MINIO_BUCKET"])
    assert key.startswith(f"partition/{result.dataset_id}/versions/{result.output_version}/tiles/")
    stat = minio.stat_object(real_values["CUBE_WEB_MINIO_BUCKET"], key)
    assert stat.size == tile["byte_size"] and tile["checksum"]
    dataset = store.get_dataset(result.dataset_id)
    assert dataset is not None and dataset["current_output_version"] == result.output_version


def test_real_two_dataset_partial_failure(real_values: dict[str, str]) -> None:
    store, _request_value, successful = _complete_real_dataset(real_values, real_values["CUBE_WEB_M2_GEOHASH_COG_URI"], grid_type="geohash", label="partial-ok")
    batch_id, task_id, failed_dataset = _new_ids("partial-fail")
    _insert_active_attempt(real_values, batch_id, task_id, "optical")
    request = _request(batch_id, failed_dataset, "s3://" + real_values["CUBE_WEB_MINIO_BUCKET"] + "/partition/m2-real-missing.tif", checksum="a" * 64)
    version = store.start_output(request, request.datasets[0], task_id)
    with pytest.raises(Exception):
        _ray_source_metadata(real_values, str(request.datasets[0].assets[0].cog_uri))
    store.fail_output(failed_dataset, version, error_code="partition_execution_failed", error_message="missing COG")
    successful_dataset = store.get_dataset(successful.dataset_id)
    failed_output = store.get_output_version(failed_dataset, version)
    assert successful_dataset is not None and successful_dataset["current_output_version"] == successful.output_version
    assert failed_output is not None and failed_output["status"] == "failed"


def test_real_atomic_rollback_keeps_old_pointer(real_values: dict[str, str]) -> None:
    store, request, first = _complete_real_dataset(real_values, real_values["CUBE_WEB_M2_GEOHASH_COG_URI"], grid_type="geohash", label="rollback")
    second_task = first.task_id + "-retry"
    _insert_active_attempt(real_values, request.batch_id, second_task, "optical")
    version = store.start_output(request, request.datasets[0], second_task)
    bad = _result(request, first.dataset_id, second_task, {})
    bad = bad.model_copy(update={"output_version": version, "tiles": ({**bad.tiles[0], "source_asset_id": "missing-asset"},)})
    with pytest.raises(Exception):
        store.complete_output(bad)
    store.fail_output(first.dataset_id, version, error_code="detail_constraint", error_message="intentional FK failure")
    assert store.resolve_output_version(first.dataset_id) == first.output_version
    failed_output = store.get_output_version(first.dataset_id, version)
    assert failed_output is not None and failed_output["status"] == "failed"


def test_real_unknown_commit_is_idempotent(real_values: dict[str, str]) -> None:
    store, request, first = _complete_real_dataset(real_values, real_values["CUBE_WEB_M2_GEOHASH_COG_URI"], grid_type="geohash", label="unknown")
    retry_task = first.task_id + "-retry"
    _insert_active_attempt(real_values, request.batch_id, retry_task, "optical")
    version = store.start_output(request, request.datasets[0], retry_task)
    committed = _result(request, first.dataset_id, retry_task, {})
    committed = committed.model_copy(update={"output_version": version})
    store.complete_output(committed)  # Server committed; caller intentionally discards this acknowledgement.
    recovered = OpenGaussPartitionDomainStore(dsn=real_values["CUBE_WEB_POSTGRES_DSN"])
    recovered.complete_output(committed)
    assert recovered.resolve_output_version(first.dataset_id) == version
    assert recovered.count_tiles(first.dataset_id, version) == len(committed.tiles)
    assert len(recovered._read_rows("SELECT * FROM partition_domain_outbox WHERE dataset_id = %s AND output_version = %s", (first.dataset_id, version))) == 1


def test_real_orphan_cleanup_guards_and_idempotency(real_values: dict[str, str], minio) -> None:
    store, _request_value, current = _complete_real_dataset(real_values, real_values["CUBE_WEB_M2_ISEA4H_COG_URI"], grid_type="isea4h", label="cleanup")
    objects = PartitionObjectStore(minio, bucket=real_values["CUBE_WEB_MINIO_BUCKET"])
    with pytest.raises(RuntimeError, match="current"):
        objects.cleanup_unreferenced_version(store, current.dataset_id, current.output_version, older_than=datetime.now(UTC))
    import psycopg
    with psycopg.connect(real_values["CUBE_WEB_POSTGRES_DSN"]) as connection:
        connection.execute("UPDATE partition_output_versions SET completed_at = now() - interval '2 days' WHERE dataset_id = %s AND output_version = %s", (current.dataset_id, current.output_version))
        connection.execute("UPDATE partition_datasets SET current_output_version = NULL, partition_status = 'running', partition_completed_at = NULL WHERE dataset_id = %s", (current.dataset_id,))
        run_id = uuid4()
        connection.execute("""INSERT INTO partition_quality_runs (quality_run_id,dataset_id,output_version,quality_sequence,trigger,requested_by,rule_set_version,rule_snapshot,status)
                              VALUES (%s,%s,%s,1,'manual','m2-real','m2','{}'::jsonb,'pass')""", (run_id, current.dataset_id, current.output_version))
        connection.execute("""INSERT INTO partition_publications (publication_id,dataset_id,output_version,quality_run_id,status,desired_action,service_version_id,requested_by,requested_at,activated_at)
                              VALUES (%s,%s,%s,%s,'active','activate','m2-real-service','m2-real',now(),now())""", (uuid4(), current.dataset_id, current.output_version, run_id))
        connection.commit()
    with pytest.raises(RuntimeError, match="publication_referenced"):
        objects.cleanup_unreferenced_version(store, current.dataset_id, current.output_version, older_than=datetime.now(UTC))
    with psycopg.connect(real_values["CUBE_WEB_POSTGRES_DSN"]) as connection:
        connection.execute("DELETE FROM partition_publications WHERE dataset_id = %s AND output_version = %s", (current.dataset_id, current.output_version))
        connection.execute("DELETE FROM partition_quality_runs WHERE dataset_id = %s AND output_version = %s", (current.dataset_id, current.output_version))
        connection.commit()
    first = objects.cleanup_unreferenced_version(store, current.dataset_id, current.output_version, older_than=datetime.now(UTC))
    second = objects.cleanup_unreferenced_version(store, current.dataset_id, current.output_version, older_than=datetime.now(UTC))
    assert first["cleanup_complete"] and second["deleted_keys"] == []


def test_real_catalog_matches_m3_handoff(real_values: dict[str, str]) -> None:
    import psycopg
    required = {
        "partition_quality_runs": {"quality_run_id", "dataset_id", "output_version", "trigger_event_id", "status", "claimed_at", "claimed_by"},
        "partition_quality_results": {"quality_run_id", "rule_code", "metrics"},
        "partition_quality_errors": {"quality_error_id", "quality_run_id", "error_code", "context"},
        "partition_quality_warn_approvals": {"approval_id", "quality_run_id", "approved_by"},
        "partition_publications": {"publication_id", "quality_run_id", "status", "desired_action", "service_version_id", "withdrawal_reason"},
    }
    with psycopg.connect(real_values["CUBE_WEB_POSTGRES_DSN"]) as connection:
        rows = connection.execute("SELECT table_name,column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = ANY(%s)", (list(required),)).fetchall()
        columns: dict[str, set[str]] = {name: set() for name in required}
        for table, column in rows:
            columns[str(table)].add(str(column))
        assert all(required[name] <= columns[name] for name in required)
        definitions = "\n".join(row[0] for row in connection.execute("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid IN (SELECT oid FROM pg_class WHERE relname = ANY(%s))", (list(required),)).fetchall())
        assert "withdrawing" in definitions and "withdrawn" in definitions and "desired_action" in definitions
        indexes = {row[0] for row in connection.execute("SELECT indexname FROM pg_indexes WHERE schemaname = current_schema()").fetchall()}
        assert {"idx_partition_quality_claim", "idx_partition_quality_errors_page", "idx_partition_publication_claim"} <= indexes
