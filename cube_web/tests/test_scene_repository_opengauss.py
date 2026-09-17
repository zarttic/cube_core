"""Actual OpenGauss tests for the scene repository quality-batch detail read path.

Regression guard: ``get_partition_quality_batch`` builds ``error_logs`` with
``json_agg(json_build_object('context', quality_error.context))``. ``context`` is a
``jsonb`` column, and OpenGauss aborts that expression with
``cache lookup failed for type <random oid>``. Leaving ``context`` a JSON string would
hide the failure but break the drawer's "具体原因" rendering, so the SQL casts the
column to ``json`` and this test asserts the value round-trips as a nested object.

The default unit suite drives this query through a fake cursor, so only a real
OpenGauss execution can catch this class of dialect failure.
"""

from __future__ import annotations

import os
from uuid import uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from cube_web.services.scene_repository import OpenGaussSceneRepository

pytestmark = pytest.mark.scene_repository_opengauss

CONTEXT = {"declared": "EPSG:4326", "actual": "EPSG:32648"}
ERROR_CODE = "crs_metadata_mismatch"


@pytest.fixture(scope="module")
def dsn() -> str:
    value = os.getenv("CUBE_WEB_POSTGRES_DSN", "").strip()
    if not value:
        pytest.fail("OpenGauss scene repository tests require CUBE_WEB_POSTGRES_DSN")
    with psycopg.connect(value, connect_timeout=5) as connection:
        assert connection.execute("SELECT 1").fetchone() == (1,)
    return value


@pytest.fixture
def quality_batch(dsn):
    token = uuid4().hex
    ids = {
        "batch_id": f"scene-quality-batch-{token}",
        "task_id": f"scene-quality-task-{token}",
        "dataset_id": f"scene-quality-dataset-{token}",
        "scene_id": f"scene-quality-scene-{token}",
        "output_version": f"scene-quality-version-{token}",
        "partition_run_id": f"scene-quality-partition-run-{token}",
        "band_unit_id": f"scene-quality-band-{token}",
        "asset_id": f"scene-quality-asset-{token}",
        "selection_id": f"scene-quality-selection-{token}",
        "quality_run_id": uuid4(),
        "quality_error_id": uuid4(),
    }
    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        connection.execute(
            "INSERT INTO partition_batches (batch_id, batch_name, data_type, source_schema, normalized_payload, status) "
            "VALUES (%s, %s, 'optical', '{}'::jsonb, '{}'::jsonb, 'running')",
            (ids["batch_id"], ids["batch_id"]),
        )
        connection.execute(
            "INSERT INTO partition_job_attempts (task_id, batch_id, asset_ids, operation, status, attempt_no, payload) "
            "VALUES (%s, %s, '{}'::text[], 'run', 'completed', 1, '{}'::jsonb)",
            (ids["task_id"], ids["batch_id"]),
        )
        connection.execute(
            "INSERT INTO datasets (dataset_id, dataset_code, dataset_title, data_type) "
            "VALUES (%s, %s, %s, 'optical')",
            (ids["dataset_id"], ids["dataset_id"], ids["dataset_id"]),
        )
        connection.execute(
            "INSERT INTO scenes (scene_id, dataset_id, scene_key, identity_key, source_uri) "
            "VALUES (%s, %s, %s, %s, 's3://cube/test/scene.tif')",
            (ids["scene_id"], ids["dataset_id"], ids["scene_id"], ids["scene_id"]),
        )
        connection.execute(
            "INSERT INTO scene_assets (scene_id, asset_id, source_uri) "
            "VALUES (%s, %s, 's3://cube/test/scene.tif')",
            (ids["scene_id"], ids["asset_id"]),
        )
        connection.execute(
            "INSERT INTO scene_bands (scene_id, asset_id, band_unit_id, band_code, band_type) "
            "VALUES (%s, %s, %s, 'B1', 'spectral')",
            (ids["scene_id"], ids["asset_id"], ids["band_unit_id"]),
        )
        connection.execute(
            "INSERT INTO partition_output_versions "
            "(dataset_id, output_version, task_id, grid_type, requested_grid_level, requested_grid_level_name, "
            "partition_method, status, object_prefix, completed_at) "
            "VALUES (%s, %s, %s, 'geohash', 5, 'Geohash precision 5', 'logical', 'completed', %s, now())",
            (ids["dataset_id"], ids["output_version"], ids["task_id"], f"partition/{ids['dataset_id']}/"),
        )
        connection.execute(
            "INSERT INTO partition_runs (partition_run_id, status) VALUES (%s, 'completed')",
            (ids["partition_run_id"],),
        )
        connection.execute(
            "INSERT INTO partition_run_scenes "
            "(partition_run_id, selection_id, scene_id, dataset_id, status, idempotency_key) "
            "VALUES (%s, %s, %s, %s, 'completed', %s)",
            (
                ids["partition_run_id"],
                ids["selection_id"],
                ids["scene_id"],
                ids["dataset_id"],
                ids["selection_id"],
            ),
        )
        connection.execute(
            "INSERT INTO partition_data_unit_grid_status "
            "(dataset_id, scene_id, band_unit_id, grid_type, grid_level, partition_run_id, partition_status, "
            "quality_status, output_version) "
            "VALUES (%s, %s, %s, 'geohash', 5, %s, 'completed', 'fail', %s)",
            (
                ids["dataset_id"],
                ids["scene_id"],
                ids["band_unit_id"],
                ids["partition_run_id"],
                ids["output_version"],
            ),
        )
        connection.execute(
            "INSERT INTO partition_quality_runs "
            "(quality_run_id, dataset_id, output_version, quality_sequence, trigger, requested_by, "
            "rule_set_version, rule_snapshot, status) "
            "VALUES (%s, %s, %s, 1, 'automatic', 'scene-repository-opengauss', 'test', '[]'::jsonb, 'fail')",
            (ids["quality_run_id"], ids["dataset_id"], ids["output_version"]),
        )
        connection.execute(
            "INSERT INTO partition_quality_errors "
            "(quality_error_id, quality_run_id, dataset_id, output_version, rule_code, error_code, message, "
            "field_name, context) "
            "VALUES (%s, %s, %s, %s, 'asset_crs', %s, 'declared CRS differs from raster CRS', 'crs', %s::jsonb)",
            (
                ids["quality_error_id"],
                ids["quality_run_id"],
                ids["dataset_id"],
                ids["output_version"],
                ERROR_CODE,
                psycopg.types.json.Jsonb(CONTEXT),
            ),
        )
        connection.commit()
    try:
        yield ids
    finally:
        with psycopg.connect(dsn) as connection:
            connection.execute("DELETE FROM partition_quality_errors WHERE dataset_id = %s", (ids["dataset_id"],))
            connection.execute("DELETE FROM partition_quality_runs WHERE dataset_id = %s", (ids["dataset_id"],))
            connection.execute(
                "DELETE FROM partition_data_unit_grid_status WHERE partition_run_id = %s", (ids["partition_run_id"],)
            )
            connection.execute(
                "DELETE FROM partition_run_scenes WHERE partition_run_id = %s", (ids["partition_run_id"],)
            )
            connection.execute("DELETE FROM scene_bands WHERE scene_id = %s", (ids["scene_id"],))
            connection.execute("DELETE FROM scene_assets WHERE scene_id = %s", (ids["scene_id"],))
            connection.execute(
                "DELETE FROM partition_output_versions WHERE dataset_id = %s", (ids["dataset_id"],)
            )
            connection.execute("DELETE FROM partition_runs WHERE partition_run_id = %s", (ids["partition_run_id"],))
            connection.execute("DELETE FROM scenes WHERE dataset_id = %s", (ids["dataset_id"],))
            connection.execute("DELETE FROM datasets WHERE dataset_id = %s", (ids["dataset_id"],))
            connection.execute("DELETE FROM partition_job_attempts WHERE task_id = %s", (ids["task_id"],))
            connection.execute("DELETE FROM partition_batches WHERE batch_id = %s", (ids["batch_id"],))
            connection.commit()


def test_quality_batch_detail_keeps_jsonb_context_as_object(quality_batch, dsn):
    batch = OpenGaussSceneRepository(dsn).get_partition_quality_batch(quality_batch["partition_run_id"])

    assert batch is not None, "quality batch detail should resolve for an existing partition run"
    assert batch["status"] == "completed"
    quality_runs = [run for dataset in batch["datasets"] for run in dataset["quality_runs"]]
    assert len(quality_runs) == 1

    error_logs = quality_runs[0]["error_logs"]
    assert len(error_logs) == 1
    error = error_logs[0]
    assert error["error_code"] == ERROR_CODE
    assert error["field"] == "crs"
    assert error["context"] == CONTEXT, "jsonb context must round-trip as a nested object, not a JSON string"


def test_quality_batch_detail_returns_none_for_unknown_run(dsn):
    assert OpenGaussSceneRepository(dsn).get_partition_quality_batch(f"missing-{uuid4().hex}") is None
