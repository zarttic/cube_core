"""Actual OpenGauss acceptance for the quality-to-ingest bridge and record seal.

The default unit suite drives ``create_ingest_runs_after_quality`` through fake
cursors, so the plan/gate assertions never execute its SQL. This test runs the
real statements end to end: a quality-passed band creates its ingest run and the
partition record is sealed (`merge_state='closed'`) so the pre-ingest merge
window cannot rewrite an already-consumed output.
"""

from __future__ import annotations

import os
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from cube_web.services.partition_domain_store import OpenGaussPartitionDomainStore
from cube_web.services.quality_ingest_bridge import create_ingest_runs_after_quality

pytestmark = pytest.mark.quality_repository_opengauss


@pytest.fixture(scope="module")
def dsn() -> str:
    value = os.getenv("CUBE_WEB_POSTGRES_DSN", "").strip()
    if not value:
        pytest.fail("OpenGauss quality tests require CUBE_WEB_POSTGRES_DSN")
    with psycopg.connect(value, connect_timeout=5) as connection:
        assert connection.execute("SELECT 1").fetchone() == (1,)
    return value


@pytest.fixture
def ingest_target(dsn) -> dict[str, str]:
    token = uuid4().hex
    ids = {
        "token": token,
        "dataset_id": f"ingest-bridge-dataset-{token}",
        "scene_id": f"ingest-bridge-scene-{token}",
        "band_unit_id": f"ingest-bridge-band-{token}",
        "output_version": f"ingest-bridge-version-{token}",
        "partition_run_id": f"ingest-bridge-run-{token}",
        "quality_run_id": uuid4(),
        "load_batch_id": f"ingest-bridge-load-{token}",
        "batch_id": f"ingest-bridge-batch-{token}",
        "task_id": f"ingest-bridge-task-{token}",
    }
    with psycopg.connect(dsn) as connection:
        connection.execute(
            "INSERT INTO partition_batches (batch_id,batch_name,data_type,source_schema,normalized_payload,status) "
            "VALUES (%s,%s,'optical','{}'::jsonb,'{}'::jsonb,'running')",
            (ids["batch_id"], ids["batch_id"]),
        )
        connection.execute(
            "INSERT INTO partition_job_attempts (task_id,batch_id,asset_ids,operation,status,attempt_no,payload) "
            "VALUES (%s,%s,'{}'::text[],'run','completed',1,'{}'::jsonb)",
            (ids["task_id"], ids["batch_id"]),
        )
        connection.execute(
            "INSERT INTO partition_output_versions "
            "(dataset_id, output_version, task_id, grid_type, requested_grid_level, requested_grid_level_name, "
            "partition_method, status, object_prefix, completed_at) "
            "VALUES (%s, %s, %s, 'geohash', 4, 'Geohash precision 4', 'logical', 'completed', %s, now())",
            (ids["dataset_id"], ids["output_version"], ids["task_id"], f"partition/{ids['dataset_id']}/"),
        )
        connection.execute(
            "INSERT INTO datasets (dataset_id,dataset_code,dataset_title,data_type) VALUES (%s,%s,%s,'optical')",
            (ids["dataset_id"], ids["dataset_id"], ids["dataset_id"]),
        )
        connection.execute(
            "INSERT INTO scenes (scene_id,dataset_id,scene_key,identity_key,source_uri,status) "
            "VALUES (%s,%s,%s,%s,'s3://cube/source/ingest.tif','loaded')",
            (ids["scene_id"], ids["dataset_id"], ids["scene_id"], ids["scene_id"]),
        )
        connection.execute(
            "INSERT INTO load_batches (load_batch_id,batch_name,status) VALUES (%s,%s,'succeeded')",
            (ids["load_batch_id"], ids["load_batch_id"]),
        )
        connection.execute(
            "INSERT INTO load_batch_scenes (load_batch_id,scene_id,source_asset_id,source_uri,load_status) "
            "VALUES (%s,%s,NULL,'s3://cube/source/ingest.tif','succeeded')",
            (ids["load_batch_id"], ids["scene_id"]),
        )
        connection.execute(
            "INSERT INTO partition_runs "
            "(partition_run_id,status,merge_key,merge_state,last_submitted_at,submission_count) "
            "VALUES (%s,'completed',%s,'open',now(),1)",
            (ids["partition_run_id"], f"ingest-bridge-key-{token}"),
        )
        connection.execute(
            "INSERT INTO partition_run_scenes "
            "(partition_run_id,selection_id,scene_id,dataset_id,status,output_version,idempotency_key) "
            "VALUES (%s,%s,%s,%s,'completed',%s,%s)",
            (
                ids["partition_run_id"],
                f"{ids['load_batch_id']}:{ids['dataset_id']}",
                ids["scene_id"],
                ids["dataset_id"],
                ids["output_version"],
                f"ingest-bridge-prs-{token}",
            ),
        )
        connection.execute(
            "INSERT INTO partition_data_unit_grid_status "
            "(dataset_id,scene_id,band_unit_id,grid_type,grid_level,partition_run_id,partition_status,"
            "quality_status,ingest_status,output_version) "
            "VALUES (%s,%s,%s,'geohash',4,%s,'completed','pass','pending',%s)",
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
            "(quality_run_id,dataset_id,output_version,quality_sequence,trigger,requested_by,"
            "rule_set_version,rule_snapshot,status,result_complete,completed_at) "
            "VALUES (%s,%s,%s,1,'manual','ingest-bridge-opengauss','test','[]'::jsonb,'pass',true,now())",
            (ids["quality_run_id"], ids["dataset_id"], ids["output_version"]),
        )
        connection.commit()
    try:
        yield ids
    finally:
        with psycopg.connect(dsn) as connection:
            connection.execute("DELETE FROM ingest_run_scenes WHERE partition_run_id=%s", (ids["partition_run_id"],))
            connection.execute("DELETE FROM ingest_runs WHERE partition_run_id=%s", (ids["partition_run_id"],))
            connection.execute("DELETE FROM partition_quality_runs WHERE dataset_id=%s", (ids["dataset_id"],))
            connection.execute(
                "DELETE FROM partition_data_unit_grid_status WHERE partition_run_id=%s", (ids["partition_run_id"],)
            )
            connection.execute("DELETE FROM partition_run_scenes WHERE partition_run_id=%s", (ids["partition_run_id"],))
            connection.execute("DELETE FROM partition_runs WHERE partition_run_id=%s", (ids["partition_run_id"],))
            connection.execute("DELETE FROM partition_output_versions WHERE dataset_id=%s", (ids["dataset_id"],))
            connection.execute("DELETE FROM partition_job_attempts WHERE task_id=%s", (ids["task_id"],))
            connection.execute("DELETE FROM partition_batches WHERE batch_id=%s", (ids["batch_id"],))
            connection.execute("DELETE FROM load_batch_scenes WHERE load_batch_id=%s", (ids["load_batch_id"],))
            connection.execute("DELETE FROM scenes WHERE scene_id=%s", (ids["scene_id"],))
            connection.execute("DELETE FROM load_batches WHERE load_batch_id=%s", (ids["load_batch_id"],))
            connection.execute("DELETE FROM datasets WHERE dataset_id=%s", (ids["dataset_id"],))
            connection.commit()


def test_quality_pass_creates_the_ingest_run_and_seals_the_record(dsn, ingest_target) -> None:
    store = OpenGaussPartitionDomainStore(dsn)

    with store.transaction() as tx:
        created = create_ingest_runs_after_quality(
            tx,
            quality_run_id=UUID(str(ingest_target["quality_run_id"])),
            dataset_id=ingest_target["dataset_id"],
            output_version=ingest_target["output_version"],
            quality_status="pass",
            requested_by="ingest-bridge-opengauss",
            manual=True,
            band_unit_ids={ingest_target["band_unit_id"]},
        )

    assert created == 1
    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        ingest_run = connection.execute(
            "SELECT ingest_run_id, status FROM ingest_runs WHERE partition_run_id=%s",
            (ingest_target["partition_run_id"],),
        ).fetchone()
        assert ingest_run is not None
        assert ingest_run["status"] == "queued"
        scene = connection.execute(
            "SELECT band_unit_ids, status FROM ingest_run_scenes WHERE partition_run_id=%s",
            (ingest_target["partition_run_id"],),
        ).fetchone()
        assert scene["band_unit_ids"] == [ingest_target["band_unit_id"]]
        assert scene["status"] == "queued"
        record = connection.execute(
            "SELECT merge_state FROM partition_runs WHERE partition_run_id=%s",
            (ingest_target["partition_run_id"],),
        ).fetchone()
        assert record["merge_state"] == "closed"

    # A repeated dispatch stays idempotent and keeps the record sealed.
    with store.transaction() as tx:
        again = create_ingest_runs_after_quality(
            tx,
            quality_run_id=UUID(str(ingest_target["quality_run_id"])),
            dataset_id=ingest_target["dataset_id"],
            output_version=ingest_target["output_version"],
            quality_status="pass",
            requested_by="ingest-bridge-opengauss",
            manual=True,
            band_unit_ids={ingest_target["band_unit_id"]},
        )
    assert again == 0
    with psycopg.connect(dsn) as connection:
        state = connection.execute(
            "SELECT merge_state FROM partition_runs WHERE partition_run_id=%s",
            (ingest_target["partition_run_id"],),
        ).fetchone()
        assert state[0] == "closed"
