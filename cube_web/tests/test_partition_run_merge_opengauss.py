"""Actual OpenGauss acceptance for the partition-record merge window.

One submission target owns at most one open record: repeat submissions append an
attempt (with its parameters and diff) instead of creating a sibling run that
takes over the single current grid-status row. The default unit suite drives the
repository through fake cursors, so only a real OpenGauss execution can verify
the partial unique index, the MERGE-window SQL and the submission history.
"""

from __future__ import annotations

import os
from uuid import uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from cube_web.services.scene_contracts import ScenePartitionRunRequest
from cube_web.services.scene_repository import OpenGaussSceneRepository, PartitionRunInProgressError

pytestmark = pytest.mark.scene_repository_opengauss

CHECKSUM = "a" * 64


@pytest.fixture(scope="module")
def dsn() -> str:
    value = os.getenv("CUBE_WEB_POSTGRES_DSN", "").strip()
    if not value:
        pytest.fail("OpenGauss scene repository tests require CUBE_WEB_POSTGRES_DSN")
    with psycopg.connect(value, connect_timeout=5) as connection:
        assert connection.execute("SELECT 1").fetchone() == (1,)
    return value


@pytest.fixture
def partition_target(dsn) -> dict[str, str]:
    token = uuid4().hex
    ids = {
        "token": token,
        "dataset_id": f"merge-dataset-{token}",
        "scene_id": f"merge-scene-{token}",
        "asset_id": f"merge-asset-{token}",
        "band_unit_id": f"merge-band-{token}",
        "load_batch_id": f"merge-load-{token}",
        "alternate_load_batch_id": f"merge-load-alt-{token}",
    }
    with psycopg.connect(dsn) as connection:
        connection.execute(
            "INSERT INTO datasets (dataset_id,dataset_code,dataset_title,data_type) VALUES (%s,%s,%s,'optical')",
            (ids["dataset_id"], f"MERGE-{token[:12]}", "Merge acceptance"),
        )
        connection.execute(
            "INSERT INTO scenes (scene_id,dataset_id,scene_key,identity_key,source_uri,checksum,status) "
            "VALUES (%s,%s,%s,%s,'s3://cube/source/merge.tif',%s,'loaded')",
            (ids["scene_id"], ids["dataset_id"], f"merge-key-{token}", f"merge-identity-{token}", CHECKSUM),
        )
        connection.execute(
            "INSERT INTO scene_assets (scene_id,asset_id,source_uri,cog_uri,asset_role,source_kind,source_format,checksum) "
            "VALUES (%s,%s,'s3://cube/source/merge.tif','s3://cube/source/merge.tif','data','cog','cog',%s)",
            (ids["scene_id"], ids["asset_id"], CHECKSUM),
        )
        connection.execute(
            "INSERT INTO scene_bands (scene_id,asset_id,band_unit_id,band_code,band_name,band_type,display_order) "
            "VALUES (%s,%s,%s,'B04','Red','spectral',1)",
            (ids["scene_id"], ids["asset_id"], ids["band_unit_id"]),
        )
        connection.execute(
            "INSERT INTO load_batches (load_batch_id,batch_name,status) VALUES (%s,%s,'succeeded')",
            (ids["load_batch_id"], f"Merge load {token[:8]}"),
        )
        connection.execute(
            "INSERT INTO load_batch_scenes (load_batch_id,scene_id,source_asset_id,source_uri,load_status) "
            "VALUES (%s,%s,%s,'s3://cube/source/merge.tif','succeeded')",
            (ids["load_batch_id"], ids["scene_id"], ids["asset_id"]),
        )
        connection.execute(
            "INSERT INTO load_batches (load_batch_id,batch_name,status) VALUES (%s,%s,'succeeded')",
            (ids["alternate_load_batch_id"], f"Merge alt load {token[:8]}"),
        )
        connection.execute(
            "INSERT INTO load_batch_scenes (load_batch_id,scene_id,source_asset_id,source_uri,load_status) "
            "VALUES (%s,%s,%s,'s3://cube/source/merge.tif','succeeded')",
            (ids["alternate_load_batch_id"], ids["scene_id"], ids["asset_id"]),
        )
        connection.commit()
    try:
        yield ids
    finally:
        prefix = f"merge-run-{token}%"
        with psycopg.connect(dsn) as connection:
            connection.execute("DELETE FROM ingest_runs WHERE partition_run_id LIKE %s", (prefix,))
            connection.execute("DELETE FROM partition_run_submissions WHERE partition_run_id LIKE %s", (prefix,))
            connection.execute("DELETE FROM partition_data_unit_grid_status WHERE partition_run_id LIKE %s", (prefix,))
            connection.execute("DELETE FROM partition_run_scenes WHERE partition_run_id LIKE %s", (prefix,))
            connection.execute("DELETE FROM partition_runs WHERE partition_run_id LIKE %s", (prefix,))
            connection.execute("DELETE FROM partition_job_attempts WHERE batch_id LIKE %s", (f"merge-batch-{token}%",))
            connection.execute("DELETE FROM partition_batches WHERE batch_id LIKE %s", (f"merge-batch-{token}%",))
            connection.execute("DELETE FROM load_batch_scenes WHERE load_batch_id=ANY(%s)", (
                [ids["load_batch_id"], ids["alternate_load_batch_id"]],
            ))
            connection.execute("DELETE FROM scene_bands WHERE scene_id=%s", (ids["scene_id"],))
            connection.execute("DELETE FROM scene_assets WHERE scene_id=%s", (ids["scene_id"],))
            connection.execute("DELETE FROM scenes WHERE scene_id=%s", (ids["scene_id"],))
            connection.execute("DELETE FROM datasets WHERE dataset_id=%s", (ids["dataset_id"],))
            connection.execute("DELETE FROM load_batches WHERE load_batch_id=ANY(%s)", (
                [ids["load_batch_id"], ids["alternate_load_batch_id"]],
            ))
            connection.commit()


def _request(
    ids: dict[str, str],
    suffix: str,
    *,
    grid_level: int = 4,
    grid_type: str = "geohash",
    load_batch_key: str = "load_batch_id",
) -> ScenePartitionRunRequest:
    load_batch_id = ids[load_batch_key]
    return ScenePartitionRunRequest.model_validate({
        "partition_run_id": f"merge-run-{ids['token']}-{suffix}",
        "source_batch_ids": [load_batch_id],
        "datasets": [{
            "dataset_id": ids["dataset_id"],
            "source_batch_id": load_batch_id,
            "scene_ids": [ids["scene_id"]],
            "band_unit_ids": [ids["band_unit_id"]],
            "partition": {"grid_type": grid_type, "requested_grid_level": grid_level, "partition_method": "logical"},
        }],
    })


def _finish_first_attempt(dsn: str, partition_run_id: str) -> None:
    with psycopg.connect(dsn) as connection:
        connection.execute(
            "UPDATE partition_runs SET status='completed', completed_at=now() WHERE partition_run_id=%s",
            (partition_run_id,),
        )
        connection.execute(
            "UPDATE partition_data_unit_grid_status SET partition_status='completed', quality_status='pass' "
            "WHERE partition_run_id=%s",
            (partition_run_id,),
        )
        connection.commit()


def test_repeat_submission_merges_into_the_open_record(dsn, partition_target) -> None:
    repository = OpenGaussSceneRepository(dsn)
    first = repository.create_partition_run(_request(partition_target, "a"), requested_by="alice")
    run_id = str(first["partition_run_id"])

    assert first["merged"] is False
    assert first["attempt_no"] == 1
    _finish_first_attempt(dsn, run_id)

    second = repository.create_partition_run(_request(partition_target, "b"), requested_by="bob")

    assert second["partition_run_id"] == run_id
    assert second["merged"] is True
    assert second["attempt_no"] == 2
    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        run = connection.execute(
            "SELECT status, merge_state, submission_count FROM partition_runs WHERE partition_run_id=%s",
            (run_id,),
        ).fetchone()
        assert run["status"] == "pending"
        assert run["merge_state"] == "open"
        assert run["submission_count"] == 2
        submissions = connection.execute(
            "SELECT attempt_no, requested_by, changes, parameters FROM partition_run_submissions "
            "WHERE partition_run_id=%s ORDER BY attempt_no",
            (run_id,),
        ).fetchall()
        assert [row["attempt_no"] for row in submissions] == [1, 2]
        assert submissions[0]["changes"]["first"] is True
        assert submissions[1]["changes"]["identical"] is True
        assert submissions[1]["requested_by"] == "bob"
        assert submissions[1]["parameters"]["datasets"][0]["grid"]["requested_grid_level"] == 4
        grid = connection.execute(
            "SELECT partition_run_id, partition_status, quality_status, output_version "
            "FROM partition_data_unit_grid_status WHERE band_unit_id=%s AND grid_type='geohash' AND grid_level=4",
            (partition_target["band_unit_id"],),
        ).fetchone()
        assert grid["partition_run_id"] == run_id
        assert grid["partition_status"] == "pending"
        assert grid["quality_status"] == "pending"
        assert grid["output_version"] is None


def test_repeat_submission_from_another_load_batch_records_the_batch_difference(dsn, partition_target) -> None:
    repository = OpenGaussSceneRepository(dsn)
    first = repository.create_partition_run(_request(partition_target, "a"))
    run_id = str(first["partition_run_id"])
    _finish_first_attempt(dsn, run_id)

    second = repository.create_partition_run(
        _request(partition_target, "b", load_batch_key="alternate_load_batch_id"),
        requested_by="bob",
    )

    assert second["partition_run_id"] == run_id
    assert second["merged"] is True
    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        changes = connection.execute(
            "SELECT changes FROM partition_run_submissions WHERE partition_run_id=%s AND attempt_no=2",
            (run_id,),
        ).fetchone()["changes"]
        fields = {item["field"]: (item["before"], item["after"]) for item in changes["fields"]}
        assert fields["source_batch_ids"] == (
            [partition_target["load_batch_id"]], [partition_target["alternate_load_batch_id"]],
        )
        assert changes["identical"] is False


def test_submission_history_tracks_bound_task_and_terminal_status(dsn, partition_target) -> None:
    token = partition_target["token"]
    batch_id = f"merge-batch-{token}"
    task_id = f"merge-task-{token}-1"
    with psycopg.connect(dsn) as connection:
        connection.execute(
            "INSERT INTO partition_batches (batch_id,batch_name,data_type,source_schema,normalized_payload,status) "
            "VALUES (%s,%s,'optical','{}'::jsonb,'{}'::jsonb,'running')",
            (batch_id, batch_id),
        )
        connection.execute(
            "INSERT INTO partition_job_attempts (task_id,batch_id,asset_ids,operation,status,attempt_no,payload) "
            "VALUES (%s,%s,'{}'::text[],'run','queued',1,'{}'::jsonb)",
            (task_id, batch_id),
        )
        connection.commit()
    repository = OpenGaussSceneRepository(dsn)
    first = repository.create_partition_run(_request(partition_target, "a"), requested_by="alice")
    run_id = str(first["partition_run_id"])

    repository.bind_partition_task(run_id, task_id)
    repository.update_partition_task(task_id, "completed", {"datasets": []})

    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        submission = connection.execute(
            "SELECT status, task_ids, started_at FROM partition_run_submissions "
            "WHERE partition_run_id=%s ORDER BY attempt_no DESC LIMIT 1",
            (run_id,),
        ).fetchone()
        assert submission["status"] == "completed"
        assert submission["task_ids"] == [task_id]
        assert submission["started_at"] is not None
        run = connection.execute(
            "SELECT status FROM partition_runs WHERE partition_run_id=%s",
            (run_id,),
        ).fetchone()
        assert run["status"] == "completed"


def test_repeat_submission_conflicts_with_an_in_progress_record(dsn, partition_target) -> None:
    repository = OpenGaussSceneRepository(dsn)
    first = repository.create_partition_run(_request(partition_target, "a"))
    run_id = str(first["partition_run_id"])

    with pytest.raises(PartitionRunInProgressError) as exc_info:
        repository.create_partition_run(_request(partition_target, "b"))

    assert exc_info.value.partition_run_id == run_id
    assert exc_info.value.status == "pending"
    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        open_records = connection.execute(
            "SELECT count(*) AS total FROM partition_runs "
            "WHERE merge_key=(SELECT merge_key FROM partition_runs WHERE partition_run_id=%s) AND merge_state='open'",
            (run_id,),
        ).fetchone()
        assert open_records["total"] == 1


def test_different_grid_level_merges_and_rebuilds_the_current_grid_state(dsn, partition_target) -> None:
    repository = OpenGaussSceneRepository(dsn)
    first = repository.create_partition_run(_request(partition_target, "a", grid_level=4))
    run_id = str(first["partition_run_id"])
    _finish_first_attempt(dsn, run_id)

    second = repository.create_partition_run(_request(partition_target, "b", grid_level=5), requested_by="bob")

    assert second["partition_run_id"] == run_id
    assert second["merged"] is True
    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        previous = connection.execute(
            "SELECT quality_status FROM partition_data_unit_grid_status "
            "WHERE partition_run_id=%s AND grid_type='geohash' AND grid_level=4",
            (run_id,),
        ).fetchone()
        assert previous is None, "the previous attempt's live grid row is rebuilt, not kept as current state"
        current = connection.execute(
            "SELECT partition_run_id, partition_status, quality_status FROM partition_data_unit_grid_status "
            "WHERE band_unit_id=%s AND grid_type='geohash' AND grid_level=5",
            (partition_target["band_unit_id"],),
        ).fetchone()
        assert current["partition_run_id"] == run_id
        assert current["partition_status"] == "pending"
        assert current["quality_status"] == "pending"
        changes = connection.execute(
            "SELECT changes FROM partition_run_submissions WHERE partition_run_id=%s AND attempt_no=2",
            (run_id,),
        ).fetchone()["changes"]
        fields = {item["field"]: (item["before"], item["after"]) for item in changes["fields"]}
        assert fields["datasets.%s|.grid.requested_grid_level" % partition_target["dataset_id"]] == (4, 5)


def test_different_grid_type_opens_a_separate_record(dsn, partition_target) -> None:
    repository = OpenGaussSceneRepository(dsn)
    geohash_run = repository.create_partition_run(_request(partition_target, "a", grid_type="geohash", grid_level=4))
    geohash_run_id = str(geohash_run["partition_run_id"])
    _finish_first_attempt(dsn, geohash_run_id)

    mgrs_run = repository.create_partition_run(_request(partition_target, "b", grid_type="mgrs", grid_level=1))

    assert mgrs_run["partition_run_id"] != geohash_run_id
    assert mgrs_run["merged"] is False
    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        previous = connection.execute(
            "SELECT quality_status FROM partition_data_unit_grid_status "
            "WHERE partition_run_id=%s AND grid_type='geohash' AND grid_level=4",
            (geohash_run_id,),
        ).fetchone()
        assert previous["quality_status"] == "pass"


def test_backfill_rekeys_an_open_record_so_a_new_level_merges(dsn, partition_target) -> None:
    from cube_web.services.scene_domain_schema import backfill_partition_run_merge_keys

    repository = OpenGaussSceneRepository(dsn)
    first = repository.create_partition_run(_request(partition_target, "a", grid_level=4))
    run_id = str(first["partition_run_id"])
    _finish_first_attempt(dsn, run_id)
    with psycopg.connect(dsn) as connection:
        connection.execute(
            "UPDATE partition_runs SET merge_key=%s, merge_state='open' WHERE partition_run_id=%s",
            (f"legacy-key-{partition_target['token']}", run_id),
        )
        connection.commit()

    with psycopg.connect(dsn) as connection:
        assert backfill_partition_run_merge_keys(connection, partition_run_ids=[run_id]) >= 1

    second = repository.create_partition_run(_request(partition_target, "b", grid_level=5))

    assert second["partition_run_id"] == run_id
    assert second["merged"] is True


def test_ingest_seals_the_record_and_the_next_submission_opens_a_new_one(dsn, partition_target) -> None:
    repository = OpenGaussSceneRepository(dsn)
    first = repository.create_partition_run(_request(partition_target, "a"))
    run_id = str(first["partition_run_id"])
    _finish_first_attempt(dsn, run_id)
    with psycopg.connect(dsn) as connection:
        connection.execute(
            "INSERT INTO ingest_runs (ingest_run_id,partition_run_id,dataset_id,status) VALUES (%s,%s,%s,'queued')",
            (f"ingest-run-{partition_target['token']}-a", run_id, partition_target["dataset_id"]),
        )
        connection.commit()

    second = repository.create_partition_run(_request(partition_target, "b"))

    assert second["partition_run_id"] != run_id
    assert second["merged"] is False
    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        previous = connection.execute(
            "SELECT merge_state FROM partition_runs WHERE partition_run_id=%s",
            (run_id,),
        ).fetchone()
        assert previous["merge_state"] == "closed"
