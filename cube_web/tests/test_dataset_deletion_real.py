"""Real-schema guard for the deletion plan.

Unit tests drive a fake cursor that accepts any SQL, so a predicate naming a
column the table does not have stays invisible there.  This test EXPLAINs every
step against the live schema instead: the planner validates tables, columns and
parameter types without deleting a single row.

Follows the repository convention for real-database tests: a missing DSN or an
unreachable database fails loudly instead of silently skipping (see
``pytest.ini`` markers).
"""

from __future__ import annotations

import os

import psycopg
import pytest

from cube_web.services.dataset_deletion import (
    PLAN_VERSION,
    DeletionPlan,
    build_steps,
    validate_deletion_plan,
)

pytestmark = pytest.mark.dataset_deletion_real

ALL_IDENTIFIERS = {
    "scene_ids": ["scene-selftest"],
    "draft_ids": ["draft-selftest"],
    "load_batch_ids": ["load-batch-selftest"],
    "scheduler_batch_ids": ["batch-selftest"],
    "partition_run_ids": ["run-selftest"],
    "output_versions": ["version-selftest"],
    "ingest_job_ids": ["ingest-selftest"],
}


@pytest.fixture(scope="module")
def open_gauss_connection():
    dsn = os.getenv("CUBE_WEB_POSTGRES_DSN", "").strip() or _dsn_from_runtime_config()
    if not dsn:
        pytest.fail("dataset deletion schema tests require CUBE_WEB_POSTGRES_DSN")
    try:
        connection = psycopg.connect(dsn, connect_timeout=8)
    except Exception as exc:
        pytest.fail(f"database not reachable: {type(exc).__name__}: {exc}")
    connection.autocommit = True
    try:
        yield connection
    finally:
        connection.close()


def _dsn_from_runtime_config() -> str:
    try:
        from cube_split import runtime_config

        return str(runtime_config.postgres_dsn() or "")
    except Exception:
        return ""


def test_every_plan_step_explains_against_the_live_schema(open_gauss_connection) -> None:
    plan = DeletionPlan(
        dataset_id="dataset-deletion-selftest",
        dataset_title="selftest",
        requested_by="pytest",
        identifiers={key: list(value) for key, value in ALL_IDENTIFIERS.items()},
        object_prefix="partition/dataset-deletion-selftest/",
        steps=build_steps(
            open_gauss_connection,
            dataset_id="dataset-deletion-selftest",
            identifiers=ALL_IDENTIFIERS,
        ),
        plan_version=PLAN_VERSION,
    )

    failures = validate_deletion_plan(open_gauss_connection, plan)

    assert failures == [], "plan steps rejected by the database: " + "; ".join(
        f"{name}: {error}" for name, error in failures
    )
    # The self-test dataset id matches nothing, so the plan is a no-op by design.
    assert len(plan.steps) >= 30, "the plan must cover the whole delete chain"
