"""Actual OpenGauss transaction tests for normalized quality runs."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from cube_web.routes.auth import Actor
from cube_web.services.partition_domain_store import OpenGaussPartitionDomainStore, get_partition_domain_store, set_partition_domain_store
from cube_web.services.publication_service import PublishRequest, publish_dataset, withdraw_publication
from cube_web.services.quality_contracts import QualityErrorFilter, RuleSnapshot
from cube_web.services.quality_export import stream_quality_errors
from cube_web.services.quality_repository import (
    NewQualityError,
    QualityCompletionConflict,
    QualityLease,
    QualityTriggerConflict,
    allocate_quality_run,
    complete_quality_run_if_current,
    count_quality_errors,
    iter_quality_errors,
    list_quality_errors,
    set_quality_lease_on_transaction,
    write_quality_error_batch,
)
from cube_web.services.quality_rules import RegisteredRule, RuleRegistry
from cube_web.services.quality_worker import claim_quality_runs, execute_quality_run, heartbeat_quality_run

pytestmark = pytest.mark.m3_real


@pytest.fixture(scope="module")
def open_gauss_tx():
    dsn = os.getenv("CUBE_WEB_POSTGRES_DSN", "").strip()
    if not dsn:
        pytest.fail("M3 quality repository tests require CUBE_WEB_POSTGRES_DSN")
    with psycopg.connect(dsn, connect_timeout=5) as connection:
        assert connection.execute("SELECT 1").fetchone() == (1,)

    def connect():
        return psycopg.connect(dsn, row_factory=dict_row)

    connect.store = lambda: OpenGaussPartitionDomainStore(dsn=dsn)
    return connect


@pytest.fixture
def seeded_outputs(open_gauss_tx):
    token = uuid4().hex
    batch_id = f"m3-quality-batch-{token}"
    task_id = f"m3-quality-task-{token}"
    dataset_id = f"m3-quality-dataset-{token}"
    dataset_code = f"M3-Q-{token}"
    current_version = f"m3-quality-current-{token}"
    old_version = f"m3-quality-old-{token}"
    with open_gauss_tx() as connection:
        connection.execute(
            "INSERT INTO partition_batches (batch_id, batch_name, data_type, source_schema, normalized_payload, status) "
            "VALUES (%s, %s, 'optical', '{}'::jsonb, '{}'::jsonb, 'running')",
            (batch_id, batch_id),
        )
        connection.execute(
            "INSERT INTO partition_job_attempts (task_id, batch_id, asset_ids, operation, status, attempt_no, payload) "
            "VALUES (%s, %s, '{}'::text[], 'run', 'completed', 1, '{}'::jsonb)",
            (task_id, batch_id),
        )
        connection.execute(
            "INSERT INTO partition_datasets "
            "(dataset_id, batch_id, dataset_code, dataset_title, data_type, attributes, grid_type, requested_grid_level, "
            "requested_grid_level_name, partition_method, cover_mode, partition_status, partition_completed_at) "
            "VALUES (%s, %s, %s, %s, 'optical', '{}'::jsonb, 'geohash', 5, 'Geohash precision 5', 'logical', "
            "'intersect', 'completed', now())",
            (dataset_id, batch_id, dataset_code, dataset_code),
        )
        for version in (old_version, current_version):
            connection.execute(
                "INSERT INTO partition_output_versions "
                "(dataset_id, output_version, task_id, grid_type, requested_grid_level, requested_grid_level_name, "
                "partition_method, status, object_prefix, completed_at) "
                "VALUES (%s, %s, %s, 'geohash', 5, 'Geohash precision 5', 'logical', 'completed', %s, now())",
                (dataset_id, version, task_id, f"partition/{dataset_id}/versions/{version}/"),
            )
        connection.execute("UPDATE partition_datasets SET current_output_version = %s WHERE dataset_id = %s", (current_version, dataset_id))
        connection.commit()
    try:
        yield {
            "dataset_id": dataset_id,
            "dataset_code": dataset_code,
            "current_version": current_version,
            "old_version": old_version,
        }
    finally:
        with open_gauss_tx() as connection:
            connection.execute("UPDATE partition_datasets SET current_quality_run_id = NULL WHERE dataset_id = %s", (dataset_id,))
            connection.execute("DELETE FROM partition_quality_errors WHERE dataset_id = %s", (dataset_id,))
            connection.execute("DELETE FROM partition_quality_results WHERE dataset_id = %s", (dataset_id,))
            connection.execute("DELETE FROM partition_quality_warn_approvals WHERE dataset_id = %s", (dataset_id,))
            connection.execute("DELETE FROM partition_publications WHERE dataset_id = %s", (dataset_id,))
            connection.execute("DELETE FROM partition_quality_runs WHERE dataset_id = %s", (dataset_id,))
            connection.execute("UPDATE partition_datasets SET current_output_version = NULL WHERE dataset_id = %s", (dataset_id,))
            connection.execute("DELETE FROM partition_output_versions WHERE dataset_id = %s", (dataset_id,))
            connection.execute("DELETE FROM partition_datasets WHERE dataset_id = %s", (dataset_id,))
            connection.execute("DELETE FROM partition_job_attempts WHERE task_id = %s", (task_id,))
            connection.execute("DELETE FROM partition_batches WHERE batch_id = %s", (batch_id,))
            connection.commit()


def _snapshots() -> tuple[RuleSnapshot, ...]:
    return (
        RuleSnapshot(
            code="output_count_consistency",
            name="Output count consistency",
            applicability={"data_types": ["optical"]},
            mandatory=True,
            parameters={"batch_size": 1000},
            implementation_version="1.0.0",
        ),
    )


def _allocate(
    connection,
    *,
    dataset_id: str,
    output_version: str,
    run_id: UUID,
    event_id: UUID | None = None,
    expected_current: bool = True,
):
    return allocate_quality_run(
        connection,
        dataset_id=dataset_id,
        output_version=output_version,
        expected_current_output_version=output_version if event_id is None and expected_current else None,
        quality_run_id=run_id,
        trigger_event_id=event_id,
        trigger="automatic" if event_id is not None else "manual",
        requested_by="m3-quality-real",
        rule_set_version="2026.07.14-v1",
        rule_snapshot=_snapshots(),
    )


def _start(connection, run_id: UUID, *, worker_id: str = "m3-real-worker") -> QualityLease:
    connection.execute(
        "UPDATE partition_quality_runs SET status = 'running', claimed_by = %s, claimed_at = now(), attempt_count = 1, started_at = now() "
        "WHERE quality_run_id = %s",
        (worker_id, run_id),
    )
    lease = QualityLease(quality_run_id=run_id, claimed_by=worker_id, attempt_count=1)
    set_quality_lease_on_transaction(connection, lease)
    return lease


def test_automatic_redelivery_returns_same_run_after_identity_verification(open_gauss_tx, seeded_outputs: dict[str, str]) -> None:
    event_id, run_id = uuid4(), uuid4()
    with open_gauss_tx() as connection:
        first = _allocate(
            connection,
            dataset_id=seeded_outputs["dataset_id"],
            output_version=seeded_outputs["current_version"],
            run_id=run_id,
            event_id=event_id,
        )
        second = _allocate(
            connection,
            dataset_id=seeded_outputs["dataset_id"],
            output_version=seeded_outputs["current_version"],
            run_id=uuid4(),
            event_id=event_id,
        )
        assert second.quality_run_id == first.quality_run_id
        assert second.quality_sequence == first.quality_sequence


def test_automatic_redelivery_with_different_output_raises_trigger_conflict(open_gauss_tx, seeded_outputs: dict[str, str]) -> None:
    event_id = uuid4()
    with open_gauss_tx() as connection:
        _allocate(
            connection,
            dataset_id=seeded_outputs["dataset_id"],
            output_version=seeded_outputs["current_version"],
            run_id=uuid4(),
            event_id=event_id,
        )
        with pytest.raises(QualityTriggerConflict):
            _allocate(
                connection,
                dataset_id=seeded_outputs["dataset_id"],
                output_version=seeded_outputs["old_version"],
                run_id=uuid4(),
                event_id=event_id,
            )


def test_explicit_completed_historical_output_does_not_change_current_fields(open_gauss_tx, seeded_outputs: dict[str, str]) -> None:
    with open_gauss_tx() as connection:
        run = _allocate(
            connection,
            dataset_id=seeded_outputs["dataset_id"],
            output_version=seeded_outputs["old_version"],
            run_id=uuid4(),
            expected_current=False,
        )
        dataset = connection.execute(
            "SELECT current_output_version, current_quality_run_id FROM partition_datasets WHERE dataset_id = %s",
            (seeded_outputs["dataset_id"],),
        ).fetchone()
        assert run.is_current is False
        assert dataset == {"current_output_version": seeded_outputs["current_version"], "current_quality_run_id": None}


def test_late_completion_does_not_overwrite_newer_current_run(open_gauss_tx, seeded_outputs: dict[str, str]) -> None:
    with open_gauss_tx() as connection:
        first = _allocate(
            connection,
            dataset_id=seeded_outputs["dataset_id"],
            output_version=seeded_outputs["current_version"],
            run_id=uuid4(),
        )
        second = _allocate(
            connection,
            dataset_id=seeded_outputs["dataset_id"],
            output_version=seeded_outputs["current_version"],
            run_id=uuid4(),
        )
        _start(connection, first.quality_run_id)
        assert (
            complete_quality_run_if_current(
                connection,
                quality_run_id=first.quality_run_id,
                terminal_status="fail",
                error_count=0,
                warning_count=0,
                results_complete=True,
                execution_error=None,
                completed_at=datetime.now(UTC),
            )
            is False
        )
        current = connection.execute(
            "SELECT current_quality_run_id, quality_status FROM partition_datasets WHERE dataset_id = %s",
            (seeded_outputs["dataset_id"],),
        ).fetchone()
        assert current == {"current_quality_run_id": second.quality_run_id, "quality_status": "pending"}
        with pytest.raises(QualityCompletionConflict):
            complete_quality_run_if_current(
                connection,
                quality_run_id=first.quality_run_id,
                terminal_status="fail",
                error_count=0,
                warning_count=0,
                results_complete=True,
                execution_error=None,
                completed_at=datetime.now(UTC),
            )


def test_error_batches_store_every_row_without_page_or_stream_truncation(open_gauss_tx, seeded_outputs: dict[str, str]) -> None:
    with open_gauss_tx() as connection:
        run = _allocate(
            connection,
            dataset_id=seeded_outputs["dataset_id"],
            output_version=seeded_outputs["current_version"],
            run_id=uuid4(),
        )
        _start(connection, run.quality_run_id)
        errors = [
            NewQualityError(
                quality_error_id=uuid4(),
                quality_run_id=run.quality_run_id,
                rule_code="bounds",
                error_code="outside_extent",
                message=f"error {number}",
                row_number=number,
                field="bbox",
                context={"number": number},
            )
            for number in range(2505)
        ]
        for offset in range(0, len(errors), 1000):
            assert write_quality_error_batch(connection, quality_run_id=run.quality_run_id, errors=errors[offset : offset + 1000]) <= 1000
        filters = QualityErrorFilter(rule_code="bounds", error_code="outside_extent", field="bbox")
        assert len(list_quality_errors(connection, quality_run_id=run.quality_run_id, filters=filters, limit=20, offset=0)) == 20
        assert count_quality_errors(connection, quality_run_id=run.quality_run_id, filters=filters) == 2505
        assert sum(1 for _ in iter_quality_errors(connection, quality_run_id=run.quality_run_id, filters=filters, fetch_size=257)) == 2505


def test_export_stream_count_matches_open_gauss(open_gauss_tx, seeded_outputs: dict[str, str]) -> None:
    previous_store = get_partition_domain_store()
    set_partition_domain_store(open_gauss_tx.store())
    try:
        with open_gauss_tx() as connection:
            run = _allocate(
                connection,
                dataset_id=seeded_outputs["dataset_id"],
                output_version=seeded_outputs["current_version"],
                run_id=uuid4(),
            )
            _start(connection, run.quality_run_id)
            errors = [
                NewQualityError(uuid4(), run.quality_run_id, "bounds", "outside_extent", "outside", field="bbox") for _ in range(1001)
            ]
            for offset in range(0, len(errors), 1000):
                write_quality_error_batch(connection, quality_run_id=run.quality_run_id, errors=errors[offset : offset + 1000])
        stream, count, _, _ = stream_quality_errors(run.quality_run_id, QualityErrorFilter(rule_code="bounds"), "csv")
        payload = b"".join(stream).decode("utf-8-sig")
        assert count == 1001
        assert len(payload.splitlines()) - 1 == count
    finally:
        set_partition_domain_store(previous_store)


def test_quality_run_lease_is_fenced_and_heartbeatable(open_gauss_tx, seeded_outputs: dict[str, str]) -> None:
    with open_gauss_tx() as connection:
        run = _allocate(
            connection,
            dataset_id=seeded_outputs["dataset_id"],
            output_version=seeded_outputs["current_version"],
            run_id=uuid4(),
        )
        leases = claim_quality_runs(connection, worker_id="lease-worker", limit=10)
        lease = next(item for item in leases if item.quality_run_id == run.quality_run_id)
        assert lease.attempt_count == 1
        connection.execute("UPDATE partition_quality_runs SET status = 'running' WHERE quality_run_id = %s", (run.quality_run_id,))
        assert heartbeat_quality_run(connection, lease=lease, now=datetime.now(UTC)) is True


def test_rule_exception_terminalizes_run_as_incomplete_error(open_gauss_tx, seeded_outputs: dict[str, str], monkeypatch) -> None:
    def broken_rule(_):
        raise RuntimeError("rule input is invalid")

    snapshot = RuleSnapshot(
        code="broken_rule",
        name="Broken rule",
        applicability={"data_types": ["optical"]},
        mandatory=True,
        parameters={},
        implementation_version="1.0.0",
    )
    registry = RuleRegistry(
        [
            RegisteredRule(
                code=snapshot.code,
                name=snapshot.name,
                applicability=snapshot.applicability,
                mandatory=snapshot.mandatory,
                parameters=snapshot.parameters,
                implementation_version=snapshot.implementation_version,
                evaluator=broken_rule,
            )
        ]
    )
    previous_store = get_partition_domain_store()
    set_partition_domain_store(open_gauss_tx.store())
    monkeypatch.setattr("cube_web.services.quality_worker.default_rule_registry", lambda: registry)
    try:
        with open_gauss_tx() as connection:
            run = allocate_quality_run(
                connection,
                dataset_id=seeded_outputs["dataset_id"],
                output_version=seeded_outputs["current_version"],
                expected_current_output_version=seeded_outputs["current_version"],
                quality_run_id=uuid4(),
                trigger_event_id=None,
                trigger="manual",
                requested_by="m3-quality-real",
                rule_set_version="2026.07.14-v1",
                rule_snapshot=(snapshot,),
            )
            lease = next(
                item for item in claim_quality_runs(connection, worker_id="rule-worker") if item.quality_run_id == run.quality_run_id
            )
        execute_quality_run(lease)
        with open_gauss_tx() as connection:
            terminal = connection.execute(
                "SELECT status, result_complete, last_error FROM partition_quality_runs WHERE quality_run_id = %s",
                (run.quality_run_id,),
            ).fetchone()
            result = connection.execute(
                "SELECT status, execution_error FROM partition_quality_results WHERE quality_run_id = %s AND rule_code = %s",
                (run.quality_run_id, snapshot.code),
            ).fetchone()
        assert terminal["status"] == "error"
        assert terminal["result_complete"] is False
        assert "quality rule failed" in terminal["last_error"]
        assert result == {"status": "error", "execution_error": "rule input is invalid"}
    finally:
        set_partition_domain_store(previous_store)


def test_publication_is_active_and_withdraws_in_database(open_gauss_tx, seeded_outputs: dict[str, str]) -> None:
    previous_store = get_partition_domain_store()
    set_partition_domain_store(open_gauss_tx.store())
    try:
        with open_gauss_tx() as connection:
            run = _allocate(
                connection,
                dataset_id=seeded_outputs["dataset_id"],
                output_version=seeded_outputs["current_version"],
                run_id=uuid4(),
            )
            connection.execute(
                "UPDATE partition_quality_runs SET status = 'pass', result_complete = TRUE, completed_at = now() WHERE quality_run_id = %s",
                (run.quality_run_id,),
            )
        publication = publish_dataset(seeded_outputs["dataset_id"], PublishRequest(), Actor("publisher", "admin"))
        assert publication.status == "active"
        assert publication.service_version_id == seeded_outputs["current_version"]
        withdrawn = withdraw_publication(
            seeded_outputs["dataset_id"], publication.publication_id, "superseded", Actor("publisher", "admin")
        )
        assert withdrawn.status == "withdrawn"
        assert withdrawn.publication_id == publication.publication_id
        assert withdrawn.withdrawal_reason == "superseded"
    finally:
        set_partition_domain_store(previous_store)
