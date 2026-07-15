#!/usr/bin/env python3
"""Preview or reset the M2 partition domain with explicit development guards."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import Any

from cube_split import runtime_config

from cube_web.services.partition_domain_schema import (
    LEGACY_ALLOWLIST,
    NEW_DOMAIN_OBJECTS,
    NEW_DOMAIN_TABLES,
    apply_schema,
    inventory_partition_objects,
)

DROP_ORDER = (
    "partition_domain_outbox", "partition_quality_warn_approvals", "partition_publications",
    "partition_quality_errors", "partition_quality_results", "partition_quality_runs", "partition_indexes",
    "partition_tiles", "partition_grid_cells", "partition_output_versions", "partition_dataset_bands",
    "partition_dataset_assets", "partition_datasets", "quality_reports", "partition_job_attempts",
    "partition_assets", "partition_batches",
)


@dataclass(frozen=True)
class ResetPlan:
    drop_statements: tuple[str, ...]
    inventory: tuple[tuple[str, str, str | None, str], ...]


def _actual_database(connection: Any) -> str:
    if hasattr(connection, "current_database"):
        return str(connection.current_database)
    row = connection.execute("SELECT current_database()").fetchone()
    return str(row[0])


def validate_reset_guards(connection: Any, database_name: str, dangerous: bool, environment: str) -> None:
    if environment != "development":
        raise RuntimeError("partition-domain reset requires CUBE_WEB_ENV=development")
    if not dangerous:
        raise RuntimeError("partition-domain reset requires --dangerously-reset-partition-domain")
    actual = _actual_database(connection)
    if database_name != actual:
        raise RuntimeError(f"requested database {database_name!r} does not match actual database {actual!r}")


def build_reset_plan(connection: Any) -> ResetPlan:
    inventory = inventory_partition_objects(connection)
    objects = tuple(sorted(inventory.objects, key=lambda row: (row[0], row[1], row[2] or "", row[3])))
    known = NEW_DOMAIN_TABLES | NEW_DOMAIN_OBJECTS | LEGACY_ALLOWLIST
    # Indexes, constraints and owned sequences disappear with their known
    # table.  Independently named relation-like objects must still block reset.
    dependent_kinds = {"i", "S"}
    unknown = [row for row in objects if row[3] not in known and row[0] not in dependent_kinds]
    if unknown:
        names = ", ".join(row[3] for row in unknown)
        raise RuntimeError(f"refusing reset with unknown partition/quality object(s): {names}")
    present = {row[3] for row in objects}
    drops = tuple(f"DROP TABLE IF EXISTS {name} CASCADE" for name in DROP_ORDER if name in present or name in known)
    return ResetPlan(drop_statements=drops, inventory=objects)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-name", required=True)
    parser.add_argument("--dangerously-reset-partition-domain", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--purge-partition-objects", action="store_true")
    return parser.parse_args()


def _connect() -> Any:
    import psycopg

    return psycopg.connect(runtime_config.postgres_dsn())


def _bootstrap_scheduling(connection: Any) -> None:
    """Recreate the three legacy scheduler tables after an approved full reset."""
    statements = (
        """CREATE TABLE partition_batches (
          batch_id TEXT PRIMARY KEY, batch_name TEXT NOT NULL, data_type TEXT NOT NULL, source_system TEXT,
          source_schema JSONB NOT NULL, normalized_payload JSONB NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
          priority INT NOT NULL DEFAULT 0, attempt_count INT NOT NULL DEFAULT 0, max_auto_retries INT NOT NULL DEFAULT 1,
          last_task_id TEXT, last_error TEXT, quality_status TEXT, quality_report_id TEXT, quality_failure_reason TEXT,
          ingest_status TEXT NOT NULL DEFAULT 'not_ready', ingest_job_id TEXT, ingest_error TEXT, ingested_at TIMESTAMPTZ,
          partitioned_at TIMESTAMPTZ, manual_required_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )""",
        """CREATE TABLE partition_assets (
          asset_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL REFERENCES partition_batches(batch_id) ON DELETE CASCADE,
          data_type TEXT NOT NULL, scene_id TEXT, source_uri TEXT NOT NULL, asset_payload JSONB NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending', attempt_count INT NOT NULL DEFAULT 0, last_error TEXT, last_run_dir TEXT,
          partitioned_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )""",
        """CREATE TABLE partition_job_attempts (
          task_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL REFERENCES partition_batches(batch_id) ON DELETE CASCADE,
          asset_ids TEXT[] NOT NULL DEFAULT '{}', operation TEXT NOT NULL, status TEXT NOT NULL, attempt_no INT NOT NULL,
          payload JSONB NOT NULL, runner_result JSONB, error_type TEXT, error_message TEXT, requested_by TEXT NOT NULL DEFAULT 'system',
          source_task_id TEXT, retry_strategy TEXT, failure_reason TEXT, started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )""",
        "CREATE INDEX idx_partition_batches_status ON partition_batches(status)",
        "CREATE INDEX idx_partition_batches_type_status ON partition_batches(data_type, status)",
        "CREATE INDEX idx_partition_assets_batch_status ON partition_assets(batch_id, status)",
        "CREATE INDEX idx_partition_attempts_batch ON partition_job_attempts(batch_id, created_at DESC)",
    )
    with connection.cursor() as cursor:
        for statement in statements:
            cursor.execute(statement)


def main() -> int:
    args = _parse_args()
    with _connect() as connection:
        plan = build_reset_plan(connection)
        print("inventory=" + repr(plan.inventory))
        for statement in plan.drop_statements:
            print(statement + ";")
        print("execution=" + str(bool(args.execute)).lower())
        if not args.execute:
            return 0
        validate_reset_guards(connection, args.database_name, args.dangerously_reset_partition_domain, os.getenv("CUBE_WEB_ENV", ""))
        if args.purge_partition_objects:
            raise RuntimeError("object purge must be performed by the configured object lifecycle service")
        with connection.cursor() as cursor:
            for statement in plan.drop_statements:
                cursor.execute(statement)
        connection.commit()
        _bootstrap_scheduling(connection)
        apply_schema(connection)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
