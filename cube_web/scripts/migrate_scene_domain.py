#!/usr/bin/env python3
"""Install the production Dataset/Scene domain schema."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
for package_path in (ROOT / "cube_encoder", ROOT / "cube_split", ROOT / "cube_web"):
    if str(package_path) not in sys.path:
        sys.path.insert(0, str(package_path))

from cube_split import runtime_config  # noqa: E402

from cube_web.services.config_store import PostgresConfigStore  # noqa: E402
from cube_web.services.partition_domain_schema import apply_schema as apply_partition_domain_schema  # noqa: E402
from cube_web.services.partition_job_store import PostgresPartitionJobStore  # noqa: E402
from cube_web.services.scene_domain_schema import (  # noqa: E402
    SCENE_DOMAIN_SCHEMA_VERSION,
    apply_scene_domain_schema,
    backfill_scene_band_unit_ids,
    backfill_partition_grid_status,
    backfill_scene_resolution_metadata,
    record_scene_domain_install,
    schema_statements,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Install the production Dataset/Scene domain in an empty database."
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="install the schema; without this flag only the DDL plan is reported",
    )
    args = parser.parse_args()
    if not args.execute:
        print(json.dumps({
            "schema_version": SCENE_DOMAIN_SCHEMA_VERSION,
            "mode": "preview",
            "includes_partition_and_quality_domain": True,
            "schema_statements": len(schema_statements()),
        }, sort_keys=True))
        return 0

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - deployment dependency guard
        raise RuntimeError("Dataset/Scene OpenGauss installation requires psycopg") from exc

    # runtime_config resolves process environment, CUBE_WEB_ENV_FILE and the
    # ignored local .cube_web.env without printing the DSN or credentials.
    dsn = runtime_config.postgres_dsn()
    PostgresPartitionJobStore(dsn).ensure_schema()
    with psycopg.connect(dsn, client_encoding="UTF8") as connection:
        # Keep the full production install explicit. The scheduler store also
        # performs this idempotent ensure during runtime recovery.
        apply_partition_domain_schema(connection)
        report = apply_scene_domain_schema(connection, commit=False, record_version=False)
        resolution_rows_updated = backfill_scene_resolution_metadata(connection, commit=False)
        band_unit_rows_updated = backfill_scene_band_unit_ids(connection, commit=False)
        partition_grid_rows_updated = backfill_partition_grid_status(connection, commit=False)
        record_scene_domain_install(connection, {
            **asdict(report),
            "resolution_rows_updated": resolution_rows_updated,
            "band_unit_rows_updated": band_unit_rows_updated,
            "partition_grid_rows_updated": partition_grid_rows_updated,
        })
    PostgresConfigStore(dsn).ensure_schema()
    print(json.dumps({
        **asdict(report),
        "band_unit_rows_updated": band_unit_rows_updated,
        "resolution_rows_updated": resolution_rows_updated,
        "partition_grid_rows_updated": partition_grid_rows_updated,
        "status": "completed",
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
