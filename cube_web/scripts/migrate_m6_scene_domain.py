#!/usr/bin/env python3
"""Install the additive M6 scene domain without modifying legacy tables."""

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
from cube_web.services.m6_scene_schema import (  # noqa: E402
    M6_SCENE_SCHEMA_VERSION,
    apply_m6_scene_schema,
    schema_statements,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create and backfill the additive M6 Dataset/Scene domain. Legacy tables are retained."
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="apply the migration; without this flag only the additive DDL plan is reported",
    )
    args = parser.parse_args()
    if not args.execute:
        print(json.dumps({
            "schema_version": M6_SCENE_SCHEMA_VERSION,
            "mode": "preview",
            "additive_statements": len(schema_statements()),
            "legacy_tables_retained": True,
        }, sort_keys=True))
        return 0

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - deployment dependency guard
        raise RuntimeError("M6 OpenGauss migration requires psycopg") from exc

    # runtime_config resolves process environment, CUBE_WEB_ENV_FILE and the
    # ignored local .cube_web.env without printing the DSN or credentials.
    with psycopg.connect(runtime_config.postgres_dsn(), client_encoding="UTF8") as connection:
        report = apply_m6_scene_schema(connection)
    print(json.dumps({**asdict(report), "status": "completed", "legacy_tables_retained": True}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
