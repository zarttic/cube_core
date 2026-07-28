#!/usr/bin/env python3
"""Publish ready partition tiles that completed the existing manual ingest flow."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
for package_path in (ROOT / "cube_encoder", ROOT / "cube_split", ROOT / "cube_web"):
    if str(package_path) not in sys.path:
        sys.path.insert(0, str(package_path))

from cube_split import runtime_config  # noqa: E402
from cube_web.services.partition_domain_schema import (  # noqa: E402
    apply_schema,
    backfill_partition_tile_publication_status,
    count_partition_tile_publication_backfill_candidates,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill published partition tiles from completed ingest units.")
    parser.add_argument("--execute", action="store_true", help="apply the schema addition and publish eligible tiles")
    args = parser.parse_args()

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - deployment dependency guard
        raise RuntimeError("Partition tile publication backfill requires psycopg") from exc

    dsn = runtime_config.postgres_dsn()
    with psycopg.connect(dsn, client_encoding="UTF8") as connection:
        candidates = count_partition_tile_publication_backfill_candidates(connection)
        if not args.execute:
            print(json.dumps({"mode": "preview", "candidate_tiles": candidates}, sort_keys=True))
            return 0
        apply_schema(connection)
        updated = backfill_partition_tile_publication_status(connection)
    print(json.dumps({"mode": "execute", "candidate_tiles": candidates, "published_tiles": updated}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
