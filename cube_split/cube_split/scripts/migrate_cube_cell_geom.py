from __future__ import annotations

import argparse
import json
from typing import Any

from grid_core.sdk import CubeEncoderSDK

from cube_split.ingest.ray_ingest_job import cell_geometry_geojson
from cube_split.runtime_config import postgres_dsn


ALTER_SQL = """
ALTER TABLE rs_cube_cell_fact
ADD COLUMN IF NOT EXISTS cell_geom geometry(Polygon, 4326);
"""


def _column_exists(conn: Any) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1
              FROM information_schema.columns
              WHERE table_name = 'rs_cube_cell_fact' AND column_name = 'cell_geom'
            )
            """
        )
        return bool(cur.fetchone()[0])


def _missing_cells(conn: Any, *, column_exists: bool) -> list[tuple[str, int, str]]:
    missing_condition = "AND cell_geom IS NULL" if column_exists else ""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT DISTINCT grid_type, grid_level, space_code
            FROM rs_cube_cell_fact
            WHERE grid_type IN ('geohash', 'mgrs', 'isea4h')
            {missing_condition}
            ORDER BY grid_type, grid_level, space_code
            """
        )
        return [(str(row[0]), int(row[1]), str(row[2])) for row in cur.fetchall()]


def backfill(conn: Any, cells: list[tuple[str, int, str]]) -> int:
    sdk = CubeEncoderSDK()
    updated = 0
    with conn.cursor() as cur:
        for grid_type, grid_level, space_code in cells:
            geometry_json = cell_geometry_geojson(
                grid_type=grid_type,
                grid_level=grid_level,
                space_code=space_code,
                sdk=sdk,
            )
            cur.execute(
                """
                UPDATE rs_cube_cell_fact
                SET cell_geom = ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)
                WHERE grid_type = %s
                  AND grid_level = %s
                  AND space_code = %s
                  AND cell_geom IS NULL
                """,
                (geometry_json, grid_type, grid_level, space_code),
            )
            updated += max(0, int(cur.rowcount or 0))
    return updated


def validate(conn: Any) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              SUM(CASE WHEN cell_geom IS NULL THEN 1 ELSE 0 END),
              SUM(CASE WHEN cell_geom IS NOT NULL
                  AND (GeometryType(cell_geom) <> 'POLYGON' OR ST_SRID(cell_geom) <> 4326 OR NOT ST_IsValid(cell_geom))
                THEN 1 ELSE 0 END),
              SUM(CASE WHEN cell_geom IS NOT NULL
                  AND ST_NPoints(ST_ExteriorRing(cell_geom)) <> CASE grid_type
                    WHEN 'geohash' THEN 5
                    WHEN 'mgrs' THEN 5
                    WHEN 'isea4h' THEN 7
                    ELSE ST_NPoints(ST_ExteriorRing(cell_geom))
                  END
                THEN 1 ELSE 0 END)
            FROM rs_cube_cell_fact
            WHERE grid_type IN ('geohash', 'mgrs', 'isea4h')
            """
        )
        missing, invalid, wrong_point_count = (int(value or 0) for value in cur.fetchone())
    result = {"missing": missing, "invalid": invalid, "wrong_point_count": wrong_point_count}
    if any(result.values()):
        raise RuntimeError(f"cell_geom validation failed: {result}")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Add and backfill rs_cube_cell_fact.cell_geom without deleting legacy data")
    parser.add_argument("--postgres-dsn", default=postgres_dsn())
    parser.add_argument("--execute", action="store_true", help="Apply the additive migration; default is preview only")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.postgres_dsn:
        raise ValueError("OpenGauss-compatible PostgreSQL DSN is required")
    try:
        import psycopg
    except ModuleNotFoundError as exc:
        raise RuntimeError("cell_geom migration requires psycopg") from exc

    with psycopg.connect(args.postgres_dsn, client_encoding="UTF8") as conn:
        column_exists = _column_exists(conn)
        cells = _missing_cells(conn, column_exists=column_exists)
        print(json.dumps({"column_exists": column_exists, "distinct_cells_to_backfill": len(cells), "execute": args.execute}))
        if not args.execute:
            conn.rollback()
            return 0
        try:
            with conn.cursor() as cur:
                cur.execute(ALTER_SQL)
            updated = backfill(conn, cells)
            validation = validate(conn)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        print(json.dumps({"updated_rows": updated, "validation": validation}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
