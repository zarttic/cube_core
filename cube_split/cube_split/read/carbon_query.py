from __future__ import annotations

import argparse
import json
from typing import Any

from grid_core.sdk import CubeEncoderSDK

from cube_split import runtime_config


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query carbon satellite observations by AOI and time")
    parser.add_argument("--metadata-backend", default="postgres", choices=["postgres"], help="Metadata backend")
    parser.add_argument("--postgres-dsn", default=runtime_config.postgres_dsn(), help="PostgreSQL DSN when metadata-backend=postgres")
    parser.add_argument("--bbox", nargs=4, type=float, required=True, metavar=("MIN_LON", "MIN_LAT", "MAX_LON", "MAX_LAT"))
    parser.add_argument("--time-start", required=True, help="Start time bucket such as 20201231")
    parser.add_argument("--time-end", required=True, help="End time bucket such as 20201231")
    parser.add_argument("--quality-flags", nargs="*", default=None, help="Quality flags to include")
    parser.add_argument("--product-type", default="xco2", help="Product type")
    parser.add_argument("--grid-type", default="isea4h", choices=["s2", "mgrs", "isea4h"])
    parser.add_argument("--grid-level", type=int, default=5)
    parser.add_argument("--cube-version", default="v1")
    parser.add_argument("--limit", type=int, default=10000)
    return parser.parse_args()


def _decode_json(value: Any) -> Any:
    if not value:
        return {}
    if isinstance(value, (dict, list)):
        return value
    return json.loads(value)


def _postgres_row_to_dict(row: dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    item["footprint_geojson"] = _decode_json(item.get("footprint_geojson"))
    item["metadata_json"] = _decode_json(item.get("metadata_json"))
    if item.get("acq_time") is not None:
        item["acq_time"] = item["acq_time"].isoformat().replace("+00:00", "Z")
    if item.get("ingest_time") is not None:
        item["ingest_time"] = item["ingest_time"].isoformat().replace("+00:00", "Z")
    return item


def query_carbon_observations(
    *,
    postgres_dsn: str = "",
    metadata_backend: str = "postgres",
    bbox: list[float],
    time_start: str,
    time_end: str,
    quality_flags: list[str] | None = None,
    product_type: str = "xco2",
    grid_type: str = "isea4h",
    grid_level: int = 5,
    cube_version: str = "v1",
    limit: int = 10000,
) -> list[dict[str, Any]]:
    sdk = CubeEncoderSDK()
    cells = sdk.cover_compact(
        grid_type=grid_type,
        requested_grid_level=grid_level,
        cover_mode="intersect",
        bbox=bbox,
        crs="EPSG:4326",
    )
    space_codes = sorted({cell.space_code for cell in cells})
    if not space_codes:
        return []

    min_lon, min_lat, max_lon, max_lat = map(float, bbox)
    select_sql = """
        SELECT
          satellite, product_type, observation_id, acq_time, time_bucket,
          grid_type, grid_level, space_code, st_code, xco2, quality_flag,
          center_lon, center_lat, footprint_geojson, source_uri, source_index,
          metadata_json, cube_version, run_id, ingest_time
        FROM rs_carbon_observation_fact
    """
    if metadata_backend != "postgres":
        raise ValueError(f"Unsupported metadata_backend: {metadata_backend}")
    postgres_dsn = postgres_dsn or runtime_config.postgres_dsn()
    if not postgres_dsn:
        raise ValueError("postgres_dsn is required when metadata_backend=postgres")
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ModuleNotFoundError as exc:
        raise RuntimeError("Postgres backend requires `psycopg` package") from exc

    where = [
        "grid_type = %s",
        "grid_level = %s",
        "time_bucket >= %s",
        "time_bucket <= %s",
        "product_type = %s",
        "cube_version = %s",
        "space_code = ANY(%s)",
    ]
    params: list[Any] = [
        grid_type,
        int(grid_level),
        str(time_start),
        str(time_end),
        product_type,
        cube_version,
        space_codes,
    ]
    if quality_flags:
        normalized_quality_flags = [str(flag) for flag in quality_flags]
        where.append("quality_flag = ANY(%s)")
        params.append(normalized_quality_flags)
    where.extend(
        [
            "center_lon >= %s",
            "center_lon <= %s",
            "center_lat >= %s",
            "center_lat <= %s",
        ]
    )
    params.extend([min_lon, max_lon, min_lat, max_lat, max(1, int(limit))])
    sql = f"""
        {select_sql}
        WHERE {' AND '.join(where)}
        ORDER BY acq_time, observation_id
        LIMIT %s
    """
    with psycopg.connect(postgres_dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [_postgres_row_to_dict(row) for row in cur.fetchall()]


def summarize_xco2(rows: list[dict[str, Any]]) -> dict[str, float | int | None]:
    if not rows:
        return {"count": 0, "xco2_min": None, "xco2_max": None, "xco2_avg": None}
    values = [float(row["xco2"]) for row in rows]
    return {
        "count": len(values),
        "xco2_min": min(values),
        "xco2_max": max(values),
        "xco2_avg": sum(values) / len(values),
    }


def main() -> None:
    args = _parse_args()
    rows = query_carbon_observations(
        postgres_dsn=args.postgres_dsn,
        metadata_backend=args.metadata_backend,
        bbox=args.bbox,
        time_start=args.time_start,
        time_end=args.time_end,
        quality_flags=args.quality_flags,
        product_type=args.product_type,
        grid_type=args.grid_type,
        grid_level=args.grid_level,
        cube_version=args.cube_version,
        limit=args.limit,
    )
    print(json.dumps({"summary": summarize_xco2(rows), "rows": rows}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
