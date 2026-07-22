from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cube_split import runtime_config
from cube_split.tile_probe import TileProbeMetric, report_tile_metrics


@dataclass(frozen=True)
class CarbonObservationFact:
    satellite: str
    product_type: str
    observation_id: str
    acq_time: str
    time_bucket: str
    grid_type: str
    grid_level: int
    space_code: str
    st_code: str
    xco2: float
    quality_flag: str | None
    center_lon: float
    center_lat: float
    footprint_geojson: str
    source_uri: str
    source_index: int | None
    metadata_json: str
    cube_version: str
    run_id: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest carbon satellite observation rows into metadata storage")
    parser.add_argument("--run-dir", default="", help="Directory containing carbon_observation_rows.jsonl")
    parser.add_argument("--rows-path", default="", help="Explicit carbon observation JSONL path")
    parser.add_argument("--job-id", required=True, help="Ingestion job id")
    parser.add_argument("--cube-version", default="v1", help="Carbon fact version")
    parser.add_argument("--metadata-backend", default="postgres", choices=["postgres"], help="Metadata backend")
    parser.add_argument("--postgres-dsn", default=runtime_config.postgres_dsn(), help="PostgreSQL DSN when metadata-backend=postgres")
    return parser.parse_args()


def ensure_carbon_tables_postgres(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rs_carbon_observation_fact (
              id BIGSERIAL PRIMARY KEY,
              satellite TEXT NOT NULL,
              product_type TEXT NOT NULL,
              observation_id TEXT NOT NULL,
              acq_time TIMESTAMPTZ NOT NULL,
              time_bucket TEXT NOT NULL,
              grid_type TEXT NOT NULL,
              grid_level INTEGER NOT NULL,
              space_code TEXT NOT NULL,
              st_code TEXT NOT NULL,
              xco2 DOUBLE PRECISION NOT NULL,
              quality_flag TEXT,
              center_lon DOUBLE PRECISION NOT NULL,
              center_lat DOUBLE PRECISION NOT NULL,
              footprint_geojson JSONB NOT NULL,
              source_uri TEXT NOT NULL,
              source_index INTEGER,
              metadata_json JSONB NOT NULL,
              cube_version TEXT NOT NULL,
              run_id TEXT NOT NULL,
              ingest_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              UNIQUE (satellite, observation_id, product_type, cube_version)
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_carbon_fact_grid_time
              ON rs_carbon_observation_fact (grid_type, grid_level, time_bucket, space_code);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_carbon_fact_quality
              ON rs_carbon_observation_fact (product_type, quality_flag);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_carbon_fact_acq_time
              ON rs_carbon_observation_fact (acq_time);
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rs_ingest_job (
              job_id TEXT PRIMARY KEY,
              status TEXT NOT NULL,
              params_json JSONB NOT NULL,
              stats_json JSONB NOT NULL,
              error_msg TEXT,
              retry_count INTEGER NOT NULL DEFAULT 0,
              started_at TIMESTAMPTZ,
              finished_at TIMESTAMPTZ,
              output_snapshot TEXT,
              created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
    conn.commit()


def _resolve_rows_path(args: argparse.Namespace) -> Path:
    if getattr(args, "rows_path", ""):
        rows_path = Path(args.rows_path)
    else:
        run_dir = Path(args.run_dir)
        rows_path = run_dir / "carbon_observation_rows.jsonl"
        if not rows_path.exists():
            rows_path = run_dir / "index_rows.jsonl"
    if not rows_path.exists():
        raise FileNotFoundError(f"Carbon observation rows not found: {rows_path}")
    return rows_path


def load_carbon_rows(rows_path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with rows_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            text = line.strip()
            if text:
                rows.append(json.loads(text))
    if not rows:
        raise RuntimeError(f"No rows found in: {rows_path}")
    return rows


def _json_text(value: Any) -> str:
    if value is None:
        return "{}"
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def build_carbon_facts(rows: list[dict[str, Any]], cube_version: str, run_id: str) -> list[CarbonObservationFact]:
    facts: dict[tuple[str, str, str, str], CarbonObservationFact] = {}
    for row in rows:
        product_type = str(row.get("product_type") or "xco2")
        key = (str(row["satellite"]), str(row["observation_id"]), product_type, cube_version)
        facts[key] = CarbonObservationFact(
            satellite=key[0],
            product_type=product_type,
            observation_id=key[1],
            acq_time=str(row["acq_time"]),
            time_bucket=str(row["time_bucket"]),
            grid_type=str(row["grid_type"]),
            grid_level=int(row["grid_level"]),
            space_code=str(row["space_code"]),
            st_code=str(row["st_code"]),
            xco2=float(row["xco2"]),
            quality_flag=(None if row.get("quality_flag") is None else str(row.get("quality_flag"))),
            center_lon=float(row["center_lon"]),
            center_lat=float(row["center_lat"]),
            footprint_geojson=_json_text(row.get("footprint_geojson")),
            source_uri=str(row.get("source_uri") or ""),
            source_index=(None if row.get("source_index") is None else int(row["source_index"])),
            metadata_json=_json_text(row.get("metadata_json")),
            cube_version=cube_version,
            run_id=run_id,
        )
    return list(facts.values())


def _report_carbon_fact_metrics(rows: list[CarbonObservationFact]) -> None:
    report_tile_metrics(
        TileProbeMetric(
            task_name="cube.partition.carbon.ingest",
            tile_type="ingest",
            method_name="merge.rs_carbon_observation_fact",
            attributes={
                "cube.stage": "ingest",
                "cube.target_table": "rs_carbon_observation_fact",
                "cube.data_type": "carbon",
                "cube.satellite": row.satellite,
                "cube.product_type": row.product_type,
                "cube.grid_type": row.grid_type,
                "cube.grid_level": row.grid_level,
                "cube.space_code": row.space_code,
                "cube.time_bucket": row.time_bucket,
                "cube.st_code": row.st_code,
                "cube.observation_id": row.observation_id,
                "cube.source_index": row.source_index,
                "cube.run_id": row.run_id,
                "cube.cube_version": row.cube_version,
            },
        )
        for row in rows
    )


def upsert_carbon_facts_postgres(conn: Any, rows: list[CarbonObservationFact]) -> None:
    if not rows:
        return
    columns = """
        satellite TEXT NOT NULL,
        product_type TEXT NOT NULL,
        observation_id TEXT NOT NULL,
        acq_time TIMESTAMPTZ NOT NULL,
        time_bucket TEXT NOT NULL,
        grid_type TEXT NOT NULL,
        grid_level INTEGER NOT NULL,
        space_code TEXT NOT NULL,
        st_code TEXT NOT NULL,
        xco2 DOUBLE PRECISION NOT NULL,
        quality_flag TEXT,
        center_lon DOUBLE PRECISION NOT NULL,
        center_lat DOUBLE PRECISION NOT NULL,
        footprint_geojson JSONB NOT NULL,
        source_uri TEXT NOT NULL,
        source_index INTEGER,
        metadata_json JSONB NOT NULL,
        cube_version TEXT NOT NULL,
        run_id TEXT NOT NULL
    """
    merge_sql = """
        MERGE INTO rs_carbon_observation_fact target
        USING tmp_carbon_observation_fact source
        ON (
          CAST(target.satellite AS VARCHAR(128)) = CAST(source.satellite AS VARCHAR(128))
          AND CAST(target.observation_id AS VARCHAR(256)) = CAST(source.observation_id AS VARCHAR(256))
          AND CAST(target.product_type AS VARCHAR(128)) = CAST(source.product_type AS VARCHAR(128))
          AND CAST(target.cube_version AS VARCHAR(128)) = CAST(source.cube_version AS VARCHAR(128))
        )
        WHEN MATCHED THEN UPDATE SET
          acq_time = source.acq_time,
          time_bucket = source.time_bucket,
          grid_type = source.grid_type,
          grid_level = source.grid_level,
          space_code = source.space_code,
          st_code = source.st_code,
          xco2 = source.xco2,
          quality_flag = source.quality_flag,
          center_lon = source.center_lon,
          center_lat = source.center_lat,
          footprint_geojson = source.footprint_geojson,
          source_uri = source.source_uri,
          source_index = source.source_index,
          metadata_json = source.metadata_json,
          run_id = source.run_id,
          ingest_time = NOW()
        WHEN NOT MATCHED THEN INSERT (
          satellite, product_type, observation_id, acq_time, time_bucket,
          grid_type, grid_level, space_code, st_code, xco2, quality_flag,
          center_lon, center_lat, footprint_geojson, source_uri, source_index,
          metadata_json, cube_version, run_id
        ) VALUES (
          source.satellite, source.product_type, source.observation_id, source.acq_time, source.time_bucket,
          source.grid_type, source.grid_level, source.space_code, source.st_code, source.xco2, source.quality_flag,
          source.center_lon, source.center_lat, source.footprint_geojson, source.source_uri, source.source_index,
          source.metadata_json, source.cube_version, source.run_id
        )
    """
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS tmp_carbon_observation_fact")
        cur.execute(f"CREATE TEMP TABLE tmp_carbon_observation_fact ({columns})")
        with cur.copy(
            """
            COPY tmp_carbon_observation_fact (
              satellite, product_type, observation_id, acq_time, time_bucket,
              grid_type, grid_level, space_code, st_code, xco2, quality_flag,
              center_lon, center_lat, footprint_geojson, source_uri, source_index,
              metadata_json, cube_version, run_id
            ) FROM STDIN
            """
        ) as copy:
            for row in rows:
                copy.write_row(_carbon_fact_values(row))
        cur.execute(merge_sql)


def _carbon_fact_values(row: CarbonObservationFact) -> tuple[Any, ...]:
    return (
        row.satellite,
        row.product_type,
        row.observation_id,
        _parse_timestamp(row.acq_time),
        row.time_bucket,
        row.grid_type,
        row.grid_level,
        row.space_code,
        row.st_code,
        row.xco2,
        row.quality_flag,
        row.center_lon,
        row.center_lat,
        row.footprint_geojson,
        row.source_uri,
        row.source_index,
        row.metadata_json,
        row.cube_version,
        row.run_id,
    )

def _upsert_job_status_postgres(
    conn: Any,
    *,
    job_id: str,
    status: str,
    params_json: dict[str, Any],
    stats_json: dict[str, Any] | None = None,
    error_msg: str | None = None,
    started_at: str | None = None,
    finished_at: str | None = None,
    output_snapshot: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT retry_count FROM rs_ingest_job WHERE job_id = %s", (job_id,))
        row = cur.fetchone()
        retry_count = int(row[0]) if row else 0
        if status == "running" and row:
            retry_count += 1
        cur.execute(
            """
            MERGE INTO rs_ingest_job target
            USING (
              SELECT
                %s::text AS job_id,
                %s::text AS status,
                %s::jsonb AS params_json,
                %s::jsonb AS stats_json,
                %s::text AS error_msg,
                %s::int AS retry_count,
                %s::timestamptz AS started_at,
                %s::timestamptz AS finished_at,
                %s::text AS output_snapshot
            ) source
            ON (target.job_id = source.job_id)
            WHEN MATCHED THEN UPDATE SET
              status = source.status,
              params_json = source.params_json,
              stats_json = source.stats_json,
              error_msg = source.error_msg,
              retry_count = source.retry_count,
              started_at = source.started_at,
              finished_at = source.finished_at,
              output_snapshot = source.output_snapshot,
              updated_at = NOW()
            WHEN NOT MATCHED THEN INSERT (
              job_id, status, params_json, stats_json, error_msg, retry_count,
              started_at, finished_at, output_snapshot
            ) VALUES (
              source.job_id, source.status, source.params_json, source.stats_json, source.error_msg, source.retry_count,
              source.started_at, source.finished_at, source.output_snapshot
            )
            """,
            (
                job_id,
                status,
                json.dumps(params_json, ensure_ascii=False),
                json.dumps(stats_json or {}, ensure_ascii=False),
                error_msg,
                retry_count,
                _parse_timestamp(started_at) if started_at else None,
                _parse_timestamp(finished_at) if finished_at else None,
                output_snapshot,
            ),
        )

def run_carbon_ingest(args: argparse.Namespace) -> dict[str, Any]:
    rows_path = _resolve_rows_path(args)
    started_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    params = {
        "rows_path": str(rows_path.resolve()),
        "cube_version": args.cube_version,
        "metadata_backend": args.metadata_backend,
    }
    rows = load_carbon_rows(rows_path)
    facts = build_carbon_facts(rows=rows, cube_version=args.cube_version, run_id=args.job_id)
    stats = {
        "rows_path": str(rows_path.resolve()),
        "input_rows": len(rows),
        "carbon_fact_rows": len(facts),
        "metadata_backend": args.metadata_backend,
    }

    if args.metadata_backend != "postgres":
        raise ValueError(f"Unsupported metadata_backend: {args.metadata_backend}")
    if not args.postgres_dsn:
        raise ValueError("--postgres-dsn is required when metadata-backend=postgres")
    try:
        import psycopg
    except ModuleNotFoundError as exc:
        raise RuntimeError("Postgres backend requires `psycopg` package") from exc

    try:
        conn_ctx = psycopg.connect(args.postgres_dsn, client_encoding="UTF8")
    except TypeError:
        conn_ctx = psycopg.connect(args.postgres_dsn)

    with conn_ctx as conn:
        try:
            ensure_carbon_tables_postgres(conn)
            _upsert_job_status_postgres(
                conn,
                job_id=args.job_id,
                status="running",
                params_json=params,
                started_at=started_at,
            )
            upsert_carbon_facts_postgres(conn, facts)
            finished_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            _upsert_job_status_postgres(
                conn,
                job_id=args.job_id,
                status="succeeded",
                params_json=params,
                stats_json=stats,
                started_at=started_at,
                finished_at=finished_at,
                output_snapshot=f"cube_version={args.cube_version},job_id={args.job_id}",
            )
            conn.commit()
            _report_carbon_fact_metrics(facts)
            return stats
        except Exception as exc:
            finished_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            conn.rollback()
            _upsert_job_status_postgres(
                conn,
                job_id=args.job_id,
                status="failed",
                params_json=params,
                error_msg=str(exc),
                started_at=started_at,
                finished_at=finished_at,
            )
            conn.commit()
            raise


def main() -> None:
    args = parse_args()
    start = time.perf_counter()
    stats = run_carbon_ingest(args)
    stats["elapsed_sec"] = round(time.perf_counter() - start, 3)
    print(json.dumps(stats, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
