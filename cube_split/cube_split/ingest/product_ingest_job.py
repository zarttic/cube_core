from __future__ import annotations

import argparse
import json
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from cube_split.ingest.ray_ingest_job import (
    DEFAULT_MINIO_ACCESS_KEY,
    DEFAULT_MINIO_BUCKET,
    DEFAULT_MINIO_ENDPOINT,
    DEFAULT_MINIO_SECRET_KEY,
    DEFAULT_POSTGRES_DSN,
    _build_window_ref_uri,
    _parse_timestamp,
    _resolve_backends,
    _upsert_job_status,
    _upsert_job_status_postgres,
    load_rows,
    materialize_cog_assets,
    upload_assets_to_minio,
)


@dataclass(frozen=True)
class ProductAssetRecord:
    dataset: str
    product_name: str
    scene_id: str
    product_year: int
    acq_time: str
    cog_uri: str
    version: str
    run_id: str


@dataclass(frozen=True)
class ProductFactRecord:
    dataset: str
    product_name: str
    product_year: int
    product_band: str
    grid_type: str
    grid_level: int
    space_code: str
    time_bucket: str
    st_code: str
    cell_min_lon: float
    cell_min_lat: float
    cell_max_lon: float
    cell_max_lat: float
    value_ref_uri: str
    sample_mean: float | None
    cube_version: str
    run_id: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest product partition rows into metadata storage")
    parser.add_argument("--run-dir", required=True, help="Path to product run directory containing index_rows.jsonl")
    parser.add_argument("--job-id", required=True, help="Ingestion job id")
    parser.add_argument("--dataset", default="dianzhong_ecological_security", help="Dataset name")
    parser.add_argument("--product-name", default="滇中地区30米生态安全评价数据集", help="Product name")
    parser.add_argument("--asset-version", default="v1", help="Asset version")
    parser.add_argument("--cube-version", default="product_v1", help="Product cube version")
    parser.add_argument("--metadata-backend", default="postgres", choices=["postgres", "sqlite"], help="Metadata store backend")
    parser.add_argument("--postgres-dsn", default=DEFAULT_POSTGRES_DSN, help="PostgreSQL DSN")
    parser.add_argument("--db-path", default="data/ingest/product_ingest.db", help="SQLite DB path")
    parser.add_argument("--asset-storage-backend", default="minio", choices=["minio", "local"], help="Asset storage backend")
    parser.add_argument("--minio-endpoint", default=DEFAULT_MINIO_ENDPOINT, help="MinIO endpoint host:port")
    parser.add_argument("--minio-access-key", default=DEFAULT_MINIO_ACCESS_KEY, help="MinIO access key")
    parser.add_argument("--minio-secret-key", default=DEFAULT_MINIO_SECRET_KEY, help="MinIO secret key")
    parser.add_argument("--minio-bucket", default=DEFAULT_MINIO_BUCKET, help="MinIO bucket name")
    parser.add_argument("--minio-prefix", default="cube/product", help="Object key prefix")
    parser.add_argument("--minio-secure", action="store_true", help="Use TLS for MinIO connection")
    parser.add_argument("--minio-upload-workers", type=int, default=8, help="Parallel upload workers")
    parser.add_argument("--cog-output-root", default="data/cog/product_raw", help="Local COG materialization root")
    parser.add_argument("--cog-materialize-mode", default="copy", choices=["copy", "hardlink", "symlink"], help="Local materialization mode")
    return parser.parse_args()


def _product_year(row: dict) -> int:
    return _parse_timestamp(row["acq_time"]).year


def ensure_product_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS rs_product_asset (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          dataset TEXT NOT NULL,
          product_name TEXT NOT NULL,
          scene_id TEXT NOT NULL,
          product_year INTEGER NOT NULL,
          acq_time TEXT NOT NULL,
          cog_uri TEXT NOT NULL,
          version TEXT NOT NULL,
          run_id TEXT NOT NULL,
          ingest_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          UNIQUE (dataset, scene_id, version)
        );

        CREATE TABLE IF NOT EXISTS rs_product_cell_fact (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          dataset TEXT NOT NULL,
          product_name TEXT NOT NULL,
          product_year INTEGER NOT NULL,
          product_band TEXT NOT NULL,
          grid_type TEXT NOT NULL,
          grid_level INTEGER NOT NULL,
          space_code TEXT NOT NULL,
          time_bucket TEXT NOT NULL,
          st_code TEXT NOT NULL,
          cell_min_lon REAL NOT NULL,
          cell_min_lat REAL NOT NULL,
          cell_max_lon REAL NOT NULL,
          cell_max_lat REAL NOT NULL,
          value_ref_uri TEXT NOT NULL,
          sample_mean REAL,
          cube_version TEXT NOT NULL,
          run_id TEXT NOT NULL,
          ingest_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          UNIQUE (dataset, grid_type, grid_level, space_code, time_bucket, product_band, cube_version)
        );

        CREATE TABLE IF NOT EXISTS rs_ingest_job (
          job_id TEXT PRIMARY KEY,
          status TEXT NOT NULL,
          params_json TEXT NOT NULL,
          stats_json TEXT NOT NULL,
          error_msg TEXT,
          retry_count INTEGER NOT NULL DEFAULT 0,
          started_at TEXT,
          finished_at TEXT,
          output_snapshot TEXT,
          created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    conn.commit()


def ensure_product_tables_postgres(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rs_product_asset (
              id BIGSERIAL PRIMARY KEY,
              dataset TEXT NOT NULL,
              product_name TEXT NOT NULL,
              scene_id TEXT NOT NULL,
              product_year INTEGER NOT NULL,
              acq_time TIMESTAMPTZ NOT NULL,
              cog_uri TEXT NOT NULL,
              version TEXT NOT NULL,
              run_id TEXT NOT NULL,
              ingest_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              UNIQUE (dataset, scene_id, version)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rs_product_cell_fact (
              id BIGSERIAL PRIMARY KEY,
              dataset TEXT NOT NULL,
              product_name TEXT NOT NULL,
              product_year INTEGER NOT NULL,
              product_band TEXT NOT NULL,
              grid_type TEXT NOT NULL,
              grid_level INTEGER NOT NULL,
              space_code TEXT NOT NULL,
              time_bucket TEXT NOT NULL,
              st_code TEXT NOT NULL,
              cell_min_lon DOUBLE PRECISION NOT NULL,
              cell_min_lat DOUBLE PRECISION NOT NULL,
              cell_max_lon DOUBLE PRECISION NOT NULL,
              cell_max_lat DOUBLE PRECISION NOT NULL,
              value_ref_uri TEXT NOT NULL,
              sample_mean DOUBLE PRECISION,
              cube_version TEXT NOT NULL,
              run_id TEXT NOT NULL,
              ingest_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              UNIQUE (dataset, grid_type, grid_level, space_code, time_bucket, product_band, cube_version)
            );
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


def build_product_asset_records(
    rows: Iterable[dict],
    dataset: str,
    product_name: str,
    asset_version: str,
    run_id: str,
    asset_uri_map: dict[str, str],
) -> list[ProductAssetRecord]:
    by_scene: dict[str, ProductAssetRecord] = {}
    for row in rows:
        by_scene.setdefault(
            row["scene_id"],
            ProductAssetRecord(
                dataset=dataset,
                product_name=product_name,
                scene_id=row["scene_id"],
                product_year=_product_year(row),
                acq_time=row["acq_time"],
                cog_uri=asset_uri_map[row["asset_path"]],
                version=asset_version,
                run_id=run_id,
            ),
        )
    return list(by_scene.values())


def build_product_fact_records(
    rows: Iterable[dict],
    dataset: str,
    product_name: str,
    cube_version: str,
    run_id: str,
    asset_uri_map: dict[str, str],
) -> list[ProductFactRecord]:
    facts: list[ProductFactRecord] = []
    seen: set[tuple[str, str, int, str, str, str, str]] = set()
    for row in rows:
        key = (
            dataset,
            row["grid_type"],
            int(row["grid_level"]),
            row["space_code"],
            row["time_bucket"],
            row["band"],
            cube_version,
        )
        if key in seen:
            continue
        seen.add(key)
        asset_uri = asset_uri_map[row["asset_path"]]
        facts.append(
            ProductFactRecord(
                dataset=dataset,
                product_name=product_name,
                product_year=_product_year(row),
                product_band=row["band"],
                grid_type=row["grid_type"],
                grid_level=int(row["grid_level"]),
                space_code=row["space_code"],
                time_bucket=row["time_bucket"],
                st_code=row["st_code"],
                cell_min_lon=float(row["cell_min_lon"]),
                cell_min_lat=float(row["cell_min_lat"]),
                cell_max_lon=float(row["cell_max_lon"]),
                cell_max_lat=float(row["cell_max_lat"]),
                value_ref_uri=_build_window_ref_uri(asset_uri, row),
                sample_mean=row.get("sample_mean_band1"),
                cube_version=cube_version,
                run_id=run_id,
            )
        )
    return facts


def upsert_product_assets(conn: sqlite3.Connection, rows: list[ProductAssetRecord]) -> None:
    conn.executemany(
        """
        INSERT INTO rs_product_asset (
          dataset, product_name, scene_id, product_year, acq_time, cog_uri, version, run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(dataset, scene_id, version) DO UPDATE SET
          product_name=excluded.product_name,
          product_year=excluded.product_year,
          acq_time=excluded.acq_time,
          cog_uri=excluded.cog_uri,
          run_id=excluded.run_id,
          ingest_time=CURRENT_TIMESTAMP
        """,
        [(row.dataset, row.product_name, row.scene_id, row.product_year, row.acq_time, row.cog_uri, row.version, row.run_id) for row in rows],
    )


def upsert_product_facts(conn: sqlite3.Connection, rows: list[ProductFactRecord]) -> None:
    conn.executemany(
        """
        INSERT INTO rs_product_cell_fact (
          dataset, product_name, product_year, product_band, grid_type, grid_level,
          space_code, time_bucket, st_code, cell_min_lon, cell_min_lat, cell_max_lon,
          cell_max_lat, value_ref_uri, sample_mean, cube_version, run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(dataset, grid_type, grid_level, space_code, time_bucket, product_band, cube_version)
        DO UPDATE SET
          product_name=excluded.product_name,
          product_year=excluded.product_year,
          st_code=excluded.st_code,
          cell_min_lon=excluded.cell_min_lon,
          cell_min_lat=excluded.cell_min_lat,
          cell_max_lon=excluded.cell_max_lon,
          cell_max_lat=excluded.cell_max_lat,
          value_ref_uri=excluded.value_ref_uri,
          sample_mean=excluded.sample_mean,
          run_id=excluded.run_id,
          ingest_time=CURRENT_TIMESTAMP
        """,
        [
            (
                row.dataset,
                row.product_name,
                row.product_year,
                row.product_band,
                row.grid_type,
                row.grid_level,
                row.space_code,
                row.time_bucket,
                row.st_code,
                row.cell_min_lon,
                row.cell_min_lat,
                row.cell_max_lon,
                row.cell_max_lat,
                row.value_ref_uri,
                row.sample_mean,
                row.cube_version,
                row.run_id,
            )
            for row in rows
        ],
    )


def upsert_product_assets_postgres(conn: Any, rows: list[ProductAssetRecord]) -> None:
    sql = """
        INSERT INTO rs_product_asset (
          dataset, product_name, scene_id, product_year, acq_time, cog_uri, version, run_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(dataset, scene_id, version) DO UPDATE SET
          product_name=excluded.product_name,
          product_year=excluded.product_year,
          acq_time=excluded.acq_time,
          cog_uri=excluded.cog_uri,
          run_id=excluded.run_id,
          ingest_time=NOW()
    """
    values = [(row.dataset, row.product_name, row.scene_id, row.product_year, _parse_timestamp(row.acq_time), row.cog_uri, row.version, row.run_id) for row in rows]
    with conn.cursor() as cur:
        cur.executemany(sql, values)


def upsert_product_facts_postgres(conn: Any, rows: list[ProductFactRecord]) -> None:
    sql = """
        INSERT INTO rs_product_cell_fact (
          dataset, product_name, product_year, product_band, grid_type, grid_level,
          space_code, time_bucket, st_code, cell_min_lon, cell_min_lat, cell_max_lon,
          cell_max_lat, value_ref_uri, sample_mean, cube_version, run_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(dataset, grid_type, grid_level, space_code, time_bucket, product_band, cube_version)
        DO UPDATE SET
          product_name=excluded.product_name,
          product_year=excluded.product_year,
          st_code=excluded.st_code,
          cell_min_lon=excluded.cell_min_lon,
          cell_min_lat=excluded.cell_min_lat,
          cell_max_lon=excluded.cell_max_lon,
          cell_max_lat=excluded.cell_max_lat,
          value_ref_uri=excluded.value_ref_uri,
          sample_mean=excluded.sample_mean,
          run_id=excluded.run_id,
          ingest_time=NOW()
    """
    values = [
        (
            row.dataset,
            row.product_name,
            row.product_year,
            row.product_band,
            row.grid_type,
            row.grid_level,
            row.space_code,
            row.time_bucket,
            row.st_code,
            row.cell_min_lon,
            row.cell_min_lat,
            row.cell_max_lon,
            row.cell_max_lat,
            row.value_ref_uri,
            row.sample_mean,
            row.cube_version,
            row.run_id,
        )
        for row in rows
    ]
    with conn.cursor() as cur:
        cur.executemany(sql, values)


def run_product_ingest(args: argparse.Namespace) -> dict:
    run_dir = Path(args.run_dir)
    rows_path = run_dir / "index_rows.jsonl"
    if not rows_path.exists():
        raise FileNotFoundError(f"index_rows.jsonl not found under run dir: {run_dir}")

    metadata_backend, asset_storage_backend = _resolve_backends(args)
    rows = load_rows(rows_path)
    started_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    params = {
        "run_dir": str(run_dir.resolve()),
        "dataset": args.dataset,
        "product_name": args.product_name,
        "asset_version": args.asset_version,
        "cube_version": args.cube_version,
        "metadata_backend": metadata_backend,
        "asset_storage_backend": asset_storage_backend,
    }

    if asset_storage_backend == "minio":
        asset_uri_map = upload_assets_to_minio(
            rows=rows,
            dataset=args.dataset,
            sensor="data_product",
            asset_version=args.asset_version,
            endpoint=args.minio_endpoint,
            access_key=args.minio_access_key,
            secret_key=args.minio_secret_key,
            bucket=args.minio_bucket,
            prefix=args.minio_prefix,
            secure=bool(args.minio_secure),
            workers=max(1, int(args.minio_upload_workers)),
        )
    else:
        asset_uri_map = materialize_cog_assets(
            rows=rows,
            dataset=args.dataset,
            sensor="data_product",
            asset_version=args.asset_version,
            cog_output_root=Path(args.cog_output_root),
            materialize_mode=args.cog_materialize_mode,
        )

    asset_records = build_product_asset_records(rows, args.dataset, args.product_name, args.asset_version, args.job_id, asset_uri_map)
    fact_records = build_product_fact_records(rows, args.dataset, args.product_name, args.cube_version, args.job_id, asset_uri_map)
    stats = {
        "run_dir": str(run_dir.resolve()),
        "input_rows": len(rows),
        "materialized_cog_assets": len(asset_uri_map),
        "product_asset_rows": len(asset_records),
        "product_fact_rows": len(fact_records),
        "metadata_backend": metadata_backend,
        "asset_storage_backend": asset_storage_backend,
    }

    if metadata_backend == "sqlite":
        db_path = Path(args.db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode = WAL")
        try:
            ensure_product_tables(conn)
            _upsert_job_status(conn, args.job_id, "running", params, started_at=started_at)
            upsert_product_assets(conn, asset_records)
            upsert_product_facts(conn, fact_records)
            finished_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            _upsert_job_status(
                conn,
                args.job_id,
                "succeeded",
                params,
                stats_json=stats,
                output_snapshot=f"cube_version={args.cube_version},job_id={args.job_id}",
                started_at=started_at,
                finished_at=finished_at,
            )
            conn.commit()
            return stats
        except Exception as exc:
            finished_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            _upsert_job_status(conn, args.job_id, "failed", params, error_msg=str(exc), started_at=started_at, finished_at=finished_at)
            conn.commit()
            raise
        finally:
            conn.close()

    if not args.postgres_dsn:
        raise ValueError("--postgres-dsn is required when metadata-backend=postgres")
    try:
        import psycopg
    except ModuleNotFoundError as exc:
        raise RuntimeError("Postgres backend requires `psycopg` package") from exc

    with psycopg.connect(args.postgres_dsn) as conn:
        try:
            ensure_product_tables_postgres(conn)
            _upsert_job_status_postgres(conn, args.job_id, "running", params, started_at=started_at)
            upsert_product_assets_postgres(conn, asset_records)
            upsert_product_facts_postgres(conn, fact_records)
            finished_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            _upsert_job_status_postgres(
                conn,
                args.job_id,
                "succeeded",
                params,
                stats_json=stats,
                output_snapshot=f"cube_version={args.cube_version},job_id={args.job_id}",
                started_at=started_at,
                finished_at=finished_at,
            )
            conn.commit()
            return stats
        except Exception as exc:
            finished_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            _upsert_job_status_postgres(conn, args.job_id, "failed", params, error_msg=str(exc), started_at=started_at, finished_at=finished_at)
            conn.commit()
            raise


def main() -> None:
    args = parse_args()
    start = time.perf_counter()
    stats = run_product_ingest(args)
    stats["elapsed_sec"] = round(time.perf_counter() - start, 3)
    print(json.dumps(stats, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
