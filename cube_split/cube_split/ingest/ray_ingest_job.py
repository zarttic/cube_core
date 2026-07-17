from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from grid_core.sdk import CubeEncoderSDK, GridAddress

from cube_split import runtime_config
from cube_split.jobs.ray_partition_core import _local_file_identity, _object_identity, _read_identity_sidecar, _write_identity_sidecar
from cube_split.tile_probe import TileProbeMetric, report_tile_metrics

DEFAULT_POSTGRES_BATCH_SIZE = 1000


@dataclass(frozen=True)
class RawAssetRecord:
    dataset: str
    sensor: str
    scene_id: str
    band: str
    acq_time: str
    raw_cog_uri: str
    version: str
    run_id: str


@dataclass(frozen=True)
class CubeFactRecord:
    grid_type: str
    grid_level: int
    space_code: str
    time_bucket: str
    band: str
    st_code: str
    cell_min_lon: float
    cell_min_lat: float
    cell_max_lon: float
    cell_max_lat: float
    cell_geom_geojson: str
    value_ref_uri: str
    source_scene_count: int
    provenance_json: str
    quality_rule: str
    cube_version: str
    run_id: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest partition rows into PostgreSQL metadata + MinIO asset storage")
    parser.add_argument("--run-dir", required=True, help="Path to Ray run directory containing index_rows.jsonl")
    parser.add_argument("--job-id", required=True, help="Ingestion job id")
    parser.add_argument("--dataset", default="unknown", help="Dataset name")
    parser.add_argument("--sensor", default="unknown", help="Sensor name")
    parser.add_argument("--asset-version", default="v1", help="Raw asset version")
    parser.add_argument("--cube-version", default="v1", help="Cube fact version")
    parser.add_argument(
        "--quality-rule",
        default="best_quality_wins",
        choices=["best_quality_wins", "latest_wins"],
        help="Conflict resolution rule for same cell-time-band",
    )

    parser.add_argument(
        "--metadata-backend",
        default="postgres",
        choices=["postgres", "sqlite"],
        help="Metadata store backend",
    )
    minio = runtime_config.minio_settings()
    parser.add_argument("--postgres-dsn", default=runtime_config.postgres_dsn(), help="PostgreSQL DSN, required when metadata-backend=postgres")
    parser.add_argument("--db-path", default="data/ingest/ingest.db", help="SQLite DB path (used when metadata-backend=sqlite)")

    parser.add_argument(
        "--asset-storage-backend",
        default="minio",
        choices=["minio", "local"],
        help="Asset storage backend",
    )
    parser.add_argument("--minio-endpoint", default=minio.endpoint, help="MinIO endpoint host:port")
    parser.add_argument("--minio-access-key", default=minio.access_key, help="MinIO access key")
    parser.add_argument("--minio-secret-key", default=minio.secret_key, help="MinIO secret key")
    parser.add_argument("--minio-bucket", default=minio.bucket, help="MinIO bucket name")
    parser.add_argument("--minio-prefix", default="cube/raw", help="Object key prefix")
    parser.add_argument("--minio-secure", action="store_true", help="Use TLS for MinIO connection")
    parser.add_argument("--minio-upload-workers", type=int, default=8, help="Parallel upload workers for MinIO")
    parser.add_argument("--postgres-batch-size", type=int, default=DEFAULT_POSTGRES_BATCH_SIZE, help="Rows per PostgreSQL/OpenGauss MERGE batch")

    parser.add_argument(
        "--cog-output-root",
        default="data/cog/raw",
        help="Root directory used to materialize/stage COG assets when asset-storage-backend=local",
    )
    parser.add_argument(
        "--cog-materialize-mode",
        default="copy",
        choices=["copy", "hardlink", "symlink"],
        help="How to materialize COG assets into --cog-output-root when asset-storage-backend=local",
    )
    return parser.parse_args()


def ensure_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS rs_raw_scene_asset (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          dataset TEXT NOT NULL,
          sensor TEXT NOT NULL,
          scene_id TEXT NOT NULL,
          band TEXT NOT NULL,
          acq_time TEXT NOT NULL,
          raw_cog_uri TEXT NOT NULL,
          version TEXT NOT NULL,
          run_id TEXT NOT NULL,
          ingest_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          UNIQUE (scene_id, band, version)
        );

        CREATE TABLE IF NOT EXISTS rs_cube_cell_fact (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          grid_type TEXT NOT NULL,
          grid_level INTEGER NOT NULL,
          space_code TEXT NOT NULL,
          time_bucket TEXT NOT NULL,
          band TEXT NOT NULL,
          st_code TEXT NOT NULL,
          cell_min_lon REAL NOT NULL,
          cell_min_lat REAL NOT NULL,
          cell_max_lon REAL NOT NULL,
          cell_max_lat REAL NOT NULL,
          cell_geom TEXT,
          value_ref_uri TEXT NOT NULL,
          source_scene_count INTEGER NOT NULL,
          provenance_json TEXT NOT NULL,
          quality_rule TEXT NOT NULL,
          cube_version TEXT NOT NULL,
          run_id TEXT NOT NULL,
          ingest_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          UNIQUE (grid_type, grid_level, space_code, time_bucket, band, cube_version)
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
    columns = {row[1] for row in conn.execute("PRAGMA table_info(rs_cube_cell_fact)").fetchall()}
    if "cell_geom" not in columns:
        conn.execute("ALTER TABLE rs_cube_cell_fact ADD COLUMN cell_geom TEXT")
    conn.commit()


def ensure_tables_postgres(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rs_raw_scene_asset (
              id BIGSERIAL PRIMARY KEY,
              dataset TEXT NOT NULL,
              sensor TEXT NOT NULL,
              scene_id TEXT NOT NULL,
              band TEXT NOT NULL,
              acq_time TIMESTAMPTZ NOT NULL,
              raw_cog_uri TEXT NOT NULL,
              version TEXT NOT NULL,
              run_id TEXT NOT NULL,
              ingest_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              UNIQUE (scene_id, band, version)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rs_cube_cell_fact (
              id BIGSERIAL PRIMARY KEY,
              grid_type TEXT NOT NULL,
              grid_level INTEGER NOT NULL,
              space_code TEXT NOT NULL,
              time_bucket TEXT NOT NULL,
              band TEXT NOT NULL,
              st_code TEXT NOT NULL,
              cell_min_lon DOUBLE PRECISION NOT NULL,
              cell_min_lat DOUBLE PRECISION NOT NULL,
              cell_max_lon DOUBLE PRECISION NOT NULL,
              cell_max_lat DOUBLE PRECISION NOT NULL,
              cell_geom geometry(Polygon, 4326),
              value_ref_uri TEXT NOT NULL,
              source_scene_count INTEGER NOT NULL,
              provenance_json JSONB NOT NULL,
              quality_rule TEXT NOT NULL,
              cube_version TEXT NOT NULL,
              run_id TEXT NOT NULL,
              ingest_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              UNIQUE (grid_type, grid_level, space_code, time_bucket, band, cube_version)
            );
            """
        )
        cur.execute(
            """
            ALTER TABLE rs_cube_cell_fact
            ADD COLUMN IF NOT EXISTS cell_geom geometry(Polygon, 4326);
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


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _time_score(value: str) -> float:
    return _parse_timestamp(value).timestamp()


def _choice_score(row: dict, quality_rule: str) -> tuple[float, str]:
    if quality_rule == "latest_wins":
        return (_time_score(row["acq_time"]), row["scene_id"])
    return (float(row.get("sample_mean_band1") or float("-inf")), _time_score(row["acq_time"]), row["scene_id"])


def _build_window_ref_uri(asset_uri: str, row: dict) -> str:
    return (
        f"{asset_uri}#window="
        f"{row['window_col_off']},{row['window_row_off']},{row['window_width']},{row['window_height']}"
    )


def load_rows(rows_path: Path) -> list[dict]:
    rows: list[dict] = []
    with rows_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            text = line.strip()
            if not text:
                continue
            rows.append(json.loads(text))
    if not rows:
        raise RuntimeError(f"No rows found in: {rows_path}")
    return rows


def _acq_date_path(acq_time: str) -> str:
    dt = _parse_timestamp(acq_time)
    return dt.strftime("%Y/%m/%d")


def _materialize_one_asset(source: Path, target: Path, mode: str) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        return
    if mode == "copy":
        shutil.copy2(source, target)
        return
    if mode == "hardlink":
        os.link(source, target)
        return
    if mode == "symlink":
        os.symlink(source, target)
        return
    raise ValueError(f"Unsupported cog materialize mode: {mode}")


def _build_asset_key(dataset: str, sensor: str, asset_version: str, sample_row: dict, source_path: Path, prefix: str) -> str:
    date_path = _acq_date_path(sample_row["acq_time"])
    key = (
        f"{prefix.rstrip('/')}/"
        f"dataset={dataset}/"
        f"sensor={sensor}/"
        f"acq_date={date_path}/"
        f"scene_id={sample_row['scene_id']}/"
        f"version={asset_version}/"
        f"{source_path.name}"
    )
    return key


def materialize_cog_assets(
    rows: Iterable[dict],
    dataset: str,
    sensor: str,
    asset_version: str,
    cog_output_root: Path,
    materialize_mode: str,
) -> dict[str, str]:
    source_to_target: dict[str, str] = {}
    unique_assets: dict[str, dict] = {}
    for row in rows:
        unique_assets.setdefault(row["asset_path"], row)

    for source_uri, sample_row in unique_assets.items():
        source_path = Path(source_uri)
        if not source_path.exists():
            raise FileNotFoundError(f"Asset file not found: {source_path}")
        date_path = _acq_date_path(sample_row["acq_time"])
        target_path = (
            cog_output_root
            / f"dataset={dataset}"
            / f"sensor={sensor}"
            / f"acq_date={date_path}"
            / f"scene_id={sample_row['scene_id']}"
            / f"version={asset_version}"
            / source_path.name
        )
        _materialize_one_asset(source=source_path, target=target_path, mode=materialize_mode)
        source_to_target[source_uri] = str(target_path.resolve())
    return source_to_target


def upload_assets_to_minio(
    rows: Iterable[dict],
    dataset: str,
    sensor: str,
    asset_version: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
    bucket: str,
    prefix: str,
    secure: bool,
    workers: int,
) -> dict[str, str]:
    try:
        from minio import Minio
        from minio.error import S3Error
    except ModuleNotFoundError as exc:
        raise RuntimeError("MinIO backend requires `minio` package") from exc

    if not endpoint or not access_key or not secret_key or not bucket:
        raise ValueError("minio endpoint/access-key/secret-key/bucket are required for minio backend")

    unique_assets: dict[str, dict] = {}
    for row in rows:
        unique_assets.setdefault(row["asset_path"], row)

    client = Minio(endpoint=endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    source_to_uri: dict[str, str] = {}

    def upload_one(item: tuple[str, dict]) -> tuple[str, str]:
        source_uri, sample_row = item
        if str(source_uri).startswith("s3://"):
            return source_uri, str(source_uri)
        source_path = Path(source_uri)
        if not source_path.exists():
            raise FileNotFoundError(f"Asset file not found: {source_path}")
        key = _build_asset_key(dataset, sensor, asset_version, sample_row, source_path, prefix)
        local_identity = _local_file_identity(source_path)
        identity_state = _read_identity_sidecar(source_path)
        try:
            stat = client.stat_object(bucket, key)
            remote_identity = _object_identity(stat)
            if (
                stat.size == source_path.stat().st_size
                and identity_state.get("local") == local_identity
                and identity_state.get("remote") == remote_identity
            ):
                return source_uri, f"s3://{bucket}/{key}"
        except S3Error as exc:
            if exc.code != "NoSuchKey" and exc.code != "NoSuchObject":
                raise
        client.fput_object(bucket, key, str(source_path))
        stat = client.stat_object(bucket, key)
        _write_identity_sidecar(source_path, local=local_identity, remote=_object_identity(stat))
        return source_uri, f"s3://{bucket}/{key}"

    max_workers = max(1, workers)
    items = list(unique_assets.items())
    if max_workers == 1:
        for item in items:
            src, uri = upload_one(item)
            source_to_uri[src] = uri
        return source_to_uri

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for src, uri in pool.map(upload_one, items):
            source_to_uri[src] = uri
    return source_to_uri


def build_raw_asset_records(
    rows: Iterable[dict],
    dataset: str,
    sensor: str,
    asset_version: str,
    run_id: str,
    asset_uri_map: dict[str, str],
) -> list[RawAssetRecord]:
    by_asset: dict[tuple[str, str], RawAssetRecord] = {}
    for row in rows:
        key = (row["scene_id"], row["band"])
        existing = by_asset.get(key)
        if existing is None or _time_score(row["acq_time"]) > _time_score(existing.acq_time):
            by_asset[key] = RawAssetRecord(
                dataset=dataset,
                sensor=sensor,
                scene_id=row["scene_id"],
                band=row["band"],
                acq_time=row["acq_time"],
                raw_cog_uri=_asset_uri_for_path(asset_uri_map, row["asset_path"]),
                version=asset_version,
                run_id=run_id,
            )
    return list(by_asset.values())


def build_cube_fact_records(
    rows: Iterable[dict],
    cube_version: str,
    run_id: str,
    quality_rule: str,
    asset_uri_map: dict[str, str],
) -> list[CubeFactRecord]:
    sdk = CubeEncoderSDK()
    geometry_cache: dict[tuple[str, int, str, str | None], str] = {}
    grouped: dict[tuple[str, int, str, str, str], list[dict]] = {}
    for row in rows:
        key = (
            row["grid_type"],
            int(row["grid_level"]),
            row["space_code"],
            row["time_bucket"],
            row["band"],
        )
        grouped.setdefault(key, []).append(row)

    facts: list[CubeFactRecord] = []
    for key, candidates in grouped.items():
        winner = max(candidates, key=lambda row: _choice_score(row, quality_rule))
        provenance = {
            "winner_scene_id": winner["scene_id"],
            "candidate_scene_ids": sorted({row["scene_id"] for row in candidates}),
            "rule": quality_rule,
        }
        winner_asset_uri = _asset_uri_for_path(asset_uri_map, winner["asset_path"])
        geometry_key = (key[0], key[1], key[2], winner.get("topology_code"))
        cell_geom_geojson = geometry_cache.get(geometry_key)
        if cell_geom_geojson is None:
            cell_geom_geojson = cell_geometry_geojson(
                grid_type=key[0],
                grid_level=key[1],
                space_code=key[2],
                topology_code=winner.get("topology_code"),
                geometry=winner.get("cell_geom"),
                sdk=sdk,
            )
            geometry_cache[geometry_key] = cell_geom_geojson
        facts.append(
            CubeFactRecord(
                grid_type=key[0],
                grid_level=key[1],
                space_code=key[2],
                time_bucket=key[3],
                band=key[4],
                st_code=winner["st_code"],
                cell_min_lon=float(winner["cell_min_lon"]),
                cell_min_lat=float(winner["cell_min_lat"]),
                cell_max_lon=float(winner["cell_max_lon"]),
                cell_max_lat=float(winner["cell_max_lat"]),
                cell_geom_geojson=cell_geom_geojson,
                value_ref_uri=_build_window_ref_uri(winner_asset_uri, winner),
                source_scene_count=len({row["scene_id"] for row in candidates}),
                provenance_json=json.dumps(provenance, ensure_ascii=False),
                quality_rule=quality_rule,
                cube_version=cube_version,
                run_id=run_id,
            )
        )
    return facts


def cell_geometry_geojson(
    *,
    grid_type: str,
    grid_level: int,
    space_code: str,
    topology_code: str | None = None,
    geometry: dict[str, object] | None = None,
    sdk: CubeEncoderSDK | None = None,
) -> str:
    """Return the SDK-computed WGS84 cell polygon as compact GeoJSON."""
    normalized_type = str(grid_type or "").strip().lower()
    expected_points = {"geohash": 5, "mgrs": 5, "isea4h": 7}
    if normalized_type not in expected_points:
        raise ValueError(f"cell_geom is unsupported for grid_type={grid_type!r}")

    if geometry is None:
        geometry = (sdk or CubeEncoderSDK()).code_to_geometry(
            address=GridAddress(
                grid_type=normalized_type,
                grid_level=int(grid_level),
                space_code=str(space_code),
                topology_code=topology_code,
            )
        )
    if geometry.get("type") != "Polygon":
        raise ValueError(f"cell_geom must be a Polygon for {normalized_type}:{space_code}")
    coordinates = geometry.get("coordinates")
    if not isinstance(coordinates, (list, tuple)) or len(coordinates) != 1 or not isinstance(coordinates[0], (list, tuple)):
        raise ValueError(f"cell_geom must contain one exterior ring for {normalized_type}:{space_code}")

    ring = coordinates[0]
    if normalized_type == "mgrs" and len(ring) != 5:
        edge_count = len(ring) - 1
        if edge_count <= 0 or edge_count % 4 != 0 or ring[0] != ring[-1]:
            raise ValueError(f"MGRS SDK boundary cannot be reduced to four actual corners: {space_code}")
        edge_size = edge_count // 4
        ring = [ring[index * edge_size] for index in range(4)] + [ring[0]]

    normalized_ring = [[float(point[0]), float(point[1])] for point in ring]
    if len(normalized_ring) != expected_points[normalized_type] or normalized_ring[0] != normalized_ring[-1]:
        raise ValueError(
            f"invalid {normalized_type} cell boundary point count/closure for {space_code}: {len(normalized_ring)}"
        )
    if any(len(point) != 2 or not (-180.0 <= point[0] <= 180.0 and -90.0 <= point[1] <= 90.0) for point in normalized_ring):
        raise ValueError(f"cell_geom coordinates must be WGS84 [longitude, latitude] for {space_code}")

    return json.dumps({"type": "Polygon", "coordinates": [normalized_ring]}, ensure_ascii=False, separators=(",", ":"))


def _asset_uri_for_path(asset_uri_map: dict[str, str], asset_path: str) -> str:
    return asset_uri_map.get(str(asset_path), str(asset_path))


def _report_cube_fact_metrics(rows: list[CubeFactRecord], *, data_type: str, dataset: str, sensor: str) -> None:
    report_tile_metrics(
        TileProbeMetric(
            task_name=f"cube.partition.logical.ingest.{data_type}",
            tile_type="ingest",
            method_name="merge.rs_cube_cell_fact",
            attributes={
                "cube.stage": "ingest",
                "cube.target_table": "rs_cube_cell_fact",
                "cube.data_type": data_type,
                "cube.dataset": dataset,
                "cube.sensor": sensor,
                "cube.grid_type": row.grid_type,
                "cube.grid_level": row.grid_level,
                "cube.space_code": row.space_code,
                "cube.time_bucket": row.time_bucket,
                "cube.st_code": row.st_code,
                "cube.band": row.band,
                "cube.source_scene_count": row.source_scene_count,
                "cube.run_id": row.run_id,
                "cube.cube_version": row.cube_version,
                "cube.quality_rule": row.quality_rule,
            },
        )
        for row in rows
    )


def upsert_raw_assets(conn: sqlite3.Connection, rows: list[RawAssetRecord]) -> None:
    conn.executemany(
        """
        INSERT INTO rs_raw_scene_asset (
          dataset, sensor, scene_id, band, acq_time, raw_cog_uri, version, run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scene_id, band, version) DO UPDATE SET
          dataset=excluded.dataset,
          sensor=excluded.sensor,
          acq_time=excluded.acq_time,
          raw_cog_uri=excluded.raw_cog_uri,
          run_id=excluded.run_id,
          ingest_time=CURRENT_TIMESTAMP
        """,
        [
            (
                row.dataset,
                row.sensor,
                row.scene_id,
                row.band,
                row.acq_time,
                row.raw_cog_uri,
                row.version,
                row.run_id,
            )
            for row in rows
        ],
    )


def upsert_cube_facts(conn: sqlite3.Connection, rows: list[CubeFactRecord]) -> None:
    conn.executemany(
        """
        INSERT INTO rs_cube_cell_fact (
          grid_type, grid_level, space_code, time_bucket, band, st_code,
          cell_min_lon, cell_min_lat, cell_max_lon, cell_max_lat,
          cell_geom, value_ref_uri, source_scene_count, provenance_json, quality_rule, cube_version, run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(grid_type, grid_level, space_code, time_bucket, band, cube_version) DO UPDATE SET
          st_code=excluded.st_code,
          cell_min_lon=excluded.cell_min_lon,
          cell_min_lat=excluded.cell_min_lat,
          cell_max_lon=excluded.cell_max_lon,
          cell_max_lat=excluded.cell_max_lat,
          cell_geom=excluded.cell_geom,
          value_ref_uri=excluded.value_ref_uri,
          source_scene_count=excluded.source_scene_count,
          provenance_json=excluded.provenance_json,
          quality_rule=excluded.quality_rule,
          run_id=excluded.run_id,
          ingest_time=CURRENT_TIMESTAMP
        """,
        [
            (
                row.grid_type,
                row.grid_level,
                row.space_code,
                row.time_bucket,
                row.band,
                row.st_code,
                row.cell_min_lon,
                row.cell_min_lat,
                row.cell_max_lon,
                row.cell_max_lat,
                row.cell_geom_geojson,
                row.value_ref_uri,
                row.source_scene_count,
                row.provenance_json,
                row.quality_rule,
                row.cube_version,
                row.run_id,
            )
            for row in rows
        ],
    )


def upsert_raw_assets_postgres(conn: Any, rows: list[RawAssetRecord], batch_size: int = DEFAULT_POSTGRES_BATCH_SIZE) -> None:
    columns = (
        "dataset",
        "sensor",
        "scene_id",
        "band",
        "acq_time",
        "raw_cog_uri",
        "version",
        "run_id",
    )
    casts = ("text", "text", "text", "text", "timestamptz", "text", "text", "text")
    sql_template = """
        MERGE INTO rs_raw_scene_asset target
        USING ({source_sql}) source
        ON (
          target.scene_id = source.scene_id
          AND target.band = source.band
          AND target.version = source.version
        )
        WHEN MATCHED THEN UPDATE SET
          dataset = source.dataset,
          sensor = source.sensor,
          acq_time = source.acq_time,
          raw_cog_uri = source.raw_cog_uri,
          run_id = source.run_id,
          ingest_time = NOW()
        WHEN NOT MATCHED THEN INSERT (
          dataset, sensor, scene_id, band, acq_time, raw_cog_uri, version, run_id
        ) VALUES (
          source.dataset, source.sensor, source.scene_id, source.band, source.acq_time, source.raw_cog_uri, source.version, source.run_id
        )
    """
    values = [
        (
            row.dataset,
            row.sensor,
            row.scene_id,
            row.band,
            _parse_timestamp(row.acq_time),
            row.raw_cog_uri,
            row.version,
            row.run_id,
        )
        for row in rows
    ]
    with conn.cursor() as cur:
        for batch in _batches(values, batch_size):
            cur.execute(sql_template.format(source_sql=_postgres_values_source(columns, casts, len(batch))), _flatten(batch))


def upsert_cube_facts_postgres(conn: Any, rows: list[CubeFactRecord], batch_size: int = DEFAULT_POSTGRES_BATCH_SIZE) -> None:
    columns = (
        "grid_type",
        "grid_level",
        "space_code",
        "time_bucket",
        "band",
        "st_code",
        "cell_min_lon",
        "cell_min_lat",
        "cell_max_lon",
        "cell_max_lat",
        "cell_geom_geojson",
        "value_ref_uri",
        "source_scene_count",
        "provenance_json",
        "quality_rule",
        "cube_version",
        "run_id",
    )
    casts = (
        "text",
        "int",
        "text",
        "text",
        "text",
        "text",
        "double precision",
        "double precision",
        "double precision",
        "double precision",
        "text",
        "text",
        "int",
        "jsonb",
        "text",
        "text",
        "text",
    )
    sql_template = """
        MERGE INTO rs_cube_cell_fact target
        USING ({source_sql}) source
        ON (
          target.grid_type = source.grid_type
          AND target.grid_level = source.grid_level
          AND target.space_code = source.space_code
          AND target.time_bucket = source.time_bucket
          AND target.band = source.band
          AND target.cube_version = source.cube_version
        )
        WHEN MATCHED THEN UPDATE SET
          st_code = source.st_code,
          cell_min_lon = source.cell_min_lon,
          cell_min_lat = source.cell_min_lat,
          cell_max_lon = source.cell_max_lon,
          cell_max_lat = source.cell_max_lat,
          cell_geom = ST_SetSRID(ST_GeomFromGeoJSON(source.cell_geom_geojson), 4326),
          value_ref_uri = source.value_ref_uri,
          source_scene_count = source.source_scene_count,
          provenance_json = source.provenance_json,
          quality_rule = source.quality_rule,
          run_id = source.run_id,
          ingest_time = NOW()
        WHEN NOT MATCHED THEN INSERT (
          grid_type, grid_level, space_code, time_bucket, band, st_code,
          cell_min_lon, cell_min_lat, cell_max_lon, cell_max_lat,
          cell_geom, value_ref_uri, source_scene_count, provenance_json, quality_rule, cube_version, run_id
        ) VALUES (
          source.grid_type, source.grid_level, source.space_code, source.time_bucket, source.band, source.st_code,
          source.cell_min_lon, source.cell_min_lat, source.cell_max_lon, source.cell_max_lat,
          ST_SetSRID(ST_GeomFromGeoJSON(source.cell_geom_geojson), 4326), source.value_ref_uri,
          source.source_scene_count, source.provenance_json, source.quality_rule, source.cube_version, source.run_id
        )
    """
    values = [
        (
            row.grid_type,
            row.grid_level,
            row.space_code,
            row.time_bucket,
            row.band,
            row.st_code,
            row.cell_min_lon,
            row.cell_min_lat,
            row.cell_max_lon,
            row.cell_max_lat,
            row.cell_geom_geojson,
            row.value_ref_uri,
            row.source_scene_count,
            row.provenance_json,
            row.quality_rule,
            row.cube_version,
            row.run_id,
        )
        for row in rows
    ]
    with conn.cursor() as cur:
        for batch in _batches(values, batch_size):
            cur.execute(sql_template.format(source_sql=_postgres_values_source(columns, casts, len(batch))), _flatten(batch))


def _postgres_values_source(columns: tuple[str, ...], casts: tuple[str, ...], row_count: int) -> str:
    if row_count <= 0:
        raise ValueError("row_count must be greater than 0")
    row_sql = "(" + ", ".join(f"%s::{cast}" for cast in casts) + ")"
    values_sql = ", ".join(row_sql for _ in range(row_count))
    return f"SELECT * FROM (VALUES {values_sql}) AS source ({', '.join(columns)})"


def _batches(values: list[tuple[Any, ...]], batch_size: int) -> Iterable[list[tuple[Any, ...]]]:
    size = max(1, int(batch_size))
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


def _flatten(values: list[tuple[Any, ...]]) -> tuple[Any, ...]:
    return tuple(item for row in values for item in row)


def _postgres_batch_size(args: argparse.Namespace) -> int:
    return max(1, int(getattr(args, "postgres_batch_size", DEFAULT_POSTGRES_BATCH_SIZE) or DEFAULT_POSTGRES_BATCH_SIZE))


def _upsert_job_status(
    conn: sqlite3.Connection,
    job_id: str,
    status: str,
    params_json: dict,
    stats_json: dict | None = None,
    error_msg: str | None = None,
    output_snapshot: str | None = None,
    started_at: str | None = None,
    finished_at: str | None = None,
) -> None:
    row = conn.execute("SELECT retry_count FROM rs_ingest_job WHERE job_id = ?", (job_id,)).fetchone()
    retry_count = int(row[0]) if row else 0
    if status == "running" and row:
        retry_count += 1
    conn.execute(
        """
        INSERT INTO rs_ingest_job (
          job_id, status, params_json, stats_json, error_msg, retry_count, started_at, finished_at, output_snapshot, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(job_id) DO UPDATE SET
          status=excluded.status,
          params_json=excluded.params_json,
          stats_json=excluded.stats_json,
          error_msg=excluded.error_msg,
          retry_count=excluded.retry_count,
          started_at=excluded.started_at,
          finished_at=excluded.finished_at,
          output_snapshot=excluded.output_snapshot,
          updated_at=CURRENT_TIMESTAMP
        """,
        (
            job_id,
            status,
            json.dumps(params_json, ensure_ascii=False),
            json.dumps(stats_json or {}, ensure_ascii=False),
            error_msg,
            retry_count,
            started_at,
            finished_at,
            output_snapshot,
        ),
    )


def _upsert_job_status_postgres(
    conn: Any,
    job_id: str,
    status: str,
    params_json: dict,
    stats_json: dict | None = None,
    error_msg: str | None = None,
    output_snapshot: str | None = None,
    started_at: str | None = None,
    finished_at: str | None = None,
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


def _resolve_backends(args: argparse.Namespace) -> tuple[str, str]:
    metadata_backend = getattr(args, "metadata_backend", "postgres")
    asset_storage_backend = getattr(args, "asset_storage_backend", None)
    if not asset_storage_backend:
        asset_storage_backend = "local" if metadata_backend == "sqlite" else "minio"
    return metadata_backend, asset_storage_backend


def run_ingest(args: argparse.Namespace) -> dict:
    run_dir = Path(args.run_dir)
    if not run_dir.exists():
        raise FileNotFoundError(f"Run directory not found: {run_dir}")
    rows_path = run_dir / "index_rows.jsonl"
    if not rows_path.exists():
        raise FileNotFoundError(f"index_rows.jsonl not found under run dir: {run_dir}")

    metadata_backend, asset_storage_backend = _resolve_backends(args)
    postgres_batch_size = _postgres_batch_size(args)

    started_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    params = {
        "run_dir": str(run_dir.resolve()),
        "dataset": args.dataset,
        "sensor": args.sensor,
        "asset_version": args.asset_version,
        "cube_version": args.cube_version,
        "quality_rule": args.quality_rule,
        "metadata_backend": metadata_backend,
        "asset_storage_backend": asset_storage_backend,
        "postgres_batch_size": postgres_batch_size,
    }

    rows = load_rows(rows_path)
    if asset_storage_backend == "minio":
        asset_uri_map = upload_assets_to_minio(
            rows=rows,
            dataset=args.dataset,
            sensor=args.sensor,
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
            sensor=args.sensor,
            asset_version=args.asset_version,
            cog_output_root=Path(args.cog_output_root),
            materialize_mode=args.cog_materialize_mode,
        )

    raw_records = build_raw_asset_records(
        rows=rows,
        dataset=args.dataset,
        sensor=args.sensor,
        asset_version=args.asset_version,
        run_id=args.job_id,
        asset_uri_map=asset_uri_map,
    )
    cube_records = build_cube_fact_records(
        rows=rows,
        cube_version=args.cube_version,
        run_id=args.job_id,
        quality_rule=args.quality_rule,
        asset_uri_map=asset_uri_map,
    )

    stats = {
        "run_dir": str(run_dir.resolve()),
        "input_rows": len(rows),
        "materialized_cog_assets": len(asset_uri_map),
        "raw_asset_rows": len(raw_records),
        "cube_fact_rows": len(cube_records),
        "metadata_backend": metadata_backend,
        "asset_storage_backend": asset_storage_backend,
        "postgres_batch_size": postgres_batch_size,
    }

    if metadata_backend == "sqlite":
        db_path = Path(args.db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode = WAL")
        try:
            ensure_tables(conn)
            _upsert_job_status(
                conn,
                job_id=args.job_id,
                status="running",
                params_json=params,
                started_at=started_at,
            )
            upsert_raw_assets(conn, raw_records)
            upsert_cube_facts(conn, cube_records)
            finished_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            _upsert_job_status(
                conn,
                job_id=args.job_id,
                status="succeeded",
                params_json=params,
                stats_json=stats,
                output_snapshot=f"cube_version={args.cube_version},job_id={args.job_id}",
                started_at=started_at,
                finished_at=finished_at,
            )
            conn.commit()
            _report_cube_fact_metrics(
                cube_records,
                data_type=str(getattr(args, "data_type", "optical") or "optical"),
                dataset=str(args.dataset),
                sensor=str(args.sensor),
            )
            return stats
        except Exception as exc:
            finished_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            _upsert_job_status(
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
        finally:
            conn.close()

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
            ensure_tables_postgres(conn)
            _upsert_job_status_postgres(
                conn,
                job_id=args.job_id,
                status="running",
                params_json=params,
                started_at=started_at,
            )
            upsert_raw_assets_postgres(conn, raw_records, batch_size=postgres_batch_size)
            upsert_cube_facts_postgres(conn, cube_records, batch_size=postgres_batch_size)
            finished_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            _upsert_job_status_postgres(
                conn,
                job_id=args.job_id,
                status="succeeded",
                params_json=params,
                stats_json=stats,
                output_snapshot=f"cube_version={args.cube_version},job_id={args.job_id}",
                started_at=started_at,
                finished_at=finished_at,
            )
            conn.commit()
            _report_cube_fact_metrics(
                cube_records,
                data_type=str(getattr(args, "data_type", "optical") or "optical"),
                dataset=str(args.dataset),
                sensor=str(args.sensor),
            )
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
    stats = run_ingest(args)
    stats["elapsed_sec"] = round(time.perf_counter() - start, 3)
    print(json.dumps(stats, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
