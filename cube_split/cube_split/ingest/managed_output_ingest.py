"""Ingest a committed normalized partition output into the legacy RS catalog."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable
from urllib.parse import unquote, urlparse

from psycopg.rows import dict_row

from cube_split import runtime_config
from cube_split.ingest.carbon_ingest_job import (
    CarbonObservationFact,
    ensure_carbon_tables_postgres,
    upsert_carbon_facts_postgres,
)
from cube_split.ingest.product_ingest_job import (
    ProductAssetRecord,
    ProductFactRecord,
    ensure_product_tables_postgres,
    upsert_product_assets_postgres,
    upsert_product_facts_postgres,
)
from cube_split.ingest.ray_ingest_job import (
    CubeFactRecord,
    RawAssetRecord,
    _upsert_job_status_postgres,
    cell_geometry_geojson,
    ensure_tables_postgres,
    upsert_cube_facts_postgres,
    upsert_raw_assets_postgres,
)
from cube_split.jobs.entity_partition_job import (
    _ensure_entity_tables_postgres,
    _upsert_entity_tiles_postgres,
)


@dataclass(frozen=True)
class ManagedIngestResult:
    target_tables: tuple[str, ...]
    row_counts: dict[str, int]


def ingest_managed_output(
    conn: Any,
    *,
    dataset_id: str,
    output_dataset_id: str,
    output_version: str,
    ingest_job_id: str,
    scene_ids: tuple[str, ...],
    before_commit: Callable[[Any], None] | None = None,
) -> ManagedIngestResult:
    """MERGE one immutable dataset output and verify its RS target rows."""
    snapshot = _load_snapshot(conn, dataset_id, output_dataset_id, output_version, scene_ids)
    _verify_minio_objects(snapshot)
    data_type = snapshot["dataset"]["data_type"]
    method = snapshot["output"]["partition_method"]
    started_at = _timestamp()
    params = {
        "dataset": dataset_id,
        "output_dataset_id": output_dataset_id,
        "output_version": output_version,
        "scene_ids": list(scene_ids),
        "managed_output": True,
    }

    # All ensure functions are additive and retain existing RS tables/data.
    ensure_tables_postgres(conn)
    _upsert_job_status_postgres(conn, ingest_job_id, "running", params, started_at=started_at)
    try:
        if data_type == "carbon":
            result = _ingest_carbon(conn, snapshot, output_version, ingest_job_id)
        elif data_type == "product" and method == "logical":
            result = _ingest_product(conn, snapshot, output_version, ingest_job_id)
        else:
            result = _ingest_raster(conn, snapshot, output_version, ingest_job_id, entity=method == "entity")
        _verify_targets(conn, ingest_job_id, output_version, result)
        if before_commit is not None:
            before_commit(conn)
        _upsert_job_status_postgres(
            conn,
            ingest_job_id,
            "succeeded",
            params,
            stats_json=result.row_counts,
            output_snapshot=f"output_version={output_version}",
            started_at=started_at,
            finished_at=_timestamp(),
        )
        conn.commit()
        return result
    except Exception as exc:
        conn.rollback()
        ensure_tables_postgres(conn)
        _upsert_job_status_postgres(
            conn,
            ingest_job_id,
            "failed",
            params,
            error_msg=str(exc)[:2000],
            started_at=started_at,
            finished_at=_timestamp(),
        )
        conn.commit()
        raise


def _load_snapshot(conn: Any, dataset_id: str, output_dataset_id: str, output_version: str, scene_ids: tuple[str, ...]) -> dict[str, Any]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM datasets WHERE dataset_id=%s", (dataset_id,))
        dataset = cur.fetchone()
        cur.execute(
            "SELECT * FROM partition_output_versions WHERE dataset_id=%s AND output_version=%s AND status='completed'",
            (output_dataset_id, output_version),
        )
        output = cur.fetchone()
        if dataset is None or output is None:
            raise RuntimeError("completed managed partition output is missing")
        cur.execute(
            "SELECT s.scene_id,s.acquisition_time,s.attributes AS scene_attributes,sa.asset_id,sa.source_uri,sa.attributes AS asset_attributes "
            "FROM scenes s JOIN scene_assets sa ON sa.scene_id=s.scene_id "
            "WHERE s.dataset_id=%s AND s.scene_id=ANY(%s) AND sa.asset_role='data'",
            (dataset_id, list(scene_ids)),
        )
        scene_assets = cur.fetchall()
        if not scene_assets:
            raise RuntimeError("managed output has no Scene asset mapping")
        cur.execute(
            "SELECT i.*,a.cog_uri,a.source_uri,a.checksum AS source_checksum,a.time_start,a.attributes AS asset_attributes,b.band_name,"
            "g.bbox AS cell_bbox,g.geometry AS cell_geometry,t.width,t.height,t.checksum AS tile_checksum,t.tile_uri "
            "FROM partition_indexes i "
            "JOIN partition_dataset_assets a ON a.dataset_id=i.dataset_id AND a.source_asset_id=i.source_asset_id "
            "LEFT JOIN partition_dataset_bands b ON b.dataset_id=i.dataset_id AND b.source_asset_id=i.source_asset_id AND b.band_code=i.band_code "
            "JOIN partition_grid_cells g ON g.dataset_id=i.dataset_id AND g.output_version=i.output_version "
            " AND g.grid_type=i.grid_type AND g.grid_level=i.grid_level AND g.space_code=i.space_code "
            " AND (g.topology_code=i.topology_code OR (g.topology_code IS NULL AND i.topology_code IS NULL)) "
            "LEFT JOIN partition_tiles t ON t.output_id=i.tile_output_id "
            "WHERE i.dataset_id=%s AND i.output_version=%s ORDER BY i.output_id",
            (output_dataset_id, output_version),
        )
        indexes = cur.fetchall()
    if not indexes:
        raise RuntimeError("managed output has no indexes")
    return {"dataset": dict(dataset), "output": dict(output), "scene_assets": [dict(row) for row in scene_assets], "indexes": [dict(row) for row in indexes]}


def _asset_scene_map(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(row["asset_id"]): row for row in snapshot["scene_assets"]}


def _bbox(row: dict[str, Any]) -> tuple[float, float, float, float]:
    value = row["cell_bbox"]
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, (list, tuple)) or len(value) != 4:
        raise RuntimeError(f"partition grid cell has invalid bbox: {row['space_code']}")
    return tuple(float(item) for item in value)


def _geometry(row: dict[str, Any]) -> dict[str, Any]:
    value = row["cell_geometry"]
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise RuntimeError(f"partition grid cell has no geometry: {row['space_code']}")
    return value


def _ingest_raster(conn: Any, snapshot: dict[str, Any], version: str, job_id: str, *, entity: bool) -> ManagedIngestResult:
    asset_scenes = _asset_scene_map(snapshot)
    dataset = snapshot["dataset"]
    dataset_attributes = dataset.get("attributes") or {}
    if isinstance(dataset_attributes, str):
        dataset_attributes = json.loads(dataset_attributes)
    sensor = str(dataset_attributes.get("sensor") or dataset["data_type"])
    raw: dict[tuple[str, str], RawAssetRecord] = {}
    fact_candidates: dict[tuple[str, int, str, str, str], list[tuple[dict[str, Any], dict[str, Any], str]]] = {}
    entity_rows: list[dict[str, Any]] = []
    for row in snapshot["indexes"]:
        scene = asset_scenes.get(str(row["source_asset_id"]))
        if scene is None:
            raise RuntimeError(f"partition asset is not mapped to a selected Scene: {row['source_asset_id']}")
        scene_id = str(scene["scene_id"])
        acq = row.get("acquisition_time") or scene.get("acquisition_time") or row.get("time_start")
        if acq is None:
            raise RuntimeError(f"Scene acquisition time is missing: {scene_id}")
        acq_text = acq.isoformat() if hasattr(acq, "isoformat") else str(acq)
        source_uri = str(row.get("cog_uri") or row.get("source_uri") or "")
        band = str(row["band_code"])
        raw.setdefault((scene_id, band), RawAssetRecord(dataset["dataset_id"], sensor, scene_id, band, acq_text, source_uri, version, job_id))
        bounds = _bbox(row)
        value_uri = str(row["value_ref_uri"])
        key = (str(row["grid_type"]), int(row["grid_level"]), str(row["space_code"]), str(row["time_bucket"]), band)
        fact_candidates.setdefault(key, []).append((row, scene, acq_text))
        if entity:
            entity_rows.append({
                "scene_id": scene_id, "band": band, "acq_time": acq_text, "grid_type": key[0], "grid_level": key[1],
                "space_code": key[2], "space_code_prefix": key[2][:3], "st_code": str(row["st_code"]), "time_bucket": key[3],
                "entity_tile_uri": value_uri, "asset_path": value_uri, "source_asset_path": source_uri, "cover_mode": "intersect",
                "cell_min_lon": bounds[0], "cell_min_lat": bounds[1], "cell_max_lon": bounds[2], "cell_max_lat": bounds[3],
                "window_width": int(row.get("width") or 1), "window_height": int(row.get("height") or 1),
                "nodata": None, "valid_pixel_ratio": 1.0, "partition_type": "entity", "data_type": dataset["data_type"],
            })
    facts: list[CubeFactRecord] = []
    for key, candidates in fact_candidates.items():
        winner_row, winner_scene, _ = max(candidates, key=lambda item: (item[2], str(item[1]["scene_id"])))
        bounds = _bbox(winner_row)
        geom_text = cell_geometry_geojson(
            grid_type=key[0], grid_level=key[1], space_code=key[2],
            topology_code=winner_row.get("topology_code"), geometry=_geometry(winner_row),
        )
        candidate_scene_ids = sorted({str(item[1]["scene_id"]) for item in candidates})
        facts.append(CubeFactRecord(
            key[0], key[1], key[2], key[3], key[4], str(winner_row["st_code"]), *bounds, geom_text,
            str(winner_row["value_ref_uri"]), len(candidate_scene_ids),
            json.dumps({"winner_scene_id": str(winner_scene["scene_id"]), "candidate_scene_ids": candidate_scene_ids, "rule": "quality_approved_output"}),
            "quality_approved_output", version, job_id,
        ))
    ensure_tables_postgres(conn)
    upsert_raw_assets_postgres(conn, list(raw.values()))
    upsert_cube_facts_postgres(conn, facts)
    targets = ["rs_raw_scene_asset", "rs_cube_cell_fact"]
    counts = {"rs_raw_scene_asset": len(raw), "rs_cube_cell_fact": len(facts)}
    if entity:
        _ensure_entity_tables_postgres(conn)
        _upsert_entity_tiles_postgres(conn, entity_rows, dataset["dataset_id"], sensor, version, job_id)
        targets.append("rs_entity_tile_asset")
        counts["rs_entity_tile_asset"] = len(entity_rows)
    return ManagedIngestResult(tuple(targets), counts)


def _ingest_product(conn: Any, snapshot: dict[str, Any], version: str, job_id: str) -> ManagedIngestResult:
    asset_scenes = _asset_scene_map(snapshot)
    dataset = snapshot["dataset"]
    product_name = str(dataset.get("dataset_title") or dataset["dataset_id"])
    assets: dict[str, ProductAssetRecord] = {}
    facts: dict[tuple[str, int, str, str, str], ProductFactRecord] = {}
    for row in snapshot["indexes"]:
        scene = asset_scenes.get(str(row["source_asset_id"]))
        if scene is None:
            raise RuntimeError(f"partition asset is not mapped to a selected Scene: {row['source_asset_id']}")
        acq = row.get("acquisition_time") or scene.get("acquisition_time") or row.get("time_start")
        acq_text = acq.isoformat() if hasattr(acq, "isoformat") else str(acq)
        year = int(acq_text[:4])
        source_uri = str(row.get("cog_uri") or row.get("source_uri") or "")
        scene_id = str(scene["scene_id"])
        assets.setdefault(scene_id, ProductAssetRecord(dataset["dataset_id"], product_name, scene_id, year, acq_text, source_uri, version, job_id))
        bounds = _bbox(row)
        key = (str(row["grid_type"]), int(row["grid_level"]), str(row["space_code"]), str(row["time_bucket"]), str(row["band_code"]))
        facts[key] = ProductFactRecord(dataset["dataset_id"], product_name, year, key[4], key[0], key[1], key[2], key[3], str(row["st_code"]), *bounds, str(row["value_ref_uri"]), None, version, job_id)
    ensure_product_tables_postgres(conn)
    upsert_product_assets_postgres(conn, list(assets.values()))
    upsert_product_facts_postgres(conn, list(facts.values()))
    return ManagedIngestResult(("rs_product_asset", "rs_product_cell_fact"), {"rs_product_asset": len(assets), "rs_product_cell_fact": len(facts)})


def _ingest_carbon(conn: Any, snapshot: dict[str, Any], version: str, job_id: str) -> ManagedIngestResult:
    facts: dict[tuple[str, str, str], CarbonObservationFact] = {}
    for row in snapshot["indexes"]:
        attrs = row.get("attributes") or {}
        if isinstance(attrs, str):
            attrs = json.loads(attrs)
        required = {"satellite", "observation_id", "xco2", "center_lon", "center_lat"}
        missing = sorted(required - attrs.keys())
        if missing:
            raise RuntimeError(f"carbon managed output lacks observation attributes: {', '.join(missing)}")
        fact = CarbonObservationFact(
            str(attrs["satellite"]), str(attrs.get("product_type") or snapshot["dataset"].get("product_type") or "xco2"),
            str(attrs["observation_id"]), str(row["acquisition_time"]), str(row["time_bucket"]), str(row["grid_type"]),
            int(row["grid_level"]), str(row["space_code"]), str(row["st_code"]), float(attrs["xco2"]),
            None if attrs.get("quality_flag") is None else str(attrs["quality_flag"]), float(attrs["center_lon"]), float(attrs["center_lat"]),
            json.dumps(attrs.get("footprint_geojson") or {}), str(row["value_ref_uri"]),
            None if attrs.get("source_index") is None else int(attrs["source_index"]), json.dumps(attrs.get("metadata_json") or {}), version, job_id,
        )
        facts[(fact.satellite, fact.observation_id, fact.product_type)] = fact
    ensure_carbon_tables_postgres(conn)
    upsert_carbon_facts_postgres(conn, list(facts.values()))
    return ManagedIngestResult(("rs_carbon_observation_fact",), {"rs_carbon_observation_fact": len(facts)})


def _verify_targets(conn: Any, job_id: str, version: str, result: ManagedIngestResult) -> None:
    version_columns = {
        "rs_raw_scene_asset": "version", "rs_cube_cell_fact": "cube_version", "rs_entity_tile_asset": "tile_version",
        "rs_product_asset": "version", "rs_product_cell_fact": "cube_version", "rs_carbon_observation_fact": "cube_version",
    }
    with conn.cursor() as cur:
        for table in result.target_tables:
            geometry_clause = (
                " AND cell_geom IS NOT NULL AND ST_SRID(cell_geom)=4326 "
                "AND ST_IsClosed(ST_ExteriorRing(cell_geom))" if table == "rs_cube_cell_fact" else ""
            )
            cur.execute(
                f"SELECT count(*) FROM {table} WHERE run_id=%s AND {version_columns[table]}=%s{geometry_clause}",
                (job_id, version),
            )
            actual = int(cur.fetchone()[0])
            if actual < result.row_counts[table] or actual == 0:
                raise RuntimeError(f"managed ingest verification failed for {table}: {actual} < {result.row_counts[table]}")


def _verify_minio_objects(snapshot: dict[str, Any]) -> None:
    """Stat every source/value object without exposing credentials in task data."""
    from minio import Minio

    settings = runtime_config.minio_settings()
    client = Minio(
        settings.endpoint,
        access_key=settings.access_key,
        secret_key=settings.secret_key,
        secure=settings.secure,
    )
    uris = {
        str(value).split("#", 1)[0]
        for row in snapshot["indexes"]
        for value in (row.get("cog_uri"), row.get("source_uri"), row.get("value_ref_uri"))
        if str(value or "").startswith("s3://")
    }
    tile_checksums = {
        str(row["value_ref_uri"]).split("#", 1)[0]: str(row["tile_checksum"])
        for row in snapshot["indexes"]
        if row.get("tile_checksum") and str(row.get("value_ref_uri") or "").startswith("s3://")
    }
    for uri in sorted(uris):
        parsed = urlparse(uri)
        if parsed.netloc != settings.bucket:
            raise RuntimeError(f"managed ingest object is outside configured MinIO bucket: {uri}")
        stat = client.stat_object(parsed.netloc, unquote(parsed.path.lstrip("/")))
        expected_checksum = tile_checksums.get(uri)
        if expected_checksum:
            metadata = {str(key).lower(): str(value) for key, value in (getattr(stat, "metadata", {}) or {}).items()}
            actual_checksum = metadata.get("checksum-sha256") or metadata.get("x-amz-meta-checksum-sha256")
            if actual_checksum != expected_checksum:
                raise RuntimeError(f"managed entity tile checksum mismatch: {uri}")


def _timestamp() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")
