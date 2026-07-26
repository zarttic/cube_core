"""Distributed, metadata-only logical grid chunk generation for Ray workers."""

from __future__ import annotations

import gzip
import json
import os
from datetime import datetime
from hashlib import sha256
from typing import Any, Callable
from urllib.parse import urlparse

from cube_split import runtime_config


def _ray_init_runtime_env(runtime_env: dict[str, Any] | None) -> dict[str, Any] | None:
    """Avoid reapplying a Ray Job's runtime environment from its own driver."""
    if os.environ.get("CUBE_WEB_RAY_JOB_DRIVER") == "1":
        return None
    return runtime_env


def _stage_chunk_rows(
    *, dataset_id: str, output_version: str, chunk_id: str, rows: list[dict[str, Any]],
) -> None:
    """Persist a chunk's rows in the version-isolated OpenGauss staging table."""
    dsn = runtime_config.require_postgres_dsn()
    import psycopg

    values = [
        (dataset_id, output_version, chunk_id, number, item["kind"], json.dumps(item["row"], separators=(",", ":")))
        for number, item in enumerate(rows)
    ]
    with psycopg.connect(dsn, client_encoding="UTF8") as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM partition_logical_staging_rows "
                "WHERE dataset_id=%s AND output_version=%s AND chunk_id=%s",
                (dataset_id, output_version, chunk_id),
            )
            existing = int(cursor.fetchone()[0])
            if existing == len(values):
                return
            if existing:
                raise RuntimeError(f"logical staging chunk is incomplete: {chunk_id}")
            with cursor.copy(
                "COPY partition_logical_staging_rows "
                "(dataset_id,output_version,chunk_id,row_number,kind,payload) FROM STDIN"
            ) as copy:
                for row in values:
                    copy.write_row(row)
        connection.commit()


def logical_output_id(
    *, dataset_id: str, output_version: str, source_asset_id: str, band_code: str,
    grid_type: str, grid_level: int, space_code: str, topology_code: str | None,
    time_bucket: str, window_identity: str,
) -> str:
    """Match the normalized partition output identity without importing cube_web."""
    identity = {
        "dataset_id": dataset_id, "output_version": output_version, "source_asset_id": source_asset_id,
        "band_code": band_code, "grid_type": grid_type, "grid_level": grid_level,
        "space_code": space_code, "topology_code": topology_code, "time_bucket": time_bucket,
        "window_identity": window_identity,
    }
    return sha256(json.dumps(identity, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def _time_bucket(value: str, granularity: str) -> str:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).strftime(
        {"second": "%Y%m%d%H%M%S", "minute": "%Y%m%d%H%M", "hour": "%Y%m%d%H", "day": "%Y%m%d", "month": "%Y%m"}[granularity]
    )


def _shards(bbox: list[float] | tuple[float, float, float, float], degrees: float) -> list[tuple[float, float, float, float]]:
    west, south, east, north = (float(value) for value in bbox)
    if not -180 <= west <= 180 or not -180 <= east <= 180 or not -90 <= south <= 90 or not -90 <= north <= 90 or west > east or south > north:
        raise ValueError("logical chunk source bbox must be a non-wrapping WGS84 bbox")
    result = []
    lat = south
    while lat < north:
        next_lat = min(north, lat + degrees)
        lon = west
        while lon < east:
            next_lon = min(east, lon + degrees)
            result.append((lon, lat, next_lon, next_lat))
            lon = next_lon
        lat = next_lat
    return result


def run_logical_chunk_job(
    payload: dict[str, Any], runtime_env: dict[str, Any] | None,
    cancellation_check: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Submit spatial planning work; source COGs are never opened in this path."""
    import ray

    @ray.remote
    def plan_chunk(value: dict[str, Any]) -> dict[str, Any]:
        from io import BytesIO

        from grid_core.app.core.enums import BoundaryType
        from grid_core.app.models.grid_address import GridAddress
        from grid_core.sdk import CubeEncoderSDK
        from minio import Minio

        dataset, asset = value["dataset"], value["asset"]
        sdk = CubeEncoderSDK()
        grid_type, level = value["grid_type"], int(value["requested_grid_level"])
        bucket = _time_bucket(asset["time_start"], value["time_granularity"])
        timestamp = datetime.fromisoformat(asset["time_start"].replace("Z", "+00:00"))
        bands = [band for band in dataset["bands"] if band["source_asset_id"] == asset["source_asset_id"]]
        cells: dict[tuple[str, int, str | None], dict[str, Any]] = {}
        rows: list[dict[str, Any]] = []
        for shard in value["shards"]:
            for cell in sdk.cover(grid_type=grid_type, requested_grid_level=level, cover_mode=value["cover_mode"], boundary_type=BoundaryType.BBOX, bbox=shard, crs="EPSG:4326"):
                address = GridAddress(grid_type=grid_type, grid_level=int(cell.grid_level), space_code=cell.space_code, topology_code=cell.topology_code)
                cell_key = (cell.space_code, int(cell.grid_level), cell.topology_code)
                if cell_key not in cells:
                    cells[cell_key] = {
                        "output_id": logical_output_id(dataset_id=dataset["dataset_id"], output_version=value["output_version"], source_asset_id="_grid", band_code="_cell", grid_type=grid_type, grid_level=int(cell.grid_level), space_code=cell.space_code, topology_code=cell.topology_code, time_bucket="_", window_identity="cell"),
                        "grid_type": grid_type, "grid_level": int(cell.grid_level), "space_code": cell.space_code, "topology_code": cell.topology_code,
                        "bbox": cell.bbox, "geometry": cell.geometry or sdk.code_to_geometry(address=address),
                    }
                for band in bands:
                    output_id = logical_output_id(dataset_id=dataset["dataset_id"], output_version=value["output_version"], source_asset_id=asset["source_asset_id"], band_code=band["band_code"], grid_type=grid_type, grid_level=int(cell.grid_level), space_code=cell.space_code, topology_code=cell.topology_code, time_bucket=bucket, window_identity="logical-reference")
                    base = {"source_asset_id": asset["source_asset_id"], "band_code": band["band_code"], "grid_type": grid_type, "grid_level": int(cell.grid_level), "space_code": cell.space_code, "topology_code": cell.topology_code, "time_bucket": bucket}
                    rows.append({"kind": "tiles", "row": {"output_id": output_id, **base, "tile_uri": asset["cog_uri"], "tile_kind": "logical_reference", "bbox": cell.bbox}})
                    rows.append({"kind": "indexes", "row": {"output_id": f"{output_id}-index", "tile_output_id": None, **base, "acquisition_time": asset["time_start"], "st_code": sdk.generate_st_code(address=address, timestamp=timestamp, time_granularity=value["time_granularity"]).st_code, "value_ref_uri": asset["cog_uri"], "window_col_off": None, "window_row_off": None, "window_width": None, "window_height": None, "attributes": {"band_unit_id": (band.get("attributes") or {}).get("band_unit_id")}}})
        rows = [{"kind": "grid_cells", "row": row} for row in cells.values()] + rows
        body = gzip.compress(b"".join(json.dumps(row, separators=(",", ":"), ensure_ascii=False).encode() + b"\n" for row in rows))
        checksum = sha256(body).hexdigest()
        chunk_id = sha256(f"{dataset['dataset_id']}\0{value['output_version']}\0{value['shard_id']}".encode()).hexdigest()[:32]
        settings = runtime_config.minio_settings()
        key = f"partition/{dataset['dataset_id']}/versions/{value['output_version']}/logical-chunks/{chunk_id}.jsonl.gz"
        client = Minio(settings.endpoint, access_key=settings.access_key, secret_key=settings.secret_key, secure=settings.secure)
        try:
            existing = client.stat_object(settings.bucket, key)
        except Exception as exc:
            if getattr(exc, "code", None) not in {"NoSuchKey", "NoSuchObject", "ResourceNotFound"}:
                raise
            existing = None
        if existing is None:
            client.put_object(settings.bucket, key, BytesIO(body), len(body), content_type="application/gzip", metadata={"checksum-sha256": checksum})
        else:
            metadata = {str(name).lower(): str(item) for name, item in (getattr(existing, "metadata", {}) or {}).items()}
            if int(getattr(existing, "size", -1)) != len(body) or (metadata.get("checksum-sha256") or metadata.get("x-amz-meta-checksum-sha256")) != checksum:
                raise RuntimeError(f"immutable logical chunk collision for {key}")
        _stage_chunk_rows(
            dataset_id=dataset["dataset_id"], output_version=value["output_version"],
            chunk_id=chunk_id, rows=rows,
        )
        return {"chunk_id": chunk_id, "object_uri": f"s3://{settings.bucket}/{key}", "checksum": checksum, "byte_size": len(body), "grid_cell_count": len(cells), "tile_count": sum(row["kind"] == "tiles" for row in rows), "index_count": sum(row["kind"] == "indexes" for row in rows)}

    if not ray.is_initialized():
        ray.init(
            address=payload["ray_address"],
            ignore_reinit_error=True,
            include_dashboard=False,
            logging_level=40,
            runtime_env=_ray_init_runtime_env(runtime_env),
        )
    degrees = float(runtime_config.env_text("CUBE_LOGICAL_SHARD_DEGREES", "1"))
    if not 0 < degrees <= 10:
        raise ValueError("CUBE_LOGICAL_SHARD_DEGREES must be within (0, 10]")
    per_task = max(1, int(runtime_config.env_text("CUBE_LOGICAL_SHARDS_PER_TASK", "16")))
    tasks = []
    for asset in payload["dataset"]["assets"]:
        asset_shards = _shards(asset["bbox"], degrees)
        for start in range(0, len(asset_shards), per_task):
            tasks.append({**payload, "asset": asset, "shards": asset_shards[start:start + per_task], "shard_id": f"{asset['source_asset_id']}:{start // per_task}"})
    pending = [plan_chunk.options(num_cpus=1).remote(task) for task in tasks]
    chunks = []
    while pending:
        if cancellation_check and cancellation_check():
            for ref in pending:
                ray.cancel(ref, force=True)
            from cube_split.jobs.cancellation import PartitionCancelledError
            raise PartitionCancelledError("Partition task cancelled")
        ready, pending = ray.wait(pending, num_returns=1, timeout=1.0)
        if ready:
            chunks.append(ray.get(ready[0]))
    return {"dataset_id": payload["dataset"]["dataset_id"], "task_id": payload["task_id"], "output_version": payload["output_version"], "grid_type": payload["grid_type"], "requested_grid_level": payload["requested_grid_level"], "partition_method": "logical", "execution_engine": "ray", "object_prefix": f"partition/{payload['dataset']['dataset_id']}/versions/{payload['output_version']}/", "tiles": [], "indexes": [], "grid_cells": [], "chunks": chunks}
