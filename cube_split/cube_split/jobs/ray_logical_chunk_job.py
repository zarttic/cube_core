"""Distributed, metadata-only logical grid chunk generation for Ray workers."""

from __future__ import annotations

import json
import os
from collections import deque
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any, Callable, Iterator

from cube_split import runtime_config
from cube_split.jobs.logical_chunk_codec import compress_logical_chunk, logical_chunk_id, serialize_logical_chunk_rows
from cube_split.partition_timing import TimingRecorder


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
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC).strftime(
        {"second": "%Y%m%d%H%M%S", "minute": "%Y%m%d%H%M", "hour": "%Y%m%d%H", "day": "%Y%m%d", "month": "%Y%m"}[granularity]
    )


def _iter_shards(
    bbox: list[float] | tuple[float, float, float, float], degrees: float,
) -> Iterator[tuple[float, float, float, float]]:
    west, south, east, north = (float(value) for value in bbox)
    if not -180 <= west <= 180 or not -180 <= east <= 180 or not -90 <= south <= 90 or not -90 <= north <= 90 or west > east or south > north:
        raise ValueError("logical chunk source bbox must be a non-wrapping WGS84 bbox")
    lat = south
    while lat < north:
        next_lat = min(north, lat + degrees)
        lon = west
        while lon < east:
            next_lon = min(east, lon + degrees)
            yield lon, lat, next_lon, next_lat
            lon = next_lon
        lat = next_lat


def _shards(bbox: list[float] | tuple[float, float, float, float], degrees: float) -> list[tuple[float, float, float, float]]:
    return list(_iter_shards(bbox, degrees))


def _logical_task_limit() -> int:
    raw = runtime_config.env_text("CUBE_LOGICAL_MAX_IN_FLIGHT", "4")
    try:
        return max(1, int(raw))
    except ValueError:
        return 4


def _logical_task_values(payload: dict[str, Any]) -> Iterator[dict[str, Any]]:
    degrees = float(runtime_config.env_text("CUBE_LOGICAL_SHARD_DEGREES", "1"))
    if not 0 < degrees <= 10:
        raise ValueError("CUBE_LOGICAL_SHARD_DEGREES must be within (0, 10]")
    per_task = max(1, int(runtime_config.env_text("CUBE_LOGICAL_SHARDS_PER_TASK", "16")))
    for asset in payload["dataset"]["assets"]:
        shards: list[tuple[float, float, float, float]] = []
        shard_number = 0
        for shard in _iter_shards(asset["bbox"], degrees):
            shards.append(shard)
            if len(shards) < per_task:
                continue
            yield {
                **payload,
                "asset": asset,
                "shards": shards,
                "shard_id": f"{asset['source_asset_id']}:{shard_number}",
            }
            shard_number += 1
            shards = []
        if shards:
            yield {
                **payload,
                "asset": asset,
                "shards": shards,
                "shard_id": f"{asset['source_asset_id']}:{shard_number}",
            }


def _plan_logical_chunk(value: dict[str, Any]) -> dict[str, Any]:
    import socket
    from io import BytesIO

    from grid_core.app.core.enums import BoundaryType
    from grid_core.app.models.grid_address import GridAddress
    from grid_core.sdk import CubeEncoderSDK
    from minio import Minio

    dataset, asset = value["dataset"], value["asset"]
    timing = TimingRecorder("logical_worker")
    timing.set_attribute("worker_hostname", socket.gethostname())
    timing.set_attribute("shard_id", value["shard_id"])
    timing.set_attribute("source_asset_id", asset["source_asset_id"])
    sdk = CubeEncoderSDK()
    grid_type, level = value["grid_type"], int(value["requested_grid_level"])
    bucket = _time_bucket(asset["time_start"], value["time_granularity"])
    timestamp = datetime.fromisoformat(asset["time_start"].replace("Z", "+00:00"))
    bands = [band for band in dataset["bands"] if band["source_asset_id"] == asset["source_asset_id"]]
    cells: dict[tuple[str, int, str | None], dict[str, Any]] = {}
    rows: list[dict[str, Any]] = []
    for shard in value["shards"]:
        with timing.phase("grid.cover"):
            covered = sdk.cover(
                grid_type=grid_type,
                requested_grid_level=level,
                cover_mode=value["cover_mode"],
                boundary_type=BoundaryType.BBOX,
                bbox=shard,
                crs="EPSG:4326",
            )
        timing.add_counter("grid_shard_count")
        with timing.phase("logical.row_generation"):
            for cell in covered:
                timing.add_counter("grid_cell_count")
                address = GridAddress(grid_type=grid_type, grid_level=int(cell.grid_level), space_code=cell.space_code, topology_code=cell.topology_code)
                cell_key = (cell.space_code, int(cell.grid_level), cell.topology_code)
                if cell_key not in cells:
                    geometry = cell.geometry
                    if geometry is None:
                        with timing.phase("grid.geometry"):
                            geometry = sdk.code_to_geometry(address=address)
                    cells[cell_key] = {
                        "output_id": logical_output_id(dataset_id=dataset["dataset_id"], output_version=value["output_version"], source_asset_id="_grid", band_code="_cell", grid_type=grid_type, grid_level=int(cell.grid_level), space_code=cell.space_code, topology_code=cell.topology_code, time_bucket="_", window_identity="cell"),
                        "grid_type": grid_type, "grid_level": int(cell.grid_level), "space_code": cell.space_code, "topology_code": cell.topology_code,
                        "bbox": cell.bbox, "geometry": geometry,
                    }
                for band in bands:
                    output_id = logical_output_id(dataset_id=dataset["dataset_id"], output_version=value["output_version"], source_asset_id=asset["source_asset_id"], band_code=band["band_code"], grid_type=grid_type, grid_level=int(cell.grid_level), space_code=cell.space_code, topology_code=cell.topology_code, time_bucket=bucket, window_identity="logical-reference")
                    base = {"source_asset_id": asset["source_asset_id"], "band_code": band["band_code"], "grid_type": grid_type, "grid_level": int(cell.grid_level), "space_code": cell.space_code, "topology_code": cell.topology_code, "time_bucket": bucket}
                    rows.append({"kind": "tiles", "row": {"output_id": output_id, **base, "tile_uri": asset["cog_uri"], "tile_kind": "logical_reference", "bbox": cell.bbox}})
                    rows.append({"kind": "indexes", "row": {"output_id": f"{output_id}-index", "tile_output_id": None, **base, "acquisition_time": asset["time_start"], "st_code": sdk.generate_st_code(address=address, timestamp=timestamp, time_granularity=value["time_granularity"]).st_code, "value_ref_uri": asset["cog_uri"], "window_col_off": None, "window_row_off": None, "window_width": None, "window_height": None, "attributes": {"band_unit_id": (band.get("attributes") or {}).get("band_unit_id")}}})
    rows = [{"kind": "grid_cells", "row": row} for row in cells.values()] + rows
    with timing.phase("logical.chunk_serialize"):
        content = serialize_logical_chunk_rows(rows)
        body = compress_logical_chunk(content)
    checksum = sha256(body).hexdigest()
    chunk_id = logical_chunk_id(
        dataset_id=dataset["dataset_id"], output_version=value["output_version"],
        shard_id=value["shard_id"], bands=bands,
    )
    settings = runtime_config.minio_settings()
    key = f"partition/{dataset['dataset_id']}/versions/{value['output_version']}/logical-chunks/{chunk_id}.jsonl.gz"
    client = Minio(settings.endpoint, access_key=settings.access_key, secret_key=settings.secret_key, secure=settings.secure)
    with timing.phase("minio.chunk_stat"):
        try:
            existing = client.stat_object(settings.bucket, key)
        except Exception as exc:
            if getattr(exc, "code", None) not in {"NoSuchKey", "NoSuchObject", "ResourceNotFound"}:
                raise
            existing = None
    if existing is None:
        with timing.phase("minio.chunk_upload"):
            client.put_object(settings.bucket, key, BytesIO(body), len(body), content_type="application/gzip", metadata={"checksum-sha256": checksum})
    else:
        metadata = {str(name).lower(): str(item) for name, item in (getattr(existing, "metadata", {}) or {}).items()}
        if int(getattr(existing, "size", -1)) != len(body) or (metadata.get("checksum-sha256") or metadata.get("x-amz-meta-checksum-sha256")) != checksum:
            raise RuntimeError(f"immutable logical chunk collision for {key}")
    with timing.phase("opengauss.logical_stage"):
        _stage_chunk_rows(
            dataset_id=dataset["dataset_id"], output_version=value["output_version"],
            chunk_id=chunk_id, rows=rows,
        )
    timing.add_counter("tile_row_count", sum(row["kind"] == "tiles" for row in rows))
    timing.add_counter("index_row_count", sum(row["kind"] == "indexes" for row in rows))
    return {
        "chunk_id": chunk_id,
        "object_uri": f"s3://{settings.bucket}/{key}",
        "checksum": checksum,
        "byte_size": len(body),
        "grid_cell_count": len(cells),
        "tile_count": sum(row["kind"] == "tiles" for row in rows),
        "index_count": sum(row["kind"] == "indexes" for row in rows),
        "timing": timing.finish(),
    }


def run_logical_chunk_jobs(
    payloads: list[dict[str, Any]], runtime_env: dict[str, Any] | None,
    cancellation_check: Callable[[], bool] | None = None,
) -> list[dict[str, Any]]:
    """Plan all logical dataset chunks through one bounded Ray queue."""
    import ray

    driver_timing = TimingRecorder("logical_batch_driver")
    driver_timing.set_attribute("ray_address", payloads[0]["ray_address"])
    driver_timing.set_attribute("payload_count", len(payloads))
    if not ray.is_initialized():
        with driver_timing.phase("ray.init"):
            ray.init(
                address=payloads[0]["ray_address"],
                ignore_reinit_error=True,
                include_dashboard=False,
                logging_level=40,
                runtime_env=_ray_init_runtime_env(runtime_env),
            )
    else:
        driver_timing.add_counter("ray_init_reused")
    plan_chunk = ray.remote(_plan_logical_chunk)
    queued = deque(range(len(payloads)))
    task_values = [iter(_logical_task_values(payload)) for payload in payloads]
    chunks: list[list[dict[str, Any]]] = [[] for _ in payloads]
    errors: list[str | None] = [None for _ in payloads]
    pending: dict[Any, int] = {}
    limit = _logical_task_limit()
    while queued or pending:
        while queued and len(pending) < limit:
            index = queued.popleft()
            if errors[index] is not None:
                continue
            try:
                with driver_timing.phase("ray.task_prepare"):
                    value = next(task_values[index])
            except StopIteration:
                continue
            with driver_timing.phase("ray.task_submit"):
                pending[plan_chunk.options(num_cpus=1).remote(value)] = index
            driver_timing.add_counter("ray_task_submit_count")
            queued.append(index)
        if cancellation_check and cancellation_check():
            for ref in pending:
                ray.cancel(ref, force=True)
            from cube_split.jobs.cancellation import PartitionCancelledError
            raise PartitionCancelledError("Partition task cancelled")
        if not pending:
            continue
        with driver_timing.phase("ray.wait"):
            ready, _ = ray.wait(list(pending), num_returns=1, timeout=1.0)
        for ref in ready:
            index = pending.pop(ref)
            try:
                with driver_timing.phase("ray.result_get"):
                    chunks[index].append(ray.get(ref))
            except Exception as exc:
                errors[index] = str(exc)
                for active_ref, active_index in tuple(pending.items()):
                    if active_index == index:
                        ray.cancel(active_ref, force=True)
                        pending.pop(active_ref)
    result_payloads: list[tuple[dict[str, Any], list[dict[str, Any]]] | None] = []
    with driver_timing.phase("driver.result_merge"):
        for index, payload in enumerate(payloads):
            if errors[index] is not None:
                result_payloads.append(None)
                continue
            worker_timings = [chunk["timing"] for chunk in chunks[index] if isinstance(chunk.get("timing"), dict)]
            result_payloads.append((payload, worker_timings))
    driver_record = driver_timing.finish()
    first_success_index = next(
        (index for index, outcome in enumerate(result_payloads) if outcome is not None),
        None,
    )
    results = []
    for index, payload in enumerate(payloads):
        if errors[index] is not None:
            results.append({"error": errors[index]})
            continue
        result_payload = result_payloads[index]
        if result_payload is None:
            results.append({"error": "logical result assembly failed"})
            continue
        _payload, worker_timings = result_payload
        timings = {"workers": worker_timings}
        if index == first_success_index:
            timings["driver"] = driver_record
        results.append({"result": {
                "dataset_id": payload["dataset"]["dataset_id"], "task_id": payload["task_id"],
                "output_version": payload["output_version"], "grid_type": payload["grid_type"],
                "requested_grid_level": payload["requested_grid_level"], "partition_method": "logical",
                "execution_engine": "ray",
                "object_prefix": f"partition/{payload['dataset']['dataset_id']}/versions/{payload['output_version']}/",
                "tiles": [], "indexes": [], "grid_cells": [], "chunks": chunks[index],
                "timings": timings,
        }})
    return results


def run_logical_chunk_job(
    payload: dict[str, Any], runtime_env: dict[str, Any] | None,
    cancellation_check: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Submit one logical dataset through the batch scheduler compatibility path."""
    outcome = run_logical_chunk_jobs([payload], runtime_env, cancellation_check)[0]
    if "error" in outcome:
        raise RuntimeError(str(outcome["error"]))
    return outcome["result"]
