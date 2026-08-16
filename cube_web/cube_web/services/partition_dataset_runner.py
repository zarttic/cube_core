"""Ray execution adapter for the normalized production dataset contract."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from threading import Lock
from typing import Any, Callable
from urllib.parse import urlparse

from cube_split import runtime_config
from cube_split.jobs.logical_chunk_codec import compress_logical_chunk, logical_chunk_id, serialize_logical_chunk_rows
from cube_split.partition_timing import TimingRecorder

from cube_web.services.partition_contracts import OutputIdentity, make_output_id


class SourceObjectMissingError(ValueError):
    """Raised before Ray work starts when a loader-owned source object is missing or empty.

    Logical partition plans derive grid coverage from declared asset bbox metadata and
    only open source objects lazily, so a missing source would otherwise produce index
    rows referencing objects that do not exist until quality catches them later.
    """


def _entity_ray_parallelism(default: int = 16) -> int:
    """Entity planning fan-out; CUBE_ENTITY_RAY_PARALLELISM overrides the default."""
    raw = runtime_config.env_text("CUBE_ENTITY_RAY_PARALLELISM")
    if raw:
        try:
            return max(1, int(raw))
        except ValueError:
            pass
    return default


ENTITY_RAY_PARALLELISM = _entity_ray_parallelism()
ENTITY_PLANNING_OVERLAP_DEGREES = 0.02
_ENTITY_COVER_CACHE: dict[tuple[str, int, str, tuple[float, ...]], list[dict[str, Any]]] = {}
_ENTITY_COVER_CACHE_LOCK = Lock()
_RAY_INIT_LOCK = Lock()
_SOURCE_CHECKSUM_CACHE: dict[tuple[str, int, int], tuple[str, int]] = {}
_SOURCE_CHECKSUM_CACHE_LOCK = Lock()


def _sha256_file(path: Path) -> tuple[str, int]:
    digest = sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _sha256_file_cached(path: Path) -> tuple[str, int, bool]:
    """Reuse a checksum for an unchanged worker-local source cache file."""
    stat = path.stat()
    key = (str(path), int(stat.st_size), int(stat.st_mtime_ns))
    with _SOURCE_CHECKSUM_CACHE_LOCK:
        cached = _SOURCE_CHECKSUM_CACHE.get(key)
    if cached is not None:
        return cached[0], cached[1], True
    digest, size = _sha256_file(path)
    with _SOURCE_CHECKSUM_CACHE_LOCK:
        _SOURCE_CHECKSUM_CACHE[key] = (digest, size)
    return digest, size, False
def _ray_init_runtime_env(runtime_env: dict[str, Any] | None) -> dict[str, Any] | None:
    """Ray Jobs already apply their runtime environment to the driver and children."""
    if os.environ.get("CUBE_WEB_RAY_JOB_DRIVER") == "1":
        return None
    return runtime_env


def _time_bucket(value: str, granularity: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    formats = {"second": "%Y%m%d%H%M%S", "minute": "%Y%m%d%H%M", "hour": "%Y%m%d%H", "day": "%Y%m%d", "month": "%Y%m"}
    return parsed.strftime(formats[granularity])


def _geometry_bbox(geometry: dict[str, Any] | None) -> list[float] | None:
    """Return a WGS84 bbox for a GeoJSON geometry returned by the encoder."""
    if not geometry:
        return None

    coordinates = geometry.get("coordinates")
    values: list[tuple[float, float]] = []

    def visit(value: Any) -> None:
        if isinstance(value, (list, tuple)) and len(value) >= 2 and all(isinstance(item, (int, float)) for item in value[:2]):
            values.append((float(value[0]), float(value[1])))
            return
        if isinstance(value, (list, tuple)):
            for item in value:
                visit(item)

    visit(coordinates)
    if not values:
        return None
    longitudes, latitudes = zip(*values)
    return [min(longitudes), min(latitudes), max(longitudes), max(latitudes)]


def _record_asset_cell(
    cells: set[tuple[str, int, str | None]],
    cell: tuple[str, int, str | None],
    max_cells_per_asset: int,
) -> None:
    """Record one located raw observation cell."""
    cells.add(cell)


def _consume_observation_budget(remaining: int | None, consumed: int) -> int | None:
    if remaining is None:
        return None
    return max(0, remaining - consumed)


def _source_band_index(band: dict[str, Any], source_band_count: int) -> int:
    """Resolve the one-based source raster band represented by a normalized band unit."""
    attributes = band.get("attributes") or {}
    value = attributes.get("source_band_index", int(band.get("display_order", 0)) + 1)
    index = int(value)
    if index < 1 or index > source_band_count:
        raise ValueError(f"source band index {index} is outside raster band count {source_band_count}")
    return index


def _carbon_index_attributes(row: dict[str, Any], *, source_index: int) -> dict[str, Any]:
    return {
        "satellite": row["satellite"],
        "observation_id": row["observation_id"],
        "xco2": row["xco2"],
        "quality_flag": row["quality_flag"],
        "center_lon": row["center_lon"],
        "center_lat": row["center_lat"],
        "footprint_geojson": row["footprint_geojson"],
        "source_index": source_index,
        "metadata_json": row["metadata_json"],
        "product_type": row["product_type"],
    }


def _wait_for_ray_result(ray: Any, ref: Any, cancellation_check: Callable[[], bool] | None) -> dict[str, Any]:
    """Wait for one Ray task while allowing the web task to cancel it."""
    if cancellation_check is None:
        return ray.get(ref)
    while True:
        if cancellation_check():
            try:
                ray.cancel(ref, force=True)
            except Exception:
                # A disconnected driver must still prevent the completed result from committing.
                pass
            from cube_split.jobs.cancellation import PartitionCancelledError

            raise PartitionCancelledError("Partition task cancelled")
        ready, _ = ray.wait([ref], num_returns=1, timeout=1.0)
        if ready:
            return ray.get(ready[0])


def _wait_for_ray_batch(
    ray: Any,
    pending: list[Any],
    parallelism: int,
) -> tuple[list[Any], list[Any]]:
    """Wait for one full parallel window of results from Ray."""
    if not pending:
        return [], []
    target = min(len(pending), max(1, int(parallelism)))
    return ray.wait(pending, num_returns=target)


def _logical_shards(bbox: list[float] | tuple[float, float, float, float], degrees: float) -> list[tuple[float, float, float, float]]:
    """Split a WGS84 asset extent into bounded planning shards."""
    west, south, east, north = _normalize_wgs84_bbox(bbox)
    if west > east:
        raise ValueError("logical global planning does not support antimeridian-wrapping source bounds")
    shards: list[tuple[float, float, float, float]] = []
    latitude = south
    while latitude < north:
        next_latitude = min(north, latitude + degrees)
        longitude = west
        while longitude < east:
            next_longitude = min(east, longitude + degrees)
            shards.append((longitude, latitude, next_longitude, next_latitude))
            longitude = next_longitude
        latitude = next_latitude
    return shards


def _entity_planning_shards(
    bbox: list[float] | tuple[float, float, float, float],
    parallelism: int | None = None,
) -> list[list[float]]:
    """Split one entity source into a stable square task grid."""
    if parallelism is None:
        parallelism = _entity_ray_parallelism()
    west, south, east, north = _normalize_wgs84_bbox(bbox)
    side = max(1, int(parallelism**0.5))
    shards: list[list[float]] = []
    for y_index in range(side):
        lower = south + (north - south) * y_index / side
        upper = south + (north - south) * (y_index + 1) / side
        for x_index in range(side):
            left = west + (east - west) * x_index / side
            right = west + (east - west) * (x_index + 1) / side
            shards.append([
                max(-180.0, left - ENTITY_PLANNING_OVERLAP_DEGREES),
                max(-90.0, lower - ENTITY_PLANNING_OVERLAP_DEGREES),
                min(180.0, right + ENTITY_PLANNING_OVERLAP_DEGREES),
                min(90.0, upper + ENTITY_PLANNING_OVERLAP_DEGREES),
            ])
    return shards[:parallelism]


def _bbox_intersects(
    left_bbox: list[float] | tuple[float, float, float, float],
    right_bbox: list[float] | tuple[float, float, float, float],
) -> bool:
    """Return whether two non-wrapping WGS84 bboxes overlap with positive area."""
    left = _normalize_wgs84_bbox(left_bbox)
    right = _normalize_wgs84_bbox(right_bbox)
    return max(left[0], right[0]) < min(left[2], right[2]) and max(left[1], right[1]) < min(left[3], right[3])


def _entity_cells_for_shard(
    cells: list[dict[str, Any]],
    shard: list[float] | tuple[float, float, float, float],
) -> list[dict[str, Any]]:
    """Select precomputed cells whose bboxes can intersect one planning shard."""
    return [cell for cell in cells if _bbox_intersects(cell["bbox"], shard)]


def _entity_cells_by_shard(
    cells: list[dict[str, Any]],
    shards: list[list[float]],
) -> list[list[dict[str, Any]]]:
    """Assign each covered cell to exactly one spatial shard.

    Planning shards overlap slightly to avoid raster edge effects. A pure
    intersection filter therefore schedules the same cell more than once. Use
    the cell center as a stable owner, falling back to the first intersecting
    shard for cells whose center lies outside the source extent.
    """
    assignments: list[list[dict[str, Any]]] = [[] for _ in shards]
    for cell in cells:
        candidates = [
            index for index, shard in enumerate(shards)
            if _bbox_intersects(cell["bbox"], shard)
        ]
        if not candidates:
            continue
        bbox = cell.get("bbox") or []
        center = cell.get("center")
        if not isinstance(center, (list, tuple)) or len(center) < 2:
            center = [
                (float(bbox[0]) + float(bbox[2])) / 2.0,
                (float(bbox[1]) + float(bbox[3])) / 2.0,
            ]
        owner = next(
            (
                index for index in candidates
                if _normalize_wgs84_bbox(shards[index])[0] <= float(center[0]) <= _normalize_wgs84_bbox(shards[index])[2]
                and _normalize_wgs84_bbox(shards[index])[1] <= float(center[1]) <= _normalize_wgs84_bbox(shards[index])[3]
            ),
            candidates[0],
        )
        assignments[owner].append(cell)
    return assignments


def _entity_cover_cells(
    *,
    grid_type: str,
    requested_grid_level: int,
    cover_mode: str,
    asset_bbox: list[float] | tuple[float, float, float, float],
    timing: TimingRecorder,
) -> list[dict[str, Any]]:
    """Compute one asset cover on the driver and reuse it across spatial tasks."""
    from grid_core.app.core.enums import BoundaryType
    from grid_core.sdk import CubeEncoderSDK

    normalized_bbox = tuple(_normalize_wgs84_bbox(asset_bbox))
    key = (grid_type, int(requested_grid_level), cover_mode, normalized_bbox)
    with _ENTITY_COVER_CACHE_LOCK:
        cached = _ENTITY_COVER_CACHE.get(key)
        if cached is not None:
            timing.add_counter("grid_cover_cache_hit_count")
            return cached
        sdk = CubeEncoderSDK()
        with timing.phase("grid.cover"):
            covered = sdk.cover(
                grid_type=grid_type,
                requested_grid_level=int(requested_grid_level),
                cover_mode=cover_mode,
                boundary_type=BoundaryType.BBOX,
                bbox=list(normalized_bbox),
                crs="EPSG:4326",
            )
        serialized = [cell.model_dump(mode="json") for cell in covered]
        _ENTITY_COVER_CACHE[key] = serialized
        timing.add_counter("grid_cover_cache_miss_count")
        return serialized


def _run_logical_dataset_on_ray(
    payload: dict[str, Any],
    runtime_env: dict[str, Any] | None,
    cancellation_check: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Plan Geohash/MGRS chunks from registered bounds without opening a COG."""
    import ray

    @ray.remote
    def plan_chunk(value: dict[str, Any]) -> dict[str, Any]:
        from datetime import datetime
        from hashlib import sha256
        from io import BytesIO

        from cube_split import runtime_config as worker_runtime_config
        from grid_core.app.core.enums import BoundaryType
        from grid_core.app.models.grid_address import GridAddress
        from grid_core.sdk import CubeEncoderSDK
        from minio import Minio

        dataset = value["dataset"]
        asset = value["asset"]
        grid_type = value["grid_type"]
        level = int(value["requested_grid_level"])
        sdk = CubeEncoderSDK()
        cells: dict[tuple[str, int, str | None], dict[str, Any]] = {}
        rows: list[dict[str, Any]] = []
        bucket = _time_bucket(asset["time_start"], value["time_granularity"])
        timestamp = datetime.fromisoformat(asset["time_start"].replace("Z", "+00:00"))
        bands = [band for band in dataset["bands"] if band["source_asset_id"] == asset["source_asset_id"]]
        for shard in value["shards"]:
            covered = sdk.cover(
                grid_type=grid_type,
                requested_grid_level=level,
                cover_mode=value["cover_mode"],
                boundary_type=BoundaryType.BBOX,
                bbox=shard,
                crs="EPSG:4326",
            )
            for cell in covered:
                address = GridAddress(
                    grid_type=grid_type, grid_level=int(cell.grid_level), space_code=cell.space_code, topology_code=cell.topology_code,
                )
                cell_key = (cell.space_code, int(cell.grid_level), cell.topology_code)
                if cell_key not in cells:
                    identity = OutputIdentity(
                        dataset_id=dataset["dataset_id"], output_version=value["output_version"], source_asset_id="_grid",
                        band_code="_cell", grid_type=grid_type, grid_level=int(cell.grid_level), space_code=cell.space_code,
                        topology_code=cell.topology_code, time_bucket="_", window_identity="cell",
                    )
                    cells[cell_key] = {
                        "output_id": make_output_id(identity), "grid_type": grid_type, "grid_level": int(cell.grid_level),
                        "space_code": cell.space_code, "topology_code": cell.topology_code, "bbox": cell.bbox,
                        "geometry": cell.geometry or sdk.code_to_geometry(address=address),
                    }
                for band in bands:
                    identity = OutputIdentity(
                        dataset_id=dataset["dataset_id"], output_version=value["output_version"], source_asset_id=asset["source_asset_id"],
                        band_code=band["band_code"], grid_type=grid_type, grid_level=int(cell.grid_level), space_code=cell.space_code,
                        topology_code=cell.topology_code, time_bucket=bucket, window_identity="logical-reference",
                    )
                    output_id = make_output_id(identity)
                    rows.append({"kind": "tiles", "row": {
                        "output_id": output_id, "source_asset_id": asset["source_asset_id"], "band_code": band["band_code"],
                        "grid_type": grid_type, "grid_level": int(cell.grid_level), "space_code": cell.space_code,
                        "topology_code": cell.topology_code, "time_bucket": bucket, "tile_uri": asset["cog_uri"],
                        "tile_kind": "logical_reference", "bbox": cell.bbox,
                    }})
                    rows.append({"kind": "indexes", "row": {
                        "output_id": f"{output_id}-index", "tile_output_id": None, "source_asset_id": asset["source_asset_id"],
                        "band_code": band["band_code"], "acquisition_time": asset["time_start"], "grid_type": grid_type,
                        "grid_level": int(cell.grid_level), "space_code": cell.space_code, "topology_code": cell.topology_code,
                        "time_bucket": bucket, "st_code": sdk.generate_st_code(address=address, timestamp=timestamp, time_granularity=value["time_granularity"]).st_code,
                        "value_ref_uri": asset["cog_uri"], "window_col_off": None, "window_row_off": None,
                        "window_width": None, "window_height": None, "attributes": {"band_unit_id": (band.get("attributes") or {}).get("band_unit_id")},
                    }})
        rows = [{"kind": "grid_cells", "row": row} for row in cells.values()] + rows
        content = serialize_logical_chunk_rows(rows)
        body = compress_logical_chunk(content)
        checksum = sha256(body).hexdigest()
        chunk_id = logical_chunk_id(
            dataset_id=dataset["dataset_id"], output_version=value["output_version"],
            shard_id=value["shard_id"], bands=bands,
        )
        settings = worker_runtime_config.minio_settings()
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
            remote_checksum = metadata.get("checksum-sha256") or metadata.get("x-amz-meta-checksum-sha256")
            if int(getattr(existing, "size", -1)) != len(body) or remote_checksum != checksum:
                raise RuntimeError(f"immutable logical chunk collision for {key}")
        counts = {"grid_cells": len(cells), "tiles": sum(item["kind"] == "tiles" for item in rows), "indexes": sum(item["kind"] == "indexes" for item in rows)}
        return {"chunk_id": chunk_id, "object_uri": f"s3://{settings.bucket}/{key}", "checksum": checksum, "byte_size": len(body), **{f"{name[:-1] if name.endswith('s') else name}_count": count for name, count in counts.items()}}

    if not ray.is_initialized():
        ray.init(address=payload["ray_address"], ignore_reinit_error=True, include_dashboard=False, logging_level=40, runtime_env=_ray_init_runtime_env(runtime_env))
    shard_degrees = float(runtime_config.env_text("CUBE_LOGICAL_SHARD_DEGREES", "1"))
    if not 0 < shard_degrees <= 10:
        raise ValueError("CUBE_LOGICAL_SHARD_DEGREES must be within (0, 10]")
    shards_per_task = max(1, int(runtime_config.env_text("CUBE_LOGICAL_SHARDS_PER_TASK", "16")))
    task_values: list[dict[str, Any]] = []
    for asset in payload["dataset"]["assets"]:
        asset_shards = _logical_shards(asset["bbox"], shard_degrees)
        for start in range(0, len(asset_shards), shards_per_task):
            shards = asset_shards[start:start + shards_per_task]
            task_values.append({**payload, "asset": asset, "shards": shards, "shard_id": f"{asset['source_asset_id']}:{start // shards_per_task}"})
    from cube_split.jobs.ray_logical_partition_job import _ray_actor_options_from_env

    options = {"num_cpus": 1, **_ray_actor_options_from_env()}
    pending = [plan_chunk.options(**options).remote(value) for value in task_values]
    chunks: list[dict[str, Any]] = []
    while pending:
        if cancellation_check is not None and cancellation_check():
            for ref in pending:
                ray.cancel(ref, force=True)
            from cube_split.jobs.cancellation import PartitionCancelledError
            raise PartitionCancelledError("Partition task cancelled")
        ready, pending = ray.wait(pending, num_returns=1, timeout=1.0)
        if ready:
            chunks.append(ray.get(ready[0]))
    return {
        "dataset_id": payload["dataset"]["dataset_id"], "task_id": payload["task_id"], "output_version": payload["output_version"],
        "grid_type": payload["grid_type"], "requested_grid_level": payload["requested_grid_level"], "partition_method": "logical",
        "execution_engine": "ray", "object_prefix": f"partition/{payload['dataset']['dataset_id']}/versions/{payload['output_version']}/",
        "tiles": [], "indexes": [], "grid_cells": [], "chunks": chunks,
    }


def _run_carbon_dataset_on_ray(
    payload: dict[str, Any],
    runtime_env: dict[str, Any] | None,
    cancellation_check: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Run one carbon dataset from loader-owned NetCDF/HDF sources on Ray."""
    import ray

    @ray.remote
    def execute(value: dict[str, Any]) -> dict[str, Any]:
        import socket
        from pathlib import Path

        from cube_split import runtime_config as worker_runtime_config
        from cube_split.jobs.ray_partition_core import cache_source_cog
        from cube_split.partition.carbon import CarbonPartitionConfig, load_observations_from_file, partition_observation
        from grid_core.sdk import CubeEncoderSDK
        from minio import Minio

        minio_settings = worker_runtime_config.minio_settings()
        settings = {
            "endpoint": minio_settings.endpoint,
            "access_key": minio_settings.access_key,
            "secret_key": minio_settings.secret_key,
            "bucket": minio_settings.bucket,
            "secure": minio_settings.secure,
        }
        client = Minio(
            settings["endpoint"], access_key=settings["access_key"], secret_key=settings["secret_key"], secure=settings["secure"]
        )

        dataset = value["dataset"]
        grid_type = value["grid_type"]
        requested_grid_level = int(value["requested_grid_level"])
        entity = grid_type == "isea4h"
        product_type = str(dataset.get("product_type") or "xco2")
        timing = TimingRecorder("carbon_worker")
        timing.set_attribute("worker_hostname", socket.gethostname())
        timing.set_attribute("grid_type", grid_type)
        timing.set_attribute("requested_grid_level", requested_grid_level)
        config = CarbonPartitionConfig(
            grid_type=grid_type,
            grid_level=requested_grid_level,
            time_granularity=value["time_granularity"],
            product_type=product_type,
        )
        sdk = CubeEncoderSDK()
        cells: dict[tuple[str, int, str | None], dict[str, Any]] = {}
        tiles: list[dict[str, Any]] = []
        indexes: list[dict[str, Any]] = []

        remaining_observations = value.get("max_observations")
        for asset in dataset["assets"]:
            if remaining_observations == 0:
                break
            source_uri = str(asset["source_uri"])
            parsed = urlparse(source_uri)
            if parsed.scheme != "s3" or not parsed.netloc:
                raise ValueError("normalized carbon source must use an accessible MinIO bucket")
            source_format = str(asset["source_format"])
            suffix = Path(parsed.path).suffix.lower()
            allowed_suffixes = {
                "netcdf": {".nc", ".nc4"},
                "hdf5": {".h5", ".hdf", ".hdf5"},
                "sif": {".sif"},
            }
            if suffix not in allowed_suffixes.get(source_format, set()):
                raise ValueError(f"carbon source suffix {suffix!r} does not match source_format={source_format!r}")
            cache_metrics: dict[str, Any] = {}
            with timing.phase("source.cache"):
                local_path = cache_source_cog(
                    source_uri,
                    Path("/tmp/cube_split_source_cache") / "loader",
                    client,
                    parsed.netloc,
                    metrics=cache_metrics,
                )
            timing.add_phase("source.download", float(cache_metrics.get("download_elapsed_sec") or 0.0))
            timing.add_counter("source_cache_hit_count" if cache_metrics.get("cache_hit") else "source_cache_miss_count")
            with timing.phase("source.checksum"):
                actual_source_checksum, source_size, checksum_cache_hit = _sha256_file_cached(local_path)
            timing.add_counter("source_checksum_cache_hit_count" if checksum_cache_hit else "source_checksum_cache_miss_count")
            if actual_source_checksum != asset["checksum"]:
                raise ValueError("carbon source checksum does not match the strict loader contract")
            with timing.phase("carbon.observation_load"):
                observations = load_observations_from_file(
                    local_path,
                    max_observations=remaining_observations,
                    product_type=product_type,
                )
            remaining_observations = _consume_observation_budget(remaining_observations, len(observations))
            timing.add_counter("observation_count", len(observations))
            bands = [band for band in dataset["bands"] if band["source_asset_id"] == asset["source_asset_id"]]
            asset_cell_keys: set[tuple[str, int, str | None]] = set()
            for ordinal, observation in enumerate(observations):
                with timing.phase("carbon.partition_observation"):
                    base_row = partition_observation(observation, config, sdk=sdk)
                with timing.phase("grid.locate"):
                    address = sdk.locate(
                        grid_type=grid_type,
                        requested_grid_level=requested_grid_level,
                        point=[observation.lon, observation.lat],
                    )
                with timing.phase("grid.geometry"):
                    geometry = sdk.code_to_geometry(address=address)
                cell_key = (address.space_code, int(address.grid_level), address.topology_code)
                _record_asset_cell(asset_cell_keys, cell_key, int(value["max_cells_per_asset"]))
                timing.add_counter("grid_cell_count")
                cell_identity = OutputIdentity(
                    dataset_id=dataset["dataset_id"], output_version=value["output_version"], source_asset_id=asset["source_asset_id"],
                    band_code="_cell", grid_type=grid_type, grid_level=int(address.grid_level), space_code=address.space_code,
                    topology_code=address.topology_code, time_bucket="_", window_identity="cell",
                )
                cells.setdefault(
                    cell_key,
                    {
                        "output_id": make_output_id(cell_identity), "grid_type": grid_type, "grid_level": int(address.grid_level),
                        "space_code": address.space_code, "topology_code": address.topology_code,
                        "bbox": _geometry_bbox(geometry), "geometry": geometry,
                    },
                )
                source_index = ordinal if observation.source_index is None else int(observation.source_index)
                # Multiple observations can share a cell and time period.  The stable suffix preserves each raw observation.
                observation_bucket = f"{base_row['time_bucket']}-{source_index:09d}"
                for band in bands:
                    with timing.phase("carbon.measurement_partition"):
                        row = partition_observation(
                            observation,
                            config,
                            sdk=sdk,
                            measurement_name=str(band["band_code"]),
                        )
                    identity = OutputIdentity(
                        dataset_id=dataset["dataset_id"], output_version=value["output_version"], source_asset_id=asset["source_asset_id"],
                        band_code=band["band_code"], grid_type=grid_type, grid_level=int(address.grid_level), space_code=address.space_code,
                        topology_code=address.topology_code, time_bucket=observation_bucket,
                        window_identity=f"observation:{observation.observation_id}:{source_index}",
                    )
                    output_id = make_output_id(identity)
                    tiles.append(
                        {
                            "output_id": output_id, "source_asset_id": asset["source_asset_id"], "band_code": band["band_code"],
                            "grid_type": grid_type, "grid_level": int(address.grid_level), "space_code": address.space_code,
                            "topology_code": address.topology_code, "time_bucket": observation_bucket,
                            # A raw observation source is never a generated entity tile.
                            "tile_uri": source_uri, "tile_kind": "logical_reference",
                            "bbox": _geometry_bbox(geometry), "byte_size": source_size, "checksum": actual_source_checksum,
                        }
                    )
                    indexes.append(
                        {
                            "output_id": f"{output_id}-index", "tile_output_id": None,
                            "source_asset_id": asset["source_asset_id"], "band_code": band["band_code"],
                            "acquisition_time": row["acq_time"], "grid_type": grid_type, "grid_level": int(address.grid_level),
                            "space_code": address.space_code, "topology_code": address.topology_code, "time_bucket": observation_bucket,
                            "st_code": row["st_code"], "value_ref_uri": source_uri,
                            "window_col_off": None, "window_row_off": None, "window_width": None, "window_height": None,
                            "attributes": _carbon_index_attributes(row, source_index=source_index),
                        }
                    )
        return {
            "dataset_id": dataset["dataset_id"], "task_id": value["task_id"], "output_version": value["output_version"],
            "grid_type": grid_type, "requested_grid_level": requested_grid_level,
            "partition_method": "entity" if entity else "logical",
            "execution_engine": "ray",
            "object_prefix": f"partition/{dataset['dataset_id']}/versions/{value['output_version']}/",
            "tiles": tiles, "indexes": indexes, "grid_cells": list(cells.values()),
            "timings": {"worker": timing.finish()},
        }

    driver_timing = TimingRecorder("carbon_driver")
    driver_timing.set_attribute("ray_address", payload["ray_address"])
    driver_timing.set_attribute("grid_type", payload.get("grid_type"))
    if not ray.is_initialized():
        with driver_timing.phase("ray.init"):
            ray.init(
                address=payload["ray_address"],
                ignore_reinit_error=True,
                include_dashboard=False,
                logging_level=40,
                runtime_env=_ray_init_runtime_env(runtime_env),
            )
    else:
        driver_timing.add_counter("ray_init_reused")
    from cube_split.jobs.ray_logical_partition_job import _ray_actor_options_from_env

    with driver_timing.phase("ray.task_submit"):
        reference = execute.options(**_ray_actor_options_from_env()).remote(payload)
    with driver_timing.phase("ray.wait_get"):
        result = _wait_for_ray_result(ray, reference, cancellation_check)
    with driver_timing.phase("driver.result_merge"):
        existing_timings = result.get("timings") if isinstance(result.get("timings"), dict) else {}
    result["timings"] = {
        "driver": driver_timing.finish(),
        "workers": [existing_timings["worker"]] if isinstance(existing_timings.get("worker"), dict) else [],
    }
    return result


def _run_dataset_on_ray(
    payload: dict[str, Any],
    runtime_env: dict[str, Any] | None,
    cancellation_check: Callable[[], bool] | None = None,
    *,
    _executor_only: bool = False,
) -> Any:
    """Execute one normalized dataset on a configured Ray worker."""
    import ray

    @ray.remote
    def execute(value: dict[str, Any]) -> dict[str, Any]:
        import socket
        from concurrent.futures import ThreadPoolExecutor
        from pathlib import Path
        from time import perf_counter

        import rasterio
        import rasterio.mask
        from cube_split import runtime_config as worker_runtime_config
        from cube_split.jobs.ray_partition_core import cache_source_cog
        from grid_core.app.core.enums import BoundaryType
        from grid_core.app.models.grid_address import GridAddress
        from grid_core.app.models.grid_cell import GridCell
        from grid_core.sdk import CubeEncoderSDK
        from minio import Minio
        from rasterio.errors import WindowError
        from rasterio.io import MemoryFile
        from rasterio.warp import transform_bounds, transform_geom
        from rasterio.windows import Window, from_bounds, intersection

        minio_settings = worker_runtime_config.minio_settings()
        settings = {"endpoint": minio_settings.endpoint, "access_key": minio_settings.access_key,
                    "secret_key": minio_settings.secret_key, "bucket": minio_settings.bucket, "secure": minio_settings.secure}
        client = Minio(
            settings["endpoint"], access_key=settings["access_key"], secret_key=settings["secret_key"], secure=settings["secure"]
        )

        try:
            minio_parallel_uploads = int(
                value.get("entity_minio_parallel_uploads")
                or worker_runtime_config.env_text("CUBE_ENTITY_MINIO_PARALLEL_UPLOADS", "3")
            )
        except (TypeError, ValueError):
            minio_parallel_uploads = 3
        minio_parallel_uploads = max(1, min(minio_parallel_uploads, 8))

        def persist_entity_tile(item: tuple[bytes, str, str]) -> tuple[str, float, float]:
            tile_bytes, checksum, object_key = item
            stat_started = perf_counter()
            try:
                existing = client.stat_object(settings["bucket"], object_key)
            except Exception as exc:
                if getattr(exc, "code", None) not in {"NoSuchKey", "NoSuchObject", "ResourceNotFound"}:
                    raise
                existing = None
            stat_elapsed = perf_counter() - stat_started
            upload_elapsed = 0.0
            if existing is None:
                upload_started = perf_counter()
                client.put_object(
                    settings["bucket"], object_key, BytesIO(tile_bytes), len(tile_bytes), content_type="image/tiff",
                    metadata={"checksum-sha256": checksum},
                    num_parallel_uploads=minio_parallel_uploads,
                )
                upload_elapsed = perf_counter() - upload_started
            else:
                metadata = {str(name).lower(): str(item) for name, item in (getattr(existing, "metadata", {}) or {}).items()}
                existing_checksum = metadata.get("checksum-sha256") or metadata.get("x-amz-meta-checksum-sha256")
                if getattr(existing, "size", None) != len(tile_bytes) or existing_checksum != checksum:
                    raise ValueError(f"immutable entity tile collision for {object_key}")
            return f"s3://{settings['bucket']}/{object_key}", stat_elapsed, upload_elapsed
        dataset = value["dataset"]
        grid_type = value["grid_type"]
        requested_grid_level = int(value["requested_grid_level"])
        entity = grid_type == "isea4h"
        timing = TimingRecorder("raster_worker")
        timing.set_attribute("worker_hostname", socket.gethostname())
        timing.set_attribute("grid_type", grid_type)
        timing.set_attribute("requested_grid_level", requested_grid_level)
        timing.set_attribute("entity_output", entity)
        sdk = CubeEncoderSDK()
        cells: dict[tuple[str, int, str | None], dict[str, Any]] = {}
        tiles: list[dict[str, Any]] = []
        indexes: list[dict[str, Any]] = []
        for asset in dataset["assets"]:
            source_uri = str(asset["cog_uri"])
            parsed = urlparse(source_uri)
            if parsed.scheme != "s3" or not parsed.netloc:
                raise ValueError("normalized source COG must use an accessible MinIO bucket")
            cache_metrics: dict[str, Any] = {}
            with timing.phase("source.cache"):
                local_path = cache_source_cog(
                    source_uri,
                    Path("/tmp/cube_split_source_cache") / "loader",
                    client,
                    parsed.netloc,
                    metrics=cache_metrics,
                )
            timing.add_phase("source.download", float(cache_metrics.get("download_elapsed_sec") or 0.0))
            timing.add_counter("source_cache_hit_count" if cache_metrics.get("cache_hit") else "source_cache_miss_count")
            with timing.phase("source.checksum"):
                actual_source_checksum, _, checksum_cache_hit = _sha256_file_cached(local_path)
            timing.add_counter("source_checksum_cache_hit_count" if checksum_cache_hit else "source_checksum_cache_miss_count")
            if actual_source_checksum != asset["checksum"]:
                raise ValueError("source COG checksum does not match the strict loader contract")
            open_started = perf_counter()
            source = rasterio.open(local_path)
            timing.add_phase("source.raster_open", perf_counter() - open_started)
            with source:
                bounds = source.bounds
                if source.crs and str(source.crs).upper() != "EPSG:4326":
                    source_bbox = _normalize_wgs84_bbox(transform_bounds(
                        source.crs, "EPSG:4326", bounds.left, bounds.bottom, bounds.right, bounds.top,
                    ))
                else:
                    source_bbox = _normalize_wgs84_bbox([
                        bounds.left, bounds.bottom, bounds.right, bounds.top,
                    ])
                planning_bbox = asset.get("planning_bbox")
                if planning_bbox is not None:
                    planned = _normalize_wgs84_bbox(planning_bbox)
                    source_bbox = [
                        max(source_bbox[0], planned[0]),
                        max(source_bbox[1], planned[1]),
                        min(source_bbox[2], planned[2]),
                        min(source_bbox[3], planned[3]),
                    ]
                    if source_bbox[0] >= source_bbox[2] or source_bbox[1] >= source_bbox[3]:
                        continue
                precomputed_cells = value.get("entity_cover_cells") if entity else None
                if precomputed_cells is not None:
                    covered = [GridCell.model_validate(item) for item in precomputed_cells]
                    timing.add_counter("grid_cover_reused_count")
                else:
                    try:
                        with timing.phase("grid.cover"):
                            covered = sdk.cover(
                                grid_type=grid_type,
                                requested_grid_level=requested_grid_level,
                                cover_mode=value["cover_mode"],
                                boundary_type=BoundaryType.BBOX,
                                bbox=source_bbox,
                                crs="EPSG:4326",
                            )
                    except Exception as exc:
                        # Some SDK exception classes cannot be deserialized by Ray.
                        raise RuntimeError(f"grid cover failed: {exc}") from None
                asset_bands = [
                    (band, _source_band_index(band, source.count))
                    for band in dataset["bands"]
                    if band["source_asset_id"] == asset["source_asset_id"]
                ]
                bucket = _time_bucket(asset["time_start"], value["time_granularity"])
                for cell in covered:
                    timing.add_counter("grid_cell_count")
                    if source.crs and str(source.crs).upper() != "EPSG:4326":
                        cell_bounds = transform_bounds("EPSG:4326", source.crs, *cell.bbox)
                    else:
                        cell_bounds = tuple(cell.bbox)
                    try:
                        with timing.phase("grid.window_calculation"):
                            window = intersection(
                                from_bounds(*cell_bounds, transform=source.transform).round_offsets().round_lengths(),
                                Window(0, 0, source.width, source.height),
                            ).round_offsets().round_lengths()
                    except WindowError:
                        continue
                    if window.width <= 0 or window.height <= 0:
                        continue
                    cell_key = (cell.space_code, int(cell.grid_level), cell.topology_code)
                    cell_identity = OutputIdentity(
                        dataset_id=dataset["dataset_id"], output_version=value["output_version"], source_asset_id=asset["source_asset_id"],
                        band_code="_cell", grid_type=grid_type, grid_level=int(cell.grid_level), space_code=cell.space_code,
                        topology_code=cell.topology_code, time_bucket="_", window_identity="cell",
                    )
                    address = GridAddress(
                        grid_type=grid_type,
                        grid_level=int(cell.grid_level),
                        space_code=cell.space_code,
                        topology_code=cell.topology_code,
                    )
                    with timing.phase("grid.geometry"):
                        cell_geometry = cell.geometry or sdk.code_to_geometry(address=address)
                    cells.setdefault(
                        cell_key,
                        {"output_id": make_output_id(cell_identity), "grid_type": grid_type, "grid_level": int(cell.grid_level),
                         "space_code": cell.space_code, "topology_code": cell.topology_code, "bbox": cell.bbox, "geometry": cell_geometry},
                    )
                    band_records: list[tuple[dict[str, Any], int, dict[str, Any], dict[str, Any]]] = []
                    st_code = sdk.generate_st_code(
                        address=address, timestamp=datetime.fromisoformat(asset["time_start"].replace("Z", "+00:00")),
                        time_granularity=value["time_granularity"],
                    ).st_code
                    for band, source_band_index in asset_bands:
                        identity = OutputIdentity(
                            dataset_id=dataset["dataset_id"], output_version=value["output_version"], source_asset_id=asset["source_asset_id"],
                            band_code=band["band_code"], grid_type=grid_type, grid_level=int(cell.grid_level), space_code=cell.space_code,
                            topology_code=cell.topology_code, time_bucket=bucket, window_identity="entity" if entity else "logical-window",
                        )
                        output_id = make_output_id(identity)
                        tile_uri = source_uri
                        tile: dict[str, Any] = {
                            "output_id": output_id, "source_asset_id": asset["source_asset_id"], "band_code": band["band_code"],
                            "grid_type": grid_type, "grid_level": int(cell.grid_level), "space_code": cell.space_code,
                            "topology_code": cell.topology_code, "time_bucket": bucket, "tile_kind": "entity_file" if entity else "logical_reference",
                            "bbox": cell.bbox,
                        }
                        index: dict[str, Any] = {
                            "output_id": output_id + "-index", "tile_output_id": output_id if entity else None,
                            "source_asset_id": asset["source_asset_id"], "band_code": band["band_code"], "grid_type": grid_type,
                            "grid_level": int(cell.grid_level), "space_code": cell.space_code, "topology_code": cell.topology_code,
                            "time_bucket": bucket, "value_ref_uri": source_uri,
                            "attributes": {
                                "band_unit_id": (band.get("attributes") or {}).get("band_unit_id"),
                                "source_band_index": source_band_index,
                            },
                        }
                        index["st_code"] = st_code
                        band_records.append((band, source_band_index, tile, index))

                    if entity:
                        geometry = cell_geometry
                        if source.crs and str(source.crs).upper() != "EPSG:4326":
                            geometry = transform_geom("EPSG:4326", source.crs, geometry)
                        with timing.phase("entity.tile_mask"):
                            data, tile_transform = rasterio.mask.mask(
                                source,
                                [geometry],
                                crop=True,
                                indexes=[source_band_index for _, source_band_index in asset_bands],
                            )
                        pending_tile_uploads: list[tuple[dict[str, Any], dict[str, Any], bytes, str, str, int, int]] = []
                        for band_position, (band, _source_band_index_value, tile, index) in enumerate(band_records):
                            band_data = data[band_position:band_position + 1]
                            with timing.phase("entity.tile_write"):
                                profile = source.profile.copy()
                                profile.update(
                                    driver="GTiff", count=1, width=band_data.shape[2], height=band_data.shape[1], transform=tile_transform
                                )
                                with MemoryFile() as memory:
                                    with memory.open(**profile) as destination:
                                        destination.write(band_data)
                                    tile_bytes = memory.read()
                            with timing.phase("entity.tile_checksum"):
                                checksum = sha256(tile_bytes).hexdigest()
                            object_key = f"partition/{dataset['dataset_id']}/versions/{value['output_version']}/tiles/{tile['output_id']}.tif"
                            pending_tile_uploads.append(
                                (tile, index, tile_bytes, checksum, object_key, band_data.shape[2], band_data.shape[1])
                            )
                        upload_workers = min(
                            max(1, int(value.get("entity_upload_workers") or 1)),
                            len(pending_tile_uploads),
                        )
                        upload_items = [
                            (tile_bytes, checksum, object_key)
                            for _tile, _index, tile_bytes, checksum, object_key, _width, _height in pending_tile_uploads
                        ]
                        if upload_workers == 1:
                            persisted = [persist_entity_tile(item) for item in upload_items]
                        else:
                            with ThreadPoolExecutor(max_workers=upload_workers, thread_name_prefix="cube-entity-tile") as pool:
                                futures = [pool.submit(persist_entity_tile, item) for item in upload_items]
                                persisted = [future.result() for future in futures]
                        for item, result in zip(pending_tile_uploads, persisted, strict=True):
                            tile, index, tile_bytes, checksum, object_key, width, height = item
                            tile_uri, stat_elapsed, upload_elapsed = result
                            timing.add_phase("minio.tile_stat", stat_elapsed)
                            if upload_elapsed:
                                timing.add_phase("minio.tile_upload", upload_elapsed)
                            tile.update({"tile_uri": tile_uri, "checksum": checksum, "byte_size": len(tile_bytes), "width": width, "height": height})
                            index.update({"value_ref_uri": tile_uri, "window_col_off": None, "window_row_off": None, "window_width": None, "window_height": None})
                    else:
                        for band, _source_band_index_value, tile, index in band_records:
                            tile["tile_uri"] = source_uri
                            index.update({"window_col_off": int(window.col_off), "window_row_off": int(window.row_off), "window_width": int(window.width), "window_height": int(window.height)})
                    for _band, _source_band_index_value, tile, index in band_records:
                        tiles.append(tile)
                        indexes.append(index)
                        timing.add_counter("tile_count")
                        timing.add_counter("index_count")
        return {
            "dataset_id": dataset["dataset_id"], "task_id": value["task_id"], "output_version": value["output_version"],
            "grid_type": grid_type, "requested_grid_level": requested_grid_level,
            "partition_method": "entity" if entity else "logical",
            "execution_engine": "ray",
            "object_prefix": f"partition/{dataset['dataset_id']}/versions/{value['output_version']}/",
            "tiles": tiles, "indexes": indexes, "grid_cells": list(cells.values()),
            "timings": {"worker": timing.finish()},
        }

    if _executor_only:
        return execute

    driver_timing = TimingRecorder("raster_driver")
    driver_timing.set_attribute("ray_address", payload["ray_address"])
    driver_timing.set_attribute("grid_type", payload.get("grid_type"))
    with _RAY_INIT_LOCK:
        if not ray.is_initialized():
            with driver_timing.phase("ray.init"):
                ray.init(
                    address=payload["ray_address"],
                    ignore_reinit_error=True,
                    include_dashboard=False,
                    logging_level=40,
                    runtime_env=_ray_init_runtime_env(runtime_env),
                )
        else:
            driver_timing.add_counter("ray_init_reused")
    from cube_split.jobs.ray_logical_partition_job import _ray_actor_options_from_env

    assets = list((payload.get("dataset") or {}).get("assets") or [])
    ray_parallelism = _entity_ray_parallelism()
    driver_timing.set_attribute("ray_parallelism", ray_parallelism)
    if not assets:
        with driver_timing.phase("ray.task_submit"):
            reference = execute.options(**_ray_actor_options_from_env()).remote(payload)
        with driver_timing.phase("ray.wait_get"):
            result = _wait_for_ray_result(ray, reference, cancellation_check)
        with driver_timing.phase("driver.result_merge"):
            existing_timings = result.get("timings") if isinstance(result.get("timings"), dict) else {}
        result["timings"] = {
            "driver": driver_timing.finish(),
            "workers": [existing_timings["worker"]] if isinstance(existing_timings.get("worker"), dict) else [],
        }
        return result
    options = {"num_cpus": 1, **_ray_actor_options_from_env()}
    pending = []
    for asset in assets:
        asset_bbox = asset.get("bbox")
        if not isinstance(asset_bbox, (list, tuple)) or len(asset_bbox) != 4:
            raise ValueError("entity source asset bbox is required for parallel planning")
        cover_cells = None
        if payload["grid_type"] == "isea4h":
            cover_cells = _entity_cover_cells(
                grid_type=payload["grid_type"],
                requested_grid_level=int(payload["requested_grid_level"]),
                cover_mode=str(payload["cover_mode"]),
                asset_bbox=asset_bbox,
                timing=driver_timing,
            )
            driver_timing.add_counter("grid_cover_cell_count", len(cover_cells))
        shards = _entity_planning_shards(asset_bbox, ray_parallelism)
        assigned_cells = _entity_cells_by_shard(cover_cells, shards)
        for shard, shard_cells in zip(shards, assigned_cells, strict=True):
            if cover_cells is not None and not shard_cells:
                driver_timing.add_counter("entity_empty_shard_count")
                continue
            task_asset = {**asset, "planning_bbox": shard}
            task_dataset = {**payload["dataset"], "assets": [task_asset]}
            task_payload = {**payload, "dataset": task_dataset}
            if shard_cells is not None:
                task_payload["entity_cover_cells"] = shard_cells
            with driver_timing.phase("ray.task_submit"):
                pending.append(execute.options(**options).remote(task_payload))
            driver_timing.add_counter("entity_cell_task_count", len(shard_cells))
            driver_timing.add_counter("ray_task_submit_count")

    merged: dict[str, dict[str, Any]] = {"tiles": {}, "indexes": {}, "grid_cells": {}}
    worker_timings: list[dict[str, Any]] = []
    while pending:
        if cancellation_check is not None and cancellation_check():
            for ref in pending:
                ray.cancel(ref, force=True)
            from cube_split.jobs.cancellation import PartitionCancelledError
            raise PartitionCancelledError("Partition task cancelled")
        with driver_timing.phase("ray.wait"):
            ready, pending = ray.wait(pending, num_returns=1, timeout=1.0)
        for ref in ready:
            with driver_timing.phase("ray.result_get"):
                result = ray.get(ref)
            result_timings = result.get("timings") if isinstance(result.get("timings"), dict) else {}
            if isinstance(result_timings.get("worker"), dict):
                worker_timings.append(result_timings["worker"])
            with driver_timing.phase("driver.result_merge"):
                for kind in merged:
                    for row in result.get(kind, []):
                        merged[kind].setdefault(str(row["output_id"]), row)
    return {
        "dataset_id": payload["dataset"]["dataset_id"], "task_id": payload["task_id"],
        "output_version": payload["output_version"], "grid_type": payload["grid_type"],
        "requested_grid_level": int(payload["requested_grid_level"]), "partition_method": "entity",
        "execution_engine": "ray", "ray_parallelism": ray_parallelism,
        "object_prefix": f"partition/{payload['dataset']['dataset_id']}/versions/{payload['output_version']}/",
        **{kind: list(rows.values()) for kind, rows in merged.items()},
        "timings": {"driver": driver_timing.finish(), "workers": worker_timings},
    }


def _entity_payload_group_key(payload: dict[str, Any]) -> tuple[Any, ...]:
    """Return the execution identity shared by compatible entity band units."""
    dataset = payload.get("dataset") or {}
    asset_ids = tuple(sorted(str(asset.get("source_asset_id")) for asset in dataset.get("assets") or []))
    return (
        str(dataset.get("dataset_id") or ""),
        str(payload.get("task_id") or ""),
        str(payload.get("output_version") or ""),
        str(payload.get("grid_type") or ""),
        int(payload.get("requested_grid_level") or 0),
        str(payload.get("cover_mode") or ""),
        int(payload.get("max_cells_per_asset") or 0),
        str(payload.get("time_granularity") or ""),
        payload.get("max_observations"),
        str(payload.get("ray_address") or ""),
        asset_ids,
    )


def _entity_bands_per_task(default: int = 1) -> int:
    """Limit same-source bands per task so grouping does not erase parallelism."""
    try:
        value = int(runtime_config.env_text("CUBE_ENTITY_BANDS_PER_TASK", str(default)))
    except (TypeError, ValueError):
        value = default
    return max(1, min(value, 32))


def _entity_upload_workers(default: int = 4) -> int:
    """Return the bounded per-task MinIO I/O concurrency."""
    try:
        value = int(runtime_config.env_text("CUBE_ENTITY_UPLOAD_WORKERS", str(default)))
    except (TypeError, ValueError):
        value = default
    return max(1, min(value, 16))


def _entity_minio_parallel_uploads(default: int = 3) -> int:
    """Return the bounded multipart connection count for each MinIO put."""
    try:
        value = int(runtime_config.env_text("CUBE_ENTITY_MINIO_PARALLEL_UPLOADS", str(default)))
    except (TypeError, ValueError):
        value = default
    return max(1, min(value, 8))


def _group_entity_payloads(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Combine same-source band payloads while retaining original unit indexes."""
    groups: list[dict[str, Any]] = []
    by_key: dict[tuple[Any, ...], dict[str, Any]] = {}
    for unit_index, payload in enumerate(payloads):
        key = _entity_payload_group_key(payload)
        group = by_key.get(key)
        if group is None:
            group = {
                "payload": dict(payload),
                "payload_indexes": [],
                "assets": {},
                "bands": [],
                "unit_band_counts": [],
            }
            by_key[key] = group
            groups.append(group)
        group["payload_indexes"].append(unit_index)
        dataset = payload.get("dataset") or {}
        for asset in dataset.get("assets") or []:
            group["assets"].setdefault(str(asset["source_asset_id"]), asset)
        group["unit_band_counts"].append(len(dataset.get("bands") or []))
        seen_bands = {
            (str(item.get("source_asset_id")), str(item.get("band_code")))
            for item in group["bands"]
        }
        for band in dataset.get("bands") or []:
            band_key = (str(band.get("source_asset_id")), str(band.get("band_code")))
            if band_key not in seen_bands:
                group["bands"].append(band)
                seen_bands.add(band_key)

    band_limit = _entity_bands_per_task()
    split_groups: list[dict[str, Any]] = []
    for group in groups:
        payload_indexes = list(group["payload_indexes"])
        bands = list(group["bands"])
        if any(count != 1 for count in group["unit_band_counts"]):
            chunks = [(payload_indexes, bands)]
        else:
            chunks = [
                (payload_indexes[start:start + band_limit], bands[start:start + band_limit])
                for start in range(0, len(payload_indexes), band_limit)
            ]
        for chunk_indexes, chunk_bands in chunks:
            chunk = dict(group)
            payload = dict(group["payload"])
            dataset = dict(payload["dataset"])
            selected_asset_ids = {str(band["source_asset_id"]) for band in chunk_bands}
            dataset["assets"] = [
                asset for asset_id, asset in group["assets"].items()
                if asset_id in selected_asset_ids
            ]
            dataset["bands"] = chunk_bands
            payload["dataset"] = dataset
            payload["batch_group_unit_count"] = len(chunk_indexes)
            chunk["payload"] = payload
            chunk["payload_indexes"] = chunk_indexes
            chunk["bands"] = chunk_bands
            split_groups.append(chunk)
    return split_groups


def _entity_band_keys(payload: dict[str, Any]) -> set[tuple[str, str]]:
    dataset = payload.get("dataset") or {}
    return {
        (str(band.get("source_asset_id")), str(band.get("band_code")))
        for band in dataset.get("bands") or []
    }


def _run_entity_dataset_batch_on_ray(
    payloads: list[dict[str, Any]],
    runtime_env: dict[str, Any] | None,
    cancellation_check: Callable[[], bool] | None = None,
) -> list[dict[str, Any]]:
    """Run entity units through one Ray driver and one shared task queue.

    Compatible band units sharing one source asset are grouped into one spatial
    task set. The worker reads and masks all bands together, while the result is
    split back to the original unit order for retry and scene accounting.
    """
    if not payloads:
        return []
    import ray

    batch_timing = TimingRecorder("raster_batch_driver")
    batch_timing.set_attribute("ray_address", payloads[0]["ray_address"])
    groups = _group_entity_payloads(payloads)
    batch_timing.set_attribute("entity_unit_count", len(payloads))
    batch_timing.set_attribute("entity_group_count", len(groups))
    batch_timing.set_attribute("entity_bands_per_task", _entity_bands_per_task())
    batch_timing.set_attribute("entity_upload_workers", _entity_upload_workers())
    batch_timing.set_attribute("entity_minio_parallel_uploads", _entity_minio_parallel_uploads())
    ray_parallelism = _entity_ray_parallelism()
    batch_timing.set_attribute("ray_parallelism", ray_parallelism)
    unit_timings = [TimingRecorder("raster_driver") for _ in payloads]
    for index, (timing, payload) in enumerate(zip(unit_timings, payloads, strict=True)):
        timing.set_attribute("ray_address", payload["ray_address"])
        timing.set_attribute("grid_type", payload.get("grid_type"))
        timing.set_attribute("ray_parallelism", ray_parallelism)
        timing.set_attribute("batch_unit_index", index)
        timing.set_attribute("batch_unit_count", len(payloads))

    with _RAY_INIT_LOCK:
        if not ray.is_initialized():
            with batch_timing.phase("ray.init"):
                ray.init(
                    address=payloads[0]["ray_address"],
                    ignore_reinit_error=True,
                    include_dashboard=False,
                    logging_level=40,
                    runtime_env=_ray_init_runtime_env(runtime_env),
                )
        else:
            batch_timing.add_counter("ray_init_reused")

    execute = _run_dataset_on_ray({}, runtime_env, _executor_only=True)
    from cube_split.jobs.ray_logical_partition_job import _ray_actor_options_from_env

    options = {"num_cpus": 1, **_ray_actor_options_from_env()}
    pending: list[Any] = []
    ref_to_group: dict[int, int] = {}
    unit_to_group: dict[int, int] = {
        unit_index: group_index
        for group_index, group in enumerate(groups)
        for unit_index in group["payload_indexes"]
    }
    group_merged: list[dict[str, dict[str, Any]]] = [
        {"tiles": {}, "indexes": {}, "grid_cells": {}}
        for _ in groups
    ]
    group_errors: list[str | None] = [None] * len(groups)
    group_worker_timings: list[list[dict[str, Any]]] = [[] for _ in groups]

    for group_index, group in enumerate(groups):
        payload = group["payload"]
        unit_index = group["payload_indexes"][0]
        driver_timing = unit_timings[unit_index]
        assets = list((payload.get("dataset") or {}).get("assets") or [])
        for asset in assets:
            asset_bbox = asset.get("bbox")
            if not isinstance(asset_bbox, (list, tuple)) or len(asset_bbox) != 4:
                raise ValueError("entity source asset bbox is required for parallel planning")
            cover_cells = _entity_cover_cells(
                grid_type=payload["grid_type"],
                requested_grid_level=int(payload["requested_grid_level"]),
                cover_mode=str(payload["cover_mode"]),
                asset_bbox=asset_bbox,
                timing=driver_timing,
            )
            driver_timing.add_counter("grid_cover_cell_count", len(cover_cells))
            shards = _entity_planning_shards(asset_bbox, ray_parallelism)
            for shard, shard_cells in zip(shards, _entity_cells_by_shard(cover_cells, shards), strict=True):
                if not shard_cells:
                    driver_timing.add_counter("entity_empty_shard_count")
                    continue
                task_asset = {**asset, "planning_bbox": shard}
                task_dataset = {**payload["dataset"], "assets": [task_asset]}
                task_payload = {**payload, "dataset": task_dataset, "batch_group_index": group_index}
                task_payload["entity_cover_cells"] = shard_cells
                task_payload["entity_upload_workers"] = _entity_upload_workers()
                task_payload["entity_minio_parallel_uploads"] = _entity_minio_parallel_uploads()
                with batch_timing.phase("ray.task_submit"):
                    reference = execute.options(**options).remote(task_payload)
                pending.append(reference)
                ref_to_group[id(reference)] = group_index
                driver_timing.add_counter("entity_cell_task_count", len(shard_cells))
                driver_timing.add_counter("ray_task_submit_count")
                batch_timing.add_counter("ray_task_submit_count")

    while pending:
        if cancellation_check is not None and cancellation_check():
            for reference in pending:
                ray.cancel(reference, force=True)
            from cube_split.jobs.cancellation import PartitionCancelledError
            raise PartitionCancelledError("Partition task cancelled")
        with batch_timing.phase("ray.wait"):
            ready, pending = _wait_for_ray_batch(ray, pending, ray_parallelism)
        batch_timing.add_counter("ray_wait_batch_count")
        batch_timing.add_counter("ray_ready_ref_count", len(ready))
        ready_results: list[tuple[Any, dict[str, Any] | None, Exception | None]] = []
        if ready:
            with batch_timing.phase("ray.result_get"):
                try:
                    ready_results = [
                        (reference, result, None)
                        for reference, result in zip(ready, ray.get(ready), strict=True)
                    ]
                except Exception:
                    for reference in ready:
                        try:
                            ready_results.append((reference, ray.get(reference), None))
                        except Exception as exc:
                            ready_results.append((reference, None, exc))
        for reference, result, result_error in ready_results:
            group_index = ref_to_group.pop(id(reference))
            if result_error is not None:
                group_errors[group_index] = str(result_error)
                continue
            result_timings = result.get("timings") if isinstance(result.get("timings"), dict) else {}
            if isinstance(result_timings.get("worker"), dict):
                group_worker_timings[group_index].append(result_timings["worker"])
            with batch_timing.phase("driver.result_merge"):
                for kind in group_merged[group_index]:
                    for row in result.get(kind, []):
                        group_merged[group_index][kind].setdefault(str(row["output_id"]), row)

    batch_record = batch_timing.finish()
    outcomes: list[dict[str, Any]] = []
    for unit_index, payload in enumerate(payloads):
        group_index = unit_to_group[unit_index]
        group = groups[group_index]
        if group_errors[group_index] is not None:
            outcomes.append({"error": group_errors[group_index]})
            continue
        unit_rows = {"tiles": {}, "indexes": {}, "grid_cells": {}}
        band_keys = _entity_band_keys(payload)
        for kind in ("tiles", "indexes"):
            for output_id, row in group_merged[group_index][kind].items():
                row_key = (str(row.get("source_asset_id")), str(row.get("band_code")))
                if row_key in band_keys:
                    unit_rows[kind][output_id] = row
        if unit_index == group["payload_indexes"][0]:
            unit_rows["grid_cells"] = dict(group_merged[group_index]["grid_cells"])
        first_unit_index = group["payload_indexes"][0]
        result = {
            "dataset_id": payload["dataset"]["dataset_id"],
            "task_id": payload["task_id"],
            "output_version": payload["output_version"],
            "grid_type": payload["grid_type"],
            "requested_grid_level": int(payload["requested_grid_level"]),
            "partition_method": "entity",
            "execution_engine": "ray",
            "ray_parallelism": ray_parallelism,
            "object_prefix": f"partition/{payload['dataset']['dataset_id']}/versions/{payload['output_version']}/",
            **{kind: list(rows.values()) for kind, rows in unit_rows.items()},
            "timings": {
                "driver": unit_timings[unit_index].finish(),
                "workers": group_worker_timings[group_index] if unit_index == first_unit_index else [],
            },
        }
        if unit_index == first_unit_index:
            result["timings"]["batch_driver"] = batch_record
        outcomes.append({"result": result})
    return outcomes


class NormalizedPartitionDatasetRunner:
    """Production runner used by normalized partition runs."""

    @staticmethod
    def _verify_assets_exist(payloads: list[dict[str, Any]]) -> None:
        """Preflight every loader-owned source object so missing sources fail fast."""
        from urllib.parse import unquote

        from minio import Minio

        settings = runtime_config.minio_settings()
        if not all((settings.endpoint, settings.access_key, settings.secret_key)):
            return
        client = Minio(settings.endpoint, access_key=settings.access_key, secret_key=settings.secret_key, secure=settings.secure)
        failures: list[str] = []
        checked_objects: set[tuple[str, str]] = set()
        for payload in payloads:
            for asset in payload.get("dataset", {}).get("assets") or []:
                uri = str(asset.get("cog_uri") or asset.get("source_uri") or "")
                parsed = urlparse(uri)
                if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
                    failures.append(f"invalid_uri:{uri}")
                    continue
                key = unquote(parsed.path).lstrip("/")
                object_identity = (parsed.netloc, key)
                if object_identity in checked_objects:
                    continue
                checked_objects.add(object_identity)
                try:
                    stat = client.stat_object(parsed.netloc, key)
                except Exception as exc:
                    code = str(getattr(exc, "code", "") or "")
                    if code in {"NoSuchKey", "NoSuchObject", "ResourceNotFound"}:
                        failures.append(f"source_missing:{uri}")
                    else:
                        raise
                else:
                    if int(getattr(stat, "size", 0) or 0) <= 0:
                        failures.append(f"source_empty:{uri}")
        if failures:
            raise SourceObjectMissingError("source preflight failed: " + ", ".join(failures))

    def _ray_execution_context(self) -> tuple[str, dict[str, Any]]:
        from cube_split.jobs.ray_logical_partition_job import _ray_runtime_env_from_env

        ray_address = runtime_config.require_ray_address()
        if ray_address.startswith("ray://"):
            # Ray Client workers load their protected node-local runtime settings.
            return ray_address, {"env_vars": {}}
        minio = runtime_config.minio_settings()
        ray_runtime_env = _ray_runtime_env_from_env() or {"env_vars": {}}
        env_vars = dict(ray_runtime_env.get("env_vars") or {})
        env_vars.update({
            "CUBE_WEB_POSTGRES_DSN": runtime_config.require_postgres_dsn(),
            "CUBE_WEB_MINIO_ENDPOINT": minio.endpoint,
            "CUBE_WEB_MINIO_ACCESS_KEY": minio.access_key,
            "CUBE_WEB_MINIO_SECRET_KEY": minio.secret_key,
            "CUBE_WEB_MINIO_BUCKET": minio.bucket,
        })
        ray_runtime_env["env_vars"] = env_vars
        return ray_address, ray_runtime_env

    @staticmethod
    def _payload(
        *, dataset: Any, task_id: str, output_version: str, grid_type: str,
        requested_grid_level: int, cover_mode: str, max_cells_per_asset: int,
        time_granularity: str, max_observations: int | None, ray_address: str,
    ) -> dict[str, Any]:
        return {
            "dataset": dataset.model_dump(mode="json"), "task_id": task_id, "output_version": output_version,
            "grid_type": grid_type, "requested_grid_level": requested_grid_level, "cover_mode": cover_mode,
            "time_granularity": time_granularity, "max_cells_per_asset": max_cells_per_asset,
            "max_observations": max_observations, "ray_address": ray_address,
        }

    @staticmethod
    def _run_payload(
        payload: dict[str, Any], runtime_env: dict[str, Any] | None,
        cancellation_check: Callable[[], bool] | None,
    ) -> dict[str, Any]:
        if payload["dataset"].get("data_type") == "carbon":
            return _run_carbon_dataset_on_ray(payload, runtime_env, cancellation_check)
        if payload["grid_type"] in {"geohash", "mgrs"}:
            from cube_split.jobs.ray_logical_chunk_job import run_logical_chunk_job

            return run_logical_chunk_job(payload, runtime_env, cancellation_check)
        return _run_dataset_on_ray(payload, runtime_env, cancellation_check)

    def run_datasets(
        self, *, runs: list[dict[str, Any]], cancellation_check: Callable[[], bool] | None = None,
    ) -> list[dict[str, Any]]:
        """Submit all logical scene units through one Ray queue for a batch."""
        if not runs:
            return []
        ray_address, runtime_env = self._ray_execution_context()
        payload_fields = {
            "dataset", "task_id", "output_version", "grid_type", "requested_grid_level",
            "cover_mode", "max_cells_per_asset", "time_granularity", "max_observations",
        }
        payloads = [
            self._payload(ray_address=ray_address, **{key: value for key, value in run.items() if key in payload_fields})
            for run in runs
        ]
        preflight_timing = TimingRecorder("source_preflight")
        with preflight_timing.phase("minio.stat"):
            self._verify_assets_exist(payloads)
        preflight_record = preflight_timing.finish()
        outcomes: list[dict[str, Any] | None] = [None] * len(payloads)
        logical_positions = [
            index for index, payload in enumerate(payloads)
            if payload["dataset"].get("data_type") != "carbon" and payload["grid_type"] in {"geohash", "mgrs"}
        ]
        if logical_positions:
            from cube_split.jobs.ray_logical_chunk_job import run_logical_chunk_jobs

            logical_outcomes = run_logical_chunk_jobs(
                [payloads[index] for index in logical_positions], runtime_env, cancellation_check,
            )
            for index, outcome in zip(logical_positions, logical_outcomes, strict=True):
                outcomes[index] = outcome
        entity_positions = [
            index for index, payload in enumerate(payloads)
            if outcomes[index] is None
            and payload["dataset"].get("data_type") != "carbon"
            and payload["grid_type"] == "isea4h"
        ]
        if entity_positions:
            entity_outcomes = _run_entity_dataset_batch_on_ray(
                [payloads[index] for index in entity_positions], runtime_env, cancellation_check,
            )
            for index, outcome in zip(entity_positions, entity_outcomes, strict=True):
                outcomes[index] = outcome
        for index, payload in enumerate(payloads):
            if outcomes[index] is not None:
                continue
            try:
                outcomes[index] = {"result": self._run_payload(payload, runtime_env, cancellation_check)}
            except Exception as exc:
                outcomes[index] = {"error": str(exc)}
        completed_outcomes = [outcome for outcome in outcomes if outcome is not None]
        for outcome in completed_outcomes:
            result = outcome.get("result") if isinstance(outcome, dict) else None
            if not isinstance(result, dict):
                continue
            timings = dict(result.get("timings") or {})
            timings["source_preflight"] = preflight_record
            result["timings"] = timings
        return completed_outcomes

    def run_dataset(
        self,
        *,
        dataset: Any,
        task_id: str,
        output_version: str,
        grid_type: str,
        requested_grid_level: int,
        cover_mode: str,
        max_cells_per_asset: int = 0,
        time_granularity: str = "day",
        max_observations: int | None = None,
        cancellation_check: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        ray_address, runtime_env = self._ray_execution_context()
        payload = self._payload(
            dataset=dataset,
            task_id=task_id,
            output_version=output_version,
            grid_type=grid_type,
            requested_grid_level=requested_grid_level,
            cover_mode=cover_mode,
            max_cells_per_asset=max_cells_per_asset,
            time_granularity=time_granularity,
            max_observations=max_observations,
            ray_address=ray_address,
        )
        preflight_timing = TimingRecorder("source_preflight")
        with preflight_timing.phase("minio.stat"):
            self._verify_assets_exist([payload])
        result = self._run_payload(payload, runtime_env, cancellation_check)
        timings = dict(result.get("timings") or {})
        timings["source_preflight"] = preflight_timing.finish()
        result["timings"] = timings
        return result


def _normalize_wgs84_bbox(bbox: list[float] | tuple[float, ...]) -> list[float]:
    """Clamp raster-derived WGS84 bounds to the legal geographic range."""
    if len(bbox) != 4:
        raise ValueError("WGS84 bbox must contain four coordinates")
    west, south, east, north = (float(value) for value in bbox)
    west = max(-180.0, min(180.0, west))
    east = max(-180.0, min(180.0, east))
    south = max(-90.0, min(90.0, south))
    north = max(-90.0, min(90.0, north))
    if south > north:
        south, north = north, south
    return [west, south, east, north]
