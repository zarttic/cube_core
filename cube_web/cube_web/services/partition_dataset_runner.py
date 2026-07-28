"""Ray execution adapter for the normalized production dataset contract."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from typing import Any, Callable
from urllib.parse import urlparse

from cube_split import runtime_config
from cube_split.jobs.logical_chunk_codec import compress_logical_chunk, logical_chunk_id, serialize_logical_chunk_rows

from cube_web.services.partition_contracts import OutputIdentity, make_output_id


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
            allowed_suffixes = {"netcdf": {".nc", ".nc4"}, "hdf5": {".h5", ".hdf", ".hdf5"}}
            if suffix not in allowed_suffixes.get(source_format, set()):
                raise ValueError(f"carbon source suffix {suffix!r} does not match source_format={source_format!r}")
            local_path = cache_source_cog(
                source_uri, Path("/tmp/cube_split_source_cache") / "loader", client, parsed.netloc
            )
            source_bytes = local_path.read_bytes()
            actual_source_checksum = sha256(source_bytes).hexdigest()
            if actual_source_checksum != asset["checksum"]:
                raise ValueError("carbon source checksum does not match the strict loader contract")
            observations = load_observations_from_file(
                local_path,
                max_observations=remaining_observations,
                product_type=product_type,
            )
            remaining_observations = _consume_observation_budget(remaining_observations, len(observations))
            source_size = len(source_bytes)
            bands = [band for band in dataset["bands"] if band["source_asset_id"] == asset["source_asset_id"]]
            asset_cell_keys: set[tuple[str, int, str | None]] = set()
            for ordinal, observation in enumerate(observations):
                row = partition_observation(observation, config, sdk=sdk)
                address = sdk.locate(
                    grid_type=grid_type,
                    requested_grid_level=requested_grid_level,
                    point=[observation.lon, observation.lat],
                )
                geometry = sdk.code_to_geometry(address=address)
                cell_key = (address.space_code, int(address.grid_level), address.topology_code)
                _record_asset_cell(asset_cell_keys, cell_key, int(value["max_cells_per_asset"]))
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
                observation_bucket = f"{row['time_bucket']}-{source_index:09d}"
                for band in bands:
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
        }

    if not ray.is_initialized():
        ray.init(
            address=payload["ray_address"],
            ignore_reinit_error=True,
            include_dashboard=False,
            logging_level=40,
            runtime_env=_ray_init_runtime_env(runtime_env),
        )
    from cube_split.jobs.ray_logical_partition_job import _ray_actor_options_from_env

    return _wait_for_ray_result(ray, execute.options(**_ray_actor_options_from_env()).remote(payload), cancellation_check)


def _run_dataset_on_ray(
    payload: dict[str, Any],
    runtime_env: dict[str, Any] | None,
    cancellation_check: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Execute one normalized dataset on a configured Ray worker."""
    import ray

    @ray.remote
    def execute(value: dict[str, Any]) -> dict[str, Any]:
        from pathlib import Path

        import rasterio
        import rasterio.mask
        from cube_split import runtime_config as worker_runtime_config
        from cube_split.jobs.ray_partition_core import cache_source_cog
        from grid_core.app.core.enums import BoundaryType
        from grid_core.app.models.grid_address import GridAddress
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
        dataset = value["dataset"]
        grid_type = value["grid_type"]
        requested_grid_level = int(value["requested_grid_level"])
        entity = grid_type == "isea4h"
        sdk = CubeEncoderSDK()
        cells: dict[tuple[str, int, str | None], dict[str, Any]] = {}
        tiles: list[dict[str, Any]] = []
        indexes: list[dict[str, Any]] = []
        for asset in dataset["assets"]:
            source_uri = str(asset["cog_uri"])
            parsed = urlparse(source_uri)
            if parsed.scheme != "s3" or not parsed.netloc:
                raise ValueError("normalized source COG must use an accessible MinIO bucket")
            local_path = cache_source_cog(
                source_uri, Path("/tmp/cube_split_source_cache") / "loader", client, parsed.netloc
            )
            actual_source_checksum = sha256(local_path.read_bytes()).hexdigest()
            if actual_source_checksum != asset["checksum"]:
                raise ValueError("source COG checksum does not match the strict loader contract")
            with rasterio.open(local_path) as source:
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
                try:
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
                for cell in covered:
                    if source.crs and str(source.crs).upper() != "EPSG:4326":
                        cell_bounds = transform_bounds("EPSG:4326", source.crs, *cell.bbox)
                    else:
                        cell_bounds = tuple(cell.bbox)
                    try:
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
                    cell_geometry = cell.geometry or sdk.code_to_geometry(address=address)
                    cells.setdefault(
                        cell_key,
                        {"output_id": make_output_id(cell_identity), "grid_type": grid_type, "grid_level": int(cell.grid_level),
                         "space_code": cell.space_code, "topology_code": cell.topology_code, "bbox": cell.bbox, "geometry": cell_geometry},
                    )
                    for band in (band for band in dataset["bands"] if band["source_asset_id"] == asset["source_asset_id"]):
                        source_band_index = _source_band_index(band, source.count)
                        bucket = _time_bucket(asset["time_start"], value["time_granularity"])
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
                        index["st_code"] = sdk.generate_st_code(
                            address=address, timestamp=datetime.fromisoformat(asset["time_start"].replace("Z", "+00:00")),
                            time_granularity=value["time_granularity"],
                        ).st_code
                        if entity:
                            geometry = cell_geometry
                            if source.crs and str(source.crs).upper() != "EPSG:4326":
                                geometry = transform_geom("EPSG:4326", source.crs, geometry)
                            data, tile_transform = rasterio.mask.mask(
                                source, [geometry], crop=True, indexes=[source_band_index]
                            )
                            profile = source.profile.copy()
                            profile.update(
                                driver="GTiff", count=1, width=data.shape[2], height=data.shape[1], transform=tile_transform
                            )
                            with MemoryFile() as memory:
                                with memory.open(**profile) as destination:
                                    destination.write(data)
                                tile_bytes = memory.read()
                            checksum = sha256(tile_bytes).hexdigest()
                            object_key = f"partition/{dataset['dataset_id']}/versions/{value['output_version']}/tiles/{output_id}.tif"
                            try:
                                existing = client.stat_object(settings["bucket"], object_key)
                            except Exception as exc:
                                if getattr(exc, "code", None) not in {"NoSuchKey", "NoSuchObject", "ResourceNotFound"}:
                                    raise
                                existing = None
                            if existing is None:
                                client.put_object(
                                    settings["bucket"], object_key, BytesIO(tile_bytes), len(tile_bytes), content_type="image/tiff",
                                    metadata={"checksum-sha256": checksum},
                                )
                            else:
                                metadata = {str(name).lower(): str(item) for name, item in (getattr(existing, "metadata", {}) or {}).items()}
                                existing_checksum = metadata.get("checksum-sha256") or metadata.get("x-amz-meta-checksum-sha256")
                                if getattr(existing, "size", None) != len(tile_bytes) or existing_checksum != checksum:
                                    raise ValueError(f"immutable entity tile collision for {object_key}")
                            tile_uri = f"s3://{settings['bucket']}/{object_key}"
                            tile.update({"tile_uri": tile_uri, "checksum": checksum, "byte_size": len(tile_bytes), "width": data.shape[2], "height": data.shape[1]})
                            index.update({"value_ref_uri": tile_uri, "window_col_off": None, "window_row_off": None, "window_width": None, "window_height": None})
                        else:
                            tile["tile_uri"] = tile_uri
                            index.update({"window_col_off": int(window.col_off), "window_row_off": int(window.row_off), "window_width": int(window.width), "window_height": int(window.height)})
                        tiles.append(tile)
                        indexes.append(index)
        return {
            "dataset_id": dataset["dataset_id"], "task_id": value["task_id"], "output_version": value["output_version"],
            "grid_type": grid_type, "requested_grid_level": requested_grid_level,
            "partition_method": "entity" if entity else "logical",
            "execution_engine": "ray",
            "object_prefix": f"partition/{dataset['dataset_id']}/versions/{value['output_version']}/",
            "tiles": tiles, "indexes": indexes, "grid_cells": list(cells.values()),
        }

    if not ray.is_initialized():
        ray.init(
            address=payload["ray_address"],
            ignore_reinit_error=True,
            include_dashboard=False,
            logging_level=40,
            runtime_env=_ray_init_runtime_env(runtime_env),
        )
    from cube_split.jobs.ray_logical_partition_job import _ray_actor_options_from_env

    assets = list((payload.get("dataset") or {}).get("assets") or [])
    ray_parallelism = _entity_ray_parallelism()
    if not assets:
        return _wait_for_ray_result(ray, execute.options(**_ray_actor_options_from_env()).remote(payload), cancellation_check)
    options = {"num_cpus": 1, **_ray_actor_options_from_env()}
    pending = []
    for asset in assets:
        asset_bbox = asset.get("bbox")
        if not isinstance(asset_bbox, (list, tuple)) or len(asset_bbox) != 4:
            raise ValueError("entity source asset bbox is required for parallel planning")
        for shard in _entity_planning_shards(asset_bbox, ray_parallelism):
            task_asset = {**asset, "planning_bbox": shard}
            task_dataset = {**payload["dataset"], "assets": [task_asset]}
            pending.append(execute.options(**options).remote({**payload, "dataset": task_dataset}))

    merged: dict[str, dict[str, Any]] = {"tiles": {}, "indexes": {}, "grid_cells": {}}
    while pending:
        if cancellation_check is not None and cancellation_check():
            for ref in pending:
                ray.cancel(ref, force=True)
            from cube_split.jobs.cancellation import PartitionCancelledError
            raise PartitionCancelledError("Partition task cancelled")
        ready, pending = ray.wait(pending, num_returns=1, timeout=1.0)
        for ref in ready:
            result = ray.get(ref)
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
    }


class NormalizedPartitionDatasetRunner:
    """Production runner used by normalized partition runs."""

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
        for index, payload in enumerate(payloads):
            if outcomes[index] is not None:
                continue
            try:
                outcomes[index] = {"result": self._run_payload(payload, runtime_env, cancellation_check)}
            except Exception as exc:
                outcomes[index] = {"error": str(exc)}
        return [outcome for outcome in outcomes if outcome is not None]

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
        return self._run_payload(payload, runtime_env, cancellation_check)
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
