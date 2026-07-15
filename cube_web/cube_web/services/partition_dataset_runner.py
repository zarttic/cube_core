"""Ray execution adapter for the M2 normalized dataset contract."""

from __future__ import annotations

from datetime import datetime
from hashlib import sha256
from io import BytesIO
from typing import Any
from urllib.parse import urlparse

from cube_split import runtime_config

from cube_web.services.partition_contracts import OutputIdentity, make_output_id


def _time_bucket(value: str, granularity: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    formats = {"second": "%Y%m%d%H%M%S", "minute": "%Y%m%d%H%M", "hour": "%Y%m%d%H", "day": "%Y%m%d", "month": "%Y%m"}
    return parsed.strftime(formats[granularity])


def _run_dataset_on_ray(payload: dict[str, Any], runtime_env: dict[str, Any] | None) -> dict[str, Any]:
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
            if parsed.scheme != "s3" or parsed.netloc != settings["bucket"]:
                raise ValueError("normalized source COG must be in the configured MinIO bucket")
            local_path = cache_source_cog(
                source_uri, Path("/tmp/cube_split_source_cache") / "loader", client, settings["bucket"]
            )
            actual_source_checksum = sha256(local_path.read_bytes()).hexdigest()
            if actual_source_checksum != asset["checksum"]:
                raise ValueError("source COG checksum does not match the strict loader contract")
            with rasterio.open(local_path) as source:
                bounds = source.bounds
                if source.crs and str(source.crs).upper() != "EPSG:4326":
                    source_bbox = list(transform_bounds(source.crs, "EPSG:4326", bounds.left, bounds.bottom, bounds.right, bounds.top))
                else:
                    source_bbox = [float(bounds.left), float(bounds.bottom), float(bounds.right), float(bounds.top)]
                covered = sdk.cover(
                    grid_type=grid_type,
                    requested_grid_level=requested_grid_level,
                    cover_mode=value["cover_mode"],
                    boundary_type=BoundaryType.BBOX,
                    bbox=source_bbox,
                    crs="EPSG:4326",
                )
                limit = int(value["max_cells_per_asset"])
                if limit and len(covered) > limit:
                    raise RuntimeError(f"Cover cells exceed max limit: {len(covered)} > {limit}")
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
                    cells.setdefault(
                        cell_key,
                        {"output_id": make_output_id(cell_identity), "grid_type": grid_type, "grid_level": int(cell.grid_level),
                         "space_code": cell.space_code, "topology_code": cell.topology_code, "bbox": cell.bbox, "geometry": cell.geometry},
                    )
                    for band in (band for band in dataset["bands"] if band["source_asset_id"] == asset["source_asset_id"]):
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
                        }
                        address = GridAddress(grid_type=grid_type, grid_level=int(cell.grid_level), space_code=cell.space_code, topology_code=cell.topology_code)
                        index["st_code"] = sdk.generate_st_code(
                            address=address, timestamp=datetime.fromisoformat(asset["time_start"].replace("Z", "+00:00")),
                            time_granularity=value["time_granularity"],
                        ).st_code
                        if entity:
                            geometry = cell.geometry
                            if geometry is None:
                                geometry = sdk.code_to_geometry(address=address)
                            if source.crs and str(source.crs).upper() != "EPSG:4326":
                                geometry = transform_geom("EPSG:4326", source.crs, geometry)
                            data, tile_transform = rasterio.mask.mask(source, [geometry], crop=True)
                            profile = source.profile.copy()
                            profile.update(driver="GTiff", width=data.shape[2], height=data.shape[1], transform=tile_transform)
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
            "object_prefix": f"partition/{dataset['dataset_id']}/versions/{value['output_version']}/",
            "tiles": tiles, "indexes": indexes, "grid_cells": list(cells.values()),
        }

    if not ray.is_initialized():
        ray.init(
            address=payload["ray_address"],
            ignore_reinit_error=True,
            include_dashboard=False,
            logging_level=40,
            runtime_env=runtime_env,
        )
    return ray.get(execute.remote(payload))


class NormalizedPartitionDatasetRunner:
    """Production runner used exclusively by the M2 strict partition route."""

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
    ) -> dict[str, Any]:
        minio = runtime_config.minio_settings()
        from cube_split.jobs.ray_logical_partition_job import _ray_runtime_env_from_env

        ray_runtime_env = _ray_runtime_env_from_env() or {"env_vars": {}}
        env_vars = dict(ray_runtime_env.get("env_vars") or {})
        env_vars.update({
            "CUBE_WEB_MINIO_ENDPOINT": minio.endpoint,
            "CUBE_WEB_MINIO_ACCESS_KEY": minio.access_key,
            "CUBE_WEB_MINIO_SECRET_KEY": minio.secret_key,
            "CUBE_WEB_MINIO_BUCKET": minio.bucket,
        })
        ray_runtime_env["env_vars"] = env_vars
        return _run_dataset_on_ray(
            {
                "dataset": dataset.model_dump(mode="json"), "task_id": task_id, "output_version": output_version,
                "grid_type": grid_type, "requested_grid_level": requested_grid_level, "cover_mode": cover_mode,
                "time_granularity": time_granularity, "max_cells_per_asset": max_cells_per_asset,
                "ray_address": runtime_config.require_ray_address(),
            },
            ray_runtime_env,
        )
