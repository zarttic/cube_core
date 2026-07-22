from __future__ import annotations

from math import isfinite
from pathlib import Path
from typing import Any, Callable, Protocol
from uuid import uuid4

from cube_split.jobs.ray_partition_core import resolve_asset_source_path
from cube_split.partition.carbon import CarbonSatelliteObservation, load_observations_from_file
from grid_core.sdk import CubeEncoderSDK
from cube_web.services.http_errors import HTTPException
from cube_web.services.partition_contracts import DatasetInput, StrictPartitionRequest
from cube_web.services.partition_defaults import default_grid_level_for_resolution, resolution_metadata_from_assets
from cube_web.services.scene_contracts import CarbonFootprintPreviewRequest, CarbonGridPreviewRequest, PartitionDraftCreateRequest, ScenePartitionRunRequest


class SceneRepository(Protocol):
    def upsert_load_schema(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def list_load_batches(
        self,
        *,
        status: str | None = None,
        data_type: str | None = None,
        keyword: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]: ...

    def get_load_batch(self, load_batch_id: str) -> dict[str, Any] | None: ...

    def list_load_batch_scenes(
        self,
        load_batch_id: str,
        *,
        status: str | None = None,
        data_type: str | None = None,
        dataset_id: str | None = None,
    ) -> list[dict[str, Any]]: ...

    def list_carbon_preview_sources(
        self,
        source_batch_ids: tuple[str, ...],
        scene_ids: tuple[str, ...],
    ) -> list[dict[str, Any]]: ...

    def materialize_partition_datasets(self, request: ScenePartitionRunRequest) -> tuple[DatasetInput, ...]: ...

    def create_partition_run(self, request: ScenePartitionRunRequest) -> dict[str, Any]: ...

    def bind_partition_task(self, partition_run_id: str, task_id: str) -> None: ...

    def rebind_partition_task(self, source_task_id: str, task_id: str) -> str | None: ...

    def fail_partition_run(self, partition_run_id: str, error_message: str) -> None: ...

    def update_partition_task(self, task_id: str, status: str, result: dict[str, Any] | None = None) -> str | None: ...

    def list_partition_quality_batches(self, *, limit: int = 100) -> list[dict[str, Any]]: ...

    def get_partition_quality_batch(self, partition_run_id: str) -> dict[str, Any] | None: ...

    def list_partition_quality_targets(self, partition_run_id: str) -> list[dict[str, Any]]: ...

    def get_partition_run_task_id(self, partition_run_id: str) -> str | None: ...

    def create_partition_draft(self, *, draft_id: str, draft_name: str, data_type: str, source_batch_ids: tuple[str, ...], selection: dict[str, Any], created_by: str) -> dict[str, Any]: ...

    def list_partition_drafts(self, *, data_type: str | None = None, limit: int = 100) -> list[dict[str, Any]]: ...

    def mark_partition_draft_submitted(self, draft_id: str, partition_run_id: str) -> dict[str, Any] | None: ...


class SceneDomainService:
    def __init__(
        self,
        repository: SceneRepository,
        workflow: Any,
        *,
        quality_requester: Callable[[str, str, Any], Any] | None = None,
    ) -> None:
        self.repository = repository
        self.workflow = workflow
        self.quality_requester = quality_requester
        add_listener = getattr(workflow, "add_task_event_listener", None)
        if add_listener is not None:
            add_listener(self._on_partition_task_event)

    def _on_partition_task_event(self, task_id: str, status: str, result: dict[str, Any] | None) -> None:
        partition_run_id = self.repository.update_partition_task(task_id, status, result)
        if partition_run_id is not None:
            return
        store = getattr(self.workflow, "store", None)
        attempt = store.get_attempt(task_id) if store is not None else None
        source_task_id = str((attempt or {}).get("source_task_id") or "")
        if source_task_id:
            self.bind_partition_retry(source_task_id, task_id)

    def bind_partition_retry(self, source_task_id: str, task_id: str) -> str | None:
        candidate = source_task_id
        partition_run_id = None
        visited: set[str] = set()
        while candidate and candidate not in visited and len(visited) < 100:
            visited.add(candidate)
            partition_run_id = self.repository.rebind_partition_task(candidate, task_id)
            if partition_run_id is not None:
                break
            attempt = self.workflow.store.get_attempt(candidate)
            candidate = str((attempt or {}).get("source_task_id") or "")
        if partition_run_id is None:
            return None
        current = self.workflow.get_task(task_id).to_dict()
        result = current.get("result") if isinstance(current.get("result"), dict) else None
        self.repository.update_partition_task(task_id, str(current.get("status") or "queued"), result)
        return partition_run_id

    def list_load_batches(
        self,
        *,
        status: str | None = None,
        data_type: str | None = None,
        keyword: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        return {
            "load_batches": self.repository.list_load_batches(
                status=status,
                data_type=data_type,
                keyword=keyword,
                limit=limit,
            )
        }

    def import_load_schema(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.repository.upsert_load_schema(payload)

    def get_load_batch(self, load_batch_id: str) -> dict[str, Any]:
        batch = self.repository.get_load_batch(load_batch_id)
        if batch is None:
            raise HTTPException(status_code=404, detail=f"Load batch not found: {load_batch_id}")
        return batch

    def list_load_batch_scenes(
        self,
        load_batch_id: str,
        *,
        status: str | None = None,
        data_type: str | None = None,
        dataset_id: str | None = None,
    ) -> dict[str, Any]:
        batch = self.get_load_batch(load_batch_id)
        scenes = self.repository.list_load_batch_scenes(
            load_batch_id,
            status=status,
            data_type=data_type,
            dataset_id=dataset_id,
        )
        grouped: dict[str, dict[str, Any]] = {}
        for scene in scenes:
            group_id = str(scene["dataset_id"])
            group = grouped.setdefault(
                group_id,
                {
                    "dataset_id": group_id,
                    "dataset_code": scene.get("dataset_code"),
                    "dataset_title": scene.get("dataset_title"),
                    "data_type": scene.get("data_type"),
                    "product_type": scene.get("product_type"),
                    "scenes": [],
                },
            )
            group["scenes"].append(scene)
        for group in grouped.values():
            if group.get("data_type") == "carbon":
                continue
            resolution_metadata = resolution_metadata_from_assets(group["scenes"])
            if not resolution_metadata:
                continue
            group.update(resolution_metadata)
            resolution_m = resolution_metadata["resolution_m"]
            group["suggested_grid_levels"] = {
                grid_type: default_grid_level_for_resolution(resolution_m, grid_type=grid_type)
                for grid_type in ("geohash", "mgrs", "isea4h")
            }
        return {
            "load_batch": batch,
            "datasets": list(grouped.values()),
            "scene_count": len(scenes),
        }

    def preview_carbon_footprints(self, request: CarbonFootprintPreviewRequest) -> dict[str, Any]:
        selected_scene_ids = set(request.scene_ids)
        for batch_id in request.source_batch_ids:
            self.get_load_batch(batch_id)
        sources = self.repository.list_carbon_preview_sources(request.source_batch_ids, request.scene_ids)
        available_scene_ids = {str(source.get("scene_id") or "") for source in sources}
        missing = sorted(selected_scene_ids - available_scene_ids)
        if missing:
            raise ValueError(f"carbon scenes are not eligible in source_batch_ids: {missing}")

        items: list[dict[str, Any]] = []
        unavailable_sources: list[dict[str, str]] = []
        truncated = False
        seen_sources: set[tuple[str, str]] = set()
        for source in sources:
            scene_id = str(source["scene_id"])
            source_batch_id = str(source["load_batch_id"])
            source_uri = str(source.get("source_uri") or "").strip()
            if not source_uri:
                unavailable_sources.append({
                    "scene_id": scene_id,
                    "source_batch_id": source_batch_id,
                    "reason": "missing_source_uri",
                })
                continue
            source_key = (source_uri, str(source.get("product_type") or "xco2"))
            if source_key in seen_sources:
                continue
            seen_sources.add(source_key)
            try:
                local_path = Path(resolve_asset_source_path(
                    source_uri,
                    {"source_cache_dir": "/tmp/cube_split_source_cache/preview"},
                ))
                observations = load_observations_from_file(
                    local_path,
                    max_observations=None,
                    product_type=source_key[1],
                )
                source_items = [
                    _carbon_footprint_item(scene_id, source_batch_id, observation)
                    for observation in observations
                ]
            except Exception as exc:
                unavailable_sources.append({
                    "scene_id": scene_id,
                    "source_batch_id": source_batch_id,
                    "reason": _carbon_preview_source_error_reason(exc),
                })
                continue
            items.extend(source_items)
        return {
            "items": items,
            "truncated": truncated,
            "unavailable_sources": unavailable_sources,
        }

    def preview_carbon_grid(self, request: CarbonGridPreviewRequest) -> dict[str, Any]:
        footprint_preview = self.preview_carbon_footprints(request)
        encoder = CubeEncoderSDK()
        cells: dict[tuple[str, int, str, str | None], dict[str, Any]] = {}
        for item in footprint_preview["items"]:
            geometry = item["geometry"]
            if geometry.get("type") == "Point":
                cells_for_observation = [encoder.locate(
                    grid_type=request.grid_type,
                    requested_grid_level=request.requested_grid_level,
                    point=geometry["coordinates"],
                )]
            else:
                cells_for_observation = encoder.cover(
                    grid_type=request.grid_type,
                    requested_grid_level=request.requested_grid_level,
                    cover_mode="intersect",
                    boundary_type="polygon",
                    geometry=geometry,
                    bbox=None,
                    crs="EPSG:4326",
                )
            for cell in cells_for_observation:
                key = (cell.grid_type, int(cell.grid_level), cell.space_code, cell.topology_code)
                if key in cells:
                    continue
                cells[key] = cell.model_dump(mode="json")
        return {
            **footprint_preview,
            "cells": list(cells.values()),
            "cell_limit_reached": False,
        }

    def submit_partition_run(self, request: ScenePartitionRunRequest) -> dict[str, Any]:
        datasets = self.repository.materialize_partition_datasets(request)
        strict_request = build_partition_execution_request(request, datasets)
        run = self.repository.create_partition_run(request)
        if not run.get("created"):
            attributes = run.get("attributes") if isinstance(run.get("attributes"), dict) else {}
            task_id = str(attributes.get("task_id") or "")
            if task_id:
                data_types = {dataset.data_type for dataset in datasets}
                return {
                    "partition_run_id": request.partition_run_id,
                    "source_batch_ids": list(request.source_batch_ids),
                    "task_id": task_id,
                    "status": str(run.get("status") or "queued"),
                    "data_type": "mixed" if len(data_types) > 1 else next(iter(data_types)),
                    "operation": "run",
                }
            raise HTTPException(
                status_code=409,
                detail=f"Partition run is being created: {request.partition_run_id}",
            )
        data_types = {dataset.data_type for dataset in datasets}
        try:
            if len(data_types) > 1:
                task = self.workflow.submit_mixed(strict_request)
            else:
                task = self.workflow.submit_strict(next(iter(data_types)), strict_request)
            self.repository.bind_partition_task(request.partition_run_id, task.task_id)
        except Exception as exc:
            self.repository.fail_partition_run(request.partition_run_id, str(exc))
            raise
        try:
            current = self.workflow.get_task(task.task_id).to_dict()
        except Exception:
            current = None
        if current is not None and str(current.get("status") or "") != "queued":
            self.repository.update_partition_task(
                task.task_id,
                str(current.get("status") or "queued"),
                current.get("result") if isinstance(current.get("result"), dict) else None,
            )
        task_value = task.to_dict()
        return {
            "partition_run_id": request.partition_run_id,
            "source_batch_ids": list(request.source_batch_ids),
            "task_id": task_value["task_id"],
            "status": task_value["status"],
            "data_type": task_value["data_type"],
            "operation": task_value["operation"],
        }

    def list_partition_quality_batches(self, *, limit: int = 100) -> dict[str, Any]:
        items = self.repository.list_partition_quality_batches(limit=limit)
        return {"items": items, "total": len(items)}

    def get_partition_quality_batch(self, partition_run_id: str) -> dict[str, Any]:
        batch = self.repository.get_partition_quality_batch(partition_run_id)
        if batch is None:
            raise HTTPException(status_code=404, detail=f"Partition run not found: {partition_run_id}")
        return batch

    def request_partition_quality(self, partition_run_id: str, actor: Any) -> dict[str, Any]:
        if self.quality_requester is None:
            raise HTTPException(status_code=503, detail="partition quality is not configured")
        targets = self.repository.list_partition_quality_targets(partition_run_id)
        if not targets:
            raise HTTPException(status_code=409, detail="partition batch has no completed band units awaiting quality")
        runs = [
            self.quality_requester(str(target["dataset_id"]), str(target["output_version"]), actor)
            for target in targets
        ]
        return {"partition_run_id": partition_run_id, "quality_runs": runs}

    def retry_failed_partition(self, partition_run_id: str) -> dict[str, Any]:
        """Retry failed units under the original immutable partition batch."""
        task_id = self.repository.get_partition_run_task_id(partition_run_id)
        if not task_id:
            raise HTTPException(status_code=409, detail="partition batch has no retryable task")
        task = self.workflow.retry_task(task_id)
        self.bind_partition_retry(task_id, task.task_id)
        return task.to_dict()

    def create_partition_draft(self, payload: PartitionDraftCreateRequest, actor: Any) -> dict[str, Any]:
        draft_id = f"partition-draft-{uuid4().hex[:12]}"
        request = ScenePartitionRunRequest.model_validate({
            "partition_run_id": draft_id,
            "source_batch_ids": payload.source_batch_ids,
            "selection_source": "dataset",
            "datasets": [
                {
                    "dataset_id": item.get("dataset_id"),
                    "scene_ids": [scene.get("scene_id") for scene in item.get("scenes", [])],
                    "band_unit_ids": item.get("band_unit_ids"),
                    "partition": item.get("partition"),
                }
                for item in payload.datasets
            ],
        })
        resolved = self.repository.materialize_partition_datasets(request)
        if {dataset.data_type for dataset in resolved} != {payload.data_type}:
            raise ValueError("draft data_type must match every selected dataset")
        return self.repository.create_partition_draft(
            draft_id=draft_id,
            draft_name=payload.draft_name.strip(),
            data_type=payload.data_type,
            source_batch_ids=payload.source_batch_ids,
            selection={"datasets": [dict(item) for item in payload.datasets]},
            created_by=str(getattr(actor, "username", actor) or "system"),
        )

    def list_partition_drafts(self, *, data_type: str | None = None, limit: int = 100) -> dict[str, Any]:
        items = self.repository.list_partition_drafts(data_type=data_type, limit=limit)
        return {"items": items, "total": len(items)}

    def mark_partition_draft_submitted(self, draft_id: str, partition_run_id: str) -> dict[str, Any]:
        draft = self.repository.mark_partition_draft_submitted(draft_id, partition_run_id)
        if draft is None:
            raise HTTPException(status_code=404, detail=f"Pending partition draft not found: {draft_id}")
        return draft


def _carbon_footprint_item(
    scene_id: str,
    source_batch_id: str,
    observation: CarbonSatelliteObservation,
) -> dict[str, Any]:
    if observation.footprint:
        coordinates = [[float(point[0]), float(point[1])] for point in observation.footprint]
        if len(coordinates) < 3 or not all(
            isfinite(longitude) and isfinite(latitude) and -180 <= longitude <= 180 and -90 <= latitude <= 90
            for longitude, latitude in coordinates
        ):
            raise ValueError(f"carbon observation has an invalid footprint: {observation.observation_id}")
        if coordinates[0] != coordinates[-1]:
            coordinates.append(coordinates[0])
        geometry: dict[str, Any] = {"type": "Polygon", "coordinates": [coordinates]}
    else:
        longitude, latitude = float(observation.lon), float(observation.lat)
        if not isfinite(longitude) or not isfinite(latitude) or not -180 <= longitude <= 180 or not -90 <= latitude <= 90:
            raise ValueError(f"carbon observation has invalid coordinates: {observation.observation_id}")
        geometry = {"type": "Point", "coordinates": [longitude, latitude]}
    return {
        "scene_id": scene_id,
        "source_batch_id": source_batch_id,
        "observation_id": observation.observation_id,
        "source_index": observation.source_index,
        "geometry": geometry,
    }


def _carbon_preview_source_error_reason(exc: Exception) -> str:
    if str(getattr(exc, "code", "")) in {"NoSuchKey", "NoSuchObject", "NoSuchBucket"}:
        return "source_not_found"
    if isinstance(exc, FileNotFoundError):
        return "source_not_found"
    return "source_unreadable"


def build_partition_execution_request(
    request: ScenePartitionRunRequest,
    datasets: tuple[DatasetInput, ...],
) -> StrictPartitionRequest:
    """Build the executor request using the partition run as its queue key.

    Load batch IDs remain source lineage and never become execution IDs.
    """
    if not datasets:
        raise ValueError("partition run must resolve at least one dataset")
    selections = {selection.dataset_id: selection for selection in request.datasets}
    resolved: list[DatasetInput] = []
    for dataset in datasets:
        selection = selections.get(dataset.dataset_id)
        if selection is None:
            raise ValueError(f"resolved unexpected dataset: {dataset.dataset_id}")
        resolved.append(dataset.model_copy(update={"partition": selection.partition}))
    first = request.datasets[0].partition
    missing = [
        name
        for name in ("grid_type", "requested_grid_level", "partition_method")
        if getattr(first, name) is None
    ]
    if missing:
        raise ValueError(f"dataset partition requires explicit fields: {', '.join(missing)}")
    return StrictPartitionRequest(
        batch_id=request.partition_run_id,
        grid_type=first.grid_type,
        requested_grid_level=first.requested_grid_level,
        partition_method=first.partition_method,
        cover_mode=first.cover_mode or "intersect",
        time_granularity=first.time_granularity or "day",
        max_cells_per_asset=0,
        datasets=tuple(resolved),
    )
