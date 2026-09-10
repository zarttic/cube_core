from __future__ import annotations

import logging
from datetime import date
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
from cube_web.services.scene_contracts import CarbonFootprintPreviewRequest, CarbonGridPreviewRequest, DatasetReloadBatchRequest, PartitionDraftCreateRequest, ScenePartitionRunRequest, reload_selection_band_unit_ids
from cube_web.services.partition_workflow import _safe_dataset_error

logger = logging.getLogger(__name__)


class SceneRepository(Protocol):
    def upsert_load_schema(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def list_load_batches(
        self,
        *,
        status: str | None = None,
        data_type: str | None = None,
        keyword: str | None = None,
        dataset_id: str | None = None,
        page: int = 1,
        page_size: int = 20,
        limit: int | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]: ...

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

    def rebind_partition_task(
        self,
        source_task_id: str,
        task_id: str,
        *,
        band_unit_ids: tuple[str, ...] | None = None,
    ) -> str | None: ...

    def fail_partition_run(self, partition_run_id: str, error_message: str) -> None: ...

    def update_partition_task(self, task_id: str, status: str, result: dict[str, Any] | None = None) -> str | None: ...

    def list_partition_quality_batches(
        self,
        *,
        keyword: str | None = None,
        data_type: str | None = None,
        status: str | None = None,
        created_from: date | None = None,
        created_to: date | None = None,
        page: int = 1,
        page_size: int = 20,
        limit: int | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]: ...

    def get_partition_quality_batch(self, partition_run_id: str) -> dict[str, Any] | None: ...

    def list_partition_quality_targets(self, partition_run_id: str) -> list[dict[str, Any]]: ...

    def list_failed_quality_band_unit_ids(self, partition_run_id: str) -> list[dict[str, Any]]: ...

    def get_partition_run_task_id(self, partition_run_id: str) -> str | None: ...

    def create_partition_draft(self, *, draft_id: str, draft_name: str, data_type: str, source_batch_ids: tuple[str, ...], selection: dict[str, Any], created_by: str) -> dict[str, Any]: ...

    def create_dataset_reload_batch(self, *, load_batch_id: str, batch_name: str, source_batch_ids: tuple[str, ...], dataset_id: str, scene_ids: tuple[str, ...], selection: dict[str, Any], created_by: str) -> dict[str, Any]: ...

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

    def bind_partition_retry(
        self,
        source_task_id: str,
        task_id: str,
        *,
        band_unit_ids: tuple[str, ...] | None = None,
    ) -> str | None:
        candidate = source_task_id
        partition_run_id = None
        visited: set[str] = set()
        while candidate and candidate not in visited and len(visited) < 100:
            visited.add(candidate)
            if band_unit_ids:
                partition_run_id = self.repository.rebind_partition_task(
                    candidate,
                    task_id,
                    band_unit_ids=band_unit_ids,
                )
            else:
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
        dataset_id: str | None = None,
        page: int = 1,
        page_size: int = 20,
        limit: int | None = None,
    ) -> dict[str, Any]:
        if limit is not None:
            page = 1
            page_size = limit
        result = self.repository.list_load_batches(
            status=status,
            data_type=data_type,
            keyword=keyword,
            dataset_id=dataset_id,
            page=page,
            page_size=page_size,
        )
        if isinstance(result, dict):
            items = result.get("items", result.get("load_batches", []))
            return {
                **result,
                "items": items,
                "load_batches": result.get("load_batches", items),
                "total": int(result.get("total", len(items))),
                "page": int(result.get("page", page)),
                "page_size": int(result.get("page_size", page_size)),
            }
        return {
            "items": result,
            "load_batches": result,
            "total": len(result),
            "page": page,
            "page_size": page_size,
            "dataset_options": [],
        }

    def archive_load_batch(self, load_batch_id: str) -> dict[str, Any] | None:
        return self.repository.archive_load_batch(load_batch_id)

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
            selected_band_unit_ids = reload_selection_band_unit_ids(
                batch.get("attributes"),
                str(scene["dataset_id"]),
            )
            if selected_band_unit_ids is not None:
                scene["bands"] = [
                    band for band in scene.get("bands", [])
                    if str(band.get("band_unit_id") or "") in selected_band_unit_ids
                ]
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
        run = self.repository.create_partition_run(request)
        if not run.get("created"):
            attributes = run.get("attributes") if isinstance(run.get("attributes"), dict) else {}
            task_id = str(attributes.get("task_id") or "")
            if task_id:
                task = self.workflow.get_task(task_id).to_dict()
                return {
                    "partition_run_id": request.partition_run_id,
                    "source_batch_ids": list(request.source_batch_ids),
                    "task_id": task_id,
                    "status": str(task.get("status") or run.get("status") or "queued"),
                    "data_type": str(task.get("data_type") or "mixed"),
                    "operation": str(task.get("operation") or "run"),
                }
            raise HTTPException(
                status_code=409,
                detail=f"Partition run is being created: {request.partition_run_id}",
            )
        try:
            datasets = self.repository.materialize_partition_datasets(request)
            strict_request = build_partition_execution_request(request, datasets)
            data_types = {dataset.data_type for dataset in datasets}
            if len(data_types) > 1:
                task = self.workflow.submit_mixed(strict_request)
            else:
                task = self.workflow.submit_strict(next(iter(data_types)), strict_request)
            self.repository.bind_partition_task(request.partition_run_id, task.task_id)
        except Exception as exc:
            safe_error = _safe_dataset_error(exc)
            try:
                self.repository.fail_partition_run(request.partition_run_id, safe_error)
            except Exception:
                logger.exception("Failed to persist partition run failure %s", request.partition_run_id)
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

    def list_partition_quality_batches(
        self,
        *,
        keyword: str | None = None,
        data_type: str | None = None,
        status: str | None = None,
        created_from: date | None = None,
        created_to: date | None = None,
        page: int = 1,
        page_size: int = 20,
        limit: int | None = None,
    ) -> dict[str, Any]:
        if limit is not None:
            page = 1
            page_size = limit
        if created_from and created_to and created_from > created_to:
            raise HTTPException(status_code=422, detail="创建时间范围无效：开始日期不能晚于结束日期")
        result = self.repository.list_partition_quality_batches(
            keyword=keyword,
            data_type=data_type,
            status=status,
            created_from=created_from,
            created_to=created_to,
            page=page,
            page_size=page_size,
        )
        if isinstance(result, dict):
            items = result.get("items", [])
            return {
                **result,
                "items": items,
                "total": int(result.get("total", len(items))),
                "page": int(result.get("page", page)),
                "page_size": int(result.get("page_size", page_size)),
            }
        return {
            "items": result,
            "total": len(result),
            "page": page,
            "page_size": page_size,
        }

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
        """Retry failed partition or quality-failed units in the original batch."""
        task_id = self.repository.get_partition_run_task_id(partition_run_id)
        if not task_id:
            raise HTTPException(status_code=409, detail="partition batch has no retryable task")
        quality_failed = self.repository.list_failed_quality_band_unit_ids(partition_run_id)
        retry_band_unit_ids: dict[str, set[str]] = {}
        for item in quality_failed:
            retry_band_unit_ids.setdefault(str(item["dataset_id"]), set()).add(str(item["band_unit_id"]))
        task = self.workflow.retry_task(
            task_id,
            retry_band_unit_ids=retry_band_unit_ids or None,
            retry_strategy="quality_failed_units" if retry_band_unit_ids else None,
        )
        self.bind_partition_retry(
            task_id,
            task.task_id,
            band_unit_ids=tuple(sorted({band for bands in retry_band_unit_ids.values() for band in bands})) or None,
        )
        return task.to_dict()

    def cancel_partition_run(self, partition_run_id: str) -> dict[str, Any]:
        """Force-stop the task bound to a partition run."""
        task_id = self.repository.get_partition_run_task_id(partition_run_id)
        if not task_id:
            raise HTTPException(status_code=409, detail="partition batch has no cancellable task")
        force_cancel = getattr(self.workflow, "force_cancel_task", None)
        task = (force_cancel or self.workflow.cancel_task)(task_id)
        task_value = task.to_dict() if hasattr(task, "to_dict") else dict(task)
        return {"partition_run_id": partition_run_id, **task_value}

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

    def create_dataset_reload_batch(self, payload: DatasetReloadBatchRequest, actor: Any) -> dict[str, Any]:
        if len(payload.datasets) != 1:
            raise ValueError("dataset reload must contain exactly one dataset selection")
        selection = dict(payload.datasets[0])
        dataset_id = str(selection.get("dataset_id") or "").strip()
        scene_ids = tuple(str(scene.get("scene_id") or "").strip() for scene in selection.get("scenes", ()))
        band_unit_ids = selection.get("band_unit_ids")
        if not dataset_id or not scene_ids or any(not scene_id for scene_id in scene_ids):
            raise ValueError("dataset reload requires one dataset and one or more scenes")
        if not isinstance(band_unit_ids, (list, tuple)) or not band_unit_ids or any(not str(value).strip() for value in band_unit_ids):
            raise ValueError("dataset reload requires one or more selected band units")
        request = ScenePartitionRunRequest.model_validate({
            "partition_run_id": f"reload-validation-{uuid4().hex[:12]}",
            "source_batch_ids": payload.source_batch_ids,
            "selection_source": "dataset",
            "datasets": [{
                "dataset_id": dataset_id,
                "scene_ids": scene_ids,
                "band_unit_ids": selection.get("band_unit_ids"),
                "partition": selection.get("partition"),
            }],
        })
        resolved = self.repository.materialize_partition_datasets(request)
        if {dataset.data_type for dataset in resolved} != {payload.data_type}:
            raise ValueError("reload data_type must match the selected dataset")
        load_batch_id = f"dataset-reload-{uuid4().hex[:12]}"
        formal_selection = {
            **selection,
            "selection_id": f"{load_batch_id}:{dataset_id}",
            "source_batch_id": load_batch_id,
            "selection_source": "dataset_reload",
            "grid_config_locked": True,
            "scenes": [
                {**dict(scene), "source_batch_ids": [load_batch_id], "load_batch_id": load_batch_id}
                for scene in selection.get("scenes", ())
            ],
        }
        batch = self.repository.create_dataset_reload_batch(
            load_batch_id=load_batch_id,
            batch_name=payload.draft_name.strip(),
            source_batch_ids=payload.source_batch_ids,
            dataset_id=dataset_id,
            scene_ids=scene_ids,
            selection={"datasets": [formal_selection]},
            created_by=str(getattr(actor, "username", actor) or "system"),
        )
        return {**batch, "selection": {"datasets": [formal_selection]}}

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
    resolved: list[DatasetInput] = []
    if len(datasets) != len(request.datasets):
        raise ValueError("resolved partition selections do not match request")
    for dataset, selection in zip(datasets, request.datasets, strict=True):
        if dataset.dataset_id != selection.dataset_id:
            raise ValueError(f"resolved unexpected dataset: {dataset.dataset_id}")
        resolved.append(dataset.model_copy(update={
            "selection_id": selection.selection_id,
            "partition": selection.partition,
        }))
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
        worker_container_limit=request.worker_container_limit,
        cover_mode=first.cover_mode or "intersect",
        time_granularity=first.time_granularity or "day",
        max_cells_per_asset=0,
        datasets=tuple(resolved),
    )
