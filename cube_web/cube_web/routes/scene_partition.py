from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from cube_web.routes.auth import current_actor, require_admin
from cube_web.services.scene_contracts import (
    CarbonFootprintPreviewRequest,
    CarbonGridPreviewRequest,
    DatasetReloadBatchRequest,
    PartitionDraftCreateRequest,
    PartitionDraftSubmittedRequest,
    ScenePartitionRunRequest,
    ScenePartitionRunResponse,
)
from cube_web.services.scene_service import SceneDomainService


def create_scene_partition_router(service: SceneDomainService) -> APIRouter:
    router = APIRouter(prefix="/partition", tags=["partition-scenes"])

    @router.get("/load-batches")
    def list_load_batches(
        status: str | None = None,
        data_type: str | None = None,
        keyword: str | None = None,
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict:
        return service.list_load_batches(status=status, data_type=data_type, keyword=keyword, limit=limit)

    @router.get("/load-batches/{load_batch_id}")
    def get_load_batch(load_batch_id: str) -> dict:
        return service.get_load_batch(load_batch_id)

    @router.get("/load-batches/{load_batch_id}/scenes")
    def list_load_batch_scenes(
        load_batch_id: str,
        status: str | None = None,
        data_type: str | None = None,
        dataset_id: str | None = None,
    ) -> dict:
        return service.list_load_batch_scenes(
            load_batch_id,
            status=status,
            data_type=data_type,
            dataset_id=dataset_id,
        )

    @router.post("/carbon/footprints")
    def preview_carbon_footprints(payload: CarbonFootprintPreviewRequest, request: Request) -> dict:
        require_admin(current_actor(request))
        try:
            return service.preview_carbon_footprints(payload)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @router.post("/carbon/grid-preview")
    def preview_carbon_grid(payload: CarbonGridPreviewRequest, request: Request) -> dict:
        require_admin(current_actor(request))
        try:
            return service.preview_carbon_grid(payload)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @router.post("/runs", response_model=ScenePartitionRunResponse, status_code=202)
    def submit_partition_run(payload: ScenePartitionRunRequest, request: Request) -> dict:
        require_admin(current_actor(request))
        try:
            return service.submit_partition_run(payload)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @router.get("/drafts")
    def list_partition_drafts(
        data_type: str | None = None,
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict:
        return service.list_partition_drafts(data_type=data_type, limit=limit)

    @router.post("/drafts", status_code=201)
    def create_partition_draft(payload: PartitionDraftCreateRequest, request: Request) -> dict:
        actor = require_admin(current_actor(request))
        try:
            return service.create_partition_draft(payload, actor)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @router.post("/reload-batches", status_code=201)
    def create_dataset_reload_batch(payload: DatasetReloadBatchRequest, request: Request) -> dict:
        actor = require_admin(current_actor(request))
        try:
            return service.create_dataset_reload_batch(payload, actor)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @router.post("/drafts/{draft_id}/submitted")
    def mark_partition_draft_submitted(
        draft_id: str,
        payload: PartitionDraftSubmittedRequest,
        request: Request,
    ) -> dict:
        require_admin(current_actor(request))
        return service.mark_partition_draft_submitted(draft_id, payload.partition_run_id)

    @router.get("/runs")
    def list_partition_quality_runs(limit: int = Query(default=100, ge=1, le=500)) -> dict:
        return service.list_partition_quality_batches(limit=limit)

    @router.get("/runs/{partition_run_id}/quality")
    def get_partition_quality_run(partition_run_id: str) -> dict:
        return service.get_partition_quality_batch(partition_run_id)

    @router.post("/runs/{partition_run_id}/quality", status_code=202)
    def request_partition_quality_run(partition_run_id: str, request: Request) -> dict:
        actor = require_admin(current_actor(request))
        return service.request_partition_quality(partition_run_id, actor)

    @router.post("/runs/{partition_run_id}/retry-failed", status_code=202)
    def retry_failed_partition_run(partition_run_id: str, request: Request) -> dict:
        require_admin(current_actor(request))
        return service.retry_failed_partition(partition_run_id)

    return router
