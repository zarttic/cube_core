from __future__ import annotations

from datetime import date
from typing import Any, Callable

from cube_split import runtime_config
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from cube_web.routes.auth import current_actor, require_admin
from cube_web.services import load_batch_deletion
from cube_web.services.db_pool import _PostgresPool
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


class ArchiveLoadBatchRequest(BaseModel):
    reason: str = Field(default="ARD 删除归档", min_length=1, max_length=2000)


class DeleteLoadBatchRequest(BaseModel):
    reason: str = Field(default="ARD 批次物理删除", min_length=1, max_length=2000)
    dry_run: bool = Field(default=False, description="只返回将要删除的范围，不写库")


class DeleteLoadBatchResponse(BaseModel):
    load_batch_id: str
    status: str | None = None
    source_type: str | None = None
    dry_run: bool = False
    scene_ids: list[str] = Field(default_factory=list)
    dataset_ids: list[str] = Field(default_factory=list)
    deletable_dataset_ids: list[str] = Field(default_factory=list)
    retained_dataset_ids: list[str] = Field(default_factory=list)
    deleted: dict[str, int] = Field(default_factory=dict)
    dataset_results: list[dict] = Field(default_factory=list)
    reason: str | None = None
    actor: str | None = None


def create_scene_partition_router(
    service: SceneDomainService,
    *,
    dataset_service_factory: Callable[[], Any] | None = None,
) -> APIRouter:
    router = APIRouter(prefix="/partition", tags=["partition-scenes"])

    @router.get("/load-batches")
    def list_load_batches(
        status: str | None = None,
        data_type: str | None = None,
        keyword: str | None = None,
        dataset_id: str | None = None,
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=20, ge=1, le=500),
        limit: int | None = Query(default=None, ge=1, le=500),
    ) -> dict:
        if limit is not None:
            page = 1
            page_size = limit
        return service.list_load_batches(
            status=status,
            data_type=data_type,
            keyword=keyword,
            dataset_id=dataset_id,
            page=page,
            page_size=page_size,
        )

    @router.get("/load-batches/{load_batch_id}")
    def get_load_batch(load_batch_id: str) -> dict:
        return service.get_load_batch(load_batch_id)

    @router.post("/load-batches/{load_batch_id}/archive")
    def archive_load_batch(load_batch_id: str, payload: ArchiveLoadBatchRequest, request: Request) -> dict:
        current_actor(request)
        batch = service.archive_load_batch(load_batch_id)
        if batch is None:
            raise HTTPException(status_code=404, detail="Load batch not found")
        batch["reason"] = payload.reason
        return batch

    @router.post("/load-batches/{load_batch_id}/delete", response_model=DeleteLoadBatchResponse)
    def delete_load_batch_endpoint(
        load_batch_id: str, payload: DeleteLoadBatchRequest, request: Request
    ) -> DeleteLoadBatchResponse:
        """Physically delete one load batch (ARD loader contract; replaces archive).

        Admin-only.  A batch whose dataset is shared with another batch only drops
        its own lineage rows; the dataset is deleted only when this batch is its
        sole owner.  Missing batch -> 404 so the caller's legacy fallback keeps
        working; guards -> 409.
        """
        actor = require_admin(current_actor(request))
        dataset_admin = dataset_service_factory() if dataset_service_factory else None
        if dataset_admin is None:
            raise HTTPException(status_code=503, detail="dataset deletion is not configured")
        database = runtime_config.postgres_dsn()
        if not database:
            raise HTTPException(status_code=503, detail="database is not configured")
        try:
            with _PostgresPool.for_dsn(database).connection() as connection:
                result = load_batch_deletion.delete_load_batch(
                    connection,
                    load_batch_id=load_batch_id,
                    actor=actor.username,
                    reason=payload.reason,
                    dataset_admin=dataset_admin,
                    dry_run=payload.dry_run,
                )
        except load_batch_deletion.LoadBatchNotFound as exc:
            raise HTTPException(status_code=404, detail="Load batch not found") from exc
        except load_batch_deletion.LoadBatchDeletionConflict as exc:
            raise HTTPException(
                status_code=409, detail={"code": "load_batch_delete_conflict", "message": str(exc)}
            ) from exc
        return DeleteLoadBatchResponse(**result)

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
    def list_partition_quality_runs(
        keyword: str | None = None,
        data_type: str | None = None,
        status: str | None = None,
        created_from: date | None = Query(default=None, description="创建时间起始日期，包含当天"),
        created_to: date | None = Query(default=None, description="创建时间结束日期，包含当天"),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=20, ge=1, le=500),
        limit: int | None = Query(default=None, ge=1, le=500),
    ) -> dict:
        if limit is not None:
            page = 1
            page_size = limit
        return service.list_partition_quality_batches(
            keyword=keyword,
            data_type=data_type,
            status=status,
            created_from=created_from,
            created_to=created_to,
            page=page,
            page_size=page_size,
        )

    @router.get("/runs/{partition_run_id}/quality")
    def get_partition_quality_run(partition_run_id: str) -> dict:
        return service.get_partition_quality_batch(partition_run_id)

    @router.post("/runs/{partition_run_id}/cancel")
    def cancel_partition_run(partition_run_id: str, request: Request) -> dict:
        require_admin(current_actor(request))
        return service.cancel_partition_run(partition_run_id)

    @router.post("/runs/{partition_run_id}/quality", status_code=202)
    def request_partition_quality_run(partition_run_id: str, request: Request) -> dict:
        actor = require_admin(current_actor(request))
        return service.request_partition_quality(partition_run_id, actor)

    @router.post("/runs/{partition_run_id}/retry-failed", status_code=202)
    def retry_failed_partition_run(partition_run_id: str, request: Request) -> dict:
        require_admin(current_actor(request))
        return service.retry_failed_partition(partition_run_id)

    return router
