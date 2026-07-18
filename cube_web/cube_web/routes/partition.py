from __future__ import annotations

from typing import Any

from cube_split import runtime_config
from fastapi import APIRouter, HTTPException, Query, Request

from cube_web.routes.auth import current_actor, require_admin
from cube_web.schemas import (
    PartitionSchemaImportRequest,
    PartitionTaskCreateResponse,
    PartitionTaskResponse,
    payload_from_model,
)
from cube_web.services.partition_dataset_runner import NormalizedPartitionDatasetRunner
from cube_web.services.partition_domain_store import OpenGaussPartitionDomainStore, set_partition_domain_store
from cube_web.services.partition_service import PartitionService
from cube_web.services.partition_workflow import PartitionWorkflowService

partition_service = PartitionService()
partition_domain_store = OpenGaussPartitionDomainStore(dsn=runtime_config.postgres_dsn())
set_partition_domain_store(partition_domain_store)
partition_workflow_service = PartitionWorkflowService(
    partition_service,
    domain_store=partition_domain_store,
    runner=NormalizedPartitionDatasetRunner(),
)


def create_partition_router(
    service: PartitionService | None = None,
    workflow: PartitionWorkflowService | None = None,
    scene_service: Any | None = None,
) -> APIRouter:
    service = service or partition_service
    router = APIRouter(prefix="/partition", tags=["partition"])
    workflow_service = workflow or (partition_workflow_service if service is partition_service else PartitionWorkflowService(service))

    @router.get("/tasks")
    def list_partition_tasks(
        status: str | None = None,
        data_type: str | None = None,
        keyword: str | None = None,
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=20, ge=1, le=500),
        limit: int | None = Query(default=None, ge=1),
    ) -> dict:
        if limit is not None:
            page = 1
            page_size = limit
        return workflow_service.list_tasks(
            status=status,
            data_type=data_type,
            keyword=keyword,
            page=page,
            page_size=page_size,
        )

    @router.get("/tasks/{task_id}", response_model=PartitionTaskResponse)
    def get_partition_task(task_id: str) -> dict:
        return workflow_service.get_task(task_id).to_dict()

    @router.post("/tasks/{task_id}/cancel")
    def cancel_partition_task(task_id: str, request: Request) -> dict:
        require_admin(current_actor(request))
        return workflow_service.cancel_task(task_id)

    @router.post("/tasks/{task_id}/retry", response_model=PartitionTaskCreateResponse, status_code=202)
    def retry_partition_task(task_id: str, request: Request) -> dict:
        require_admin(current_actor(request))
        task = workflow_service.retry_task(task_id)
        bind_retry = getattr(scene_service, "bind_partition_retry", None)
        if bind_retry is not None:
            bind_retry(task_id, task.task_id)
        return task.to_dict()

    @router.post("/schemas/import")
    def import_partition_schema(payload: PartitionSchemaImportRequest) -> dict:
        request = payload_from_model(payload)
        return import_partition_schema_payload(workflow_service, scene_service, request)

    return router


def import_partition_schema_payload(
    workflow_service: PartitionWorkflowService,
    scene_service: Any | None,
    request: dict[str, Any],
) -> dict[str, Any]:
    """Import the canonical Dataset/Scene loader manifest."""
    if scene_service is None:
        raise HTTPException(status_code=503, detail="scene-domain import is not configured")
    try:
        scene_result = scene_service.import_load_schema(request)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {**scene_result, "status": "imported"}
