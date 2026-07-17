from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from cube_web.routes.auth import current_actor, require_admin
from cube_web.services.m6_scene_contracts import ScenePartitionRunRequest, ScenePartitionRunResponse
from cube_web.services.m6_scene_service import SceneDomainService


def create_m6_scene_router(service: SceneDomainService) -> APIRouter:
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

    @router.post("/runs", response_model=ScenePartitionRunResponse, status_code=202)
    def submit_partition_run(payload: ScenePartitionRunRequest, request: Request) -> dict:
        require_admin(current_actor(request))
        try:
            return service.submit_partition_run(payload)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    return router
