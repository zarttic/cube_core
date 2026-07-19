from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request

from cube_web.routes.auth import current_actor, require_admin
from cube_web.services.ingest_contracts import CancelIngestRun, ManualCollectionIngest, RetryIngestBandUnits
from cube_web.services.ingest_repository import (
    IngestRunNotFound,
    IngestSceneNotFound,
    InvalidIngestTransition,
)
from cube_web.services.ingest_service import IngestRunService


def create_ingest_runs_router(service: IngestRunService) -> APIRouter:
    router = APIRouter(prefix="/ingest-runs", tags=["ingest-runs"])

    @router.get("")
    def list_ingest_runs(
        keyword: str | None = None,
        dataset_id: str | None = None,
        status: Literal["pending", "queued", "running", "completed", "partial_failure", "failed", "cancelled"] | None = None,
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=20, ge=1, le=200),
        sort_by: str = "created_at",
        sort_order: str = "desc",
    ) -> dict:
        try:
            return service.list_runs(
                keyword=keyword,
                dataset_id=dataset_id,
                status=status,
                page=page,
                page_size=page_size,
                sort_by=sort_by,
                sort_order=sort_order,
            ).model_dump(mode="json")
        except ValueError as exc:
            raise HTTPException(status_code=422, detail={"code": "invalid_ingest_query", "message": str(exc)}) from exc

    @router.get("/collections")
    def list_collections(page: int = Query(default=1, ge=1), page_size: int = Query(default=20, ge=1, le=100)) -> dict:
        try:
            return service.list_collections(page=page, page_size=page_size)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail={"code": "invalid_ingest_query", "message": str(exc)}) from exc

    @router.post("/collections/{partition_run_id}/ingest", status_code=202)
    def ingest_collection(partition_run_id: str, payload: ManualCollectionIngest, request: Request) -> dict:
        actor = require_admin(current_actor(request))
        try:
            return service.request_collection_ingest(partition_run_id, payload.band_unit_ids, requested_by=actor.username)
        except RuntimeError as exc:
            raise _conflict(exc) from exc

    @router.get("/{ingest_run_id}")
    def get_ingest_run(ingest_run_id: str) -> dict:
        try:
            return service.get(ingest_run_id).model_dump(mode="json")
        except IngestRunNotFound as exc:
            raise _not_found(exc) from exc

    @router.post("/{ingest_run_id}/retry")
    def retry_ingest_band_units(ingest_run_id: str, payload: RetryIngestBandUnits, request: Request) -> dict:
        actor = require_admin(current_actor(request))
        try:
            return service.retry_failed(ingest_run_id, payload.band_unit_ids, requested_by=actor.username).model_dump(mode="json")
        except (IngestRunNotFound, IngestSceneNotFound) as exc:
            raise _not_found(exc) from exc
        except InvalidIngestTransition as exc:
            raise _conflict(exc) from exc

    @router.post("/{ingest_run_id}/cancel")
    def cancel_ingest_run(ingest_run_id: str, payload: CancelIngestRun, request: Request) -> dict:
        require_admin(current_actor(request))
        try:
            return service.cancel(ingest_run_id, payload.reason).model_dump(mode="json")
        except IngestRunNotFound as exc:
            raise _not_found(exc) from exc
        except InvalidIngestTransition as exc:
            raise _conflict(exc) from exc

    return router

def _not_found(exc: Exception) -> HTTPException:
    return HTTPException(status_code=404, detail={"code": "ingest_run_not_found", "message": str(exc)})


def _conflict(exc: Exception) -> HTTPException:
    return HTTPException(status_code=409, detail={"code": "invalid_ingest_transition", "message": str(exc)})
