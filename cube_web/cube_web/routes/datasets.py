from __future__ import annotations

from datetime import datetime
from uuid import UUID

from cube_split import runtime_config
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field, model_validator

from cube_web.routes.auth import current_actor, require_admin
from cube_web.services.dataset_management import (
    DETAILS,
    DatasetManagementConflict,
    DatasetManagementService,
    ManagedDatasetNotFound,
    ManagedDatasetQuery,
    ManagedSceneNotFound,
    OpenGaussDatasetManagementRepository,
)
from cube_web.services.publication_service import (
    PublicationNotFound,
    PublicationPolicyRejected,
    PublicationWithdrawalConflict,
    PublishRequest,
    publish_dataset,
    withdraw_publication,
)
from cube_web.services.quality_repository import DatasetNotFound, OutputVersionNotFound, QualityRunNotFound
from cube_web.services.quality_run_service import request_manual_quality_run


class StrictPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MetadataPatch(StrictPayload):
    dataset_title: str | None = Field(default=None, min_length=1, max_length=500)
    description: str | None = Field(default=None, max_length=5000)
    keywords: list[str] | None = None

    @model_validator(mode="after")
    def require_change(self) -> "MetadataPatch":
        if not self.model_fields_set:
            raise ValueError("at least one metadata field is required")
        return self


class SceneReassignment(StrictPayload):
    target_dataset_id: str = Field(min_length=1)
    reason: str = Field(min_length=1, max_length=2000)


class ArchiveRequest(StrictPayload):
    reason: str = Field(min_length=1, max_length=2000)


class WithdrawRequest(StrictPayload):
    reason: str = Field(default="数据管理页面撤回", min_length=1, max_length=2000)


def create_datasets_router(service: DatasetManagementService | None = None) -> APIRouter:
    service = service or _production_service()
    router = APIRouter(prefix="/datasets", tags=["datasets"])

    @router.get("")
    def list_datasets(
        keyword: str | None = None,
        data_type: str | None = None,
        product_type: str | None = None,
        ingest_status: str | None = None,
        quality_status: str | None = None,
        publish_status: str | None = None,
        archived: bool | None = None,
        time_start: datetime | None = None,
        time_end: datetime | None = None,
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=20, ge=1, le=500),
        sort_by: str = "updated_at",
        sort_order: str = "desc",
    ) -> dict:
        try:
            return service.list_datasets(ManagedDatasetQuery(
                keyword=keyword, data_type=data_type, product_type=product_type,
                ingest_status=ingest_status, quality_status=quality_status,
                publish_status=publish_status, archived=archived,
                time_start=time_start, time_end=time_end, page=page,
                page_size=page_size, sort_by=sort_by, sort_order=sort_order,
            ))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail={"code": "invalid_dataset_query", "message": str(exc)}) from exc

    @router.get("/{dataset_id}")
    def get_dataset(dataset_id: str) -> dict:
        return _call(lambda: service.get_dataset(dataset_id))

    @router.patch("/{dataset_id}")
    def update_dataset(dataset_id: str, payload: MetadataPatch, request: Request) -> dict:
        actor = require_admin(current_actor(request))
        return _call(lambda: service.update_metadata(dataset_id, payload.model_dump(exclude_unset=True), actor=actor.username))

    @router.post("/{dataset_id}/scenes/{scene_id}/reassign")
    def reassign_scene(dataset_id: str, scene_id: str, payload: SceneReassignment, request: Request) -> dict:
        actor = require_admin(current_actor(request))
        return _call(lambda: service.reassign_scene(
            dataset_id, scene_id, payload.target_dataset_id, reason=payload.reason, actor=actor.username
        ))

    @router.post("/{dataset_id}/quality-runs", status_code=202)
    def request_quality(dataset_id: str, request: Request) -> dict:
        actor = require_admin(current_actor(request))
        return _call(lambda: service.request_quality(dataset_id, actor))

    @router.post("/{dataset_id}/scenes/{scene_id}/ingest-retry", status_code=202)
    def retry_scene_ingest(dataset_id: str, scene_id: str, request: Request) -> dict:
        actor = require_admin(current_actor(request))
        return _call(lambda: service.retry_failed_scene_ingest(dataset_id, scene_id, actor=actor.username))

    @router.post("/{dataset_id}/publish", status_code=201)
    def publish(dataset_id: str, request: Request) -> dict:
        actor = require_admin(current_actor(request))
        return _call(lambda: service.publish(dataset_id, actor))

    @router.post("/{dataset_id}/publications/{publication_id}/withdraw")
    def withdraw(dataset_id: str, publication_id: UUID, payload: WithdrawRequest, request: Request) -> dict:
        actor = require_admin(current_actor(request))
        return _call(lambda: service.withdraw(dataset_id, str(publication_id), payload.reason, actor))

    @router.post("/{dataset_id}/archive")
    def archive(dataset_id: str, payload: ArchiveRequest, request: Request) -> dict:
        actor = require_admin(current_actor(request))
        return _call(lambda: service.archive(dataset_id, reason=payload.reason, actor=actor.username))

    for detail in sorted(DETAILS):
        router.add_api_route(
            f"/{{dataset_id}}/{detail}", _detail_handler(service, detail), methods=["GET"],
            name=f"list_managed_dataset_{detail.replace('-', '_')}",
        )
    return router


def _detail_handler(service: DatasetManagementService, detail: str):
    def handler(
        dataset_id: str,
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=20, ge=1, le=500),
    ) -> dict:
        return _call(lambda: service.list_detail(dataset_id, detail, page=page, page_size=page_size))

    return handler


def _call(callback):
    try:
        return callback()
    except ManagedDatasetNotFound as exc:
        raise HTTPException(status_code=404, detail={"code": "dataset_not_found", "message": str(exc)}) from exc
    except ManagedSceneNotFound as exc:
        raise HTTPException(status_code=404, detail={"code": "scene_not_found", "message": str(exc)}) from exc
    except (DatasetNotFound, OutputVersionNotFound, QualityRunNotFound, PublicationNotFound) as exc:
        raise HTTPException(status_code=404, detail={"code": "dataset_action_target_not_found", "message": str(exc)}) from exc
    except (DatasetManagementConflict, PublicationPolicyRejected, PublicationWithdrawalConflict) as exc:
        raise HTTPException(status_code=409, detail={"code": "dataset_action_conflict", "message": str(exc)}) from exc


def _production_service() -> DatasetManagementService:
    repository = OpenGaussDatasetManagementRepository(runtime_config.postgres_dsn())

    def quality_hook(dataset_id, actor):
        return request_manual_quality_run(dataset_id, None, actor).model_dump(mode="json")

    def publish_hook(dataset_id, actor):
        return publish_dataset(dataset_id, PublishRequest(), actor).model_dump(mode="json")

    def withdraw_hook(dataset_id, publication_id, reason, actor):
        return withdraw_publication(dataset_id, UUID(publication_id), reason, actor).model_dump(mode="json")

    return DatasetManagementService(
        repository, quality_hook=quality_hook, publish_hook=publish_hook, withdraw_hook=withdraw_hook
    )
