from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field, model_validator

from cube_web.routes.auth import current_actor, require_admin
from cube_web.services.access_control import HIDEABLE_DATASET_ROLES, normalize_role
from cube_web.services.dataset_admin import build_dataset_management_service
from cube_web.services.dataset_management import (
    DETAILS,
    DatasetManagementConflict,
    DatasetManagementService,
    ManagedDatasetNotFound,
    ManagedDatasetQuery,
    ManagedSceneNotFound,
)
from cube_web.services.partition_contracts import GridType
from cube_web.services.publication_service import (
    PublicationNotFound,
    PublicationPolicyRejected,
    PublicationWithdrawalConflict,
)
from cube_web.services.quality_repository import DatasetNotFound, OutputVersionNotFound, QualityRunNotFound


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


class AssetUpdateRequest(StrictPayload):
    source_uri: str | None = Field(default=None, min_length=1, max_length=2000)
    cog_uri: str | None = Field(default=None, min_length=1, max_length=2000)
    checksum: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def require_change(self) -> "AssetUpdateRequest":
        if not self.model_fields_set:
            raise ValueError("at least one asset field is required")
        return self


class SceneReassignment(StrictPayload):
    target_dataset_id: str = Field(min_length=1)
    reason: str = Field(min_length=1, max_length=2000)


class ArchiveRequest(StrictPayload):
    reason: str = Field(min_length=1, max_length=2000)


class WithdrawRequest(StrictPayload):
    reason: str = Field(default="数据管理页面撤回", min_length=1, max_length=2000)


class PublishRequestPayload(StrictPayload):
    targets: list[dict[str, str]] = Field(default_factory=list)


class RoleRestrictionsPayload(StrictPayload):
    hidden_roles: list[str] = Field(default_factory=list, max_length=len(HIDEABLE_DATASET_ROLES))


def create_datasets_router(service: DatasetManagementService | None = None, task_store: Any | None = None) -> APIRouter:
    service = service or _production_service()
    if task_store is None:
        from cube_web.routes.partition import partition_service

        task_store = partition_service.task_store
    router = APIRouter(prefix="/datasets", tags=["datasets"])

    @router.get("")
    def list_datasets(
        request: Request,
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
            actor = current_actor(request)
            return service.list_datasets(ManagedDatasetQuery(
                keyword=keyword, data_type=data_type, product_type=product_type,
                ingest_status=ingest_status, quality_status=quality_status,
                publish_status=publish_status, archived=archived,
                time_start=time_start, time_end=time_end, page=page,
                page_size=page_size, sort_by=sort_by, sort_order=sort_order,
            ), viewer_role=actor.role)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail={"code": "invalid_dataset_query", "message": str(exc)}) from exc

    # Registered before "/{dataset_id}" so the literal path wins the match.
    @router.get("/deletions")
    def list_dataset_deletions(
        request: Request,
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=20, ge=1, le=200),
        status: str | None = Query(default=None),
    ) -> dict:
        require_admin(current_actor(request))
        return _call(lambda: service.list_deletions(page=page, page_size=page_size, status=status))

    @router.get("/deletions/{deletion_id}")
    def get_dataset_deletion(deletion_id: str, request: Request) -> dict:
        require_admin(current_actor(request))
        return _call(lambda: service.get_deletion(deletion_id))

    @router.get("/{dataset_id}")
    def get_dataset(dataset_id: str, request: Request) -> dict:
        return _call(lambda: service.get_dataset(dataset_id, viewer_role=current_actor(request).role))

    @router.get("/{dataset_id}/role-restrictions")
    def get_role_restrictions(dataset_id: str, request: Request) -> dict:
        require_admin(current_actor(request))
        return _call(lambda: {"hidden_roles": service.list_hidden_roles(dataset_id)})

    @router.put("/{dataset_id}/role-restrictions")
    def replace_role_restrictions(dataset_id: str, payload: RoleRestrictionsPayload, request: Request) -> dict:
        actor = require_admin(current_actor(request))
        try:
            roles = tuple(sorted({normalize_role(role) for role in payload.hidden_roles}))
            if any(role not in HIDEABLE_DATASET_ROLES for role in roles):
                raise ValueError("administrator visibility cannot be restricted")
        except ValueError as exc:
            raise HTTPException(status_code=422, detail={"code": "invalid_dataset_role", "message": str(exc)}) from exc
        return _call(lambda: {"hidden_roles": service.replace_hidden_roles(dataset_id, roles, actor=actor.username)})

    @router.patch("/{dataset_id}")
    def update_dataset(dataset_id: str, payload: MetadataPatch, request: Request) -> dict:
        actor = require_admin(current_actor(request))
        return _call(lambda: service.update_metadata(dataset_id, payload.model_dump(exclude_unset=True), actor=actor.username))

    @router.patch("/{dataset_id}/assets/{source_asset_id}")
    def update_dataset_asset(dataset_id: str, source_asset_id: str, payload: AssetUpdateRequest, request: Request) -> dict:
        actor = require_admin(current_actor(request))
        return _call(lambda: service.update_dataset_asset(
            dataset_id, source_asset_id, payload.model_dump(exclude_unset=True), actor=actor.username
        ))

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

    @router.post("/{dataset_id}/bands/{band_unit_id}/ingest-retry", status_code=202)
    def retry_band_ingest(dataset_id: str, band_unit_id: str, request: Request) -> dict:
        actor = require_admin(current_actor(request))
        return _call(lambda: service.retry_failed_band_ingest(dataset_id, band_unit_id, actor=actor.username))

    @router.delete("/{dataset_id}/bands/{band_unit_id}/grids/{grid_type}", status_code=202)
    def delete_band_grid(dataset_id: str, band_unit_id: str, grid_type: GridType, request: Request) -> dict:
        actor = require_admin(current_actor(request))
        return _call(lambda: service.queue_band_grid_deletion(
            dataset_id, band_unit_id, grid_type, actor=actor.username, task_store=task_store
        ))

    @router.post("/{dataset_id}/ingest", status_code=202)
    def request_ingest(dataset_id: str, request: Request) -> dict:
        actor = require_admin(current_actor(request))
        return _call(lambda: service.request_ingest(dataset_id, actor))

    @router.post("/{dataset_id}/publish", status_code=201)
    def publish(dataset_id: str, payload: PublishRequestPayload, request: Request) -> dict:
        actor = require_admin(current_actor(request))
        return _call(lambda: service.publish(dataset_id, actor, tuple(payload.targets)))

    @router.post("/{dataset_id}/publications/{publication_id}/withdraw")
    def withdraw(dataset_id: str, publication_id: UUID, payload: WithdrawRequest, request: Request) -> dict:
        actor = require_admin(current_actor(request))
        return _call(lambda: service.withdraw(dataset_id, str(publication_id), payload.reason, actor))

    @router.post("/{dataset_id}/archive")
    def archive(dataset_id: str, payload: ArchiveRequest, request: Request) -> dict:
        actor = require_admin(current_actor(request))
        return _call(lambda: service.archive(dataset_id, reason=payload.reason, actor=actor.username))

    @router.delete("/{dataset_id}", status_code=202)
    def delete_dataset(dataset_id: str, request: Request) -> dict:
        """Queue the deletion and return its job; the worker removes the rows.

        Deleting synchronously meant one 790 s transaction for a large dataset
        (2026-09-19), which stalled every other session.  The response carries the
        deletion id the UI polls: ``GET /v1/datasets/deletions/{deletion_id}``.
        """
        actor = require_admin(current_actor(request))
        return _call(lambda: service.request_deletion(dataset_id, actor=actor.username))

    for detail in sorted(DETAILS):
        router.add_api_route(
            f"/{{dataset_id}}/{detail}", _detail_handler(service, detail), methods=["GET"],
            name=f"list_managed_dataset_{detail.replace('-', '_')}",
        )
    return router


def _detail_handler(service: DatasetManagementService, detail: str):
    def handler(
        dataset_id: str,
        request: Request,
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=20, ge=1, le=500),
    ) -> dict:
        return _call(lambda: service.list_detail(
            dataset_id, detail, page=page, page_size=page_size, viewer_role=current_actor(request).role
        ))

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
    # Wiring lives in services/dataset_admin.py so the load-batch deletion route
    # shares the exact same hooks (quality/ingest/publish/withdraw/object cleanup).
    return build_dataset_management_service()
