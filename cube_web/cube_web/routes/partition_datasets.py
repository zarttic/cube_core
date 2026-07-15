from __future__ import annotations

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request

from cube_web.routes.auth import current_actor
from cube_web.schemas import DatasetQualityRunRequest, WarnApprovalRequest, WithdrawPublicationRequest
from cube_web.schemas import PublishRequest as PublishPayload
from cube_web.services.dataset_service import DatasetQuery, DatasetService, PartitionDatasetNotFound, PartitionOutputVersionNotFound
from cube_web.services.partition_domain_store import get_partition_domain_store
from cube_web.services.publication_service import (
    PublicationNotFound,
    PublicationPolicyRejected,
    PublicationWithdrawalConflict,
    PublishRequest,
    WarnApprovalConflict,
    WarnApprovalRejected,
    approve_warn,
    publish_dataset,
    withdraw_publication,
)
from cube_web.services.quality_contracts import Page, page_offset, validate_sort
from cube_web.services.quality_repository import (
    DatasetNotFound,
    OutputVersionNotFound,
    QualityRunNotFound,
    count_quality_runs,
    list_quality_runs,
    require_open_gauss_domain_store,
)
from cube_web.services.quality_run_service import request_manual_quality_run


def create_partition_datasets_router(service: DatasetService | None = None) -> APIRouter:
    service = service or DatasetService(get_partition_domain_store())
    router = APIRouter(prefix="/partition/datasets", tags=["partition-datasets"])

    @router.get("")
    def list_datasets(
        keyword: str | None = None,
        data_type: str | None = None,
        product_type: str | None = None,
        batch_id: str | None = None,
        grid_type: str | None = None,
        partition_status: str | None = None,
        quality_status: str | None = None,
        publish_status: str | None = None,
        time_start: datetime | None = None,
        time_end: datetime | None = None,
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=20, ge=1, le=500),
        sort_by: str = "updated_at",
        sort_order: str = "desc",
    ) -> dict:
        try:
            return service.list_datasets(
                DatasetQuery(
                    keyword=keyword,
                    data_type=data_type,
                    product_type=product_type,
                    batch_id=batch_id,
                    grid_type=grid_type,
                    partition_status=partition_status,
                    quality_status=quality_status,
                    publish_status=publish_status,
                    time_start=time_start,
                    time_end=time_end,
                    page=page,
                    page_size=page_size,
                    sort_by=sort_by,
                    sort_order=sort_order,
                )
            ).model_dump(mode="json")
        except ValueError as exc:
            raise HTTPException(status_code=422, detail={"code": "invalid_sort", "message": str(exc)}) from exc

    @router.get("/{dataset_id}")
    def get_dataset(dataset_id: str) -> dict:
        return _dataset_response(lambda: service.get_dataset(dataset_id))

    @router.post("/{dataset_id}/quality-runs", status_code=202)
    def create_dataset_quality_run(dataset_id: str, payload: DatasetQualityRunRequest, request: Request) -> dict:
        try:
            return request_manual_quality_run(dataset_id, payload.output_version, current_actor(request)).model_dump(mode="json")
        except (DatasetNotFound, OutputVersionNotFound) as exc:
            raise HTTPException(status_code=404, detail={"code": "partition_dataset_not_found", "message": str(exc)}) from exc

    @router.post("/{dataset_id}/quality-runs/{quality_run_id}/warn-approvals", status_code=201)
    def approve_dataset_warn(dataset_id: str, quality_run_id: UUID, payload: WarnApprovalRequest, request: Request) -> dict:
        try:
            return approve_warn(dataset_id, quality_run_id, payload.reason, current_actor(request)).model_dump(mode="json")
        except (DatasetNotFound, QualityRunNotFound) as exc:
            raise HTTPException(status_code=404, detail={"code": "quality_run_not_found", "message": str(exc)}) from exc
        except (WarnApprovalRejected, WarnApprovalConflict) as exc:
            raise HTTPException(status_code=409, detail={"code": "warn_approval_rejected", "message": str(exc)}) from exc

    @router.post("/{dataset_id}/publish", status_code=201)
    def publish_dataset_route(dataset_id: str, payload: PublishPayload, request: Request) -> dict:
        try:
            quality_run_id = UUID(payload.quality_run_id) if payload.quality_run_id else None
            return publish_dataset(dataset_id, PublishRequest(payload.output_version, quality_run_id), current_actor(request)).model_dump(
                mode="json"
            )
        except (DatasetNotFound, OutputVersionNotFound, QualityRunNotFound) as exc:
            raise HTTPException(status_code=404, detail={"code": "publication_target_not_found", "message": str(exc)}) from exc
        except (PublicationPolicyRejected, ValueError) as exc:
            raise HTTPException(status_code=409, detail={"code": "publication_policy_rejected", "message": str(exc)}) from exc

    @router.post("/{dataset_id}/publications/{publication_id}/withdraw")
    def withdraw_dataset_publication(dataset_id: str, publication_id: UUID, payload: WithdrawPublicationRequest, request: Request) -> dict:
        try:
            return withdraw_publication(dataset_id, publication_id, payload.reason, current_actor(request)).model_dump(mode="json")
        except PublicationNotFound as exc:
            raise HTTPException(status_code=404, detail={"code": "publication_not_found", "message": str(exc)}) from exc
        except PublicationWithdrawalConflict as exc:
            raise HTTPException(status_code=409, detail={"code": "publication_withdrawal_rejected", "message": str(exc)}) from exc

    router.add_api_route(
        "/{dataset_id}/quality",
        _quality_detail_handler(service),
        methods=["GET"],
        name="list_partition_dataset_quality",
    )

    for detail in ("assets", "bands", "tiles", "indexes", "grid", "publications"):
        router.add_api_route(
            f"/{{dataset_id}}/{detail}",
            _detail_handler(service, detail),
            methods=["GET"],
            name=f"list_partition_dataset_{detail}",
        )

    return router


def _detail_handler(service: DatasetService, detail: str):
    def handler(
        dataset_id: str,
        output_version: str | None = None,
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=20, ge=1, le=500),
        sort_by: str = "created_at",
        sort_order: str = "asc",
    ) -> dict:
        try:
            return service.list_detail(
                dataset_id,
                detail,
                output_version=output_version,
                page=page,
                page_size=page_size,
                sort_by=sort_by,
                sort_order=sort_order,
            ).model_dump(mode="json")
        except PartitionDatasetNotFound as exc:
            raise HTTPException(status_code=404, detail={"code": "partition_dataset_not_found", "message": str(exc)}) from exc
        except PartitionOutputVersionNotFound as exc:
            raise HTTPException(status_code=404, detail={"code": "partition_output_version_not_found", "message": str(exc)}) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail={"code": "invalid_sort", "message": str(exc)}) from exc

    return handler


def _quality_detail_handler(service: DatasetService):
    def handler(
        dataset_id: str,
        output_version: str | None = None,
        status: str | None = None,
        trigger: str | None = None,
        current_only: bool = False,
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=20, ge=1, le=500),
        sort_by: str = "generated_at",
        sort_order: str = "desc",
    ) -> dict:
        try:
            service.get_dataset(dataset_id)
            if output_version is not None and service.store.get_output_version(dataset_id, output_version) is None:
                raise PartitionOutputVersionNotFound(output_version)
            sort_by, sort_order = validate_sort(
                sort_by,
                sort_order,
                {"created_at", "completed_at", "generated_at", "quality_sequence", "status"},
            )
            with require_open_gauss_domain_store().transaction() as tx:
                items = list_quality_runs(
                    tx,
                    keyword=None,
                    dataset_id=dataset_id,
                    output_version=output_version,
                    data_type=None,
                    status=status,
                    trigger=trigger,
                    requested_by=None,
                    current_only=current_only,
                    started_from=None,
                    started_to=None,
                    limit=page_size,
                    offset=page_offset(page, page_size),
                    sort_by=sort_by,
                    sort_order=sort_order,
                )
                total = count_quality_runs(
                    tx,
                    keyword=None,
                    dataset_id=dataset_id,
                    output_version=output_version,
                    data_type=None,
                    status=status,
                    trigger=trigger,
                    requested_by=None,
                    current_only=current_only,
                    started_from=None,
                    started_to=None,
                )
            return Page(items=tuple(items), total=total, page=page, page_size=page_size).model_dump(mode="json")
        except PartitionDatasetNotFound as exc:
            raise HTTPException(status_code=404, detail={"code": "partition_dataset_not_found", "message": str(exc)}) from exc
        except PartitionOutputVersionNotFound as exc:
            raise HTTPException(status_code=404, detail={"code": "partition_output_version_not_found", "message": str(exc)}) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail={"code": "invalid_sort", "message": str(exc)}) from exc

    return handler


def _dataset_response(call):
    try:
        return call()
    except PartitionDatasetNotFound as exc:
        raise HTTPException(status_code=404, detail={"code": "partition_dataset_not_found", "message": str(exc)}) from exc
