from __future__ import annotations

from importlib import import_module
from typing import Any, Literal

from cube_split import runtime_config
from fastapi import APIRouter, HTTPException, Query

from cube_web.schemas import (
    PartitionAssetRetryRequest,
    PartitionBatchRunRequest,
    PartitionResult,
    PartitionRetryRequest,
    PartitionSchemaImportRequest,
    PartitionSchemaReconcileRequest,
    PartitionTaskCreateResponse,
    PartitionTaskResponse,
    StrictPartitionRequest,
    payload_from_model,
)
from cube_web.services.partition_contracts import validate_partition_method
from cube_web.services.partition_dataset_runner import NormalizedPartitionDatasetRunner
from cube_web.services.partition_domain_store import OpenGaussPartitionDomainStore, set_partition_domain_store
from cube_web.services.partition_service import PartitionService
from cube_web.services.partition_workflow import PartitionWorkflowService


def create_partition_service() -> PartitionService:
    partition_adapters = import_module("cube_web.routes.partition_adapters")
    partition_service_module = import_module("cube_web.services.partition_service")
    return PartitionService(
        partition_service_module.build_production_partition_registry(
            optical_run=partition_adapters.partition_optical_run,
            carbon_run=partition_adapters.partition_carbon_run,
            product_run=partition_adapters.partition_product_run,
            radar_run=partition_adapters.partition_radar_run,
            entity_run=partition_adapters.partition_entity_run,
        )
    )


def create_legacy_partition_service(source_service: PartitionService | None = None) -> PartitionService:
    partition_adapters = import_module("cube_web.routes.partition_adapters")
    partition_service_module = import_module("cube_web.services.partition_service")
    return PartitionService(
        partition_service_module.build_legacy_partition_registry(
            optical_demo=partition_adapters.partition_optical_demo,
            optical_test=partition_adapters.partition_optical_test,
            optical_retry=partition_adapters.partition_optical_retry,
            carbon_demo=partition_adapters.partition_carbon_demo,
            carbon_test=partition_adapters.partition_carbon_test,
            carbon_retry=partition_adapters.partition_carbon_retry,
            product_demo=partition_adapters.partition_product_demo,
            product_test=partition_adapters.partition_product_test,
            product_retry=partition_adapters.partition_product_retry,
            radar_demo=partition_adapters.partition_radar_demo,
            radar_test=partition_adapters.partition_radar_test,
            radar_retry=partition_adapters.partition_radar_retry,
            entity_demo=partition_adapters.partition_entity_demo,
            entity_test=partition_adapters.partition_entity_test,
            entity_retry=partition_adapters.partition_entity_retry,
        ),
        task_store=(source_service.task_store if source_service is not None else None),
    )


partition_service = create_partition_service()
legacy_partition_service = create_legacy_partition_service(partition_service)
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
    legacy_service: PartitionService | None = None,
    scene_service: Any | None = None,
) -> APIRouter:
    service = service or partition_service
    legacy_service = legacy_service or legacy_partition_service
    router = APIRouter(prefix="/partition", tags=["partition"])
    workflow_service = workflow or (partition_workflow_service if service is partition_service else PartitionWorkflowService(service))

    @router.post("/assets/retry", response_model=PartitionTaskCreateResponse, status_code=202)
    def retry_partition_assets(payload: PartitionAssetRetryRequest) -> dict:
        request = payload_from_model(payload)
        return workflow_service.retry_assets(
            list(request.get("asset_ids") or []),
            config_override=request.get("config_override") or {},
        ).to_dict()

    @router.post("/tasks/run", response_model=PartitionTaskCreateResponse, status_code=202, deprecated=True)
    def submit_mixed_partition_run(payload: StrictPartitionRequest) -> dict:
        if len({dataset.data_type for dataset in payload.datasets}) < 2:
            raise HTTPException(status_code=422, detail="mixed partition batches require at least two dataset data types")
        return workflow_service.submit_mixed(payload).to_dict()

    @router.post("/{data_type}/demo", response_model=PartitionResult)
    def partition_demo(data_type: str, payload: dict | None = None) -> dict:
        return legacy_service.demo(data_type, payload_from_model(payload))

    @router.post("/{data_type}/run", response_model=PartitionResult)
    def partition_run(data_type: str, payload: dict | None = None) -> dict:
        return workflow_service.run_payload_sync(data_type, payload_from_model(payload))

    @router.post("/{data_type}/retry", response_model=PartitionResult)
    def partition_retry(data_type: str, payload: PartitionRetryRequest | None = None) -> dict:
        return legacy_service.retry(data_type, payload_from_model(payload))

    @router.post("/{data_type}/test", response_model=PartitionResult)
    def partition_test(data_type: str, payload: dict | None = None) -> dict:
        return legacy_service.test(data_type, payload_from_model(payload))

    @router.post("/{data_type}/tasks/demo", response_model=PartitionTaskCreateResponse, status_code=202)
    def submit_partition_demo(data_type: str, payload: dict | None = None) -> dict:
        return legacy_service.submit(data_type, "demo", payload_from_model(payload)).to_dict()

    @router.post("/{data_type}/tasks/run", response_model=PartitionTaskCreateResponse, status_code=202, deprecated=True)
    def submit_partition_run(
        data_type: Literal["optical", "radar", "product", "carbon"],
        payload: StrictPartitionRequest,
    ) -> dict:
        if {dataset.data_type for dataset in payload.datasets} != {data_type}:
            raise HTTPException(status_code=422, detail="path data_type must match every dataset data_type")
        partition_method = validate_partition_method(payload.grid_type, payload.partition_method)
        request = payload.model_copy(update={"partition_method": partition_method})
        return workflow_service.submit_strict(data_type, request).to_dict()

    @router.post("/{data_type}/tasks/retry", response_model=PartitionTaskCreateResponse, status_code=202)
    def submit_partition_retry(data_type: str, payload: PartitionRetryRequest | None = None) -> dict:
        return legacy_service.submit(data_type, "retry", payload_from_model(payload)).to_dict()

    @router.post("/{data_type}/tasks/test", response_model=PartitionTaskCreateResponse, status_code=202)
    def submit_partition_test(data_type: str, payload: dict | None = None) -> dict:
        return legacy_service.submit(data_type, "test", payload_from_model(payload)).to_dict()

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
    def cancel_partition_task(task_id: str) -> dict:
        return workflow_service.cancel_task(task_id)

    @router.post("/schemas/import")
    def import_partition_schema(payload: PartitionSchemaImportRequest) -> dict:
        request = payload_from_model(payload)
        return import_partition_schema_payload(workflow_service, scene_service, request)

    @router.post("/schemas/reconcile")
    def reconcile_partition_schema(payload: PartitionSchemaReconcileRequest) -> dict:
        return workflow_service.reconcile_schemas(payload_from_model(payload))

    @router.get("/batches")
    def list_partition_batches(
        status: str | None = None,
        data_type: str | None = None,
        keyword: str | None = None,
        include_succeeded: bool = False,
        limit: int = 100,
    ) -> dict:
        return {
            "batches": workflow_service.list_batches(
                status=status,
                data_type=data_type,
                keyword=keyword,
                include_succeeded=include_succeeded,
                limit=limit,
            )
        }

    @router.get("/batches/{batch_id}")
    def get_partition_batch(batch_id: str) -> dict:
        return workflow_service.get_batch(batch_id)

    @router.get("/batches/{batch_id}/assets")
    def list_partition_assets(batch_id: str, status: str | None = None) -> dict:
        return {"assets": workflow_service.list_assets(batch_id, status=status)}

    @router.get("/batches/{batch_id}/attempts")
    def list_partition_attempts(batch_id: str) -> dict:
        return {"attempts": workflow_service.list_attempts(batch_id)}

    @router.post("/batches/{batch_id}/run", response_model=PartitionTaskCreateResponse, status_code=202, deprecated=True)
    def run_partition_batch(batch_id: str, payload: PartitionBatchRunRequest | None = None) -> dict:
        request = payload_from_model(payload)
        return workflow_service.run_batch(batch_id, config_override=request.get("config_override") or {}).to_dict()

    @router.post("/batches/{batch_id}/retry", response_model=PartitionTaskCreateResponse, status_code=202)
    def retry_partition_batch(batch_id: str, payload: PartitionBatchRunRequest | None = None) -> dict:
        request = payload_from_model(payload)
        return workflow_service.retry_batch(batch_id, config_override=request.get("config_override") or {}).to_dict()

    @router.post("/batches/{batch_id}/archive")
    def archive_partition_batch(batch_id: str) -> dict:
        return workflow_service.archive_batch(batch_id)

    @router.post("/batches/{batch_id}/requeue")
    def requeue_partition_batch(batch_id: str) -> dict:
        return workflow_service.requeue_batch(batch_id)

    @router.post("/batches/{batch_id}/cancel")
    def cancel_partition_batch(batch_id: str) -> dict:
        batch = workflow_service.get_batch(batch_id)
        task_id = batch.get("last_task_id")
        if not task_id:
            return {"batch_id": batch_id, "status": batch.get("status")}
        return workflow_service.cancel_task(str(task_id))

    return router


def import_partition_schema_payload(
    workflow_service: PartitionWorkflowService,
    scene_service: Any | None,
    request: dict[str, Any],
) -> dict[str, Any]:
    """Route legacy and M6 loader manifests through explicit adapters."""
    if request.get("datasets"):
        if scene_service is None:
            raise HTTPException(status_code=503, detail="M6 scene-domain import is not configured")
        try:
            scene_result = scene_service.import_load_schema(request)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return {**scene_result, "status": "imported", "scene_domain": scene_result}

    result = workflow_service.import_schema(request)
    should_sync = scene_service is not None and scene_service.should_sync_import(workflow_service.store)
    if should_sync:
        try:
            scene_result = scene_service.import_load_schema(request)
        except Exception as exc:
            scene_result = {
                "status": "sync_failed",
                "reconciliation_required": True,
                "error_type": type(exc).__name__,
            }
        result = {**result, "scene_domain": scene_result}
    return result
