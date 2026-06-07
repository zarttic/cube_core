from __future__ import annotations

from importlib import import_module

from fastapi import APIRouter, Query

from cube_web.schemas import (
    PartitionAssetRetryRequest,
    PartitionBatchRunRequest,
    PartitionDemoRequest,
    PartitionResult,
    PartitionRetryRequest,
    PartitionSchemaImportRequest,
    PartitionTaskCreateResponse,
    PartitionTaskResponse,
    payload_from_model,
)
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
partition_workflow_service = PartitionWorkflowService(partition_service)


def create_partition_router(
    service: PartitionService | None = None,
    workflow: PartitionWorkflowService | None = None,
    legacy_service: PartitionService | None = None,
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

    @router.post("/{data_type}/demo", response_model=PartitionResult)
    def partition_demo(data_type: str, payload: PartitionDemoRequest | None = None) -> dict:
        return legacy_service.demo(data_type, payload_from_model(payload))

    @router.post("/{data_type}/run", response_model=PartitionResult)
    def partition_run(data_type: str, payload: PartitionDemoRequest | None = None) -> dict:
        return service.run(data_type, payload_from_model(payload))

    @router.post("/{data_type}/retry", response_model=PartitionResult)
    def partition_retry(data_type: str, payload: PartitionRetryRequest | None = None) -> dict:
        return legacy_service.retry(data_type, payload_from_model(payload))

    @router.post("/{data_type}/test", response_model=PartitionResult)
    def partition_test(data_type: str, payload: PartitionDemoRequest | None = None) -> dict:
        return legacy_service.test(data_type, payload_from_model(payload))

    @router.post("/{data_type}/tasks/demo", response_model=PartitionTaskCreateResponse, status_code=202)
    def submit_partition_demo(data_type: str, payload: PartitionDemoRequest | None = None) -> dict:
        return legacy_service.submit(data_type, "demo", payload_from_model(payload)).to_dict()

    @router.post("/{data_type}/tasks/run", response_model=PartitionTaskCreateResponse, status_code=202)
    def submit_partition_run(data_type: str, payload: PartitionDemoRequest | None = None) -> dict:
        return workflow_service.run_payload(data_type, payload_from_model(payload)).to_dict()

    @router.post("/{data_type}/tasks/retry", response_model=PartitionTaskCreateResponse, status_code=202)
    def submit_partition_retry(data_type: str, payload: PartitionRetryRequest | None = None) -> dict:
        return legacy_service.submit(data_type, "retry", payload_from_model(payload)).to_dict()

    @router.post("/{data_type}/tasks/test", response_model=PartitionTaskCreateResponse, status_code=202)
    def submit_partition_test(data_type: str, payload: PartitionDemoRequest | None = None) -> dict:
        return legacy_service.submit(data_type, "test", payload_from_model(payload)).to_dict()

    @router.get("/tasks")
    def list_partition_tasks(
        status: str | None = None,
        data_type: str | None = None,
        keyword: str | None = None,
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict:
        return {
            "tasks": workflow_service.list_tasks(
                status=status,
                data_type=data_type,
                keyword=keyword,
                limit=limit,
            )
        }

    @router.get("/tasks/{task_id}", response_model=PartitionTaskResponse)
    def get_partition_task(task_id: str) -> dict:
        return service.get_task(task_id).to_dict()

    @router.post("/tasks/{task_id}/cancel")
    def cancel_partition_task(task_id: str) -> dict:
        return workflow_service.cancel_task(task_id)

    @router.post("/schemas/import")
    def import_partition_schema(payload: PartitionSchemaImportRequest) -> dict:
        return workflow_service.import_schema(payload_from_model(payload))

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

    @router.post("/batches/{batch_id}/run", response_model=PartitionTaskCreateResponse, status_code=202)
    def run_partition_batch(batch_id: str, payload: PartitionBatchRunRequest | None = None) -> dict:
        request = payload_from_model(payload)
        return workflow_service.run_batch(batch_id, config_override=request.get("config_override") or {}).to_dict()

    @router.post("/batches/{batch_id}/retry", response_model=PartitionTaskCreateResponse, status_code=202)
    def retry_partition_batch(batch_id: str, payload: PartitionBatchRunRequest | None = None) -> dict:
        request = payload_from_model(payload)
        return workflow_service.retry_batch(batch_id, config_override=request.get("config_override") or {}).to_dict()

    @router.post("/batches/{batch_id}/cancel")
    def cancel_partition_batch(batch_id: str) -> dict:
        batch = workflow_service.get_batch(batch_id)
        task_id = batch.get("last_task_id")
        if not task_id:
            return {"batch_id": batch_id, "status": batch.get("status")}
        return workflow_service.cancel_task(str(task_id))

    return router
