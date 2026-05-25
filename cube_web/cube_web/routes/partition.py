from __future__ import annotations

from fastapi import APIRouter

from cube_web.schemas import (
    PartitionDemoRequest,
    PartitionResult,
    PartitionRetryRequest,
    PartitionTaskCreateResponse,
    PartitionTaskResponse,
    payload_from_model,
)
from cube_web.services.partition_service import PartitionService


def create_partition_router(service: PartitionService) -> APIRouter:
    router = APIRouter(prefix="/partition", tags=["partition"])

    @router.post("/{data_type}/demo", response_model=PartitionResult)
    def partition_demo(data_type: str, payload: PartitionDemoRequest | None = None) -> dict:
        return service.demo(data_type, payload_from_model(payload))

    @router.post("/{data_type}/retry", response_model=PartitionResult)
    def partition_retry(data_type: str, payload: PartitionRetryRequest | None = None) -> dict:
        return service.retry(data_type, payload_from_model(payload))

    @router.post("/optical/test", response_model=PartitionResult)
    def partition_optical_test(payload: PartitionDemoRequest | None = None) -> dict:
        return service.test("optical", payload_from_model(payload))

    @router.post("/{data_type}/tasks/demo", response_model=PartitionTaskCreateResponse, status_code=202)
    def submit_partition_demo(data_type: str, payload: PartitionDemoRequest | None = None) -> dict:
        return service.submit(data_type, "demo", payload_from_model(payload)).to_dict()

    @router.post("/{data_type}/tasks/retry", response_model=PartitionTaskCreateResponse, status_code=202)
    def submit_partition_retry(data_type: str, payload: PartitionRetryRequest | None = None) -> dict:
        return service.submit(data_type, "retry", payload_from_model(payload)).to_dict()

    @router.get("/tasks/{task_id}", response_model=PartitionTaskResponse)
    def get_partition_task(task_id: str) -> dict:
        return service.get_task(task_id).to_dict()

    return router
