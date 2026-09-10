from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Lock
from typing import Any

from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from grid_core.sdk import CubeEncoderSDK, GridCoreError, NotImplementedCapabilityError, ValidationError

from cube_web.routes import partition as partition_route
from cube_web.routes.auth import create_auth_router, require_auth_for_api
from cube_web.routes.config import create_config_router
from cube_web.routes.partition import create_partition_router
from cube_web.routes.quality import create_quality_router
from cube_web.routes.sdk import create_sdk_router
from cube_web.services import health_service
from cube_web.services.api_errors import (
    detail_code,
    detail_message,
    error_response,
    ensure_request_id,
    request_id_from_header,
)
from cube_web.services.dataset_management import (
    DatasetManagementConflict,
    ManagedDatasetNotFound,
    ManagedSceneNotFound,
)
from cube_web.services.partition_job_store import (
    PartitionBatchAlreadyActiveError,
    PartitionBatchArchivedError,
    PartitionBatchNotRequeueableError,
)
from cube_web.services.quality_repository import (
    DatasetNotFound,
    OutputVersionNotCompleted,
    OutputVersionNotFound,
    QualityRunNotFound,
    QualityTriggerConflict,
)
from cube_web.services.quality_worker import QualityRuntime

ENCODER_SDK_CLASS = CubeEncoderSDK
logger = logging.getLogger(__name__)


async def request_id_middleware(request: Request, call_next):
    request_id_from_header(request)
    response = await call_next(request)
    response.headers.setdefault("X-Request-ID", request.state.request_id)
    return response


def _repo_root():
    return Path(__file__).resolve().parents[2]


partition_service = partition_route.partition_service
partition_workflow_service = partition_route.partition_workflow_service


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI):
        quality_runtime = QualityRuntime()
        try:
            partition_workflow_service.reconcile_orphaned_tasks()
        except Exception as exc:
            logger.warning("Skipping partition task reconcile during startup: %s", exc)
        try:
            quality_runtime.start()
            yield
        finally:
            quality_runtime.stop()

    web_app = FastAPI(title="cube-web", lifespan=lifespan)
    sdk = CubeEncoderSDK()
    api_router = APIRouter(prefix="/v1", tags=["sdk-web"])
    scene_domain_service, domain_routers = _build_domain_components()

    web_app.middleware("http")(request_id_middleware)
    web_app.middleware("http")(require_auth_for_api)
    web_app.add_exception_handler(GridCoreError, handle_grid_core_error)
    web_app.add_exception_handler(HTTPException, handle_http_exception)
    web_app.add_exception_handler(RequestValidationError, handle_validation_error)
    for exception_type in (
        DatasetNotFound,
        OutputVersionNotFound,
        QualityRunNotFound,
        ManagedDatasetNotFound,
        ManagedSceneNotFound,
    ):
        web_app.add_exception_handler(exception_type, handle_not_found_error)
    for exception_type in (
        OutputVersionNotCompleted,
        QualityTriggerConflict,
        PartitionBatchAlreadyActiveError,
        PartitionBatchArchivedError,
        PartitionBatchNotRequeueableError,
        DatasetManagementConflict,
    ):
        web_app.add_exception_handler(exception_type, handle_conflict_error)
    web_app.add_exception_handler(Exception, handle_unexpected_error)

    @web_app.get("/")
    async def root() -> dict[str, str]:
        return {"service": "cube-web", "status": "ok"}

    @web_app.get("/health")
    def health(
        checks: list[str] | None = Query(default=None),
        check: list[str] | None = Query(default=None),
    ) -> dict[str, Any]:
        report = health_service.health_report([*(checks or []), *(check or [])])
        report["checks"]["partition_queue"] = {
            "status": "ok",
            "queued": partition_service.queue_depth(),
            "max_workers": partition_service.task_store.max_workers,
        }
        return report

    api_router.include_router(create_sdk_router(sdk))
    api_router.include_router(create_quality_router())
    api_router.include_router(create_config_router())
    api_router.include_router(create_partition_router(scene_service=scene_domain_service))
    for router in domain_routers:
        api_router.include_router(router)
    web_app.include_router(api_router)
    web_app.include_router(create_auth_router())
    return web_app


class _LazyRepository:
    """Delay repository construction, including connection-pool startup, until first use."""

    def __init__(self, factory: Any) -> None:
        self._factory = factory
        self._instance: Any | None = None
        self._lock = Lock()

    def __getattr__(self, name: str) -> Any:
        instance = self._instance
        if instance is None:
            with self._lock:
                instance = self._instance
                if instance is None:
                    instance = self._factory()
                    self._instance = instance
        return getattr(instance, name)


def _build_domain_components() -> tuple[Any, tuple[APIRouter, ...]]:
    from cube_split import runtime_config

    from cube_web.routes.datasets import create_datasets_router
    from cube_web.routes.ingest_runs import create_ingest_runs_router
    from cube_web.routes.scene_partition import create_scene_partition_router
    from cube_web.services.ingest_repository import OpenGaussIngestRepository
    from cube_web.services.ingest_service import IngestRunService
    from cube_web.services.quality_run_service import request_manual_quality_run
    from cube_web.services.scene_repository import OpenGaussSceneRepository
    from cube_web.services.scene_service import SceneDomainService

    dsn = runtime_config.postgres_dsn()
    scene_service = SceneDomainService(
        OpenGaussSceneRepository(dsn),
        partition_workflow_service,
        quality_requester=lambda dataset_id, output_version, actor: request_manual_quality_run(
            dataset_id, output_version, actor
        ).model_dump(mode="json"),
    )
    ingest_service = IngestRunService(_LazyRepository(lambda: OpenGaussIngestRepository(dsn)))
    return scene_service, (
        create_scene_partition_router(scene_service),
        create_datasets_router(),
        create_ingest_runs_router(ingest_service),
    )

async def handle_grid_core_error(request: Request, exc: GridCoreError):
    status_code = 400
    if isinstance(exc, ValidationError):
        status_code = 422
    elif isinstance(exc, NotImplementedCapabilityError):
        status_code = 501
    return error_response(request, status_code=status_code, code=exc.code, message=exc.message)


async def handle_not_found_error(request: Request, exc: Exception) -> JSONResponse:
    code = {
        DatasetNotFound: "dataset_not_found",
        OutputVersionNotFound: "output_version_not_found",
        QualityRunNotFound: "quality_run_not_found",
        ManagedDatasetNotFound: "dataset_not_found",
        ManagedSceneNotFound: "scene_not_found",
    }.get(type(exc), "resource_not_found")
    return error_response(request, status_code=404, code=code, message=str(exc) or "资源不存在")


async def handle_conflict_error(request: Request, exc: Exception) -> JSONResponse:
    code = {
        OutputVersionNotCompleted: "output_version_not_completed",
        QualityTriggerConflict: "quality_trigger_conflict",
        PartitionBatchAlreadyActiveError: "partition_batch_active",
        PartitionBatchArchivedError: "partition_batch_archived",
        PartitionBatchNotRequeueableError: "partition_batch_not_requeueable",
        DatasetManagementConflict: "dataset_action_conflict",
    }.get(type(exc), "operation_conflict")
    return error_response(request, status_code=409, code=code, message=str(exc) or "当前状态不允许执行此操作")


async def handle_http_exception(request: Request, exc: HTTPException) -> JSONResponse:
    if exc.status_code >= 500:
        code = {
            502: "bad_gateway",
            503: "service_unavailable",
        }.get(exc.status_code, "internal_error")
        message = {
            502: "上游服务暂时不可用，请稍后重试",
            503: "服务暂时不可用，请稍后重试",
        }.get(exc.status_code, "服务器内部错误，请稍后重试")
        return error_response(
            request,
            status_code=exc.status_code,
            code=code,
            message=message,
            headers=exc.headers,
        )
    return error_response(
        request,
        status_code=exc.status_code,
        code=detail_code(exc.status_code, exc.detail),
        message=detail_message(exc.detail, "请求失败"),
        detail=exc.detail,
        headers=exc.headers,
    )


async def handle_validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
    details = jsonable_encoder(exc.errors())
    return error_response(
        request,
        status_code=422,
        code="validation_error",
        message=detail_message(details, "请求参数校验失败"),
        detail=details,
    )


async def handle_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
    request_id = ensure_request_id(request)
    logger.exception(
        "Unhandled API exception request_id=%s method=%s path=%s",
        request_id,
        request.method,
        request.url.path,
    )
    return error_response(
        request,
        status_code=500,
        code="internal_error",
        message=f"服务器内部错误，请稍后重试（请求 ID：{request_id}）",
    )


app = create_app()
