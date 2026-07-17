from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Lock
from typing import Any

from fastapi import APIRouter, FastAPI, Query, Request
from fastapi.routing import APIRoute
from fastapi.responses import JSONResponse
from grid_core.sdk import CubeEncoderSDK, GridCoreError, NotImplementedCapabilityError, ValidationError

from cube_web.routes import partition as partition_route
from cube_web.routes.auth import create_auth_router, require_auth_for_api
from cube_web.routes.config import create_config_router
from cube_web.routes.partition import create_partition_router
from cube_web.routes.partition_datasets import create_partition_datasets_router
from cube_web.routes.quality import create_quality_router
from cube_web.routes.sdk import create_sdk_router
from cube_web.services import health_service
from cube_web.services.m6_runtime import M6RuntimePolicy, m6_runtime_policy
from cube_web.services.quality_worker import QualityRuntime

ENCODER_SDK_CLASS = CubeEncoderSDK
logger = logging.getLogger(__name__)


def _repo_root():
    return Path(__file__).resolve().parents[2]


create_partition_service = partition_route.create_partition_service
partition_service = partition_route.partition_service
legacy_partition_service = partition_route.legacy_partition_service
partition_workflow_service = partition_route.partition_workflow_service


def create_app(*, m6_mode: str | None = None) -> FastAPI:
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
    m6_policy = m6_runtime_policy(m6_mode)
    scene_domain_service = None
    m6_routers: tuple[APIRouter, ...] = ()
    if m6_policy.expose_m6_reads:
        scene_domain_service, m6_routers = _build_m6_components()

    web_app.middleware("http")(require_auth_for_api)
    web_app.add_exception_handler(GridCoreError, handle_grid_core_error)

    @web_app.get("/")
    async def root() -> dict[str, str]:
        return {"service": "cube-web", "status": "ok"}

    @web_app.get("/health")
    async def health(
        checks: list[str] | None = Query(default=None),
        check: list[str] | None = Query(default=None),
    ) -> dict[str, Any]:
        return health_service.health_report([*(checks or []), *(check or [])])

    api_router.include_router(create_sdk_router(sdk))
    api_router.include_router(create_quality_router())
    api_router.include_router(create_config_router())
    api_router.include_router(
        create_partition_router(scene_service=scene_domain_service if m6_policy.use_m6_import else None)
    )
    api_router.include_router(create_partition_datasets_router())
    _include_m6_routers(api_router, m6_routers, m6_policy)
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


def _build_m6_components() -> tuple[Any, tuple[APIRouter, ...]]:
    from cube_split import runtime_config

    from cube_web.routes.m6_datasets import create_m6_datasets_router
    from cube_web.routes.m6_ingest_runs import create_m6_ingest_runs_router
    from cube_web.routes.m6_scene import create_m6_scene_router
    from cube_web.services.m6_ingest_repository import OpenGaussIngestRepository
    from cube_web.services.m6_ingest_service import IngestRunService
    from cube_web.services.m6_scene_repository import OpenGaussSceneRepository
    from cube_web.services.m6_scene_service import SceneDomainService

    dsn = runtime_config.postgres_dsn()
    scene_service = SceneDomainService(OpenGaussSceneRepository(dsn), partition_workflow_service)
    ingest_service = IngestRunService(_LazyRepository(lambda: OpenGaussIngestRepository(dsn)))
    return scene_service, (
        create_m6_scene_router(scene_service),
        create_m6_datasets_router(),
        create_m6_ingest_runs_router(ingest_service),
    )


def _include_m6_routers(
    api_router: APIRouter,
    routers: tuple[APIRouter, ...],
    policy: M6RuntimePolicy,
) -> None:
    for router in routers:
        if policy.expose_m6_writes:
            api_router.include_router(router)
            continue
        read_router = APIRouter()
        read_router.routes.extend(
            route
            for route in router.routes
            if isinstance(route, APIRoute) and route.methods <= {"GET", "HEAD"}
        )
        api_router.include_router(read_router)


async def handle_grid_core_error(_: Request, exc: GridCoreError):
    status_code = 400
    if isinstance(exc, ValidationError):
        status_code = 422
    elif isinstance(exc, NotImplementedCapabilityError):
        status_code = 501
    return JSONResponse(status_code=status_code, content={"error": {"code": exc.code, "message": exc.message}})


app = create_app()
