from __future__ import annotations

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse
from grid_core.app.core.exceptions import GridCoreError, NotImplementedCapabilityError, ValidationError
from grid_core.sdk import CubeEncoderSDK

from cube_web.routes import partition as partition_route
from cube_web.routes.auth import create_auth_router, require_auth_for_api
from cube_web.routes.config import create_config_router
from cube_web.routes.ingest import create_ingest_router
from cube_web.routes.pages import create_pages_router
from cube_web.routes.partition import create_partition_router
from cube_web.routes.quality import create_quality_router
from cube_web.routes.sdk import create_sdk_router
from cube_web.services import quality_service

ENCODER_SDK_CLASS = CubeEncoderSDK


def _repo_root():
    return quality_service.repo_root()


create_partition_service = partition_route.create_partition_service
partition_service = partition_route.partition_service
partition_workflow_service = partition_route.partition_workflow_service


def create_app() -> FastAPI:
    web_app = FastAPI(title="cube-web")
    sdk = CubeEncoderSDK()
    api_router = APIRouter(prefix="/v1", tags=["sdk-web"])

    web_app.middleware("http")(require_auth_for_api)
    web_app.add_exception_handler(GridCoreError, handle_grid_core_error)

    @web_app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    api_router.include_router(create_sdk_router(sdk))
    api_router.include_router(create_quality_router())
    api_router.include_router(create_ingest_router())
    api_router.include_router(create_config_router())
    api_router.include_router(create_partition_router())
    web_app.include_router(api_router)
    web_app.include_router(create_auth_router())
    web_app.include_router(create_pages_router())
    return web_app


async def handle_grid_core_error(_: Request, exc: GridCoreError):
    status_code = 400
    if isinstance(exc, ValidationError):
        status_code = 422
    elif isinstance(exc, NotImplementedCapabilityError):
        status_code = 501
    return JSONResponse(status_code=status_code, content={"error": {"code": exc.code, "message": exc.message}})


app = create_app()
