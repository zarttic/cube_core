from __future__ import annotations

from pathlib import Path
from urllib.parse import urlencode

from fastapi import APIRouter, FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response

from cube_web.routes.config import create_config_router
from cube_web.routes.ingest import create_ingest_router
from cube_web.routes.partition import create_partition_router
from cube_web.routes.quality import create_quality_router
from cube_web.routes.sdk import create_sdk_router
from cube_web.schemas import PartitionDemoRequest, PartitionRetryRequest, payload_from_model
from cube_web.services import auth_service, partition_runners, quality_checks, quality_service
from cube_web.services.partition_service import PartitionService, build_partition_registry
from cube_web.services.quality_pdf import quality_report_pdf_response
from cube_web.services.quality_report_store import get_quality_report_store
from grid_core.app.core.exceptions import GridCoreError, NotImplementedCapabilityError, ValidationError
from grid_core.sdk import CubeEncoderSDK

WEB_DIR = Path(__file__).resolve().parent / "web"
STATIC_MEDIA_TYPES = {
    ".css": "text/css",
    ".js": "application/javascript",
}

# Importing the SDK here makes cube_web explicitly depend on the installed
# cube_encoder package instead of only depending on its HTTP API shape.
ENCODER_SDK_CLASS = CubeEncoderSDK

app = FastAPI(title="cube-web")
api_router = APIRouter(prefix="/v1", tags=["sdk-web"])
auth_router = APIRouter(prefix="/api", tags=["auth"])
sdk = CubeEncoderSDK()


def _repo_root() -> Path:
    return quality_service.repo_root()


_quality_args = quality_service.quality_args

_demo_run_dir = partition_runners._demo_run_dir
_demo_task_metadata = partition_runners._demo_task_metadata
_optical_demo_input_dir = partition_runners._optical_demo_input_dir
_resolve_optical_demo_source = partition_runners._resolve_optical_demo_source
_selected_optical_manifest_assets = partition_runners._selected_optical_manifest_assets
_int_payload_value = partition_runners._int_payload_value
_warn_checks_from_result = partition_runners._warn_checks_from_result
_warning_asset_paths = partition_runners._warning_asset_paths
_asset_matches_warning_path = partition_runners._asset_matches_warning_path
_retry_payload_for_warning_assets = partition_runners._retry_payload_for_warning_assets

run_optical_quality_check = quality_checks.run_optical_quality_check
run_product_quality_check = quality_checks.run_product_quality_check
run_carbon_quality_check = quality_checks.run_carbon_quality_check


def _run_carbon_partition_demo() -> dict:
    return partition_runners._run_carbon_partition_demo()


def _run_carbon_partition_test(payload: dict | None = None) -> dict:
    return partition_runners._run_carbon_partition_test(payload)


def _run_carbon_partition_retry(payload: dict | None = None) -> dict:
    return partition_runners._run_carbon_partition_retry(payload)


def _run_product_partition_demo(payload: dict | None = None, mode: str = "partition_demo") -> dict:
    return partition_runners._run_product_partition_demo(payload, mode=mode)


def _run_product_partition_test(payload: dict | None = None) -> dict:
    return partition_runners._run_product_partition_test(payload)


def _run_product_partition_retry(payload: dict | None = None) -> dict:
    request = (payload or {}).get("request") or {}
    request_payload = request.get("payload") if isinstance(request, dict) else {}
    if not isinstance(request_payload, dict):
        request_payload = {}
    result = _run_product_partition_demo(request_payload, mode="partition_retry")
    result["retry"] = {
        "strategy": "full_request",
        "warning_check_names": [],
        "warning_asset_count": 0,
        "retried_asset_count": 0,
    }
    return result


def _run_optical_partition_from_payload(payload: dict | None = None, mode: str = "partition_demo") -> dict:
    return partition_runners._run_optical_partition_from_payload(payload, mode=mode)


def _run_optical_partition_demo(payload: dict | None = None) -> dict:
    return partition_runners._run_optical_partition_demo(payload)


def _run_optical_partition_test(payload: dict | None = None) -> dict:
    return partition_runners._run_optical_partition_test(payload)


def _resolve_web_file(path_name: str) -> Path:
    candidate = WEB_DIR / path_name
    if candidate.exists() and candidate.is_file():
        return candidate

    if "." not in path_name:
        html_candidate = WEB_DIR / f"{path_name}.html"
        if html_candidate.exists() and html_candidate.is_file():
            return html_candidate
        index_candidate = WEB_DIR / "index.html"
        if index_candidate.exists() and index_candidate.is_file():
            return index_candidate

    if path_name in {"partition.html", "quality.html", "encoding.html", "config.html", "门户首页.html"}:
        index_candidate = WEB_DIR / "index.html"
        if index_candidate.exists() and index_candidate.is_file():
            return index_candidate

    raise HTTPException(status_code=404, detail=f"Page not found: {path_name}")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.middleware("http")
async def require_auth_for_api(request: Request, call_next):
    settings = auth_service.auth_settings()
    if settings.required and request.url.path.startswith("/v1/"):
        try:
            token = auth_service.bearer_token(request.headers.get("Authorization"))
            auth_service.verify_access_token(token, settings)
        except HTTPException as exc:
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
    return await call_next(request)


@auth_router.get("/config")
def auth_config() -> dict[str, str]:
    settings = auth_service.auth_settings()
    return {
        "client_id": settings.client_id,
        "redirect_uri": settings.redirect_uri,
        "main_system_url": settings.main_system_url,
    }


@auth_router.get("/callback")
def auth_callback(code: str, state: str | None = None) -> dict:
    token_response = auth_service.exchange_code_for_token(code)
    token = token_response.get("access_token") or token_response.get("token")
    if not token:
        raise HTTPException(status_code=502, detail="Auth service did not return access_token")
    return {
        "access_token": token,
        "token_type": token_response.get("token_type", "bearer"),
        "expires_in": token_response.get("expires_in"),
        "state": state,
    }


@auth_router.get("/verify")
def auth_verify(authorization: str | None = Header(default=None)) -> dict:
    token = auth_service.bearer_token(authorization)
    payload = auth_service.verify_access_token(token)
    return {"valid": True, "sub": payload.get("sub")}


@auth_router.get("/me")
def auth_me(authorization: str | None = Header(default=None)) -> dict:
    token = auth_service.bearer_token(authorization)
    return auth_service.user_info_from_token(token)


@auth_router.post("/logout")
def auth_logout(authorization: str | None = Header(default=None)) -> dict:
    token = None
    if authorization:
        token = auth_service.bearer_token(authorization)
    return auth_service.notify_logout(token)


@app.exception_handler(GridCoreError)
async def handle_grid_core_error(_: Request, exc: GridCoreError):
    status_code = 400
    if isinstance(exc, ValidationError):
        status_code = 422
    elif isinstance(exc, NotImplementedCapabilityError):
        status_code = 501
    return JSONResponse(status_code=status_code, content={"error": {"code": exc.code, "message": exc.message}})


def partition_carbon_demo() -> dict:
    try:
        return _run_carbon_partition_demo()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def partition_carbon_test(payload: PartitionDemoRequest | dict | None = None) -> dict:
    try:
        return _run_carbon_partition_test(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def partition_carbon_retry(payload: PartitionRetryRequest | dict | None = None) -> dict:
    try:
        return _run_carbon_partition_retry(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def partition_product_demo(payload: PartitionDemoRequest | dict | None = None) -> dict:
    try:
        return _run_product_partition_demo(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def partition_product_test(payload: PartitionDemoRequest | dict | None = None) -> dict:
    try:
        return _run_product_partition_test(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def partition_product_retry(payload: PartitionRetryRequest | dict | None = None) -> dict:
    try:
        return _run_product_partition_retry(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def partition_radar_demo(payload: PartitionDemoRequest | dict | None = None) -> dict:
    raise HTTPException(status_code=501, detail="Radar partition demo is not implemented")


def partition_radar_retry(payload: PartitionRetryRequest | dict | None = None) -> dict:
    raise HTTPException(status_code=501, detail="Radar partition retry is not implemented")


def partition_optical_demo(payload: PartitionDemoRequest | dict | None = None) -> dict:
    try:
        return _run_optical_partition_demo(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def partition_optical_test(payload: PartitionDemoRequest | dict | None = None) -> dict:
    try:
        return _run_optical_partition_test(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def partition_optical_retry(payload: PartitionRetryRequest | dict | None = None) -> dict:
    try:
        return _run_optical_partition_retry(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def _run_optical_partition_retry(payload: dict | None = None) -> dict:
    payload = payload or {}
    request = payload.get("request") or {}
    if not isinstance(request, dict):
        raise ValueError("request must be an object")
    last_result = payload.get("last_result") or {}
    if not isinstance(last_result, dict):
        raise ValueError("last_result must be an object")

    request_payload = request.get("payload") or {}
    if not isinstance(request_payload, dict):
        raise ValueError("request.payload must be an object")
    warn_checks = _warn_checks_from_result(last_result)
    warning_paths = _warning_asset_paths(warn_checks)
    retry_payload, retried_asset_count = _retry_payload_for_warning_assets(request_payload, warning_paths)
    result = _run_optical_partition_from_payload(retry_payload, mode="partition_retry")
    result["retry"] = {
        "strategy": "warning_assets" if retried_asset_count else "full_request",
        "warning_check_names": [str(check.get("name")) for check in warn_checks],
        "warning_asset_count": len(warning_paths),
        "retried_asset_count": retried_asset_count,
    }
    return result


def quality_optical_latest(payload: dict | None = None) -> dict:
    payload_from_model(payload)
    report = get_quality_report_store().latest_report("optical")
    if report is None:
        raise HTTPException(status_code=404, detail="No optical quality report found")
    return report


def quality_optical_report(payload: dict) -> dict:
    payload = payload_from_model(payload)
    report_id = str(payload.get("report_id", "")).strip()
    if not report_id:
        raise HTTPException(status_code=422, detail="report_id is required")
    report = get_quality_report_store().get_report("optical", report_id)
    if report is None:
        raise HTTPException(status_code=404, detail=f"Optical quality report not found: {report_id}")
    return report


def quality_optical_report_pdf(payload: dict) -> Response:
    return quality_report_pdf_response(quality_optical_report(payload), data_type="optical")


def quality_optical_history(payload: dict | None = None) -> dict:
    payload = payload_from_model(payload)
    limit = _history_limit(payload)
    records = get_quality_report_store().list_reports("optical", limit=limit)
    return {"records": records, "count": len(records)}


def quality_product_history(payload: dict | None = None) -> dict:
    payload = payload_from_model(payload)
    limit = _history_limit(payload)
    records = get_quality_report_store().list_reports("product", limit=limit)
    return {"records": records, "count": len(records)}


def quality_carbon_latest(payload: dict | None = None) -> dict:
    payload_from_model(payload)
    report = get_quality_report_store().latest_report("carbon")
    if report is None:
        raise HTTPException(status_code=404, detail="No carbon quality report found")
    return report


def quality_carbon_report(payload: dict) -> dict:
    payload = payload_from_model(payload)
    report_id = str(payload.get("report_id", "")).strip()
    if not report_id:
        raise HTTPException(status_code=422, detail="report_id is required")
    report = get_quality_report_store().get_report("carbon", report_id)
    if report is None:
        raise HTTPException(status_code=404, detail=f"Carbon quality report not found: {report_id}")
    return report


def quality_carbon_report_pdf(payload: dict) -> Response:
    return quality_report_pdf_response(quality_carbon_report(payload), data_type="carbon")


def quality_carbon_history(payload: dict | None = None) -> dict:
    payload = payload_from_model(payload)
    limit = _history_limit(payload)
    records = get_quality_report_store().list_reports("carbon", limit=limit)
    return {"records": records, "count": len(records)}


def _history_limit(payload: dict) -> int:
    try:
        limit = int(payload.get("limit", 20) or 20)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="limit must be an integer") from None
    if limit <= 0:
        raise HTTPException(status_code=422, detail="limit must be greater than 0")
    return limit


partition_service = PartitionService(
    build_partition_registry(
        optical_demo=partition_optical_demo,
        optical_test=partition_optical_test,
        optical_retry=partition_optical_retry,
        carbon_demo=lambda payload=None: partition_carbon_demo(),
        carbon_test=partition_carbon_test,
        carbon_retry=partition_carbon_retry,
        product_demo=partition_product_demo,
        product_test=partition_product_test,
        product_retry=partition_product_retry,
    )
)
api_router.include_router(create_sdk_router(sdk))
api_router.include_router(create_quality_router())
api_router.include_router(create_ingest_router())
api_router.include_router(create_config_router())
api_router.include_router(create_partition_router(partition_service))
app.include_router(api_router)
app.include_router(auth_router)


@app.get("/callback")
def auth_callback_page(code: str | None = None, state: str | None = None):
    if code:
        query = {"code": code}
        if state:
            query["state"] = state
        return RedirectResponse(f"/?{urlencode(query)}")
    return FileResponse(WEB_DIR / "index.html", media_type="text/html")


@app.get("/")
def home() -> FileResponse:
    return FileResponse(WEB_DIR / "index.html", media_type="text/html")


@app.get("/{path_name:path}")
def serve_web_asset(path_name: str) -> FileResponse:
    file_path = _resolve_web_file(path_name)
    media_type = STATIC_MEDIA_TYPES.get(file_path.suffix, "text/html")
    return FileResponse(file_path, media_type=media_type)
