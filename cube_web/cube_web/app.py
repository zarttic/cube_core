from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response

from cube_web.routes.partition import create_partition_router
from cube_web.routes.quality import create_quality_router
from cube_web.routes.sdk import create_sdk_router
from cube_web.schemas import PartitionDemoRequest, PartitionRetryRequest, payload_from_model
from cube_web.services import partition_runners, quality_checks, quality_service
from cube_web.services.partition_service import PartitionService, build_partition_registry
from cube_web.services.quality_pdf import quality_report_pdf_response
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
sdk = CubeEncoderSDK()


def _repo_root() -> Path:
    return quality_service.repo_root()


# Compatibility wrappers for existing tests and direct module callers.
def _quality_run_dirs(data_type: str) -> list[Path]:
    return quality_service.quality_run_dirs(data_type)


def _allowed_quality_roots() -> list[Path]:
    return quality_service.allowed_quality_roots()


def _resolve_quality_run_dir(run_dir_text: str) -> Path:
    run_dir = Path(run_dir_text).expanduser().resolve()
    for root in _allowed_quality_roots():
        if run_dir == root or root in run_dir.parents:
            return run_dir
    roots = ", ".join(str(root) for root in _allowed_quality_roots())
    raise HTTPException(status_code=403, detail=f"run_dir must be under one of: {roots}")


def _optical_quality_run_dirs() -> list[Path]:
    return quality_service.optical_quality_run_dirs()


def _latest_optical_quality_run_dir() -> str:
    return quality_service.latest_optical_quality_run_dir()


def _latest_product_quality_run_dir() -> str:
    return quality_service.latest_product_quality_run_dir()


_quality_args = quality_service.quality_args
_quality_history_record = quality_service.quality_history_record
_read_quality_history_record = quality_service.read_quality_history_record
_read_quality_report = quality_service.read_quality_report

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


def _run_carbon_partition_demo() -> dict:
    return partition_runners._run_carbon_partition_demo()


def _run_carbon_partition_retry(payload: dict | None = None) -> dict:
    return partition_runners._run_carbon_partition_retry(payload)


def _run_product_partition_demo(payload: dict | None = None, mode: str = "partition_demo") -> dict:
    return partition_runners._run_product_partition_demo(payload, mode=mode)


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

    if path_name in {"partition.html", "quality.html", "encoding.html", "门户首页.html"}:
        index_candidate = WEB_DIR / "index.html"
        if index_candidate.exists() and index_candidate.is_file():
            return index_candidate

    raise HTTPException(status_code=404, detail=f"Page not found: {path_name}")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


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
    payload = payload_from_model(payload)
    run_dir = _latest_optical_quality_run_dir()
    cached_report = _read_quality_report(Path(run_dir), data_type="optical")
    if cached_report is not None:
        return cached_report
    if run_optical_quality_check is None:
        raise HTTPException(status_code=500, detail="cube_split quality module is not available")
    args = _quality_args(run_dir, payload)
    report = run_optical_quality_check(args)
    report["run_dir"] = run_dir
    return report


def quality_optical_report(payload: dict) -> dict:
    payload = payload_from_model(payload)
    run_dir_text = str(payload.get("run_dir", "")).strip()
    if not run_dir_text:
        raise HTTPException(status_code=422, detail="run_dir is required")
    run_dir = _resolve_quality_run_dir(run_dir_text)
    report = _read_quality_report(run_dir, data_type="optical")
    if report is None:
        raise HTTPException(status_code=404, detail=f"quality_report.json not found under run_dir: {run_dir}")
    return report


def quality_optical_report_pdf(payload: dict) -> Response:
    return quality_report_pdf_response(quality_optical_report(payload), data_type="optical")


def quality_optical_history(payload: dict | None = None) -> dict:
    payload = payload_from_model(payload)
    limit = _history_limit(payload)
    records: list[dict] = []
    for run_dir in _optical_quality_run_dirs():
        record = _read_quality_history_record(run_dir, data_type="optical")
        if record is None:
            continue
        records.append(record)
        if len(records) >= limit:
            break
    return {"records": records, "count": len(records)}


def quality_product_history(payload: dict | None = None) -> dict:
    payload = payload_from_model(payload)
    limit = _history_limit(payload)
    records: list[dict] = []
    for run_dir in _quality_run_dirs("product"):
        record = _read_quality_history_record(run_dir, data_type="product")
        if record is None:
            continue
        records.append(record)
        if len(records) >= limit:
            break
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
        carbon_retry=partition_carbon_retry,
        product_demo=partition_product_demo,
        product_retry=partition_product_retry,
    )
)
api_router.include_router(create_sdk_router(sdk))
api_router.include_router(create_quality_router())
api_router.include_router(create_partition_router(partition_service))
app.include_router(api_router)


@app.get("/")
def home() -> FileResponse:
    return FileResponse(WEB_DIR / "index.html", media_type="text/html")


@app.get("/{path_name:path}")
def serve_web_asset(path_name: str) -> FileResponse:
    file_path = _resolve_web_file(path_name)
    media_type = STATIC_MEDIA_TYPES.get(file_path.suffix, "text/html")
    return FileResponse(file_path, media_type=media_type)
