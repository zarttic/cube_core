from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.responses import JSONResponse
from grid_core.sdk import CubeEncoderSDK
from grid_core.app.core.exceptions import GridCoreError, NotImplementedCapabilityError, ValidationError
from grid_core.app.models.request import (
    BatchCodeToGeometryRequest,
    ChildrenRequest,
    CodeToGeometryRequest,
    CoverRequest,
    LocateRequest,
    NeighborsRequest,
    ParentRequest,
    STCodeBatchGenerateRequest,
    STCodeGenerateRequest,
    STCodeParseRequest,
)
from grid_core.app.models.response import (
    BatchGeometryResponse,
    ChildrenResponse,
    CoverResponse,
    GeometryResponse,
    LocateResponse,
    NeighborsResponse,
    ParentResponse,
    STCodeBatchGenerateResponse,
    STCodeGenerateResponse,
    STCodeParseResponse,
)

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


def _resolve_web_file(path_name: str) -> Path:
    candidate = WEB_DIR / path_name
    if candidate.exists() and candidate.is_file():
        return candidate

    if "." not in path_name:
        html_candidate = WEB_DIR / f"{path_name}.html"
        if html_candidate.exists() and html_candidate.is_file():
            return html_candidate

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


@api_router.post("/grid/locate", response_model=LocateResponse)
def locate(req: LocateRequest) -> LocateResponse:
    cell = sdk.locate(grid_type=req.grid_type, level=req.level, point=req.point)
    return LocateResponse(cell=cell)


@api_router.post("/grid/cover", response_model=CoverResponse)
def cover(req: CoverRequest) -> CoverResponse:
    cells = sdk.cover(
        grid_type=req.grid_type,
        level=req.level,
        cover_mode=req.cover_mode,
        boundary_type=req.boundary_type,
        geometry=req.geometry,
        bbox=req.bbox,
        crs=req.crs,
    )
    return CoverResponse(
        grid_type=req.grid_type.value,
        level=req.level,
        cover_mode=req.cover_mode.value,
        cells=cells,
        statistics={"cell_count": len(cells)},
    )


@api_router.post("/topology/neighbors", response_model=NeighborsResponse)
def neighbors(req: NeighborsRequest) -> NeighborsResponse:
    result_codes = sdk.neighbors(grid_type=req.grid_type, code=req.code, k=req.k)
    return NeighborsResponse(result_codes=result_codes, statistics={"count": len(result_codes)})


@api_router.post("/topology/geometry", response_model=GeometryResponse)
def code_to_geometry(req: CodeToGeometryRequest) -> GeometryResponse:
    geometry = sdk.code_to_geometry(grid_type=req.grid_type, code=req.code, boundary_type=req.boundary_type)
    return GeometryResponse(geometry=geometry)


@api_router.post("/topology/geometries", response_model=BatchGeometryResponse)
def codes_to_geometries(req: BatchCodeToGeometryRequest) -> BatchGeometryResponse:
    geometries = sdk.codes_to_geometries(grid_type=req.grid_type, codes=req.codes, boundary_type=req.boundary_type)
    return BatchGeometryResponse(geometries=geometries, statistics={"count": len(geometries)})


@api_router.post("/topology/parent", response_model=ParentResponse)
def parent(req: ParentRequest) -> ParentResponse:
    parent_code = sdk.parent(grid_type=req.grid_type, code=req.code)
    return ParentResponse(parent_code=parent_code)


@api_router.post("/topology/children", response_model=ChildrenResponse)
def children(req: ChildrenRequest) -> ChildrenResponse:
    child_codes = sdk.children(grid_type=req.grid_type, code=req.code, target_level=req.target_level)
    return ChildrenResponse(child_codes=child_codes, statistics={"count": len(child_codes)})


@api_router.post("/code/st", response_model=STCodeGenerateResponse)
def generate_st(req: STCodeGenerateRequest) -> STCodeGenerateResponse:
    result = sdk.generate_st_code(
        grid_type=req.grid_type,
        level=req.level,
        space_code=req.space_code,
        timestamp=req.timestamp,
        time_granularity=req.time_granularity,
        version=req.version,
    )
    return STCodeGenerateResponse(st_code=result.st_code)


@api_router.post("/code/parse", response_model=STCodeParseResponse)
def parse_st(req: STCodeParseRequest) -> STCodeParseResponse:
    result = sdk.parse_st_code(req.st_code)
    return STCodeParseResponse(
        grid_type=result.grid_type,
        level=result.level,
        space_code=result.space_code,
        time_code=result.time_code,
        version=result.version,
    )


@api_router.post("/code/st/batch", response_model=STCodeBatchGenerateResponse)
def batch_generate_st(req: STCodeBatchGenerateRequest) -> STCodeBatchGenerateResponse:
    st_codes = sdk.batch_generate_st_codes(
        grid_type=req.grid_type,
        level=req.level,
        items=[{"space_code": item.space_code, "timestamp": item.timestamp} for item in req.items],
        time_granularity=req.time_granularity,
        version=req.version,
    )
    return STCodeBatchGenerateResponse(st_codes=st_codes, statistics={"count": len(st_codes)})


app.include_router(api_router)


@app.get("/")
def home() -> FileResponse:
    return FileResponse(WEB_DIR / "index.html", media_type="text/html")


@app.get("/{path_name:path}")
def serve_web_asset(path_name: str) -> FileResponse:
    file_path = _resolve_web_file(path_name)
    media_type = STATIC_MEDIA_TYPES.get(file_path.suffix, "text/html")
    return FileResponse(file_path, media_type=media_type)
