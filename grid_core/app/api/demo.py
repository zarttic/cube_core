from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import HTMLResponse, PlainTextResponse

from grid_core.app.models.request import (
    BatchCodeToGeometryRequest,
    ChildrenRequest,
    CodeToGeometryRequest,
    CoverRequest,
    LocateRequest,
    NeighborsRequest,
    ParentRequest,
)
from grid_core.app.models.response import (
    BatchGeometryResponse,
    ChildrenResponse,
    CoverResponse,
    GeometryResponse,
    LocateResponse,
    NeighborsResponse,
    ParentResponse,
)
from grid_core.app.services.grid_service import GridService
from grid_core.app.services.topology_service import TopologyService

router = APIRouter(prefix="/demo", tags=["demo"])
grid_service = GridService()
topology_service = TopologyService()
INDEX_HTML = Path(__file__).resolve().parents[2] / "web" / "index.html"
ENCODING_HTML = Path(__file__).resolve().parents[2] / "web" / "encoding.html"
PARTITION_HTML = Path(__file__).resolve().parents[2] / "web" / "partition.html"
SCRIPT_JS = Path(__file__).resolve().parents[2] / "web" / "script.js"
STYLES_CSS = Path(__file__).resolve().parents[2] / "web" / "styles.css"


@router.get("/map", response_class=HTMLResponse)
def map_page() -> HTMLResponse:
    return HTMLResponse(INDEX_HTML.read_text(encoding="utf-8"))


@router.get("/", response_class=HTMLResponse)
def demo_home() -> HTMLResponse:
    return HTMLResponse(INDEX_HTML.read_text(encoding="utf-8"))


@router.get("/index.html", response_class=HTMLResponse)
def demo_index_page() -> HTMLResponse:
    return HTMLResponse(INDEX_HTML.read_text(encoding="utf-8"))


@router.get("/encoding", response_class=HTMLResponse)
def encoding_page() -> HTMLResponse:
    return HTMLResponse(ENCODING_HTML.read_text(encoding="utf-8"))


@router.get("/encoding.html", response_class=HTMLResponse)
def encoding_html_page() -> HTMLResponse:
    return HTMLResponse(ENCODING_HTML.read_text(encoding="utf-8"))


@router.get("/partition", response_class=HTMLResponse)
def partition_page() -> HTMLResponse:
    return HTMLResponse(PARTITION_HTML.read_text(encoding="utf-8"))


@router.get("/partition.html", response_class=HTMLResponse)
def partition_html_page() -> HTMLResponse:
    return HTMLResponse(PARTITION_HTML.read_text(encoding="utf-8"))


@router.get("/script.js", response_class=PlainTextResponse)
def demo_script() -> PlainTextResponse:
    return PlainTextResponse(SCRIPT_JS.read_text(encoding="utf-8"), media_type="application/javascript")


@router.get("/styles.css", response_class=PlainTextResponse)
def demo_styles() -> PlainTextResponse:
    return PlainTextResponse(STYLES_CSS.read_text(encoding="utf-8"), media_type="text/css")


@router.post("/sdk/locate", response_model=LocateResponse)
def sdk_locate(req: LocateRequest) -> LocateResponse:
    cell = grid_service.locate(grid_type=req.grid_type, level=req.level, point=req.point)
    return LocateResponse(cell=cell)


@router.post("/sdk/cover", response_model=CoverResponse)
def sdk_cover(req: CoverRequest) -> CoverResponse:
    cells = grid_service.cover(
        grid_type=req.grid_type,
        level=req.level,
        geometry=req.geometry,
        bbox=req.bbox,
        cover_mode=req.cover_mode.value,
        boundary_type=req.boundary_type,
        crs=req.crs,
    )
    return CoverResponse(
        grid_type=req.grid_type.value,
        level=req.level,
        cover_mode=req.cover_mode.value,
        cells=cells,
        statistics={"cell_count": len(cells)},
    )


@router.post("/sdk/topology/neighbors", response_model=NeighborsResponse)
def sdk_neighbors(req: NeighborsRequest) -> NeighborsResponse:
    codes = topology_service.neighbors(grid_type=req.grid_type, code=req.code, k=req.k)
    return NeighborsResponse(result_codes=codes, statistics={"count": len(codes)})


@router.post("/sdk/topology/geometry", response_model=GeometryResponse)
def sdk_code_to_geometry(req: CodeToGeometryRequest) -> GeometryResponse:
    geometry = topology_service.code_to_geometry(req.grid_type, req.code, req.boundary_type)
    return GeometryResponse(geometry=geometry)


@router.post("/sdk/topology/geometries", response_model=BatchGeometryResponse)
def sdk_codes_to_geometries(req: BatchCodeToGeometryRequest) -> BatchGeometryResponse:
    geometries = topology_service.codes_to_geometries(req.grid_type, req.codes, req.boundary_type)
    return BatchGeometryResponse(geometries=geometries, statistics={"count": len(geometries)})


@router.post("/sdk/topology/parent", response_model=ParentResponse)
def sdk_parent(req: ParentRequest) -> ParentResponse:
    code = topology_service.parent(req.grid_type, req.code)
    return ParentResponse(parent_code=code)


@router.post("/sdk/topology/children", response_model=ChildrenResponse)
def sdk_children(req: ChildrenRequest) -> ChildrenResponse:
    codes = topology_service.children(req.grid_type, req.code, req.target_level)
    return ChildrenResponse(child_codes=codes, statistics={"count": len(codes)})
