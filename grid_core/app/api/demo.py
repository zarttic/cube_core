from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from grid_core.app.models.request import ChildrenRequest, CodeToGeometryRequest, CoverRequest, LocateRequest, NeighborsRequest, ParentRequest
from grid_core.app.models.response import ChildrenResponse, CoverResponse, GeometryResponse, LocateResponse, NeighborsResponse, ParentResponse
from grid_core.app.services.grid_service import GridService
from grid_core.app.services.topology_service import TopologyService

router = APIRouter(prefix="/demo", tags=["demo"])
grid_service = GridService()
topology_service = TopologyService()
INDEX_HTML = Path(__file__).resolve().parents[2] / "web" / "index.html"


@router.get("/map", response_class=HTMLResponse)
def map_page() -> HTMLResponse:
    return HTMLResponse(INDEX_HTML.read_text(encoding="utf-8"))


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


@router.post("/sdk/topology/parent", response_model=ParentResponse)
def sdk_parent(req: ParentRequest) -> ParentResponse:
    code = topology_service.parent(req.grid_type, req.code)
    return ParentResponse(parent_code=code)


@router.post("/sdk/topology/children", response_model=ChildrenResponse)
def sdk_children(req: ChildrenRequest) -> ChildrenResponse:
    codes = topology_service.children(req.grid_type, req.code, req.target_level)
    return ChildrenResponse(child_codes=codes, statistics={"count": len(codes)})
