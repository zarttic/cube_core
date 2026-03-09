from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from grid_core.app.models.request import CoverRequest, LocateRequest
from grid_core.app.models.response import CoverResponse, LocateResponse
from grid_core.app.services.grid_service import GridService

router = APIRouter(prefix="/demo", tags=["demo"])
service = GridService()
INDEX_HTML = Path(__file__).resolve().parents[2] / "web" / "index.html"


@router.get("/map", response_class=HTMLResponse)
def map_page() -> HTMLResponse:
    return HTMLResponse(INDEX_HTML.read_text(encoding="utf-8"))


@router.post("/sdk/locate", response_model=LocateResponse)
def sdk_locate(req: LocateRequest) -> LocateResponse:
    cell = service.locate(grid_type=req.grid_type, level=req.level, point=req.point)
    return LocateResponse(cell=cell)


@router.post("/sdk/cover", response_model=CoverResponse)
def sdk_cover(req: CoverRequest) -> CoverResponse:
    cells = service.cover(
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
