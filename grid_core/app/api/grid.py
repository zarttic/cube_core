from fastapi import APIRouter

from grid_core.app.models.request import CoverRequest, LocateRequest
from grid_core.app.models.response import CoverResponse, LocateResponse
from grid_core.app.services.grid_service import GridService

router = APIRouter(prefix="/grid", tags=["grid"])
service = GridService()


@router.post("/locate", response_model=LocateResponse)
def locate(req: LocateRequest) -> LocateResponse:
    cell = service.locate(grid_type=req.grid_type, level=req.level, point=req.point)
    return LocateResponse(cell=cell)


@router.post("/cover", response_model=CoverResponse)
def cover(req: CoverRequest) -> CoverResponse:
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
