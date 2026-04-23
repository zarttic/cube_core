from fastapi import APIRouter

from grid_core.app.models.request import BatchCodeToGeometryRequest, ChildrenRequest, CodeToGeometryRequest, NeighborsRequest, ParentRequest
from grid_core.app.models.response import BatchGeometryResponse, ChildrenResponse, GeometryResponse, NeighborsResponse, ParentResponse
from grid_core.app.services.topology_service import TopologyService

router = APIRouter(prefix="/topology", tags=["topology"])
service = TopologyService()


@router.post("/neighbors", response_model=NeighborsResponse)
def neighbors(req: NeighborsRequest) -> NeighborsResponse:
    codes = service.neighbors(grid_type=req.grid_type, code=req.code, k=req.k)
    return NeighborsResponse(result_codes=codes, statistics={"count": len(codes)})


@router.post("/geometry", response_model=GeometryResponse)
def code_to_geometry(req: CodeToGeometryRequest) -> GeometryResponse:
    geometry = service.code_to_geometry(req.grid_type, req.code, req.boundary_type)
    return GeometryResponse(geometry=geometry)


@router.post("/geometries", response_model=BatchGeometryResponse)
def codes_to_geometries(req: BatchCodeToGeometryRequest) -> BatchGeometryResponse:
    geometries = service.codes_to_geometries(req.grid_type, req.codes, req.boundary_type)
    return BatchGeometryResponse(geometries=geometries, statistics={"count": len(geometries)})


@router.post("/parent", response_model=ParentResponse)
def parent(req: ParentRequest) -> ParentResponse:
    parent_code = service.parent(req.grid_type, req.code)
    return ParentResponse(parent_code=parent_code)


@router.post("/children", response_model=ChildrenResponse)
def children(req: ChildrenRequest) -> ChildrenResponse:
    child_codes = service.children(req.grid_type, req.code, req.target_level)
    return ChildrenResponse(child_codes=child_codes, statistics={"count": len(child_codes)})
