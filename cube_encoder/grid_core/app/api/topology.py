from fastapi import APIRouter

from grid_core.app.models.request import BatchAddressRequest, ChildrenRequest, CodeToGeometryRequest, NeighborsRequest, ParentRequest
from grid_core.app.models.response import BatchGeometryResponse, ChildrenResponse, GeometryResponse, NeighborsResponse, ParentResponse
from grid_core.app.services.topology_service import TopologyService

router = APIRouter(prefix="/topology", tags=["topology"])
service = TopologyService()


@router.post("/neighbors", response_model=NeighborsResponse)
def neighbors(req: NeighborsRequest) -> NeighborsResponse:
    addresses = service.neighbors(req.address, k=req.k)
    return NeighborsResponse(addresses=addresses, statistics={"count": len(addresses)})


@router.post("/geometry", response_model=GeometryResponse)
def code_to_geometry(req: CodeToGeometryRequest) -> GeometryResponse:
    geometry = service.code_to_geometry(req.address, req.boundary_type)
    return GeometryResponse(geometry=geometry)


@router.post("/geometries", response_model=BatchGeometryResponse)
def codes_to_geometries(req: BatchAddressRequest) -> BatchGeometryResponse:
    geometries = service.codes_to_geometries(req.addresses, req.boundary_type)
    return BatchGeometryResponse(geometries=geometries, statistics={"count": len(geometries)})


@router.post("/parent", response_model=ParentResponse)
def parent(req: ParentRequest) -> ParentResponse:
    address = service.parent(req.address)
    return ParentResponse(address=address)


@router.post("/children", response_model=ChildrenResponse)
def children(req: ChildrenRequest) -> ChildrenResponse:
    addresses = service.children(req.address, req.target_grid_level)
    return ChildrenResponse(addresses=addresses, statistics={"count": len(addresses)})
