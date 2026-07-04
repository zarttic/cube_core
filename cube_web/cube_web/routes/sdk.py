from __future__ import annotations

from fastapi import APIRouter
from fastapi import HTTPException
from grid_core.sdk import (
    BatchCodeToGeometryRequest,
    BatchGeometryResponse,
    ChildrenRequest,
    ChildrenResponse,
    CodeToGeometryRequest,
    CoverRequest,
    CoverResponse,
    CubeEncoderSDK,
    GeometryResponse,
    LocateRequest,
    LocateResponse,
    NeighborsRequest,
    NeighborsResponse,
    ParentRequest,
    ParentResponse,
    STCodeBatchGenerateRequest,
    STCodeBatchGenerateResponse,
    STCodeGenerateRequest,
    STCodeGenerateResponse,
    STCodeParseRequest,
    STCodeParseResponse,
)
from cube_split.read.carbon_query import query_carbon_observations
from cube_web.schemas import SpatiotemporalQueryRequest


def create_sdk_router(sdk: CubeEncoderSDK) -> APIRouter:
    router = APIRouter(tags=["sdk-web"])

    @router.post("/grid/locate", response_model=LocateResponse)
    def locate(req: LocateRequest) -> LocateResponse:
        cell = sdk.locate(grid_type=req.grid_type, level=req.level, point=req.point)
        return LocateResponse(cell=cell)

    @router.post("/grid/cover", response_model=CoverResponse)
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

    @router.post("/topology/neighbors", response_model=NeighborsResponse)
    def neighbors(req: NeighborsRequest) -> NeighborsResponse:
        result_codes = sdk.neighbors(grid_type=req.grid_type, code=req.code, k=req.k)
        return NeighborsResponse(result_codes=result_codes, statistics={"count": len(result_codes)})

    @router.post("/topology/geometry", response_model=GeometryResponse)
    def code_to_geometry(req: CodeToGeometryRequest) -> GeometryResponse:
        geometry = sdk.code_to_geometry(grid_type=req.grid_type, code=req.code, boundary_type=req.boundary_type)
        return GeometryResponse(geometry=geometry)

    @router.post("/topology/geometries", response_model=BatchGeometryResponse)
    def codes_to_geometries(req: BatchCodeToGeometryRequest) -> BatchGeometryResponse:
        geometries = sdk.codes_to_geometries(grid_type=req.grid_type, codes=req.codes, boundary_type=req.boundary_type)
        return BatchGeometryResponse(geometries=geometries, statistics={"count": len(geometries)})

    @router.post("/topology/parent", response_model=ParentResponse)
    def parent(req: ParentRequest) -> ParentResponse:
        parent_code = sdk.parent(grid_type=req.grid_type, code=req.code)
        return ParentResponse(parent_code=parent_code)

    @router.post("/topology/children", response_model=ChildrenResponse)
    def children(req: ChildrenRequest) -> ChildrenResponse:
        child_codes = sdk.children(grid_type=req.grid_type, code=req.code, target_level=req.target_level)
        return ChildrenResponse(child_codes=child_codes, statistics={"count": len(child_codes)})

    @router.post("/code/st", response_model=STCodeGenerateResponse)
    def generate_st(req: STCodeGenerateRequest) -> STCodeGenerateResponse:
        result = sdk.generate_st_code(
            grid_type=req.grid_type,
            level=req.level,
            space_code=req.space_code,
            timestamp=req.timestamp,
            time_granularity=req.time_granularity,
        )
        return STCodeGenerateResponse(st_code=result.st_code)

    @router.post("/code/parse", response_model=STCodeParseResponse)
    def parse_st(req: STCodeParseRequest) -> STCodeParseResponse:
        result = sdk.parse_st_code(req.st_code)
        return STCodeParseResponse(
            grid_type=result.grid_type,
            level=result.level,
            space_code=result.space_code,
            time_code=result.time_code,
        )

    @router.post("/code/st/batch", response_model=STCodeBatchGenerateResponse)
    def batch_generate_st(req: STCodeBatchGenerateRequest) -> STCodeBatchGenerateResponse:
        st_codes = sdk.batch_generate_st_codes(
            grid_type=req.grid_type,
            level=req.level,
            items=[{"space_code": item.space_code, "timestamp": item.timestamp} for item in req.items],
            time_granularity=req.time_granularity,
        )
        return STCodeBatchGenerateResponse(st_codes=st_codes, statistics={"count": len(st_codes)})

    @router.post("/query/st")
    def spatiotemporal_query(req: SpatiotemporalQueryRequest) -> dict:
        bbox = req.bbox
        if bbox is None:
            if not req.point:
                raise HTTPException(status_code=422, detail="point or bbox is required")
            lon, lat = map(float, req.point[:2])
            epsilon = 0.00001
            bbox = [lon - epsilon, lat - epsilon, lon + epsilon, lat + epsilon]
        rows = query_carbon_observations(
            bbox=[float(value) for value in bbox],
            time_start=req.time_start,
            time_end=req.time_end,
            quality_flags=req.quality_flags,
            product_type=req.product_type,
            grid_type=req.grid_type,
            grid_level=req.grid_level,
            cube_version=req.cube_version,
            limit=req.limit,
        )
        return {
            "data_type": req.data_type,
            "query_mode": "point" if req.point is not None and req.bbox is None else "bbox",
            "grid_type": req.grid_type,
            "grid_level": req.grid_level,
            "time_start": req.time_start,
            "time_end": req.time_end,
            "statistics": {"count": len(rows)},
            "items": rows,
        }

    return router
