from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, Field

from grid_core.app.models.grid_address import GridAddress
from grid_core.app.models.grid_cell import GridCell


class ApiError(BaseModel):
    code: str
    message: str


class BaseResponse(BaseModel):
    warnings: List[str] = Field(default_factory=list)
    statistics: Dict[str, Any] = Field(default_factory=dict)


class LocateResponse(BaseResponse):
    cell: GridCell


class CoverResponse(BaseResponse):
    grid_type: str
    requested_grid_level: int
    cover_mode: str
    cells: List[GridCell]


class STCodeGenerateResponse(BaseResponse):
    st_code: str


class STCodeBatchGenerateResponse(BaseResponse):
    st_codes: List[str]


class STCodeParseResponse(BaseResponse):
    grid_type: str
    grid_level: int
    space_code: str
    time_code: str


class NeighborsResponse(BaseResponse):
    addresses: List[GridAddress]


class GeometryResponse(BaseResponse):
    geometry: Dict[str, Any]


class BatchGeometryResponse(BaseResponse):
    geometries: Dict[str, Dict[str, Any]]


class ParentResponse(BaseResponse):
    address: GridAddress


class ChildrenResponse(BaseResponse):
    addresses: List[GridAddress]
