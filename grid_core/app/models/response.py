from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from grid_core.app.models.grid_cell import GridCell


class ApiError(BaseModel):
    code: str
    message: str


class BaseResponse(BaseModel):
    warnings: list[str] = Field(default_factory=list)
    statistics: dict[str, Any] = Field(default_factory=dict)


class LocateResponse(BaseResponse):
    cell: GridCell


class CoverResponse(BaseResponse):
    grid_type: str
    level: int
    cover_mode: str
    cells: list[GridCell]


class STCodeGenerateResponse(BaseResponse):
    st_code: str


class STCodeBatchGenerateResponse(BaseResponse):
    st_codes: list[str]


class STCodeParseResponse(BaseResponse):
    grid_type: str
    level: int
    space_code: str
    time_code: str
    version: str


class NeighborsResponse(BaseResponse):
    result_codes: list[str]


class GeometryResponse(BaseResponse):
    geometry: dict[str, Any]


class BatchGeometryResponse(BaseResponse):
    geometries: dict[str, dict[str, Any]]


class ParentResponse(BaseResponse):
    parent_code: str


class ChildrenResponse(BaseResponse):
    child_codes: list[str]
