from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, model_validator

from grid_core.app.core.enums import BoundaryType, CoverMode, GridType, TimeGranularity


class LocateRequest(BaseModel):
    grid_type: GridType = GridType.GEOHASH
    level: int = Field(ge=1, le=12)
    point: list[float] = Field(min_length=2, max_length=2)

    @model_validator(mode="after")
    def validate_point_range(self):
        lon, lat = self.point
        if lon < -180.0 or lon > 180.0:
            raise ValueError("Point longitude must be in [-180, 180]")
        if lat < -90.0 or lat > 90.0:
            raise ValueError("Point latitude must be in [-90, 90]")
        return self


class CoverRequest(BaseModel):
    grid_type: GridType = GridType.GEOHASH
    level: int = Field(ge=1, le=12)
    cover_mode: CoverMode = CoverMode.INTERSECT
    boundary_type: BoundaryType = BoundaryType.BBOX
    geometry: dict[str, Any] | None = None
    bbox: list[float] | None = Field(default=None, min_length=4, max_length=4)
    crs: str = "EPSG:4326"

    @model_validator(mode="after")
    def validate_geometry_or_bbox(self):
        if self.geometry is None and self.bbox is None:
            raise ValueError("Either geometry or bbox must be provided")
        return self


class STCodeGenerateRequest(BaseModel):
    grid_type: GridType = GridType.GEOHASH
    level: int = Field(ge=1, le=12)
    space_code: str
    timestamp: datetime
    time_granularity: TimeGranularity = TimeGranularity.MINUTE
    version: str = "v1"


class STCodeParseRequest(BaseModel):
    st_code: str


class STCodeBatchItem(BaseModel):
    space_code: str
    timestamp: datetime


class STCodeBatchGenerateRequest(BaseModel):
    grid_type: GridType = GridType.GEOHASH
    level: int = Field(ge=1, le=12)
    time_granularity: TimeGranularity = TimeGranularity.MINUTE
    version: str = "v1"
    items: list[STCodeBatchItem] = Field(min_length=1, max_length=1000)


class NeighborsRequest(BaseModel):
    grid_type: GridType = GridType.GEOHASH
    code: str
    k: int = Field(default=1, ge=1, le=5)


class CodeToGeometryRequest(BaseModel):
    grid_type: GridType = GridType.GEOHASH
    code: str
    boundary_type: BoundaryType = BoundaryType.POLYGON


class BatchCodeToGeometryRequest(BaseModel):
    grid_type: GridType = GridType.GEOHASH
    codes: list[str] = Field(min_length=1, max_length=500)
    boundary_type: BoundaryType = BoundaryType.POLYGON


class ParentRequest(BaseModel):
    grid_type: GridType = GridType.GEOHASH
    code: str


class ChildrenRequest(BaseModel):
    grid_type: GridType = GridType.GEOHASH
    code: str
    target_level: int = Field(ge=1, le=12)
