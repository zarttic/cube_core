from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from grid_core.app.core.enums import BoundaryType, CoverMode, GridType, TimeGranularity
from grid_core.app.models.grid_address import GridAddress

# ---------------------------------------------------------------------------
# Canonical level ranges (inclusive on both ends).
# Consumers import validate_requested_grid_level from this module; do not duplicate.
# ---------------------------------------------------------------------------

LEVEL_RANGES: dict[GridType, tuple[int, int]] = {
    GridType.GEOHASH: (1, 12),
    GridType.MGRS: (0, 5),
    GridType.ISEA4H: (0, 15),
}


def validate_requested_grid_level(grid_type: GridType, requested_grid_level: int) -> int:
    """Validate that requested_grid_level is within the accepted inclusive range for grid_type.

    Raises ValueError with 'requested_grid_level' in the message on out-of-range input.
    Downstream packages import this function rather than duplicating range logic.
    """
    minimum, maximum = LEVEL_RANGES[grid_type]
    if not minimum <= requested_grid_level <= maximum:
        raise ValueError(f"{grid_type.value} requested_grid_level must be in [{minimum}, {maximum}]")
    return requested_grid_level


# ---------------------------------------------------------------------------
# Grid request models — all use requested_grid_level, never legacy "level".
# extra="forbid" ensures legacy callers get a clear validation error.
# ---------------------------------------------------------------------------


class LocateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    grid_type: GridType
    requested_grid_level: int
    point: List[float] = Field(min_length=2, max_length=2)

    @model_validator(mode="after")
    def _validate_level_and_point(self) -> "LocateRequest":
        validate_requested_grid_level(self.grid_type, self.requested_grid_level)
        lon, lat = self.point
        if lon < -180.0 or lon > 180.0:
            raise ValueError("Point longitude must be in [-180, 180]")
        if lat < -90.0 or lat > 90.0:
            raise ValueError("Point latitude must be in [-90, 90]")
        return self


class CoverRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    grid_type: GridType
    requested_grid_level: int
    cover_mode: CoverMode = CoverMode.INTERSECT
    boundary_type: BoundaryType = BoundaryType.BBOX
    geometry: Optional[Dict[str, Any]] = None
    bbox: Optional[List[float]] = Field(default=None, min_length=4, max_length=4)
    crs: str = "EPSG:4326"

    @model_validator(mode="after")
    def _validate_level_and_geometry(self) -> "CoverRequest":
        validate_requested_grid_level(self.grid_type, self.requested_grid_level)
        if self.geometry is None and self.bbox is None:
            raise ValueError("Either geometry or bbox must be provided")
        return self


# ---------------------------------------------------------------------------
# Address-based topology request models in the public contract.
# Topology and geometry operations consume a GridAddress because an ISEA4H
# sequence number is only meaningful with its resolution, and MGRS topology
# topology_code remains optional for compatibility with historical records.
# ---------------------------------------------------------------------------


class AddressRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    address: GridAddress


class NeighborsRequest(AddressRequest):
    k: int = Field(default=1, ge=1, le=5)


class ChildrenRequest(AddressRequest):
    target_grid_level: int


class BatchAddressRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    addresses: List[GridAddress] = Field(min_length=1, max_length=500)
    boundary_type: BoundaryType = BoundaryType.POLYGON


class CodeToGeometryRequest(AddressRequest):
    boundary_type: BoundaryType = BoundaryType.POLYGON


class ParentRequest(AddressRequest):
    pass


# ---------------------------------------------------------------------------
# ST-code request models. Generation carries a GridAddress (mirrors the
# frozen CubeEncoderSDK.generate_st_code(address, timestamp, ...) signature).
# ---------------------------------------------------------------------------


class STCodeGenerateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    address: GridAddress
    timestamp: datetime
    time_granularity: TimeGranularity = TimeGranularity.MINUTE


class STCodeParseRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    st_code: str


class STCodeBatchItem(BaseModel):
    space_code: str
    timestamp: datetime


class STCodeBatchGenerateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    grid_type: GridType
    requested_grid_level: int
    time_granularity: TimeGranularity = TimeGranularity.MINUTE
    items: List[STCodeBatchItem] = Field(min_length=1, max_length=1000)

    @model_validator(mode="after")
    def _validate_level(self) -> "STCodeBatchGenerateRequest":
        validate_requested_grid_level(self.grid_type, self.requested_grid_level)
        return self
