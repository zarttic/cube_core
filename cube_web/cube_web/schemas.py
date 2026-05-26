from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class CubeWebModel(BaseModel):
    model_config = ConfigDict(extra="allow")


GridType = Literal["geohash", "mgrs", "isea4h"]


class OpticalAssetSelection(CubeWebModel):
    source_uri: str = Field(min_length=1)
    scene_id: str | None = None
    acq_time: str | None = None
    bands: list[str] | None = None
    band: str | None = None
    corners: list[list[float]] | None = None
    sensor: str | None = None
    product_family: str | None = None


class PartitionDemoRequest(CubeWebModel):
    grid_type: GridType | None = None
    grid_level: int | None = Field(default=None, ge=1)
    input_dir: str | None = None
    manifest_path: str | None = None
    batch_id: str | None = None
    batch_name: str | None = None
    selected_assets: list[OpticalAssetSelection] | None = None
    target_crs: str | None = None
    cover_mode: str | None = None
    time_granularity: str | None = None
    max_cells_per_asset: int | None = Field(default=None, ge=1)
    cog_workers: int | None = Field(default=None, ge=0)
    partition_workers: int | None = Field(default=None, ge=0)
    ray_parallelism: int | None = Field(default=None, ge=0)
    partition_prefix_len: int | None = Field(default=None, ge=1)


class PartitionRequestRecord(CubeWebModel):
    endpoint: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)


class PartitionRetryRequest(CubeWebModel):
    request: PartitionRequestRecord = Field(default_factory=PartitionRequestRecord)
    last_result: dict[str, Any] = Field(default_factory=dict)


class PartitionResult(CubeWebModel):
    status: str | None = None
    mode: str | None = None
    data_type: str | None = None


class PartitionTaskCreateResponse(CubeWebModel):
    task_id: str
    status: str
    data_type: str
    operation: str


class PartitionTaskResponse(PartitionTaskCreateResponse):
    created_at: float
    updated_at: float
    result: dict[str, Any] | None = None
    error: str | None = None


class QualityRunRequest(CubeWebModel):
    run_dir: str = Field(min_length=1)
    target_crs: str | None = "EPSG:4326"


class QualityLatestRequest(CubeWebModel):
    pass


class QualityReportRequest(CubeWebModel):
    report_id: str = Field(min_length=1)


class QualityHistoryRequest(CubeWebModel):
    limit: int = Field(default=20, ge=1)


class QualityResponse(CubeWebModel):
    status: str | None = None
    run_dir: str | None = None


class OpticalIngestRequest(CubeWebModel):
    run_dir: str | None = None
    report_id: str | None = None
    dataset: str = "demo_optical"
    sensor: str = "optical_mosaic"
    asset_version: str | None = None
    cube_version: str | None = None
    quality_rule: Literal["best_quality_wins", "latest_wins"] = "best_quality_wins"
    allow_failed_quality: bool = False


def payload_from_model(value: BaseModel | dict[str, Any] | None) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json", exclude_none=True)
    return dict(value)
