from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class CubeWebModel(BaseModel):
    model_config = ConfigDict(extra="allow")


GridType = Literal["s2", "mgrs", "tile_matrix", "isea4h"]


class OpticalAssetSelection(CubeWebModel):
    source_uri: str = Field(min_length=1)
    scene_id: str | None = None
    acq_time: str | None = None
    bands: list[str] | None = None
    band: str | None = None
    resolution: float | None = Field(default=None, gt=0)
    corners: list[list[float]] | None = None
    sensor: str | None = None
    product_family: str | None = None


class PartitionDemoRequest(CubeWebModel):
    partition_method: Literal["logical", "entity"] | None = None
    grid_type: GridType | None = None
    grid_level: int | None = Field(default=None, ge=1)
    grid_level_mode: Literal["auto", "manual"] | None = None
    target_pixels_per_hex_edge: int | None = Field(default=None, ge=1)
    input_dir: str | None = None
    manifest_path: str | None = None
    batch_id: str | None = None
    batch_name: str | None = None
    selected_assets: list[OpticalAssetSelection] | None = None
    target_crs: str | None = None
    cover_mode: Literal["intersect", "contain", "minimal"] | None = None
    time_granularity: Literal["year", "month", "day", "hour", "minute"] | None = None
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


class PartitionSchemaImportRequest(CubeWebModel):
    schema_version: str | None = "1.0"
    batch_id: str = Field(min_length=1)
    batch_name: str | None = None
    data_type: Literal["optical", "product", "carbon", "radar"] = "optical"
    source_system: str | None = None
    loaded_at: str | None = None
    updated_at: str | None = None
    raw_meta_uri: str | None = None
    assets: list[dict[str, Any]] | None = None
    observations: list[dict[str, Any]] | None = None
    normalized_payload: dict[str, Any] | None = None
    priority: int = 0
    max_auto_retries: int = Field(default=1, ge=0)


class PartitionSchemaReconcileRequest(CubeWebModel):
    source_system: str | None = None
    batch_ids: list[str] | None = None
    asset_ids: list[str] | None = None
    observation_ids: list[str] | None = None
    updated_since: str | None = None
    include_assets: bool = True
    include_attempts: bool = False


class PartitionBatchRunRequest(CubeWebModel):
    config_override: dict[str, Any] = Field(default_factory=dict)


class PartitionAssetRetryRequest(CubeWebModel):
    asset_ids: list[str] = Field(min_length=1)
    config_override: dict[str, Any] = Field(default_factory=dict)


class QualityRunRequest(CubeWebModel):
    run_dir: str = Field(min_length=1)
    target_crs: str | None = "EPSG:4326"


class QualityLatestRequest(CubeWebModel):
    pass


class QualityReportRequest(CubeWebModel):
    report_id: str = Field(min_length=1)


class QualityHistoryRequest(CubeWebModel):
    limit: int | None = Field(default=None, ge=1)
    page: int = Field(default=1, ge=1)
    page_size: int | None = Field(default=None, ge=1)
    keyword: str | None = None
    status: str | None = None


class QualityResponse(CubeWebModel):
    status: str | None = None
    run_dir: str | None = None


class SpatiotemporalQueryRequest(CubeWebModel):
    data_type: Literal["carbon"] = "carbon"
    point: list[float] | None = Field(default=None, min_length=2, max_length=2)
    bbox: list[float] | None = Field(default=None, min_length=4, max_length=4)
    time_start: str = Field(min_length=1)
    time_end: str = Field(min_length=1)
    quality_flags: list[str] | None = None
    product_type: str = "xco2"
    grid_type: GridType = "isea4h"
    grid_level: int = Field(default=5, ge=1)
    cube_version: str = "v1"
    limit: int = Field(default=1000, ge=1, le=10000)


class OpticalIngestRequest(CubeWebModel):
    batch_id: str | None = None
    run_dir: str | None = None
    report_id: str | None = None
    dataset: str = "demo_optical"
    sensor: str = "optical_mosaic"
    asset_version: str | None = None
    cube_version: str | None = None
    quality_rule: Literal["best_quality_wins", "latest_wins"] = "best_quality_wins"
    allow_failed_quality: bool = False


class ConfigGetRequest(CubeWebModel):
    pass


class ConfigUpdateRequest(CubeWebModel):
    config: dict[str, Any] = Field(default_factory=dict)


class ConfigResetRequest(CubeWebModel):
    pass


class ConfigResponse(CubeWebModel):
    config: dict[str, Any]
    defaults: dict[str, Any]
    runtime: dict[str, Any]
    updated_at: str | None = None


def payload_from_model(value: BaseModel | dict[str, Any] | None) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json", exclude_none=True)
    return dict(value)
