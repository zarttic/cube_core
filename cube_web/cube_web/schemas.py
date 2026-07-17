from __future__ import annotations

from typing import Any, Literal

from grid_core.app.core.enums import GridType as EncoderGridType
from grid_core.app.models.request import validate_requested_grid_level
from pydantic import BaseModel, ConfigDict, Field, model_validator

from cube_web.services.partition_contracts import StrictPartitionRequest as StrictPartitionRequest


class CubeWebModel(BaseModel):
    model_config = ConfigDict(extra="allow")


GridType = Literal["geohash", "mgrs", "isea4h"]
PartitionBackend = Literal["auto", "ray", "thread", "process", "local"]


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
    batch_id: str | None = Field(default=None, min_length=1)
    load_batch_id: str | None = Field(default=None, min_length=1)
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

    @model_validator(mode="after")
    def normalize_load_batch_id(self) -> "PartitionSchemaImportRequest":
        load_batch_id = self.load_batch_id or self.batch_id
        if not load_batch_id:
            raise ValueError("load_batch_id is required")
        if self.batch_id is None:
            object.__setattr__(self, "batch_id", load_batch_id)
        return self


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


class ManualQualityRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_id: str = Field(min_length=1)
    output_version: str | None = None


class DatasetQualityRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    output_version: str | None = None


class WarnApprovalRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str = Field(min_length=1, max_length=2000)


class PublishRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    output_version: str | None = None
    quality_run_id: str | None = None


class WithdrawPublicationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str = Field(min_length=1, max_length=2000)


class SpatiotemporalQueryRequest(CubeWebModel):
    data_type: Literal["carbon"] = "carbon"
    point: list[float] | None = Field(default=None, min_length=2, max_length=2)
    bbox: list[float] | None = Field(default=None, min_length=4, max_length=4)
    time_start: str = Field(min_length=1)
    time_end: str = Field(min_length=1)
    quality_flags: list[str] | None = None
    product_type: str = "xco2"
    grid_type: GridType = "isea4h"
    grid_level: int = Field(default=5, ge=0)
    cube_version: str = "v1"
    limit: int = Field(default=1000, ge=1, le=10000)

    @model_validator(mode="after")
    def _validate_grid_level(self) -> "SpatiotemporalQueryRequest":
        validate_requested_grid_level(EncoderGridType(self.grid_type), self.grid_level)
        return self


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
