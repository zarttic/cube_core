from __future__ import annotations

from typing import Any, Literal

from grid_core.app.core.enums import GridType as EncoderGridType
from grid_core.app.models.request import validate_requested_grid_level
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from cube_split.partition.carbon_products import normalize_carbon_product_type
from cube_web.services.partition_contracts import StrictPartitionRequest as StrictPartitionRequest


class CubeWebModel(BaseModel):
    model_config = ConfigDict(extra="allow")


GridType = Literal["geohash", "mgrs", "isea4h"]
PartitionBackend = Literal["auto", "ray", "thread", "process", "local"]


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
    model_config = ConfigDict(extra="forbid")

    schema_version: str | None = "1.0"
    load_batch_id: str = Field(min_length=1)
    batch_name: str | None = None
    source_system: str | None = None
    loaded_at: str | None = None
    datasets: list[dict[str, Any]] = Field(min_length=1)


class ManualQualityRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_id: str = Field(min_length=1)
    output_version: str | None = None


class QualityRuleSettingsUpdate(BaseModel):
    """Update which optional quality rules are enabled for subsequent quality runs."""

    model_config = ConfigDict(extra="forbid")

    enabled_optional_rules: list[str] = Field(default_factory=list)


class QualityRuleEnabledUpdate(BaseModel):
    """Toggle one optional quality rule for future quality runs."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool


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

    @field_validator("product_type")
    @classmethod
    def _normalize_carbon_product_type(cls, value: str) -> str:
        return normalize_carbon_product_type(value)

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
