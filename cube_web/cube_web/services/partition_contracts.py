from __future__ import annotations

from hashlib import sha256
from typing import Any, Literal

from grid_core.app.core.enums import GridType as EncoderGridType
from grid_core.app.models.request import validate_requested_grid_level
from pydantic import AnyUrl, BaseModel, ConfigDict, Field, field_validator, model_validator

GridType = Literal["geohash", "mgrs", "isea4h"]
PartitionMethod = Literal["logical", "entity"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SourceAssetInput(StrictModel):
    source_asset_id: str = Field(min_length=1)
    cog_uri: AnyUrl
    checksum: str = Field(pattern=r"^[0-9a-f]{64}$")
    bbox: tuple[float, float, float, float]
    crs: str = Field(min_length=1)
    time_start: str
    time_end: str
    attributes: dict[str, Any] = Field(default_factory=dict)

    @field_validator("cog_uri")
    @classmethod
    def require_s3_cog(cls, value: AnyUrl) -> AnyUrl:
        if value.scheme != "s3":
            raise ValueError("cog_uri must use s3://")
        return value


class BandInput(StrictModel):
    source_asset_id: str = Field(min_length=1)
    band_code: str = Field(min_length=1)
    band_name: str = Field(min_length=1)
    band_type: Literal["spectral", "polarization", "variable"]
    unit: str | None = None
    display_order: int = Field(ge=0)
    attributes: dict[str, Any] = Field(default_factory=dict)


class DatasetInput(StrictModel):
    dataset_id: str = Field(min_length=1)
    dataset_code: str = Field(min_length=1)
    dataset_title: str = Field(min_length=1)
    data_type: Literal["optical", "radar", "product", "carbon"]
    product_type: str | None = None
    assets: tuple[SourceAssetInput, ...] = Field(min_length=1)
    bands: tuple[BandInput, ...] = Field(min_length=1)
    attributes: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def bands_reference_assets(self) -> "DatasetInput":
        asset_ids = {asset.source_asset_id for asset in self.assets}
        unknown = sorted({band.source_asset_id for band in self.bands} - asset_ids)
        if unknown:
            raise ValueError(f"bands reference unknown source assets: {unknown}")
        return self


class StrictPartitionRequest(StrictModel):
    batch_id: str = Field(min_length=1)
    grid_type: GridType
    requested_grid_level: int
    partition_method: PartitionMethod
    cover_mode: Literal["intersect", "contain", "minimal"] = "intersect"
    time_granularity: Literal["second", "minute", "hour", "day", "month"] = "day"
    max_cells_per_asset: int = Field(default=0, ge=0)
    datasets: tuple[DatasetInput, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_grid_contract(self) -> "StrictPartitionRequest":
        validate_requested_grid_level(EncoderGridType(self.grid_type), self.requested_grid_level)
        validate_partition_method(self.grid_type, self.partition_method)
        return self


class OutputIdentity(StrictModel):
    dataset_id: str
    output_version: str
    source_asset_id: str
    band_code: str
    grid_type: GridType
    grid_level: int
    space_code: str
    topology_code: str | None = None
    time_bucket: str
    window_identity: str


class PartitionDatasetResult(StrictModel):
    dataset_id: str
    task_id: str
    output_version: str
    grid_type: GridType
    requested_grid_level: int
    partition_method: PartitionMethod
    object_prefix: str
    tiles: tuple[dict[str, Any], ...]
    indexes: tuple[dict[str, Any], ...]
    grid_cells: tuple[dict[str, Any], ...]


def derive_partition_method(grid_type: GridType) -> PartitionMethod:
    return "entity" if grid_type == "isea4h" else "logical"


def validate_partition_method(grid_type: GridType, supplied: PartitionMethod) -> PartitionMethod:
    derived = derive_partition_method(grid_type)
    if supplied != derived:
        raise ValueError(f"partition_method must be {derived} for grid_type={grid_type}")
    return derived


def group_datasets(request: StrictPartitionRequest) -> dict[str, DatasetInput]:
    grouped: dict[str, DatasetInput] = {}
    for dataset in request.datasets:
        if dataset.dataset_id in grouped:
            raise ValueError(f"duplicate dataset_id: {dataset.dataset_id}")
        grouped[dataset.dataset_id] = dataset
    return grouped


def make_output_version(dataset_id: str, task_id: str) -> str:
    return sha256(f"{dataset_id}\0{task_id}".encode("utf-8")).hexdigest()[:32]


def make_output_id(identity: OutputIdentity) -> str:
    canonical = identity.model_dump_json(exclude_none=False)
    return sha256(canonical.encode("utf-8")).hexdigest()
