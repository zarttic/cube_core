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
    cog_uri: AnyUrl | None = None
    source_uri: AnyUrl | None = None
    source_kind: Literal["cog", "raw"] = "cog"
    source_format: Literal["cog", "netcdf", "hdf5"] = "cog"
    checksum: str = Field(pattern=r"^[0-9a-f]{64}$")
    bbox: tuple[float, float, float, float] | None = None
    crs: str | None = Field(default=None, min_length=1)
    time_start: str
    time_end: str
    attributes: dict[str, Any] = Field(default_factory=dict)

    @field_validator("cog_uri", "source_uri")
    @classmethod
    def require_s3_uri(cls, value: AnyUrl | None) -> AnyUrl | None:
        if value is not None and value.scheme != "s3":
            raise ValueError("source asset URIs must use s3://")
        return value


class BandInput(StrictModel):
    source_asset_id: str = Field(min_length=1)
    band_code: str = Field(min_length=1)
    band_name: str = Field(min_length=1)
    band_type: Literal["spectral", "polarization", "variable"]
    unit: str | None = None
    display_order: int = Field(ge=0)
    attributes: dict[str, Any] = Field(default_factory=dict)


class DatasetPartitionConfig(StrictModel):
    """Optional dataset overrides for a mixed normalized partition batch.

    Missing fields inherit the batch-level strict request configuration.  The
    resolved combination is validated by ``resolve_dataset_partition``.
    """

    grid_type: GridType | None = None
    requested_grid_level: int | None = None
    partition_method: PartitionMethod | None = None
    cover_mode: Literal["intersect", "contain", "minimal"] | None = None
    time_granularity: Literal["second", "minute", "hour", "day", "month"] | None = None
    max_cells_per_asset: int | None = Field(default=None, ge=0)
    max_observations: int | None = Field(default=None, ge=1)


class DatasetInput(StrictModel):
    dataset_id: str = Field(min_length=1)
    dataset_code: str = Field(min_length=1)
    dataset_title: str = Field(min_length=1)
    data_type: Literal["optical", "radar", "product", "carbon"]
    product_type: str | None = None
    assets: tuple[SourceAssetInput, ...] = Field(min_length=1)
    bands: tuple[BandInput, ...] = Field(min_length=1)
    attributes: dict[str, Any] = Field(default_factory=dict)
    partition: DatasetPartitionConfig | None = None

    @model_validator(mode="after")
    def bands_reference_assets(self) -> "DatasetInput":
        asset_ids = {asset.source_asset_id for asset in self.assets}
        unknown = sorted({band.source_asset_id for band in self.bands} - asset_ids)
        if unknown:
            raise ValueError(f"bands reference unknown source assets: {unknown}")
        for asset in self.assets:
            if self.data_type == "carbon":
                if asset.cog_uri is not None or asset.source_uri is None:
                    raise ValueError("carbon assets require source_uri and must not use cog_uri")
                if asset.source_kind != "raw" or asset.source_format not in {"netcdf", "hdf5"}:
                    raise ValueError("carbon assets require source_kind=raw and source_format netcdf or hdf5")
                suffix = asset.source_uri.path.lower()
                allowed = {
                    "netcdf": (".nc", ".nc4"),
                    "hdf5": (".h5", ".hdf", ".hdf5"),
                }[asset.source_format]
                if not suffix.endswith(allowed):
                    raise ValueError(f"carbon source_uri does not match source_format={asset.source_format}")
            elif asset.source_kind != "cog" or asset.source_format != "cog" or asset.cog_uri is None or asset.source_uri is not None:
                raise ValueError("non-carbon assets require cog_uri with source_format=cog")
            elif asset.bbox is None or asset.crs is None:
                raise ValueError("non-carbon COG assets require bbox and crs")
        if self.data_type != "carbon" and self.partition is not None and self.partition.max_observations is not None:
            raise ValueError("max_observations is only valid for carbon datasets")
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
        for dataset in self.datasets:
            resolve_dataset_partition(self, dataset)
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
    execution_engine: str | None = None
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


def resolve_dataset_partition(
    request: StrictPartitionRequest,
    dataset: DatasetInput,
) -> DatasetPartitionConfig:
    """Resolve and validate a dataset's effective partition settings."""
    override = dataset.partition
    resolved = DatasetPartitionConfig(
        grid_type=request.grid_type if override is None or override.grid_type is None else override.grid_type,
        requested_grid_level=(
            request.requested_grid_level
            if override is None or override.requested_grid_level is None
            else override.requested_grid_level
        ),
        partition_method=(
            request.partition_method
            if override is None or override.partition_method is None
            else override.partition_method
        ),
        cover_mode=request.cover_mode if override is None or override.cover_mode is None else override.cover_mode,
        time_granularity=(
            request.time_granularity
            if override is None or override.time_granularity is None
            else override.time_granularity
        ),
        max_cells_per_asset=(
            request.max_cells_per_asset
            if override is None or override.max_cells_per_asset is None
            else override.max_cells_per_asset
        ),
        max_observations=None if override is None else override.max_observations,
    )
    validate_requested_grid_level(EncoderGridType(resolved.grid_type), resolved.requested_grid_level)
    validate_partition_method(resolved.grid_type, resolved.partition_method)
    return resolved


def effective_dataset_request(request: StrictPartitionRequest, dataset: DatasetInput) -> StrictPartitionRequest:
    """Return the already-validated strict request effective for one dataset."""
    resolved = resolve_dataset_partition(request, dataset)
    return request.model_copy(
        update={
            "grid_type": resolved.grid_type,
            "requested_grid_level": resolved.requested_grid_level,
            "partition_method": resolved.partition_method,
            "cover_mode": resolved.cover_mode,
            "time_granularity": resolved.time_granularity,
            "max_cells_per_asset": resolved.max_cells_per_asset,
            "datasets": (dataset,),
        }
    )


def make_output_version(dataset_id: str, task_id: str) -> str:
    return sha256(f"{dataset_id}\0{task_id}".encode("utf-8")).hexdigest()[:32]


def make_output_id(identity: OutputIdentity) -> str:
    canonical = identity.model_dump_json(exclude_none=False)
    return sha256(canonical.encode("utf-8")).hexdigest()
