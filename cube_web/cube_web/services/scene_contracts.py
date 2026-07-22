from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from cube_web.services.partition_contracts import DatasetPartitionConfig


class SceneStrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SceneDatasetSelection(SceneStrictModel):
    dataset_id: str = Field(min_length=1)
    scene_ids: tuple[str, ...] = Field(min_length=1)
    band_unit_ids: tuple[str, ...] | None = None
    partition: DatasetPartitionConfig

    @model_validator(mode="after")
    def validate_unique_scenes(self) -> "SceneDatasetSelection":
        if len(set(self.scene_ids)) != len(self.scene_ids):
            raise ValueError(f"duplicate scene_id in dataset {self.dataset_id}")
        if self.band_unit_ids is not None:
            if not self.band_unit_ids:
                raise ValueError(f"band_unit_ids must not be empty in dataset {self.dataset_id}")
            if len(set(self.band_unit_ids)) != len(self.band_unit_ids):
                raise ValueError(f"duplicate band_unit_id in dataset {self.dataset_id}")
        return self


class ScenePartitionRunRequest(SceneStrictModel):
    partition_run_id: str = Field(min_length=1)
    source_batch_ids: tuple[str, ...] = Field(min_length=1)
    selection_source: Literal["load_batch", "dataset"] = "load_batch"
    datasets: tuple[SceneDatasetSelection, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_identity(self) -> "ScenePartitionRunRequest":
        if len(set(self.source_batch_ids)) != len(self.source_batch_ids):
            raise ValueError("duplicate source_batch_id")
        dataset_ids = [dataset.dataset_id for dataset in self.datasets]
        if len(set(dataset_ids)) != len(dataset_ids):
            raise ValueError("duplicate dataset_id")
        selected_scenes = [scene_id for dataset in self.datasets for scene_id in dataset.scene_ids]
        if len(set(selected_scenes)) != len(selected_scenes):
            raise ValueError("a scene may only be selected once per partition run")
        selected_band_units = [
            band_unit_id
            for dataset in self.datasets
            for band_unit_id in (dataset.band_unit_ids or ())
        ]
        if len(set(selected_band_units)) != len(selected_band_units):
            raise ValueError("a band unit may only be selected once per partition run")
        grid_configs = {
            (dataset.partition.grid_type, dataset.partition.requested_grid_level)
            for dataset in self.datasets
        }
        if len(grid_configs) != 1:
            raise ValueError("all datasets in one partition run must use the same grid type and level")
        return self


class ScenePartitionRunResponse(SceneStrictModel):
    partition_run_id: str
    source_batch_ids: tuple[str, ...]
    task_id: str
    status: str
    data_type: str
    operation: str


class CarbonFootprintPreviewRequest(SceneStrictModel):
    source_batch_ids: tuple[str, ...] = Field(min_length=1)
    scene_ids: tuple[str, ...] = Field(min_length=1)
    limit: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_identity(self) -> "CarbonFootprintPreviewRequest":
        if len(set(self.source_batch_ids)) != len(self.source_batch_ids):
            raise ValueError("duplicate source_batch_id")
        if len(set(self.scene_ids)) != len(self.scene_ids):
            raise ValueError("duplicate scene_id")
        return self


class CarbonGridPreviewRequest(CarbonFootprintPreviewRequest):
    grid_type: Literal["geohash", "mgrs", "isea4h"]
    requested_grid_level: int = Field(ge=1, le=15)
    max_cells: int | None = Field(default=None, ge=1)


class PartitionDraftCreateRequest(SceneStrictModel):
    data_type: Literal["optical", "radar", "product", "carbon"]
    draft_name: str = Field(min_length=1, max_length=160)
    source_batch_ids: tuple[str, ...] = Field(min_length=1)
    datasets: tuple[dict, ...] = Field(min_length=1)


class PartitionDraftSubmittedRequest(SceneStrictModel):
    partition_run_id: str = Field(min_length=1)


class LoadBatchSceneQuery(SceneStrictModel):
    status: str | None = None
    data_type: Literal["optical", "radar", "product", "carbon"] | None = None
    dataset_id: str | None = None
