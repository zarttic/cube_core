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
        return self


class ScenePartitionRunResponse(SceneStrictModel):
    partition_run_id: str
    source_batch_ids: tuple[str, ...]
    task_id: str
    status: str
    data_type: str
    operation: str


class LoadBatchSceneQuery(SceneStrictModel):
    status: str | None = None
    data_type: Literal["optical", "radar", "product", "carbon"] | None = None
    dataset_id: str | None = None
