from __future__ import annotations

from datetime import datetime
from typing import Generic, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator

IngestRunStatus = Literal[
    "pending",
    "queued",
    "running",
    "completed",
    "partial_failure",
    "failed",
    "cancelled",
]
IngestSceneStatus = Literal["pending", "queued", "running", "completed", "failed", "cancelled"]
T = TypeVar("T")


class FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class IngestSceneInput(FrozenModel):
    scene_id: str = Field(min_length=1)
    output_version: str = Field(min_length=1)
    quality_run_id: str = Field(min_length=1)
    source_load_batch_ids: tuple[str, ...] = ()
    # `ingest_run_scenes` is a legacy table name. Each new record represents
    # one concrete band unit, keeping ingest and retry state independent.
    band_unit_ids: tuple[str, ...] = Field(min_length=1, max_length=1)

    @field_validator("band_unit_ids")
    @classmethod
    def require_one_band_unit(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value[0].strip():
            raise ValueError("band_unit_ids must contain a non-empty band unit id")
        return value


class CreateIngestRun(FrozenModel):
    partition_run_id: str = Field(min_length=1)
    dataset_id: str = Field(min_length=1)
    scenes: tuple[IngestSceneInput, ...] = Field(min_length=1)
    requested_by: str = Field(default="system", min_length=1)


class RetryIngestBandUnits(FrozenModel):
    band_unit_ids: tuple[str, ...] = Field(min_length=1)


class ManualCollectionIngest(FrozenModel):
    band_unit_ids: tuple[str, ...] = Field(min_length=1)


class CancelIngestRun(FrozenModel):
    reason: str = Field(default="", max_length=1000)


class IngestRetryEvent(FrozenModel):
    error_message: str | None
    retried_by: str
    retried_at: datetime
    attempt_count: int


class IngestRunScene(FrozenModel):
    ingest_run_id: str
    scene_id: str
    partition_run_id: str
    output_version: str
    status: IngestSceneStatus
    idempotency_key: str
    attempt_count: int
    error_message: str | None
    quality_run_id: str | None
    source_load_batch_ids: tuple[str, ...]
    band_unit_ids: tuple[str, ...] = Field(min_length=1, max_length=1)
    retry_history: tuple[IngestRetryEvent, ...] = ()
    created_at: datetime
    updated_at: datetime


class IngestRun(FrozenModel):
    ingest_run_id: str
    partition_run_id: str
    dataset_id: str
    dataset_code: str | None = None
    status: IngestRunStatus
    requested_by: str
    error_message: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    cancel_reason: str | None = None
    scenes: tuple[IngestRunScene, ...]

    @computed_field
    @property
    def scene_count(self) -> int:
        return len(self.scenes)

    @computed_field
    @property
    def band_count(self) -> int:
        return len(self.scenes)

    @computed_field
    @property
    def completed_scene_count(self) -> int:
        return sum(scene.status == "completed" for scene in self.scenes)

    @computed_field
    @property
    def completed_band_count(self) -> int:
        return sum(scene.status == "completed" for scene in self.scenes)

    @computed_field
    @property
    def failed_scene_count(self) -> int:
        return sum(scene.status == "failed" for scene in self.scenes)

    @computed_field
    @property
    def failed_band_count(self) -> int:
        return sum(scene.status == "failed" for scene in self.scenes)


class IngestSummary(FrozenModel):
    run_count: int
    scene_count: int
    completed_scene_count: int
    failed_scene_count: int

    @computed_field
    @property
    def band_count(self) -> int:
        return self.scene_count

    @computed_field
    @property
    def completed_band_count(self) -> int:
        return self.completed_scene_count

    @computed_field
    @property
    def failed_band_count(self) -> int:
        return self.failed_scene_count


class IngestPage(FrozenModel, Generic[T]):
    items: tuple[T, ...]
    total: int
    page: int
    page_size: int
    summary: IngestSummary
