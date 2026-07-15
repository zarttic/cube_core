from __future__ import annotations

from datetime import datetime
from typing import Any, Generic, Literal, TypeVar
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

QualityStatus = Literal["pending", "running", "pass", "warn", "fail", "error", "cancelled"]
TerminalQualityStatus = Literal["pass", "warn", "fail", "error", "cancelled"]
TriggerKind = Literal["automatic", "manual"]
SortOrder = Literal["asc", "desc"]
ExportFormat = Literal["csv", "json"]

DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 500
ERROR_BATCH_SIZE = 1000

T = TypeVar("T")


class FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class RuleSnapshot(FrozenModel):
    code: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=256)
    applicability: dict[str, Any]
    mandatory: bool
    parameters: dict[str, Any]
    implementation_version: str = Field(min_length=1, max_length=64)


class QualityRun(FrozenModel):
    quality_run_id: UUID
    dataset_id: str
    dataset_code: str
    batch_id: str
    data_type: str
    product_type: str | None
    partition_status: str
    output_version: str
    quality_sequence: int
    trigger_event_id: UUID | None
    trigger: TriggerKind
    requested_by: str
    rule_set_version: str
    rule_snapshot: tuple[RuleSnapshot, ...]
    status: QualityStatus
    results_complete: bool
    error_count: int
    warning_count: int
    execution_error: str | None
    started_at: datetime | None
    completed_at: datetime | None
    created_at: datetime
    is_current: bool


class QualityResult(FrozenModel):
    quality_run_id: UUID
    rule_code: str
    status: Literal["pass", "warn", "fail", "error"]
    finding_count: int
    error_count: int
    warning_count: int
    metrics: dict[str, Any]
    execution_error: str | None
    started_at: datetime
    completed_at: datetime


class QualityError(FrozenModel):
    quality_error_id: UUID
    quality_run_id: UUID
    rule_code: str
    source_asset_id: str | None
    tile_id: str | None
    index_id: str | None
    output_id: str | None
    row_number: int | None
    field: str | None
    error_code: str
    message: str
    context: dict[str, Any]
    created_at: datetime


class QualityErrorFilter(FrozenModel):
    rule_code: str | None = None
    error_code: str | None = None
    source_asset_id: str | None = None
    output_id: str | None = None
    field: str | None = None

    def active(self) -> bool:
        return any(value is not None for value in self.model_dump().values())


class WarnApproval(FrozenModel):
    approval_id: UUID
    dataset_id: str
    output_version: str
    quality_run_id: UUID
    rule_set_version: str
    approved_by: str
    approved_at: datetime
    reason: str


class Publication(FrozenModel):
    publication_id: UUID
    dataset_id: str
    output_version: str
    quality_run_id: UUID
    status: Literal["publishing", "active", "withdrawing", "failed", "withdrawn"]
    service_version_id: str | None
    requested_by: str
    requested_at: datetime
    activated_at: datetime | None
    failure: str | None
    withdrawn_by: str | None
    withdrawn_at: datetime | None
    withdrawal_reason: str | None


class Page(FrozenModel, Generic[T]):
    items: tuple[T, ...]
    total: int
    page: int
    page_size: int


def page_offset(page: int, page_size: int) -> int:
    if page < 1 or page_size < 1 or page_size > MAX_PAGE_SIZE:
        raise ValueError("page must be >= 1 and page_size must be between 1 and 500")
    return (page - 1) * page_size


def validate_sort(sort_by: str, sort_order: str, allowed: set[str]) -> tuple[str, SortOrder]:
    if sort_by not in allowed:
        raise ValueError(f"sort_by must be one of: {', '.join(sorted(allowed))}")
    if sort_order not in {"asc", "desc"}:
        raise ValueError("sort_order must be asc or desc")
    return sort_by, sort_order  # type: ignore[return-value]
