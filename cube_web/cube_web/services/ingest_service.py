from __future__ import annotations

from uuid import uuid4

from cube_web.services.ingest_contracts import CreateIngestRun, IngestPage, IngestRun
from cube_web.services.ingest_repository import IngestRepository


class QualityGateRejected(RuntimeError):
    pass


class IngestRunService:
    def __init__(self, repository: IngestRepository) -> None:
        self.repository = repository

    def schedule_after_quality(self, request: CreateIngestRun, *, quality_passed: bool) -> IngestRun:
        if not quality_passed:
            raise QualityGateRejected("ingest requires a passing quality decision")
        return self.repository.create(f"ingest-run-{uuid4()}", request)

    def get(self, ingest_run_id: str) -> IngestRun:
        return self.repository.get(ingest_run_id)

    def list_runs(
        self,
        *,
        keyword: str | None = None,
        dataset_id: str | None = None,
        status: str | None = None,
        page: int = 1,
        page_size: int = 20,
        sort_by: str = "created_at",
        sort_order: str = "desc",
    ) -> IngestPage[IngestRun]:
        if page < 1:
            raise ValueError("page must be positive")
        filters = {"keyword": keyword, "dataset_id": dataset_id, "status": status}
        items = self.repository.list_runs(
            **filters,
            limit=page_size,
            offset=(page - 1) * page_size,
            sort_by=sort_by,
            sort_order=sort_order,
        )
        return IngestPage(
            items=items,
            total=self.repository.count_runs(**filters),
            page=page,
            page_size=page_size,
            summary=self.repository.summarize_runs(**filters),
        )

    def start_scene(self, ingest_run_id: str, scene_id: str) -> IngestRun:
        return self.repository.start_scene(ingest_run_id, scene_id)

    def complete_scene(self, ingest_run_id: str, scene_id: str) -> IngestRun:
        return self.repository.complete_scene(ingest_run_id, scene_id)

    def fail_scene(self, ingest_run_id: str, scene_id: str, error_message: str) -> IngestRun:
        return self.repository.fail_scene(ingest_run_id, scene_id, error_message)

    def retry_failed(self, ingest_run_id: str, scene_ids: tuple[str, ...] | None = None, *, requested_by: str = "system") -> IngestRun:
        return self.repository.retry_failed(ingest_run_id, scene_ids, requested_by=requested_by)

    def cancel(self, ingest_run_id: str, reason: str = "") -> IngestRun:
        return self.repository.cancel(ingest_run_id, reason)
