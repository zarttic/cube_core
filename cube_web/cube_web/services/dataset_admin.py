"""Shared construction of the dataset management service for the HTTP layer.

Both the dataset router and the load-batch deletion route need the same service
with the same hooks (manual quality/ingest, publish/withdraw and MinIO object
cleanup).  Keeping one factory here avoids two diverging copies of the wiring.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from cube_split import runtime_config
from minio import Minio

from cube_web.services.dataset_management import (
    DatasetManagementConflict,
    DatasetManagementService,
    OpenGaussDatasetManagementRepository,
)
from cube_web.services.partition_object_store import PartitionObjectStore
from cube_web.services.publication_service import (
    PublishRequest,
    publish_dataset,
    withdraw_publication,
)
from cube_web.services.quality_ingest_bridge import ManualIngestRejected, request_manual_ingest
from cube_web.services.quality_run_service import request_manual_quality_run


def build_dataset_management_service() -> DatasetManagementService:
    repository = OpenGaussDatasetManagementRepository(runtime_config.postgres_dsn())

    def quality_hook(dataset_id: str, actor: Any) -> dict[str, Any]:
        return request_manual_quality_run(dataset_id, None, actor).model_dump(mode="json")

    def ingest_hook(dataset_id: str, actor: Any) -> dict[str, Any]:
        try:
            return request_manual_ingest(dataset_id, requested_by=actor.username)
        except ManualIngestRejected as exc:
            raise DatasetManagementConflict(str(exc)) from exc

    def publish_hook(dataset_id: str, actor: Any, targets: tuple[dict[str, str], ...] = ()) -> dict[str, Any]:
        return publish_dataset(dataset_id, PublishRequest(targets=tuple(targets)), actor).model_dump(mode="json")

    def withdraw_hook(dataset_id: str, publication_id: str, reason: str, actor: Any) -> dict[str, Any]:
        return withdraw_publication(dataset_id, UUID(publication_id), reason, actor).model_dump(mode="json")

    def grid_object_cleanup(object_uris: tuple[str, ...]) -> dict[str, Any]:
        settings = runtime_config.minio_settings()
        store = PartitionObjectStore(
            Minio(settings.endpoint, access_key=settings.access_key, secret_key=settings.secret_key, secure=settings.secure),
            bucket=settings.bucket,
        )
        deleted_keys = store.remove_objects(object_uris)
        return {"status": "completed", "object_count": len(deleted_keys), "deleted_object_keys": deleted_keys}

    return DatasetManagementService(
        repository,
        quality_hook=quality_hook,
        ingest_hook=ingest_hook,
        publish_hook=publish_hook,
        withdraw_hook=withdraw_hook,
        grid_object_cleanup=grid_object_cleanup,
    )
