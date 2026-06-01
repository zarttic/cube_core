from __future__ import annotations

from typing import Any
from uuid import uuid4

from fastapi import HTTPException

from cube_web.services.partition_job_store import PartitionJobStore, get_partition_job_store
from cube_web.services.partition_service import PartitionService, PartitionTask


class PartitionWorkflowService:
    def __init__(self, partition_service: PartitionService, store: PartitionJobStore | None = None) -> None:
        self.partition_service = partition_service
        self._store = store

    @property
    def store(self) -> PartitionJobStore:
        if self._store is None:
            self._store = get_partition_job_store()
        return self._store

    def import_schema(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.store.upsert_schema(payload)

    def list_batches(
        self,
        *,
        status: str | None = None,
        data_type: str | None = None,
        keyword: str | None = None,
        include_succeeded: bool = False,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        return self.store.list_batches(
            status=status,
            data_type=data_type,
            keyword=keyword,
            include_succeeded=include_succeeded,
            limit=limit,
        )

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        batch = self.store.get_batch(batch_id)
        if batch is None:
            raise HTTPException(status_code=404, detail=f"Partition batch not found: {batch_id}")
        return batch

    def list_assets(self, batch_id: str, status: str | None = None) -> list[dict[str, Any]]:
        self.get_batch(batch_id)
        return self.store.list_assets(batch_id, status=status)

    def list_attempts(self, batch_id: str) -> list[dict[str, Any]]:
        self.get_batch(batch_id)
        return self.store.list_attempts(batch_id)

    def run_batch(
        self,
        batch_id: str,
        *,
        operation: str = "auto_run",
        config_override: dict[str, Any] | None = None,
        asset_ids: list[str] | None = None,
        requested_by: str = "system",
    ) -> PartitionTask:
        batch = self.get_batch(batch_id)
        payload = self._payload_for_batch(batch, config_override=config_override, asset_ids=asset_ids)
        task_id = f"partition-{uuid4().hex[:12]}"

        def cancellation_check() -> bool:
            return self.store.is_cancel_requested(task_id)

        self.store.create_attempt(
            task_id=task_id,
            batch_id=batch_id,
            operation=operation,
            payload=payload,
            asset_ids=asset_ids,
            requested_by=requested_by,
        )
        self.store.mark_batch_queued(batch_id, task_id, operation=operation)
        data_type = str(batch["data_type"])
        return self.partition_service.submit(
            data_type,
            "demo",
            payload,
            task_id=task_id,
            on_started=self.on_task_started,
            on_succeeded=self.on_task_succeeded,
            on_failed=self.on_task_failed,
            cancellation_check=cancellation_check,
        )

    def retry_batch(self, batch_id: str, config_override: dict[str, Any] | None = None) -> PartitionTask:
        return self.run_batch(batch_id, operation="manual_retry", config_override=config_override, requested_by="operator")

    def retry_assets(self, asset_ids: list[str], config_override: dict[str, Any] | None = None) -> PartitionTask:
        if not asset_ids:
            raise HTTPException(status_code=422, detail="asset_ids is required")
        first_batch_id: str | None = None
        for batch in self.store.list_batches(include_succeeded=True, limit=10000):
            batch_assets = {asset["asset_id"] for asset in self.store.list_assets(batch["batch_id"])}
            if asset_ids[0] in batch_assets:
                first_batch_id = batch["batch_id"]
                if not set(asset_ids).issubset(batch_assets):
                    raise HTTPException(status_code=422, detail="asset_ids must belong to the same batch")
                break
        if not first_batch_id:
            raise HTTPException(status_code=404, detail=f"Partition asset not found: {asset_ids[0]}")
        return self.run_batch(
            first_batch_id,
            operation="manual_asset_retry",
            config_override=config_override,
            asset_ids=asset_ids,
            requested_by="operator",
        )

    def cancel_task(self, task_id: str) -> dict[str, Any]:
        attempt = self.store.request_cancel(task_id)
        if attempt is None:
            self.partition_service.cancel_task(task_id)
            return {"task_id": task_id, "status": "cancel_requested"}
        self.partition_service.cancel_task(task_id)
        return attempt

    def on_task_started(self, task_id: str) -> None:
        attempt = self.store.get_attempt(task_id)
        if attempt is not None:
            self.store.start_attempt(task_id)

    def on_task_succeeded(self, task_id: str, result: dict[str, Any]) -> None:
        attempt = self.store.get_attempt(task_id)
        if attempt is not None:
            self.store.succeed_attempt(task_id, result)

    def on_task_failed(self, task_id: str, error: str) -> None:
        attempt = self.store.get_attempt(task_id)
        if attempt is None:
            return
        if "cancel" in error.lower():
            self.store.mark_cancelled(task_id)
            return
        batch = self.get_batch(attempt["batch_id"])
        attempt_no = int(attempt.get("attempt_no") or 1)
        max_auto_retries = int(batch.get("max_auto_retries") or 1)
        should_auto_retry = attempt_no <= max_auto_retries and attempt.get("operation") in {"auto_run", "auto_retry"}
        self.store.fail_attempt(task_id, error, manual_required=not should_auto_retry)
        if should_auto_retry:
            self.run_batch(attempt["batch_id"], operation="auto_retry", requested_by="system")

    def _payload_for_batch(
        self,
        batch: dict[str, Any],
        *,
        config_override: dict[str, Any] | None = None,
        asset_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        payload = dict(batch.get("normalized_payload") or {})
        if asset_ids:
            assets = self.store.list_assets(batch["batch_id"])
            selected = [asset["asset_payload"] for asset in assets if asset["asset_id"] in set(asset_ids)]
            key = "selected_observations" if batch["data_type"] == "carbon" else "selected_assets"
            payload[key] = selected
        if config_override:
            payload.update(config_override)
        return payload
