from __future__ import annotations

import copy
from typing import Any
from uuid import uuid4

from fastapi import HTTPException

from cube_web.services.partition_job_store import PartitionJobStore, get_partition_job_store
from cube_web.services.partition_service import PartitionService, PartitionTask

ACTIVE_BATCH_RUN_STATUSES = {"queued", "running", "retrying", "cancel_requested"}
ACTIVE_TASK_STATUSES = {"queued", "running", "cancel_requested"}


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
        try:
            return self.store.upsert_schema(payload)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

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
        source_task_id: str | None = None,
        retry_strategy: str | None = None,
        failure_reason: str | None = None,
    ) -> PartitionTask:
        batch = self.get_batch(batch_id)
        active_task = self._active_task_for_batch(batch)
        if active_task is not None:
            return active_task
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
            source_task_id=source_task_id,
            retry_strategy=retry_strategy,
            failure_reason=failure_reason,
        )
        self.store.mark_batch_queued(batch_id, task_id, operation=operation)
        data_type = str(batch["data_type"])
        return self.partition_service.submit(
            data_type,
            "run",
            payload,
            task_id=task_id,
            on_started=self.on_task_started,
            on_succeeded=self.on_task_succeeded,
            on_failed=self.on_task_failed,
            cancellation_check=cancellation_check,
        )

    def retry_batch(self, batch_id: str, config_override: dict[str, Any] | None = None) -> PartitionTask:
        batch = self.get_batch(batch_id)
        return self.run_batch(
            batch_id,
            operation="manual_retry",
            config_override=config_override,
            requested_by="operator",
            source_task_id=_text_or_none(batch.get("last_task_id")),
            retry_strategy="full_batch",
            failure_reason=_batch_failure_reason(batch),
        )

    def retry_assets(self, asset_ids: list[str], config_override: dict[str, Any] | None = None) -> PartitionTask:
        if not asset_ids:
            raise HTTPException(status_code=422, detail="asset_ids is required")
        first_batch: dict[str, Any] | None = None
        for batch in self.store.list_batches(include_succeeded=True, limit=10000):
            batch_assets = {asset["asset_id"] for asset in self.store.list_assets(batch["batch_id"])}
            if asset_ids[0] in batch_assets:
                first_batch = batch
                if not set(asset_ids).issubset(batch_assets):
                    raise HTTPException(status_code=422, detail="asset_ids must belong to the same batch")
                break
        if not first_batch:
            raise HTTPException(status_code=404, detail=f"Partition asset not found: {asset_ids[0]}")
        source_assets = self.store.list_assets(first_batch["batch_id"])
        return self.run_batch(
            first_batch["batch_id"],
            operation="manual_asset_retry",
            config_override=config_override,
            asset_ids=asset_ids,
            requested_by="operator",
            source_task_id=_text_or_none(first_batch.get("last_task_id")),
            retry_strategy="selected_assets",
            failure_reason=_asset_failure_reason(source_assets, asset_ids) or _batch_failure_reason(first_batch),
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
            batch = self.get_batch(attempt["batch_id"])
            attempt_no = int(attempt.get("attempt_no") or 1)
            max_auto_retries = int(batch.get("max_auto_retries") or 1)
            retryable_asset_ids = _retryable_failed_asset_ids(result)
            all_failed_asset_ids = _failed_asset_ids(result)
            should_auto_retry = (
                bool(retryable_asset_ids)
                and attempt_no <= max_auto_retries
                and attempt.get("operation") in {"auto_run", "auto_retry"}
            )
            result_to_store = result if should_auto_retry else _manual_required_asset_result(result, set(all_failed_asset_ids))
            self.store.succeed_attempt(task_id, result_to_store)
            if should_auto_retry:
                self.run_batch(
                    attempt["batch_id"],
                    operation="auto_retry",
                    asset_ids=retryable_asset_ids,
                    requested_by="system",
                    source_task_id=task_id,
                    retry_strategy="retryable_assets",
                    failure_reason=_result_failure_reason(result, retryable_asset_ids),
                )

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
        error_type = classify_partition_error(error)
        should_auto_retry = (
            is_retryable_partition_error(error_type)
            and attempt_no <= max_auto_retries
            and attempt.get("operation") in {"auto_run", "auto_retry"}
        )
        self.store.fail_attempt(task_id, error, manual_required=not should_auto_retry, error_type=error_type)
        if should_auto_retry:
            self.run_batch(
                attempt["batch_id"],
                operation="auto_retry",
                requested_by="system",
                source_task_id=task_id,
                retry_strategy="full_batch",
                failure_reason=error,
            )

    def _payload_for_batch(
        self,
        batch: dict[str, Any],
        *,
        config_override: dict[str, Any] | None = None,
        asset_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        payload = dict(batch.get("normalized_payload") or {})
        assets = self.store.list_assets(batch["batch_id"])
        if assets:
            target_asset_ids = set(asset_ids) if asset_ids else None
            selected = [
                _payload_asset_with_identity(asset)
                for asset in assets
                if target_asset_ids is None or asset["asset_id"] in target_asset_ids
            ]
            key = "selected_observations" if batch["data_type"] == "carbon" else "selected_assets"
            payload[key] = selected
        if config_override:
            payload.update(config_override)
        return payload

    def _active_task_for_batch(self, batch: dict[str, Any]) -> PartitionTask | None:
        if str(batch.get("status") or "") not in ACTIVE_BATCH_RUN_STATUSES:
            return None
        task_id = str(batch.get("last_task_id") or "").strip()
        if not task_id:
            return None
        try:
            task = self.partition_service.get_task(task_id)
        except HTTPException:
            return None
        if task.status in ACTIVE_TASK_STATUSES:
            return task
        return None


def classify_partition_error(error: str) -> str:
    normalized = error.lower()
    if any(token in normalized for token in ("timed out", "timeout", "temporarily", "temporary", "connection reset", "connection refused", "network", "503", "502", "504")):
        return "transient"
    if any(token in normalized for token in ("not found", "no such file", "no such key", "missing", "does not exist", "source missing")):
        return "source_missing"
    if any(token in normalized for token in ("invalid", "validation", "bad request", "unsupported", "must be", "required")):
        return "validation"
    if any(token in normalized for token in ("permission denied", "access denied", "forbidden", "unauthorized")):
        return "permission"
    return "unknown"


def is_retryable_partition_error(error_type: str) -> bool:
    return error_type in {"transient", "unknown"}


def _payload_asset_with_identity(asset: dict[str, Any]) -> dict[str, Any]:
    payload = dict(asset.get("asset_payload") or {})
    payload.setdefault("asset_id", asset.get("asset_id"))
    payload.setdefault("source_uri", asset.get("source_uri"))
    if asset.get("scene_id") is not None:
        payload.setdefault("scene_id", asset.get("scene_id"))
    return payload


def _asset_results(result: dict[str, Any]) -> list[dict[str, Any]]:
    items = result.get("asset_results") if isinstance(result, dict) else None
    if items is None and isinstance(result, dict):
        items = result.get("partition_asset_results")
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def _asset_result_failed(item: dict[str, Any]) -> bool:
    return str(item.get("status") or "").strip().lower() in {"failed", "manual_required", "error"}


def _asset_result_id(item: dict[str, Any]) -> str:
    return str(item.get("asset_id") or "").strip()


def _asset_result_error_type(item: dict[str, Any]) -> str:
    explicit = str(item.get("error_type") or "").strip().lower()
    if explicit:
        return explicit
    error = str(item.get("last_error") or item.get("error") or item.get("error_message") or "")
    return classify_partition_error(error)


def _failed_asset_ids(result: dict[str, Any]) -> list[str]:
    return [_asset_result_id(item) for item in _asset_results(result) if _asset_result_failed(item) and _asset_result_id(item)]


def _retryable_failed_asset_ids(result: dict[str, Any]) -> list[str]:
    return [
        _asset_result_id(item)
        for item in _asset_results(result)
        if _asset_result_failed(item)
        and _asset_result_id(item)
        and is_retryable_partition_error(_asset_result_error_type(item))
    ]


def _text_or_none(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _batch_failure_reason(batch: dict[str, Any]) -> str | None:
    return _text_or_none(batch.get("quality_failure_reason") or batch.get("last_error"))


def _asset_failure_reason(assets: list[dict[str, Any]], asset_ids: list[str]) -> str | None:
    target_ids = set(asset_ids)
    reasons: list[str] = []
    for asset in assets:
        asset_id = str(asset.get("asset_id") or "").strip()
        if asset_id not in target_ids:
            continue
        reason = _text_or_none(asset.get("last_error"))
        if reason:
            reasons.append(f"{asset_id}: {reason}")
    return _joined_reason(reasons)


def _result_failure_reason(result: dict[str, Any], asset_ids: list[str]) -> str | None:
    target_ids = set(asset_ids)
    reasons: list[str] = []
    for item in _asset_results(result):
        asset_id = _asset_result_id(item)
        if asset_id not in target_ids:
            continue
        reason = _text_or_none(item.get("last_error") or item.get("error") or item.get("error_message"))
        if reason:
            reasons.append(f"{asset_id}: {reason}")
    return _joined_reason(reasons)


def _joined_reason(reasons: list[str]) -> str | None:
    if not reasons:
        return None
    return "; ".join(reasons[:3])[:500]


def _manual_required_asset_result(result: dict[str, Any], asset_ids: set[str]) -> dict[str, Any]:
    if not asset_ids:
        return result
    next_result = copy.deepcopy(result)
    for key in ("asset_results", "partition_asset_results"):
        items = next_result.get(key)
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict) and _asset_result_id(item) in asset_ids and _asset_result_failed(item):
                item["status"] = "manual_required"
    return next_result
