from __future__ import annotations

import copy
import os
import shlex
import time
from datetime import datetime, timezone
from threading import Lock
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

from cube_split import runtime_config

from cube_web.services.http_errors import HTTPException
from cube_web.services.partition_job_store import (
    InMemoryPartitionJobStore,
    PartitionBatchAlreadyActiveError,
    PartitionBatchArchivedError,
    PartitionJobStore,
    get_partition_job_store,
)
from cube_web.services.partition_service import PartitionService, PartitionTask

ACTIVE_BATCH_RUN_STATUSES = {"queued", "running", "retrying", "cancel_requested"}
ACTIVE_TASK_STATUSES = {"queued", "running", "cancel_requested"}
CANCELLATION_CHECK_INTERVAL_SECONDS = 1.0
RAY_JOB_RUNNING_STATUSES = {"PENDING", "RUNNING"}


class PartitionWorkflowService:
    def __init__(self, partition_service: PartitionService, store: PartitionJobStore | None = None) -> None:
        self.partition_service = partition_service
        self._store = store
        self._run_lock = Lock()

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

    def reconcile_schemas(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            return _reconcile_partition_schemas(self.store, payload)
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

    def archive_batch(self, batch_id: str) -> dict[str, Any]:
        try:
            batch = self.store.archive_batch(batch_id)
        except PartitionBatchAlreadyActiveError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        if batch is None:
            raise HTTPException(status_code=404, detail=f"Partition batch not found: {batch_id}")
        return batch

    def list_tasks(
        self,
        *,
        status: str | None = None,
        data_type: str | None = None,
        keyword: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        offset = (page - 1) * page_size
        return {
            "tasks": self.store.list_tasks(
                status=status,
                data_type=data_type,
                keyword=keyword,
                limit=page_size,
                offset=offset,
            ),
            "total": self.store.count_tasks(status=status, data_type=data_type, keyword=keyword),
            "page": page,
            "page_size": page_size,
        }

    def reconcile_orphaned_tasks(self) -> int:
        resolved = 0
        for status in ACTIVE_TASK_STATUSES:
            for task in self.store.list_tasks(status=status, limit=10_000, offset=0):
                task_id = str(task.get("task_id") or "").strip()
                if not task_id:
                    continue
                attempt = self.store.get_attempt(task_id)
                if attempt is None:
                    continue
                refreshed = self._refresh_active_attempt(task_id, attempt)
                if refreshed is not None and str(refreshed.get("status") or "") not in ACTIVE_TASK_STATUSES:
                    resolved += 1
        return resolved

    def get_task(self, task_id: str) -> PartitionTask:
        attempt = self.store.get_attempt(task_id)
        if attempt is None:
            return self.partition_service.get_task(task_id)
        attempt = self._refresh_active_attempt(task_id, attempt) or attempt
        batch = self.store.get_batch(str(attempt.get("batch_id") or ""))
        return _task_from_attempt(attempt, batch or {})

    def run_payload(
        self,
        data_type: str,
        payload: dict[str, Any] | None = None,
        *,
        requested_by: str = "operator",
    ) -> PartitionTask:
        self.partition_service._resolve(data_type, "run")
        with self._run_lock:
            raw_payload = copy.deepcopy(payload or {})
            task_id = f"partition-{uuid4().hex[:12]}"
            batch_id = _text_or_none(raw_payload.get("batch_id"))
            batch = self.store.get_batch(batch_id) if batch_id else None
            if batch is not None and str(batch.get("data_type") or "") != data_type:
                raise HTTPException(status_code=422, detail=f"Partition batch {batch_id} is not a {data_type} batch")
            if batch is None:
                batch_id = batch_id or f"runtime-{task_id}"
                batch = self.store.ensure_runtime_batch(
                    batch_id=batch_id,
                    batch_name=_text_or_none(raw_payload.get("batch_name")) or batch_id,
                    data_type=data_type,
                    payload=raw_payload,
                    max_auto_retries=0,
                )
            else:
                active_task = self._active_task_for_batch(batch)
                if active_task is not None:
                    return active_task
                if str(batch.get("source_system") or "") == "runtime":
                    batch = self.store.ensure_runtime_batch(
                        batch_id=str(batch["batch_id"]),
                        batch_name=_text_or_none(raw_payload.get("batch_name")) or str(batch.get("batch_name") or batch["batch_id"]),
                        data_type=data_type,
                        payload=raw_payload,
                        max_auto_retries=0,
                    )
            active_task = self._active_task_for_batch(batch)
            if active_task is not None:
                return active_task

            asset_ids = self._selected_asset_ids_for_payload(str(batch["batch_id"]), str(batch["data_type"]), raw_payload)
            cancellation_state = {"last_checked_at": None, "last_result": False}

            def cancellation_check() -> bool:
                now = time.monotonic()
                last_checked_at = cancellation_state["last_checked_at"]
                if last_checked_at is not None and now - last_checked_at < CANCELLATION_CHECK_INTERVAL_SECONDS:
                    return bool(cancellation_state["last_result"])
                result = self.store.is_cancel_requested(task_id)
                cancellation_state["last_checked_at"] = now
                cancellation_state["last_result"] = result
                return bool(result)

            try:
                self.store.create_attempt(
                    task_id=task_id,
                    batch_id=str(batch["batch_id"]),
                    operation="auto_run",
                    payload=raw_payload,
                    asset_ids=asset_ids,
                    requested_by=requested_by,
                )
            except (PartitionBatchAlreadyActiveError, PartitionBatchArchivedError) as exc:
                active_task = self._active_task_for_batch(self.get_batch(str(batch["batch_id"])))
                if active_task is not None:
                    return active_task
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            self.store.mark_batch_queued(str(batch["batch_id"]), task_id, operation="auto_run")
            return self._submit_attempt(
                task_id=task_id,
                batch=batch,
                data_type=data_type,
                payload=raw_payload,
                cancellation_check=cancellation_check,
            )

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
        with self._run_lock:
            batch = self.get_batch(batch_id)
            active_task = self._active_task_for_batch(batch)
            if active_task is not None:
                return active_task
            payload = self._payload_for_batch(batch, config_override=config_override, asset_ids=asset_ids)
            task_id = f"partition-{uuid4().hex[:12]}"
            cancellation_state = {"last_checked_at": None, "last_result": False}

            def cancellation_check() -> bool:
                now = time.monotonic()
                last_checked_at = cancellation_state["last_checked_at"]
                if last_checked_at is not None and now - last_checked_at < CANCELLATION_CHECK_INTERVAL_SECONDS:
                    return bool(cancellation_state["last_result"])
                result = self.store.is_cancel_requested(task_id)
                cancellation_state["last_checked_at"] = now
                cancellation_state["last_result"] = result
                return bool(result)

            try:
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
            except (PartitionBatchAlreadyActiveError, PartitionBatchArchivedError) as exc:
                active_task = self._active_task_for_batch(self.get_batch(batch_id))
                if active_task is not None:
                    return active_task
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            self.store.mark_batch_queued(batch_id, task_id, operation=operation)
            data_type = str(batch["data_type"])
            return self._submit_attempt(
                task_id=task_id,
                batch=batch,
                data_type=data_type,
                payload=payload,
                cancellation_check=cancellation_check,
            )

    def retry_batch(self, batch_id: str, config_override: dict[str, Any] | None = None) -> PartitionTask:
        batch = self.get_batch(batch_id)
        source_assets = self.store.list_assets(batch_id)
        quality_asset_ids = _quality_warning_retry_asset_ids(
            batch,
            source_assets,
            self.store.list_attempts(batch_id),
        )
        return self.run_batch(
            batch_id,
            operation="manual_retry",
            config_override=config_override,
            asset_ids=quality_asset_ids,
            requested_by="operator",
            source_task_id=_text_or_none(batch.get("last_task_id")),
            retry_strategy="quality_warning_assets" if quality_asset_ids else "full_batch",
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
        retryable_statuses = {"failed", "manual_required"}
        non_retryable = [
            asset["asset_id"]
            for asset in source_assets
            if asset["asset_id"] in set(asset_ids) and str(asset.get("status") or "") not in retryable_statuses
        ]
        if non_retryable:
            raise HTTPException(
                status_code=422,
                detail=f"asset_ids are not retryable: {', '.join(non_retryable[:5])}",
            )
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
        task: PartitionTask | None = None
        if attempt is not None and self._attempt_uses_remote_ray(attempt):
            self._stop_remote_attempt(task_id)
            refreshed = self._refresh_active_attempt(task_id, self.store.get_attempt(task_id) or attempt)
            return refreshed or attempt
        try:
            task = self.partition_service.cancel_task(task_id)
        except HTTPException as exc:
            if attempt is not None and exc.status_code == 404:
                cancelled = self.store.mark_cancelled(task_id)
                if cancelled is not None:
                    return cancelled
            if attempt is None or exc.status_code != 404:
                raise
        if attempt is None:
            if task is None:
                raise HTTPException(status_code=404, detail=f"Partition task not found: {task_id}")
            return task.to_dict()
        return attempt

    def on_task_started(self, task_id: str) -> None:
        attempt = self.store.get_attempt(task_id)
        if attempt is not None:
            self.store.start_attempt(task_id)

    def on_task_succeeded(self, task_id: str, result: dict[str, Any]) -> None:
        attempt = self.store.get_attempt(task_id)
        if attempt is not None:
            batch = self.get_batch(attempt["batch_id"])
            max_auto_retries = _max_auto_retries(batch)
            auto_retries_used = _auto_retries_used_in_chain(self.store.list_attempts(attempt["batch_id"]), task_id)
            scoped_asset_ids = set(attempt.get("asset_ids") or [])
            retryable_asset_ids = _retryable_failed_asset_ids(result, scoped_asset_ids or None)
            all_failed_asset_ids = _failed_asset_ids(result, scoped_asset_ids or None)
            should_auto_retry = (
                bool(retryable_asset_ids)
                and auto_retries_used < max_auto_retries
                and attempt.get("operation") in {"auto_run", "auto_retry", "manual_retry", "manual_asset_retry"}
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
        max_auto_retries = _max_auto_retries(batch)
        auto_retries_used = _auto_retries_used_in_chain(self.store.list_attempts(attempt["batch_id"]), task_id)
        error_type = classify_partition_error(error)
        should_auto_retry = (
            is_retryable_partition_error(error_type)
            and auto_retries_used < max_auto_retries
            and attempt.get("operation") in {"auto_run", "auto_retry", "manual_retry", "manual_asset_retry"}
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

    def _selected_asset_ids_for_payload(self, batch_id: str, data_type: str, payload: dict[str, Any]) -> list[str] | None:
        key = "selected_observations" if data_type == "carbon" else "selected_assets"
        selected = payload.get(key)
        if not isinstance(selected, list) or not selected:
            return None
        assets = self.store.list_assets(batch_id)
        matched: list[str] = []
        for item in selected:
            if not isinstance(item, dict):
                continue
            match = _find_asset_for_payload_item(assets, item)
            if match is not None and match.get("asset_id"):
                matched.append(str(match["asset_id"]))
        return matched or None

    def _active_task_for_batch(self, batch: dict[str, Any]) -> PartitionTask | None:
        if str(batch.get("status") or "") not in ACTIVE_BATCH_RUN_STATUSES:
            return None
        task_id = str(batch.get("last_task_id") or "").strip()
        if not task_id:
            return None
        attempt = self.store.get_attempt(task_id)
        if attempt is None:
            return None
        attempt = self._refresh_active_attempt(task_id, attempt) or attempt
        if str(attempt.get("status") or "") in ACTIVE_TASK_STATUSES:
            return _task_from_attempt(attempt, batch)
        return None

    def _submit_attempt(
        self,
        *,
        task_id: str,
        batch: dict[str, Any],
        data_type: str,
        payload: dict[str, Any],
        cancellation_check,
    ) -> PartitionTask:
        if self._payload_uses_remote_ray(data_type, payload):
            self._submit_remote_ray_job(
                task_id=task_id,
                batch_id=str(batch["batch_id"]),
                data_type=data_type,
                payload=payload,
            )
            attempt = self.store.get_attempt(task_id)
            return _task_from_attempt(attempt or {"task_id": task_id, "status": "queued", "operation": "auto_run"}, batch)
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

    def _payload_uses_remote_ray(self, data_type: str, payload: dict[str, Any] | None) -> bool:
        return (
            self._supports_remote_ray_jobs()
            and self._effective_partition_backend(data_type, payload) == "ray"
            and bool(self._resolved_ray_address(payload))
        )

    def _supports_remote_ray_jobs(self) -> bool:
        configured = getattr(self.store, "supports_remote_jobs", None)
        if configured is not None:
            return bool(configured)
        return not isinstance(self.store, InMemoryPartitionJobStore)

    def _attempt_uses_remote_ray(self, attempt: dict[str, Any], batch: dict[str, Any] | None = None) -> bool:
        raw_payload = attempt.get("payload")
        payload: dict[str, Any] = raw_payload if isinstance(raw_payload, dict) else {}
        resolved_batch: dict[str, Any] = batch or self.store.get_batch(str(attempt.get("batch_id") or "")) or {}
        data_type = str(resolved_batch.get("data_type") or payload.get("data_type") or "").strip().lower()
        return self._payload_uses_remote_ray(data_type, payload)

    def _effective_partition_backend(self, data_type: str, payload: dict[str, Any] | None) -> str:
        options = payload if isinstance(payload, dict) else {}
        requested = str(options.get("partition_backend") or self._default_partition_backend(data_type)).strip().lower()
        ray_address = self._resolved_ray_address(options)
        if data_type == "carbon":
            if requested == "auto":
                return "ray" if ray_address else "process"
            return requested
        if requested == "auto":
            return "ray" if ray_address else "thread"
        if requested in {"local", "process"}:
            return "thread"
        return requested

    def _default_partition_backend(self, data_type: str) -> str:
        if data_type == "radar":
            return "thread"
        if data_type == "carbon":
            return str(os.environ.get("CUBE_WEB_CARBON_PARTITION_BACKEND", "ray") or "ray")
        return "ray"

    def _resolved_ray_address(self, payload: dict[str, Any] | None = None) -> str:
        options = payload if isinstance(payload, dict) else {}
        return str(options.get("ray_address") or runtime_config.ray_address() or "").strip()

    def _submit_remote_ray_job(
        self,
        *,
        task_id: str,
        batch_id: str,
        data_type: str,
        payload: dict[str, Any],
    ) -> None:
        client = _build_ray_job_client(self._resolved_ray_address(payload))
        try:
            client.submit_job(
                entrypoint=f"python3.11 -m cube_web.services.partition_remote_job --task-id {shlex.quote(task_id)}",
                submission_id=task_id,
                runtime_env=_ray_job_runtime_env(),
                metadata={
                    "task_id": task_id,
                    "batch_id": batch_id,
                    "data_type": data_type,
                    "operation": "run",
                },
            )
        except Exception as exc:
            self.store.fail_attempt(
                task_id,
                _error_text(exc),
                manual_required=True,
                error_type="ray_job_submit_failed",
            )
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    def _refresh_active_attempt(self, task_id: str, attempt: dict[str, Any] | None = None) -> dict[str, Any] | None:
        attempt = attempt or self.store.get_attempt(task_id)
        if attempt is None or str(attempt.get("status") or "") not in ACTIVE_TASK_STATUSES:
            return attempt
        if self._attempt_uses_remote_ray(attempt):
            self._reconcile_remote_attempt(task_id, attempt)
        else:
            self._reconcile_local_attempt(task_id)
        return self.store.get_attempt(task_id) or attempt

    def _reconcile_local_attempt(self, task_id: str) -> None:
        try:
            current = self.partition_service.get_task(task_id)
        except HTTPException as exc:
            if exc.status_code != 404:
                raise
            self.store.mark_cancelled(task_id)
            return
        if current.status == "cancelled":
            self.store.mark_cancelled(task_id)
            return
        if current.status == "failed":
            self.store.fail_attempt(
                task_id,
                current.error or "Partition task failed",
                manual_required=True,
                error_type="local_task_failed",
            )
            return
        if current.status == "completed" and isinstance(current.result, dict):
            self.store.succeed_attempt(task_id, current.result)

    def _reconcile_remote_attempt(self, task_id: str, attempt: dict[str, Any]) -> None:
        try:
            client = _build_ray_job_client(self._resolved_ray_address(attempt.get("payload") if isinstance(attempt.get("payload"), dict) else {}))
            status = _ray_job_status_text(client.get_job_status(task_id))
        except Exception as exc:
            if _is_missing_ray_job_error(exc):
                if str(attempt.get("status") or "") in {"cancel_requested", "cancelled"}:
                    self.store.mark_cancelled(task_id)
                else:
                    self.store.fail_attempt(
                        task_id,
                        f"Ray job not found: {task_id}",
                        manual_required=True,
                        error_type="ray_job_missing",
                    )
            return
        if status == "RUNNING" and str(attempt.get("status") or "") == "queued":
            self.store.start_attempt(task_id)
            return
        if status in RAY_JOB_RUNNING_STATUSES:
            return
        if status == "STOPPED":
            self.store.mark_cancelled(task_id)
            return
        if status == "SUCCEEDED":
            refreshed = self.store.get_attempt(task_id)
            if refreshed is not None and str(refreshed.get("status") or "") in ACTIVE_TASK_STATUSES:
                self.store.fail_attempt(
                    task_id,
                    "Ray job succeeded but partition result was not persisted",
                    manual_required=True,
                    error_type="ray_job_state_mismatch",
                )
            return
        if status == "FAILED":
            info = None
            try:
                info = client.get_job_info(task_id)
            except Exception:
                info = None
            error_message = _ray_job_failure_message(info) or f"Ray job failed: {task_id}"
            if "cancel" in error_message.lower():
                self.store.mark_cancelled(task_id)
                return
            self.store.fail_attempt(
                task_id,
                error_message,
                manual_required=True,
                error_type="ray_job_failed",
            )

    def _stop_remote_attempt(self, task_id: str) -> None:
        attempt = self.store.get_attempt(task_id)
        if attempt is None or not self._attempt_uses_remote_ray(attempt):
            return
        try:
            client = _build_ray_job_client(self._resolved_ray_address(attempt.get("payload") if isinstance(attempt.get("payload"), dict) else {}))
            client.stop_job(task_id)
        except Exception:
            return


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


def _ray_job_runtime_env() -> dict[str, Any]:
    from cube_split.jobs.ray_logical_partition_job import _ray_runtime_env_from_env

    runtime_env = dict(_ray_runtime_env_from_env() or {})
    env_vars = dict(runtime_env.get("env_vars") or {})
    minio = runtime_config.minio_settings()
    resolved_defaults = {
        "CUBE_WEB_POSTGRES_DSN": runtime_config.postgres_dsn(),
        "CUBE_WEB_RAY_ADDRESS": runtime_config.ray_address(),
        "CUBE_WEB_MINIO_ENDPOINT": minio.endpoint,
        "CUBE_WEB_MINIO_ACCESS_KEY": minio.access_key,
        "CUBE_WEB_MINIO_SECRET_KEY": minio.secret_key,
        "CUBE_WEB_MINIO_BUCKET": minio.bucket,
        "RAY_OVERRIDE_JOB_RUNTIME_ENV": "1",
    }
    for name in (
        "CUBE_WEB_POSTGRES_DSN",
        "POSTGRES_DSN",
        "DATABASE_URL",
        "CUBE_WEB_RAY_ADDRESS",
        "RAY_ADDRESS",
        "CUBE_WEB_MINIO_ENDPOINT",
        "MINIO_ENDPOINT",
        "CUBE_WEB_MINIO_ACCESS_KEY",
        "MINIO_ACCESS_KEY",
        "CUBE_WEB_MINIO_SECRET_KEY",
        "MINIO_SECRET_KEY",
        "CUBE_WEB_MINIO_BUCKET",
        "MINIO_BUCKET",
        "CUBE_WEB_CARBON_PARTITION_BACKEND",
        "CUBE_WEB_ENV_FILE",
        "RAY_OVERRIDE_JOB_RUNTIME_ENV",
    ):
        value = os.environ.get(name)
        if not value:
            value = resolved_defaults.get(name, "")
        if value:
            env_vars[name] = value
    if env_vars:
        runtime_env["env_vars"] = env_vars
    return runtime_env


def _build_ray_job_client(ray_address: str):
    from ray.job_submission import JobSubmissionClient

    dashboard_url = _ray_job_dashboard_url(ray_address)
    if not dashboard_url:
        raise RuntimeError("Ray address is required for durable partition jobs")
    return JobSubmissionClient(dashboard_url)


def _ray_job_dashboard_url(ray_address: str) -> str:
    address = str(ray_address or "").strip()
    if not address:
        return ""
    if address.startswith("http://") or address.startswith("https://"):
        return address.rstrip("/")
    if address.startswith("ray://"):
        parsed = urlparse(address)
        host = parsed.hostname or parsed.netloc.replace("ray://", "")
        return f"http://{host}:8265"
    host = address.split("://", 1)[-1].split("/", 1)[0]
    if ":" in host:
        host = host.rsplit(":", 1)[0]
    return f"http://{host}:8265"


def _ray_job_status_text(status: Any) -> str:
    value = getattr(status, "value", status)
    return str(value or "").strip().upper()


def _ray_job_failure_message(info: Any) -> str | None:
    for name in ("message", "error_type"):
        value = getattr(info, name, None)
        if value:
            return str(value)
    if isinstance(info, dict):
        for name in ("message", "error_type"):
            value = info.get(name)
            if value:
                return str(value)
    return None


def _is_missing_ray_job_error(exc: Exception) -> bool:
    message = _error_text(exc).lower()
    return "job does not exist" in message or "does not exist" in message or "not found" in message


def _error_text(exc: Exception) -> str:
    text = str(exc).strip()
    return text or exc.__class__.__name__


def _reconcile_partition_schemas(store: PartitionJobStore, payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("payload must be an object")
    source_system = _text_or_none(payload.get("source_system")) or "loader"
    batch_ids = _normalized_string_list(payload.get("batch_ids"))
    asset_ids = _normalized_string_list(payload.get("asset_ids"))
    observation_ids = _normalized_string_list(payload.get("observation_ids"))
    updated_since = _text_or_none(payload.get("updated_since"))
    include_assets = bool(payload.get("include_assets", True))
    include_attempts = bool(payload.get("include_attempts", False))
    if not batch_ids and not asset_ids and not observation_ids and not updated_since:
        raise ValueError("one of batch_ids, asset_ids, observation_ids, or updated_since is required")
    updated_since_dt = _parse_optional_iso_datetime(updated_since, "updated_since")

    requested_batch_rows = store.list_received_batches(batch_ids=batch_ids, source_system=source_system) if batch_ids else []
    matched_assets = store.list_received_assets(asset_ids=asset_ids, source_system=source_system) if asset_ids else []
    matched_observations = (
        store.list_received_observations(observation_ids=observation_ids, source_system=source_system)
        if observation_ids
        else []
    )
    updated_batches = store.list_received_batches(updated_since=updated_since_dt, source_system=source_system) if updated_since_dt is not None else []

    related_batch_ids = [
        *[str(row.get("batch_key") or "") for row in matched_assets],
        *[str(row.get("batch_key") or "") for row in matched_observations],
    ]
    fetched_batch_map = {
        str(row["batch_id"]): row
        for row in [*requested_batch_rows, *updated_batches]
    }
    missing_related_batch_ids = [
        batch_id
        for batch_id in related_batch_ids
        if batch_id and batch_id not in fetched_batch_map
    ]
    if missing_related_batch_ids:
        for row in store.list_received_batches(batch_ids=missing_related_batch_ids, source_system=source_system):
            fetched_batch_map[str(row["batch_id"])] = row

    ordered_batch_ids: list[str] = []
    seen_batch_ids: set[str] = set()
    for batch_id in [*batch_ids, *related_batch_ids, *[str(row["batch_id"]) for row in updated_batches]]:
        if batch_id and batch_id not in seen_batch_ids:
            seen_batch_ids.add(batch_id)
            ordered_batch_ids.append(batch_id)

    known_batch_ids = [batch_id for batch_id in ordered_batch_ids if batch_id in fetched_batch_map]
    all_assets = store.list_received_assets(batch_ids=known_batch_ids, source_system=source_system) if known_batch_ids else []
    all_observations = store.list_received_observations(batch_ids=known_batch_ids, source_system=source_system) if known_batch_ids else []
    members_by_batch: dict[str, list[dict[str, Any]]] = {}
    for row in [*all_assets, *all_observations]:
        members_by_batch.setdefault(str(row.get("batch_key") or ""), []).append(row)

    batches: list[dict[str, Any]] = []
    for batch_id in ordered_batch_ids:
        batch = fetched_batch_map.get(batch_id)
        if batch is None:
            batches.append({"batch_id": batch_id, "known": False, "status": "missing"})
            continue
        members = members_by_batch.get(batch_id, [])
        batches.append(_reconcile_received_batch_row(batch, members, include_assets=include_assets, include_attempts=include_attempts))

    matched_asset_ids = {
        str(asset["asset_id"])
        for asset in matched_assets
        if asset.get("asset_id")
    }
    matched_observation_ids = {
        str(observation["observation_id"])
        for observation in matched_observations
        if observation.get("observation_id")
    }
    missing_batch_ids = [batch_id for batch_id in batch_ids if batch_id not in set(known_batch_ids)]
    missing_asset_ids = [asset_id for asset_id in asset_ids if asset_id not in matched_asset_ids]
    missing_observation_ids = [
        observation_id for observation_id in observation_ids if observation_id not in matched_observation_ids
    ]
    return {
        "source_system": source_system,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "batches": batches,
        "missing_batch_ids": missing_batch_ids,
        "missing_asset_ids": missing_asset_ids,
        "missing_observation_ids": missing_observation_ids,
        "summary": {
            "requested_batches": len(batch_ids),
            "known_batches": len(known_batch_ids),
            "missing_batches": len(missing_batch_ids),
            "requested_assets": len(asset_ids),
            "known_assets": len(matched_asset_ids),
            "missing_assets": len(missing_asset_ids),
            "requested_observations": len(observation_ids),
            "known_observations": len(matched_observation_ids),
            "missing_observations": len(missing_observation_ids),
        },
    }


def _normalized_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("batch_ids, asset_ids, and observation_ids must be arrays when provided")
    rows: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        rows.append(text)
    return rows


def _parse_optional_iso_datetime(value: str | None, label: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError(f"{label} must be an ISO8601 datetime") from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed


def _asset_counts(assets: list[dict[str, Any]]) -> dict[str, int]:
    return {"total": len(assets)}


def _reconcile_received_batch_row(
    batch: dict[str, Any],
    members: list[dict[str, Any]],
    *,
    include_assets: bool,
    include_attempts: bool,
) -> dict[str, Any]:
    return {
        "batch_id": batch.get("batch_id"),
        "known": True,
        "data_type": batch.get("data_type"),
        "batch_name": batch.get("batch_name"),
        "source_system": batch.get("source_system"),
        "status": batch.get("status") or "pending",
        "loaded_at": _iso_datetime_or_none(batch.get("loaded_at")),
        "updated_at": _iso_datetime_or_none(batch.get("updated_at")),
        "raw_meta_uri": batch.get("raw_meta_uri"),
        "asset_counts": _asset_counts(members),
        "assets": [_reconcile_received_member_row(member) for member in members] if include_assets else [],
        "attempts": [] if include_attempts else [],
    }


def _reconcile_received_member_row(member: dict[str, Any]) -> dict[str, Any]:
    if member.get("asset_id"):
        return {
            "kind": "asset",
            "asset_id": member.get("asset_id"),
            "source_uri": member.get("source_uri"),
            "scene_id": member.get("scene_id"),
        }
    return {
        "kind": "observation",
        "observation_id": member.get("observation_id"),
        "source_uri": member.get("source_uri"),
        "source_index": member.get("source_index"),
    }


def _iso_datetime_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return None
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def _task_from_attempt(attempt: dict[str, Any], batch: dict[str, Any]) -> PartitionTask:
    raw_result = attempt.get("runner_result") if isinstance(attempt.get("runner_result"), dict) else None
    result = None
    if raw_result is not None:
        result = dict(raw_result)
        result.setdefault("batch_id", batch.get("batch_id"))
        result.setdefault("batch_name", batch.get("batch_name"))
        result["batch_status"] = batch.get("status")
        for key in (
            "quality_status",
            "quality_report_id",
            "quality_failure_reason",
            "ingest_status",
            "ingest_job_id",
            "ingest_error",
            "ingested_at",
        ):
            if batch.get(key) is not None:
                result[key] = batch.get(key)
    return PartitionTask(
        task_id=str(attempt.get("task_id") or ""),
        status=_task_response_status(str(attempt.get("status") or "")),
        data_type=str(batch.get("data_type") or (result or {}).get("data_type") or ""),
        operation=_task_response_operation(str(attempt.get("operation") or "")),
        created_at=_timestamp_or_now(attempt.get("created_at")),
        updated_at=_timestamp_or_now(attempt.get("updated_at")),
        result=result,
        error=_text_or_none(attempt.get("error_message")),
    )


def _task_response_status(status: str) -> str:
    return "completed" if status == "succeeded" else status


def _task_response_operation(operation: str) -> str:
    if operation.endswith("_run"):
        return "run"
    if operation.endswith("_retry"):
        return "retry"
    return operation


def _timestamp_or_now(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, datetime):
        timestamp = value
    elif isinstance(value, str) and value.strip():
        try:
            timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return time.time()
    else:
        return time.time()
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.timestamp()


def is_retryable_partition_error(error_type: str) -> bool:
    return error_type in {"transient", "unknown"}


def _payload_asset_with_identity(asset: dict[str, Any]) -> dict[str, Any]:
    payload = dict(asset.get("asset_payload") or {})
    payload.setdefault("asset_id", asset.get("asset_id"))
    payload.setdefault("source_uri", asset.get("source_uri"))
    if asset.get("scene_id") is not None:
        payload.setdefault("scene_id", asset.get("scene_id"))
    return payload


def _find_asset_for_payload_item(assets: list[dict[str, Any]], item: dict[str, Any]) -> dict[str, Any] | None:
    asset_id = str(item.get("asset_id") or "").strip()
    if asset_id:
        for asset in assets:
            if str(asset.get("asset_id") or "") == asset_id:
                return asset
    source_uri = str(item.get("source_uri") or "").strip()
    scene_id = str(item.get("scene_id") or item.get("product_year") or item.get("observation_id") or item.get("source_index") or "").strip()
    for asset in assets:
        if source_uri and str(asset.get("source_uri") or "") == source_uri:
            return asset
        if scene_id and str(asset.get("scene_id") or "") == scene_id:
            return asset
    return None


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


def _failed_asset_ids(result: dict[str, Any], scoped_asset_ids: set[str] | None = None) -> list[str]:
    return [
        _asset_result_id(item)
        for item in _asset_results(result)
        if _asset_result_failed(item)
        and _asset_result_id(item)
        and (scoped_asset_ids is None or _asset_result_id(item) in scoped_asset_ids)
    ]


def _retryable_failed_asset_ids(result: dict[str, Any], scoped_asset_ids: set[str] | None = None) -> list[str]:
    return [
        _asset_result_id(item)
        for item in _asset_results(result)
        if _asset_result_failed(item)
        and _asset_result_id(item)
        and (scoped_asset_ids is None or _asset_result_id(item) in scoped_asset_ids)
        and is_retryable_partition_error(_asset_result_error_type(item))
    ]


def _auto_retries_used_in_chain(attempts: list[dict[str, Any]], task_id: str) -> int:
    by_task_id = {str(attempt.get("task_id") or ""): attempt for attempt in attempts}
    used = 0
    current_id = task_id
    seen: set[str] = set()
    while current_id and current_id not in seen:
        seen.add(current_id)
        attempt = by_task_id.get(current_id)
        if not attempt:
            break
        operation = str(attempt.get("operation") or "")
        if operation in {"manual_retry", "manual_asset_retry"}:
            break
        if operation == "auto_retry":
            used += 1
        current_id = str(attempt.get("source_task_id") or "")
    return used


def _max_auto_retries(batch: dict[str, Any]) -> int:
    value = batch.get("max_auto_retries")
    if value is None or value == "":
        return 1
    return max(0, int(value))


def _quality_warning_retry_asset_ids(
    batch: dict[str, Any],
    assets: list[dict[str, Any]],
    attempts: list[dict[str, Any]],
) -> list[str] | None:
    if str(batch.get("quality_status") or "").strip().upper() != "WARN":
        return None
    report = _latest_quality_report(attempts)
    warning_paths = _quality_warning_paths(report)
    if not warning_paths:
        return None
    asset_ids = [
        str(asset.get("asset_id") or "")
        for asset in assets
        if any(_asset_matches_warning_path(asset, warning_path) for warning_path in warning_paths)
        and str(asset.get("asset_id") or "")
    ]
    return asset_ids or None


def _latest_quality_report(attempts: list[dict[str, Any]]) -> dict[str, Any]:
    for attempt in attempts:
        result = attempt.get("runner_result")
        if not isinstance(result, dict):
            continue
        report = result.get("quality_report")
        if isinstance(report, dict):
            return report
    return {}


def _quality_warning_paths(report: dict[str, Any]) -> set[str]:
    checks = report.get("checks") if isinstance(report, dict) else None
    if not isinstance(checks, list):
        return set()
    paths: set[str] = set()
    for check in checks:
        if not isinstance(check, dict) or str(check.get("status") or "").upper() != "WARN":
            continue
        metrics = check.get("metrics") or {}
        if not isinstance(metrics, dict):
            continue
        for item in metrics.get("zero_assets") or []:
            if isinstance(item, dict) and item.get("path"):
                paths.add(str(item["path"]))
        for item in metrics.get("duplicates") or []:
            if isinstance(item, dict):
                paths.update(str(path) for path in item.get("asset_paths") or [] if path)
    return paths


def _asset_matches_warning_path(asset: dict[str, Any], warning_path: str) -> bool:
    source_uri = str(asset.get("source_uri") or "")
    if not source_uri:
        return False
    warning = _path_like_parts(warning_path)
    source = _path_like_parts(source_uri)
    if source_uri == warning_path or source[-1:] == warning[-1:]:
        return True
    if len(source) >= 1 and len(warning) >= 1:
        source_name = source[-1]
        warning_name = warning[-1]
        source_stem, source_suffix = _split_name(source_name)
        warning_stem, warning_suffix = _split_name(warning_name)
        if warning_stem == f"{source_stem}_cog" and warning_suffix.lower() == source_suffix.lower():
            return True
        if (
            warning_suffix.lower() == source_suffix.lower()
            and warning_stem.startswith(f"{source_stem}_")
            and warning_stem.endswith("_cog")
        ):
            return True
    return len(source) <= len(warning) and tuple(warning[-len(source) :]) == tuple(source)


def _path_like_parts(value: str) -> tuple[str, ...]:
    text = str(value or "").strip().replace("\\", "/")
    if text.startswith("s3://"):
        text = text[5:]
    return tuple(part for part in text.split("/") if part)


def _split_name(name: str) -> tuple[str, str]:
    if "." not in name:
        return name, ""
    stem, suffix = name.rsplit(".", 1)
    return stem, f".{suffix}"


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
