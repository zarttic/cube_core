from __future__ import annotations

import copy
import inspect
import time
from collections.abc import Callable
from datetime import datetime, timezone
from threading import Lock
from typing import Any
from uuid import uuid4

from cube_web.services.http_errors import HTTPException
from cube_web.services.partition_contracts import (
    PartitionDatasetResult,
    StrictPartitionRequest,
    effective_dataset_request,
    group_datasets,
    make_output_version,
    resolve_dataset_partition,
)
from cube_web.services.partition_domain_store import get_partition_domain_store
from cube_web.services.partition_job_store import (
    PartitionBatchAlreadyActiveError,
    PartitionBatchArchivedError,
    PartitionJobStore,
    get_partition_job_store,
    normalized_dataset_asset_id,
)
from cube_web.services.partition_service import PartitionService, PartitionTask

ACTIVE_BATCH_RUN_STATUSES = {"queued", "running", "retrying", "cancel_requested"}
ACTIVE_TASK_STATUSES = {"queued", "running", "cancel_requested"}
CANCELLATION_CHECK_INTERVAL_SECONDS = 1.0
TASK_SYNC_WAIT_SECONDS = 30.0


class PartitionCancelledError(RuntimeError):
    """Raised when a dataset attempt is cancelled before its commit."""


class PartitionWorkflowService:
    def __init__(
        self,
        partition_service: PartitionService,
        store: PartitionJobStore | None = None,
        *,
        domain_store: Any | None = None,
        runner: Any | None = None,
    ) -> None:
        self.partition_service = partition_service
        self._store = store
        self.domain_store = domain_store
        self.dataset_runner = runner
        self.after_ray: Callable[[], None] | None = None
        self.task_event_listeners: list[Callable[[str, str, dict[str, Any] | None], None]] = []
        self._run_lock = Lock()

    def add_task_event_listener(self, listener: Callable[[str, str, dict[str, Any] | None], None]) -> None:
        if listener not in self.task_event_listeners:
            self.task_event_listeners.append(listener)

    def _notify_task_event(self, task_id: str, status: str, result: dict[str, Any] | None = None) -> None:
        for listener in tuple(self.task_event_listeners):
            try:
                listener(task_id, status, result)
            except Exception:
                # Projection failures are reconciled independently from task execution.
                continue

    @property
    def store(self) -> PartitionJobStore:
        if self._store is None:
            self._store = get_partition_job_store()
        return self._store

    def run(
        self,
        *,
        task_id: str,
        request: StrictPartitionRequest,
        runner: Any | None = None,
        domain_store: Any | None = None,
        job_store: Any | None = None,
    ) -> dict[str, Any]:
        """Execute and commit each normalized dataset independently."""
        datasets = group_datasets(request)
        selected_runner = runner or self.dataset_runner
        selected_domain_store = domain_store or self.domain_store or get_partition_domain_store()
        selected_job_store = job_store or self.store
        if selected_runner is None:
            raise RuntimeError("dataset runner is required")
        if selected_domain_store is None:
            raise RuntimeError("partition domain store is required")

        results: list[dict[str, Any]] = []
        for dataset_id, dataset in datasets.items():
            output_version = make_output_version(dataset_id, task_id)
            effective_request = effective_dataset_request(request, dataset)
            effective_partition = resolve_dataset_partition(request, dataset)
            started = False
            try:
                started_version = selected_domain_store.start_output(effective_request, dataset, task_id)
                started = True
                if started_version != output_version:
                    raise ValueError("domain store returned a non-deterministic output version")
                result, scene_outcomes = _run_dataset_by_scene(
                    selected_runner,
                    dataset=dataset,
                    task_id=task_id,
                    output_version=output_version,
                    grid_type=effective_request.grid_type,
                    requested_grid_level=effective_request.requested_grid_level,
                    cover_mode=effective_request.cover_mode,
                    max_cells_per_asset=effective_request.max_cells_per_asset,
                    time_granularity=effective_request.time_granularity,
                    max_observations=effective_partition.max_observations,
                )
                if self.after_ray is not None:
                    self.after_ray()
                if _is_cancelled(selected_job_store, task_id):
                    raise PartitionCancelledError("Partition task cancelled")
                if result.dataset_id != dataset_id or result.output_version != output_version or result.task_id != task_id:
                    raise ValueError("dataset result identity does not match the active attempt")
                committed = selected_domain_store.complete_output(result)
                completed_result = _completed_dataset_result(result, committed)
                if scene_outcomes is not None:
                    completed_result["scenes"] = scene_outcomes
                    if any(item["status"] != "completed" for item in scene_outcomes):
                        completed_result["status"] = "partial_failure"
                results.append(completed_result)
            except PartitionCancelledError:
                if started:
                    selected_domain_store.fail_output(
                        dataset_id,
                        output_version,
                        error_code="partition_cancelled",
                        error_message="Partition task cancelled",
                    )
                results.append({"dataset_id": dataset_id, "output_version": output_version, "status": "cancelled"})
            except Exception as exc:
                message = _safe_dataset_error(exc)
                if started:
                    selected_domain_store.fail_output(
                        dataset_id,
                        output_version,
                        error_code="partition_execution_failed",
                        error_message=message,
                    )
                failed_result = {
                        "dataset_id": dataset_id,
                        "output_version": output_version,
                        "status": "failed",
                        "error": {"code": "partition_execution_failed", "message": message},
                    }
                if isinstance(exc, _ScenePartitionFailure):
                    failed_result["scenes"] = exc.outcomes
                results.append(failed_result)

        statuses = [str(item["status"]) for item in results]
        completed = statuses.count("completed")
        if completed == len(statuses):
            status = "completed"
        elif completed or "partial_failure" in statuses:
            status = "partial_failure"
        elif statuses and all(value == "cancelled" for value in statuses):
            status = "cancelled"
        else:
            status = "failed"
        return {"batch_id": request.batch_id, "status": status, "datasets": results}

    def submit_strict(
        self,
        data_type: str,
        request: StrictPartitionRequest,
        *,
        requested_by: str = "operator",
    ) -> PartitionTask:
        """Queue a normalized request whose worker commits to the domain store."""
        if {dataset.data_type for dataset in request.datasets} != {data_type}:
            raise HTTPException(status_code=422, detail="path data_type must match every dataset data_type")
        return self._submit_normalized(data_type, request, requested_by=requested_by)

    def submit_mixed(
        self,
        request: StrictPartitionRequest,
        *,
        requested_by: str = "operator",
    ) -> PartitionTask:
        """Queue a normalized batch whose datasets may have different types."""
        if len({dataset.data_type for dataset in request.datasets}) < 2:
            raise HTTPException(status_code=422, detail="mixed partition batches require at least two dataset data types")
        return self._submit_normalized("mixed", request, requested_by=requested_by)

    def _submit_normalized(
        self,
        data_type: str,
        request: StrictPartitionRequest,
        *,
        requested_by: str,
        operation: str = "auto_run",
        source_task_id: str | None = None,
        retry_strategy: str | None = None,
        failure_reason: str | None = None,
    ) -> PartitionTask:
        """Persist and queue a strict request under its batch-level data type."""
        if self.dataset_runner is None:
            raise RuntimeError("strict partition dataset runner is required")
        group_datasets(request)

        with self._run_lock:
            full_payload = request.model_dump(mode="json")
            full_payload["strict_partition_request"] = True
            full_payload["dataset_partitions"] = _dataset_partitions(request)
            batch = self.store.get_batch(request.batch_id)
            if batch is not None and str(batch.get("data_type") or "") != data_type:
                raise HTTPException(status_code=422, detail=f"Partition batch {request.batch_id} is not a {data_type} batch")
            if batch is None:
                batch = self.store.ensure_runtime_batch(
                    batch_id=request.batch_id,
                    batch_name=request.batch_id,
                    data_type=data_type,
                    payload=full_payload,
                    max_auto_retries=0,
                )
            else:
                active_task = self._active_task_for_batch(batch)
                if active_task is not None:
                    return active_task
                if str(batch.get("source_system") or "") == "runtime":
                    batch = self.store.ensure_runtime_batch(
                        batch_id=request.batch_id,
                        batch_name=str(batch.get("batch_name") or request.batch_id),
                        data_type=data_type,
                        payload=full_payload,
                        max_auto_retries=0,
                    )

            active_task = self._active_task_for_batch(batch)
            if active_task is not None:
                return active_task
            completed_keys = _completed_dataset_partition_keys(self.store.list_attempts(request.batch_id))
            pending_datasets = tuple(
                dataset
                for dataset in request.datasets
                if _dataset_partition_key(_dataset_partition_row(request, dataset)) not in completed_keys
            )
            if not pending_datasets:
                raise HTTPException(status_code=409, detail=f"All requested partition dataset configurations already completed: {request.batch_id}")
            execution_request = request.model_copy(update={"datasets": pending_datasets})
            payload = execution_request.model_dump(mode="json")
            payload["strict_partition_request"] = True
            payload["dataset_partitions"] = _dataset_partitions(execution_request)
            task_id = f"partition-{uuid4().hex[:12]}"
            cancellation_state: dict[str, bool | float | None] = {"last_checked_at": None, "last_result": False}

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
                    batch_id=request.batch_id,
                    operation=operation,
                    payload=payload,
                    requested_by=requested_by,
                    source_task_id=source_task_id,
                    retry_strategy=retry_strategy,
                    failure_reason=failure_reason,
                )
            except (PartitionBatchAlreadyActiveError, PartitionBatchArchivedError) as exc:
                active_task = self._active_task_for_batch(self.get_batch(request.batch_id))
                if active_task is not None:
                    return active_task
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            self.store.mark_batch_queued(request.batch_id, task_id, operation=operation)
            return self.partition_service.task_store.submit(
                data_type,
                "run",
                lambda: _strict_task_result(self.run(task_id=task_id, request=execution_request), execution_request),
                task_id=task_id,
                on_started=self.on_task_started,
                on_succeeded=self.on_task_succeeded,
                on_failed=self.on_task_failed,
                cancellation_check=cancellation_check,
            )

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        batch = self.store.get_batch(batch_id)
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

    def retry_task(self, task_id: str) -> PartitionTask:
        attempt = self.store.get_attempt(task_id)
        if attempt is None:
            raise HTTPException(status_code=404, detail=f"Managed partition task not found: {task_id}")
        task = self.get_task(task_id)
        if task.status not in {"failed", "cancelled", "manual_required"}:
            raise HTTPException(status_code=409, detail=f"Partition task is not retryable: {task.status}")
        batch_id = str(attempt["batch_id"])
        batch = self.get_batch(batch_id)
        if str(batch.get("last_task_id") or "") != task_id:
            raise HTTPException(status_code=409, detail="Only the latest partition task can be retried")
        payload = copy.deepcopy(attempt.get("payload") or {})
        payload.pop("_operation", None)
        payload.pop("_cancellation_check", None)
        payload.pop("cancellation_check", None)
        is_strict = payload.pop("strict_partition_request", False) is True
        payload.pop("dataset_partitions", None)
        if is_strict and payload.get("datasets"):
            try:
                request = StrictPartitionRequest.model_validate(payload)
            except ValueError as exc:
                raise HTTPException(status_code=409, detail="Partition task payload is no longer retryable") from exc
            data_type = str(batch.get("data_type") or "")
            if data_type not in {"mixed", "optical", "radar", "product", "carbon"}:
                raise HTTPException(status_code=409, detail="Partition task has no retryable normalized data type")
            return self._submit_normalized(
                data_type,
                request,
                requested_by="operator",
                operation="manual_retry",
                source_task_id=task_id,
                retry_strategy="full_batch",
                failure_reason=_text_or_none(attempt.get("error_message")) or _text_or_none(batch.get("last_error")),
            )
        raise HTTPException(status_code=409, detail="Partition task payload is not a normalized production request")

    def cancel_task(self, task_id: str) -> dict[str, Any]:
        attempt = self.store.request_cancel(task_id)
        if attempt is not None and attempt.get("status") == "cancelled":
            self._notify_task_event(task_id, "cancelled", None)
        task: PartitionTask | None = None
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
        return self.store.get_attempt(task_id) or attempt

    def on_task_started(self, task_id: str) -> None:
        attempt = self.store.get_attempt(task_id)
        if attempt is not None:
            self.store.start_attempt(task_id)
        self._notify_task_event(task_id, "running")

    def on_task_succeeded(self, task_id: str, result: dict[str, Any]) -> None:
        attempt = self.store.get_attempt(task_id)
        result_status = str(result.get("status") or "completed")
        if attempt is not None:
            self.store.succeed_attempt(task_id, result)
            if result_status == "cancelled":
                self.store.mark_cancelled(task_id)
            elif result_status in {"failed", "partial_failure"}:
                self.store.mark_result_manual_required(
                    task_id,
                    "One or more datasets failed during partition execution",
                    error_type="partition_execution_failed",
                )
        projected_status = "cancelled" if result_status == "cancelled" else ("failed" if result_status in {"failed", "partial_failure"} else result_status)
        self._notify_task_event(task_id, projected_status, result)

    def on_task_failed(self, task_id: str, error: str) -> None:
        attempt = self.store.get_attempt(task_id)
        if attempt is None:
            self._notify_task_event(task_id, "failed", {"error": error})
            return
        if "cancel" in error.lower():
            self.store.mark_cancelled(task_id)
            self._notify_task_event(task_id, "cancelled", {"error": error})
            return
        error_type = classify_partition_error(error)
        self.store.fail_attempt(task_id, error, manual_required=True, error_type=error_type)
        self._notify_task_event(task_id, "failed", {"error": error})

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


    def _refresh_active_attempt(self, task_id: str, attempt: dict[str, Any] | None = None) -> dict[str, Any] | None:
        attempt = attempt or self.store.get_attempt(task_id)
        if attempt is None or str(attempt.get("status") or "") not in ACTIVE_TASK_STATUSES:
            return attempt
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



def classify_partition_error(error: str) -> str:
    normalized = error.lower()
    if any(token in normalized for token in ("not found", "no such file", "no such key", "missing", "does not exist", "source missing")):
        return "source_missing"
    if any(
        token in normalized
        for token in (
            "timed out",
            "timeout",
            "temporarily",
            "temporary",
            "connection reset",
            "connection refused",
            "network",
            "503",
            "502",
            "504",
        )
    ):
        return "transient"
    if any(token in normalized for token in ("invalid", "validation", "bad request", "unsupported", "must be", "required")):
        return "validation"
    if any(token in normalized for token in ("permission denied", "access denied", "forbidden", "unauthorized")):
        return "permission"
    return "unknown"


def _dataset_partitions(request: StrictPartitionRequest) -> list[dict[str, Any]]:
    return [_dataset_partition_row(request, dataset) for dataset in request.datasets]


def _dataset_partition_row(request: StrictPartitionRequest, dataset: Any) -> dict[str, Any]:
    effective = effective_dataset_request(request, dataset)
    resolved = resolve_dataset_partition(request, dataset)
    return {
        "dataset_id": dataset.dataset_id,
        "data_type": dataset.data_type,
        "grid_type": effective.grid_type,
        "requested_grid_level": effective.requested_grid_level,
        "partition_method": effective.partition_method,
        "max_observations": resolved.max_observations or 0,
    }


def _dataset_partition_key(value: Any) -> tuple[str, str, int, str, int] | None:
    if not isinstance(value, dict):
        return None
    dataset_id = str(value.get("dataset_id") or "").strip()
    grid_type = str(value.get("grid_type") or "").strip().lower()
    partition_method = str(value.get("partition_method") or "").strip().lower()
    try:
        grid_level = int(value.get("requested_grid_level"))
        max_observations = int(value.get("max_observations") or 0)
    except (TypeError, ValueError):
        return None
    if not dataset_id or not grid_type or not partition_method:
        return None
    return dataset_id, grid_type, grid_level, partition_method, max_observations


def _dataset_partition_keys(value: Any) -> set[tuple[str, str, int, str, int]]:
    if not isinstance(value, list):
        return set()
    return {key for item in value if (key := _dataset_partition_key(item)) is not None}


def _completed_dataset_partition_keys(attempts: list[dict[str, Any]]) -> set[tuple[str, str, int, str, int]]:
    completed: set[tuple[str, str, int, str, int]] = set()
    for attempt in attempts:
        payload = attempt.get("payload") if isinstance(attempt.get("payload"), dict) else {}
        statuses = _dataset_result_statuses(attempt)
        partitions = payload.get("dataset_partitions") if isinstance(payload.get("dataset_partitions"), list) else []
        if statuses:
            completed.update(
                key
                for partition in partitions
                if (key := _dataset_partition_key(partition)) is not None and statuses.get(key[0]) == "completed"
            )
        elif str(attempt.get("status") or "") == "succeeded":
            completed.update(_dataset_partition_keys(partitions))
    return completed


def _dataset_result_statuses(attempt: dict[str, Any]) -> dict[str, str]:
    result = attempt.get("runner_result") if isinstance(attempt.get("runner_result"), dict) else {}
    datasets = result.get("datasets") if isinstance(result.get("datasets"), list) else []
    return {
        str(item.get("dataset_id")): str(item.get("status") or "")
        for item in datasets
        if isinstance(item, dict) and item.get("dataset_id")
    }


def _task_from_attempt(attempt: dict[str, Any], batch: dict[str, Any]) -> PartitionTask:
    raw_result = attempt.get("runner_result") if isinstance(attempt.get("runner_result"), dict) else None
    result = None
    if raw_result is not None:
        result = dict(raw_result)
        result.setdefault("batch_id", batch.get("batch_id"))
        result.setdefault("batch_name", batch.get("batch_name"))
        result["batch_status"] = batch.get("status")
        for key in (
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


def _text_or_none(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _strict_task_result(result: dict[str, Any], request: StrictPartitionRequest) -> dict[str, Any]:
    """Persist dataset outcomes so mixed retries can skip committed siblings."""
    statuses = {
        str(item.get("dataset_id")): item
        for item in result.get("datasets", [])
        if isinstance(item, dict) and item.get("dataset_id")
    }
    asset_results: list[dict[str, Any]] = []
    for dataset in request.datasets:
        outcome = statuses.get(dataset.dataset_id, {})
        scene_statuses = {
            str(item.get("scene_id")): item
            for item in outcome.get("scenes", ())
            if isinstance(item, dict) and item.get("scene_id")
        }
        status = "succeeded" if outcome.get("status") == "completed" else str(outcome.get("status") or "failed")
        error = outcome.get("error") if isinstance(outcome.get("error"), dict) else {}
        for asset_index, asset in enumerate(dataset.assets):
            scene_id = str((asset.attributes or {}).get("scene_id") or "")
            scene_outcome = scene_statuses.get(scene_id, {})
            asset_status = "succeeded" if scene_outcome.get("status") == "completed" else str(scene_outcome.get("status") or status)
            scene_error = scene_outcome.get("error") if isinstance(scene_outcome.get("error"), dict) else error
            source_uri = asset.source_uri or asset.cog_uri
            source_text = None if source_uri is None else str(source_uri)
            asset_results.append(
                {
                    "asset_id": normalized_dataset_asset_id(request.batch_id, dataset.dataset_id, source_text or "", asset_index),
                    "source_uri": source_text,
                    "status": asset_status,
                    "error_message": scene_error.get("message"),
                }
            )
    return {**result, "asset_results": asset_results}


class _ScenePartitionFailure(RuntimeError):
    def __init__(self, outcomes: list[dict[str, Any]]) -> None:
        self.outcomes = outcomes
        failures = [item.get("error", {}).get("message", "scene failed") for item in outcomes if item["status"] == "failed"]
        super().__init__("; ".join(failures) or "all scenes failed")


def _run_dataset_by_scene(runner: Any, *, dataset: Any, **kwargs: Any) -> tuple[PartitionDatasetResult, list[dict[str, Any]] | None]:
    grouped: dict[str, list[Any]] = {}
    for asset in dataset.assets:
        scene_id = str((asset.attributes or {}).get("scene_id") or "").strip()
        if not scene_id:
            raw = _run_dataset_runner(runner, dataset=dataset, **kwargs)
            return PartitionDatasetResult.model_validate(raw), None
        grouped.setdefault(scene_id, []).append(asset)
    if len(grouped) <= 1:
        raw = _run_dataset_runner(runner, dataset=dataset, **kwargs)
        return PartitionDatasetResult.model_validate(raw), None

    results: list[PartitionDatasetResult] = []
    outcomes: list[dict[str, Any]] = []
    for scene_id, assets in grouped.items():
        asset_ids = {asset.source_asset_id for asset in assets}
        scene_dataset = dataset.model_copy(
            update={
                "assets": tuple(assets),
                "bands": tuple(band for band in dataset.bands if band.source_asset_id in asset_ids),
            }
        )
        try:
            raw = _run_dataset_runner(runner, dataset=scene_dataset, **kwargs)
            result = PartitionDatasetResult.model_validate(raw)
            results.append(result)
            outcomes.append({"scene_id": scene_id, "status": "completed"})
        except Exception as exc:
            outcomes.append(
                {
                    "scene_id": scene_id,
                    "status": "failed",
                    "error": {"code": "partition_execution_failed", "message": _safe_dataset_error(exc)},
                }
            )
    if not results:
        raise _ScenePartitionFailure(outcomes)
    first = results[0]
    combined = first.model_copy(
        update={
            "tiles": _merge_scene_rows(results, "tiles"),
            "indexes": _merge_scene_rows(results, "indexes"),
            "grid_cells": _merge_scene_rows(results, "grid_cells"),
        }
    )
    return combined, outcomes


def _merge_scene_rows(results: list[PartitionDatasetResult], field: str) -> tuple[dict[str, Any], ...]:
    rows: dict[Any, dict[str, Any]] = {}
    for result in results:
        for row in getattr(result, field):
            if field == "grid_cells":
                identity = (
                    row.get("grid_type"), int(row.get("grid_level") or 0),
                    row.get("topology_code"), row.get("space_code"),
                )
                existing = rows.get(identity)
                if existing is not None:
                    comparable = {key: value for key, value in row.items() if key not in {"output_id", "tile_count", "index_count"}}
                    prior = {key: value for key, value in existing.items() if key not in {"output_id", "tile_count", "index_count"}}
                    if comparable != prior:
                        raise ValueError(f"conflicting grid cell geometry across scenes: {identity}")
                    existing["tile_count"] = int(existing.get("tile_count") or 0) + int(row.get("tile_count") or 0)
                    existing["index_count"] = int(existing.get("index_count") or 0) + int(row.get("index_count") or 0)
                    continue
            else:
                identity = str(row.get("output_id") or "")
                if identity in rows and rows[identity] != row:
                    raise ValueError(f"conflicting {field} output_id across scenes: {identity}")
            rows[identity] = dict(row)
    return tuple(rows.values())


def _run_dataset_runner(runner: Any, **kwargs: Any) -> Any:
    execute = getattr(runner, "run_dataset", None)
    if execute is None and callable(runner):
        execute = runner
    if execute is None:
        raise TypeError("dataset runner must expose run_dataset() or be callable")
    try:
        signature = inspect.signature(execute)
        accepts_kwargs = any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in signature.parameters.values())
        arguments = kwargs if accepts_kwargs else {name: value for name, value in kwargs.items() if name in signature.parameters}
        return execute(**arguments)
    except Exception as exc:
        if exc.__class__.__name__ == "PartitionCancelledError":
            raise PartitionCancelledError(str(exc)) from exc
        raise


def _is_cancelled(job_store: Any, task_id: str) -> bool:
    check = getattr(job_store, "is_cancel_requested", None)
    if callable(check):
        return bool(check(task_id))
    return bool(getattr(job_store, "cancelled", False))


def _completed_dataset_result(result: PartitionDatasetResult, committed: Any) -> dict[str, Any]:
    counts = committed.get("counts") if isinstance(committed, dict) else None
    if not isinstance(counts, dict):
        counts = {
            "tiles": len(result.tiles),
            "indexes": len(result.indexes),
            "grid_cells": len(result.grid_cells),
        }
    completed = {
        "dataset_id": result.dataset_id,
        "output_version": result.output_version,
        "status": "completed",
        "counts": {
            "tiles": int(counts.get("tiles") or 0),
            "indexes": int(counts.get("indexes") or 0),
            "grid_cells": int(counts.get("grid_cells") or 0),
        },
    }
    if result.execution_engine:
        completed["execution_engine"] = result.execution_engine
    return completed


def _safe_dataset_error(exc: Exception) -> str:
    message = str(exc).strip() or exc.__class__.__name__
    return message[:1000]
