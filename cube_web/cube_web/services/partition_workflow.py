from __future__ import annotations

import copy
import inspect
import logging
import time
from collections.abc import Callable
from datetime import datetime, timezone
from threading import Lock
from typing import Any
from uuid import uuid4

from cube_split import runtime_config
from cube_split.partition_timing import TimingRecorder

from cube_web.services.http_errors import HTTPException
from cube_web.services.partition_contracts import (
    DatasetInput,
    PartitionDatasetResult,
    StrictPartitionRequest,
    effective_dataset_request,
    group_datasets,
    make_output_version,
    resolve_dataset_partition,
)
from cube_web.services.partition_domain_store import get_partition_domain_store
from cube_web.services.partition_job_store import (
    InMemoryPartitionJobStore,
    PartitionBatchAlreadyActiveError,
    PartitionBatchArchivedError,
    PartitionJobStore,
    get_partition_job_store,
    normalized_dataset_asset_id,
)
from cube_web.services.partition_service import PartitionService, PartitionTask
from cube_web.services.ray_job_submitter import (
    RayJobPartitionSubmitter,
    job_client_timeout_seconds,
    ray_job_executor_enabled,
)

ACTIVE_BATCH_RUN_STATUSES = {"queued", "running", "retrying", "cancel_requested"}
ACTIVE_TASK_STATUSES = {"queued", "running", "cancel_requested"}
CANCELLATION_CHECK_INTERVAL_SECONDS = 1.0
TASK_SYNC_WAIT_SECONDS = 30.0
RAY_JOB_SUBMISSION_GRACE_SECONDS = 5.0
logger = logging.getLogger(__name__)


def ray_batch_scheduler_enabled() -> bool:
    return runtime_config.bool_option(runtime_config.env_text("CUBE_WEB_RAY_BATCH_SCHEDULER", "0"), default=False)


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
        ray_job_submitter: Any | None = None,
        scene_repository: Any | None = None,
    ) -> None:
        self.partition_service = partition_service
        self._store = store
        self.domain_store = domain_store
        self.dataset_runner = runner
        self.ray_job_submitter = ray_job_submitter
        self.scene_repository = scene_repository
        self.after_ray: Callable[[], None] | None = None
        self.task_event_listeners: list[Callable[[str, str, dict[str, Any] | None], None]] = []
        # Submission is serialized per batch: the job store already rejects a
        # second active attempt for one batch, so batches must not block each
        # other on a process-wide lock. Locks are never evicted; the number of
        # distinct batches per process stays small.
        self._batch_locks: dict[str, Lock] = {}
        self._batch_locks_guard = Lock()

    def _lock_for_batch(self, batch_id: str) -> Lock:
        with self._batch_locks_guard:
            return self._batch_locks.setdefault(batch_id, Lock())

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
        run_batch = getattr(selected_runner, "run_datasets", None)
        if callable(run_batch) and ray_batch_scheduler_enabled():
            return self._run_ray_batch(
                task_id=task_id,
                request=request,
                datasets=datasets,
                runner=selected_runner,
                domain_store=selected_domain_store,
                job_store=selected_job_store,
            )

        results: list[dict[str, Any]] = []
        for selection_key, dataset in datasets.items():
            dataset_id = dataset.dataset_id
            execution_task_id = task_id if dataset.selection_id is None else f"{task_id}:{dataset.selection_id}"
            output_version = make_output_version(dataset_id, execution_task_id)
            effective_request = effective_dataset_request(request, dataset)
            effective_partition = resolve_dataset_partition(request, dataset)
            workflow_timing = TimingRecorder("workflow_dataset")
            started = False
            try:
                with workflow_timing.phase("opengauss.start_output"):
                    started_version = selected_domain_store.start_output(effective_request, dataset, execution_task_id)
                started = True
                if started_version != output_version:
                    raise ValueError("domain store returned a non-deterministic output version")
                with workflow_timing.phase("runner.execute"):
                    result, scene_outcomes = _run_dataset_by_scene(
                        selected_runner,
                        dataset=dataset,
                        task_id=execution_task_id,
                        output_version=output_version,
                        grid_type=effective_request.grid_type,
                        requested_grid_level=effective_request.requested_grid_level,
                        cover_mode=effective_request.cover_mode,
                        max_cells_per_asset=effective_request.max_cells_per_asset,
                        time_granularity=effective_request.time_granularity,
                        max_observations=effective_partition.max_observations,
                        cancellation_check=lambda: _is_cancelled(selected_job_store, task_id),
                    )
                if self.after_ray is not None:
                    self.after_ray()
                if _is_cancelled(selected_job_store, task_id):
                    raise PartitionCancelledError("Partition task cancelled")
                if result.dataset_id != dataset_id or result.output_version != output_version or result.task_id != execution_task_id:
                    raise ValueError("dataset result identity does not match the active attempt")
                record_chunks = getattr(selected_domain_store, "record_output_chunks", None)
                if result.chunks and callable(record_chunks):
                    with workflow_timing.phase("opengauss.record_output_chunks"):
                        record_chunks(result)
                    verify_chunks = getattr(selected_domain_store, "verify_output_chunks", None)
                    if callable(verify_chunks):
                        with workflow_timing.phase("opengauss.verify_output_chunks"):
                            verify_chunks(result)
                    promote_chunks = getattr(selected_domain_store, "promote_logical_staging", None)
                    if callable(promote_chunks):
                        with workflow_timing.phase("opengauss.promote_logical_staging"):
                            promote_chunks(result)
                with workflow_timing.phase("opengauss.complete_output"):
                    committed = selected_domain_store.complete_output(result)
                result = result.model_copy(update={
                    "timings": {**result.timings, "workflow": workflow_timing.finish()},
                })
                completed_result = _completed_dataset_result(result, committed)
                if dataset.selection_id is not None:
                    completed_result["selection_id"] = dataset.selection_id
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
                results.append({
                    "dataset_id": dataset_id,
                    "output_version": output_version,
                    "status": "cancelled",
                    "timings": {"workflow": workflow_timing.finish()},
                    **({"selection_id": dataset.selection_id} if dataset.selection_id is not None else {}),
                })
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
                        "timings": {"workflow": workflow_timing.finish()},
                    }
                if dataset.selection_id is not None:
                    failed_result["selection_id"] = dataset.selection_id
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

    def _run_ray_batch(
        self,
        *,
        task_id: str,
        request: StrictPartitionRequest,
        datasets: dict[Any, Any],
        runner: Any,
        domain_store: Any,
        job_store: Any,
    ) -> dict[str, Any]:
        """Submit all independent scene units to the batch-owned Ray driver."""
        results: list[dict[str, Any]] = []
        prepared: list[dict[str, Any]] = []
        for _selection_key, dataset in datasets.items():
            dataset_id = dataset.dataset_id
            execution_task_id = task_id if dataset.selection_id is None else f"{task_id}:{dataset.selection_id}"
            output_version = make_output_version(dataset_id, execution_task_id)
            effective_request = effective_dataset_request(request, dataset)
            effective_partition = resolve_dataset_partition(request, dataset)
            workflow_timing = TimingRecorder("workflow_dataset")
            started = False
            try:
                with workflow_timing.phase("opengauss.start_output"):
                    started_version = domain_store.start_output(effective_request, dataset, execution_task_id)
                started = True
                if started_version != output_version:
                    raise ValueError("domain store returned a non-deterministic output version")
                units, track_scenes = _scene_execution_units(dataset)
                for unit in units:
                    unit.update({
                        "task_id": execution_task_id,
                        "output_version": output_version,
                        "grid_type": effective_request.grid_type,
                        "requested_grid_level": effective_request.requested_grid_level,
                        "cover_mode": effective_request.cover_mode,
                        "max_cells_per_asset": effective_request.max_cells_per_asset,
                        "time_granularity": effective_request.time_granularity,
                        "max_observations": effective_partition.max_observations,
                    })
                prepared.append({
                    "dataset": dataset,
                    "dataset_id": dataset_id,
                    "task_id": execution_task_id,
                    "output_version": output_version,
                    "units": units,
                    "track_scenes": track_scenes,
                    "workflow_timing": workflow_timing,
                })
            except Exception as exc:
                message = _safe_dataset_error(exc)
                if started:
                    domain_store.fail_output(
                        dataset_id,
                        output_version,
                        error_code="partition_execution_failed",
                        error_message=message,
                    )
                failure = {
                    "dataset_id": dataset_id,
                    "output_version": output_version,
                    "status": "failed",
                    "error": {"code": "partition_execution_failed", "message": message},
                }
                if dataset.selection_id is not None:
                    failure["selection_id"] = dataset.selection_id
                results.append(failure)

        units = [unit for item in prepared for unit in item["units"]]
        try:
            outcomes = runner.run_datasets(
                runs=units,
                cancellation_check=lambda: _is_cancelled(job_store, task_id),
            )
            if len(outcomes) != len(units):
                raise RuntimeError("batch runner returned a mismatched number of outcomes")
        except PartitionCancelledError:
            return self._cancel_ray_batch(prepared, results, request.batch_id, domain_store)
        except Exception as exc:
            message = _safe_dataset_error(exc)
            for item in prepared:
                domain_store.fail_output(
                    item["dataset_id"],
                    item["output_version"],
                    error_code="partition_execution_failed",
                    error_message=message,
                )
                results.append(_failed_batch_dataset(item, message))
            return _batch_result(request.batch_id, results)

        for unit, outcome in zip(units, outcomes, strict=True):
            unit["outcome"] = outcome
        if self.after_ray is not None:
            self.after_ray()
        if _is_cancelled(job_store, task_id):
            return self._cancel_ray_batch(prepared, results, request.batch_id, domain_store)

        for item in prepared:
            dataset = item["dataset"]
            try:
                result, scene_outcomes = _combine_batch_unit_outcomes(item)
                if (
                    result.dataset_id != item["dataset_id"]
                    or result.task_id != item["task_id"]
                    or result.output_version != item["output_version"]
                ):
                    raise ValueError("dataset result identity does not match the active attempt")
                record_chunks = getattr(domain_store, "record_output_chunks", None)
                if result.chunks and callable(record_chunks):
                    with item["workflow_timing"].phase("opengauss.record_output_chunks"):
                        record_chunks(result)
                    verify_chunks = getattr(domain_store, "verify_output_chunks", None)
                    if callable(verify_chunks):
                        with item["workflow_timing"].phase("opengauss.verify_output_chunks"):
                            verify_chunks(result)
                    promote_chunks = getattr(domain_store, "promote_logical_staging", None)
                    if callable(promote_chunks):
                        with item["workflow_timing"].phase("opengauss.promote_logical_staging"):
                            promote_chunks(result)
                with item["workflow_timing"].phase("opengauss.complete_output"):
                    committed = domain_store.complete_output(result)
                result = result.model_copy(update={
                    "timings": {**result.timings, "workflow": item["workflow_timing"].finish()},
                })
                completed = _completed_dataset_result(result, committed)
                if dataset.selection_id is not None:
                    completed["selection_id"] = dataset.selection_id
                if scene_outcomes is not None:
                    completed["scenes"] = scene_outcomes
                    if any(outcome["status"] != "completed" for outcome in scene_outcomes):
                        completed["status"] = "partial_failure"
                results.append(completed)
            except PartitionCancelledError:
                domain_store.fail_output(
                    item["dataset_id"],
                    item["output_version"],
                    error_code="partition_cancelled",
                    error_message="Partition task cancelled",
                )
                results.append(_cancelled_batch_dataset(item))
            except Exception as exc:
                message = _safe_dataset_error(exc)
                domain_store.fail_output(
                    item["dataset_id"],
                    item["output_version"],
                    error_code="partition_execution_failed",
                    error_message=message,
                )
                failed = _failed_batch_dataset(item, message)
                if isinstance(exc, _ScenePartitionFailure):
                    failed["scenes"] = exc.outcomes
                results.append(failed)
        return _batch_result(request.batch_id, results)

    @staticmethod
    def _cancel_ray_batch(prepared: list[dict[str, Any]], results: list[dict[str, Any]], batch_id: str, domain_store: Any) -> dict[str, Any]:
        for item in prepared:
            domain_store.fail_output(
                item["dataset_id"],
                item["output_version"],
                error_code="partition_cancelled",
                error_message="Partition task cancelled",
            )
            results.append(_cancelled_batch_dataset(item))
        return _batch_result(batch_id, results)

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
        retry_band_unit_ids: dict[str, set[str]] | None = None,
    ) -> PartitionTask:
        """Persist and queue a strict request under its batch-level data type."""
        if self.dataset_runner is None:
            raise RuntimeError("strict partition dataset runner is required")
        group_datasets(request)

        with self._lock_for_batch(request.batch_id):
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
            # Explicitly selected retry units may replace an already completed
            # output version after quality validation fails.
            if retry_band_unit_ids:
                pending_datasets = _filter_retry_datasets(request.datasets, retry_band_unit_ids)
            else:
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
        # The Ray Dashboard round-trip happens outside the batch lock: the
        # attempt row already guards this batch against duplicate submissions,
        # and a slow dashboard must not stall other callers.
        if ray_job_executor_enabled() and not isinstance(self.store, InMemoryPartitionJobStore):
            submitter = self.ray_job_submitter or RayJobPartitionSubmitter()
            try:
                ray_job_id = submitter.submit(task_id)
                self.store.set_ray_job_id(task_id, ray_job_id)
            except Exception as exc:
                self.on_task_failed(task_id, str(exc))
            attempt = self.store.get_attempt(task_id)
            return _task_from_attempt(attempt or {"task_id": task_id, "data_type": data_type}, self.get_batch(request.batch_id))
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

    def retry_task(
        self,
        task_id: str,
        *,
        retry_band_unit_ids: dict[str, set[str]] | None = None,
        retry_strategy: str | None = None,
    ) -> PartitionTask:
        attempt = self.store.get_attempt(task_id)
        if attempt is None:
            raise HTTPException(status_code=404, detail=f"Managed partition task not found: {task_id}")
        task = self.get_task(task_id)
        quality_retry = bool(retry_band_unit_ids)
        retryable_statuses = {"failed", "cancelled", "manual_required"}
        if quality_retry:
            retryable_statuses.add("completed")
        if task.status not in retryable_statuses:
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
            request = self._refresh_retry_assets(request)
            data_type = str(batch.get("data_type") or "")
            if data_type not in {"mixed", "optical", "radar", "product", "carbon"}:
                raise HTTPException(status_code=409, detail="Partition task has no retryable normalized data type")
            cancelled = task.status == "cancelled"
            return self._submit_normalized(
                data_type,
                request,
                requested_by="operator",
                operation="manual_retry",
                source_task_id=task_id,
                retry_strategy=retry_strategy or ("unfinished_units" if cancelled else "failed_units"),
                failure_reason=_text_or_none(attempt.get("error_message")) or _text_or_none(batch.get("last_error")),
                retry_band_unit_ids=(
                    retry_band_unit_ids
                    if quality_retry
                    else (_unfinished_band_unit_ids(attempt) if cancelled else _failed_band_unit_ids(attempt))
                ),
            )
        raise HTTPException(status_code=409, detail="Partition task payload is not a normalized production request")

    def _refresh_retry_assets(self, request: StrictPartitionRequest) -> StrictPartitionRequest:
        """Re-materialize retried asset source URIs from the authoritative scene_assets table.

        Retry reuses the original attempt payload, which may reference stale source
        URIs after an operator fixes the underlying object. Refreshing from
        ``scene_assets`` makes the fix effective on retry instead of silently
        reproducing references to the old (missing) object.
        """
        if self.scene_repository is None:
            return request
        asset_ids = sorted({asset.source_asset_id for dataset in request.datasets for asset in dataset.assets})
        rows = self.scene_repository.read_scene_assets_by_ids(asset_ids)
        by_id: dict[str, dict[str, Any]] = {str(row["asset_id"]): row for row in rows}
        normalized_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for dataset in request.datasets:
            dataset_asset_ids = [asset.source_asset_id for asset in dataset.assets]
            for row in self.scene_repository.read_partition_dataset_asset_uris(dataset.dataset_id, dataset_asset_ids):
                normalized_by_key[(dataset.dataset_id, str(row["source_asset_id"]))] = row
        refreshed_datasets = []
        for dataset in request.datasets:
            refreshed_assets = []
            for asset in dataset.assets:
                row = by_id.get(asset.source_asset_id)
                if row is None:
                    refreshed_assets.append(asset)
                    continue
                if dataset.data_type == "carbon":
                    update = {"source_uri": str(row.get("source_uri") or "") or None, "checksum": str(row.get("checksum") or asset.checksum)}
                    authoritative_uri = str(row.get("source_uri") or "")
                else:
                    # Non-carbon COG assets carry the object URI in cog_uri and must
                    # keep source_uri None; scene_assets.source_uri may hold the raw
                    # loader URI which the normalized contract forbids.
                    update = {
                        "source_uri": None,
                        "cog_uri": str(row.get("cog_uri") or "") or None,
                        "checksum": str(row.get("checksum") or asset.checksum),
                    }
                    authoritative_uri = str(row.get("cog_uri") or "")
                normalized = normalized_by_key.get((dataset.dataset_id, asset.source_asset_id))
                normalized_uri = (
                    str(normalized.get("source_uri") or "")
                    if dataset.data_type == "carbon"
                    else str(normalized.get("cog_uri") or "")
                ) if normalized else ""
                if authoritative_uri and normalized_uri and authoritative_uri != normalized_uri:
                    logger.warning(
                        "asset %s source definition diverges (scene_assets=%s, partition_dataset_assets=%s); retry uses scene_assets",
                        asset.source_asset_id,
                        authoritative_uri,
                        normalized_uri,
                    )
                refreshed_assets.append(asset.model_copy(update=update))
            refreshed_datasets.append(dataset.model_copy(update={"assets": tuple(refreshed_assets)}))
        return request.model_copy(update={"datasets": tuple(refreshed_datasets)})

    def cancel_task(self, task_id: str) -> dict[str, Any]:
        attempt = self.store.request_cancel(task_id)
        ray_job_id = str((attempt or {}).get("ray_job_id") or "")
        if ray_job_id and ray_job_executor_enabled():
            try:
                (self.ray_job_submitter or RayJobPartitionSubmitter()).stop(ray_job_id)
            finally:
                cancelled = self.store.mark_cancelled(task_id)
                self._notify_task_event(task_id, "cancelled", None)
            return cancelled or attempt or {"task_id": task_id, "status": "cancelled"}
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
                failure_message = _first_dataset_error([
                    item for item in result.get("datasets", ()) if isinstance(item, dict)
                ]) or "One or more datasets failed during partition execution"
                self.store.mark_result_manual_required(
                    task_id,
                    failure_message,
                    error_type=classify_partition_error(failure_message),
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
        attempt = self.store.get_attempt(task_id)
        ray_job_id = str((attempt or {}).get("ray_job_id") or "")
        if ray_job_id:
            self._reconcile_ray_job_attempt(task_id, ray_job_id)
            return
        if (
            ray_job_executor_enabled()
            and not isinstance(self.store, InMemoryPartitionJobStore)
            and _attempt_age_seconds(attempt) <= job_client_timeout_seconds() + RAY_JOB_SUBMISSION_GRACE_SECONDS
        ):
            # The Ray Job submission runs outside the batch lock and is bounded
            # by the job client timeout; a fresh attempt without a ray_job_id is
            # still in flight and must not be failed by a reader, or a duplicate
            # attempt could be created for the same batch.
            return
        try:
            current = self.partition_service.get_task(task_id)
        except HTTPException as exc:
            if exc.status_code != 404:
                raise
            self.store.mark_result_manual_required(
                task_id,
                "Local partition worker is unavailable after restart; Ray execution requires manual recovery",
                error_type="local_worker_lost",
            )
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

    def _reconcile_ray_job_attempt(self, task_id: str, ray_job_id: str) -> None:
        try:
            status = (self.ray_job_submitter or RayJobPartitionSubmitter()).status(ray_job_id).upper()
        except Exception as exc:
            logger.warning("Unable to reconcile Ray job status for task %s (job %s): %s", task_id, ray_job_id, exc)
            return
        if status.endswith("SUCCEEDED"):
            self.store.mark_result_manual_required(
                task_id,
                "Ray partition job exited without finalizing its managed attempt",
                error_type="ray_job_incomplete",
            )
        elif status.endswith("FAILED"):
            self.store.fail_attempt(
                task_id,
                "Ray partition job failed; inspect the Ray Job logs for the driver error",
                manual_required=True,
                error_type="ray_job_failed",
            )
        elif status.endswith("STOPPED"):
            self.store.mark_cancelled(task_id)



def classify_partition_error(error: str) -> str:
    normalized = error.lower()
    if "max_candidate_cells" in normalized or "max_output_cells" in normalized or "gridlimitexceeded" in normalized:
        return "grid_limit"
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


def _first_dataset_error(outcomes: list[dict[str, Any]]) -> str | None:
    for item in outcomes:
        error = item.get("error")
        if isinstance(error, dict) and error.get("message"):
            return str(error["message"])
        if item.get("error_message"):
            return str(item["error_message"])
    return None


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


def _failed_band_unit_ids(attempt: dict[str, Any]) -> dict[str, set[str]]:
    return _band_unit_ids_with_status(attempt, lambda status: status == "failed")


def _unfinished_band_unit_ids(attempt: dict[str, Any]) -> dict[str, set[str]]:
    return _band_unit_ids_with_status(attempt, lambda status: status != "completed")


def _band_unit_ids_with_status(
    attempt: dict[str, Any], include_status: Callable[[str], bool]
) -> dict[str, set[str]]:
    result = attempt.get("runner_result") if isinstance(attempt.get("runner_result"), dict) else {}
    selected: dict[str, set[str]] = {}
    for dataset in result.get("datasets", []):
        if not isinstance(dataset, dict) or not dataset.get("dataset_id") or dataset.get("status") == "completed":
            continue
        band_outcomes = [item for item in dataset.get("scenes", ()) if isinstance(item, dict) and item.get("band_unit_id")]
        if band_outcomes:
            selected[str(dataset["dataset_id"])] = {
                str(item["band_unit_id"]) for item in band_outcomes if include_status(str(item.get("status") or ""))
            }
    return selected


def _filter_retry_datasets(datasets: tuple[DatasetInput, ...], retry_band_unit_ids: dict[str, set[str]]) -> tuple[DatasetInput, ...]:
    filtered = []
    for dataset in datasets:
        if dataset.dataset_id not in retry_band_unit_ids:
            filtered.append(dataset)
            continue
        band_unit_ids = retry_band_unit_ids[dataset.dataset_id]
        if not band_unit_ids:
            continue
        bands = tuple(
            band for band in dataset.bands
            if _band_unit_id(band) in band_unit_ids
        )
        asset_ids = {band.source_asset_id for band in bands}
        assets = tuple(asset for asset in dataset.assets if asset.source_asset_id in asset_ids)
        if assets and bands:
            filtered.append(dataset.model_copy(update={"assets": assets, "bands": bands}))
    if not filtered:
        raise HTTPException(status_code=409, detail="Partition task has no failed data units to retry")
    return tuple(filtered)


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


def _attempt_age_seconds(attempt: dict[str, Any] | None) -> float:
    if not isinstance(attempt, dict):
        return float("inf")
    return max(time.time() - _timestamp_or_now(attempt.get("created_at")), 0.0)


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


def _scene_execution_units(dataset: Any) -> tuple[list[dict[str, Any]], bool]:
    """Build independent scene units without dispatching them one by one."""
    grouped: dict[str, tuple[str, Any]] = {}
    assets_by_id = {asset.source_asset_id: asset for asset in dataset.assets}
    for band in dataset.bands:
        band_unit_id = _band_unit_id(band)
        asset = assets_by_id.get(band.source_asset_id)
        scene_id = str((asset.attributes or {}).get("scene_id") or "").strip() if asset is not None else ""
        if not band_unit_id or not scene_id or asset is None:
            return [{"dataset": dataset, "scene_id": None, "band_unit_id": None}], False
        grouped[band_unit_id] = (scene_id, band)
    return [
        {
            "dataset": dataset.model_copy(update={"assets": (assets_by_id[band.source_asset_id],), "bands": (band,)}),
            "scene_id": scene_id,
            "band_unit_id": band_unit_id,
        }
        for band_unit_id, (scene_id, band) in grouped.items()
    ], True


def _combine_batch_unit_outcomes(item: dict[str, Any]) -> tuple[PartitionDatasetResult, list[dict[str, Any]] | None]:
    successful: list[PartitionDatasetResult] = []
    scene_outcomes: list[dict[str, Any]] | None = [] if item["track_scenes"] else None
    for unit in item["units"]:
        outcome = unit.get("outcome") or {}
        if "error" in outcome:
            if scene_outcomes is not None:
                scene_outcomes.append({
                    "scene_id": unit["scene_id"],
                    "band_unit_id": unit["band_unit_id"],
                    "status": "failed",
                    "error": {"code": "partition_execution_failed", "message": str(outcome["error"])},
                })
            continue
        raw = outcome.get("result")
        if not isinstance(raw, dict):
            raise RuntimeError("batch runner outcome is missing result")
        result = PartitionDatasetResult.model_validate(raw)
        successful.append(result)
        if scene_outcomes is not None:
            scene_outcomes.append({
                "scene_id": unit["scene_id"],
                "band_unit_id": unit["band_unit_id"],
                "status": "completed",
            })
    if not successful:
        if scene_outcomes is not None:
            raise _ScenePartitionFailure(scene_outcomes)
        raise RuntimeError("batch runner did not produce a dataset result")
    first = successful[0]
    return first.model_copy(update={
        "tiles": _merge_scene_rows(successful, "tiles"),
        "indexes": _merge_scene_rows(successful, "indexes"),
        "grid_cells": _merge_scene_rows(successful, "grid_cells"),
        "chunks": tuple(chunk for result in successful for chunk in result.chunks),
        "timings": {"units": [result.timings for result in successful if result.timings]},
    }), scene_outcomes


def _cancelled_batch_dataset(item: dict[str, Any]) -> dict[str, Any]:
    dataset = item["dataset"]
    result = {
        "dataset_id": item["dataset_id"],
        "output_version": item["output_version"],
        "status": "cancelled",
    }
    if dataset.selection_id is not None:
        result["selection_id"] = dataset.selection_id
    return result


def _failed_batch_dataset(item: dict[str, Any], message: str) -> dict[str, Any]:
    dataset = item["dataset"]
    result = {
        "dataset_id": item["dataset_id"],
        "output_version": item["output_version"],
        "status": "failed",
        "error": {"code": "partition_execution_failed", "message": message},
    }
    if dataset.selection_id is not None:
        result["selection_id"] = dataset.selection_id
    return result


def _batch_result(batch_id: str, results: list[dict[str, Any]]) -> dict[str, Any]:
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
    return {"batch_id": batch_id, "status": status, "datasets": results}


def _run_dataset_by_scene(runner: Any, *, dataset: Any, **kwargs: Any) -> tuple[PartitionDatasetResult, list[dict[str, Any]] | None]:
    """Run each selected band independently while retaining scene context."""
    grouped: dict[str, tuple[str, Any]] = {}
    assets_by_id = {asset.source_asset_id: asset for asset in dataset.assets}
    for band in dataset.bands:
        band_unit_id = _band_unit_id(band)
        asset = assets_by_id.get(band.source_asset_id)
        scene_id = str((asset.attributes or {}).get("scene_id") or "").strip() if asset is not None else ""
        if not band_unit_id or not scene_id or asset is None:
            raw = _run_dataset_runner(runner, dataset=dataset, **kwargs)
            return PartitionDatasetResult.model_validate(raw), None
        grouped[band_unit_id] = (scene_id, band)

    results: list[PartitionDatasetResult] = []
    outcomes: list[dict[str, Any]] = []
    for band_unit_id, (scene_id, band) in grouped.items():
        asset = assets_by_id[band.source_asset_id]
        band_dataset = dataset.model_copy(
            update={
                "assets": (asset,),
                "bands": (band,),
            }
        )
        try:
            raw = _run_dataset_runner(runner, dataset=band_dataset, **kwargs)
            result = PartitionDatasetResult.model_validate(raw)
            results.append(result)
            outcomes.append({"scene_id": scene_id, "band_unit_id": band_unit_id, "status": "completed"})
        except Exception as exc:
            outcomes.append(
                {
                    "scene_id": scene_id,
                    "band_unit_id": band_unit_id,
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
            "chunks": tuple(chunk for result in results for chunk in result.chunks),
            "timings": {"units": [result.timings for result in results if result.timings]},
        }
    )
    return combined, outcomes


def _band_unit_id(band: Any) -> str:
    """Use the registered id, with a deterministic fallback for legacy input."""
    registered = str((band.attributes or {}).get("band_unit_id") or "").strip()
    return registered or f"{band.source_asset_id}:{band.band_code}"


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
        "timings": result.timings,
    }
    if result.execution_engine:
        completed["execution_engine"] = result.execution_engine
    return completed


def _safe_dataset_error(exc: Exception) -> str:
    message = str(exc).strip() or exc.__class__.__name__
    return message[:1000]
