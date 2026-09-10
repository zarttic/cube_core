from __future__ import annotations

import time
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from threading import Lock
from typing import Callable
from uuid import uuid4

from cube_split import runtime_config

from cube_web.services.http_errors import HTTPException

TaskHook = Callable[[str], None]
TaskResultHook = Callable[[str, dict], None]
TaskErrorHook = Callable[[str, str], None]

DEFAULT_PARTITION_MAX_WORKERS = 4
logger = logging.getLogger(__name__)


def _safe_task_error(exc: Exception | str) -> str:
    message = str(exc).strip() or (exc.__class__.__name__ if isinstance(exc, Exception) else "任务异常")
    message = re.sub(
        r"(?i)\b(?:password|passwd|secret|token|access[_-]?key)\s*[:=]\s*[^\s,;]+",
        "[credential redacted]",
        message,
    )
    message = re.sub(r"(?i)s3://[^\s\"'<>]+", "[object URI redacted]", message)
    message = re.sub(r"(?i)\b(?:postgres(?:ql)?|mysql|mariadb|redis)://[^\s\"'<>]+", "[connection URI redacted]", message)
    return message[:1000]


def _resolve_max_workers() -> int:
    raw = runtime_config.env_text("CUBE_WEB_PARTITION_MAX_WORKERS")
    if not raw:
        return DEFAULT_PARTITION_MAX_WORKERS
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_PARTITION_MAX_WORKERS
    return max(1, value)


@dataclass
class PartitionTask:
    task_id: str
    status: str
    data_type: str
    operation: str
    created_at: float
    updated_at: float
    result: dict | None = None
    error: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)


class PartitionTaskStore:
    def __init__(self, max_workers: int | None = None) -> None:
        self._tasks: dict[str, PartitionTask] = {}
        self._cancellation_checks: dict[str, Callable[[], bool] | None] = {}
        self._allow_running_cancel: dict[str, bool] = {}
        self._lock = Lock()
        self.max_workers = max_workers if max_workers is not None else _resolve_max_workers()
        self._executor = ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="cube-web-partition")

    def _new_task(self, data_type: str, operation: str, task_id: str | None = None) -> PartitionTask:
        now = time.time()
        return PartitionTask(
            task_id=task_id or f"partition-{uuid4().hex[:12]}",
            status="queued",
            data_type=data_type,
            operation=operation,
            created_at=now,
            updated_at=now,
        )

    def _schedule(
        self,
        task: PartitionTask,
        runner: Callable[[], dict],
        on_started: TaskHook | None,
        on_succeeded: TaskResultHook | None,
        on_failed: TaskErrorHook | None,
        cancellation_check: Callable[[], bool] | None,
    ) -> PartitionTask:
        self._executor.submit(self._run, task.task_id, runner, on_started, on_succeeded, on_failed, cancellation_check)
        return task

    def submit(
        self,
        data_type: str,
        operation: str,
        runner: Callable[[], dict],
        task_id: str | None = None,
        on_started: TaskHook | None = None,
        on_succeeded: TaskResultHook | None = None,
        on_failed: TaskErrorHook | None = None,
        cancellation_check: Callable[[], bool] | None = None,
        allow_running_cancel: bool = True,
    ) -> PartitionTask:
        task = self._new_task(data_type, operation, task_id)
        with self._lock:
            self._tasks[task.task_id] = task
            self._cancellation_checks[task.task_id] = cancellation_check
            self._allow_running_cancel[task.task_id] = allow_running_cancel
        return self._schedule(task, runner, on_started, on_succeeded, on_failed, cancellation_check)

    def submit_if_absent(
        self,
        data_type: str,
        operation: str,
        runner: Callable[[], dict],
        task_id: str | None = None,
        on_started: TaskHook | None = None,
        on_succeeded: TaskResultHook | None = None,
        on_failed: TaskErrorHook | None = None,
        cancellation_check: Callable[[], bool] | None = None,
        allow_running_cancel: bool = True,
    ) -> PartitionTask:
        """Return an active task for ``operation`` or submit exactly one new task.

        The lookup and insertion intentionally happen under the same lock.  This
        is used by idempotent management actions where two browser requests can
        arrive before the first worker has started.
        """
        with self._lock:
            for existing in self._tasks.values():
                if existing.operation == operation and existing.status in {"queued", "running", "cancel_requested"}:
                    return existing
            task = self._new_task(data_type, operation, task_id)
            self._tasks[task.task_id] = task
            self._cancellation_checks[task.task_id] = cancellation_check
            self._allow_running_cancel[task.task_id] = allow_running_cancel
        return self._schedule(task, runner, on_started, on_succeeded, on_failed, cancellation_check)

    def get(self, task_id: str) -> PartitionTask | None:
        with self._lock:
            return self._tasks.get(task_id)

    def supports_running_cancellation(self, task_id: str) -> bool:
        with self._lock:
            return self._allow_running_cancel.get(task_id, True)

    def queue_depth(self) -> int:
        with self._lock:
            return sum(task.status == "queued" for task in self._tasks.values())

    def cancel(self, task_id: str) -> PartitionTask | None:
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return None
            if task.status == "queued":
                task.status = "cancelled"
            elif task.status == "running" and self._allow_running_cancel.get(task_id, True):
                task.status = "cancel_requested"
            task.updated_at = time.time()
            return task

    def force_cancel(self, task_id: str) -> PartitionTask | None:
        """Mark a task cancelled immediately while its worker observes the stop.

        Python threads cannot be safely killed.  The worker is therefore allowed
        to finish its current cancellation checkpoint, but the terminal state is
        published immediately and completion is still guarded by the cancellation
        state in ``_complete_task_if_not_cancelled``.
        """
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return None
            if task.status not in {"completed", "failed", "cancelled"}:
                task.status = "cancelled"
                task.error = "Partition task cancelled"
                task.updated_at = time.time()
            return task

    def _set_task(self, task_id: str, **updates) -> None:
        with self._lock:
            task = self._tasks[task_id]
            for key, value in updates.items():
                setattr(task, key, value)
            task.updated_at = time.time()

    def _start_task(self, task_id: str) -> bool:
        with self._lock:
            task = self._tasks[task_id]
            if task.status != "queued":
                return False
            task.status = "running"
            task.updated_at = time.time()
            return True

    def _cancel_task(self, task_id: str, on_failed: TaskErrorHook | None) -> None:
        self._finish_with_error(task_id, "cancelled", "Partition task cancelled", on_failed)

    def _safe_on_failed(self, task_id: str, error: str, on_failed: TaskErrorHook | None) -> None:
        if on_failed is None:
            return
        try:
            on_failed(task_id, error)
        except Exception:
            logger.exception("Partition task failure hook failed for task %s", task_id)

    def _finish_with_error(
        self,
        task_id: str,
        status: str,
        error: str,
        on_failed: TaskErrorHook | None,
    ) -> None:
        error = _safe_task_error(error)
        try:
            self._set_task(task_id, status=status, error=error)
        except Exception:
            logger.exception("Unable to persist local terminal state for task %s", task_id)
        self._safe_on_failed(task_id, error, on_failed)

    def _complete_task_if_not_cancelled(self, task_id: str, result: dict) -> bool:
        # Check-and-commit under a single lock so a concurrent cancel() cannot be overwritten.
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return False
            if task.status in ("cancel_requested", "cancelled"):
                return False
            task.status = "completed"
            task.result = result
            task.updated_at = time.time()
            return True

    def _run(
        self,
        task_id: str,
        runner: Callable[[], dict],
        on_started: TaskHook | None,
        on_succeeded: TaskResultHook | None,
        on_failed: TaskErrorHook | None,
        cancellation_check: Callable[[], bool] | None,
    ) -> None:
        try:
            task = self.get(task_id)
            if task is not None and (task.status == "cancelled" or (cancellation_check is not None and cancellation_check())):
                self._cancel_task(task_id, on_failed)
                return
            if not self._start_task(task_id):
                task = self.get(task_id)
                if task is not None and task.status == "cancelled":
                    self._safe_on_failed(task_id, _safe_task_error("Partition task cancelled"), on_failed)
                return
            task = self.get(task_id)
            if task is not None and task.status == "cancel_requested":
                self._cancel_task(task_id, on_failed)
                return
            if cancellation_check is not None and cancellation_check():
                self._cancel_task(task_id, on_failed)
                return
            if on_started is not None:
                on_started(task_id)
            if cancellation_check is not None and cancellation_check():
                self._cancel_task(task_id, on_failed)
                return
            result = runner()
        except Exception as exc:  # pragma: no cover - covered through public task status.
            status = "cancelled" if exc.__class__.__name__ == "PartitionCancelledError" else "failed"
            self._finish_with_error(task_id, status, _safe_task_error(exc), on_failed)
            return
        # Atomically commit completion first (single lock), then consult the DB-side
        # cancellation check outside the lock. If the DB says cancelled after the
        # in-memory commit, flip the task back to cancelled so both sides agree.
        try:
            completed = self._complete_task_if_not_cancelled(task_id, result)
        except Exception as exc:
            self._finish_with_error(task_id, "failed", _safe_task_error(exc), on_failed)
            return
        if not completed:
            self._cancel_task(task_id, on_failed)
            return
        if cancellation_check is not None:
            try:
                cancelled_after_commit = cancellation_check()
            except Exception:
                # The result is already committed locally. Keep it completed and
                # let the persistence/reconciliation hook settle the durable state.
                logger.exception("Unable to check cancellation after completing task %s", task_id)
                cancelled_after_commit = False
            if cancelled_after_commit:
                self._cancel_task(task_id, on_failed)
                return
        if on_succeeded is not None:
            try:
                on_succeeded(task_id, result)
            except Exception:
                # A completion hook persists a projection; it must not turn a
                # completed computation into a second, conflicting local failure.
                logger.exception("Partition task completion hook failed for task %s", task_id)


class PartitionService:
    def __init__(self, task_store: PartitionTaskStore | None = None) -> None:
        self.task_store = task_store or PartitionTaskStore()

    def queue_depth(self) -> int:
        return self.task_store.queue_depth()

    def get_task(self, task_id: str) -> PartitionTask:
        task = self.task_store.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail=f"Partition task not found: {task_id}")
        return task

    def cancel_task(self, task_id: str) -> PartitionTask:
        task = self.task_store.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail=f"Partition task not found: {task_id}")
        if task.status == "running" and not self.task_store.supports_running_cancellation(task_id):
            raise HTTPException(
                status_code=409,
                detail=f"Partition task cannot be cancelled after execution started: {task_id}",
            )
        task = self.task_store.cancel(task_id)
        if task is None:  # pragma: no cover - guarded by the lookup above.
            raise HTTPException(status_code=404, detail=f"Partition task not found: {task_id}")
        return task

    def force_cancel_task(self, task_id: str) -> PartitionTask:
        task = self.task_store.force_cancel(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail=f"Partition task not found: {task_id}")
        return task
