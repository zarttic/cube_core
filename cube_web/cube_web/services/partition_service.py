from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from threading import Lock
from typing import Callable
from uuid import uuid4

from cube_web.services.http_errors import HTTPException

TaskHook = Callable[[str], None]
TaskResultHook = Callable[[str, dict], None]
TaskErrorHook = Callable[[str, str], None]


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
    def __init__(self, max_workers: int = 4) -> None:
        self._tasks: dict[str, PartitionTask] = {}
        self._lock = Lock()
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="cube-web-partition")

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
    ) -> PartitionTask:
        now = time.time()
        task = PartitionTask(
            task_id=task_id or f"partition-{uuid4().hex[:12]}",
            status="queued",
            data_type=data_type,
            operation=operation,
            created_at=now,
            updated_at=now,
        )
        with self._lock:
            self._tasks[task.task_id] = task
        self._executor.submit(self._run, task.task_id, runner, on_started, on_succeeded, on_failed, cancellation_check)
        return task

    def get(self, task_id: str) -> PartitionTask | None:
        with self._lock:
            return self._tasks.get(task_id)

    def cancel(self, task_id: str) -> PartitionTask | None:
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return None
            if task.status == "queued":
                task.status = "cancelled"
            elif task.status == "running":
                task.status = "cancel_requested"
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
        self._set_task(task_id, status="cancelled", error="Partition task cancelled")
        if on_failed is not None:
            on_failed(task_id, "Partition task cancelled")

    def _run(
        self,
        task_id: str,
        runner: Callable[[], dict],
        on_started: TaskHook | None,
        on_succeeded: TaskResultHook | None,
        on_failed: TaskErrorHook | None,
        cancellation_check: Callable[[], bool] | None,
    ) -> None:
        task = self.get(task_id)
        if task is not None and (task.status == "cancelled" or (cancellation_check is not None and cancellation_check())):
            self._cancel_task(task_id, on_failed)
            return
        if not self._start_task(task_id):
            task = self.get(task_id)
            if task is not None and task.status == "cancelled" and on_failed is not None:
                on_failed(task_id, "Partition task cancelled")
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
        try:
            result = runner()
        except Exception as exc:  # pragma: no cover - covered through public task status.
            status = "cancelled" if exc.__class__.__name__ == "PartitionCancelledError" else "failed"
            self._set_task(task_id, status=status, error=str(exc))
            if on_failed is not None:
                on_failed(task_id, str(exc))
            return
        task = self.get(task_id)
        if task is not None and (task.status == "cancel_requested" or (cancellation_check is not None and cancellation_check())):
            self._cancel_task(task_id, on_failed)
            return
        self._set_task(task_id, status="completed", result=result)
        if on_succeeded is not None:
            on_succeeded(task_id, result)


class PartitionService:
    def __init__(self, task_store: PartitionTaskStore | None = None) -> None:
        self.task_store = task_store or PartitionTaskStore()

    def get_task(self, task_id: str) -> PartitionTask:
        task = self.task_store.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail=f"Partition task not found: {task_id}")
        return task

    def cancel_task(self, task_id: str) -> PartitionTask:
        task = self.task_store.cancel(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail=f"Partition task not found: {task_id}")
        return task
