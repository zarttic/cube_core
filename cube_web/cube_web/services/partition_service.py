from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from threading import Lock
from typing import Callable, Dict, Optional, Tuple
from uuid import uuid4

from fastapi import HTTPException

PartitionRunner = Callable[[Optional[dict]], dict]
TaskHook = Callable[[str], None]
TaskResultHook = Callable[[str, dict], None]
TaskErrorHook = Callable[[str, str], None]


@dataclass(frozen=True)
class PartitionBackend:
    data_type: str
    run: Optional[PartitionRunner] = None
    demo: Optional[PartitionRunner] = None
    retry: Optional[PartitionRunner] = None
    test: Optional[PartitionRunner] = None
    implemented: bool = True


@dataclass
class PartitionTask:
    task_id: str
    status: str
    data_type: str
    operation: str
    created_at: float
    updated_at: float
    result: Optional[dict] = None
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


class PartitionTaskStore:
    def __init__(self, max_workers: int = 4) -> None:
        self._tasks: Dict[str, PartitionTask] = {}
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
        self._executor.submit(self._run, task.task_id, runner, on_started, on_succeeded, on_failed)
        return task

    def get(self, task_id: str) -> Optional[PartitionTask]:
        with self._lock:
            return self._tasks.get(task_id)

    def cancel(self, task_id: str) -> Optional[PartitionTask]:
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

    def _run(
        self,
        task_id: str,
        runner: Callable[[], dict],
        on_started: TaskHook | None = None,
        on_succeeded: TaskResultHook | None = None,
        on_failed: TaskErrorHook | None = None,
    ) -> None:
        task = self.get(task_id)
        if task is not None and task.status == "cancelled":
            return
        self._set_task(task_id, status="running")
        if on_started is not None:
            on_started(task_id)
        try:
            result = runner()
        except Exception as exc:  # pragma: no cover - covered through public task status.
            if exc.__class__.__name__ == "PartitionCancelledError":
                self._set_task(task_id, status="cancelled", error=str(exc))
                if on_failed is not None:
                    on_failed(task_id, str(exc))
                return
            self._set_task(task_id, status="failed", error=str(exc))
            if on_failed is not None:
                on_failed(task_id, str(exc))
            return
        task = self.get(task_id)
        if task is not None and task.status == "cancel_requested":
            self._set_task(task_id, status="cancelled", error="Partition task cancelled")
            if on_failed is not None:
                on_failed(task_id, "Partition task cancelled")
            return
        self._set_task(task_id, status="completed", result=result)
        if on_succeeded is not None:
            on_succeeded(task_id, result)


class PartitionService:
    def __init__(self, registry: Dict[str, PartitionBackend], task_store: Optional[PartitionTaskStore] = None) -> None:
        self.registry = registry
        self.task_store = task_store or PartitionTaskStore()

    def run(self, data_type: str, payload: Optional[dict] = None) -> dict:
        return self._run(data_type, "run", payload)

    def demo(self, data_type: str, payload: Optional[dict] = None) -> dict:
        return self._run(data_type, "demo", payload)

    def retry(self, data_type: str, payload: Optional[dict] = None) -> dict:
        return self._run(data_type, "retry", payload)

    def test(self, data_type: str, payload: Optional[dict] = None) -> dict:
        return self._run(data_type, "test", payload)

    def submit(
        self,
        data_type: str,
        operation: str,
        payload: Optional[dict] = None,
        task_id: str | None = None,
        on_started: TaskHook | None = None,
        on_succeeded: TaskResultHook | None = None,
        on_failed: TaskErrorHook | None = None,
        cancellation_check: Callable[[], bool] | None = None,
    ) -> PartitionTask:
        backend, runner = self._resolve(data_type, operation)
        return self.task_store.submit(
            backend.data_type,
            operation,
            lambda: runner(
                {
                    **(payload or {}),
                    "_cancellation_check": cancellation_check,
                    "cancellation_check": cancellation_check,
                }
                if cancellation_check is not None
                else payload
            ),
            task_id=task_id,
            on_started=on_started,
            on_succeeded=on_succeeded,
            on_failed=on_failed,
        )

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

    def _run(self, data_type: str, operation: str, payload: Optional[dict]) -> dict:
        _, runner = self._resolve(data_type, operation)
        try:
            return runner(payload)
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    def _resolve(self, data_type: str, operation: str) -> Tuple[PartitionBackend, PartitionRunner]:
        backend = self.registry.get(data_type)
        if backend is None:
            raise HTTPException(status_code=404, detail=f"Unknown partition data_type: {data_type}")
        if not backend.implemented:
            raise HTTPException(status_code=501, detail=f"{data_type.title()} partition {operation} is not implemented")
        runner = getattr(backend, operation, None)
        if runner is None:
            raise HTTPException(status_code=404, detail=f"Partition {operation} is not available for {data_type}")
        return backend, runner


def build_partition_registry(
    *,
    optical_demo: PartitionRunner,
    optical_test: PartitionRunner,
    optical_retry: PartitionRunner,
    carbon_demo: PartitionRunner,
    carbon_test: PartitionRunner,
    carbon_retry: PartitionRunner,
    product_demo: PartitionRunner,
    product_test: PartitionRunner,
    product_retry: PartitionRunner,
    radar_demo: PartitionRunner,
    radar_test: PartitionRunner,
    radar_retry: PartitionRunner,
    entity_demo: PartitionRunner,
    entity_test: PartitionRunner,
    entity_retry: PartitionRunner,
) -> Dict[str, PartitionBackend]:
    return {
        "optical": PartitionBackend(
            data_type="optical",
            run=optical_demo,
            demo=optical_demo,
            test=optical_test,
            retry=optical_retry,
        ),
        "carbon": PartitionBackend(
            data_type="carbon",
            run=carbon_demo,
            demo=carbon_demo,
            test=carbon_test,
            retry=carbon_retry,
        ),
        "product": PartitionBackend(
            data_type="product",
            run=product_demo,
            demo=product_demo,
            test=product_test,
            retry=product_retry,
        ),
        "radar": PartitionBackend(
            data_type="radar",
            run=radar_demo,
            demo=radar_demo,
            test=radar_test,
            retry=radar_retry,
        ),
        "entity": PartitionBackend(
            data_type="entity",
            run=entity_demo,
            demo=entity_demo,
            test=entity_test,
            retry=entity_retry,
        ),
    }
