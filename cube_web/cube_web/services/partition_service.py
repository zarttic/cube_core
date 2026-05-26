from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from threading import Lock
from typing import Callable
from uuid import uuid4

from fastapi import HTTPException


PartitionRunner = Callable[[dict | None], dict]


@dataclass(frozen=True)
class PartitionBackend:
    data_type: str
    demo: PartitionRunner | None = None
    retry: PartitionRunner | None = None
    test: PartitionRunner | None = None
    implemented: bool = True


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

    def submit(self, data_type: str, operation: str, runner: Callable[[], dict]) -> PartitionTask:
        now = time.time()
        task = PartitionTask(
            task_id=f"partition-{uuid4().hex[:12]}",
            status="queued",
            data_type=data_type,
            operation=operation,
            created_at=now,
            updated_at=now,
        )
        with self._lock:
            self._tasks[task.task_id] = task
        self._executor.submit(self._run, task.task_id, runner)
        return task

    def get(self, task_id: str) -> PartitionTask | None:
        with self._lock:
            return self._tasks.get(task_id)

    def _set_task(self, task_id: str, **updates) -> None:
        with self._lock:
            task = self._tasks[task_id]
            for key, value in updates.items():
                setattr(task, key, value)
            task.updated_at = time.time()

    def _run(self, task_id: str, runner: Callable[[], dict]) -> None:
        self._set_task(task_id, status="running")
        try:
            result = runner()
        except Exception as exc:  # pragma: no cover - covered through public task status.
            self._set_task(task_id, status="failed", error=str(exc))
            return
        self._set_task(task_id, status="completed", result=result)


class PartitionService:
    def __init__(self, registry: dict[str, PartitionBackend], task_store: PartitionTaskStore | None = None) -> None:
        self.registry = registry
        self.task_store = task_store or PartitionTaskStore()

    def demo(self, data_type: str, payload: dict | None = None) -> dict:
        return self._run(data_type, "demo", payload)

    def retry(self, data_type: str, payload: dict | None = None) -> dict:
        return self._run(data_type, "retry", payload)

    def test(self, data_type: str, payload: dict | None = None) -> dict:
        return self._run(data_type, "test", payload)

    def submit(self, data_type: str, operation: str, payload: dict | None = None) -> PartitionTask:
        backend, runner = self._resolve(data_type, operation)
        return self.task_store.submit(backend.data_type, operation, lambda: runner(payload))

    def get_task(self, task_id: str) -> PartitionTask:
        task = self.task_store.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail=f"Partition task not found: {task_id}")
        return task

    def _run(self, data_type: str, operation: str, payload: dict | None) -> dict:
        _, runner = self._resolve(data_type, operation)
        try:
            return runner(payload)
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    def _resolve(self, data_type: str, operation: str) -> tuple[PartitionBackend, PartitionRunner]:
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
    carbon_retry: PartitionRunner,
    product_demo: PartitionRunner,
    product_retry: PartitionRunner,
) -> dict[str, PartitionBackend]:
    return {
        "optical": PartitionBackend(
            data_type="optical",
            demo=optical_demo,
            test=optical_test,
            retry=optical_retry,
        ),
        "carbon": PartitionBackend(
            data_type="carbon",
            demo=carbon_demo,
            retry=carbon_retry,
        ),
        "product": PartitionBackend(
            data_type="product",
            demo=product_demo,
            retry=product_retry,
        ),
        "radar": PartitionBackend(data_type="radar", implemented=False),
    }
