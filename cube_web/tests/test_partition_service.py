from __future__ import annotations

import threading

import pytest

from cube_web.services.partition_service import (
    DEFAULT_PARTITION_MAX_WORKERS,
    PartitionService,
    PartitionTaskStore,
    _resolve_max_workers,
)


def _wait_for_terminal_status(store: PartitionTaskStore, task_id: str, timeout: float = 5.0) -> str:
    deadline = threading.Event()
    for _ in range(int(timeout / 0.01)):
        task = store.get(task_id)
        if task is not None and task.status in ("completed", "failed", "cancelled"):
            return task.status
        deadline.wait(0.01)
    task = store.get(task_id)
    return task.status if task is not None else "missing"


def test_cancel_between_runner_return_and_completion_commit_is_honoured() -> None:
    store = PartitionTaskStore(max_workers=1)
    about_to_commit = threading.Event()
    cancel_finished = threading.Event()
    succeeded_calls: list[str] = []
    failed_calls: list[str] = []

    original_commit = store._complete_task_if_not_cancelled

    def paused_commit(task_id: str, result: dict) -> bool:
        # Freeze the worker right after the runner returned, before the commit,
        # so cancel() lands exactly inside the former check-then-act window.
        about_to_commit.set()
        assert cancel_finished.wait(timeout=5.0)
        return original_commit(task_id, result)

    store._complete_task_if_not_cancelled = paused_commit  # type: ignore[method-assign]

    task = store.submit(
        data_type="optical",
        operation="run",
        runner=lambda: {"ok": True},
        on_succeeded=lambda task_id, result: succeeded_calls.append(task_id),
        on_failed=lambda task_id, error: failed_calls.append(task_id),
    )

    assert about_to_commit.wait(timeout=5.0)
    store.cancel(task.task_id)
    cancel_finished.set()

    final_status = _wait_for_terminal_status(store, task.task_id)
    assert final_status == "cancelled"
    assert succeeded_calls == []
    assert failed_calls == [task.task_id]


def test_successful_completion_calls_on_succeeded_exactly_once() -> None:
    store = PartitionTaskStore(max_workers=1)
    succeeded_calls: list[tuple[str, dict]] = []
    failed_calls: list[str] = []

    task = store.submit(
        data_type="optical",
        operation="run",
        runner=lambda: {"ok": True},
        on_succeeded=lambda task_id, result: succeeded_calls.append((task_id, result)),
        on_failed=lambda task_id, error: failed_calls.append(task_id),
    )

    final_status = _wait_for_terminal_status(store, task.task_id)
    assert final_status == "completed"
    assert succeeded_calls == [(task.task_id, {"ok": True})]
    assert failed_calls == []
    stored = store.get(task.task_id)
    assert stored is not None and stored.result == {"ok": True}


def test_submit_if_absent_is_atomic_for_same_operation() -> None:
    store = PartitionTaskStore(max_workers=1)
    barrier = threading.Barrier(2)
    release = threading.Event()
    task_ids: list[str] = []

    def run() -> dict:
        release.wait(timeout=5)
        return {"ok": True}

    def submit() -> None:
        barrier.wait(timeout=5)
        task = store.submit_if_absent(
            data_type="management",
            operation="delete_band_grid:dataset-a:band-a:geohash",
            runner=run,
            allow_running_cancel=False,
        )
        task_ids.append(task.task_id)

    threads = [threading.Thread(target=submit) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert len(task_ids) == 2
    assert task_ids[0] == task_ids[1]
    release.set()
    assert _wait_for_terminal_status(store, task_ids[0]) == "completed"


def test_running_non_cancellable_task_rejects_cancel_request() -> None:
    store = PartitionTaskStore(max_workers=1)
    started = threading.Event()
    release = threading.Event()
    task = store.submit_if_absent(
        data_type="management",
        operation="delete_band_grid:dataset-a:band-a:geohash",
        runner=lambda: (started.set(), release.wait(timeout=5), {"ok": True})[-1],
        allow_running_cancel=False,
    )
    assert started.wait(timeout=5)

    with pytest.raises(Exception) as exc_info:
        PartitionService(store).cancel_task(task.task_id)
    assert getattr(exc_info.value, "status_code", None) == 409
    assert store.get(task.task_id).status == "running"

    release.set()
    assert _wait_for_terminal_status(store, task.task_id) == "completed"


def test_force_cancel_marks_running_task_terminal_immediately() -> None:
    store = PartitionTaskStore(max_workers=1)
    started = threading.Event()
    release = threading.Event()
    task = store.submit(
        data_type="optical",
        operation="run",
        runner=lambda: (started.set(), release.wait(timeout=5), {"ok": True})[-1],
    )
    assert started.wait(timeout=5)

    cancelled = PartitionService(store).force_cancel_task(task.task_id)

    assert cancelled.status == "cancelled"
    assert store.get(task.task_id).status == "cancelled"
    release.set()
    assert _wait_for_terminal_status(store, task.task_id) == "cancelled"


def test_db_side_cancellation_after_commit_flips_task_back_to_cancelled() -> None:
    store = PartitionTaskStore(max_workers=1)
    succeeded_calls: list[str] = []
    failed_calls: list[str] = []
    check_results = iter([False, False, False, True])

    task = store.submit(
        data_type="optical",
        operation="run",
        runner=lambda: {"ok": True},
        on_succeeded=lambda task_id, result: succeeded_calls.append(task_id),
        on_failed=lambda task_id, error: failed_calls.append(task_id),
        cancellation_check=lambda: next(check_results, True),
    )

    final_status = _wait_for_terminal_status(store, task.task_id)
    assert final_status == "cancelled"
    assert succeeded_calls == []
    assert failed_calls == [task.task_id]


def test_failure_hook_exception_does_not_leave_worker_exception_unhandled() -> None:
    store = PartitionTaskStore(max_workers=1)

    def failed(_task_id: str, _error: str) -> None:
        raise RuntimeError("projection database unavailable")

    task = store.submit(
        data_type="optical",
        operation="run",
        runner=lambda: (_ for _ in ()).throw(RuntimeError("runner failed")),
        on_failed=failed,
    )

    assert _wait_for_terminal_status(store, task.task_id) == "failed"
    assert store.get(task.task_id).error == "runner failed"


def test_started_hook_failure_marks_task_failed_and_does_not_run_runner() -> None:
    store = PartitionTaskStore(max_workers=1)
    runner_called = []
    failed_errors: list[str] = []

    task = store.submit(
        data_type="optical",
        operation="run",
        runner=lambda: (runner_called.append(True), {"ok": True})[1],
        on_started=lambda _task_id: (_ for _ in ()).throw(RuntimeError("start projection failed")),
        on_failed=lambda _task_id, error: failed_errors.append(error),
    )

    assert _wait_for_terminal_status(store, task.task_id) == "failed"
    assert runner_called == []
    assert failed_errors == ["start projection failed"]


def test_completion_hook_failure_does_not_relabel_completed_task() -> None:
    store = PartitionTaskStore(max_workers=1)

    task = store.submit(
        data_type="optical",
        operation="run",
        runner=lambda: {"ok": True},
        on_succeeded=lambda _task_id, _result: (_ for _ in ()).throw(RuntimeError("completion projection failed")),
    )

    assert _wait_for_terminal_status(store, task.task_id) == "completed"
    assert store.get(task.task_id).result == {"ok": True}


def test_cancellation_check_failure_after_commit_keeps_completed_result() -> None:
    store = PartitionTaskStore(max_workers=1)
    checks = iter([False, False, False])

    def cancellation_check() -> bool:
        try:
            return next(checks)
        except StopIteration as exc:
            raise RuntimeError("cancellation store unavailable") from exc

    task = store.submit(
        data_type="optical",
        operation="run",
        runner=lambda: {"ok": True},
        cancellation_check=cancellation_check,
    )

    assert _wait_for_terminal_status(store, task.task_id) == "completed"
    assert store.get(task.task_id).result == {"ok": True}


def test_max_workers_reads_environment_variable(monkeypatch) -> None:
    monkeypatch.setenv("CUBE_WEB_PARTITION_MAX_WORKERS", "7")
    store = PartitionTaskStore()
    assert store.max_workers == 7


def test_max_workers_defaults_and_falls_back_on_bad_values(monkeypatch) -> None:
    monkeypatch.delenv("CUBE_WEB_PARTITION_MAX_WORKERS", raising=False)
    assert _resolve_max_workers() == DEFAULT_PARTITION_MAX_WORKERS

    monkeypatch.setenv("CUBE_WEB_PARTITION_MAX_WORKERS", "not-a-number")
    assert _resolve_max_workers() == DEFAULT_PARTITION_MAX_WORKERS

    monkeypatch.setenv("CUBE_WEB_PARTITION_MAX_WORKERS", "0")
    assert _resolve_max_workers() == 1

    monkeypatch.setenv("CUBE_WEB_PARTITION_MAX_WORKERS", "-3")
    assert _resolve_max_workers() == 1


def test_max_workers_reads_runtime_env_file(monkeypatch, tmp_path) -> None:
    env_file = tmp_path / "cube_web.env"
    env_file.write_text("CUBE_WEB_PARTITION_MAX_WORKERS=6\n", encoding="utf-8")
    monkeypatch.delenv("CUBE_WEB_PARTITION_MAX_WORKERS", raising=False)
    monkeypatch.setenv("CUBE_WEB_ENV_FILE", str(env_file))

    assert _resolve_max_workers() == 6


def test_explicit_max_workers_overrides_environment(monkeypatch) -> None:
    monkeypatch.setenv("CUBE_WEB_PARTITION_MAX_WORKERS", "9")
    store = PartitionTaskStore(max_workers=2)
    assert store.max_workers == 2
