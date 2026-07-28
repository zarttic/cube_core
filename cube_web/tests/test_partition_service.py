from __future__ import annotations

import threading

from cube_web.services.partition_service import (
    DEFAULT_PARTITION_MAX_WORKERS,
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
