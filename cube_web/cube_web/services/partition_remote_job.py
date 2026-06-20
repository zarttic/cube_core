from __future__ import annotations

import argparse
import copy
import os
from importlib import import_module
from typing import Any

from cube_web.services.partition_job_store import get_partition_job_store
from cube_web.services.partition_service import PartitionService, build_production_partition_registry
from cube_web.services.partition_workflow import PartitionWorkflowService


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a persisted partition attempt inside a Ray Job")
    parser.add_argument("--task-id", required=True)
    return parser.parse_args()


def _runner_for_data_type(data_type: str):
    runners = import_module("cube_web.services.partition_runners")
    mapping = {
        "optical": lambda payload: runners._run_optical_partition_from_payload(payload, mode="partition_run"),
        "product": lambda payload: runners._run_product_partition_demo(payload, mode="partition_run"),
        "radar": lambda payload: runners._run_radar_partition_demo(payload, mode="partition_run"),
        "entity": lambda payload: runners._run_entity_partition_from_payload(payload, mode="partition_run"),
        "carbon": lambda payload: runners._run_carbon_partition_demo(mode="partition_run", payload=payload),
    }
    try:
        return mapping[data_type]
    except KeyError as exc:
        raise RuntimeError(f"Unsupported partition data_type for remote job: {data_type}") from exc


def _create_remote_partition_service() -> PartitionService:
    return PartitionService(
        build_production_partition_registry(
            optical_run=_runner_for_data_type("optical"),
            carbon_run=_runner_for_data_type("carbon"),
            product_run=_runner_for_data_type("product"),
            radar_run=_runner_for_data_type("radar"),
            entity_run=_runner_for_data_type("entity"),
        )
    )


def run_task(task_id: str) -> int:
    store = get_partition_job_store()
    workflow = PartitionWorkflowService(_create_remote_partition_service(), store=store)
    attempt = store.get_attempt(task_id)
    if attempt is None:
        raise RuntimeError(f"Partition task not found: {task_id}")
    batch = store.get_batch(str(attempt.get("batch_id") or ""))
    if batch is None:
        raise RuntimeError(f"Partition batch not found for task: {task_id}")
    payload = copy.deepcopy(attempt.get("payload") if isinstance(attempt.get("payload"), dict) else {})
    data_type = str(batch.get("data_type") or payload.get("data_type") or "").strip().lower()
    if not data_type:
        raise RuntimeError(f"Partition data_type missing for task: {task_id}")

    payload.setdefault("batch_id", batch.get("batch_id"))
    payload.setdefault("batch_name", batch.get("batch_name"))
    payload["job_id"] = task_id
    payload["ray_task_id"] = task_id

    def cancellation_check() -> bool:
        return store.is_cancel_requested(task_id)

    payload["_cancellation_check"] = cancellation_check
    payload["cancellation_check"] = cancellation_check

    runner = _runner_for_data_type(data_type)
    if _should_skip_cancelled_attempt(attempt, store, task_id):
        store.mark_cancelled(task_id)
        return 0

    store.start_attempt(task_id)
    attempt = store.get_attempt(task_id)
    if attempt is None:
        raise RuntimeError(f"Partition task not found after start: {task_id}")
    if _should_skip_cancelled_attempt(attempt, store, task_id):
        store.mark_cancelled(task_id)
        return 0
    if str(attempt.get("status") or "") != "running":
        raise RuntimeError(f"Partition task could not enter running state: {task_id}")
    try:
        result = runner(payload)
    except Exception as exc:
        error = _error_text(exc)
        if "cancel" in error.lower():
            workflow.on_task_failed(task_id, error)
            return 1
        workflow.on_task_failed(task_id, error)
        raise
    result = dict(result or {})
    result.setdefault("ray_task_id", task_id)
    result.setdefault("job_id", task_id)
    if store.is_cancel_requested(task_id):
        store.mark_cancelled(task_id)
        return 0
    workflow.on_task_succeeded(task_id, result)
    return 0


def main() -> int:
    args = _parse_args()
    task_id = str(args.task_id).strip()
    if not task_id:
        raise RuntimeError("task_id is required")
    if not os.environ.get("PYTHONPATH"):
        os.environ["PYTHONPATH"] = ".:./cube_encoder:./cube_split:./cube_web"
    return run_task(task_id)


def _error_text(exc: Exception) -> str:
    detail = getattr(exc, "detail", None)
    if isinstance(detail, str) and detail.strip():
        return detail.strip()
    text = str(exc).strip()
    return text or exc.__class__.__name__


def _should_skip_cancelled_attempt(store_attempt: dict[str, Any], store, task_id: str) -> bool:
    status = str(store_attempt.get("status") or "")
    return status in {"cancel_requested", "cancelled"} or store.is_cancel_requested(task_id)


if __name__ == "__main__":
    raise SystemExit(main())
