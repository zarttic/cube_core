from __future__ import annotations

import argparse
import copy
import os
from importlib import import_module
from typing import Any

from fastapi import HTTPException

from cube_web.routes.partition import create_partition_service
from cube_web.services.partition_job_store import get_partition_job_store
from cube_web.services.partition_workflow import PartitionWorkflowService


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a persisted partition attempt inside a Ray Job")
    parser.add_argument("--task-id", required=True)
    return parser.parse_args()


def _runner_for_data_type(data_type: str):
    adapters = import_module("cube_web.routes.partition_adapters")
    mapping = {
        "optical": adapters.partition_optical_run,
        "product": adapters.partition_product_run,
        "radar": adapters.partition_radar_run,
        "entity": adapters.partition_entity_run,
        "carbon": adapters.partition_carbon_run,
    }
    try:
        return mapping[data_type]
    except KeyError as exc:
        raise RuntimeError(f"Unsupported partition data_type for remote job: {data_type}") from exc


def run_task(task_id: str) -> int:
    store = get_partition_job_store()
    workflow = PartitionWorkflowService(create_partition_service(), store=store)
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
    workflow.on_task_started(task_id)
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


if __name__ == "__main__":
    raise SystemExit(main())


def _error_text(exc: Exception) -> str:
    if isinstance(exc, HTTPException):
        detail = exc.detail
        if isinstance(detail, str) and detail.strip():
            return detail.strip()
    text = str(exc).strip()
    return text or exc.__class__.__name__
