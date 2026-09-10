"""Execute one persisted partition attempt as an independent Ray Job driver."""
from __future__ import annotations

import argparse
import logging

from cube_split import runtime_config
from cube_split.partition_timing import TimingRecorder

from cube_web.services.partition_contracts import StrictPartitionRequest
from cube_web.services.partition_dataset_runner import NormalizedPartitionDatasetRunner
from cube_web.services.partition_domain_store import OpenGaussPartitionDomainStore
from cube_web.services.partition_job_store import get_partition_job_store
from cube_web.services.partition_service import PartitionService
from cube_web.services.partition_workflow import PartitionWorkflowService, _safe_dataset_error
from cube_web.services.scene_repository import OpenGaussSceneRepository

logger = logging.getLogger(__name__)


def _project_task_safely(scene_repository, task_id: str, status: str, result: dict | None) -> None:
    try:
        scene_repository.update_partition_task(task_id, status, result)
    except Exception:
        logger.exception("Failed to project partition task %s as %s", task_id, status)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-id", required=True)
    args = parser.parse_args()

    driver_timing = TimingRecorder("ray_job_driver")
    with driver_timing.phase("driver.bootstrap"):
        store = get_partition_job_store()
        attempt = store.get_attempt(args.task_id)
        if attempt is None:
            raise RuntimeError(f"Partition attempt not found: {args.task_id}")
    with driver_timing.phase("task.start_attempt"):
        if not store.start_attempt(args.task_id):
            return
    scene_repository = None
    workflow = None
    try:
        scene_repository = OpenGaussSceneRepository(runtime_config.require_postgres_dsn())
        # Projection must not prevent the actual Ray Job from executing; terminal
        # attempt state remains the source of truth if this write is unavailable.
        _project_task_safely(scene_repository, args.task_id, "running", None)
        payload = dict(attempt.get("payload") or {})
        payload.pop("strict_partition_request", None)
        payload.pop("dataset_partitions", None)
        request = StrictPartitionRequest.model_validate(payload)
        workflow = PartitionWorkflowService(
            PartitionService(),
            store=store,
            domain_store=OpenGaussPartitionDomainStore(dsn=runtime_config.require_postgres_dsn()),
            runner=NormalizedPartitionDatasetRunner(),
        )
        with driver_timing.phase("workflow.run"):
            result = workflow.run(task_id=args.task_id, request=request)
    except Exception as exc:
        safe_error = _safe_dataset_error(exc)
        if workflow is not None:
            try:
                workflow.on_task_failed(args.task_id, safe_error)
            except Exception:
                logger.exception("Failed to persist partition task failure %s", args.task_id)
        else:
            try:
                store.fail_attempt(args.task_id, safe_error, manual_required=True, error_type="ray_job_driver_failed")
            except Exception:
                logger.exception("Failed to persist Ray driver failure %s", args.task_id)
        if scene_repository is not None:
            _project_task_safely(scene_repository, args.task_id, "failed", {"error": safe_error})
        raise
    result["timings"] = {
        **dict(result.get("timings") or {}),
        "job_driver": driver_timing.finish(),
    }
    try:
        workflow.on_task_succeeded(args.task_id, result)
    except Exception:
        logger.exception("Failed to persist partition task success %s", args.task_id)
    _project_task_safely(scene_repository, args.task_id, str(result.get("status") or "completed"), result)
    if result.get("status") in {"failed", "partial_failure"}:
        raise RuntimeError(f"Partition attempt completed with status {result['status']}")


if __name__ == "__main__":  # pragma: no cover - exercised by Ray Jobs.
    main()
