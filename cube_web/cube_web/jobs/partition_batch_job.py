"""Execute one persisted partition attempt as an independent Ray Job driver."""
from __future__ import annotations

import argparse

from cube_split import runtime_config
from cube_split.partition_timing import TimingRecorder

from cube_web.services.partition_contracts import StrictPartitionRequest
from cube_web.services.partition_dataset_runner import NormalizedPartitionDatasetRunner
from cube_web.services.partition_domain_store import OpenGaussPartitionDomainStore
from cube_web.services.partition_job_store import get_partition_job_store
from cube_web.services.partition_service import PartitionService
from cube_web.services.partition_workflow import PartitionWorkflowService
from cube_web.services.scene_repository import OpenGaussSceneRepository


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
    scene_repository = OpenGaussSceneRepository(runtime_config.require_postgres_dsn())
    try:
        with driver_timing.phase("workflow.run"):
            result = workflow.run(task_id=args.task_id, request=request)
    except Exception as exc:
        workflow.on_task_failed(args.task_id, str(exc))
        scene_repository.update_partition_task(args.task_id, "failed", {"error": str(exc)})
        raise
    result["timings"] = {
        **dict(result.get("timings") or {}),
        "job_driver": driver_timing.finish(),
    }
    workflow.on_task_succeeded(args.task_id, result)
    scene_repository.update_partition_task(args.task_id, str(result.get("status") or "completed"), result)
    if result.get("status") in {"failed", "partial_failure"}:
        raise RuntimeError(f"Partition attempt completed with status {result['status']}")


if __name__ == "__main__":  # pragma: no cover - exercised by Ray Jobs.
    main()
