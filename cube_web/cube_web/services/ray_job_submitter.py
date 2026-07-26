"""Submit managed partition attempts as independent Ray Jobs."""
from __future__ import annotations

import os
from typing import Any

from cube_split import runtime_config
from cube_split.jobs.ray_logical_partition_job import _ray_runtime_env_from_env


def ray_job_executor_enabled() -> bool:
    return runtime_config.env_text("CUBE_WEB_PARTITION_EXECUTOR", "local").lower() == "ray_job"


class RayJobPartitionSubmitter:
    def __init__(self, address: str | None = None) -> None:
        self.address = address or runtime_config.env_text("CUBE_WEB_RAY_JOB_ADDRESS")

    def submit(self, task_id: str) -> str:
        if not self.address:
            raise RuntimeError("CUBE_WEB_RAY_JOB_ADDRESS is required for the ray_job partition executor")
        from ray.job_submission import JobSubmissionClient

        runtime_env = dict(_ray_runtime_env_from_env() or {})
        env_vars = dict(runtime_env.get("env_vars") or {})
        minio = runtime_config.minio_settings()
        env_vars.update({
            "CUBE_WEB_RAY_JOB_DRIVER": "1",
            "CUBE_WEB_POSTGRES_DSN": runtime_config.require_postgres_dsn(),
            "CUBE_WEB_RAY_ADDRESS": runtime_config.require_ray_address(),
            "CUBE_WEB_MINIO_ENDPOINT": minio.endpoint,
            "CUBE_WEB_MINIO_ACCESS_KEY": minio.access_key,
            "CUBE_WEB_MINIO_SECRET_KEY": minio.secret_key,
            "CUBE_WEB_MINIO_BUCKET": minio.bucket,
        })
        runtime_env["env_vars"] = env_vars
        job_id = f"partition-{task_id.removeprefix('partition-')}"
        return JobSubmissionClient(self.address).submit_job(
            entrypoint=f"python3.11 -m cube_web.jobs.partition_batch_job --task-id {task_id}",
            submission_id=job_id,
            runtime_env=runtime_env,
            metadata={"cube_task_id": task_id, "cube_job_kind": "partition"},
            entrypoint_num_cpus=0,
        )

    def status(self, ray_job_id: str) -> str:
        from ray.job_submission import JobSubmissionClient

        return str(JobSubmissionClient(self.address).get_job_status(ray_job_id))

    def stop(self, ray_job_id: str) -> bool:
        from ray.job_submission import JobSubmissionClient

        return bool(JobSubmissionClient(self.address).stop_job(ray_job_id))
