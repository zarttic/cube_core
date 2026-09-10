"""Submit managed partition attempts as independent Ray Jobs."""
from __future__ import annotations

import time
from collections.abc import Callable
from threading import Lock, local

from cube_split import runtime_config
from cube_split.jobs.ray_logical_partition_job import _ray_runtime_env_from_env
from requests import Timeout as RequestsTimeout

DEFAULT_JOB_CLIENT_TIMEOUT_SECONDS = 30.0
MIN_JOB_CLIENT_TIMEOUT_SECONDS = 1.0
JOB_STATUS_CACHE_SECONDS = 2.0

_JOB_CLIENT_LOCAL = local()
_STATUS_CACHE: dict[tuple[str, str], tuple[float, str]] = {}
_STATUS_CACHE_LOCK = Lock()


class RayJobClientTimeoutError(TimeoutError):
    """A Ray JobSubmissionClient call exceeded the configured timeout."""


def _job_client(address: str):
    """Reuse one timeout-configured JobSubmissionClient per caller thread."""
    clients = getattr(_JOB_CLIENT_LOCAL, "clients", None)
    if clients is None:
        clients = {}
        _JOB_CLIENT_LOCAL.clients = clients
    client = clients.get(address)
    if client is None:
        from ray.job_submission import JobSubmissionClient

        timeout_seconds = job_client_timeout_seconds()

        class TimeoutJobSubmissionClient(JobSubmissionClient):
            def _do_request(self, method, endpoint, **kwargs):
                kwargs.setdefault("timeout", timeout_seconds)
                return super()._do_request(method, endpoint, **kwargs)

        client = TimeoutJobSubmissionClient(address)
        clients[address] = client
    return client


def _cached_status(address: str, ray_job_id: str) -> str | None:
    with _STATUS_CACHE_LOCK:
        cached = _STATUS_CACHE.get((address, ray_job_id))
        if cached is None:
            return None
        if time.monotonic() - cached[0] >= JOB_STATUS_CACHE_SECONDS:
            _STATUS_CACHE.pop((address, ray_job_id), None)
            return None
        return cached[1]


def _cache_status(address: str, ray_job_id: str, status: str) -> None:
    with _STATUS_CACHE_LOCK:
        _STATUS_CACHE[(address, ray_job_id)] = (time.monotonic(), status)


def _invalidate_status(address: str, ray_job_id: str) -> None:
    with _STATUS_CACHE_LOCK:
        _STATUS_CACHE.pop((address, ray_job_id), None)


def ray_job_executor_enabled() -> bool:
    return runtime_config.env_text("CUBE_WEB_PARTITION_EXECUTOR", "local").lower() == "ray_job"


def job_client_timeout_seconds() -> float:
    """Resolve the Ray job client timeout from CUBE_WEB_RAY_JOB_TIMEOUT_SECONDS."""
    raw = runtime_config.env_text("CUBE_WEB_RAY_JOB_TIMEOUT_SECONDS")
    try:
        value = float(raw) if raw else DEFAULT_JOB_CLIENT_TIMEOUT_SECONDS
    except ValueError:
        value = DEFAULT_JOB_CLIENT_TIMEOUT_SECONDS
    return max(value, MIN_JOB_CLIENT_TIMEOUT_SECONDS)


def _call_job_client(operation: str, context: str, call: Callable[[], object]) -> object:
    """Translate the transport timeout configured on JobSubmissionClient calls."""
    timeout_seconds = job_client_timeout_seconds()
    try:
        return call()
    except RequestsTimeout as exc:
        raise RayJobClientTimeoutError(
            f"Ray job {operation} timed out after {timeout_seconds:g}s ({context})"
        ) from exc


class RayJobPartitionSubmitter:
    def __init__(self, address: str | None = None) -> None:
        self.address = address or runtime_config.env_text("CUBE_WEB_RAY_JOB_ADDRESS")

    def submit(self, task_id: str) -> str:
        if not self.address:
            raise RuntimeError("CUBE_WEB_RAY_JOB_ADDRESS is required for the ray_job partition executor")
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
        for name in (
            "CUBE_WEB_RAY_BATCH_SCHEDULER",
            "CUBE_ENTITY_RAY_PARALLELISM",
            "CUBE_ENTITY_BANDS_PER_TASK",
            "CUBE_ENTITY_UPLOAD_WORKERS",
            "CUBE_ENTITY_MINIO_PARALLEL_UPLOADS",
            "CUBE_WEB_RAY_WORKER_RESOURCE",
        ):
            value = runtime_config.env_text(name)
            if value is not None:
                env_vars[name] = value
        runtime_env["env_vars"] = env_vars
        job_id = f"partition-{task_id.removeprefix('partition-')}"

        def _submit() -> str:
            return _job_client(self.address).submit_job(
                entrypoint=f"python3.11 -m cube_web.jobs.partition_batch_job --task-id {task_id}",
                submission_id=job_id,
                runtime_env=runtime_env,
                metadata={"cube_task_id": task_id, "cube_job_kind": "partition"},
                entrypoint_num_cpus=0,
            )

        submitted_job_id = str(_call_job_client("submit", task_id, _submit))
        _invalidate_status(self.address, submitted_job_id)
        return submitted_job_id

    def status(self, ray_job_id: str) -> str:
        cached = _cached_status(self.address, ray_job_id)
        if cached is not None:
            return cached

        def _status() -> str:
            return str(_job_client(self.address).get_job_status(ray_job_id))

        status = str(_call_job_client("status", ray_job_id, _status))
        _cache_status(self.address, ray_job_id, status)
        return status

    def stop(self, ray_job_id: str) -> bool:
        def _stop() -> bool:
            return bool(_job_client(self.address).stop_job(ray_job_id))

        stopped = bool(_call_job_client("stop", ray_job_id, _stop))
        _invalidate_status(self.address, ray_job_id)
        return stopped
