from __future__ import annotations

from requests import Timeout as RequestsTimeout

from cube_web.services import ray_job_submitter as module
from cube_web.services.ray_job_submitter import RayJobClientTimeoutError, RayJobPartitionSubmitter


class _FakeJobClient:
    def __init__(self) -> None:
        self.status_calls: list[str] = []
        self.stop_calls: list[str] = []

    def get_job_status(self, job_id: str) -> str:
        self.status_calls.append(job_id)
        return "RUNNING"

    def stop_job(self, job_id: str) -> bool:
        self.stop_calls.append(job_id)
        return True


def test_status_uses_short_ttl_cache_and_stop_invalidates_it(monkeypatch) -> None:
    client = _FakeJobClient()
    module._STATUS_CACHE.clear()
    module._JOB_CLIENT_LOCAL.clients = {"http://ray:8265": client}
    monkeypatch.setattr(module, "_call_job_client", lambda _operation, _context, call: call())
    submitter = RayJobPartitionSubmitter("http://ray:8265")

    assert submitter.status("job-1") == "RUNNING"
    assert submitter.status("job-1") == "RUNNING"
    assert client.status_calls == ["job-1"]

    assert submitter.stop("job-1") is True
    assert submitter.status("job-1") == "RUNNING"
    assert client.stop_calls == ["job-1"]
    assert client.status_calls == ["job-1", "job-1"]


def test_transport_timeout_is_reported_without_background_executor(monkeypatch) -> None:
    monkeypatch.setattr(module, "job_client_timeout_seconds", lambda: 3.0)

    def timed_out():
        raise RequestsTimeout("request timed out")

    try:
        module._call_job_client("status", "job-1", timed_out)
    except RayJobClientTimeoutError as exc:
        assert "after 3s" in str(exc)
    else:  # pragma: no cover - assertion guard
        raise AssertionError("transport timeout must be translated")
