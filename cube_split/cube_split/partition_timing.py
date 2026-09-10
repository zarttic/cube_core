"""Small, JSON-safe timing records shared by partition drivers and workers."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from time import perf_counter
from typing import Any, Iterator


def utc_now_iso() -> str:
    """Return a stable UTC timestamp for persisted timing reports."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class TimingRecorder:
    """Accumulate phase wall time without changing the surrounding workflow."""

    def __init__(self, scope: str) -> None:
        self.scope = scope
        self.started_at = utc_now_iso()
        self._started_tick = perf_counter()
        self._phases: dict[str, dict[str, float | int]] = {}
        self._counters: dict[str, int] = {}
        self._attributes: dict[str, Any] = {}
        self._finished: dict[str, Any] | None = None

    @contextmanager
    def phase(self, name: str) -> Iterator[None]:
        started = perf_counter()
        try:
            yield
        finally:
            self.add_phase(name, perf_counter() - started)

    def add_phase(self, name: str, elapsed_sec: float) -> None:
        phase = self._phases.setdefault(name, {"elapsed_sec": 0.0, "count": 0})
        phase["elapsed_sec"] = float(phase["elapsed_sec"]) + max(0.0, float(elapsed_sec))
        phase["count"] = int(phase["count"]) + 1

    def add_counter(self, name: str, amount: int = 1) -> None:
        self._counters[name] = self._counters.get(name, 0) + int(amount)

    def set_attribute(self, name: str, value: Any) -> None:
        self._attributes[name] = value

    def finish(self) -> dict[str, Any]:
        if self._finished is not None:
            return self._finished
        finished_at = utc_now_iso()
        self._finished = {
            "schema_version": 1,
            "scope": self.scope,
            "started_at": self.started_at,
            "finished_at": finished_at,
            "elapsed_sec": round(perf_counter() - self._started_tick, 6),
            "phases": {
                name: {
                    "elapsed_sec": round(float(value["elapsed_sec"]), 6),
                    "count": int(value["count"]),
                }
                for name, value in self._phases.items()
            },
            "counters": dict(self._counters),
            "attributes": dict(self._attributes),
        }
        return self._finished


_WORKER_SCOPE_SUFFIX = "_worker"


def _timing_records(value: Any) -> Iterator[dict[str, Any]]:
    """Yield nested timing records without depending on one result shape."""
    if isinstance(value, dict):
        if isinstance(value.get("scope"), str):
            yield value
        for child in value.values():
            yield from _timing_records(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            yield from _timing_records(child)


def _timing_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def partition_timing_from_workers(timings: Any) -> dict[str, Any] | None:
    """Build the start marker for the partition-to-ingest wall-clock metric.

    A worker timing starts immediately before the worker begins partitioning.
    The latest worker start is therefore the first instant at which every
    recorded worker has started partitioning.  No fallback timestamp is
    invented when a backend does not expose worker timings.
    """
    workers = [
        record
        for record in _timing_records(timings)
        if str(record.get("scope") or "").endswith(_WORKER_SCOPE_SUFFIX)
        and _timing_datetime(record.get("started_at")) is not None
    ]
    if not workers:
        return None
    latest = max(workers, key=lambda record: _timing_datetime(record["started_at"]))
    return {
        "schema_version": 1,
        "scope": "partition_to_ingest",
        "started_at": latest["started_at"],
        "finished_at": None,
        "elapsed_sec": None,
        "start_condition": "all_workers_started_partitioning",
        "end_condition": "all_ingest_completed",
        "worker_count": len(workers),
        "worker_scopes": sorted({str(record["scope"]) for record in workers}),
    }


def finish_partition_timing(timing: dict[str, Any] | None, finished_at: Any) -> dict[str, Any] | None:
    """Attach the ingest completion timestamp and elapsed seconds."""
    if not isinstance(timing, dict):
        return None
    started = _timing_datetime(timing.get("started_at"))
    finished = _timing_datetime(finished_at)
    if started is None or finished is None or finished < started:
        return dict(timing)
    completed = dict(timing)
    completed["finished_at"] = finished_at
    completed["elapsed_sec"] = round((finished - started).total_seconds(), 6)
    return completed
