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
