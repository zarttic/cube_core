from __future__ import annotations

from typing import Any


class PartitionCancelledError(RuntimeError):
    pass


def check_cancelled(args: Any) -> None:
    checker = getattr(args, "cancellation_check", None)
    if checker is None:
        return
    if checker():
        raise PartitionCancelledError("Partition task cancelled")


def cancel_ray_refs(ray: Any, refs: list[Any]) -> None:
    for ref in refs:
        try:
            ray.cancel(ref, force=True)
        except Exception:
            pass


def shutdown_ray_if_needed(ray: Any, already_initialized: bool) -> None:
    if already_initialized:
        return
    try:
        ray.shutdown()
    except Exception:
        pass
