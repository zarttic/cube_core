from __future__ import annotations

from importlib import import_module
from typing import Any, Callable


def _load_runner(module_name: str) -> Callable[[Any], Any] | None:
    try:
        module = import_module(module_name)
    except ModuleNotFoundError:  # pragma: no cover - cube_web can run with SDK-only routes.
        return None
    runner = getattr(module, "run_quality_check", None)
    return runner if callable(runner) else None


# ponytail: optional runner availability is fixed for this process; reload if installs change.
run_optical_quality_check = _load_runner("cube_split.quality.optical_quality")
run_radar_quality_check = _load_runner("cube_split.quality.radar_quality")
run_product_quality_check = _load_runner("cube_split.quality.product_quality")
run_carbon_quality_check = _load_runner("cube_split.quality.carbon_quality")
