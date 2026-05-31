from __future__ import annotations

from importlib import import_module
from typing import Any, Callable


def _load_runner(module_name: str) -> Callable[[Any], Any] | None:
    try:
        module = import_module(module_name)
    except ModuleNotFoundError:  # pragma: no cover - cube_web can run with SDK-only routes.
        return None
    return getattr(module, "run_quality_check", None)


class _QualityRunnerProxy:
    def __init__(self, module_name: str) -> None:
        self.module_name = module_name

    def __call__(self, args: Any) -> Any:
        runner = _load_runner(self.module_name)
        if runner is None:
            return None
        return runner(args)

    def __bool__(self) -> bool:
        return _load_runner(self.module_name) is not None


run_optical_quality_check = _QualityRunnerProxy("cube_split.quality.optical_quality")
run_product_quality_check = _QualityRunnerProxy("cube_split.quality.product_quality")
run_carbon_quality_check = _QualityRunnerProxy("cube_split.quality.carbon_quality")
