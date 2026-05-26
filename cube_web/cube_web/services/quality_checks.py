from __future__ import annotations

try:
    from cube_split.quality.optical_quality import run_quality_check as run_optical_quality_check
except ModuleNotFoundError:  # pragma: no cover - cube_web can run with SDK-only routes.
    run_optical_quality_check = None

try:
    from cube_split.quality.product_quality import run_quality_check as run_product_quality_check
except ModuleNotFoundError:  # pragma: no cover - cube_web can run with SDK-only routes.
    run_product_quality_check = None
