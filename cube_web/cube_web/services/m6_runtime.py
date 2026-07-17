from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

from cube_split import runtime_config

M6Mode = Literal["legacy", "shadow", "m6-read", "m6-primary"]
VALID_M6_MODES = frozenset({"legacy", "shadow", "m6-read", "m6-primary"})
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class M6RuntimePolicy:
    mode: M6Mode
    expose_m6_reads: bool
    expose_m6_writes: bool
    use_m6_import: bool


def m6_runtime_policy(value: str | None = None) -> M6RuntimePolicy:
    raw = (value if value is not None else runtime_config.env_text("CUBE_WEB_M6_MODE", "legacy")).strip().lower()
    if raw not in VALID_M6_MODES:
        logger.error("Invalid CUBE_WEB_M6_MODE=%r; falling back to legacy", raw)
        raw = "legacy"
    if raw == "m6-primary":
        return M6RuntimePolicy("m6-primary", True, True, True)
    if raw == "m6-read":
        return M6RuntimePolicy("m6-read", True, False, False)
    if raw == "shadow":
        logger.warning("M6 shadow mode keeps legacy writes until complete dual-write is available")
        return M6RuntimePolicy("shadow", False, False, False)
    return M6RuntimePolicy("legacy", False, False, False)
