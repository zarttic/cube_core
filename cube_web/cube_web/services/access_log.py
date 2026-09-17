"""HTTP access logging with request correlation.

Registered as the outermost ASGI middleware so it also observes early responses
from the auth middleware. ``/`` and ``/health`` are skipped to keep probe traffic
out of the log; set ``CUBE_LOG_ACCESS=0`` to disable the middleware entirely.
"""

from __future__ import annotations

import logging
import time

from cube_split import logging_config, runtime_config
from starlette.requests import Request

from cube_web.services.api_errors import request_id_from_header

logger = logging.getLogger("cube_web.access")

SKIP_PATHS = {"/", "/health", "/GNent"}
_CLIENT_ERROR_LEVEL = logging.WARNING
_SERVER_ERROR_LEVEL = logging.ERROR


def access_logging_enabled() -> bool:
    return runtime_config.bool_option(runtime_config.env_text("CUBE_LOG_ACCESS", "1"), True)


def _actor_name(request: Request) -> str:
    actor = getattr(request.state, "actor", None)
    return str(getattr(actor, "username", "") or "-")


def _status_level(status_code: int) -> int:
    if status_code >= 500:
        return _SERVER_ERROR_LEVEL
    if status_code >= 400:
        return _CLIENT_ERROR_LEVEL
    return logging.INFO


async def access_log_middleware(request: Request, call_next):
    if not access_logging_enabled() or request.url.path in SKIP_PATHS:
        return await call_next(request)

    request_id = request_id_from_header(request)
    started = time.perf_counter()
    status_code = 500
    with logging_config.logging_context(request_id=request_id):
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            duration_ms = round((time.perf_counter() - started) * 1000, 1)
            logger.log(
                _status_level(status_code),
                "web.access method=%s path=%s status=%s duration_ms=%s actor=%s request_id=%s",
                request.method,
                request.url.path,
                status_code,
                duration_ms,
                _actor_name(request),
                request_id,
            )
