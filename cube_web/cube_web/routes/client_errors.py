"""Browser-side error reporting endpoint.

Accepts a small, rate-limited batch of client errors so browser exceptions and API
failures become visible in server logs. The endpoint is reachable before login
(optional auth) and never stores credentials, request bodies, or full user agents.
"""

from __future__ import annotations

import logging
import re
import time
from threading import Lock

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError

logger = logging.getLogger("cube_web.client_errors")

MAX_BODY_BYTES = 32 * 1024
RATE_LIMIT_PER_MINUTE = 30
_RATE_LIMIT_WINDOW_SECONDS = 60.0
_MAX_TRACKED_CLIENTS = 4096
_MAX_LOG_FIELD_LENGTH = 300
_BEARER_PATTERN = re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/-]+=*")
_NEWLINE_PATTERN = re.compile(r"[\r\n]+")


class ClientErrorEvent(BaseModel):
    model_config = ConfigDict(extra="ignore")

    ts: str | None = Field(default=None, max_length=40)
    level: str = Field(default="error", max_length=10)
    scope: str | None = Field(default=None, max_length=120)
    message: str = Field(min_length=1, max_length=500)
    stack: str | None = Field(default=None, max_length=2000)
    route: str | None = Field(default=None, max_length=300)
    request_id: str | None = Field(default=None, max_length=128)
    status: int | None = None
    code: str | None = Field(default=None, max_length=120)


class ClientInfo(BaseModel):
    model_config = ConfigDict(extra="ignore")

    version: str | None = Field(default=None, max_length=60)
    ua: str | None = Field(default=None, max_length=300)


class ClientErrorReport(BaseModel):
    model_config = ConfigDict(extra="ignore")

    client: ClientInfo = Field(default_factory=ClientInfo)
    events: list[ClientErrorEvent] = Field(min_length=1, max_length=10)


class _SlidingWindowLimiter:
    """Per-key in-process limiter; a multi-process deployment enforces per process."""

    def __init__(self, limit: int = RATE_LIMIT_PER_MINUTE, window_seconds: float = _RATE_LIMIT_WINDOW_SECONDS) -> None:
        self._limit = limit
        self._window_seconds = window_seconds
        self._entries: dict[str, tuple[float, int]] = {}
        self._lock = Lock()

    def allow(self, key: str, *, now: float | None = None) -> bool:
        moment = time.monotonic() if now is None else now
        with self._lock:
            start, count = self._entries.get(key, (moment, 0))
            if moment - start >= self._window_seconds:
                start, count = moment, 0
            if count >= self._limit:
                return False
            self._entries[key] = (start, count + 1)
            if len(self._entries) > _MAX_TRACKED_CLIENTS:
                self._prune(moment)
            return True

    def _prune(self, moment: float) -> None:
        expired = [key for key, (start, _count) in self._entries.items() if moment - start >= self._window_seconds]
        for key in expired:
            self._entries.pop(key, None)


limiter = _SlidingWindowLimiter()


def _sanitize(value: str | None, limit: int = _MAX_LOG_FIELD_LENGTH) -> str:
    if not value:
        return ""
    text = _NEWLINE_PATTERN.sub(" ", _BEARER_PATTERN.sub(r"\1[redacted]", str(value))).strip()
    return text[:limit]


async def _read_limited_body(request: Request) -> bytes | None:
    declared = request.headers.get("content-length")
    if declared:
        try:
            if int(declared) > MAX_BODY_BYTES:
                return None
        except ValueError:
            return None
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > MAX_BODY_BYTES:
            return None
        chunks.append(chunk)
    return b"".join(chunks)


def create_client_errors_router() -> APIRouter:
    router = APIRouter(prefix="/client-errors", tags=["client-errors"])

    @router.post("")
    async def report_client_errors(request: Request) -> JSONResponse:
        client_key = request.client.host if request.client else "unknown"
        if not limiter.allow(client_key):
            raise HTTPException(
                status_code=429,
                detail="Too many client error reports",
                headers={"Retry-After": "60"},
            )
        raw = await _read_limited_body(request)
        if raw is None:
            raise HTTPException(status_code=413, detail="Client error report is too large")
        try:
            report = ClientErrorReport.model_validate_json(raw)
        except ValidationError as exc:
            raise HTTPException(status_code=422, detail="Invalid client error report") from exc

        actor = getattr(request.state, "actor", None)
        actor_name = str(getattr(actor, "username", "") or "-")
        for event in report.events:
            logger.warning(
                "client.error scope=%s level=%s status=%s code=%s route=%s request_id=%s actor=%s message=%s",
                _sanitize(event.scope) or "-",
                event.level,
                event.status if event.status is not None else "-",
                _sanitize(event.code) or "-",
                _sanitize(event.route) or "-",
                _sanitize(event.request_id) or "-",
                actor_name,
                _sanitize(event.message),
            )
        return JSONResponse(status_code=202, content={"status": "accepted", "count": len(report.events)})

    return router
