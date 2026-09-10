from __future__ import annotations

import re
from typing import Any
from uuid import uuid4

from fastapi import Request
from fastapi.responses import JSONResponse

REQUEST_ID_HEADER = "X-Request-ID"
_REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_MISSING = object()


def ensure_request_id(request: Request) -> str:
    value = str(getattr(request.state, "request_id", "") or "").strip()
    if not _REQUEST_ID_PATTERN.fullmatch(value):
        value = uuid4().hex
        request.state.request_id = value
    return value


def request_id_from_header(request: Request) -> str:
    incoming = str(request.headers.get(REQUEST_ID_HEADER) or "").strip()
    value = incoming if _REQUEST_ID_PATTERN.fullmatch(incoming) else uuid4().hex
    request.state.request_id = value
    return value


def detail_message(detail: Any, fallback: str = "请求失败") -> str:
    if isinstance(detail, dict):
        return detail_message(detail.get("message") or detail.get("msg") or detail.get("detail"), fallback)
    if isinstance(detail, (list, tuple)):
        messages = [detail_message(item, "") for item in detail]
        return "；".join(item for item in messages if item) or fallback
    value = str(detail or "").strip()
    return value or fallback


def detail_code(status_code: int, detail: Any = None) -> str:
    if isinstance(detail, dict):
        code = str(detail.get("code") or "").strip()
        if code:
            return code
    return {
        400: "bad_request",
        401: "unauthorized",
        403: "forbidden",
        404: "not_found",
        409: "conflict",
        422: "validation_error",
        502: "bad_gateway",
        503: "service_unavailable",
    }.get(status_code, "http_error" if status_code < 500 else "internal_error")


def error_response(
    request: Request,
    *,
    status_code: int,
    code: str,
    message: str,
    detail: Any = _MISSING,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    request_id = ensure_request_id(request)
    content: dict[str, Any] = {
        "error": {
            "code": code,
            "message": message,
            "request_id": request_id,
        }
    }
    if detail is not _MISSING:
        content["detail"] = detail
    response_headers = dict(headers or {})
    response_headers[REQUEST_ID_HEADER] = request_id
    return JSONResponse(status_code=status_code, content=content, headers=response_headers)
