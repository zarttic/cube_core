from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import quote, unquote, urlencode, urljoin

from fastapi import HTTPException

from cube_web.services.runtime_config import AuthSettings, auth_settings


def bearer_token(authorization: str | None) -> str:
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise HTTPException(status_code=401, detail="Invalid Authorization header")
    return token.strip()


def verify_access_token(token: str, settings: AuthSettings | None = None) -> dict[str, Any]:
    settings = settings or auth_settings()
    if settings.jwt_algorithm.upper() != "HS256":
        raise HTTPException(status_code=500, detail=f"Unsupported JWT algorithm: {settings.jwt_algorithm}")
    if not settings.jwt_secret_key:
        raise HTTPException(status_code=500, detail="CUBE_WEB_AUTH_JWT_SECRET_KEY is required")

    parts = token.split(".")
    if len(parts) != 3:
        raise HTTPException(status_code=401, detail="Invalid token")
    signing_input = f"{parts[0]}.{parts[1]}".encode("ascii")
    expected = hmac.new(settings.jwt_secret_key.encode("utf-8"), signing_input, hashlib.sha256).digest()
    actual = _b64url_decode(parts[2])
    if not hmac.compare_digest(expected, actual):
        raise HTTPException(status_code=401, detail="Invalid token signature")

    try:
        header = json.loads(_b64url_decode(parts[0]))
        payload = json.loads(_b64url_decode(parts[1]))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise HTTPException(status_code=401, detail="Invalid token payload") from exc

    if str(header.get("alg", "")).upper() != settings.jwt_algorithm.upper():
        raise HTTPException(status_code=401, detail="Invalid token algorithm")
    exp = payload.get("exp")
    if exp is not None and float(exp) < time.time():
        raise HTTPException(status_code=401, detail="Token expired")
    return payload


def user_info_from_token(token: str, settings: AuthSettings | None = None) -> dict[str, Any]:
    payload = verify_access_token(token, settings)
    username = payload.get("username") or payload.get("name") or payload.get("sub") or ""
    role = payload.get("role") or payload.get("role_name") or payload.get("scope") or "普通用户"
    avatar_url = payload.get("avatar_url") or payload.get("avatarUrl") or payload.get("avatar") or ""
    return {
        "username": username,
        "role": role,
        "avatar_url": avatar_url,
        "avatarUrl": avatar_url,
        "sub": payload.get("sub"),
        "payload": payload,
    }


def encode_state(target_path: str, redirect_uri: str | None = None) -> str:
    payload = {
        "nonce": hashlib.sha256(f"{target_path}:{time.time_ns()}".encode("utf-8")).hexdigest()[:16],
        "target": target_path,
    }
    if redirect_uri:
        payload["redirect_uri"] = redirect_uri
    encoded = quote(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), safe="~()*!.'")
    return base64.b64encode(encoded.encode("utf-8")).decode("ascii")


def get_authorize_url(
    *,
    state: str | None = None,
    redirect_uri: str | None = None,
    settings: AuthSettings | None = None,
) -> str:
    settings = settings or auth_settings()
    if not settings.main_system_url:
        raise HTTPException(status_code=500, detail="CUBE_WEB_AUTH_MAIN_SYSTEM_URL is required")
    effective_redirect_uri = (redirect_uri or settings.redirect_uri).strip()
    if not effective_redirect_uri:
        raise HTTPException(status_code=500, detail="CUBE_WEB_AUTH_REDIRECT_URI is required")
    query = {
        "client_id": settings.client_id,
        "redirect_uri": effective_redirect_uri,
    }
    if state:
        query["state"] = state
    return f"{_endpoint_url(settings.main_system_url, settings.authorize_path)}?{urlencode(query)}"


def decode_state(state: str | None) -> dict[str, Any]:
    if not state:
        return {}
    try:
        raw = base64.b64decode(state.encode("ascii")).decode("utf-8")
        payload = json.loads(unquote(raw))
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def exchange_code_for_token(
    code: str,
    settings: AuthSettings | None = None,
    *,
    redirect_uri: str | None = None,
) -> dict[str, Any]:
    settings = settings or auth_settings()
    if not settings.client_secret:
        raise HTTPException(status_code=500, detail="CUBE_WEB_AUTH_CLIENT_SECRET is required")
    effective_redirect_uri = (redirect_uri or settings.redirect_uri).strip()
    if not effective_redirect_uri:
        raise HTTPException(status_code=500, detail="CUBE_WEB_AUTH_REDIRECT_URI is required")
    payload = {
        "grant_type": "authorization_code",
        "client_id": settings.client_id,
        "client_secret": settings.client_secret,
        "redirect_uri": effective_redirect_uri,
        "code": code,
    }
    return _post_form(_endpoint_url(settings.main_system_url, settings.token_path), payload)


def notify_logout(token: str | None, settings: AuthSettings | None = None) -> dict[str, Any]:
    if not token:
        return {"status": "skipped"}
    settings = settings or auth_settings()
    try:
        return _post_json(_endpoint_url(settings.main_system_url, settings.logout_path), {}, token=token)
    except HTTPException as exc:
        return {"status": "failed", "detail": exc.detail}


def _post_json(url: str, payload: dict[str, Any], token: str | None = None) -> dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    return _send_request(request)


def _post_form(url: str, payload: dict[str, Any], token: str | None = None) -> dict[str, Any]:
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(
        url,
        data=urlencode(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    return _send_request(request)


def _send_request(request: urllib.request.Request) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            text = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise HTTPException(status_code=exc.code, detail=detail or exc.reason) from exc
    except urllib.error.URLError as exc:
        raise HTTPException(status_code=502, detail=f"Auth service request failed: {exc.reason}") from exc

    if not text:
        return {}
    try:
        body = json.loads(text)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=502, detail="Auth service returned invalid JSON") from exc
    if not isinstance(body, dict):
        raise HTTPException(status_code=502, detail="Auth service returned invalid response")
    return body


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii"))


def _endpoint_url(base_url: str, path: str) -> str:
    if not base_url:
        raise HTTPException(status_code=500, detail="CUBE_WEB_AUTH_MAIN_SYSTEM_URL is required")
    return urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
