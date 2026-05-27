from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException


@dataclass(frozen=True)
class AuthSettings:
    main_system_url: str
    client_id: str
    client_secret: str
    redirect_uri: str
    jwt_secret_key: str
    jwt_algorithm: str
    token_path: str
    logout_path: str
    required: bool


def auth_settings() -> AuthSettings:
    return AuthSettings(
        main_system_url=os.environ.get("CUBE_WEB_AUTH_MAIN_SYSTEM_URL", "http://10.136.1.14:5177").rstrip("/"),
        client_id=os.environ.get("CUBE_WEB_AUTH_CLIENT_ID", "system_ard"),
        client_secret=os.environ.get("CUBE_WEB_AUTH_CLIENT_SECRET", "ard_secret_abc123"),
        redirect_uri=os.environ.get("CUBE_WEB_AUTH_REDIRECT_URI", "http://10.136.1.14:50040/callback"),
        jwt_secret_key=os.environ.get("CUBE_WEB_AUTH_JWT_SECRET_KEY", "your-secret-key-here-change-in-production"),
        jwt_algorithm=os.environ.get("CUBE_WEB_AUTH_JWT_ALGORITHM", "HS256"),
        token_path=os.environ.get("CUBE_WEB_AUTH_TOKEN_PATH", "/api/token"),
        logout_path=os.environ.get("CUBE_WEB_AUTH_LOGOUT_PATH", "/api/logout"),
        required=os.environ.get("CUBE_WEB_AUTH_REQUIRED", "").strip().lower() in {"1", "true", "yes", "on"},
    )


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


def exchange_code_for_token(code: str, settings: AuthSettings | None = None) -> dict[str, Any]:
    settings = settings or auth_settings()
    payload = {
        "grant_type": "authorization_code",
        "client_id": settings.client_id,
        "client_secret": settings.client_secret,
        "redirect_uri": settings.redirect_uri,
        "code": code,
    }
    return _post_json(settings.main_system_url + settings.token_path, payload)


def notify_logout(token: str | None, settings: AuthSettings | None = None) -> dict[str, Any]:
    if not token:
        return {"status": "skipped"}
    settings = settings or auth_settings()
    try:
        return _post_json(settings.main_system_url + settings.logout_path, {}, token=token)
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
