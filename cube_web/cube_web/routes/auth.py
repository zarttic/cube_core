from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from cube_web.services import auth_service, runtime_config


def create_auth_router() -> APIRouter:
    router = APIRouter(prefix="/api", tags=["auth"])

    @router.get("/config")
    def auth_config() -> dict[str, Any]:
        settings = auth_service.auth_settings()
        portal = runtime_config.portal_settings()
        return {
            "client_id": settings.client_id,
            "redirect_uri": settings.redirect_uri,
            "main_system_url": portal.main_system_url or settings.main_system_url,
            "auth_required": settings.required,
            "navigation": _navigation_items(),
        }

    @router.get("/callback")
    def auth_callback(code: str, state: str | None = None) -> dict:
        token_response = auth_service.exchange_code_for_token(code)
        token = token_response.get("access_token") or token_response.get("token")
        if not token:
            raise HTTPException(status_code=502, detail="Auth service did not return access_token")
        return {
            "access_token": token,
            "token_type": token_response.get("token_type", "bearer"),
            "expires_in": token_response.get("expires_in"),
            "state": state,
        }

    @router.get("/verify")
    def auth_verify(authorization: str | None = Header(default=None)) -> dict:
        token = auth_service.bearer_token(authorization)
        payload = auth_service.verify_access_token(token)
        return {"valid": True, "sub": payload.get("sub")}

    @router.get("/me")
    def auth_me(authorization: str | None = Header(default=None)) -> dict:
        token = auth_service.bearer_token(authorization)
        return auth_service.user_info_from_token(token)

    @router.post("/logout")
    def auth_logout(authorization: str | None = Header(default=None)) -> dict:
        token = auth_service.bearer_token(authorization) if authorization else None
        return auth_service.notify_logout(token)

    return router


def _navigation_items() -> list[dict[str, str]]:
    runtime_items = runtime_config.navigation_items()
    if runtime_items:
        return runtime_items
    try:
        from cube_web.services import config_store

        config = config_store.get_app_config()
    except Exception:
        return []
    configured = ((config.get("runtime") or {}).get("portal") or {}).get("navigation")
    if not isinstance(configured, list):
        return []
    items: list[dict[str, str]] = []
    for raw_item in configured:
        if not isinstance(raw_item, dict):
            continue
        label = str(raw_item.get("label") or "").strip()
        kind = str(raw_item.get("kind") or "external").strip() or "external"
        url = str(raw_item.get("url") or "").strip()
        path = str(raw_item.get("path") or "").strip()
        if kind == "internal" and label and path.startswith("/") and not path.startswith("//"):
            items.append({"label": label, "kind": "internal", "path": path})
        elif label and url:
            items.append({"label": label, "kind": "external", "url": url})
    return items


async def require_auth_for_api(request: Request, call_next):
    settings = auth_service.auth_settings()
    if settings.required and request.url.path.startswith("/v1/"):
        try:
            token = auth_service.bearer_token(request.headers.get("Authorization"))
            auth_service.verify_access_token(token, settings)
        except HTTPException as exc:
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
    return await call_next(request)
