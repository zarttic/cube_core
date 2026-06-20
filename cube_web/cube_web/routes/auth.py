from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse

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

    @router.get("/auth/login")
    def auth_login(target: str = "/", redirect_uri: str | None = None) -> RedirectResponse:
        state = auth_service.encode_state(_safe_target_path(target), redirect_uri=redirect_uri)
        return RedirectResponse(
            url=auth_service.get_authorize_url(state=state, redirect_uri=redirect_uri),
            status_code=307,
        )

    @router.get("/callback")
    def auth_callback(code: str, state: str | None = None) -> dict:
        state_payload = auth_service.decode_state(state)
        token_response = auth_service.exchange_code_for_token(code, redirect_uri=state_payload.get("redirect_uri"))
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

    @router.get("/auth/me")
    def auth_me_alias(authorization: str | None = Header(default=None)) -> dict:
        return auth_me(authorization)

    @router.post("/logout")
    def auth_logout(authorization: str | None = Header(default=None)) -> dict:
        token = auth_service.bearer_token(authorization) if authorization else None
        return auth_service.notify_logout(token)

    @router.post("/auth/logout")
    def auth_logout_alias(authorization: str | None = Header(default=None)) -> dict:
        return auth_logout(authorization)

    @router.get("/auth/verify")
    def auth_verify_alias(authorization: str | None = Header(default=None)) -> dict:
        return auth_verify(authorization)

    return router


def _navigation_items() -> list[dict[str, str]]:
    return runtime_config.navigation_items()


def _safe_target_path(target: str) -> str:
    value = str(target or "/").strip() or "/"
    if not value.startswith("/") or value.startswith("//"):
        return "/"
    return value


async def require_auth_for_api(request: Request, call_next):
    settings = auth_service.auth_settings()
    if settings.required and request.url.path.startswith("/v1/"):
        try:
            token = auth_service.bearer_token(request.headers.get("Authorization"))
            auth_service.verify_access_token(token, settings)
        except HTTPException as exc:
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
    return await call_next(request)
