from __future__ import annotations

from fastapi import APIRouter, HTTPException

from cube_web.schemas import ConfigGetRequest, ConfigResetRequest, ConfigResponse, ConfigUpdateRequest, payload_from_model
from cube_web.services import config_store


def create_config_router() -> APIRouter:
    router = APIRouter(prefix="/config", tags=["config"])

    @router.post("/get", response_model=ConfigResponse)
    def get_config(payload: ConfigGetRequest | None = None) -> dict:
        payload_from_model(payload)
        try:
            return _response(config_store.get_config_store().get_config_record())
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @router.post("/update", response_model=ConfigResponse)
    def update_config(payload: ConfigUpdateRequest) -> dict:
        try:
            request = payload_from_model(payload)
            record = config_store.get_config_store().update_config(request.get("config") or {})
            return _response(record)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @router.post("/reset", response_model=ConfigResponse)
    def reset_config(payload: ConfigResetRequest | None = None) -> dict:
        payload_from_model(payload)
        try:
            return _response(config_store.get_config_store().reset_config())
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    return router


def _response(record: dict) -> dict:
    return {
        "config": record["config"],
        "defaults": config_store.default_config(),
        "runtime": config_store.runtime_info(),
        "updated_at": record.get("updated_at"),
    }
