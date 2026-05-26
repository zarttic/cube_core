from __future__ import annotations

from fastapi import APIRouter, HTTPException

from cube_web.schemas import OpticalIngestRequest, payload_from_model
from cube_web.services import ingest_service


def create_ingest_router() -> APIRouter:
    router = APIRouter(prefix="/ingest", tags=["ingest"])

    @router.post("/optical/preview")
    def optical_ingest_preview(payload: OpticalIngestRequest) -> dict:
        try:
            return ingest_service.preview_optical_ingest(payload_from_model(payload))
        except HTTPException:
            raise
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @router.post("/optical/confirm")
    def optical_ingest_confirm(payload: OpticalIngestRequest) -> dict:
        try:
            return ingest_service.confirm_optical_ingest(payload_from_model(payload))
        except HTTPException:
            raise
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    return router
