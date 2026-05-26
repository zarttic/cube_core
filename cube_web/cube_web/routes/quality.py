from __future__ import annotations

from pathlib import Path
from fastapi import APIRouter, HTTPException
from fastapi.responses import Response

from cube_web.schemas import (
    QualityHistoryRequest,
    QualityLatestRequest,
    QualityReportRequest,
    QualityRunRequest,
    payload_from_model,
)
from cube_web.services import quality_checks
from cube_web.services import quality_service
from cube_web.services.quality_pdf import quality_report_pdf_response


def create_quality_router() -> APIRouter:
    router = APIRouter(prefix="/quality", tags=["quality"])

    @router.post("/optical/run")
    def quality_optical_run(payload: QualityRunRequest) -> dict:
        payload = payload_from_model(payload)
        if quality_checks.run_optical_quality_check is None:
            raise HTTPException(status_code=500, detail="cube_split quality module is not available")
        run_dir_text = str(payload.get("run_dir", "")).strip()
        if not run_dir_text:
            raise HTTPException(status_code=422, detail="run_dir is required")
        run_dir = str(quality_service.resolve_quality_run_dir(run_dir_text))
        args = quality_service.quality_args(run_dir, payload)
        try:
            return quality_checks.run_optical_quality_check(args)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @router.post("/optical/latest")
    def quality_optical_latest(payload: QualityLatestRequest | None = None) -> dict:
        payload = payload_from_model(payload)
        run_dir = quality_service.latest_optical_quality_run_dir()
        cached_report = quality_service.read_quality_report(Path(run_dir), data_type="optical")
        if cached_report is not None:
            return cached_report
        if quality_checks.run_optical_quality_check is None:
            raise HTTPException(status_code=500, detail="cube_split quality module is not available")
        args = quality_service.quality_args(run_dir, payload)
        try:
            report = quality_checks.run_optical_quality_check(args)
            report["run_dir"] = run_dir
            return report
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @router.post("/optical/report")
    def quality_optical_report(payload: QualityReportRequest) -> dict:
        payload = payload_from_model(payload)
        run_dir_text = str(payload.get("run_dir", "")).strip()
        if not run_dir_text:
            raise HTTPException(status_code=422, detail="run_dir is required")
        run_dir = quality_service.resolve_quality_run_dir(run_dir_text)
        report = quality_service.read_quality_report(run_dir, data_type="optical")
        if report is None:
            raise HTTPException(status_code=404, detail=f"quality_report.json not found under run_dir: {run_dir}")
        return report

    @router.post("/optical/report/pdf")
    def quality_optical_report_pdf(payload: QualityReportRequest) -> Response:
        report = quality_optical_report(payload)
        return quality_report_pdf_response(report, data_type="optical")

    @router.post("/optical/history")
    def quality_optical_history(payload: QualityHistoryRequest | None = None) -> dict:
        payload = payload_from_model(payload)
        limit = _history_limit(payload)
        records: list[dict] = []
        for run_dir in quality_service.optical_quality_run_dirs():
            record = quality_service.read_quality_history_record(run_dir, data_type="optical")
            if record is None:
                continue
            records.append(record)
            if len(records) >= limit:
                break
        return {"records": records, "count": len(records)}

    @router.post("/product/run")
    def quality_product_run(payload: QualityRunRequest) -> dict:
        payload = payload_from_model(payload)
        if quality_checks.run_product_quality_check is None:
            raise HTTPException(status_code=500, detail="cube_split product quality module is not available")
        run_dir_text = str(payload.get("run_dir", "")).strip()
        if not run_dir_text:
            raise HTTPException(status_code=422, detail="run_dir is required")
        run_dir = str(quality_service.resolve_quality_run_dir(run_dir_text))
        args = quality_service.quality_args(run_dir, payload)
        try:
            return quality_checks.run_product_quality_check(args)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @router.post("/product/latest")
    def quality_product_latest(payload: QualityLatestRequest | None = None) -> dict:
        payload = payload_from_model(payload)
        run_dir = quality_service.latest_product_quality_run_dir()
        cached_report = quality_service.read_quality_report(Path(run_dir), data_type="product")
        if cached_report is not None:
            return cached_report
        if quality_checks.run_product_quality_check is None:
            raise HTTPException(status_code=500, detail="cube_split product quality module is not available")
        args = quality_service.quality_args(run_dir, payload)
        try:
            report = quality_checks.run_product_quality_check(args)
            report["run_dir"] = run_dir
            return report
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @router.post("/product/report")
    def quality_product_report(payload: QualityReportRequest) -> dict:
        payload = payload_from_model(payload)
        run_dir_text = str(payload.get("run_dir", "")).strip()
        if not run_dir_text:
            raise HTTPException(status_code=422, detail="run_dir is required")
        run_dir = quality_service.resolve_quality_run_dir(run_dir_text)
        report = quality_service.read_quality_report(run_dir, data_type="product")
        if report is None:
            raise HTTPException(status_code=404, detail=f"quality_report.json not found under run_dir: {run_dir}")
        return report

    @router.post("/product/report/pdf")
    def quality_product_report_pdf(payload: QualityReportRequest) -> Response:
        report = quality_product_report(payload)
        return quality_report_pdf_response(report, data_type="product")

    @router.post("/product/history")
    def quality_product_history(payload: QualityHistoryRequest | None = None) -> dict:
        payload = payload_from_model(payload)
        limit = _history_limit(payload)
        records: list[dict] = []
        for run_dir in quality_service.quality_run_dirs("product"):
            record = quality_service.read_quality_history_record(run_dir, data_type="product")
            if record is None:
                continue
            records.append(record)
            if len(records) >= limit:
                break
        return {"records": records, "count": len(records)}

    return router


def _history_limit(payload: dict) -> int:
    try:
        limit = int(payload.get("limit", 20) or 20)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="limit must be an integer") from None
    if limit <= 0:
        raise HTTPException(status_code=422, detail="limit must be greater than 0")
    return limit
