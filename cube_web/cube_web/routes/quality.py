from __future__ import annotations

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
from cube_web.services.quality_pdf import quality_report_pdf_response, quality_report_text_response
from cube_web.services.quality_report_store import get_quality_report_store


def create_quality_router() -> APIRouter:
    router = APIRouter(prefix="/quality", tags=["quality"])

    @router.post("/optical/run")
    def quality_optical_run(payload: QualityRunRequest) -> dict:
        payload = payload_from_model(payload)
        if not quality_checks.run_optical_quality_check:
            raise HTTPException(status_code=500, detail="cube_split quality module is not available")
        run_dir_text = str(payload.get("run_dir", "")).strip()
        if not run_dir_text:
            raise HTTPException(status_code=422, detail="run_dir is required")
        run_dir = str(quality_service.resolve_quality_run_dir(run_dir_text))
        args = quality_service.quality_args(run_dir, payload)
        try:
            report = quality_checks.run_optical_quality_check(args)
            return get_quality_report_store().upsert_report("optical", run_dir, report)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @router.post("/optical/latest")
    def quality_optical_latest(payload: QualityLatestRequest | None = None) -> dict:
        payload_from_model(payload)
        report = get_quality_report_store().latest_report("optical")
        if report is None:
            raise HTTPException(status_code=404, detail="No optical quality report found")
        return report

    @router.post("/optical/report")
    def quality_optical_report(payload: QualityReportRequest) -> dict:
        payload = payload_from_model(payload)
        report_id = str(payload.get("report_id", "")).strip()
        if not report_id:
            raise HTTPException(status_code=422, detail="report_id is required")
        report = get_quality_report_store().get_report("optical", report_id)
        if report is None:
            raise HTTPException(status_code=404, detail=f"Optical quality report not found: {report_id}")
        return report

    @router.post("/optical/report/pdf")
    def quality_optical_report_pdf(payload: QualityReportRequest) -> Response:
        report = quality_optical_report(payload)
        return quality_report_pdf_response(report, data_type="optical")

    @router.post("/optical/report/txt")
    def quality_optical_report_txt(payload: QualityReportRequest) -> Response:
        report = quality_optical_report(payload)
        return quality_report_text_response(report, data_type="optical")

    @router.post("/optical/history")
    def quality_optical_history(payload: QualityHistoryRequest | None = None) -> dict:
        payload = payload_from_model(payload)
        limit = _history_limit(payload)
        records = get_quality_report_store().list_reports("optical", limit=limit)
        return {"records": records, "count": len(records)}

    @router.post("/product/run")
    def quality_product_run(payload: QualityRunRequest) -> dict:
        payload = payload_from_model(payload)
        if not quality_checks.run_product_quality_check:
            raise HTTPException(status_code=500, detail="cube_split product quality module is not available")
        run_dir_text = str(payload.get("run_dir", "")).strip()
        if not run_dir_text:
            raise HTTPException(status_code=422, detail="run_dir is required")
        run_dir = str(quality_service.resolve_quality_run_dir(run_dir_text))
        args = quality_service.quality_args(run_dir, payload)
        try:
            report = quality_checks.run_product_quality_check(args)
            return get_quality_report_store().upsert_report("product", run_dir, report)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @router.post("/product/latest")
    def quality_product_latest(payload: QualityLatestRequest | None = None) -> dict:
        payload_from_model(payload)
        report = get_quality_report_store().latest_report("product")
        if report is None:
            raise HTTPException(status_code=404, detail="No product quality report found")
        return report

    @router.post("/product/report")
    def quality_product_report(payload: QualityReportRequest) -> dict:
        payload = payload_from_model(payload)
        report_id = str(payload.get("report_id", "")).strip()
        if not report_id:
            raise HTTPException(status_code=422, detail="report_id is required")
        report = get_quality_report_store().get_report("product", report_id)
        if report is None:
            raise HTTPException(status_code=404, detail=f"Product quality report not found: {report_id}")
        return report

    @router.post("/product/report/pdf")
    def quality_product_report_pdf(payload: QualityReportRequest) -> Response:
        report = quality_product_report(payload)
        return quality_report_pdf_response(report, data_type="product")

    @router.post("/product/report/txt")
    def quality_product_report_txt(payload: QualityReportRequest) -> Response:
        report = quality_product_report(payload)
        return quality_report_text_response(report, data_type="product")

    @router.post("/product/history")
    def quality_product_history(payload: QualityHistoryRequest | None = None) -> dict:
        payload = payload_from_model(payload)
        limit = _history_limit(payload)
        records = get_quality_report_store().list_reports("product", limit=limit)
        return {"records": records, "count": len(records)}

    @router.post("/carbon/run")
    def quality_carbon_run(payload: QualityRunRequest) -> dict:
        payload = payload_from_model(payload)
        if not quality_checks.run_carbon_quality_check:
            raise HTTPException(status_code=500, detail="cube_split carbon quality module is not available")
        run_dir_text = str(payload.get("run_dir", "")).strip()
        if not run_dir_text:
            raise HTTPException(status_code=422, detail="run_dir is required")
        run_dir = str(quality_service.resolve_quality_run_dir(run_dir_text))
        args = quality_service.quality_args(run_dir, payload)
        try:
            report = quality_checks.run_carbon_quality_check(args)
            return get_quality_report_store().upsert_report("carbon", run_dir, report)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @router.post("/carbon/latest")
    def quality_carbon_latest(payload: QualityLatestRequest | None = None) -> dict:
        payload_from_model(payload)
        report = get_quality_report_store().latest_report("carbon")
        if report is None:
            raise HTTPException(status_code=404, detail="No carbon quality report found")
        return report

    @router.post("/carbon/report")
    def quality_carbon_report(payload: QualityReportRequest) -> dict:
        payload = payload_from_model(payload)
        report_id = str(payload.get("report_id", "")).strip()
        if not report_id:
            raise HTTPException(status_code=422, detail="report_id is required")
        report = get_quality_report_store().get_report("carbon", report_id)
        if report is None:
            raise HTTPException(status_code=404, detail=f"Carbon quality report not found: {report_id}")
        return report

    @router.post("/carbon/report/pdf")
    def quality_carbon_report_pdf(payload: QualityReportRequest) -> Response:
        report = quality_carbon_report(payload)
        return quality_report_pdf_response(report, data_type="carbon")

    @router.post("/carbon/report/txt")
    def quality_carbon_report_txt(payload: QualityReportRequest) -> Response:
        report = quality_carbon_report(payload)
        return quality_report_text_response(report, data_type="carbon")

    @router.post("/carbon/history")
    def quality_carbon_history(payload: QualityHistoryRequest | None = None) -> dict:
        payload = payload_from_model(payload)
        limit = _history_limit(payload)
        records = get_quality_report_store().list_reports("carbon", limit=limit)
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
