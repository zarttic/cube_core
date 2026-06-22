from __future__ import annotations

from fastapi import HTTPException
from fastapi.responses import Response

from cube_web.schemas import payload_from_model
from cube_web.services.quality_pdf import quality_report_pdf_response, quality_report_text_response
from cube_web.services.quality_report_store import get_quality_report_store


def quality_optical_latest(payload: dict | None = None) -> dict:
    payload_from_model(payload)
    report = get_quality_report_store().latest_report("optical")
    if report is None:
        raise HTTPException(status_code=404, detail="No optical quality report found")
    return report


def quality_optical_report(payload: dict) -> dict:
    return _quality_report("optical", payload)


def quality_optical_report_pdf(payload: dict) -> Response:
    return quality_report_pdf_response(quality_optical_report(payload), data_type="optical")


def quality_optical_report_txt(payload: dict) -> Response:
    return quality_report_text_response(quality_optical_report(payload), data_type="optical")


def quality_optical_history(payload: dict | None = None) -> dict:
    return _quality_history("optical", payload)


def quality_radar_latest(payload: dict | None = None) -> dict:
    payload_from_model(payload)
    report = get_quality_report_store().latest_report("radar")
    if report is None:
        raise HTTPException(status_code=404, detail="No radar quality report found")
    return report


def quality_radar_report(payload: dict) -> dict:
    return _quality_report("radar", payload)


def quality_radar_report_pdf(payload: dict) -> Response:
    return quality_report_pdf_response(quality_radar_report(payload), data_type="radar")


def quality_radar_report_txt(payload: dict) -> Response:
    return quality_report_text_response(quality_radar_report(payload), data_type="radar")


def quality_radar_history(payload: dict | None = None) -> dict:
    return _quality_history("radar", payload)


def quality_product_history(payload: dict | None = None) -> dict:
    return _quality_history("product", payload)


def quality_carbon_latest(payload: dict | None = None) -> dict:
    payload_from_model(payload)
    report = get_quality_report_store().latest_report("carbon")
    if report is None:
        raise HTTPException(status_code=404, detail="No carbon quality report found")
    return report


def quality_carbon_report(payload: dict) -> dict:
    return _quality_report("carbon", payload)


def quality_carbon_report_pdf(payload: dict) -> Response:
    return quality_report_pdf_response(quality_carbon_report(payload), data_type="carbon")


def quality_carbon_report_txt(payload: dict) -> Response:
    return quality_report_text_response(quality_carbon_report(payload), data_type="carbon")


def quality_carbon_history(payload: dict | None = None) -> dict:
    return _quality_history("carbon", payload)


def history_limit(payload: dict) -> int:
    return history_page_size(payload)


def history_page(payload: dict) -> int:
    try:
        page = int(payload.get("page", 1) or 1)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="page must be an integer") from None
    if page <= 0:
        raise HTTPException(status_code=422, detail="page must be greater than 0")
    return page


def history_page_size(payload: dict) -> int:
    value = payload.get("page_size")
    if value is None:
        value = payload.get("limit", 20)
    try:
        page_size = int(value or 20)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="page_size must be an integer") from None
    if page_size <= 0:
        raise HTTPException(status_code=422, detail="page_size must be greater than 0")
    return page_size


def _quality_report(data_type: str, payload: dict) -> dict:
    payload = payload_from_model(payload)
    report_id = str(payload.get("report_id", "")).strip()
    if not report_id:
        raise HTTPException(status_code=422, detail="report_id is required")
    report = get_quality_report_store().get_report(data_type, report_id)
    if report is None:
        raise HTTPException(status_code=404, detail=f"{data_type.title()} quality report not found: {report_id}")
    return report


def _quality_history(data_type: str, payload: dict | None = None) -> dict:
    payload = payload_from_model(payload)
    page = history_page(payload)
    page_size = history_page_size(payload)
    keyword = _optional_text(payload.get("keyword"))
    status = _optional_text(payload.get("status"))
    store = get_quality_report_store()
    records = store.list_reports(
        data_type,
        limit=page_size,
        offset=(page - 1) * page_size,
        status=status,
        keyword=keyword,
    )
    total = store.count_reports(data_type, status=status, keyword=keyword)
    return {"records": records, "count": len(records), "total": total, "page": page, "page_size": page_size}


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None
