from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from cube_web.routes.auth import current_actor, require_admin
from cube_web.schemas import ManualQualityRunRequest, QualityRuleEnabledUpdate, QualityRuleSettingsUpdate
from cube_web.services.config_store import (
    get_enabled_optional_quality_rules,
    set_enabled_optional_quality_rules,
    set_optional_quality_rule_enabled,
)
from cube_web.services.quality_contracts import Page, QualityErrorFilter, page_offset, validate_sort
from cube_web.services.quality_export import stream_quality_errors
from cube_web.services.quality_repository import (
    QualityRunNotFound,
    count_quality_errors,
    count_quality_results,
    count_quality_runs,
    get_quality_run,
    list_quality_errors,
    list_quality_results,
    list_quality_runs,
    require_open_gauss_domain_store,
)
from cube_web.services.quality_rules import (
    DEFAULT_RULE_SET_VERSION,
    RULE_DESCRIPTIONS,
    default_rule_registry,
    is_rule_enabled,
)
from cube_web.services.quality_run_service import request_manual_quality_run


def _enabled_optional_rules() -> tuple[str, ...]:
    return get_enabled_optional_quality_rules()


def _rule_catalog_payload() -> dict:
    enabled = set(_enabled_optional_rules())
    registry = default_rule_registry()
    return {
        "rule_set_version": DEFAULT_RULE_SET_VERSION,
        "enabled_optional_rules": list(enabled),
        "items": [
            {
                "code": rule.code,
                "name": rule.name,
                "description": RULE_DESCRIPTIONS.get(rule.code, ""),
                "mandatory": rule.mandatory,
                "toggleable": not rule.mandatory,
                "enabled": is_rule_enabled(rule, enabled_optional_rules=enabled),
                "applicability": dict(rule.applicability),
                "parameters": dict(rule.parameters),
                "implementation_version": rule.implementation_version,
            }
            for rule in registry.all()
        ],
    }


def create_quality_router() -> APIRouter:
    router = APIRouter(prefix="/quality", tags=["quality"])

    @router.get("/rules")
    def list_rules() -> dict:
        return _rule_catalog_payload()

    @router.put("/rules/settings")
    def update_rule_settings(payload: QualityRuleSettingsUpdate, request: Request) -> dict:
        require_admin(current_actor(request))
        try:
            enabled = set_enabled_optional_quality_rules(payload.enabled_optional_rules)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail={"code": "invalid_rule_settings", "message": str(exc)}) from exc
        catalog = _rule_catalog_payload()
        catalog["enabled_optional_rules"] = list(enabled)
        # Recompute enabled flags from the just-saved list in case catalog read races.
        enabled_set = set(enabled)
        catalog["items"] = [
            {
                **item,
                "enabled": bool(item["mandatory"] or item["code"] in enabled_set),
            }
            for item in catalog["items"]
        ]
        return catalog

    @router.put("/rules/{rule_code}/enabled")
    def update_rule_enabled(rule_code: str, payload: QualityRuleEnabledUpdate, request: Request) -> dict:
        require_admin(current_actor(request))
        try:
            enabled = set_optional_quality_rule_enabled(rule_code, payload.enabled)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail={"code": "invalid_rule_settings", "message": str(exc)}) from exc
        catalog = _rule_catalog_payload()
        enabled_set = set(enabled)
        catalog["enabled_optional_rules"] = list(enabled)
        catalog["items"] = [
            {**item, "enabled": bool(item["mandatory"] or item["code"] in enabled_set)}
            for item in catalog["items"]
        ]
        return catalog

    @router.get("/records")
    def list_records(
        keyword: str | None = None,
        dataset_id: str | None = None,
        output_version: str | None = None,
        data_type: str | None = None,
        status: str | None = None,
        trigger: str | None = None,
        requested_by: str | None = None,
        current_only: bool = False,
        started_from: datetime | None = None,
        started_to: datetime | None = None,
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=20, ge=1, le=500),
        sort_by: str = "generated_at",
        sort_order: str = "desc",
    ) -> dict:
        try:
            sort_by, sort_order = validate_sort(sort_by, sort_order, {"created_at", "completed_at", "generated_at", "quality_sequence", "status"})
            with require_open_gauss_domain_store().transaction() as tx:
                items = list_quality_runs(
                    tx, keyword=keyword, dataset_id=dataset_id, output_version=output_version, data_type=data_type, status=status,
                    trigger=trigger, requested_by=requested_by, current_only=current_only, started_from=started_from, started_to=started_to,
                    limit=page_size, offset=page_offset(page, page_size), sort_by=sort_by, sort_order=sort_order,
                )
                total = count_quality_runs(
                    tx, keyword=keyword, dataset_id=dataset_id, output_version=output_version, data_type=data_type, status=status,
                    trigger=trigger, requested_by=requested_by, current_only=current_only, started_from=started_from, started_to=started_to,
                )
            return Page(items=tuple(items), total=total, page=page, page_size=page_size).model_dump(mode="json")
        except ValueError as exc:
            raise _invalid_sort(exc) from exc

    @router.get("/records/{quality_run_id}")
    def get_record(quality_run_id: UUID) -> dict:
        try:
            with require_open_gauss_domain_store().transaction() as tx:
                return get_quality_run(tx, quality_run_id=quality_run_id).model_dump(mode="json")
        except QualityRunNotFound as exc:
            raise _not_found(exc) from exc

    @router.get("/records/{quality_run_id}/results")
    def list_results(
        quality_run_id: UUID,
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=20, ge=1, le=500),
        sort_by: str = "rule_code",
        sort_order: str = "asc",
    ) -> dict:
        try:
            sort_by, sort_order = validate_sort(sort_by, sort_order, {"rule_code", "completed_at", "status"})
            with require_open_gauss_domain_store().transaction() as tx:
                get_quality_run(tx, quality_run_id=quality_run_id)
                items = list_quality_results(tx, quality_run_id=quality_run_id, limit=page_size, offset=page_offset(page, page_size), sort_by=sort_by, sort_order=sort_order)
                total = count_quality_results(tx, quality_run_id=quality_run_id)
            return Page(items=tuple(items), total=total, page=page, page_size=page_size).model_dump(mode="json")
        except QualityRunNotFound as exc:
            raise _not_found(exc) from exc
        except ValueError as exc:
            raise _invalid_sort(exc) from exc

    @router.get("/records/{quality_run_id}/errors")
    def list_errors(
        quality_run_id: UUID,
        rule_code: str | None = None,
        error_code: str | None = None,
        source_asset_id: str | None = None,
        output_id: str | None = None,
        field: str | None = None,
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=20, ge=1, le=500),
        sort_by: str = "created_at",
        sort_order: str = "asc",
    ) -> dict:
        filters = QualityErrorFilter(rule_code=rule_code, error_code=error_code, source_asset_id=source_asset_id, output_id=output_id, field=field)
        try:
            sort_by, sort_order = validate_sort(sort_by, sort_order, {"created_at", "quality_error_id", "rule_code", "error_code"})
            with require_open_gauss_domain_store().transaction() as tx:
                get_quality_run(tx, quality_run_id=quality_run_id)
                items = list_quality_errors(tx, quality_run_id=quality_run_id, filters=filters, limit=page_size, offset=page_offset(page, page_size), sort_by=sort_by, sort_order=sort_order)
                total = count_quality_errors(tx, quality_run_id=quality_run_id, filters=filters)
            return Page(items=tuple(items), total=total, page=page, page_size=page_size).model_dump(mode="json")
        except QualityRunNotFound as exc:
            raise _not_found(exc) from exc
        except ValueError as exc:
            raise _invalid_sort(exc) from exc

    @router.get("/records/{quality_run_id}/errors/export")
    def export_errors(
        quality_run_id: UUID,
        format: Literal["csv", "json"],
        rule_code: str | None = None,
        error_code: str | None = None,
        source_asset_id: str | None = None,
        output_id: str | None = None,
        field: str | None = None,
    ) -> StreamingResponse:
        filters = QualityErrorFilter(rule_code=rule_code, error_code=error_code, source_asset_id=source_asset_id, output_id=output_id, field=field)
        try:
            stream, total, filename, media_type = stream_quality_errors(quality_run_id, filters, format)
        except QualityRunNotFound as exc:
            raise _not_found(exc) from exc
        return StreamingResponse(
            stream,
            media_type=media_type,
            headers={"Content-Disposition": f'attachment; filename="{filename}"', "X-Export-Count": str(total)},
        )

    @router.post("/runs", status_code=202)
    def create_manual_run(payload: ManualQualityRunRequest, request: Request) -> dict:
        return request_manual_quality_run(payload.dataset_id, payload.output_version, current_actor(request)).model_dump(mode="json")

    return router


def _not_found(exc: Exception) -> HTTPException:
    return HTTPException(status_code=404, detail={"code": "quality_run_not_found", "message": str(exc)})


def _invalid_sort(exc: Exception) -> HTTPException:
    return HTTPException(status_code=422, detail={"code": "invalid_sort", "message": str(exc)})
