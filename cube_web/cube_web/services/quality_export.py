from __future__ import annotations

import csv
import html
import io
import json
import re
import zipfile
from collections.abc import Iterable, Iterator, Mapping
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID

from cube_web.services.quality_contracts import ExportFormat, QualityError, QualityErrorFilter, QualityResult, QualityRun
from cube_web.services.quality_repository import (
    count_quality_errors,
    count_quality_results,
    get_quality_run,
    iter_quality_errors,
    iter_quality_results,
    list_quality_scene_bands,
    require_open_gauss_domain_store,
)

XLSX_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

CSV_COLUMNS = (
    "quality_error_id",
    "quality_run_id",
    "rule_code",
    "scene_id",
    "scene_name",
    "source_asset_id",
    "band_code",
    "band_name",
    "tile_id",
    "index_id",
    "output_id",
    "row_number",
    "field",
    "error_code",
    "message",
    "context",
    "created_at",
)
RESULT_CSV_COLUMNS = (
    "quality_run_id",
    "rule_code",
    "status",
    "finding_count",
    "error_count",
    "warning_count",
    "metrics",
    "execution_error",
    "started_at",
    "completed_at",
)
QUALITY_XLSX_COLUMNS = (
    "批次ID",
    "批次名称",
    "质检运行ID",
    "数据集ID",
    "数据集编码",
    "数据集名称",
    "数据类型",
    "产品类型",
    "输出版本",
    "格网类型",
    "格网层级",
    "景ID",
    "景名称",
    "源资产ID",
    "波段单元ID",
    "波段编码",
    "波段名称",
    "质检项",
    "结果状态",
    "发现数",
    "错误数",
    "警告数",
    "指标",
    "执行错误",
    "质检错误ID",
    "瓦片ID",
    "索引ID",
    "输出ID",
    "行号",
    "字段",
    "错误编码",
    "错误信息",
    "错误上下文",
    "开始时间",
    "完成时间",
    "记录时间",
)
_SUCCESS_RESULT_STATUSES = frozenset(("pass", "warn"))


def quality_export_filename(
    run: QualityRun,
    export_format: ExportFormat,
    filtered: bool,
    resource: Literal["errors", "results", "quality"] = "errors",
) -> str:
    dataset_code = re.sub(r"[^A-Za-z0-9._-]", "_", run.dataset_code).strip("._") or "dataset"
    quality_time = (run.completed_at or run.started_at or run.created_at).astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    filtered_suffix = "_filtered" if filtered else ""
    return f"{dataset_code}_{quality_time}_{run.quality_run_id}_{resource}{filtered_suffix}.{export_format}"


def csv_chunks(rows: Iterator[QualityError]) -> Iterator[bytes]:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=CSV_COLUMNS)
    writer.writeheader()
    yield buffer.getvalue().encode("utf-8-sig")
    buffer.seek(0)
    buffer.truncate(0)
    for row in rows:
        payload = row.model_dump(mode="json")
        payload["context"] = json.dumps(payload["context"], ensure_ascii=False, separators=(",", ":"))
        for key, value in payload.items():
            if isinstance(value, str) and value.startswith(("=", "+", "-", "@")):
                payload[key] = "'" + value
        writer.writerow(payload)
        yield buffer.getvalue().encode("utf-8")
        buffer.seek(0)
        buffer.truncate(0)


def json_chunks(rows: Iterator[QualityError]) -> Iterator[bytes]:
    yield b"["
    first = True
    for row in rows:
        if not first:
            yield b","
        first = False
        yield json.dumps(row.model_dump(mode="json"), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    yield b"]"


def csv_result_chunks(rows: Iterator[QualityResult]) -> Iterator[bytes]:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=RESULT_CSV_COLUMNS)
    writer.writeheader()
    yield buffer.getvalue().encode("utf-8-sig")
    buffer.seek(0)
    buffer.truncate(0)
    for row in rows:
        payload = row.model_dump(mode="json")
        payload["metrics"] = json.dumps(payload["metrics"], ensure_ascii=False, separators=(",", ":"))
        for key, value in payload.items():
            if isinstance(value, str) and value.startswith(("=", "+", "-", "@")):
                payload[key] = "'" + value
        writer.writerow(payload)
        yield buffer.getvalue().encode("utf-8")
        buffer.seek(0)
        buffer.truncate(0)


def json_result_chunks(rows: Iterator[QualityResult]) -> Iterator[bytes]:
    yield b"["
    first = True
    for row in rows:
        if not first:
            yield b","
        first = False
        yield json.dumps(row.model_dump(mode="json"), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    yield b"]"


def _xlsx_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        text = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    elif isinstance(value, datetime):
        text = value.isoformat()
    else:
        text = str(value)
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
    if text.startswith(("=", "+", "-", "@")):
        return "'" + text
    return text


def _xlsx_column_name(number: int) -> str:
    name = ""
    while number:
        number, remainder = divmod(number - 1, 26)
        name = chr(65 + remainder) + name
    return name


def _xlsx_cell(column: int, row_number: int, value: Any, *, style: int = 0) -> str:
    reference = f"{_xlsx_column_name(column)}{row_number}"
    text = html.escape(_xlsx_value(value), quote=True)
    return f'<c r="{reference}" s="{style}" t="inlineStr"><is><t xml:space="preserve">{text}</t></is></c>'


def _xlsx_sheet(columns: tuple[str, ...], rows: list[Mapping[str, Any]]) -> str:
    last_row = max(1, len(rows) + 1)
    dimension = f"A1:{_xlsx_column_name(len(columns))}{last_row}"
    xml_rows = [
        '<row r="1">'
        + "".join(_xlsx_cell(column_number, 1, column, style=1) for column_number, column in enumerate(columns, 1))
        + "</row>"
    ]
    for row_number, row in enumerate(rows, 2):
        xml_rows.append(
            f'<row r="{row_number}">'
            + "".join(_xlsx_cell(column_number, row_number, row.get(column)) for column_number, column in enumerate(columns, 1))
            + "</row>"
        )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f'<sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/></sheetView></sheetViews>'
        f'<dimension ref="{dimension}"/>'
        f'<cols><col min="1" max="{len(columns)}" width="18" customWidth="1"/></cols>'
        f'<sheetData>{"".join(xml_rows)}</sheetData>'
        '</worksheet>'
    )


def _xlsx_styles() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<numFmts count="0"/>'
        '<fonts count="2"><font><sz val="11"/><name val="Calibri"/></font><font><b/><sz val="11"/><name val="Calibri"/></font></fonts>'
        '<fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="solid"><fgColor rgb="FFD9EAF7"/><bgColor indexed="64"/></patternFill></fill></fills>'
        '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        '<cellXfs count="2"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/><xf numFmtId="0" fontId="1" fillId="1" borderId="0" applyFont="1" applyFill="1"/></cellXfs>'
        '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
        '</styleSheet>'
    )


def _xlsx_workbook(success_rows: list[Mapping[str, Any]], failed_rows: list[Mapping[str, Any]]) -> bytes:
    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '<Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        '</Types>'
    )
    package_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
        '</Relationships>'
    )
    workbook = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<fileVersion appName="xl"/><bookViews><workbookView/></bookViews>'
        '<sheets><sheet name="成功内容" sheetId="1" r:id="rId1"/><sheet name="失败内容" sheetId="2" r:id="rId2"/></sheets>'
        '</workbook>'
    )
    workbook_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
        '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/>'
        '<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
        '</Relationships>'
    )
    output = io.BytesIO()
    with zipfile.ZipFile(output, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("_rels/.rels", package_rels)
        archive.writestr("xl/workbook.xml", workbook)
        archive.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
        archive.writestr("xl/styles.xml", _xlsx_styles())
        archive.writestr("xl/worksheets/sheet1.xml", _xlsx_sheet(QUALITY_XLSX_COLUMNS, success_rows))
        archive.writestr("xl/worksheets/sheet2.xml", _xlsx_sheet(QUALITY_XLSX_COLUMNS, failed_rows))
    return output.getvalue()


def _scene_context(scene_band: Mapping[str, Any] | None) -> dict[str, Any]:
    if scene_band is None:
        return {}
    return {
        "scene_id": scene_band.get("scene_id"),
        "scene_name": scene_band.get("scene_name"),
        "source_asset_id": scene_band.get("source_asset_id"),
        "band_unit_id": scene_band.get("band_unit_id"),
        "band_code": scene_band.get("band_code"),
        "band_name": scene_band.get("band_name"),
    }


def _error_scene_context(error: QualityError, scene_band: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return {
        "scene_id": error.scene_id or (None if scene_band is None else scene_band.get("scene_id")),
        "scene_name": error.scene_name or (None if scene_band is None else scene_band.get("scene_name")),
        "source_asset_id": error.source_asset_id or (None if scene_band is None else scene_band.get("source_asset_id")),
        "band_unit_id": None if scene_band is None else scene_band.get("band_unit_id"),
        "band_code": error.band_code or (None if scene_band is None else scene_band.get("band_code")),
        "band_name": error.band_name or (None if scene_band is None else scene_band.get("band_name")),
    }


def _quality_export_row(
    run: QualityRun,
    *,
    result: QualityResult | None = None,
    error: QualityError | None = None,
    scene_band: Mapping[str, Any] | None = None,
    status: str | None = None,
) -> dict[str, Any]:
    context = _error_scene_context(error, scene_band) if error is not None else _scene_context(scene_band)
    return {
        "批次ID": run.batch_id,
        "批次名称": run.batch_name or run.batch_id,
        "质检运行ID": run.quality_run_id,
        "数据集ID": run.dataset_id,
        "数据集编码": run.dataset_code,
        "数据集名称": run.dataset_title or run.dataset_code,
        "数据类型": run.data_type,
        "产品类型": run.product_type,
        "输出版本": run.output_version,
        "格网类型": run.grid_type,
        "格网层级": run.grid_level,
        "景ID": context.get("scene_id"),
        "景名称": context.get("scene_name"),
        "源资产ID": context.get("source_asset_id"),
        "波段单元ID": context.get("band_unit_id"),
        "波段编码": context.get("band_code"),
        "波段名称": context.get("band_name"),
        "质检项": result.rule_code if result is not None else (error.rule_code if error is not None else "批次汇总"),
        "结果状态": result.status if result is not None else (status or "fail"),
        "发现数": result.finding_count if result is not None else None,
        "错误数": result.error_count if result is not None else None,
        "警告数": result.warning_count if result is not None else None,
        "指标": result.metrics if result is not None else run.metrics,
        "执行错误": result.execution_error if result is not None else run.execution_error,
        "质检错误ID": error.quality_error_id if error is not None else None,
        "瓦片ID": error.tile_id if error is not None else None,
        "索引ID": error.index_id if error is not None else None,
        "输出ID": error.output_id if error is not None else None,
        "行号": error.row_number if error is not None else None,
        "字段": error.field if error is not None else None,
        "错误编码": error.error_code if error is not None else None,
        "错误信息": error.message if error is not None else None,
        "错误上下文": error.context if error is not None else None,
        "开始时间": result.started_at if result is not None else run.started_at,
        "完成时间": result.completed_at if result is not None else run.completed_at,
        "记录时间": error.created_at if error is not None else run.created_at,
    }


def _quality_export_rows(
    run: QualityRun,
    results: Iterable[QualityResult],
    errors: Iterable[QualityError],
    scene_bands: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    result_items = list(results)
    error_items = list(errors)
    scene_band_items = list(scene_bands)
    contexts: list[Mapping[str, Any] | None] = scene_band_items or [None]
    errors_by_rule: dict[str, list[QualityError]] = {}
    scene_bands_by_key = {
        (item.get("scene_id"), item.get("source_asset_id"), item.get("band_code")): item
        for item in scene_band_items
    }

    def scene_band_for_error(error: QualityError) -> Mapping[str, Any] | None:
        return scene_bands_by_key.get((error.scene_id, error.source_asset_id, error.band_code))

    for error in error_items:
        errors_by_rule.setdefault(error.rule_code, []).append(error)

    success_rows: list[dict[str, Any]] = []
    failed_rows: list[dict[str, Any]] = []
    matched_error_ids: set[UUID] = set()
    for result in result_items:
        if result.status in _SUCCESS_RESULT_STATUSES:
            success_rows.extend(_quality_export_row(run, result=result, scene_band=context) for context in contexts)
            continue
        matching_errors = errors_by_rule.get(result.rule_code, [])
        if matching_errors:
            for error in matching_errors:
                failed_rows.append(_quality_export_row(run, result=result, error=error, scene_band=scene_band_for_error(error)))
                matched_error_ids.add(error.quality_error_id)
        else:
            failed_rows.extend(_quality_export_row(run, result=result, scene_band=context) for context in contexts)

    for error in error_items:
        if error.quality_error_id not in matched_error_ids:
            failed_rows.append(_quality_export_row(run, error=error, scene_band=scene_band_for_error(error)))

    if not result_items:
        target = success_rows if run.status in _SUCCESS_RESULT_STATUSES else failed_rows
        if not error_items or run.execution_error:
            target.extend(_quality_export_row(run, status=run.status, scene_band=context) for context in contexts)
    elif run.status not in _SUCCESS_RESULT_STATUSES and not failed_rows:
        failed_rows.append(_quality_export_row(run, status=run.status))
    return success_rows, failed_rows


def quality_workbook_bytes(
    run: QualityRun,
    results: Iterable[QualityResult],
    errors: Iterable[QualityError],
    scene_bands: Iterable[Mapping[str, Any]],
) -> bytes:
    success_rows, failed_rows = _quality_export_rows(run, results, errors, scene_bands)
    return _xlsx_workbook(success_rows, failed_rows)


def stream_quality_errors(
    quality_run_id: UUID,
    filters: QualityErrorFilter,
    export_format: ExportFormat,
) -> tuple[Iterator[bytes], int, str, Literal["text/csv; charset=utf-8", "application/json"]]:
    store = require_open_gauss_domain_store()
    with store.transaction() as tx:
        run = get_quality_run(tx, quality_run_id=quality_run_id)
        total = count_quality_errors(tx, quality_run_id=quality_run_id, filters=filters)
    encoder = csv_chunks if export_format == "csv" else json_chunks
    media_type: Literal["text/csv; charset=utf-8", "application/json"] = "text/csv; charset=utf-8" if export_format == "csv" else "application/json"

    def stream() -> Iterator[bytes]:
        with store.transaction() as tx:
            yield from encoder(iter_quality_errors(tx, quality_run_id=quality_run_id, filters=filters))

    return stream(), total, quality_export_filename(run, export_format, filters.active()), media_type


def stream_quality_results(
    quality_run_id: UUID,
    export_format: ExportFormat,
) -> tuple[Iterator[bytes], int, str, Literal["text/csv; charset=utf-8", "application/json"]]:
    store = require_open_gauss_domain_store()
    with store.transaction() as tx:
        run = get_quality_run(tx, quality_run_id=quality_run_id)
        total = count_quality_results(tx, quality_run_id=quality_run_id)
    encoder = csv_result_chunks if export_format == "csv" else json_result_chunks
    media_type: Literal["text/csv; charset=utf-8", "application/json"] = "text/csv; charset=utf-8" if export_format == "csv" else "application/json"

    def stream() -> Iterator[bytes]:
        with store.transaction() as tx:
            yield from encoder(iter_quality_results(tx, quality_run_id=quality_run_id))

    return stream(), total, quality_export_filename(run, export_format, False, resource="results"), media_type


def stream_quality_workbook(
    quality_run_id: UUID,
) -> tuple[Iterator[bytes], int, str, Literal["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"]]:
    store = require_open_gauss_domain_store()
    with store.transaction() as tx:
        run = get_quality_run(tx, quality_run_id=quality_run_id)
        results = tuple(iter_quality_results(tx, quality_run_id=quality_run_id))
        errors = tuple(iter_quality_errors(tx, quality_run_id=quality_run_id, filters=QualityErrorFilter()))
        scene_bands = tuple(list_quality_scene_bands(tx, dataset_id=run.dataset_id, output_version=run.output_version))
    success_rows, failed_rows = _quality_export_rows(run, results, errors, scene_bands)
    workbook = _xlsx_workbook(success_rows, failed_rows)
    return (
        iter((workbook,)),
        len(success_rows) + len(failed_rows),
        quality_export_filename(run, "xlsx", False, resource="quality"),
        XLSX_MEDIA_TYPE,
    )
