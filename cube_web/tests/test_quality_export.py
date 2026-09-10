import csv
import io
import json
import zipfile
from datetime import UTC, datetime
from uuid import uuid4
from xml.etree import ElementTree

from cube_web.services.quality_contracts import QualityError, QualityResult, QualityRun
from cube_web.services.quality_export import (
    csv_chunks,
    csv_result_chunks,
    json_chunks,
    json_result_chunks,
    quality_export_filename,
    quality_workbook_bytes,
)


def _run() -> QualityRun:
    return QualityRun(
        quality_run_id=uuid4(), dataset_id="dataset-a", dataset_code="Data Set/A", dataset_title="Dataset A", batch_id="batch-a", batch_name="Batch A", data_type="optical", product_type=None, partition_status="completed", output_version="output-a", quality_sequence=1,
        trigger_event_id=None, trigger="manual", requested_by="alice", rule_set_version="2026.07.14-v1", rule_snapshot=(),
        status="warn", results_complete=True, error_count=1, warning_count=1, execution_error=None,
        started_at=None, completed_at=datetime(2026, 7, 15, 1, 2, 3, tzinfo=UTC), created_at=datetime(2026, 7, 15, tzinfo=UTC), is_current=True,
    )


def _error() -> QualityError:
    return QualityError(
        quality_error_id=uuid4(), quality_run_id=_run().quality_run_id, rule_code="bounds", source_asset_id=None, tile_id=None,
        index_id=None, output_id=None, row_number=1, field="bbox", error_code="outside_extent", message="=unsafe, 中文",
        context={"reason": "边界"}, created_at=datetime(2026, 7, 15, tzinfo=UTC),
    )


def _result() -> QualityResult:
    run = _run()
    return QualityResult(
        quality_run_id=run.quality_run_id,
        rule_code="index_schema",
        status="pass",
        finding_count=0,
        error_count=0,
        warning_count=0,
        metrics={"checked": 10, "note": "中文"},
        execution_error=None,
        started_at=datetime(2026, 7, 15, 1, 2, 0, tzinfo=UTC),
        completed_at=datetime(2026, 7, 15, 1, 2, 3, tzinfo=UTC),
    )


def test_filename_uses_safe_dataset_code_and_filtered_suffix() -> None:
    run = _run()
    assert quality_export_filename(run, "csv", False).endswith(f"_{run.quality_run_id}_errors.csv")
    assert "Data_Set_A_20260715T010203Z" in quality_export_filename(run, "json", True)
    assert quality_export_filename(run, "json", True).endswith("_errors_filtered.json")
    assert quality_export_filename(run, "csv", False, resource="results").endswith(f"_{run.quality_run_id}_results.csv")


def test_csv_is_formula_safe_and_json_preserves_context() -> None:
    error = _error()
    csv_payload = b"".join(csv_chunks(iter((error,)))).decode("utf-8-sig")
    parsed = next(csv.DictReader(io.StringIO(csv_payload)))
    assert parsed["message"] == "'=unsafe, 中文"
    assert json.loads(parsed["context"]) == {"reason": "边界"}

    json_payload = b"".join(json_chunks(iter((error,)))).decode("utf-8")
    assert json.loads(json_payload)[0]["context"] == {"reason": "边界"}


def test_result_exports_include_successful_rule_rows_and_metrics() -> None:
    result = _result()
    csv_payload = b"".join(csv_result_chunks(iter((result,)))).decode("utf-8-sig")
    parsed = next(csv.DictReader(io.StringIO(csv_payload)))
    assert parsed["rule_code"] == "index_schema"
    assert json.loads(parsed["metrics"]) == {"checked": 10, "note": "中文"}

    json_payload = b"".join(json_result_chunks(iter((result,)))).decode("utf-8")
    assert json.loads(json_payload)[0]["status"] == "pass"


def test_quality_workbook_puts_success_before_failure_and_keeps_context() -> None:
    run = _run()
    passed = QualityResult(
        quality_run_id=run.quality_run_id,
        rule_code="index_schema",
        status="pass",
        finding_count=0,
        error_count=0,
        warning_count=0,
        metrics={"checked": 10},
        execution_error=None,
        started_at=datetime(2026, 7, 15, 1, 2, 0, tzinfo=UTC),
        completed_at=datetime(2026, 7, 15, 1, 2, 3, tzinfo=UTC),
    )
    failed = QualityResult(
        quality_run_id=run.quality_run_id,
        rule_code="bounds",
        status="fail",
        finding_count=1,
        error_count=1,
        warning_count=0,
        metrics={},
        execution_error=None,
        started_at=datetime(2026, 7, 15, 1, 2, 0, tzinfo=UTC),
        completed_at=datetime(2026, 7, 15, 1, 2, 3, tzinfo=UTC),
    )
    error = QualityError(
        quality_error_id=uuid4(),
        quality_run_id=run.quality_run_id,
        rule_code="bounds",
        source_asset_id="asset-1",
        band_code="B1",
        band_name="Blue",
        scene_id="scene-1",
        scene_name="scene-one",
        tile_id=None,
        index_id=None,
        output_id=None,
        row_number=1,
        field="bbox",
        error_code="outside_extent",
        message="outside",
        context={"reason": "边界"},
        created_at=datetime(2026, 7, 15, tzinfo=UTC),
    )
    payload = quality_workbook_bytes(
        run,
        [passed, failed],
        [error],
        [{"scene_id": "scene-1", "scene_name": "scene-one", "source_asset_id": "asset-1", "band_unit_id": "band-unit-1", "band_code": "B1", "band_name": "Blue"}],
    )

    with zipfile.ZipFile(io.BytesIO(payload)) as workbook:
        root = ElementTree.fromstring(workbook.read("xl/workbook.xml"))
        namespace = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
        assert [sheet.attrib["name"] for sheet in root.findall("main:sheets/main:sheet", namespace)] == ["成功内容", "失败内容"]
        success_root = ElementTree.fromstring(workbook.read("xl/worksheets/sheet1.xml"))
        failed_root = ElementTree.fromstring(workbook.read("xl/worksheets/sheet2.xml"))
        success_xml = ElementTree.tostring(success_root, encoding="unicode")
        failed_xml = ElementTree.tostring(failed_root, encoding="unicode")
        assert len(success_root.findall("main:sheetData/main:row", namespace)) == 2
        assert len(failed_root.findall("main:sheetData/main:row", namespace)) == 2

    assert "通过" not in success_xml
    assert "pass" in success_xml
    assert "batch-a" in success_xml
    assert "Batch A" in success_xml
    assert "Dataset A" in success_xml
    assert "scene-one" in success_xml
    assert "band-unit-1" in success_xml
    assert "bounds" in failed_xml
    assert "bbox" in failed_xml
    assert "outside_extent" in failed_xml
    assert "边界" in failed_xml
    assert "band-unit-1" in failed_xml

    empty_success_payload = quality_workbook_bytes(run.model_copy(update={"status": "fail"}), [], [error], [])
    with zipfile.ZipFile(io.BytesIO(empty_success_payload)) as workbook:
        empty_success_root = ElementTree.fromstring(workbook.read("xl/worksheets/sheet1.xml"))
        failed_only_root = ElementTree.fromstring(workbook.read("xl/worksheets/sheet2.xml"))
    assert len(empty_success_root.findall("main:sheetData/main:row", namespace)) == 1
    assert len(failed_only_root.findall("main:sheetData/main:row", namespace)) == 2
    assert "batch-a" in failed_xml
