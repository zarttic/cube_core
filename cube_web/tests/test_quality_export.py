import csv
import io
import json
from datetime import UTC, datetime
from uuid import uuid4

from cube_web.services.quality_contracts import QualityError, QualityRun
from cube_web.services.quality_export import csv_chunks, json_chunks, quality_export_filename


def _run() -> QualityRun:
    return QualityRun(
        quality_run_id=uuid4(), dataset_id="dataset-a", dataset_code="Data Set/A", batch_id="batch-a", data_type="optical", product_type=None, partition_status="completed", output_version="output-a", quality_sequence=1,
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


def test_filename_uses_safe_dataset_code_and_filtered_suffix() -> None:
    run = _run()
    assert quality_export_filename(run, "csv", False).endswith(f"_{run.quality_run_id}_errors.csv")
    assert "Data_Set_A_20260715T010203Z" in quality_export_filename(run, "json", True)
    assert quality_export_filename(run, "json", True).endswith("_errors_filtered.json")


def test_csv_is_formula_safe_and_json_preserves_context() -> None:
    error = _error()
    csv_payload = b"".join(csv_chunks(iter((error,)))).decode("utf-8-sig")
    parsed = next(csv.DictReader(io.StringIO(csv_payload)))
    assert parsed["message"] == "'=unsafe, 中文"
    assert json.loads(parsed["context"]) == {"reason": "边界"}

    json_payload = b"".join(json_chunks(iter((error,)))).decode("utf-8")
    assert json.loads(json_payload)[0]["context"] == {"reason": "边界"}
