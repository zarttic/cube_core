from uuid import uuid4

from cube_web.services.quality_rules import QualityFinding
from cube_web.services.quality_worker import _errors_from_findings


def test_worker_preserves_all_finding_identity_fields_when_persisting_errors() -> None:
    run_id = uuid4()
    finding = QualityFinding(
        error_code="invalid_bbox",
        message="bounds are invalid",
        source_asset_id="asset-1",
        tile_id="tile-1",
        index_id="index-1",
        output_id="output-1",
        row_number=7,
        field="bbox",
        context={"west": 181},
    )

    (error,) = _errors_from_findings(run_id, "cell_bbox_validity", (finding,))

    assert error.quality_run_id == run_id
    assert error.rule_code == "cell_bbox_validity"
    assert error.error_code == "invalid_bbox"
    assert error.source_asset_id == "asset-1"
    assert error.tile_id == "tile-1"
    assert error.index_id == "index-1"
    assert error.output_id == "output-1"
    assert error.row_number == 7
    assert error.field == "bbox"
    assert error.context == {"west": 181}
