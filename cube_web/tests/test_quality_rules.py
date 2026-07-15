from datetime import UTC, datetime
from uuid import uuid4

from cube_web.services.quality_contracts import QualityResult
from cube_web.services.quality_rules import default_rule_registry, reduce_quality_status, snapshot_rules


def _result(status: str) -> QualityResult:
    now = datetime.now(UTC)
    return QualityResult(
        quality_run_id=uuid4(),
        rule_code="rule",
        status=status,
        finding_count=0,
        error_count=0,
        warning_count=0,
        metrics={},
        execution_error=None,
        started_at=now,
        completed_at=now,
    )


def test_status_reduction_is_normative() -> None:
    assert reduce_quality_status([], None) == "pass"
    assert reduce_quality_status([_result("warn")], None) == "warn"
    assert reduce_quality_status([_result("warn"), _result("fail")], None) == "fail"
    assert reduce_quality_status([_result("fail")], "object unavailable") == "error"


def test_snapshot_contains_every_interpretive_field() -> None:
    registry = default_rule_registry()
    optical = snapshot_rules(registry, data_type="optical", product_type="surface-reflectance")
    radar = snapshot_rules(registry, data_type="radar", product_type="sar")
    product = snapshot_rules(registry, data_type="product", product_type="annual")
    carbon = snapshot_rules(registry, data_type="carbon", product_type="xco2")
    assert {
        "index_schema",
        "asset_readability",
        "asset_crs",
        "window_bounds",
        "cell_bbox_validity",
        "time_bucket_consistency",
        "grid_method_agreement",
    } <= {item.code for item in optical}
    assert {"index_schema", "asset_readability", "asset_crs", "window_bounds", "grid_method_agreement"} <= {item.code for item in radar}
    assert "product_year_consistency" in {item.code for item in product}
    assert {
        "carbon_schema",
        "carbon_coordinates",
        "carbon_xco2_range",
        "carbon_quality_flags",
        "carbon_observation_duplicates",
        "carbon_footprints",
    } <= {item.code for item in carbon}
    assert all(item.name and item.applicability and item.implementation_version for item in (*optical, *radar, *product, *carbon))
