from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

from cube_web.services.quality_contracts import QualityResult
from cube_web.services.quality_rules import RuleContext, default_rule_registry, reduce_quality_status, snapshot_rules


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


class _AssetCursor:
    description = tuple(SimpleNamespace(name=name) for name in ("source_asset_id", "cog_uri", "source_uri", "source_format", "checksum"))

    def __init__(self, row: tuple[object, ...]) -> None:
        self.row = row

    def __enter__(self) -> "_AssetCursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, _sql: str, _params: tuple[object, ...]) -> None:
        return None

    def fetchall(self) -> list[tuple[object, ...]]:
        return [self.row]


class _AssetRepository:
    def __init__(self, row: tuple[object, ...]) -> None:
        self.row = row

    def cursor(self) -> _AssetCursor:
        return _AssetCursor(self.row)


def test_asset_readability_accepts_raw_carbon_and_keeps_cog_requirement_for_optical() -> None:
    rule = default_rule_registry().get("asset_readability")
    assert rule is not None
    checksum = "a" * 64
    raw_context = RuleContext(
        dataset_id="carbon-a", output_version="v1", data_type="carbon", product_type="xco2",
        repository=_AssetRepository(("raw-a", None, "s3://cube/cube/source/carbon/oco2.nc4", "netcdf", checksum)), object_reader=None,
    )
    assert list(rule.evaluate(raw_context)) == []
    optical_context = RuleContext(
        dataset_id="optical-a", output_version="v1", data_type="optical", product_type=None,
        repository=_AssetRepository(("cog-a", None, "s3://cube/cube/source/optical/a.tif", "cog", checksum)), object_reader=None,
    )
    assert [finding.error_code for finding in rule.evaluate(optical_context)] == ["invalid_cog_uri"]
