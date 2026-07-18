from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4
from zoneinfo import ZoneInfo

from cube_web.services.quality_contracts import QualityResult
from cube_web.services.quality_object_reader import AssetInspection
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


def test_time_bucket_consistency_uses_utc_date() -> None:
    rule = default_rule_registry().get("time_bucket_consistency")
    context = RuleContext(
        "carbon-a", "output-a", "carbon", None,
        _RowsRepository(
            ("output_id", "acquisition_time", "time_bucket"),
            [("index-a", datetime(2021, 1, 1, 6, 56, tzinfo=ZoneInfo("Asia/Shanghai")), "20201231-000142429")],
        ),
        None,
    )
    assert list(rule.evaluate(context)) == []


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
    assert "product_year_consistency" not in {item.code for item in product}
    assert "optical_band_contract" in {item.code for item in optical}
    assert "radar_band_contract" in {item.code for item in radar}
    assert "product_band_contract" in {item.code for item in product}
    assert "pixel_sample" not in {item.code for item in carbon}
    assert "asset_crs" not in {item.code for item in carbon}
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


class _RowsCursor:
    def __init__(self, columns: tuple[str, ...], rows: list[tuple[object, ...]]) -> None:
        self.description = tuple(SimpleNamespace(name=name) for name in columns)
        self.rows = rows

    def __enter__(self) -> "_RowsCursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, _sql: str, _params: tuple[object, ...]) -> None:
        return None

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows


class _RowsRepository:
    def __init__(self, columns: tuple[str, ...], rows: list[tuple[object, ...]]) -> None:
        self.columns = columns
        self.rows = rows

    def cursor(self) -> _RowsCursor:
        return _RowsCursor(self.columns, self.rows)


class _ObjectReader:
    def __init__(self, inspection: AssetInspection | None = None, error: Exception | None = None) -> None:
        self.inspection = inspection or AssetInspection()
        self.error = error
        self.calls: list[tuple[str, str, bool, int, str | None]] = []

    def inspect(
        self,
        uri: str,
        source_format: str,
        *,
        sample_pixels: bool = False,
        sample_band_index: int = 1,
        expected_checksum: str | None = None,
    ) -> AssetInspection:
        self.calls.append((uri, source_format, sample_pixels, sample_band_index, expected_checksum))
        if self.error is not None:
            raise self.error
        return self.inspection


def test_asset_readability_accepts_raw_carbon_and_resolves_cog_source_uri() -> None:
    rule = default_rule_registry().get("asset_readability")
    assert rule is not None
    checksum = "a" * 64
    raw_context = RuleContext(
        dataset_id="carbon-a", output_version="v1", data_type="carbon", product_type="xco2",
        repository=_AssetRepository(("raw-a", None, "s3://cube/cube/source/carbon/oco2.nc4", "netcdf", checksum)),
        object_reader=_ObjectReader(),
    )
    assert list(rule.evaluate(raw_context)) == []
    optical_context = RuleContext(
        dataset_id="optical-a", output_version="v1", data_type="optical", product_type=None,
        repository=_AssetRepository(("cog-a", None, "s3://cube/cube/source/optical/a.tif", "cog", checksum)), object_reader=None,
    )
    assert [finding.error_code for finding in rule.evaluate(optical_context)] == ["object_reader_unavailable"]


def test_asset_readability_reports_real_open_failure_without_exposing_details() -> None:
    rule = default_rule_registry().get("asset_readability")
    assert rule is not None
    reader = _ObjectReader(error=OSError("secret-bearing storage failure"))
    context = RuleContext(
        dataset_id="optical-a",
        output_version="v1",
        data_type="optical",
        product_type=None,
        repository=_AssetRepository(
            (
                "cog-a",
                "s3://cube/cube/source/optical/a.tif",
                "s3://cube/cube/source/optical/a.tif",
                "cog",
                "a" * 64,
            )
        ),
        object_reader=reader,
    )

    findings = list(rule.evaluate(context))

    assert [finding.error_code for finding in findings] == ["source_object_unreadable"]
    assert findings[0].context == {"reason": "OSError"}
    assert "secret" not in findings[0].message


def test_asset_rules_are_scoped_to_the_current_output_version() -> None:
    executions = []

    class Cursor(_AssetCursor):
        def execute(self, sql, params):
            executions.append((sql, params))

    class Repository:
        def cursor(self):
            return Cursor(("asset-a", "s3://cube/a.tif", "s3://cube/a.tif", "cog", "a" * 64))

    rule = default_rule_registry().get("asset_readability")
    assert rule is not None
    context = RuleContext(
        dataset_id="optical-a",
        output_version="output-current",
        data_type="optical",
        product_type=None,
        repository=Repository(),
        object_reader=_ObjectReader(),
    )

    assert list(rule.evaluate(context)) == []
    assert executions[0][1] == ("optical-a", "output-current")
    assert "partition_indexes" in executions[0][0]
    assert "i.output_version = %s" in executions[0][0]


def test_declared_metadata_findings_split_errors_and_warnings() -> None:
    repository = _RowsRepository(
        ("source_asset_id", "attributes"),
        [
            (
                "asset-a",
                {
                    "quality_metadata_defects": [
                        {"error_code": "broken", "message": "broken metadata"},
                        {"error_code": "review", "message": "needs review", "severity": "warning"},
                    ]
                },
            )
        ],
    )
    context = RuleContext("dataset-a", "output-a", "optical", None, repository, None)
    registry = default_rule_registry()

    assert [item.error_code for item in registry.get("declared_metadata_defects").evaluate(context)] == ["broken"]
    assert [item.error_code for item in registry.get("declared_metadata_warnings").evaluate(context)] == ["review"]


def test_pixel_sample_is_deterministic_and_warns_for_all_zero_sample() -> None:
    rule = default_rule_registry().get("pixel_sample")
    assert rule is not None
    reader = _ObjectReader(AssetInspection(sample_pixels=64, valid_pixels=64, nonzero_pixels=0))
    context = RuleContext(
        dataset_id="optical-a",
        output_version="v1",
        data_type="optical",
        product_type=None,
        repository=_RowsRepository(
            (
                "source_asset_id", "cog_uri", "source_format", "checksum",
                "band_code", "band_type", "display_order", "attributes",
            ),
            [("cog-a", "s3://cube/a.tif", "cog", "a" * 64, "red", "spectral", 0, {"source_band_index": 1})],
        ),
        object_reader=reader,
    )

    findings = list(rule.evaluate(context))

    assert [finding.error_code for finding in findings] == ["zero_sample_pixels"]
    assert reader.calls == [("s3://cube/a.tif", "cog", True, 1, "a" * 64)]


def test_asset_crs_rejects_non_parseable_coordinate_system() -> None:
    rule = default_rule_registry().get("asset_crs")
    assert rule is not None
    context = RuleContext(
        dataset_id="optical-a",
        output_version="v1",
        data_type="optical",
        product_type=None,
        repository=_RowsRepository(
            ("source_asset_id", "cog_uri", "source_format", "crs", "checksum"),
            [("asset-a", "s3://cube/a.tif", "cog", "EPSG:not-a-code", "a" * 64)],
        ),
        object_reader=None,
    )

    assert [finding.error_code for finding in rule.evaluate(context)] == ["invalid_crs"]


def test_asset_crs_compares_declared_value_with_raster_metadata() -> None:
    rule = default_rule_registry().get("asset_crs")
    assert rule is not None
    context = RuleContext(
        dataset_id="optical-a",
        output_version="v1",
        data_type="optical",
        product_type=None,
        repository=_RowsRepository(
            ("source_asset_id", "cog_uri", "source_format", "crs", "checksum"),
            [("asset-a", "s3://cube/a.tif", "cog", "EPSG:3857", "a" * 64)],
        ),
        object_reader=_ObjectReader(AssetInspection(crs="EPSG:4326")),
    )

    assert [finding.error_code for finding in rule.evaluate(context)] == ["crs_metadata_mismatch"]


def test_product_specific_band_contract_requires_normalized_band_type() -> None:
    registry = default_rule_registry()
    context = RuleContext(
        dataset_id="radar-a",
        output_version="v1",
        data_type="radar",
        product_type="sar",
        repository=_RowsRepository(
            ("source_asset_id", "band_code", "band_type"),
            [("asset-a", "VV", "spectral"), ("asset-b", None, None)],
        ),
        object_reader=None,
    )

    rule = registry.get("radar_band_contract")
    assert rule is not None
    assert [finding.error_code for finding in rule.evaluate(context)] == ["invalid_band_type", "missing_band_metadata"]


def test_optical_band_contract_accepts_known_quality_variables_only() -> None:
    rule = default_rule_registry().get("optical_band_contract")
    assert rule is not None
    context = RuleContext(
        dataset_id="optical-a",
        output_version="v1",
        data_type="optical",
        product_type="surface-reflectance",
        repository=_RowsRepository(
            ("source_asset_id", "band_code", "band_type"),
            [
                ("asset-red", "sr_band3", "spectral"),
                ("asset-qa", "radsat_qa", "variable"),
                ("asset-invalid", "temperature", "variable"),
            ],
        ),
        object_reader=None,
    )

    findings = list(rule.evaluate(context))

    assert [(finding.source_asset_id, finding.error_code) for finding in findings] == [
        ("asset-invalid", "invalid_band_type"),
    ]


def test_pixel_sample_skips_known_optical_quality_variable() -> None:
    rule = default_rule_registry().get("pixel_sample")
    assert rule is not None
    reader = _ObjectReader(AssetInspection(sample_pixels=64, valid_pixels=64, nonzero_pixels=0))
    context = RuleContext(
        dataset_id="optical-a",
        output_version="v1",
        data_type="optical",
        product_type="surface-reflectance",
        repository=_RowsRepository(
            (
                "source_asset_id", "cog_uri", "source_format", "checksum",
                "band_code", "band_type", "display_order", "attributes",
            ),
            [("asset-qa", "s3://user-1/cog/qa.tif", "cog", "a" * 64, "radsat_qa", "variable", 0, {"source_band_index": 1})],
        ),
        object_reader=reader,
    )

    assert list(rule.evaluate(context)) == []
    assert reader.calls == []


def test_pixel_sample_reads_one_normalized_signal_band_per_multiband_asset() -> None:
    rule = default_rule_registry().get("pixel_sample")
    assert rule is not None
    reader = _ObjectReader(AssetInspection(sample_pixels=64, valid_pixels=64, nonzero_pixels=64))
    context = RuleContext(
        dataset_id="optical-a",
        output_version="v1",
        data_type="optical",
        product_type="surface-reflectance",
        repository=_RowsRepository(
            (
                "source_asset_id", "cog_uri", "source_format", "checksum",
                "band_code", "band_type", "display_order", "attributes",
            ),
            [
                ("asset-a", "s3://user-1/cog/a.tif", "cog", "a" * 64, "qa_pixel", "variable", 0, {"source_band_index": 1}),
                ("asset-a", "s3://user-1/cog/a.tif", "cog", "a" * 64, "red", "spectral", 1, {"source_band_index": 2}),
                ("asset-a", "s3://user-1/cog/a.tif", "cog", "a" * 64, "nir", "spectral", 2, {"source_band_index": 3}),
            ],
        ),
        object_reader=reader,
    )

    assert list(rule.evaluate(context)) == []
    assert reader.calls == [("s3://user-1/cog/a.tif", "cog", True, 2, "a" * 64)]


def test_changed_optical_quality_semantics_are_recorded_in_rule_snapshots() -> None:
    snapshots = {
        item.code: item
        for item in snapshot_rules(
            default_rule_registry(),
            data_type="optical",
            product_type="surface-reflectance",
        )
    }

    assert snapshots["pixel_sample"].implementation_version == "1.1.0"
    assert snapshots["pixel_sample"].parameters["sample_band_selection"]
    assert "radsat_qa" in snapshots["pixel_sample"].parameters["optical_auxiliary_variable_bands"]
    assert snapshots["optical_band_contract"].implementation_version == "1.1.0"
    assert "radsat_qa" in snapshots["optical_band_contract"].parameters["allowed_variable_bands"]


def test_carbon_rules_validate_current_partition_index_observations() -> None:
    registry = default_rule_registry()
    attributes = {
        "observation_id": "obs-1",
        "center_lon": 116.3,
        "center_lat": 39.9,
        "xco2": 417.2,
        "quality_flag": "0",
        "footprint_geojson": {"type": "Polygon", "coordinates": []},
    }
    context = RuleContext(
        dataset_id="carbon-a",
        output_version="v1",
        data_type="carbon",
        product_type="xco2",
        repository=_RowsRepository(
            ("output_id", "source_asset_id", "attributes"),
            [("index-1", "asset-1", attributes)],
        ),
        object_reader=None,
    )

    for code in (
        "carbon_schema",
        "carbon_coordinates",
        "carbon_xco2_range",
        "carbon_quality_flags",
        "carbon_observation_duplicates",
        "carbon_footprints",
    ):
        rule = registry.get(code)
        assert rule is not None
        assert list(rule.evaluate(context)) == []


def test_carbon_rules_report_invalid_index_observation_with_index_id() -> None:
    registry = default_rule_registry()
    context = RuleContext(
        dataset_id="carbon-a",
        output_version="v1",
        data_type="carbon",
        product_type="xco2",
        repository=_RowsRepository(
            ("output_id", "source_asset_id", "attributes"),
            [
                ("index-1", "asset-1", {"observation_id": "duplicate", "center_lon": 181, "center_lat": 0, "xco2": 1001, "quality_flag": ""}),
                ("index-2", "asset-1", {"observation_id": "duplicate", "center_lon": 116, "center_lat": 40, "xco2": 417, "quality_flag": "1"}),
            ],
        ),
        object_reader=None,
    )

    findings = []
    for code in (
        "carbon_schema",
        "carbon_coordinates",
        "carbon_xco2_range",
        "carbon_quality_flags",
        "carbon_observation_duplicates",
        "carbon_footprints",
    ):
        rule = registry.get(code)
        assert rule is not None
        findings.extend(rule.evaluate(context))
    codes = [finding.error_code for finding in findings]
    assert "invalid_coordinates" in codes
    assert "xco2_out_of_range" in codes
    assert "missing_quality_flag" in codes
    assert "duplicate_observation_id" in codes
    assert codes.count("missing_footprint") == 2
    assert all(finding.index_id for finding in findings)
