from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC
from typing import Any, Protocol
from urllib.parse import urlparse

from pyproj import CRS

from cube_web.services.quality_contracts import QualityResult, RuleSnapshot, TerminalQualityStatus

DEFAULT_RULE_SET_VERSION = "2026.07.21-v8"

# Optional rules can be toggled on/off via quality config. Mandatory rules always run.
OPTIONAL_QUALITY_RULE_CODES = frozenset(
    {
        "asset_crs",
        "optical_band_contract",
        "radar_band_contract",
        "carbon_schema",
        "carbon_coordinates",
        "carbon_xco2_range",
        "carbon_quality_flags",
    }
)

RULE_NAMES = {
    "index_schema": "索引结构完整性",
    "output_count_consistency": "输出数量一致性",
    "output_reference_integrity": "输出引用完整性",
    "grid_method_agreement": "格网与剖分方式一致性",
    "cell_bbox_validity": "格网边界有效性",
    "time_bucket_consistency": "时间分桶一致性",
    "asset_readability": "数据单元可读性",
    "asset_crs": "数据单元坐标系",
    "window_bounds": "像素窗口边界",
    "optical_band_contract": "光学波段规范",
    "radar_band_contract": "雷达极化通道规范",
    "carbon_schema": "碳卫星数据结构",
    "carbon_coordinates": "碳卫星坐标有效性",
    "carbon_xco2_range": "XCO2 数值范围",
    "carbon_quality_flags": "碳卫星质量标识",
}

RULE_DESCRIPTIONS = {
    "index_schema": "检查索引记录是否具备完整的格网编码和关联信息。",
    "output_count_consistency": "核对输出瓦片、索引与源数据单元数量是否一致。",
    "output_reference_integrity": "检查索引引用的瓦片或输出对象是否真实存在。",
    "grid_method_agreement": "确认格网类型、层级和剖分方式与任务配置一致。",
    "cell_bbox_validity": "验证瓦片边界是否为合法的经纬度范围并且方向正确。",
    "time_bucket_consistency": "检查数据时间分桶是否与数据单元采集时间一致。",
    "asset_readability": "尝试读取输出数据，确认文件可打开且内容可访问。",
    "asset_crs": "检查数据单元是否声明有效且可解析的坐标参考系。",
    "window_bounds": "验证像素窗口没有超出源影像的有效行列范围。",
    "optical_band_contract": "检查光学产品波段名称、数量和展示字段是否符合规范。",
    "radar_band_contract": "检查雷达产品极化通道名称和数据结构是否符合规范。",
    "carbon_schema": "检查碳卫星文件是否包含规定的观测变量和维度结构。",
    "carbon_coordinates": "验证碳卫星观测经纬度是否存在且处于合法范围。",
    "carbon_xco2_range": "检查XCO2观测值是否落在物理合理范围内。",
    "carbon_quality_flags": "检查碳卫星质量标识是否存在并符合有效取值。",
}

OPTICAL_AUXILIARY_VARIABLE_BANDS = frozenset({
    "aerosol_qa", "bqa", "cloud_qa", "pixel_qa", "qa_pixel", "qa_radsat", "radsat_qa", "sr_aerosol", "sr_qa_aerosol",
})


@dataclass(frozen=True)
class RuleContext:
    dataset_id: str
    output_version: str
    data_type: str
    product_type: str | None
    repository: Any
    object_reader: Any


@dataclass(frozen=True)
class QualityFinding:
    error_code: str
    message: str
    source_asset_id: str | None = None
    band_code: str | None = None
    tile_id: str | None = None
    index_id: str | None = None
    output_id: str | None = None
    row_number: int | None = None
    field: str | None = None
    context: Mapping[str, Any] | None = None


class QualityRule(Protocol):
    code: str
    name: str
    applicability: Mapping[str, Any]
    mandatory: bool
    parameters: Mapping[str, Any]
    implementation_version: str

    def applies(self, *, data_type: str, product_type: str | None) -> bool: ...

    def evaluate(self, context: RuleContext) -> Iterable[QualityFinding]: ...


@dataclass(frozen=True)
class RegisteredRule:
    code: str
    name: str
    applicability: Mapping[str, Any]
    mandatory: bool
    parameters: Mapping[str, Any]
    implementation_version: str = "1.0.0"
    evaluator: Callable[[RuleContext], Iterable[QualityFinding]] = lambda _: ()

    def applies(self, *, data_type: str, product_type: str | None) -> bool:
        data_types = self.applicability.get("data_types", ())
        product_types = self.applicability.get("product_types")
        return data_type in data_types and (product_types is None or product_type in product_types)

    def evaluate(self, context: RuleContext) -> Iterable[QualityFinding]:
        return self.evaluator(context)


class RuleRegistry:
    def __init__(self, rules: Iterable[QualityRule]) -> None:
        definitions = tuple(rules)
        self._rules = {rule.code: rule for rule in definitions}
        if len(self._rules) != len(definitions):
            raise ValueError("quality rule codes must be unique")

    def get(self, code: str) -> QualityRule | None:
        return self._rules.get(code)

    def all(self) -> tuple[QualityRule, ...]:
        return tuple(self._rules.values())

    def applicable(self, *, data_type: str, product_type: str | None) -> tuple[QualityRule, ...]:
        return tuple(rule for rule in self._rules.values() if rule.applies(data_type=data_type, product_type=product_type))


def default_enabled_optional_rules() -> frozenset[str]:
    """Optional rules are enabled by default; callers may narrow the set via config."""
    return OPTIONAL_QUALITY_RULE_CODES


def normalize_enabled_optional_rules(codes: Iterable[str] | None) -> tuple[str, ...]:
    """Validate and normalize an optional-rule enablement list.

    Only known optional rule codes are accepted. Order follows the registry definition order.
    """
    if codes is None:
        selected = set(default_enabled_optional_rules())
    else:
        selected: set[str] = set()
        for raw in codes:
            code = str(raw or "").strip()
            if not code:
                continue
            if code not in OPTIONAL_QUALITY_RULE_CODES:
                raise ValueError(f"unknown or non-optional quality rule: {code}")
            selected.add(code)
    registry_order = [rule.code for rule in default_rule_registry().all() if rule.code in OPTIONAL_QUALITY_RULE_CODES]
    return tuple(code for code in registry_order if code in selected)


def is_rule_enabled(rule: QualityRule, *, enabled_optional_rules: Iterable[str] | None) -> bool:
    if rule.mandatory:
        return True
    enabled = set(enabled_optional_rules) if enabled_optional_rules is not None else set(default_enabled_optional_rules())
    return rule.code in enabled


def snapshot_rules(
    registry: RuleRegistry,
    *,
    data_type: str,
    product_type: str | None,
    enabled_optional_rules: Iterable[str] | None = None,
) -> tuple[RuleSnapshot, ...]:
    """Build the executable rule snapshot for one dataset.

    Mandatory applicable rules always appear. Optional rules appear only when enabled.
    """
    enabled = (
        set(normalize_enabled_optional_rules(enabled_optional_rules))
        if enabled_optional_rules is not None
        else set(default_enabled_optional_rules())
    )
    snapshots: list[RuleSnapshot] = []
    for rule in registry.applicable(data_type=data_type, product_type=product_type):
        if not is_rule_enabled(rule, enabled_optional_rules=enabled):
            continue
        snapshots.append(
            RuleSnapshot(
                code=rule.code,
                name=rule.name,
                description=RULE_DESCRIPTIONS.get(rule.code, ""),
                applicability=dict(rule.applicability),
                mandatory=rule.mandatory,
                parameters=dict(rule.parameters),
                implementation_version=rule.implementation_version,
            )
        )
    return tuple(snapshots)


def reduce_quality_status(results: list[QualityResult], execution_error: str | None) -> TerminalQualityStatus:
    if execution_error is not None or any(item.status == "error" for item in results):
        return "error"
    if any(item.status == "fail" for item in results):
        return "fail"
    if any(item.status == "warn" for item in results):
        return "warn"
    return "pass"


def _index_schema(context: RuleContext) -> Iterable[QualityFinding]:
    with context.repository.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM partition_indexes WHERE dataset_id = %s AND output_version = %s AND st_code IS NULL",
            (context.dataset_id, context.output_version),
        )
        missing = int(cur.fetchone()[0])
    if missing:
        yield QualityFinding("missing_st_code", f"{missing} index rows have no ST code", field="st_code")


def _rows(context: RuleContext, sql: str, params: tuple[Any, ...] | None = None) -> list[Mapping[str, Any]]:
    with context.repository.cursor() as cur:
        cur.execute(sql, params or (context.dataset_id, context.output_version))
        columns = tuple(column.name for column in cur.description)
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def _output_reference_integrity(context: RuleContext) -> Iterable[QualityFinding]:
    for row in _rows(
        context,
        "SELECT i.output_id FROM partition_indexes i LEFT JOIN partition_tiles t ON t.output_id = i.tile_output_id "
        "WHERE i.dataset_id = %s AND i.output_version = %s AND i.tile_output_id IS NOT NULL AND t.output_id IS NULL",
    ):
        yield QualityFinding(
            "missing_tile_reference", "index references a missing tile", index_id=row["output_id"], output_id=row["output_id"]
        )


def _grid_method_agreement(context: RuleContext) -> Iterable[QualityFinding]:
    version_rows = _rows(
        context,
        "SELECT grid_type, requested_grid_level, partition_method FROM partition_output_versions "
        "WHERE dataset_id = %s AND output_version = %s",
    )
    if not version_rows:
        yield QualityFinding("missing_output_version", "quality target has no output version")
        return
    version = version_rows[0]
    # Carbon outputs reference immutable raw observation files; they never turn
    # a NetCDF/HDF source into a generated entity raster.
    expected_kind = "logical_reference" if context.data_type == "carbon" else (
        "entity_file" if version["partition_method"] == "entity" else "logical_reference"
    )
    for row in _rows(
        context,
        "SELECT output_id, grid_type, grid_level, tile_kind FROM partition_tiles WHERE dataset_id = %s AND output_version = %s",
    ):
        if row["grid_type"] != version["grid_type"] or int(row["grid_level"]) != int(version["requested_grid_level"]):
            yield QualityFinding(
                "tile_grid_mismatch", "tile grid does not match output version", tile_id=row["output_id"], output_id=row["output_id"]
            )
        if row["tile_kind"] != expected_kind:
            yield QualityFinding(
                "tile_kind_mismatch",
                f"{version['partition_method']} output requires {expected_kind}",
                tile_id=row["output_id"],
                output_id=row["output_id"],
                field="tile_kind",
            )
    for table in ("partition_indexes", "partition_grid_cells"):
        for row in _rows(context, f"SELECT output_id, grid_type, grid_level FROM {table} WHERE dataset_id = %s AND output_version = %s"):
            if row["grid_type"] != version["grid_type"] or int(row["grid_level"]) != int(version["requested_grid_level"]):
                yield QualityFinding("detail_grid_mismatch", f"{table} grid does not match output version", output_id=row["output_id"])


def _bbox_is_valid(value: Any) -> bool:
    if isinstance(value, str):
        import json

        value = json.loads(value)
    if not isinstance(value, (list, tuple)) or len(value) != 4:
        return False
    try:
        west, south, east, north = (float(item) for item in value)
    except (TypeError, ValueError):
        return False
    return -180 <= west < east <= 180 and -90 <= south < north <= 90


def _cell_bbox_validity(context: RuleContext) -> Iterable[QualityFinding]:
    for row in _rows(context, "SELECT output_id, bbox FROM partition_grid_cells WHERE dataset_id = %s AND output_version = %s"):
        if not _bbox_is_valid(row["bbox"]):
            yield QualityFinding(
                "invalid_bbox",
                "grid cell bbox must be [west, south, east, north] in WGS84 bounds",
                output_id=row["output_id"],
                field="bbox",
            )


def _time_bucket_consistency(context: RuleContext) -> Iterable[QualityFinding]:
    for row in _rows(
        context,
        "SELECT output_id, acquisition_time, time_bucket FROM partition_indexes WHERE dataset_id = %s AND output_version = %s",
    ):
        time_bucket = str(row["time_bucket"] or "")
        acquisition_time = row["acquisition_time"]
        if not time_bucket:
            yield QualityFinding(
                "missing_time_bucket",
                "index has no time bucket",
                index_id=row["output_id"],
                output_id=row["output_id"],
                field="time_bucket",
            )
        elif acquisition_time is not None and not time_bucket.startswith(acquisition_time.astimezone(UTC).strftime("%Y%m%d")):
            yield QualityFinding(
                "time_bucket_mismatch",
                "time bucket does not match acquisition date",
                index_id=row["output_id"],
                output_id=row["output_id"],
                field="time_bucket",
            )


def _asset_readability(context: RuleContext) -> Iterable[QualityFinding]:
    for row in _rows(
        context,
        "SELECT source_asset_id, cog_uri, source_uri, source_format, checksum "
        "FROM partition_dataset_assets a WHERE dataset_id = %s AND EXISTS ("
        "SELECT 1 FROM partition_indexes i WHERE i.dataset_id = a.dataset_id "
        "AND i.output_version = %s AND i.source_asset_id = a.source_asset_id)",
    ):
        source_asset_id = row["source_asset_id"]
        source_uri = str(row.get("source_uri") or "")
        source_format = str(row.get("source_format") or "")
        object_uri = source_uri if context.data_type == "carbon" else str(row.get("cog_uri") or source_uri)
        source_contract_valid = True
        if context.data_type == "carbon":
            allowed = {"netcdf": (".nc", ".nc4"), "hdf5": (".h5", ".hdf", ".hdf5")}
            if (
                source_format not in allowed
                or not _is_supported_object_uri(source_uri)
                or not source_uri.lower().endswith(allowed.get(source_format, ()))
            ):
                source_contract_valid = False
                yield QualityFinding(
                    "invalid_carbon_source",
                    "carbon source must be an s3 NetCDF/HDF5 asset matching source_format",
                    source_asset_id=source_asset_id,
                    field="source_uri",
                )
        elif source_format != "cog" or not _is_supported_object_uri(object_uri):
            source_contract_valid = False
            yield QualityFinding(
                "invalid_cog_uri",
                "source asset must use an s3 COG URI",
                source_asset_id=source_asset_id,
                field="cog_uri",
            )
        checksum = str(row["checksum"] or "")
        checksum_valid = re.fullmatch(r"[0-9a-f]{64}", checksum) is not None
        if not checksum_valid:
            yield QualityFinding(
                "invalid_checksum",
                "source asset checksum must be a SHA-256 hex digest",
                source_asset_id=source_asset_id,
                field="checksum",
            )
        if source_contract_valid and checksum_valid:
            if context.object_reader is None:
                yield QualityFinding(
                    "object_reader_unavailable",
                    "quality object reader is unavailable",
                    source_asset_id=source_asset_id,
                    field="source_uri",
                )
                continue
            try:
                context.object_reader.inspect(object_uri, source_format, expected_checksum=checksum)
            except Exception as exc:
                yield QualityFinding(
                    "source_object_unreadable",
                    "source object does not exist or cannot be opened",
                    source_asset_id=source_asset_id,
                    field="source_uri",
                    context={"reason": type(exc).__name__},
                )


def _is_supported_object_uri(value: str) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    parsed = urlparse(text)
    return parsed.scheme == "s3" and bool(parsed.netloc and parsed.path.strip("/"))


def _asset_crs(context: RuleContext) -> Iterable[QualityFinding]:
    if context.data_type == "carbon":
        return
    for row in _rows(
        context,
        "SELECT source_asset_id, cog_uri, source_format, crs, checksum FROM partition_dataset_assets a "
        "WHERE a.dataset_id = %s AND EXISTS (SELECT 1 FROM partition_indexes i WHERE i.dataset_id = a.dataset_id "
        "AND i.output_version = %s AND i.source_asset_id = a.source_asset_id)",
    ):
        value = str(row["crs"] or "").strip()
        if not value:
            yield QualityFinding("missing_crs", "source asset CRS is required", source_asset_id=row["source_asset_id"], field="crs")
            continue
        try:
            declared_crs = CRS.from_user_input(value)
        except Exception:
            yield QualityFinding(
                "invalid_crs",
                "source asset CRS is not a valid coordinate reference system",
                source_asset_id=row["source_asset_id"],
                field="crs",
            )
            continue
        if context.object_reader is None or row["source_format"] != "cog" or not row["cog_uri"]:
            continue
        try:
            inspection = context.object_reader.inspect(
                str(row["cog_uri"]),
                "cog",
                expected_checksum=str(row["checksum"]),
            )
            actual_crs = CRS.from_user_input(inspection.crs) if inspection.crs else None
        except Exception:
            # Object and raster readability belong to asset_readability.
            continue
        if actual_crs is not None and not declared_crs.equals(actual_crs):
            yield QualityFinding(
                "crs_metadata_mismatch",
                "declared CRS does not match the source raster CRS",
                source_asset_id=row["source_asset_id"],
                field="crs",
                context={"declared": declared_crs.to_string(), "actual": actual_crs.to_string()},
            )


def _band_contract(
    context: RuleContext,
    expected_type: str,
    *,
    allowed_variable_bands: frozenset[str] = frozenset(),
) -> Iterable[QualityFinding]:
    for row in _rows(
        context,
        "SELECT a.source_asset_id, b.band_code, b.band_type FROM partition_dataset_assets a "
        "LEFT JOIN partition_dataset_bands b ON b.dataset_id = a.dataset_id AND b.source_asset_id = a.source_asset_id "
        "WHERE a.dataset_id = %s AND EXISTS (SELECT 1 FROM partition_indexes i WHERE i.dataset_id = a.dataset_id "
        "AND i.output_version = %s AND i.source_asset_id = a.source_asset_id) "
        "ORDER BY a.source_asset_id, b.display_order, b.band_code",
    ):
        if not str(row["band_code"] or "").strip():
            yield QualityFinding(
                "missing_band_metadata",
                "source asset has no normalized band metadata",
                source_asset_id=row["source_asset_id"],
                band_code=None if row["band_code"] is None else str(row["band_code"]),
                field="band_code",
            )
        elif row["band_type"] != expected_type and not (
            row["band_type"] == "variable"
            and str(row["band_code"]).strip().lower() in allowed_variable_bands
        ):
            yield QualityFinding(
                "invalid_band_type",
                f"band_type must be {expected_type} for {context.data_type} data",
                source_asset_id=row["source_asset_id"],
                band_code=str(row["band_code"]),
                field="band_type",
                context={"band_code": row["band_code"], "actual": row["band_type"], "expected": expected_type},
            )


def _optical_band_contract(context: RuleContext) -> Iterable[QualityFinding]:
    return _band_contract(
        context,
        "spectral",
        allowed_variable_bands=OPTICAL_AUXILIARY_VARIABLE_BANDS,
    )


def _radar_band_contract(context: RuleContext) -> Iterable[QualityFinding]:
    return _band_contract(context, "polarization")


def _window_bounds(context: RuleContext) -> Iterable[QualityFinding]:
    for row in _rows(
        context,
        "SELECT i.output_id, i.window_col_off, i.window_row_off, i.window_width, i.window_height, t.width, t.height "
        "FROM partition_indexes i JOIN partition_tiles t ON t.output_id = i.tile_output_id "
        "WHERE i.dataset_id = %s AND i.output_version = %s AND i.tile_output_id IS NOT NULL",
    ):
        if None in (row["window_col_off"], row["window_row_off"], row["window_width"], row["window_height"], row["width"], row["height"]):
            continue
        if row["window_col_off"] + row["window_width"] > row["width"] or row["window_row_off"] + row["window_height"] > row["height"]:
            yield QualityFinding(
                "window_out_of_bounds",
                "index window exceeds tile dimensions",
                index_id=row["output_id"],
                output_id=row["output_id"],
                field="window",
            )


def _carbon_schema(context: RuleContext) -> Iterable[QualityFinding]:
    required = {"observation_id", "lon", "lat", "xco2", "quality_flag"}
    rows = _carbon_index_rows(context)
    if not rows:
        yield QualityFinding("missing_carbon_indexes", "carbon output has no observation indexes")
        return
    for row, attributes in rows:
        missing = sorted(required - set(attributes))
        if missing:
            yield QualityFinding(
                "missing_carbon_fields",
                f"missing carbon fields: {', '.join(missing)}",
                source_asset_id=row["source_asset_id"],
                index_id=row["output_id"],
            )


def _carbon_coordinates(context: RuleContext) -> Iterable[QualityFinding]:
    for finding in _carbon_attribute_rows(context, "carbon_coordinates"):
        yield finding


def _carbon_xco2_range(context: RuleContext) -> Iterable[QualityFinding]:
    for finding in _carbon_attribute_rows(context, "carbon_xco2_range"):
        yield finding


def _carbon_quality_flags(context: RuleContext) -> Iterable[QualityFinding]:
    for finding in _carbon_attribute_rows(context, "carbon_quality_flags"):
        yield finding


def _carbon_attribute_rows(context: RuleContext, kind: str) -> Iterable[QualityFinding]:
    for row, attributes in _carbon_index_rows(context):
        try:
            lon, lat = float(attributes["lon"]), float(attributes["lat"])
            xco2 = float(attributes["xco2"])
        except (KeyError, TypeError, ValueError):
            continue
        if kind == "carbon_coordinates" and not (-180 <= lon <= 180 and -90 <= lat <= 90):
            yield QualityFinding(
                "invalid_coordinates",
                "carbon coordinates are outside WGS84 bounds",
                source_asset_id=row["source_asset_id"],
                index_id=row["output_id"],
            )
        elif kind == "carbon_xco2_range" and not (0 < xco2 < 1000):
            yield QualityFinding(
                "xco2_out_of_range",
                "xco2 must be between 0 and 1000 ppm",
                source_asset_id=row["source_asset_id"],
                index_id=row["output_id"],
            )
        elif kind == "carbon_quality_flags" and (
            attributes.get("quality_flag") is None or not str(attributes["quality_flag"]).strip()
        ):
            yield QualityFinding(
                "missing_quality_flag",
                "carbon quality_flag is required",
                source_asset_id=row["source_asset_id"],
                index_id=row["output_id"],
            )


def _carbon_index_rows(context: RuleContext) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    rows = _rows(
        context,
        "SELECT output_id, source_asset_id, attributes FROM partition_indexes "
        "WHERE dataset_id = %s AND output_version = %s ORDER BY output_id",
        (context.dataset_id, context.output_version),
    )
    normalized = []
    for row in rows:
        attributes = row["attributes"] or {}
        if isinstance(attributes, str):
            import json

            attributes = json.loads(attributes)
        attributes = dict(attributes)
        if "lon" not in attributes and "center_lon" in attributes:
            attributes["lon"] = attributes["center_lon"]
        if "lat" not in attributes and "center_lat" in attributes:
            attributes["lat"] = attributes["center_lat"]
        if "footprint" not in attributes and "footprint_geojson" in attributes:
            attributes["footprint"] = attributes["footprint_geojson"]
        normalized.append((row, attributes))
    return normalized


def _output_count_consistency(context: RuleContext) -> Iterable[QualityFinding]:
    with context.repository.cursor() as cur:
        cur.execute(
            "SELECT tile_count, index_count, grid_cell_count FROM partition_output_versions WHERE dataset_id = %s AND output_version = %s",
            (context.dataset_id, context.output_version),
        )
        expected = cur.fetchone()
        cur.execute(
            "SELECT (SELECT count(*) FROM partition_tiles WHERE dataset_id = %s AND output_version = %s), "
            "(SELECT count(*) FROM partition_indexes WHERE dataset_id = %s AND output_version = %s), "
            "(SELECT count(*) FROM partition_grid_cells WHERE dataset_id = %s AND output_version = %s)",
            (
                context.dataset_id,
                context.output_version,
                context.dataset_id,
                context.output_version,
                context.dataset_id,
                context.output_version,
            ),
        )
        actual = cur.fetchone()
    if expected is None or tuple(map(int, expected)) != tuple(map(int, actual)):
        yield QualityFinding("output_count_mismatch", "output version counts do not match normalized detail rows")


def default_rule_registry() -> RuleRegistry:
    shared = (
        "index_schema",
        "output_count_consistency",
        "output_reference_integrity",
        "grid_method_agreement",
        "cell_bbox_validity",
        "time_bucket_consistency",
        "asset_readability",
        "window_bounds",
    )
    evaluators = {
        "index_schema": _index_schema,
        "output_count_consistency": _output_count_consistency,
        "output_reference_integrity": _output_reference_integrity,
        "grid_method_agreement": _grid_method_agreement,
        "cell_bbox_validity": _cell_bbox_validity,
        "time_bucket_consistency": _time_bucket_consistency,
        "asset_readability": _asset_readability,
        "asset_crs": _asset_crs,
        "window_bounds": _window_bounds,
        "optical_band_contract": _optical_band_contract,
        "radar_band_contract": _radar_band_contract,
        "carbon_schema": _carbon_schema,
        "carbon_coordinates": _carbon_coordinates,
        "carbon_xco2_range": _carbon_xco2_range,
        "carbon_quality_flags": _carbon_quality_flags,
    }
    rules = [
        RegisteredRule(
            code=code,
            name=RULE_NAMES[code],
            applicability={"data_types": ["optical", "radar", "product", "carbon"]},
            mandatory=True,
            parameters={},
        )
        for code in shared
    ]
    rules = [
        RegisteredRule(
            code=rule.code,
            name=rule.name,
            applicability=rule.applicability,
            mandatory=rule.mandatory,
            parameters=rule.parameters,
            evaluator=evaluators[rule.code],
        )
        for rule in rules
    ]
    rules.append(
        RegisteredRule(
            "asset_crs",
            RULE_NAMES["asset_crs"],
            {"data_types": ["optical", "radar", "product"]},
            False,
            {"parser": "pyproj"},
            evaluator=evaluators["asset_crs"],
        )
    )
    for code, data_type, band_type in (
        ("optical_band_contract", "optical", "spectral"),
        ("radar_band_contract", "radar", "polarization"),
    ):
        parameters: dict[str, Any] = {"expected_band_type": band_type}
        implementation_version = "1.0.0"
        if code == "optical_band_contract":
            parameters["allowed_variable_bands"] = sorted(OPTICAL_AUXILIARY_VARIABLE_BANDS)
            implementation_version = "1.1.0"
        rules.append(
            RegisteredRule(
                code,
                RULE_NAMES[code],
                {"data_types": [data_type]},
                False,
                parameters,
                implementation_version=implementation_version,
                evaluator=evaluators[code],
            )
        )
    rules.extend(
        RegisteredRule(code, RULE_NAMES[code], {"data_types": ["carbon"]}, False, {}, evaluator=evaluators[code])
        for code in (
            "carbon_schema",
            "carbon_coordinates",
            "carbon_xco2_range",
            "carbon_quality_flags",
        )
    )
    return RuleRegistry(rules)
