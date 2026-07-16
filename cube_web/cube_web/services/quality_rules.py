from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from cube_web.services.quality_contracts import QualityResult, RuleSnapshot, TerminalQualityStatus

DEFAULT_RULE_SET_VERSION = "2026.07.14-v1"


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

    def applicable(self, *, data_type: str, product_type: str | None) -> tuple[QualityRule, ...]:
        return tuple(rule for rule in self._rules.values() if rule.applies(data_type=data_type, product_type=product_type))


def snapshot_rules(registry: RuleRegistry, *, data_type: str, product_type: str | None) -> tuple[RuleSnapshot, ...]:
    return tuple(
        RuleSnapshot(
            code=rule.code,
            name=rule.name,
            applicability=dict(rule.applicability),
            mandatory=rule.mandatory,
            parameters=dict(rule.parameters),
            implementation_version=rule.implementation_version,
        )
        for rule in registry.applicable(data_type=data_type, product_type=product_type)
    )


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
        elif acquisition_time is not None and not time_bucket.startswith(acquisition_time.strftime("%Y%m%d")):
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
        "FROM partition_dataset_assets WHERE dataset_id = %s",
        (context.dataset_id,),
    ):
        source_asset_id = row["source_asset_id"]
        source_uri = str(row.get("source_uri") or "")
        source_format = str(row.get("source_format") or "")
        if context.data_type == "carbon":
            allowed = {"netcdf": (".nc", ".nc4"), "hdf5": (".h5", ".hdf", ".hdf5")}
            if source_format not in allowed or not source_uri.startswith("s3://") or not source_uri.lower().endswith(allowed.get(source_format, ())):
                yield QualityFinding(
                    "invalid_carbon_source",
                    "carbon source must be an s3 NetCDF or HDF5 asset matching source_format",
                    source_asset_id=source_asset_id,
                    field="source_uri",
                )
        elif source_format != "cog" or not str(row.get("cog_uri") or "").startswith("s3://"):
            yield QualityFinding(
                "invalid_cog_uri", "source asset must use an s3 COG URI", source_asset_id=source_asset_id, field="cog_uri"
            )
        if not re.fullmatch(r"[0-9a-f]{64}", str(row["checksum"] or "")):
            yield QualityFinding(
                "invalid_checksum",
                "source asset checksum must be a SHA-256 hex digest",
                source_asset_id=source_asset_id,
                field="checksum",
            )


def _asset_crs(context: RuleContext) -> Iterable[QualityFinding]:
    if context.data_type == "carbon":
        return
    for row in _rows(context, "SELECT source_asset_id, crs FROM partition_dataset_assets WHERE dataset_id = %s", (context.dataset_id,)):
        if not str(row["crs"] or "").strip():
            yield QualityFinding("missing_crs", "source asset CRS is required", source_asset_id=row["source_asset_id"], field="crs")


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


def _metadata_completeness(context: RuleContext) -> Iterable[QualityFinding]:
    for row in _rows(
        context,
        "SELECT source_asset_id, time_start, time_end, bbox, crs FROM partition_dataset_assets WHERE dataset_id = %s",
        (context.dataset_id,),
    ):
        required = ("time_start", "time_end") if context.data_type == "carbon" else ("time_start", "time_end", "crs")
        missing = [name for name in required if row[name] is None or not str(row[name]).strip()]
        if context.data_type != "carbon" and row["bbox"] is None:
            missing.append("bbox")
        if missing:
            yield QualityFinding(
                "incomplete_asset_metadata",
                f"missing metadata: {', '.join(missing)}",
                source_asset_id=row["source_asset_id"],
                field=missing[0],
            )


def _declared_metadata_defects(context: RuleContext) -> Iterable[QualityFinding]:
    """Persist deterministic loader-declared metadata findings without truncation."""
    for row in _rows(context, "SELECT source_asset_id, attributes FROM partition_dataset_assets WHERE dataset_id = %s", (context.dataset_id,)):
        attributes = row["attributes"] or {}
        if isinstance(attributes, str):
            import json

            attributes = json.loads(attributes)
        for item in attributes.get("quality_metadata_defects", []):
            if not isinstance(item, dict):
                continue
            yield QualityFinding(
                str(item.get("error_code") or "declared_metadata_defect"),
                str(item.get("message") or "loader-declared metadata defect"),
                source_asset_id=row["source_asset_id"],
                field=str(item.get("field") or "metadata"),
                context={"manifest_declared": True},
            )


def _product_year_consistency(context: RuleContext) -> Iterable[QualityFinding]:
    for row in _rows(
        context, "SELECT source_asset_id, time_start, attributes FROM partition_dataset_assets WHERE dataset_id = %s", (context.dataset_id,)
    ):
        attributes = row["attributes"] or {}
        if isinstance(attributes, str):
            import json

            attributes = json.loads(attributes)
        year = attributes.get("product_year")
        if year is None:
            yield QualityFinding(
                "missing_product_year", "product asset has no product_year", source_asset_id=row["source_asset_id"], field="product_year"
            )
        elif row["time_start"] is not None and str(year) != str(row["time_start"].year):
            yield QualityFinding(
                "product_year_mismatch",
                "product_year does not match source time",
                source_asset_id=row["source_asset_id"],
                field="product_year",
            )


def _carbon_schema(context: RuleContext) -> Iterable[QualityFinding]:
    required = {"observation_id", "lon", "lat", "xco2", "quality_flag"}
    for row in _rows(
        context, "SELECT source_asset_id, attributes FROM partition_dataset_assets WHERE dataset_id = %s", (context.dataset_id,)
    ):
        attributes = row["attributes"] or {}
        if isinstance(attributes, str):
            import json

            attributes = json.loads(attributes)
        missing = sorted(required - set(attributes))
        if missing:
            yield QualityFinding(
                "missing_carbon_fields", f"missing carbon fields: {', '.join(missing)}", source_asset_id=row["source_asset_id"]
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


def _carbon_observation_duplicates(context: RuleContext) -> Iterable[QualityFinding]:
    for row in _rows(context, "SELECT attributes FROM partition_dataset_assets WHERE dataset_id = %s", (context.dataset_id,)):
        attributes = row["attributes"] or {}
        if isinstance(attributes, str):
            import json

            attributes = json.loads(attributes)
        observations = attributes.get("observations", [])
        identifiers = [item.get("observation_id") for item in observations if isinstance(item, dict)]
        if len(identifiers) != len(set(identifiers)):
            yield QualityFinding("duplicate_observation_id", "carbon observations contain duplicate observation IDs")


def _carbon_footprints(context: RuleContext) -> Iterable[QualityFinding]:
    for row in _rows(
        context, "SELECT source_asset_id, attributes FROM partition_dataset_assets WHERE dataset_id = %s", (context.dataset_id,)
    ):
        attributes = row["attributes"] or {}
        if isinstance(attributes, str):
            import json

            attributes = json.loads(attributes)
        if attributes.get("footprint") is None:
            yield QualityFinding(
                "missing_footprint", "carbon asset has no footprint", source_asset_id=row["source_asset_id"], field="footprint"
            )


def _carbon_attribute_rows(context: RuleContext, kind: str) -> Iterable[QualityFinding]:
    for row in _rows(
        context, "SELECT source_asset_id, attributes FROM partition_dataset_assets WHERE dataset_id = %s", (context.dataset_id,)
    ):
        attributes = row["attributes"] or {}
        if isinstance(attributes, str):
            import json

            attributes = json.loads(attributes)
        try:
            lon, lat = float(attributes["lon"]), float(attributes["lat"])
            xco2 = float(attributes["xco2"])
        except (KeyError, TypeError, ValueError):
            continue
        if kind == "carbon_coordinates" and not (-180 <= lon <= 180 and -90 <= lat <= 90):
            yield QualityFinding(
                "invalid_coordinates", "carbon coordinates are outside WGS84 bounds", source_asset_id=row["source_asset_id"]
            )
        elif kind == "carbon_xco2_range" and not (0 < xco2 < 1000):
            yield QualityFinding("xco2_out_of_range", "xco2 must be between 0 and 1000 ppm", source_asset_id=row["source_asset_id"])
        elif kind == "carbon_quality_flags" and not str(attributes.get("quality_flag") or "").strip():
            yield QualityFinding("missing_quality_flag", "carbon quality_flag is required", source_asset_id=row["source_asset_id"])


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
        "asset_crs",
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
        "pixel_sample": lambda _: (),
        "metadata_completeness": _metadata_completeness,
        "declared_metadata_defects": _declared_metadata_defects,
        "product_year_consistency": _product_year_consistency,
        "carbon_schema": _carbon_schema,
        "carbon_coordinates": _carbon_coordinates,
        "carbon_xco2_range": _carbon_xco2_range,
        "carbon_quality_flags": _carbon_quality_flags,
        "carbon_observation_duplicates": _carbon_observation_duplicates,
        "carbon_footprints": _carbon_footprints,
    }
    rules = [
        RegisteredRule(
            code=code,
            name=code.replace("_", " ").title(),
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
    rules.extend(
        RegisteredRule(
            code=code,
            name=code.replace("_", " ").title(),
            applicability={"data_types": ["optical", "radar", "product", "carbon"]},
            mandatory=False,
            parameters={},
            evaluator=evaluators[code],
        )
        for code in ("pixel_sample", "metadata_completeness", "declared_metadata_defects")
    )
    rules.append(
        RegisteredRule(
            "product_year_consistency",
            "Product year consistency",
            {"data_types": ["product"]},
            True,
            {},
            evaluator=evaluators["product_year_consistency"],
        )
    )
    rules.extend(
        RegisteredRule(code, code.replace("_", " ").title(), {"data_types": ["carbon"]}, True, {}, evaluator=evaluators[code])
        for code in (
            "carbon_schema",
            "carbon_coordinates",
            "carbon_xco2_range",
            "carbon_quality_flags",
            "carbon_observation_duplicates",
            "carbon_footprints",
        )
    )
    return RuleRegistry(rules)
