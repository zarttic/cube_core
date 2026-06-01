from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cube_split.quality import optical_quality

REQUIRED_CARBON_FIELDS = {
    "data_type",
    "satellite",
    "product_type",
    "observation_id",
    "acq_time",
    "time_bucket",
    "grid_type",
    "grid_level",
    "space_code",
    "st_code",
    "xco2",
    "quality_flag",
    "center_lon",
    "center_lat",
}

XCO2_MIN = 250.0
XCO2_MAX = 650.0


def _check(name: str, status: str, message: str, **metrics: Any) -> dict[str, Any]:
    return {"name": name, "status": status, "message": message, "metrics": metrics}


def _validate_required_fields(rows: list[dict[str, Any]]) -> dict[str, Any]:
    missing_rows: list[dict[str, Any]] = []
    for row in rows:
        missing = sorted(field for field in REQUIRED_CARBON_FIELDS if field not in row)
        if missing:
            missing_rows.append({"line_no": row.get("_line_no"), "missing": missing})
    if missing_rows:
        return _check(
            "carbon_schema",
            "FAIL",
            "Some carbon observation rows are missing required fields.",
            missing_rows=missing_rows[:20],
            missing_count=len(missing_rows),
        )
    return _check(
        "carbon_schema",
        "PASS",
        "All carbon observation rows contain required fields.",
        required_fields=len(REQUIRED_CARBON_FIELDS),
    )


def _validate_time_buckets(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return optical_quality._validate_time_buckets(rows)


def _validate_coordinates(rows: list[dict[str, Any]]) -> dict[str, Any]:
    invalid_rows: list[dict[str, Any]] = []
    for row in rows:
        try:
            lon = float(row["center_lon"])
            lat = float(row["center_lat"])
        except Exception:
            invalid_rows.append({"line_no": row.get("_line_no"), "reason": "non_numeric"})
            continue
        if not (-180 <= lon <= 180 and -90 <= lat <= 90):
            invalid_rows.append(
                {
                    "line_no": row.get("_line_no"),
                    "reason": "out_of_wgs84_range",
                    "center_lon": row.get("center_lon"),
                    "center_lat": row.get("center_lat"),
                }
            )
    if invalid_rows:
        return _check(
            "carbon_coordinates",
            "FAIL",
            "Some carbon observation coordinates are invalid.",
            invalid_rows=invalid_rows[:20],
            invalid_count=len(invalid_rows),
        )
    return _check("carbon_coordinates", "PASS", "All carbon observation coordinates are valid.", checked_rows=len(rows))


def _validate_xco2(rows: list[dict[str, Any]]) -> dict[str, Any]:
    invalid_rows: list[dict[str, Any]] = []
    values: list[float] = []
    for row in rows:
        try:
            value = float(row["xco2"])
        except Exception:
            invalid_rows.append({"line_no": row.get("_line_no"), "reason": "non_numeric", "xco2": row.get("xco2")})
            continue
        values.append(value)
        if not (XCO2_MIN <= value <= XCO2_MAX):
            invalid_rows.append(
                {
                    "line_no": row.get("_line_no"),
                    "reason": "out_of_expected_range",
                    "xco2": value,
                    "expected_range": [XCO2_MIN, XCO2_MAX],
                }
            )
    if invalid_rows:
        return _check(
            "xco2_range",
            "FAIL",
            "Some XCO2 values are missing or outside the expected range.",
            invalid_rows=invalid_rows[:20],
            invalid_count=len(invalid_rows),
            expected_range=[XCO2_MIN, XCO2_MAX],
        )
    return _check(
        "xco2_range",
        "PASS",
        "All XCO2 values are numeric and within the expected range.",
        min_xco2=min(values) if values else None,
        max_xco2=max(values) if values else None,
        avg_xco2=round(sum(values) / len(values), 6) if values else None,
    )


def _validate_quality_flags(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(str(row.get("quality_flag", "")) for row in rows)
    bad_count = sum(count for flag, count in counts.items() if flag not in {"", "0", "1"})
    if bad_count:
        return _check(
            "carbon_quality_flag",
            "WARN",
            "Some carbon quality flags are not standard OCO-2 pass/fail values.",
            quality_counts=dict(sorted(counts.items())),
            non_standard_count=bad_count,
        )
    return _check(
        "carbon_quality_flag",
        "PASS",
        "All carbon observations use standard quality flag values.",
        quality_counts=dict(sorted(counts.items())),
    )


def _validate_observation_duplicates(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(str(row.get("observation_id", "")) for row in rows)
    duplicates = [{"observation_id": key, "count": count} for key, count in counts.items() if key and count > 1]
    if duplicates:
        return _check(
            "carbon_duplicates",
            "WARN",
            "Some carbon observation IDs appear more than once.",
            duplicate_count=len(duplicates),
            duplicates=duplicates[:20],
        )
    return _check("carbon_duplicates", "PASS", "No duplicate carbon observation IDs were detected.", observation_count=len(counts))


def _validate_footprints(rows: list[dict[str, Any]]) -> dict[str, Any]:
    invalid_rows: list[dict[str, Any]] = []
    present_count = 0
    for row in rows:
        value = row.get("footprint_geojson")
        if value in (None, "", {}):
            continue
        present_count += 1
        if not isinstance(value, dict) or value.get("type") not in {"Point", "Polygon", "MultiPolygon"}:
            invalid_rows.append({"line_no": row.get("_line_no"), "type": value.get("type") if isinstance(value, dict) else type(value).__name__})
    if invalid_rows:
        return _check(
            "carbon_footprint",
            "WARN",
            "Some carbon footprint geometries are not valid GeoJSON point or polygon objects.",
            invalid_rows=invalid_rows[:20],
            invalid_count=len(invalid_rows),
            present_count=present_count,
        )
    return _check("carbon_footprint", "PASS", "Carbon footprint geometries are valid where present.", present_count=present_count)


def _summarize(rows: list[dict[str, Any]], checks: list[dict[str, Any]]) -> dict[str, Any]:
    quality_counts = Counter(str(row.get("quality_flag", "")) for row in rows)
    product_counts = Counter(str(row.get("product_type", "")) for row in rows)
    satellite_counts = Counter(str(row.get("satellite", "")) for row in rows)
    time_counts = Counter(str(row.get("time_bucket", "")) for row in rows)
    xco2_values: list[float] = []
    for row in rows:
        try:
            xco2_values.append(float(row.get("xco2")))
        except Exception:
            continue
    return {
        "index_rows": len(rows),
        "observation_rows": len(rows),
        "asset_count": 0,
        "distinct_space_codes": len({row.get("space_code") for row in rows if row.get("space_code") is not None}),
        "distinct_st_codes": len({row.get("st_code") for row in rows if row.get("st_code") is not None}),
        "distinct_observations": len({row.get("observation_id") for row in rows if row.get("observation_id")}),
        "quality_counts": dict(sorted(quality_counts.items())),
        "rows_by_product_type": dict(sorted(product_counts.items())),
        "rows_by_satellite": dict(sorted(satellite_counts.items())),
        "rows_by_time_bucket": dict(sorted(time_counts.items())),
        "min_xco2": min(xco2_values) if xco2_values else None,
        "max_xco2": max(xco2_values) if xco2_values else None,
        "avg_xco2": round(sum(xco2_values) / len(xco2_values), 6) if xco2_values else None,
        "passed_checks": sum(1 for check in checks if check["status"] == "PASS"),
        "warning_checks": sum(1 for check in checks if check["status"] == "WARN"),
        "failed_checks": sum(1 for check in checks if check["status"] == "FAIL"),
    }


def run_quality_check(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir)
    target_crs = getattr(args, "target_crs", "EPSG:4326") or "EPSG:4326"
    rows_path = run_dir / "carbon_observation_rows.jsonl"
    checks: list[dict[str, Any]] = []

    if not rows_path.exists():
        checks.append(_check("carbon_rows", "FAIL", f"carbon_observation_rows.jsonl not found under run_dir: {run_dir}"))
        return optical_quality._finalize_report({
            "status": "FAIL",
            "run_dir": str(run_dir),
            "target_crs": target_crs,
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "summary": _summarize([], checks),
            "checks": checks,
            "assets": [],
            "data_type": "carbon",
        }, run_dir, args)

    rows = optical_quality._load_jsonl(rows_path)
    if not rows:
        checks.append(_check("carbon_rows", "FAIL", "carbon_observation_rows.jsonl is empty."))
        return optical_quality._finalize_report({
            "status": "FAIL",
            "run_dir": str(run_dir),
            "target_crs": target_crs,
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "summary": _summarize([], checks),
            "checks": checks,
            "assets": [],
            "data_type": "carbon",
        }, run_dir, args)

    checks.append(_check("carbon_rows", "PASS", "carbon_observation_rows.jsonl was loaded.", row_count=len(rows), path=str(rows_path)))
    checks.append(_validate_required_fields(rows))
    if not any(check["name"] == "carbon_schema" and check["status"] == "FAIL" for check in checks):
        checks.append(_validate_time_buckets(rows))
        checks.append(_validate_coordinates(rows))
        checks.append(_validate_xco2(rows))
        checks.append(_validate_quality_flags(rows))
        checks.append(_validate_observation_duplicates(rows))
        checks.append(_validate_footprints(rows))

    report = {
        "status": optical_quality._overall_status(checks),
        "run_dir": str(run_dir.resolve()),
        "target_crs": target_crs,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "summary": _summarize(rows, checks),
        "checks": checks,
        "assets": [],
        "data_type": "carbon",
    }
    return optical_quality._finalize_report(report, run_dir, args)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run quality checks for carbon satellite partition output")
    parser.add_argument("--run-dir", required=True, help="Partition run directory containing carbon_observation_rows.jsonl")
    parser.add_argument("--target-crs", default="EPSG:4326", help="Expected CRS label for report compatibility")
    parser.add_argument("--output", default="", help="Optional quality report JSON path")
    return parser.parse_args()


def main() -> None:
    report = run_quality_check(parse_args())
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
