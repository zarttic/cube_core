from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cube_split.quality import optical_quality


def _check(name: str, status: str, message: str, **metrics: Any) -> dict[str, Any]:
    return {"name": name, "status": status, "message": message, "metrics": metrics}


def _expected_product_years(args: argparse.Namespace) -> tuple[int, ...]:
    raw = getattr(args, "expected_years", None)
    if raw in (None, "", []):
        return ()
    values = raw if isinstance(raw, (list, tuple, set)) else str(raw).split(",")
    years: list[int] = []
    for value in values:
        text = str(value).strip()
        if not text:
            continue
        years.append(int(text))
    return tuple(sorted(set(years)))


def _validate_product_years(rows: list[dict[str, Any]], expected_years: tuple[int, ...] = ()) -> dict[str, Any]:
    years: set[int] = set()
    invalid_rows: list[dict[str, Any]] = []
    for row in rows:
        try:
            year = datetime.fromisoformat(str(row["acq_time"]).replace("Z", "+00:00")).year
        except Exception:
            invalid_rows.append({"line_no": row.get("_line_no"), "acq_time": row.get("acq_time")})
            continue
        years.add(year)
        if str(row.get("time_bucket")) != str(year):
            invalid_rows.append({"line_no": row.get("_line_no"), "time_bucket": row.get("time_bucket"), "expected": str(year)})

    if invalid_rows:
        return _check("product_years", "FAIL", "Some product rows have invalid year metadata.", invalid_rows=invalid_rows[:20])

    if expected_years:
        missing = [year for year in expected_years if year not in years]
        unexpected = sorted(year for year in years if year not in expected_years)
        if missing or unexpected:
            return _check(
                "product_years",
                "WARN",
                "Product year coverage differs from the explicitly expected years.",
                expected_years=list(expected_years),
                present_years=sorted(years),
                missing_years=missing,
                unexpected_years=unexpected,
            )
        return _check(
            "product_years",
            "PASS",
            "Product year coverage matches the explicitly expected years.",
            expected_years=list(expected_years),
            present_years=sorted(years),
        )

    return _check(
        "product_years",
        "PASS",
        "Product year metadata matches the current output rows.",
        present_years=sorted(years),
    )


def _summarize_product(rows: list[dict[str, Any]], assets: list[dict[str, Any]], checks: list[dict[str, Any]]) -> dict[str, Any]:
    summary = optical_quality._summarize(rows, assets, checks)
    years = sorted({datetime.fromisoformat(str(row["acq_time"]).replace("Z", "+00:00")).year for row in rows}) if rows else []
    rows_by_year = Counter(str(datetime.fromisoformat(str(row["acq_time"]).replace("Z", "+00:00")).year) for row in rows)
    summary["product_years"] = years
    summary["rows_by_year"] = dict(sorted(rows_by_year.items()))
    return summary


def run_quality_check(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir)
    target_crs = getattr(args, "target_crs", "EPSG:4326") or "EPSG:4326"
    expected_years = _expected_product_years(args)
    rows_path = run_dir / "index_rows.jsonl"
    checks: list[dict[str, Any]] = []

    if not rows_path.exists():
        checks.append(_check("index_rows", "FAIL", f"index_rows.jsonl not found under run_dir: {run_dir}"))
        return optical_quality._finalize_report({
            "status": "FAIL",
            "run_dir": str(run_dir),
            "target_crs": target_crs,
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "summary": _summarize_product([], [], checks),
            "checks": checks,
            "assets": [],
            "data_type": "product",
        }, run_dir, args)

    rows = optical_quality._load_jsonl(rows_path)
    if not rows:
        checks.append(_check("index_rows", "FAIL", "index_rows.jsonl is empty."))
        return optical_quality._finalize_report({
            "status": "FAIL",
            "run_dir": str(run_dir),
            "target_crs": target_crs,
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "summary": _summarize_product([], [], checks),
            "checks": checks,
            "assets": [],
            "data_type": "product",
        }, run_dir, args)

    checks.append(_check("index_rows", "PASS", "index_rows.jsonl was loaded.", row_count=len(rows), path=str(rows_path)))
    checks.append(optical_quality._validate_required_fields(rows))
    assets: list[dict[str, Any]] = []
    if not any(check["name"] == "index_schema" and check["status"] == "FAIL" for check in checks):
        checks.append(_validate_product_years(rows, expected_years=expected_years))
        checks.append(optical_quality._validate_cell_bboxes(rows))
        checks.append(optical_quality._validate_duplicates(rows))
        asset_checks, assets = optical_quality._validate_assets(rows, target_crs)
        checks.extend(asset_checks)

    report = {
        "status": optical_quality._overall_status(checks),
        "run_dir": str(run_dir.resolve()),
        "target_crs": target_crs,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "summary": _summarize_product(rows, assets, checks),
        "checks": checks,
        "assets": assets,
        "data_type": "product",
    }
    return optical_quality._finalize_report(report, run_dir, args)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run quality checks for product partition output")
    parser.add_argument("--run-dir", required=True, help="Partition run directory containing index_rows.jsonl")
    parser.add_argument("--target-crs", default="EPSG:4326", help="Expected CRS for standardized COG assets")
    parser.add_argument("--expected-years", default="", help="Optional comma-separated product years required for completeness checks")
    parser.add_argument("--output", default="", help="Optional quality report JSON path")
    return parser.parse_args()


def main() -> None:
    print(json.dumps(run_quality_check(parse_args()), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
