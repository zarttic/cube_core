from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import rasterio

from cube_split.jobs.ray_partition_core import resolve_asset_source_path


REQUIRED_INDEX_FIELDS = {
    "scene_id",
    "band",
    "asset_path",
    "acq_time",
    "grid_type",
    "grid_level",
    "space_code",
    "st_code",
    "time_bucket",
    "cell_min_lon",
    "cell_min_lat",
    "cell_max_lon",
    "cell_max_lat",
    "window_col_off",
    "window_row_off",
    "window_width",
    "window_height",
}


def _check(name: str, status: str, message: str, **metrics: Any) -> dict[str, Any]:
    return {"name": name, "status": status, "message": message, "metrics": metrics}


def _overall_status(checks: list[dict[str, Any]]) -> str:
    if any(check["status"] == "FAIL" for check in checks):
        return "FAIL"
    if any(check["status"] == "WARN" for check in checks):
        return "WARN"
    return "PASS"


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            text = line.strip()
            if not text:
                continue
            row = json.loads(text)
            row["_line_no"] = line_no
            rows.append(row)
    return rows


def _quality_report_output_path(run_dir: Path, args: argparse.Namespace) -> Path:
    output_path = getattr(args, "output", "") or ""
    return Path(output_path) if output_path else run_dir / "quality_report.json"


def _finalize_report(report: dict[str, Any], run_dir: Path, args: argparse.Namespace) -> dict[str, Any]:
    out = _quality_report_output_path(run_dir, args)
    out.parent.mkdir(parents=True, exist_ok=True)
    report["report_path"] = str(out.resolve())
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def _time_bucket_from_acq_time(acq_time: str) -> str:
    dt = datetime.fromisoformat(acq_time.replace("Z", "+00:00")).astimezone(timezone.utc)
    return dt.strftime("%Y%m%d")


def _validate_required_fields(rows: list[dict[str, Any]]) -> dict[str, Any]:
    missing_rows: list[dict[str, Any]] = []
    for row in rows:
        missing = sorted(field for field in REQUIRED_INDEX_FIELDS if field not in row)
        if missing:
            missing_rows.append({"line_no": row.get("_line_no"), "missing": missing})
    if missing_rows:
        return _check(
            "index_schema",
            "FAIL",
            "Some index rows are missing required fields.",
            missing_rows=missing_rows[:20],
            missing_count=len(missing_rows),
        )
    return _check("index_schema", "PASS", "All index rows contain required fields.", required_fields=len(REQUIRED_INDEX_FIELDS))


def _validate_time_buckets(rows: list[dict[str, Any]]) -> dict[str, Any]:
    mismatches: list[dict[str, Any]] = []
    for row in rows:
        try:
            expected = _time_bucket_from_acq_time(str(row["acq_time"]))
        except Exception:
            mismatches.append({"line_no": row.get("_line_no"), "time_bucket": row.get("time_bucket"), "acq_time": row.get("acq_time")})
            continue
        if str(row["time_bucket"]) != expected:
            mismatches.append(
                {
                    "line_no": row.get("_line_no"),
                    "time_bucket": row.get("time_bucket"),
                    "expected": expected,
                    "acq_time": row.get("acq_time"),
                }
            )
    if mismatches:
        return _check("time_bucket", "FAIL", "Some time_bucket values do not match acq_time.", mismatches=mismatches[:20], mismatch_count=len(mismatches))
    return _check("time_bucket", "PASS", "All time_bucket values match acq_time.", checked_rows=len(rows))


def _validate_cell_bboxes(rows: list[dict[str, Any]]) -> dict[str, Any]:
    invalid: list[dict[str, Any]] = []
    for row in rows:
        try:
            min_lon = float(row["cell_min_lon"])
            min_lat = float(row["cell_min_lat"])
            max_lon = float(row["cell_max_lon"])
            max_lat = float(row["cell_max_lat"])
        except Exception:
            invalid.append({"line_no": row.get("_line_no"), "reason": "non_numeric"})
            continue
        if not (-180 <= min_lon <= 180 and -180 <= max_lon <= 180 and -90 <= min_lat <= 90 and -90 <= max_lat <= 90):
            invalid.append({"line_no": row.get("_line_no"), "reason": "out_of_wgs84_range"})
            continue
        if not (min_lon < max_lon and min_lat < max_lat):
            invalid.append({"line_no": row.get("_line_no"), "reason": "inverted_bbox"})
    if invalid:
        return _check("cell_bbox", "FAIL", "Some cell bounding boxes are invalid.", invalid_rows=invalid[:20], invalid_count=len(invalid))
    return _check("cell_bbox", "PASS", "All cell bounding boxes are valid WGS84 boxes.", checked_rows=len(rows))


def _asset_rows(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["asset_path"])].append(row)
    return grouped


def _validate_assets(rows: list[dict[str, Any]], target_crs: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    checks: list[dict[str, Any]] = []
    assets: list[dict[str, Any]] = []
    missing_assets: list[str] = []
    unreadable_assets: list[dict[str, str]] = []
    crs_mismatches: list[dict[str, str | None]] = []
    window_errors: list[dict[str, Any]] = []
    valid_pixel_stats: list[dict[str, Any]] = []

    for asset_path, asset_index_rows in _asset_rows(rows).items():
        local_asset_path = resolve_asset_source_path(asset_path)
        path = Path(local_asset_path)
        if not path.exists():
            missing_assets.append(asset_path)
            continue
        try:
            with rasterio.open(path) as ds:
                crs_text = ds.crs.to_string() if ds.crs else None
                assets.append(
                    {
                        "path": asset_path,
                        "crs": crs_text,
                        "width": ds.width,
                        "height": ds.height,
                        "count": ds.count,
                        "dtype": ds.dtypes[0] if ds.dtypes else None,
                        "bounds": [float(ds.bounds.left), float(ds.bounds.bottom), float(ds.bounds.right), float(ds.bounds.top)],
                    }
                )
                asset_is_entity_tile = any(row.get("partition_type") == "entity" for row in asset_index_rows)
                if crs_text != target_crs and not asset_is_entity_tile:
                    crs_mismatches.append({"path": asset_path, "crs": crs_text})

                sample = ds.read(1, masked=True, out_shape=(1, min(ds.height, 512), min(ds.width, 512)))
                valid_count = int(sample.count()) if hasattr(sample, "count") else int(sample.size)
                nonzero_count = int((sample != 0).sum())
                valid_pixel_stats.append(
                    {
                        "path": asset_path,
                        "sample_pixels": int(sample.size),
                        "valid_pixels": valid_count,
                        "nonzero_pixels": nonzero_count,
                    }
                )

                for row in asset_index_rows:
                    col_off = int(row["window_col_off"])
                    row_off = int(row["window_row_off"])
                    width = int(row["window_width"])
                    height = int(row["window_height"])
                    if width <= 0 or height <= 0 or col_off < 0 or row_off < 0 or col_off + width > ds.width or row_off + height > ds.height:
                        window_errors.append(
                            {
                                "line_no": row.get("_line_no"),
                                "asset_path": asset_path,
                                "window": [col_off, row_off, width, height],
                                "asset_size": [ds.width, ds.height],
                            }
                        )
        except Exception as exc:
            unreadable_assets.append({"path": asset_path, "error": str(exc)})

    if missing_assets or unreadable_assets:
        checks.append(
            _check(
                "asset_readability",
                "FAIL",
                "Some asset files are missing or unreadable.",
                missing_assets=missing_assets[:20],
                missing_count=len(missing_assets),
                unreadable_assets=unreadable_assets[:20],
                unreadable_count=len(unreadable_assets),
            )
        )
    else:
        checks.append(_check("asset_readability", "PASS", "All referenced assets can be opened.", asset_count=len(assets)))

    if crs_mismatches:
        checks.append(
            _check(
                "cog_crs",
                "FAIL",
                f"Some COG assets are not in {target_crs}.",
                target_crs=target_crs,
                mismatches=crs_mismatches[:20],
                mismatch_count=len(crs_mismatches),
            )
        )
    else:
        checks.append(_check("cog_crs", "PASS", f"All readable COG assets use {target_crs}.", target_crs=target_crs))

    if window_errors:
        checks.append(
            _check(
                "window_bounds",
                "FAIL",
                "Some index windows fall outside their COG dimensions.",
                invalid_windows=window_errors[:20],
                invalid_count=len(window_errors),
            )
        )
    else:
        checks.append(_check("window_bounds", "PASS", "All index windows are within COG dimensions.", checked_rows=len(rows)))

    zero_assets = [item for item in valid_pixel_stats if item["nonzero_pixels"] == 0]
    if zero_assets:
        checks.append(_check("pixel_sample", "WARN", "Some assets have zero-valued sample windows.", zero_assets=zero_assets[:20], zero_asset_count=len(zero_assets)))
    else:
        checks.append(_check("pixel_sample", "PASS", "Asset samples contain non-zero pixels.", sampled_assets=len(valid_pixel_stats)))

    return checks, assets


def _validate_duplicates(rows: list[dict[str, Any]]) -> dict[str, Any]:
    asset_keys = Counter((row["scene_id"], row["band"], row["asset_path"]) for row in rows)
    logical_keys: dict[tuple[Any, Any], set[str]] = defaultdict(set)
    for row in rows:
        if row.get("partition_type") == "entity":
            continue
        logical_keys[(row["scene_id"], row["band"])].add(str(row["asset_path"]))
    duplicate_logical = [
        {"scene_id": scene_id, "band": band, "asset_paths": sorted(paths)}
        for (scene_id, band), paths in logical_keys.items()
        if len(paths) > 1
    ]
    if duplicate_logical:
        return _check(
            "logical_duplicates",
            "WARN",
            "Some scene_id+band pairs have multiple source assets and may be merged during ingest.",
            duplicate_count=len(duplicate_logical),
            duplicates=duplicate_logical[:20],
        )
    return _check("logical_duplicates", "PASS", "No duplicate scene_id+band assets were detected.", asset_key_count=len(asset_keys))


def _summarize(rows: list[dict[str, Any]], assets: list[dict[str, Any]], checks: list[dict[str, Any]]) -> dict[str, Any]:
    rows_by_band = Counter(str(row["band"]) for row in rows)
    rows_by_time_bucket = Counter(str(row["time_bucket"]) for row in rows)
    return {
        "index_rows": len(rows),
        "asset_count": len(assets),
        "distinct_space_codes": len({row["space_code"] for row in rows}),
        "distinct_st_codes": len({row["st_code"] for row in rows}),
        "rows_by_band": dict(sorted(rows_by_band.items())),
        "rows_by_time_bucket": dict(sorted(rows_by_time_bucket.items())),
        "passed_checks": sum(1 for check in checks if check["status"] == "PASS"),
        "warning_checks": sum(1 for check in checks if check["status"] == "WARN"),
        "failed_checks": sum(1 for check in checks if check["status"] == "FAIL"),
    }


def run_quality_check(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir)
    target_crs = getattr(args, "target_crs", "EPSG:4326") or "EPSG:4326"
    rows_path = run_dir / "index_rows.jsonl"
    checks: list[dict[str, Any]] = []

    if not rows_path.exists():
        checks.append(_check("index_rows", "FAIL", f"index_rows.jsonl not found under run_dir: {run_dir}"))
        return _finalize_report({
            "status": "FAIL",
            "run_dir": str(run_dir),
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "summary": _summarize([], [], checks),
            "checks": checks,
            "assets": [],
        }, run_dir, args)

    rows = _load_jsonl(rows_path)
    if not rows:
        checks.append(_check("index_rows", "FAIL", "index_rows.jsonl is empty."))
        return _finalize_report({
            "status": "FAIL",
            "run_dir": str(run_dir),
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "summary": _summarize([], [], checks),
            "checks": checks,
            "assets": [],
        }, run_dir, args)

    checks.append(_check("index_rows", "PASS", "index_rows.jsonl was loaded.", row_count=len(rows), path=str(rows_path)))
    checks.append(_validate_required_fields(rows))
    if any(check["name"] == "index_schema" and check["status"] == "FAIL" for check in checks):
        assets: list[dict[str, Any]] = []
    else:
        checks.append(_validate_time_buckets(rows))
        checks.append(_validate_cell_bboxes(rows))
        checks.append(_validate_duplicates(rows))
        asset_checks, assets = _validate_assets(rows, target_crs)
        checks.extend(asset_checks)

    report = {
        "status": _overall_status(checks),
        "run_dir": str(run_dir.resolve()),
        "target_crs": target_crs,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "summary": _summarize(rows, assets, checks),
        "checks": checks,
        "assets": assets,
    }

    return _finalize_report(report, run_dir, args)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run quality checks for optical partition output")
    parser.add_argument("--run-dir", required=True, help="Partition run directory containing index_rows.jsonl")
    parser.add_argument("--target-crs", default="EPSG:4326", help="Expected CRS for standardized COG assets")
    parser.add_argument("--output", default="", help="Optional quality report JSON path")
    return parser.parse_args()


def main() -> None:
    report = run_quality_check(parse_args())
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
