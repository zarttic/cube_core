from __future__ import annotations

import json
import time
from pathlib import Path

from fastapi import HTTPException


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def quality_run_dirs(data_type: str) -> list[Path]:
    output_root = repo_root() / "cube_split" / "data" / "ray_output"
    candidates = []
    for path in output_root.glob("*/run_*"):
        if not path.is_dir() or not (path / "index_rows.jsonl").exists():
            continue
        parent_name = path.parent.name.lower()
        if data_type == "product":
            if parent_name.startswith("product"):
                candidates.append(path)
            continue
        if not parent_name.startswith("product"):
            candidates.append(path)
    return sorted(candidates, key=lambda path: path.stat().st_mtime, reverse=True)


def allowed_quality_roots() -> list[Path]:
    return [
        (repo_root() / "cube_split" / "data" / "ray_output").resolve(),
        (Path("/tmp") / "cube_web_partition_demo").resolve(),
    ]


def resolve_quality_run_dir(run_dir_text: str) -> Path:
    run_dir = Path(run_dir_text).expanduser().resolve()
    for root in allowed_quality_roots():
        if run_dir == root or root in run_dir.parents:
            return run_dir
    roots = ", ".join(str(root) for root in allowed_quality_roots())
    raise HTTPException(status_code=403, detail=f"run_dir must be under one of: {roots}")


def optical_quality_run_dirs() -> list[Path]:
    return quality_run_dirs("optical")


def latest_optical_quality_run_dir() -> str:
    candidates = optical_quality_run_dirs()
    if not candidates:
        output_root = repo_root() / "cube_split" / "data" / "ray_output"
        raise RuntimeError(f"No optical partition run directories found under: {output_root}")
    return str(candidates[0])


def latest_product_quality_run_dir() -> str:
    candidates = quality_run_dirs("product")
    if not candidates:
        output_root = repo_root() / "cube_split" / "data" / "ray_output"
        raise RuntimeError(f"No product partition run directories found under: {output_root}")
    return str(candidates[0])


def quality_args(run_dir: str, payload: dict | None = None):
    payload = payload or {}
    output = str(payload.get("output", "") or "")
    if not output:
        output = str(Path(run_dir) / "quality_report.json")
    return type(
        "QualityArgs",
        (),
        {
            "run_dir": run_dir,
            "target_crs": str(payload.get("target_crs", "EPSG:4326") or "EPSG:4326"),
            "output": output,
        },
    )()


def quality_history_record(run_dir: Path, report: dict, data_type: str = "optical") -> dict:
    dataset = run_dir.parent.name
    summary = report.get("summary", {})
    return {
        "run_dir": str(run_dir),
        "data_type": data_type,
        "dataset": dataset,
        "run_name": run_dir.name,
        "status": report.get("status", "UNKNOWN"),
        "target_crs": report.get("target_crs"),
        "generated_at": report.get("generated_at"),
        "modified_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(run_dir.stat().st_mtime)),
        "summary": {
            "index_rows": summary.get("index_rows", 0),
            "asset_count": summary.get("asset_count", 0),
            "passed_checks": summary.get("passed_checks", 0),
            "warning_checks": summary.get("warning_checks", 0),
            "failed_checks": summary.get("failed_checks", 0),
            "product_years": summary.get("product_years", []),
        },
    }


def read_quality_history_record(run_dir: Path, data_type: str = "optical") -> dict | None:
    report_path = run_dir / "quality_report.json"
    if not report_path.exists():
        return None
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "run_dir": str(run_dir),
            "data_type": data_type,
            "dataset": run_dir.parent.name,
            "run_name": run_dir.name,
            "status": "FAIL",
            "target_crs": None,
            "generated_at": None,
            "modified_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(report_path.stat().st_mtime)),
            "summary": {"index_rows": 0, "asset_count": 0, "passed_checks": 0, "warning_checks": 0, "failed_checks": 1},
            "error": "quality_report.json cannot be read",
        }
    return quality_history_record(run_dir, report, data_type=data_type)


def read_quality_report(run_dir: Path, data_type: str = "optical") -> dict | None:
    report_path = run_dir / "quality_report.json"
    if not report_path.exists():
        return None
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=500, detail=f"quality_report.json cannot be read: {exc}") from exc
    report["run_dir"] = str(run_dir)
    report.setdefault("data_type", data_type)
    return report
