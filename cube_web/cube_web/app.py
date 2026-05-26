from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
import time
from types import SimpleNamespace
from uuid import uuid4
from pathlib import Path
from html import escape

from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.responses import JSONResponse
from fastapi.responses import Response
from cube_web.routes.partition import create_partition_router
from cube_web.routes.sdk import create_sdk_router
from cube_web.schemas import (
    PartitionDemoRequest,
    PartitionRetryRequest,
    QualityHistoryRequest,
    QualityLatestRequest,
    QualityReportRequest,
    QualityRunRequest,
    payload_from_model,
)
from cube_web.services.partition_service import PartitionService, build_partition_registry
from grid_core.sdk import CubeEncoderSDK
from grid_core.app.core.exceptions import GridCoreError, NotImplementedCapabilityError, ValidationError

try:
    from cube_split.quality.optical_quality import run_quality_check as run_optical_quality_check
except ModuleNotFoundError:  # pragma: no cover - cube_web can run with SDK-only routes.
    run_optical_quality_check = None

try:
    from cube_split.quality.product_quality import run_quality_check as run_product_quality_check
except ModuleNotFoundError:  # pragma: no cover - cube_web can run with SDK-only routes.
    run_product_quality_check = None

WEB_DIR = Path(__file__).resolve().parent / "web"
STATIC_MEDIA_TYPES = {
    ".css": "text/css",
    ".js": "application/javascript",
}

# Importing the SDK here makes cube_web explicitly depend on the installed
# cube_encoder package instead of only depending on its HTTP API shape.
ENCODER_SDK_CLASS = CubeEncoderSDK

app = FastAPI(title="cube-web")
api_router = APIRouter(prefix="/v1", tags=["sdk-web"])
sdk = CubeEncoderSDK()


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _quality_run_dirs(data_type: str) -> list[Path]:
    output_root = _repo_root() / "cube_split" / "data" / "ray_output"
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


def _allowed_quality_roots() -> list[Path]:
    return [
        (_repo_root() / "cube_split" / "data" / "ray_output").resolve(),
        (Path("/tmp") / "cube_web_partition_demo").resolve(),
    ]


def _resolve_quality_run_dir(run_dir_text: str) -> Path:
    run_dir = Path(run_dir_text).expanduser().resolve()
    for root in _allowed_quality_roots():
        if run_dir == root or root in run_dir.parents:
            return run_dir
    roots = ", ".join(str(root) for root in _allowed_quality_roots())
    raise HTTPException(status_code=403, detail=f"run_dir must be under one of: {roots}")


def _optical_quality_run_dirs() -> list[Path]:
    return _quality_run_dirs("optical")


def _latest_optical_quality_run_dir() -> str:
    candidates = _optical_quality_run_dirs()
    if not candidates:
        output_root = _repo_root() / "cube_split" / "data" / "ray_output"
        raise RuntimeError(f"No optical partition run directories found under: {output_root}")
    return str(candidates[0])


def _latest_product_quality_run_dir() -> str:
    candidates = _quality_run_dirs("product")
    if not candidates:
        output_root = _repo_root() / "cube_split" / "data" / "ray_output"
        raise RuntimeError(f"No product partition run directories found under: {output_root}")
    return str(candidates[0])


def _quality_args(run_dir: str, payload: dict | None = None):
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


def _quality_history_record(run_dir: Path, report: dict, data_type: str = "optical") -> dict:
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


def _read_quality_history_record(run_dir: Path, data_type: str = "optical") -> dict | None:
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
    return _quality_history_record(run_dir, report, data_type=data_type)


def _read_quality_report(run_dir: Path, data_type: str = "optical") -> dict | None:
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


def _wrap_pdf_line(text: str, width: int = 96) -> list[str]:
    text = re.sub(r"\s+", " ", str(text)).strip()
    if not text:
        return [""]
    lines: list[str] = []
    while len(text) > width:
        split_at = text.rfind(" ", 0, width + 1)
        if split_at <= 0:
            split_at = width
        lines.append(text[:split_at])
        text = text[split_at:].strip()
    lines.append(text)
    return lines


def _quality_report_pdf_lines(report: dict, data_type: str) -> list[str]:
    summary = report.get("summary", {}) or {}
    data_type_text = "数据产品" if data_type == "product" else "光学遥感"
    lines = [
        "质检报告",
        "",
        "报告名称：质检报告",
        f"数据类型：{data_type_text}",
        f"质检状态：{report.get('status', 'UNKNOWN')}",
        f"目标参考系统：{report.get('target_crs', '-')}",
        f"生成时间：{report.get('generated_at', '-')}",
        f"批次目录：{report.get('run_dir', '-')}",
        "",
        "质检概要",
        f"- 索引行数：{summary.get('index_rows', 0)}",
        f"- 资产数量：{summary.get('asset_count', 0)}",
        f"- 通过项：{summary.get('passed_checks', 0)}",
        f"- 告警项：{summary.get('warning_checks', 0)}",
        f"- 失败项：{summary.get('failed_checks', 0)}",
    ]
    if summary.get("distinct_space_codes") is not None:
        lines.append(f"- 空间格网数：{summary.get('distinct_space_codes')}")
    if summary.get("distinct_st_codes") is not None:
        lines.append(f"- 时空编码数：{summary.get('distinct_st_codes')}")

    rows_by_band = summary.get("rows_by_band") or {}
    rows_by_year = summary.get("rows_by_year") or {}
    if rows_by_band:
        lines.extend(["", "波段行数"])
        lines.extend(f"- {band}: {value}" for band, value in sorted(rows_by_band.items()))
    if rows_by_year:
        lines.extend(["", "年份行数"])
        lines.extend(f"- {year}: {value}" for year, value in sorted(rows_by_year.items()))

    lines.extend(["", "检查项"])
    for check in report.get("checks", []) or []:
        lines.append(f"- [{check.get('status', 'UNKNOWN')}] {check.get('name', '-')}: {check.get('message', '')}")
        metrics = check.get("metrics") or {}
        for key, value in list(metrics.items())[:8]:
            if isinstance(value, (list, dict)):
                value = json.dumps(value, ensure_ascii=False)
            lines.extend(f"  {wrapped}" for wrapped in _wrap_pdf_line(f"{key}: {value}", width=90))

    assets = report.get("assets", []) or []
    if assets:
        lines.extend(["", "资产抽查"])
        for asset in assets[:12]:
            lines.append(f"- {Path(str(asset.get('path', '-'))).name} | 参考系统：{asset.get('crs', '-')}")
    return lines


def _quality_report_html(lines: list[str]) -> str:
    body_parts: list[str] = []
    for line in lines:
        if not line:
            body_parts.append("<div class='spacer'></div>")
        elif line == "质检报告":
            body_parts.append(f"<div class='title'>{escape(line)}</div>")
        elif line in {"质检概要", "波段行数", "年份行数", "检查项", "资产抽查"}:
            body_parts.append(f"<h2>{escape(line)}</h2>")
        else:
            body_parts.append(f"<p>{escape(line)}</p>")
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    @page {{ size: A4; margin: 18mm 16mm; }}
    body {{
      font-family: "Noto Sans CJK SC", "Source Han Sans SC", "WenQuanYi Zen Hei", sans-serif;
      color: #1f2937;
      font-size: 11pt;
      line-height: 1.55;
    }}
    .title {{
      font-size: 24pt;
      font-weight: 700;
      margin: 0 0 14pt;
      color: #12395b;
      border-bottom: 2pt solid #2d5f8a;
      padding-bottom: 8pt;
    }}
    h2 {{
      font-size: 14pt;
      margin: 14pt 0 7pt;
      color: #12395b;
    }}
    p {{
      margin: 3pt 0;
      word-break: break-all;
    }}
    .spacer {{ height: 6pt; }}
  </style>
</head>
<body>
  {''.join(body_parts)}
</body>
</html>"""


def _build_quality_report_pdf(lines: list[str]) -> bytes:
    libreoffice = shutil.which("libreoffice")
    if not libreoffice:
        raise HTTPException(status_code=500, detail="LibreOffice is required for PDF export")
    with tempfile.TemporaryDirectory(prefix="cube-web-quality-pdf-") as tmp:
        tmp_dir = Path(tmp)
        html_path = tmp_dir / "quality_report.html"
        profile_dir = tmp_dir / "lo-profile"
        runtime_dir = tmp_dir / "runtime"
        profile_dir.mkdir()
        runtime_dir.mkdir()
        html_path.write_text(_quality_report_html(lines), encoding="utf-8")
        env = os.environ.copy()
        env.update({"HOME": str(tmp_dir), "XDG_RUNTIME_DIR": str(runtime_dir)})
        result = subprocess.run(
            [
                libreoffice,
                "--headless",
                f"-env:UserInstallation=file://{profile_dir}",
                "--convert-to",
                "pdf:writer_web_pdf_Export",
                "--outdir",
                str(tmp_dir),
                str(html_path),
            ],
            cwd=tmp_dir,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=30,
            check=False,
        )
        pdf_path = tmp_dir / "quality_report.pdf"
        if result.returncode != 0 or not pdf_path.exists():
            detail = (result.stderr or result.stdout or "PDF conversion failed").strip()
            raise HTTPException(status_code=500, detail=detail)
        return pdf_path.read_bytes()


def _quality_report_pdf_response(report: dict, data_type: str) -> Response:
    pdf = _build_quality_report_pdf(_quality_report_pdf_lines(report, data_type))
    filename = f"quality-report-{data_type}-{Path(str(report.get('run_dir', 'run'))).name or 'run'}.pdf"
    return Response(
        content=pdf,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _demo_run_dir(name: str) -> Path:
    run_dir = Path("/tmp") / "cube_web_partition_demo" / name / f"{time.strftime('run_%Y%m%d_%H%M%S')}_{time.perf_counter_ns()}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _demo_task_metadata(execution_engine: str) -> dict[str, str | None]:
    return {
        "demo_task_id": f"demo-{uuid4().hex[:12]}",
        "execution_engine": execution_engine,
        "ray_task_id": None,
    }


def _optical_demo_input_dir() -> Path:
    return _repo_root() / "cube_split" / "data" / "optocal"


def _resolve_optical_demo_source(source_uri: str, input_dir: Path) -> Path:
    source_path = Path(str(source_uri or "").strip())
    if not source_path:
        raise ValueError("selected_assets[].source_uri is required")
    if source_path.is_absolute():
        resolved = source_path.resolve()
    else:
        resolved = (input_dir / source_path).resolve()
    input_root = input_dir.resolve()
    if input_root != resolved and input_root not in resolved.parents:
        raise ValueError(f"Optical demo asset is outside input_dir: {source_uri}")
    if not resolved.exists() or resolved.suffix.lower() not in {".tif", ".tiff"}:
        raise FileNotFoundError(f"Optical demo asset not found: {resolved}")
    return resolved


def _selected_optical_manifest_assets(payload: dict, input_dir: Path) -> list[dict]:
    selected_assets = payload.get("selected_assets") or []
    if not selected_assets:
        return []
    if not isinstance(selected_assets, list):
        raise ValueError("selected_assets must be an array")

    manifest_assets: list[dict] = []
    for idx, asset in enumerate(selected_assets, start=1):
        if not isinstance(asset, dict):
            raise ValueError(f"selected_assets[{idx}] must be an object")
        source = _resolve_optical_demo_source(str(asset.get("source_uri") or ""), input_dir)
        manifest_assets.append(
            {
                "data_type": "optical",
                "source_uri": str(source),
                "scene_id": str(asset.get("scene_id") or source.stem),
                "acq_time": str(asset.get("acq_time") or "1970-01-01T00:00:00Z"),
                "bands": asset.get("bands") or ([asset["band"]] if asset.get("band") else [source.stem]),
                "corners": asset.get("corners"),
                "sensor": str(asset.get("sensor") or "optical_mosaic"),
                "product_family": str(asset.get("product_family") or "other"),
            }
        )
    return manifest_assets


def _int_payload_value(payload: dict, key: str, default: int) -> int:
    try:
        return int(payload.get(key, default) or default)
    except (TypeError, ValueError):
        raise ValueError(f"{key} must be an integer") from None


def _warn_checks_from_result(result: dict) -> list[dict]:
    report = result.get("quality_report") if isinstance(result, dict) else None
    if not isinstance(report, dict):
        return []
    checks = report.get("checks") or []
    if not isinstance(checks, list):
        return []
    return [check for check in checks if isinstance(check, dict) and check.get("status") == "WARN"]


def _warning_asset_paths(checks: list[dict]) -> set[str]:
    paths: set[str] = set()
    for check in checks:
        metrics = check.get("metrics") or {}
        if not isinstance(metrics, dict):
            continue
        for item in metrics.get("zero_assets") or []:
            if isinstance(item, dict) and item.get("path"):
                paths.add(str(item["path"]))
        for item in metrics.get("duplicates") or []:
            if isinstance(item, dict):
                paths.update(str(path) for path in item.get("asset_paths") or [] if path)
    return paths


def _asset_matches_warning_path(asset: dict, warning_path: str) -> bool:
    source_uri = str(asset.get("source_uri") or "")
    if not source_uri:
        return False
    warning = Path(warning_path)
    source = Path(source_uri)
    if source_uri == warning_path or source.name == warning.name:
        return True
    if warning.stem == f"{source.stem}_cog" and warning.suffix.lower() == source.suffix.lower():
        return True
    warning_parts = warning.parts
    source_parts = source.parts
    return len(source_parts) <= len(warning_parts) and tuple(warning_parts[-len(source_parts) :]) == tuple(source_parts)


def _retry_payload_for_warning_assets(payload: dict, warning_paths: set[str]) -> tuple[dict, int]:
    selected_assets = payload.get("selected_assets") or []
    if not warning_paths or not isinstance(selected_assets, list) or not selected_assets:
        return dict(payload), 0
    retry_assets = [
        asset
        for asset in selected_assets
        if isinstance(asset, dict) and any(_asset_matches_warning_path(asset, warning_path) for warning_path in warning_paths)
    ]
    if not retry_assets:
        return dict(payload), 0
    retry_payload = dict(payload)
    retry_payload["selected_assets"] = retry_assets
    return retry_payload, len(retry_assets)


def _run_optical_partition_retry(payload: dict | None = None) -> dict:
    payload = payload or {}
    request = payload.get("request") or {}
    if not isinstance(request, dict):
        raise ValueError("request must be an object")
    last_result = payload.get("last_result") or {}
    if not isinstance(last_result, dict):
        raise ValueError("last_result must be an object")

    request_payload = request.get("payload") or {}
    if not isinstance(request_payload, dict):
        raise ValueError("request.payload must be an object")
    warn_checks = _warn_checks_from_result(last_result)
    warning_paths = _warning_asset_paths(warn_checks)
    retry_payload, retried_asset_count = _retry_payload_for_warning_assets(request_payload, warning_paths)
    result = _run_optical_partition_from_payload(retry_payload, mode="partition_retry")
    result["retry"] = {
        "strategy": "warning_assets" if retried_asset_count else "full_request",
        "warning_check_names": [str(check.get("name")) for check in warn_checks],
        "warning_asset_count": len(warning_paths),
        "retried_asset_count": retried_asset_count,
    }
    return result


def _run_carbon_partition_demo() -> dict:
    from cube_split.partition.carbon import CarbonPartitionConfig, CarbonSatellitePartitionService

    sample = _repo_root() / "cube_split" / "oco2_LtCO2_201231_B11014Ar_220729012824s(1).nc4"
    if not sample.exists():
        raise RuntimeError(f"Carbon demo data not found: {sample}")

    root = _demo_run_dir("carbon")
    input_dir = root / "input"
    output_dir = root / "output"
    input_dir.mkdir(parents=True)
    (input_dir / sample.name).symlink_to(sample)
    workers = 4
    config = CarbonPartitionConfig(
        grid_type="geohash",
        grid_level=7,
        max_observations=1000,
        partition_chunk_size=250,
        partition_backend="process",
    )
    start = time.perf_counter()
    result = CarbonSatellitePartitionService().run(input_dir=input_dir, output_dir=output_dir, config=config, workers=workers)
    elapsed = time.perf_counter() - start
    space_codes: set[str] = set()
    quality_counts: dict[str, int] = {}
    with result.rows_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            row = json.loads(line)
            space_codes.add(row["space_code"])
            quality = str(row.get("quality_flag"))
            quality_counts[quality] = quality_counts.get(quality, 0) + 1
    return {
        "status": "completed",
        "data_type": "carbon_satellite",
        **_demo_task_metadata("local-process"),
        "demo_source": sample.name,
        "rows": result.total_rows,
        "distinct_space_codes": len(space_codes),
        "quality_counts": quality_counts,
        "elapsed_sec": round(elapsed, 3),
        "rows_per_sec": round(result.total_rows / elapsed, 1) if elapsed > 0 else 0,
        "grid_type": config.grid_type,
        "grid_level": config.grid_level,
        "workers": workers,
        "partition_backend": config.partition_backend,
        "output_path": str(result.rows_path),
    }


def _run_carbon_partition_retry(payload: dict | None = None) -> dict:
    result = _run_carbon_partition_demo()
    result["mode"] = "partition_retry"
    result["retry"] = {
        "strategy": "full_request",
        "warning_check_names": [],
        "warning_asset_count": 0,
        "retried_asset_count": 0,
    }
    return result


def _run_product_partition_demo(payload: dict | None = None, mode: str = "partition_demo") -> dict:
    from cube_split.jobs.product_partition_job import run_product_partition

    payload = payload or {}
    root = _demo_run_dir("product")
    input_dir = Path(str(payload.get("input_dir") or (_repo_root() / "cube_split" / "data" / "product"))).expanduser().resolve()
    if not input_dir.exists():
        raise FileNotFoundError(f"Product demo input_dir not found: {input_dir}")

    grid_type = str(payload.get("grid_type") or "geohash").lower()
    if grid_type not in {"geohash", "mgrs", "isea4h"}:
        raise ValueError("grid_type must be one of: geohash, mgrs, isea4h")
    grid_level = _int_payload_value(payload, "grid_level", 5)
    if grid_level <= 0:
        raise ValueError("grid_level must be greater than 0")

    args = SimpleNamespace(
        input_dir=str(input_dir),
        output_dir=str(root / "output"),
        cog_input_dir=str(root / "cog"),
        target_crs=str(payload.get("target_crs") or "EPSG:4326"),
        grid_type=grid_type,
        grid_level=grid_level,
        cover_mode=str(payload.get("cover_mode") or "intersect"),
        max_cells_per_asset=_int_payload_value(payload, "max_cells_per_asset", 20000),
        partition_prefix_len=_int_payload_value(payload, "partition_prefix_len", 3),
        cog_overwrite=True,
        cog_workers=_int_payload_value(payload, "cog_workers", 2),
        partition_workers=_int_payload_value(payload, "partition_workers", 0),
        sample_mean=bool(payload.get("sample_mean", False)),
    )
    result = run_product_partition(args)
    result["mode"] = mode
    result["output_path"] = result.get("rows_path")
    result["workers"] = args.partition_workers
    result["execution_engine"] = "thread"
    if run_product_quality_check is not None:
        quality_args = _quality_args(str(result["run_dir"]), {"target_crs": args.target_crs})
        quality_report = run_product_quality_check(quality_args)
        result["quality_status"] = quality_report.get("status")
        result["quality_report"] = quality_report
        result["quality_report_path"] = str(Path(result["run_dir"]) / "quality_report.json")
    return result


def _run_product_partition_retry(payload: dict | None = None) -> dict:
    request = (payload or {}).get("request") or {}
    request_payload = request.get("payload") if isinstance(request, dict) else {}
    if not isinstance(request_payload, dict):
        request_payload = {}
    result = _run_product_partition_demo(request_payload, mode="partition_retry")
    result["retry"] = {
        "strategy": "full_request",
        "warning_check_names": [],
        "warning_asset_count": 0,
        "retried_asset_count": 0,
    }
    return result


def _run_optical_partition_from_payload(payload: dict | None = None, mode: str = "partition_demo") -> dict:
    from cube_split.jobs.ray_logical_partition_job import run_logical_partition

    payload = payload or {}
    input_dir = Path(str(payload.get("input_dir") or _optical_demo_input_dir())).expanduser().resolve()
    if not input_dir.exists():
        raise FileNotFoundError(f"Optical demo input_dir not found: {input_dir}")

    root = _demo_run_dir("optical")
    output_root = root / "output"
    manifest_path = Path(str(payload.get("manifest_path") or "")).expanduser()
    manifest_assets = _selected_optical_manifest_assets(payload, input_dir)
    if manifest_assets:
        manifest_path = root / "selected_assets_manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "batch_id": payload.get("batch_id") or "frontend-optical-demo",
                    "batch_name": payload.get("batch_name") or "frontend optical demo",
                    "data_type": "optical",
                    "assets": manifest_assets,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    elif not str(manifest_path):
        default_manifest = input_dir / "manifest.json"
        manifest_path = default_manifest if default_manifest.exists() else Path("")

    grid_type = str(payload.get("grid_type") or "geohash").lower()
    if grid_type not in {"geohash", "mgrs", "isea4h"}:
        raise ValueError("grid_type must be one of: geohash, mgrs, isea4h")
    grid_level = _int_payload_value(payload, "grid_level", 5)
    if grid_level <= 0:
        raise ValueError("grid_level must be greater than 0")

    args = SimpleNamespace(
        input_dir=str(input_dir),
        manifest_path=(str(manifest_path.resolve()) if str(manifest_path) else ""),
        product_family=str(payload.get("product_family") or "auto"),
        output_dir=str(output_root),
        cog_input_dir=str(root / "cog"),
        cog_overwrite=True,
        cog_workers=_int_payload_value(payload, "cog_workers", 2),
        cog_compress=str(payload.get("cog_compress") or "LZW"),
        cog_predictor=_int_payload_value(payload, "cog_predictor", 2),
        cog_level=_int_payload_value(payload, "cog_level", 0),
        cog_num_threads=str(payload.get("cog_num_threads") or "ALL_CPUS"),
        target_crs=str(payload.get("target_crs") or "EPSG:4326"),
        grid_type=grid_type,
        grid_level=grid_level,
        cover_mode=str(payload.get("cover_mode") or "intersect"),
        time_granularity=str(payload.get("time_granularity") or "day"),
        max_cells_per_asset=_int_payload_value(payload, "max_cells_per_asset", 20000),
        ray_parallelism=_int_payload_value(payload, "ray_parallelism", 0),
        ray_address=str(payload.get("ray_address") or os.environ.get("CUBE_WEB_RAY_ADDRESS", "")),
        chunk_size=_int_payload_value(payload, "chunk_size", 0),
        partition_backend=str(payload.get("partition_backend") or "ray"),
        partition_prefix_len=_int_payload_value(payload, "partition_prefix_len", 3),
        timing_mode=False,
        skip_verify=False,
        sample_mean=bool(payload.get("sample_mean", False)),
    )
    report = run_logical_partition(args)
    run_dir = Path(report["run_dir"])
    rows_path = run_dir / "index_rows.jsonl"

    response = {
        "status": "completed",
        "mode": mode,
        "data_type": "optical",
        **_demo_task_metadata(str(report.get("execution_engine") or args.partition_backend)),
        "demo_source": str(input_dir),
        "batch_id": payload.get("batch_id") or "",
        "batch_name": payload.get("batch_name") or "",
        "run_dir": str(run_dir),
        "rows_path": str(rows_path),
        "output_path": str(rows_path),
        "rows": int(report.get("total_index_rows", 0)),
        "workers": report.get("ray_parallelism"),
        **report,
    }
    if run_optical_quality_check is not None:
        quality_args = _quality_args(str(run_dir), {"target_crs": args.target_crs})
        quality_report = run_optical_quality_check(quality_args)
        response["quality_status"] = quality_report.get("status")
        response["quality_report"] = quality_report
        response["quality_report_path"] = str(run_dir / "quality_report.json")
    return response


def _run_optical_partition_demo(payload: dict | None = None) -> dict:
    return _run_optical_partition_from_payload(payload, mode="partition_demo")


def _run_optical_partition_test(payload: dict | None = None) -> dict:
    return _run_optical_partition_from_payload(payload, mode="partition_test_no_ingest")


def _resolve_web_file(path_name: str) -> Path:
    candidate = WEB_DIR / path_name
    if candidate.exists() and candidate.is_file():
        return candidate

    if "." not in path_name:
        html_candidate = WEB_DIR / f"{path_name}.html"
        if html_candidate.exists() and html_candidate.is_file():
            return html_candidate
        index_candidate = WEB_DIR / "index.html"
        if index_candidate.exists() and index_candidate.is_file():
            return index_candidate

    if path_name in {"partition.html", "quality.html", "encoding.html", "门户首页.html"}:
        index_candidate = WEB_DIR / "index.html"
        if index_candidate.exists() and index_candidate.is_file():
            return index_candidate

    raise HTTPException(status_code=404, detail=f"Page not found: {path_name}")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.exception_handler(GridCoreError)
async def handle_grid_core_error(_: Request, exc: GridCoreError):
    status_code = 400
    if isinstance(exc, ValidationError):
        status_code = 422
    elif isinstance(exc, NotImplementedCapabilityError):
        status_code = 501
    return JSONResponse(status_code=status_code, content={"error": {"code": exc.code, "message": exc.message}})


def partition_carbon_demo() -> dict:
    try:
        return _run_carbon_partition_demo()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def partition_carbon_retry(payload: PartitionRetryRequest | dict | None = None) -> dict:
    try:
        return _run_carbon_partition_retry(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def partition_product_demo(payload: PartitionDemoRequest | dict | None = None) -> dict:
    try:
        return _run_product_partition_demo(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def partition_product_retry(payload: PartitionRetryRequest | dict | None = None) -> dict:
    try:
        return _run_product_partition_retry(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def partition_radar_demo(payload: PartitionDemoRequest | dict | None = None) -> dict:
    raise HTTPException(status_code=501, detail="Radar partition demo is not implemented")


def partition_radar_retry(payload: PartitionRetryRequest | dict | None = None) -> dict:
    raise HTTPException(status_code=501, detail="Radar partition retry is not implemented")


def partition_optical_demo(payload: PartitionDemoRequest | dict | None = None) -> dict:
    try:
        return _run_optical_partition_demo(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def partition_optical_test(payload: PartitionDemoRequest | dict | None = None) -> dict:
    try:
        return _run_optical_partition_test(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def partition_optical_retry(payload: PartitionRetryRequest | dict | None = None) -> dict:
    try:
        return _run_optical_partition_retry(payload_from_model(payload) if payload is not None else None)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@api_router.post("/quality/optical/run")
def quality_optical_run(payload: QualityRunRequest) -> dict:
    payload = payload_from_model(payload)
    if run_optical_quality_check is None:
        raise HTTPException(status_code=500, detail="cube_split quality module is not available")
    run_dir_text = str(payload.get("run_dir", "")).strip()
    if not run_dir_text:
        raise HTTPException(status_code=422, detail="run_dir is required")
    run_dir = str(_resolve_quality_run_dir(run_dir_text))
    args = _quality_args(run_dir, payload)
    try:
        return run_optical_quality_check(args)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@api_router.post("/quality/optical/latest")
def quality_optical_latest(payload: QualityLatestRequest | None = None) -> dict:
    payload = payload_from_model(payload)
    run_dir = _latest_optical_quality_run_dir()
    cached_report = _read_quality_report(Path(run_dir), data_type="optical")
    if cached_report is not None:
        return cached_report
    if run_optical_quality_check is None:
        raise HTTPException(status_code=500, detail="cube_split quality module is not available")
    args = _quality_args(run_dir, payload)
    try:
        report = run_optical_quality_check(args)
        report["run_dir"] = run_dir
        return report
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@api_router.post("/quality/optical/report")
def quality_optical_report(payload: QualityReportRequest) -> dict:
    payload = payload_from_model(payload)
    run_dir_text = str(payload.get("run_dir", "")).strip()
    if not run_dir_text:
        raise HTTPException(status_code=422, detail="run_dir is required")
    run_dir = _resolve_quality_run_dir(run_dir_text)
    report = _read_quality_report(run_dir, data_type="optical")
    if report is None:
        raise HTTPException(status_code=404, detail=f"quality_report.json not found under run_dir: {run_dir}")
    return report


@api_router.post("/quality/optical/report/pdf")
def quality_optical_report_pdf(payload: QualityReportRequest) -> Response:
    report = quality_optical_report(payload)
    return _quality_report_pdf_response(report, data_type="optical")


@api_router.post("/quality/optical/history")
def quality_optical_history(payload: QualityHistoryRequest | None = None) -> dict:
    payload = payload_from_model(payload)
    try:
        limit = int(payload.get("limit", 20) or 20)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="limit must be an integer") from None
    if limit <= 0:
        raise HTTPException(status_code=422, detail="limit must be greater than 0")

    records: list[dict] = []
    for run_dir in _optical_quality_run_dirs():
        record = _read_quality_history_record(run_dir, data_type="optical")
        if record is None:
            continue
        records.append(record)
        if len(records) >= limit:
            break
    return {"records": records, "count": len(records)}


@api_router.post("/quality/product/run")
def quality_product_run(payload: QualityRunRequest) -> dict:
    payload = payload_from_model(payload)
    if run_product_quality_check is None:
        raise HTTPException(status_code=500, detail="cube_split product quality module is not available")
    run_dir_text = str(payload.get("run_dir", "")).strip()
    if not run_dir_text:
        raise HTTPException(status_code=422, detail="run_dir is required")
    run_dir = str(_resolve_quality_run_dir(run_dir_text))
    args = _quality_args(run_dir, payload)
    try:
        return run_product_quality_check(args)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@api_router.post("/quality/product/latest")
def quality_product_latest(payload: QualityLatestRequest | None = None) -> dict:
    payload = payload_from_model(payload)
    run_dir = _latest_product_quality_run_dir()
    cached_report = _read_quality_report(Path(run_dir), data_type="product")
    if cached_report is not None:
        return cached_report
    if run_product_quality_check is None:
        raise HTTPException(status_code=500, detail="cube_split product quality module is not available")
    args = _quality_args(run_dir, payload)
    try:
        report = run_product_quality_check(args)
        report["run_dir"] = run_dir
        return report
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@api_router.post("/quality/product/report")
def quality_product_report(payload: QualityReportRequest) -> dict:
    payload = payload_from_model(payload)
    run_dir_text = str(payload.get("run_dir", "")).strip()
    if not run_dir_text:
        raise HTTPException(status_code=422, detail="run_dir is required")
    run_dir = _resolve_quality_run_dir(run_dir_text)
    report = _read_quality_report(run_dir, data_type="product")
    if report is None:
        raise HTTPException(status_code=404, detail=f"quality_report.json not found under run_dir: {run_dir}")
    return report


@api_router.post("/quality/product/report/pdf")
def quality_product_report_pdf(payload: QualityReportRequest) -> Response:
    report = quality_product_report(payload)
    return _quality_report_pdf_response(report, data_type="product")


@api_router.post("/quality/product/history")
def quality_product_history(payload: QualityHistoryRequest | None = None) -> dict:
    payload = payload_from_model(payload)
    try:
        limit = int(payload.get("limit", 20) or 20)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="limit must be an integer") from None
    if limit <= 0:
        raise HTTPException(status_code=422, detail="limit must be greater than 0")

    records: list[dict] = []
    for run_dir in _quality_run_dirs("product"):
        record = _read_quality_history_record(run_dir, data_type="product")
        if record is None:
            continue
        records.append(record)
        if len(records) >= limit:
            break
    return {"records": records, "count": len(records)}


partition_service = PartitionService(
    build_partition_registry(
        optical_demo=partition_optical_demo,
        optical_test=partition_optical_test,
        optical_retry=partition_optical_retry,
        carbon_demo=lambda payload=None: partition_carbon_demo(),
        carbon_retry=partition_carbon_retry,
        product_demo=partition_product_demo,
        product_retry=partition_product_retry,
    )
)
api_router.include_router(create_sdk_router(sdk))
api_router.include_router(create_partition_router(partition_service))
app.include_router(api_router)


@app.get("/")
def home() -> FileResponse:
    return FileResponse(WEB_DIR / "index.html", media_type="text/html")


@app.get("/{path_name:path}")
def serve_web_asset(path_name: str) -> FileResponse:
    file_path = _resolve_web_file(path_name)
    media_type = STATIC_MEDIA_TYPES.get(file_path.suffix, "text/html")
    return FileResponse(file_path, media_type=media_type)
