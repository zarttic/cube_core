from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
import time
from uuid import uuid4
from pathlib import Path
from html import escape

from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.responses import JSONResponse
from fastapi.responses import Response
from grid_core.sdk import CubeEncoderSDK
from grid_core.app.core.exceptions import GridCoreError, NotImplementedCapabilityError, ValidationError
from grid_core.app.models.request import (
    BatchCodeToGeometryRequest,
    ChildrenRequest,
    CodeToGeometryRequest,
    CoverRequest,
    LocateRequest,
    NeighborsRequest,
    ParentRequest,
    STCodeBatchGenerateRequest,
    STCodeGenerateRequest,
    STCodeParseRequest,
)
from grid_core.app.models.response import (
    BatchGeometryResponse,
    ChildrenResponse,
    CoverResponse,
    GeometryResponse,
    LocateResponse,
    NeighborsResponse,
    ParentResponse,
    STCodeBatchGenerateResponse,
    STCodeGenerateResponse,
    STCodeParseResponse,
)

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


def _count_jsonl_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8") as fh:
        return sum(1 for _ in fh)


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


def _run_optical_partition_demo() -> dict:
    import ray

    from cube_split.jobs.ray_partition_core import (
        _group_tasks_for_local_processing,
        _prepare_task_rows_for_partitioning,
        build_grid_tasks_driver,
        build_manifest,
        convert_assets_to_cog,
    )

    sample = _repo_root() / "cube_split" / "data" / "optical_demo" / "LC08_L2SP_120030_20260204_20260217_02_T1.tar"
    if not sample.exists():
        raise RuntimeError(f"Optical demo data not found: {sample}")

    root = _demo_run_dir("optical")
    input_dir = root / "input"
    cog_dir = root / "cog"
    output_dir = root / "output"
    input_dir.mkdir(parents=True)
    output_dir.mkdir(parents=True)

    selected_suffixes = ("_SR_B2.TIF", "_SR_B3.TIF", "_SR_B4.TIF")
    with tarfile.open(sample, "r") as archive:
        for member in archive.getmembers():
            name = Path(member.name).name
            if not member.isfile() or not name.endswith(selected_suffixes):
                continue
            src = archive.extractfile(member)
            if src is None:
                continue
            with (input_dir / name).open("wb") as dst:
                shutil.copyfileobj(src, dst)

    total_start = time.perf_counter()
    assets = build_manifest(input_dir)
    if not assets:
        raise RuntimeError(f"No optical TIF assets extracted from demo tar: {sample}")
    cog_start = time.perf_counter()
    cog_assets = convert_assets_to_cog(assets, cog_input_dir=cog_dir, overwrite=True, workers=2)
    cog_elapsed = time.perf_counter() - cog_start
    grid_tasks = build_grid_tasks_driver(
        assets=cog_assets,
        grid_type="geohash",
        grid_level=9,
        cover_mode="intersect",
        max_cells_per_asset=20000,
    )
    task_rows = _prepare_task_rows_for_partitioning(grid_tasks, partition_prefix_len=3, time_granularity="day")
    grouped = _group_tasks_for_local_processing(task_rows)
    partition_start = time.perf_counter()
    rows: list[dict] = []
    ray_init_start = time.perf_counter()
    ray.init(ignore_reinit_error=True, include_dashboard=False, logging_level="ERROR")
    ray_init_elapsed = time.perf_counter() - ray_init_start

    @ray.remote
    def process_group(group: list[dict]) -> list[dict]:
        from cube_split.jobs.ray_partition_core import _process_local_task_group

        return _process_local_task_group(group, "day", include_sample_mean=False)

    futures = [process_group.remote(group) for group in grouped]
    ray_task_ids = [str(future) for future in futures]
    try:
        for part in ray.get(futures):
            rows.extend(part)
    finally:
        ray.shutdown()
    partition_elapsed = time.perf_counter() - partition_start
    rows_path = output_dir / "index_rows.jsonl"
    with rows_path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    return {
        "status": "completed",
        "data_type": "optical",
        **_demo_task_metadata("ray"),
        "demo_source": sample.name,
        "asset_count": len(cog_assets),
        "grid_task_count": len(grid_tasks),
        "rows": _count_jsonl_rows(rows_path),
        "cog_elapsed_sec": round(cog_elapsed, 3),
        "partition_elapsed_sec": round(partition_elapsed, 3),
        "total_elapsed_sec": round(time.perf_counter() - total_start, 3),
        "grid_type": "geohash",
        "grid_level": 9,
        "workers": len(grouped),
        "ray_init_elapsed_sec": round(ray_init_elapsed, 3),
        "ray_task_id": ray_task_ids[0] if ray_task_ids else None,
        "ray_task_ids": ray_task_ids,
        "output_path": str(rows_path),
    }


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


@api_router.post("/grid/locate", response_model=LocateResponse)
def locate(req: LocateRequest) -> LocateResponse:
    cell = sdk.locate(grid_type=req.grid_type, level=req.level, point=req.point)
    return LocateResponse(cell=cell)


@api_router.post("/grid/cover", response_model=CoverResponse)
def cover(req: CoverRequest) -> CoverResponse:
    cells = sdk.cover(
        grid_type=req.grid_type,
        level=req.level,
        cover_mode=req.cover_mode,
        boundary_type=req.boundary_type,
        geometry=req.geometry,
        bbox=req.bbox,
        crs=req.crs,
    )
    return CoverResponse(
        grid_type=req.grid_type.value,
        level=req.level,
        cover_mode=req.cover_mode.value,
        cells=cells,
        statistics={"cell_count": len(cells)},
    )


@api_router.post("/topology/neighbors", response_model=NeighborsResponse)
def neighbors(req: NeighborsRequest) -> NeighborsResponse:
    result_codes = sdk.neighbors(grid_type=req.grid_type, code=req.code, k=req.k)
    return NeighborsResponse(result_codes=result_codes, statistics={"count": len(result_codes)})


@api_router.post("/topology/geometry", response_model=GeometryResponse)
def code_to_geometry(req: CodeToGeometryRequest) -> GeometryResponse:
    geometry = sdk.code_to_geometry(grid_type=req.grid_type, code=req.code, boundary_type=req.boundary_type)
    return GeometryResponse(geometry=geometry)


@api_router.post("/topology/geometries", response_model=BatchGeometryResponse)
def codes_to_geometries(req: BatchCodeToGeometryRequest) -> BatchGeometryResponse:
    geometries = sdk.codes_to_geometries(grid_type=req.grid_type, codes=req.codes, boundary_type=req.boundary_type)
    return BatchGeometryResponse(geometries=geometries, statistics={"count": len(geometries)})


@api_router.post("/topology/parent", response_model=ParentResponse)
def parent(req: ParentRequest) -> ParentResponse:
    parent_code = sdk.parent(grid_type=req.grid_type, code=req.code)
    return ParentResponse(parent_code=parent_code)


@api_router.post("/topology/children", response_model=ChildrenResponse)
def children(req: ChildrenRequest) -> ChildrenResponse:
    child_codes = sdk.children(grid_type=req.grid_type, code=req.code, target_level=req.target_level)
    return ChildrenResponse(child_codes=child_codes, statistics={"count": len(child_codes)})


@api_router.post("/code/st", response_model=STCodeGenerateResponse)
def generate_st(req: STCodeGenerateRequest) -> STCodeGenerateResponse:
    result = sdk.generate_st_code(
        grid_type=req.grid_type,
        level=req.level,
        space_code=req.space_code,
        timestamp=req.timestamp,
        time_granularity=req.time_granularity,
        version=req.version,
    )
    return STCodeGenerateResponse(st_code=result.st_code)


@api_router.post("/code/parse", response_model=STCodeParseResponse)
def parse_st(req: STCodeParseRequest) -> STCodeParseResponse:
    result = sdk.parse_st_code(req.st_code)
    return STCodeParseResponse(
        grid_type=result.grid_type,
        level=result.level,
        space_code=result.space_code,
        time_code=result.time_code,
        version=result.version,
    )


@api_router.post("/code/st/batch", response_model=STCodeBatchGenerateResponse)
def batch_generate_st(req: STCodeBatchGenerateRequest) -> STCodeBatchGenerateResponse:
    st_codes = sdk.batch_generate_st_codes(
        grid_type=req.grid_type,
        level=req.level,
        items=[{"space_code": item.space_code, "timestamp": item.timestamp} for item in req.items],
        time_granularity=req.time_granularity,
        version=req.version,
    )
    return STCodeBatchGenerateResponse(st_codes=st_codes, statistics={"count": len(st_codes)})


@api_router.post("/partition/carbon/demo")
def partition_carbon_demo() -> dict:
    try:
        return _run_carbon_partition_demo()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@api_router.post("/partition/optical/demo")
def partition_optical_demo() -> dict:
    try:
        return _run_optical_partition_demo()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@api_router.post("/quality/optical/run")
def quality_optical_run(payload: dict) -> dict:
    if run_optical_quality_check is None:
        raise HTTPException(status_code=500, detail="cube_split quality module is not available")
    run_dir = str(payload.get("run_dir", "")).strip()
    if not run_dir:
        raise HTTPException(status_code=422, detail="run_dir is required")
    args = _quality_args(run_dir, payload)
    try:
        return run_optical_quality_check(args)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@api_router.post("/quality/optical/latest")
def quality_optical_latest(payload: dict | None = None) -> dict:
    payload = payload or {}
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
def quality_optical_report(payload: dict) -> dict:
    run_dir_text = str(payload.get("run_dir", "")).strip()
    if not run_dir_text:
        raise HTTPException(status_code=422, detail="run_dir is required")
    run_dir = Path(run_dir_text)
    report = _read_quality_report(run_dir, data_type="optical")
    if report is None:
        raise HTTPException(status_code=404, detail=f"quality_report.json not found under run_dir: {run_dir}")
    return report


@api_router.post("/quality/optical/report/pdf")
def quality_optical_report_pdf(payload: dict) -> Response:
    report = quality_optical_report(payload)
    return _quality_report_pdf_response(report, data_type="optical")


@api_router.post("/quality/optical/history")
def quality_optical_history(payload: dict | None = None) -> dict:
    payload = payload or {}
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
def quality_product_run(payload: dict) -> dict:
    if run_product_quality_check is None:
        raise HTTPException(status_code=500, detail="cube_split product quality module is not available")
    run_dir = str(payload.get("run_dir", "")).strip()
    if not run_dir:
        raise HTTPException(status_code=422, detail="run_dir is required")
    args = _quality_args(run_dir, payload)
    try:
        return run_product_quality_check(args)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@api_router.post("/quality/product/latest")
def quality_product_latest(payload: dict | None = None) -> dict:
    payload = payload or {}
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
def quality_product_report(payload: dict) -> dict:
    run_dir_text = str(payload.get("run_dir", "")).strip()
    if not run_dir_text:
        raise HTTPException(status_code=422, detail="run_dir is required")
    run_dir = Path(run_dir_text)
    report = _read_quality_report(run_dir, data_type="product")
    if report is None:
        raise HTTPException(status_code=404, detail=f"quality_report.json not found under run_dir: {run_dir}")
    return report


@api_router.post("/quality/product/report/pdf")
def quality_product_report_pdf(payload: dict) -> Response:
    report = quality_product_report(payload)
    return _quality_report_pdf_response(report, data_type="product")


@api_router.post("/quality/product/history")
def quality_product_history(payload: dict | None = None) -> dict:
    payload = payload or {}
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


app.include_router(api_router)


@app.get("/")
def home() -> FileResponse:
    return FileResponse(WEB_DIR / "index.html", media_type="text/html")


@app.get("/{path_name:path}")
def serve_web_asset(path_name: str) -> FileResponse:
    file_path = _resolve_web_file(path_name)
    media_type = STATIC_MEDIA_TYPES.get(file_path.suffix, "text/html")
    return FileResponse(file_path, media_type=media_type)
