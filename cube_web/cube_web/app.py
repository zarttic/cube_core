from __future__ import annotations

import json
import shutil
import tarfile
import time
from uuid import uuid4
from pathlib import Path

from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.responses import JSONResponse
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


app.include_router(api_router)


@app.get("/")
def home() -> FileResponse:
    return FileResponse(WEB_DIR / "index.html", media_type="text/html")


@app.get("/{path_name:path}")
def serve_web_asset(path_name: str) -> FileResponse:
    file_path = _resolve_web_file(path_name)
    media_type = STATIC_MEDIA_TYPES.get(file_path.suffix, "text/html")
    return FileResponse(file_path, media_type=media_type)
