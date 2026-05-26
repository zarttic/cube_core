from __future__ import annotations

from pathlib import Path

from fastapi import HTTPException


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


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


def quality_args(run_dir: str, payload: dict | None = None):
    payload = payload or {}
    return type(
        "QualityArgs",
        (),
        {
            "run_dir": run_dir,
            "target_crs": str(payload.get("target_crs", "EPSG:4326") or "EPSG:4326"),
            "output": "",
        },
    )()
