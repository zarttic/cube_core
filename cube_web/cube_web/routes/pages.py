from __future__ import annotations

from pathlib import Path
from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, RedirectResponse

WEB_DIR = Path(__file__).resolve().parents[1] / "web"
STATIC_MEDIA_TYPES = {
    ".css": "text/css",
    ".js": "application/javascript",
}


def create_pages_router(web_dir: Path = WEB_DIR) -> APIRouter:
    router = APIRouter(tags=["pages"])

    @router.get("/callback")
    def auth_callback_page(code: str | None = None, state: str | None = None):
        if code:
            query = {"code": code}
            if state:
                query["state"] = state
            return RedirectResponse(f"/?{urlencode(query)}")
        return FileResponse(web_dir / "index.html", media_type="text/html")

    @router.get("/")
    def home() -> FileResponse:
        return FileResponse(web_dir / "index.html", media_type="text/html")

    @router.get("/{path_name:path}")
    def serve_web_asset(path_name: str) -> FileResponse:
        file_path = resolve_web_file(path_name, web_dir)
        media_type = STATIC_MEDIA_TYPES.get(file_path.suffix, "text/html")
        return FileResponse(file_path, media_type=media_type)

    return router


def resolve_web_file(path_name: str, web_dir: Path = WEB_DIR) -> Path:
    candidate = web_dir / path_name
    if candidate.exists() and candidate.is_file():
        return candidate

    if "." not in path_name:
        html_candidate = web_dir / f"{path_name}.html"
        if html_candidate.exists() and html_candidate.is_file():
            return html_candidate
        index_candidate = web_dir / "index.html"
        if index_candidate.exists() and index_candidate.is_file():
            return index_candidate

    if path_name in {"partition.html", "quality.html", "encoding.html", "config.html", "门户首页.html"}:
        index_candidate = web_dir / "index.html"
        if index_candidate.exists() and index_candidate.is_file():
            return index_candidate

    raise HTTPException(status_code=404, detail=f"Page not found: {path_name}")
