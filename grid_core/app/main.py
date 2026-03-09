from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from grid_core.app.api.code import router as code_router
from grid_core.app.api.grid import router as grid_router
from grid_core.app.api.topology import router as topology_router
from grid_core.app.core.config import config
from grid_core.app.core.exceptions import GridCoreError

app = FastAPI(title=config.app_name)
app.include_router(grid_router, prefix=config.api_prefix)
app.include_router(code_router, prefix=config.api_prefix)
app.include_router(topology_router, prefix=config.api_prefix)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.exception_handler(GridCoreError)
async def handle_grid_core_error(_: Request, exc: GridCoreError):
    return JSONResponse(status_code=400, content={"error": {"code": exc.code, "message": exc.message}})
