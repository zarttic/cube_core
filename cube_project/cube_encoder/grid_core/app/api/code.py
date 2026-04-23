from fastapi import APIRouter

from grid_core.app.models.request import STCodeBatchGenerateRequest, STCodeGenerateRequest, STCodeParseRequest
from grid_core.app.models.response import STCodeBatchGenerateResponse, STCodeGenerateResponse, STCodeParseResponse
from grid_core.app.services.code_service import CodeService

router = APIRouter(prefix="/code", tags=["code"])
service = CodeService()


@router.post("/st", response_model=STCodeGenerateResponse)
def generate_st(req: STCodeGenerateRequest) -> STCodeGenerateResponse:
    result = service.generate_st_code(
        grid_type=req.grid_type,
        level=req.level,
        space_code=req.space_code,
        timestamp=req.timestamp,
        time_granularity=req.time_granularity,
        version=req.version,
    )
    return STCodeGenerateResponse(st_code=result.st_code)


@router.post("/parse", response_model=STCodeParseResponse)
def parse_st(req: STCodeParseRequest) -> STCodeParseResponse:
    result = service.parse_st_code(req.st_code)
    return STCodeParseResponse(
        grid_type=result.grid_type,
        level=result.level,
        space_code=result.space_code,
        time_code=result.time_code,
        version=result.version,
    )


@router.post("/st/batch", response_model=STCodeBatchGenerateResponse)
def batch_generate_st(req: STCodeBatchGenerateRequest) -> STCodeBatchGenerateResponse:
    st_codes = service.batch_generate_st_codes(
        grid_type=req.grid_type,
        level=req.level,
        items=[{"space_code": item.space_code, "timestamp": item.timestamp} for item in req.items],
        time_granularity=req.time_granularity,
        version=req.version,
    )
    return STCodeBatchGenerateResponse(st_codes=st_codes, statistics={"count": len(st_codes)})
