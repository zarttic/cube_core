from datetime import datetime

import pytest

from grid_core.app.core.enums import GridType, TimeGranularity
from grid_core.app.core.exceptions import ParseError, ValidationError
from grid_core.app.services.code_service import CodeService


def test_generate_and_parse_st_code():
    service = CodeService()
    result = service.generate_st_code(
        grid_type=GridType.GEOHASH,
        level=7,
        space_code="wtw3sjq",
        timestamp=datetime(2026, 3, 9, 15, 30, 0),
        time_granularity=TimeGranularity.MINUTE,
        version="v1",
    )
    assert result.st_code == "gh:7:wtw3sjq:202603091530:v1"

    parsed = service.parse_st_code(result.st_code)
    assert parsed.grid_type == "geohash"
    assert parsed.level == 7
    assert parsed.space_code == "wtw3sjq"
    assert parsed.time_code == "202603091530"
    assert parsed.version == "v1"


def test_batch_generate_st_code():
    service = CodeService()
    result = service.batch_generate_st_codes(
        grid_type=GridType.GEOHASH,
        level=7,
        items=[
            {"space_code": "wtw3sjq", "timestamp": datetime(2026, 3, 9, 15, 30, 0)},
            {"space_code": "wtw3sjr", "timestamp": datetime(2026, 3, 9, 15, 31, 0)},
        ],
        time_granularity=TimeGranularity.MINUTE,
        version="v1",
    )
    assert result == [
        "gh:7:wtw3sjq:202603091530:v1",
        "gh:7:wtw3sjr:202603091531:v1",
    ]


def test_generate_and_parse_st_code_for_mgrs():
    service = CodeService()
    result = service.generate_st_code(
        grid_type=GridType.MGRS,
        level=5,
        space_code="50SMG1234",
        timestamp=datetime(2026, 3, 9, 15, 30, 0),
        time_granularity=TimeGranularity.HOUR,
        version="v1",
    )
    assert result.st_code == "mgrs:5:50SMG1234:2026030915:v1"

    parsed = service.parse_st_code(result.st_code)
    assert parsed.grid_type == "mgrs"
    assert parsed.level == 5
    assert parsed.space_code == "50SMG1234"


def test_generate_and_parse_st_code_for_isea4h():
    service = CodeService()
    result = service.generate_st_code(
        grid_type=GridType.ISEA4H,
        level=4,
        space_code="hx_demo",
        timestamp=datetime(2026, 3, 9, 15, 30, 0),
        time_granularity=TimeGranularity.DAY,
        version="v1",
    )
    assert result.st_code == "hx:4:hx_demo:20260309:v1"

    parsed = service.parse_st_code(result.st_code)
    assert parsed.grid_type == "isea4h"
    assert parsed.level == 4
    assert parsed.space_code == "hx_demo"


def test_parse_st_code_rejects_unknown_prefix():
    service = CodeService()
    with pytest.raises(ValidationError):
        service.parse_st_code("unknown:7:abc:202603091530:v1")


def test_parse_st_code_rejects_invalid_format():
    service = CodeService()
    with pytest.raises(ParseError):
        service.parse_st_code("not-a-valid-st-code")
