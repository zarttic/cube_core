from __future__ import annotations

from datetime import datetime, timezone

import h3
import pytest

from grid_core.app.core.enums import GridType, TimeGranularity
from grid_core.app.core.exceptions import ParseError, ValidationError
from grid_core.app.engines.mgrs_engine import MGRSEngine
from grid_core.app.engines.s2_engine import S2Engine
from grid_core.app.engines.tile_matrix_engine import TileMatrixEngine
from grid_core.app.services.code_service import CodeService


def test_generate_and_parse_st_code():
    service = CodeService()
    code = S2Engine().locate_point(lon=116.391, lat=39.907, level=7).space_code
    result = service.generate_st_code(
        grid_type=GridType.S2,
        level=7,
        space_code=code,
        timestamp=datetime(2026, 3, 9, 15, 30, 0, tzinfo=timezone.utc),
        time_granularity=TimeGranularity.MINUTE,
    )
    assert result.st_code == f"s2:7:{code}:202603091530"

    parsed = service.parse_st_code(result.st_code)
    assert parsed.grid_type == "s2"
    assert parsed.level == 7
    assert parsed.space_code == code
    assert parsed.time_code == "202603091530"


def test_batch_generate_st_code():
    service = CodeService()
    c1 = S2Engine().locate_point(lon=116.391, lat=39.907, level=7).space_code
    c2 = S2Engine().locate_point(lon=116.392, lat=39.908, level=7).space_code
    result = service.batch_generate_st_codes(
        grid_type=GridType.S2,
        level=7,
        items=[
            {"space_code": c1, "timestamp": datetime(2026, 3, 9, 15, 30, 0, tzinfo=timezone.utc)},
            {"space_code": c2, "timestamp": datetime(2026, 3, 9, 15, 31, 0, tzinfo=timezone.utc)},
        ],
        time_granularity=TimeGranularity.MINUTE,
    )
    assert result == [
        f"s2:7:{c1}:202603091530",
        f"s2:7:{c2}:202603091531",
    ]


def test_generate_and_parse_st_code_for_mgrs():
    service = CodeService()
    mgrs_code = MGRSEngine().locate_point(lon=116.391, lat=39.907, level=5).space_code
    result = service.generate_st_code(
        grid_type=GridType.MGRS,
        level=5,
        space_code=mgrs_code,
        timestamp=datetime(2026, 3, 9, 15, 30, 0, tzinfo=timezone.utc),
        time_granularity=TimeGranularity.HOUR,
    )
    assert result.st_code.startswith("mgrs:5:")
    assert result.st_code.endswith(":2026030915")

    parsed = service.parse_st_code(result.st_code)
    assert parsed.grid_type == "mgrs"
    assert parsed.level == 5
    assert parsed.space_code == mgrs_code


def test_generate_and_parse_st_code_for_isea4h():
    service = CodeService()
    cell = h3.latlng_to_cell(39.907, 116.391, 4)
    result = service.generate_st_code(
        grid_type=GridType.ISEA4H,
        level=4,
        space_code=cell,
        timestamp=datetime(2026, 3, 9, 15, 30, 0, tzinfo=timezone.utc),
        time_granularity=TimeGranularity.DAY,
    )
    assert result.st_code == f"hx:4:{cell}:20260309"

    parsed = service.parse_st_code(result.st_code)
    assert parsed.grid_type == "isea4h"
    assert parsed.level == 4
    assert parsed.space_code == cell


def test_generate_and_parse_st_code_for_tile_matrix():
    service = CodeService()
    tile_code = TileMatrixEngine().locate_point(lon=116.391, lat=39.907, level=3).space_code
    result = service.generate_st_code(
        grid_type=GridType.TILE_MATRIX,
        level=3,
        space_code=tile_code,
        timestamp=datetime(2026, 3, 9, 15, 30, 0, tzinfo=timezone.utc),
        time_granularity=TimeGranularity.DAY,
    )
    assert result.st_code == f"tm:3:{tile_code}:20260309"

    parsed = service.parse_st_code(result.st_code)
    assert parsed.grid_type == "tile_matrix"
    assert parsed.level == 3
    assert parsed.space_code == tile_code


def test_parse_st_code_rejects_unknown_prefix():
    service = CodeService()
    with pytest.raises(ValidationError):
        service.parse_st_code("unknown:7:abc:202603091530")


def test_parse_st_code_rejects_invalid_format():
    service = CodeService()
    with pytest.raises(ParseError):
        service.parse_st_code("not-a-valid-st-code")


def test_generate_st_code_rejects_invalid_s2_space_code():
    service = CodeService()
    with pytest.raises(ValidationError):
        service.generate_st_code(
            grid_type=GridType.S2,
            level=7,
            space_code="NOT_A_S2",
            timestamp=datetime(2026, 3, 9, 15, 30, 0, tzinfo=timezone.utc),
            time_granularity=TimeGranularity.MINUTE,
        )


def test_generate_st_code_rejects_level_mismatch_for_h3():
    service = CodeService()
    code = h3.latlng_to_cell(39.907, 116.391, 7)
    with pytest.raises(ValidationError):
        service.generate_st_code(
            grid_type=GridType.ISEA4H,
            level=6,
            space_code=code,
            timestamp=datetime(2026, 3, 9, 15, 30, 0, tzinfo=timezone.utc),
            time_granularity=TimeGranularity.MINUTE,
        )


def test_generate_st_code_rejects_naive_datetime():
    service = CodeService()
    code = S2Engine().locate_point(lon=116.391, lat=39.907, level=7).space_code

    with pytest.raises(ValidationError):
        service.generate_st_code(
            grid_type=GridType.S2,
            level=7,
            space_code=code,
            timestamp=datetime(2026, 3, 9, 15, 30, 0),
            time_granularity=TimeGranularity.MINUTE,
        )


def test_parse_st_code_rejects_invalid_time_code_value():
    service = CodeService()
    code = S2Engine().locate_point(lon=116.391, lat=39.907, level=7).space_code
    with pytest.raises(ValidationError):
        service.parse_st_code(f"s2:7:{code}:202613011230")
