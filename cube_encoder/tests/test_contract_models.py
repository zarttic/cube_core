"""Contract tests for the M1 frozen grid SDK contract.

These tests define the breaking contract boundaries for Task 1.
They must fail before implementation and pass after.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError as PydanticValidationError

from grid_core.app.core.enums import GridType
from grid_core.app.models.grid_address import GridAddress
from grid_core.app.models.grid_cell import GridCell
from grid_core.app.models.request import (
    LocateRequest,
    NeighborsRequest,
    validate_requested_grid_level,
)


def test_grid_types_are_exactly_the_three_production_types() -> None:
    assert {item.value for item in GridType} == {"geohash", "mgrs", "isea4h"}


def test_grid_cell_carries_result_level_and_dual_mgrs_identity() -> None:
    cell = GridCell(
        grid_type="mgrs",
        grid_level=3,
        space_code="31UDQ482511",
        topology_code="mgrs-topo-v1:utm-31n:3:31UDQ482511",
        center=[2.2945, 48.8582],
        bbox=[2.29, 48.85, 2.30, 48.86],
    )
    assert cell.grid_level == 3
    assert not hasattr(cell, "level")
    assert not hasattr(cell, "cell_id")


@pytest.mark.parametrize(
    ("grid_type", "accepted"),
    [
        (GridType.GEOHASH, (1, 12)),
        (GridType.MGRS, (0, 5)),
        (GridType.ISEA4H, (0, 15)),
    ],
)
def test_validate_requested_grid_level_exports_exact_m2_ranges(
    grid_type: GridType, accepted: tuple[int, int]
) -> None:
    minimum, maximum = accepted
    assert validate_requested_grid_level(grid_type, minimum) == minimum
    assert validate_requested_grid_level(grid_type, maximum) == maximum
    with pytest.raises(ValueError, match="requested_grid_level"):
        validate_requested_grid_level(grid_type, minimum - 1)
    with pytest.raises(ValueError, match="requested_grid_level"):
        validate_requested_grid_level(grid_type, maximum + 1)


def test_requests_reject_legacy_level() -> None:
    with pytest.raises(PydanticValidationError):
        LocateRequest.model_validate({"grid_type": "geohash", "level": 6, "point": [116.4, 39.9]})


def test_topology_request_requires_complete_address() -> None:
    request = NeighborsRequest(address=GridAddress(grid_type="isea4h", grid_level=6, space_code="1234"), k=1)
    assert request.address.grid_level == 6
