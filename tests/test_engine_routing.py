import pytest

from grid_core.app.core.enums import GridType
from grid_core.app.services.grid_service import GridService
from grid_core.app.services.topology_service import TopologyService


def test_grid_service_geohash_route_still_works():
    service = GridService()
    cell = service.locate(GridType.GEOHASH, level=6, point=[116.391, 39.907])
    assert cell.grid_type == "geohash"


def test_grid_service_mgrs_route_works_for_locate():
    service = GridService()
    cell = service.locate(GridType.MGRS, level=5, point=[116.391, 39.907])
    assert cell.grid_type == "mgrs"
    assert cell.space_code.startswith("50S")


def test_topology_service_isea4h_route_works():
    service = TopologyService()
    cell = GridService().locate(GridType.ISEA4H, level=6, point=[116.391, 39.907])
    codes = service.neighbors(GridType.ISEA4H, code=cell.space_code, k=1)
    assert len(codes) > 0
