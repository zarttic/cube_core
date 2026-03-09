import pytest

from grid_core.app.core.enums import GridType
from grid_core.app.core.exceptions import NotImplementedCapabilityError
from grid_core.app.services.grid_service import GridService
from grid_core.app.services.topology_service import TopologyService


def test_grid_service_geohash_route_still_works():
    service = GridService()
    cell = service.locate(GridType.GEOHASH, level=6, point=[116.391, 39.907])
    assert cell.grid_type == "geohash"


def test_grid_service_mgrs_route_returns_not_implemented():
    service = GridService()
    with pytest.raises(NotImplementedCapabilityError):
        service.locate(GridType.MGRS, level=6, point=[116.391, 39.907])


def test_topology_service_isea4h_route_returns_not_implemented():
    service = TopologyService()
    with pytest.raises(NotImplementedCapabilityError):
        service.neighbors(GridType.ISEA4H, code="dummy", k=1)
