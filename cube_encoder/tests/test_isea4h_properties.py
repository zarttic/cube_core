"""Deterministic ISEA4H mathematical and public-geometry properties."""
from __future__ import annotations

import math
import random

from shapely.geometry import shape

from grid_core.app.engines.isea4h.addressing import cell_count, q2di_to_seqnum, seqnum_to_q2di
from grid_core.app.engines.isea4h.projection import snyder_fwd, snyder_inv
from grid_core.app.engines.isea4h.topology import cell_children, cell_neighbors, cell_parent
from grid_core.app.engines.isea4h_engine import ISEA4HEngine
from grid_core.app.models.grid_address import GridAddress

ENGINE = ISEA4HEngine()


def _angular_error(first: float, second: float) -> float:
    return abs(math.atan2(math.sin(first - second), math.cos(first - second)))


def test_cell_count_and_q2di_seqnum_bijection_at_all_resolutions() -> None:
    for resolution in range(16):
        assert cell_count(resolution) == 10 * 4**resolution + 2
        samples = {1, cell_count(resolution)}
        rng = random.Random(20_260_714 + resolution)
        samples.update(rng.randint(1, cell_count(resolution)) for _ in range(64))
        for seqnum in samples:
            quad, i, j = seqnum_to_q2di(seqnum, resolution)
            assert q2di_to_seqnum(quad, i, j, resolution) == seqnum


def test_snyder_forward_inverse_is_stable_for_fixed_non_singular_samples() -> None:
    rng = random.Random(20_260_714)
    for _ in range(256):
        lon = rng.uniform(-math.pi, math.pi)
        lat = rng.uniform(-math.pi / 2 + 0.02, math.pi / 2 - 0.02)
        triangle, x, y = snyder_fwd(lat, lon)
        restored_lat, restored_lon = snyder_inv(triangle, x, y)
        assert abs(restored_lat - lat) < 1e-10
        assert _angular_error(restored_lon, lon) < 1e-10


def test_public_geometries_are_closed_valid_and_local_through_resolution_four() -> None:
    for resolution in range(5):
        for seqnum in range(1, cell_count(resolution) + 1):
            address = GridAddress(grid_type="isea4h", grid_level=resolution, space_code=str(seqnum))
            geometry = shape(ENGINE.code_to_geometry(address))
            bbox = ENGINE.code_to_bbox(address)
            assert geometry.is_valid and not geometry.is_empty
            assert geometry.boundary.is_closed
            assert all(-180.0 <= value <= 180.0 for value in (bbox[0], bbox[2]))
            assert -90.0 <= bbox[1] <= bbox[3] <= 90.0


def test_antimeridian_geometry_is_local() -> None:
    address = ENGINE.locate_space_code(179.9, 62.4, 6)
    bbox = ENGINE.code_to_bbox(address)
    geometry = shape(ENGINE.code_to_geometry(address))
    assert geometry.is_valid and not geometry.is_empty
    assert bbox[2] - bbox[0] <= 180.0 + 1e-6


def test_neighbors_are_symmetric_and_parent_contains_every_child() -> None:
    for resolution in range(5):
        for seqnum in range(1, cell_count(resolution) + 1):
            assert seqnum not in cell_neighbors(seqnum, resolution)
            for neighbor in cell_neighbors(seqnum, resolution):
                assert seqnum in cell_neighbors(neighbor, resolution)
    for resolution in range(1, 5):
        for seqnum in range(1, cell_count(resolution) + 1):
            parent = cell_parent(seqnum, resolution)
            assert seqnum in cell_children(parent, resolution - 1)
