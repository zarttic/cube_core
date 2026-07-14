"""TDD tests for ISEA4H pure-Python mathematical foundation (DGGRID v8.44 compatible)."""
from __future__ import annotations

import pytest

from grid_core.app.engines.isea4h.addressing import (
    cell_count,
    intra_quad_index,
    offset_per_quad,
    q2di_to_seqnum,
    seqnum_to_q2di,
    validate_seqnum,
)
from grid_core.app.engines.isea4h.constants import (
    ICOSAHEDRON_VERTICES,
    POLE_SEQNUM_N,
    QUAD_COUNT,
    SPHERE_RADIUS_KM,
    VERT0_AZ,
    VERT0_LAT,
    VERT0_LON,
)
from grid_core.app.engines.isea4h.projection import locate_cell


# ---------------------------------------------------------------------------
# constants
# ---------------------------------------------------------------------------

class TestConstants:
    def test_sphere_radius(self):
        assert SPHERE_RADIUS_KM == pytest.approx(6371.007180918475)

    def test_vert0_values(self):
        assert VERT0_LON == pytest.approx(11.25)
        assert VERT0_LAT == pytest.approx(58.28252559)
        assert VERT0_AZ == pytest.approx(0.0)

    def test_quad_count(self):
        assert QUAD_COUNT == 10

    def test_pole_seqnum_n(self):
        assert POLE_SEQNUM_N == 1

    def test_icosahedron_vertices_count(self):
        assert len(ICOSAHEDRON_VERTICES) == 12

    def test_icosahedron_vertices_structure(self):
        for v in ICOSAHEDRON_VERTICES:
            lon, lat = v
            assert -180.0 <= lon <= 360.0, f"lon {lon} out of range"
            assert -90.0 <= lat <= 90.0, f"lat {lat} out of range"


# ---------------------------------------------------------------------------
# cell_count
# ---------------------------------------------------------------------------

class TestCellCount:
    def test_cell_count_formula(self):
        for r in range(16):
            assert cell_count(r) == 10 * 4**r + 2

    def test_cell_count_res0(self):
        assert cell_count(0) == 12

    def test_cell_count_res1(self):
        assert cell_count(1) == 42

    def test_cell_count_res2(self):
        assert cell_count(2) == 162

    def test_cell_count_res6(self):
        assert cell_count(6) == 10 * 4**6 + 2  # 40962

    def test_total_cells_0_to_6(self):
        assert sum(cell_count(r) for r in range(7)) == 54624


# ---------------------------------------------------------------------------
# offset_per_quad
# ---------------------------------------------------------------------------

class TestOffsetPerQuad:
    def test_offset_per_quad_reconstructs_cell_count(self):
        for r in range(7):
            assert offset_per_quad(r) * 10 + 2 == cell_count(r)

    def test_offset_per_quad_res0(self):
        # cell_count(0)=12, (12-2)//10 = 1
        assert offset_per_quad(0) == 1

    def test_offset_per_quad_res1(self):
        # cell_count(1)=42, (42-2)//10 = 4
        assert offset_per_quad(1) == 4

    def test_offset_per_quad_res2(self):
        # cell_count(2)=162, (162-2)//10 = 16
        assert offset_per_quad(2) == 16


# ---------------------------------------------------------------------------
# intra_quad_index
# ---------------------------------------------------------------------------

class TestIntraQuadIndex:
    def test_origin_is_zero(self):
        for r in range(5):
            assert intra_quad_index(0, 0, r) == 0

    def test_row_major_res1(self):
        # mag=2, so indices are i*2+j
        assert intra_quad_index(0, 0, 1) == 0
        assert intra_quad_index(0, 1, 1) == 1
        assert intra_quad_index(1, 0, 1) == 2
        assert intra_quad_index(1, 1, 1) == 3

    def test_all_indices_unique_res2(self):
        mag = 2**2  # 4
        indices = [intra_quad_index(i, j, 2) for i in range(mag) for j in range(mag)]
        assert len(set(indices)) == len(indices)
        assert set(indices) == set(range(mag * mag))


# ---------------------------------------------------------------------------
# q2di_to_seqnum / seqnum_to_q2di
# ---------------------------------------------------------------------------

class TestSeqnumConversions:
    def test_north_pole_always_seqnum1(self):
        for r in range(7):
            assert q2di_to_seqnum(0, 0, 0, r) == 1

    def test_south_pole_always_last(self):
        for r in range(7):
            assert q2di_to_seqnum(11, 0, 0, r) == cell_count(r)

    def test_pole_seqnums(self):
        for r in range(7):
            assert q2di_to_seqnum(0, 0, 0, r) == POLE_SEQNUM_N
            assert q2di_to_seqnum(11, 0, 0, r) == cell_count(r)

    def test_seqnum_roundtrip_exhaustive_res0(self):
        r = 0
        # res0: 12 cells, quads 0-11
        # quads 1-10 each have offset_per_quad(0)=1 cell
        seen_seqnums = set()
        for seqnum in range(1, cell_count(r) + 1):
            quad, i, j = seqnum_to_q2di(seqnum, r)
            back = q2di_to_seqnum(quad, i, j, r)
            assert back == seqnum, f"seqnum {seqnum} roundtrip failed: got {back}"
            seen_seqnums.add(seqnum)
        assert len(seen_seqnums) == cell_count(r)

    def test_seqnum_roundtrip_exhaustive_res1(self):
        r = 1
        for seqnum in range(1, cell_count(r) + 1):
            quad, i, j = seqnum_to_q2di(seqnum, r)
            back = q2di_to_seqnum(quad, i, j, r)
            assert back == seqnum, f"seqnum {seqnum} roundtrip failed: got {back}"

    def test_seqnum_roundtrip_exhaustive_res2(self):
        r = 2
        for seqnum in range(1, cell_count(r) + 1):
            quad, i, j = seqnum_to_q2di(seqnum, r)
            back = q2di_to_seqnum(quad, i, j, r)
            assert back == seqnum, f"seqnum {seqnum} roundtrip failed: got {back}"

    def test_seqnum_unique_per_resolution(self):
        for r in range(5):
            seqnums = []
            mag = 2**r
            # north pole
            seqnums.append(q2di_to_seqnum(0, 0, 0, r))
            # quads 1-10
            for q in range(1, 11):
                for i in range(mag):
                    for j in range(mag):
                        seqnums.append(q2di_to_seqnum(q, i, j, r))
            # south pole
            seqnums.append(q2di_to_seqnum(11, 0, 0, r))
            assert len(seqnums) == cell_count(r), f"res {r}: expected {cell_count(r)} cells, got {len(seqnums)}"
            assert len(set(seqnums)) == cell_count(r), f"res {r}: duplicate seqnums found"

    def test_quad1_first_cell_is_seqnum2(self):
        for r in range(5):
            assert q2di_to_seqnum(1, 0, 0, r) == 2

    def test_quad10_last_cell_is_second_to_last(self):
        for r in range(5):
            mag = 2**r
            last_i, last_j = mag - 1, mag - 1
            expected = cell_count(r) - 1  # second to last (last is S pole)
            assert q2di_to_seqnum(10, last_i, last_j, r) == expected


# ---------------------------------------------------------------------------
# validate_seqnum
# ---------------------------------------------------------------------------

class TestValidateSeqnum:
    def test_valid_range(self):
        for r in range(5):
            assert validate_seqnum(1, r) is True
            assert validate_seqnum(cell_count(r), r) is True
            assert validate_seqnum(cell_count(r) // 2, r) is True

    def test_invalid_zero(self):
        for r in range(5):
            assert validate_seqnum(0, r) is False

    def test_invalid_over_max(self):
        for r in range(5):
            assert validate_seqnum(cell_count(r) + 1, r) is False

    def test_invalid_negative(self):
        assert validate_seqnum(-1, 2) is False


# ---------------------------------------------------------------------------
# locate_cell (projection)
# ---------------------------------------------------------------------------

class TestLocateCell:
    def test_returns_tuple_of_three(self):
        result = locate_cell(0.0, 0.0, 1)
        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_quad_in_range(self):
        for lon, lat in [(0, 0), (90, 45), (-90, -45), (180, 0), (0, 89), (0, -89)]:
            quad, i, j = locate_cell(lon, lat, 1)
            assert 0 <= quad <= 11, f"quad {quad} out of range for ({lon}, {lat})"

    def test_ij_in_range_res1(self):
        mag = 2**1
        for lon, lat in [(0, 0), (90, 45), (-90, -45), (45, 30), (-45, -30)]:
            quad, i, j = locate_cell(lon, lat, 1)
            if quad in (0, 11):
                pass  # pole cells i,j are 0,0
            else:
                assert 0 <= i < mag, f"i={i} out of range for res=1"
                assert 0 <= j < mag, f"j={j} out of range for res=1"

    def test_north_pole_is_quad0(self):
        quad, i, j = locate_cell(0.0, 89.9, 0)
        assert quad == 0

    def test_south_pole_is_quad11(self):
        # DGGRID oracle: the exact geographic south pole falls in quad 11
        # (whose vertex-centre is at (-168.75, -58.28)). Note (0, -89.9)
        # instead lands in quad 8 -- the icosahedron is not equator-symmetric.
        quad, i, j = locate_cell(0.0, -90.0, 0)
        assert quad == 11

    def test_deterministic(self):
        for lon, lat in [(10.5, 20.3), (-50.0, 30.0), (100.0, -20.0)]:
            r1 = locate_cell(lon, lat, 2)
            r2 = locate_cell(lon, lat, 2)
            assert r1 == r2

    def test_seqnum_from_locate_is_valid(self):
        for lon, lat in [(0, 0), (90, 45), (-90, -45), (180, 0), (0, 85), (0, -85)]:
            for r in range(4):
                quad, i, j = locate_cell(lon, lat, r)
                sn = q2di_to_seqnum(quad, i, j, r)
                assert validate_seqnum(sn, r), f"invalid seqnum {sn} at res {r} for ({lon},{lat})"
