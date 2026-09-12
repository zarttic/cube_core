"""Cache and latitude-band invariants for MGRS clipped cell geometry.

The band clip used to rebuild ``domain_polygon(domain) ∩ band_polygon(code)`` for
every cell, and ``decode_utm`` used to record every C-extension warning.  Both are
pure functions of their inputs, so these tests pin the cache identity *and* prove
the clipped geometry is unchanged against an uncached recomputation.
"""

from __future__ import annotations

import pytest

from grid_core.app.core.exceptions import ValidationError
from grid_core.app.engines.mgrs.domain import GridDomain, domain_polygon
from grid_core.app.engines.mgrs.geometry import (
    _band_polygon,
    _utm_band_letter,
    _utm_band_polygon,
    _utm_raw_geometry,
    _valid_utm_domain,
    cell_geometry_clipped,
    decode_utm,
)

BAND_MISMATCH_CODE = "32TPU85"
VALID_CODE = "32TMT1234567890"
DOMAIN = GridDomain(kind="utm", zone=32, hemisphere="n")


def test_valid_utm_domain_is_cached_and_matches_the_uncached_clip() -> None:
    _valid_utm_domain.cache_clear()
    expected = domain_polygon(DOMAIN).intersection(_utm_band_polygon(VALID_CODE))

    assert _valid_utm_domain(DOMAIN, "T").equals(expected)
    assert _valid_utm_domain(DOMAIN, "T").equals(expected)
    info = _valid_utm_domain.cache_info()
    assert (info.misses, info.hits) == (1, 1)


def test_band_clip_cache_is_shared_by_every_code_in_one_band() -> None:
    _valid_utm_domain.cache_clear()
    _band_polygon.cache_clear()

    assert _utm_band_letter("32TMT1234567890") == _utm_band_letter("32TPU85") == "T"
    _valid_utm_domain(DOMAIN, "T")
    _valid_utm_domain(DOMAIN, "T")

    assert _valid_utm_domain.cache_info().misses == 1
    assert _band_polygon.cache_info().misses == 1


def test_cell_geometry_clipped_equals_an_uncached_band_clip() -> None:
    cell_geometry_clipped.cache_clear()
    _valid_utm_domain.cache_clear()

    cached = cell_geometry_clipped(VALID_CODE, 1, DOMAIN)

    zone, hemisphere, easting, northing = decode_utm(VALID_CODE)
    raw = _utm_raw_geometry(zone, hemisphere, easting, northing, 1)
    valid_domain = domain_polygon(DOMAIN).intersection(_utm_band_polygon(VALID_CODE))
    expected = raw.intersection(valid_domain)

    assert cached.equals(expected)


def test_cell_geometry_clipped_reuses_the_band_clip_across_codes() -> None:
    cell_geometry_clipped.cache_clear()
    _valid_utm_domain.cache_clear()

    for code in ("32TMT1234567890", "32TPU85", "32TMT9999999999"):
        try:
            cell_geometry_clipped(code, 1, DOMAIN)
        except ValidationError:
            continue

    # Every accepted code sits in band T, so the clip polygon is built once.
    assert _valid_utm_domain.cache_info().misses == 1


def test_decode_utm_rejects_a_latitude_band_mismatch() -> None:
    with pytest.raises(ValidationError, match="invalid latitude band"):
        decode_utm(BAND_MISMATCH_CODE)


def test_decode_utm_caches_successful_decodes_only() -> None:
    decode_utm.cache_clear()

    assert decode_utm(VALID_CODE) == decode_utm(VALID_CODE)
    info = decode_utm.cache_info()
    assert (info.misses, info.hits) == (1, 1)

    with pytest.raises(ValidationError):
        decode_utm(BAND_MISMATCH_CODE)
    assert decode_utm.cache_info().currsize == 1  # failures are not cached


def test_utm_band_letter_rejects_codes_without_a_known_band() -> None:
    with pytest.raises(ValidationError, match="Cannot determine UTM latitude band"):
        _utm_band_letter("32ZMT1234567890")
