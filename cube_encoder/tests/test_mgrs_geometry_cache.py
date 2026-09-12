"""Cache and latitude-band invariants for MGRS clipped cell geometry.

The band clip used to rebuild ``domain_polygon(domain) ∩ band_polygon(code)`` for
every cell, and ``decode_utm`` used to record every C-extension warning.  Both are
pure functions of their inputs, so these tests pin the cache identity *and* prove
the clipped geometry is unchanged against an uncached recomputation.
"""

from __future__ import annotations

import threading
import warnings
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

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


def test_decode_utm_matches_the_mgrs_library() -> None:
    """The direct C binding must decide exactly like the library it mirrors.

    ``decode_utm`` reads the status bitmask itself instead of going through the
    library's warning, so this guards against a future ``mgrs`` version changing the
    ABI or the meaning of the error/warning bits.
    """
    from mgrs import MGRS

    library = MGRS()
    checked = 0
    for zone in (1, 32, 49, 60):
        for band in "CDEFGHJKLMNPQRSTUVWX":
            for square in ("AA", "MT", "PU", "ZZ"):
                code = f"{zone}{band}{square}12345"
                checked += 1
                try:
                    with warnings.catch_warnings(record=True) as caught:
                        warnings.simplefilter("always", RuntimeWarning)
                        expected = library.MGRSToUTM(code)
                    band_warning = any("Convert_MGRS_To_UTM" in str(item.message) for item in caught)
                except Exception:  # noqa: BLE001 - hard C error
                    with pytest.raises(ValidationError, match="Cannot decode UTM MGRS code"):
                        decode_utm(code)
                    continue
                if band_warning:
                    with pytest.raises(ValidationError, match="invalid latitude band"):
                        decode_utm(code)
                else:
                    assert decode_utm(code) == expected
    assert checked == 4 * 20 * 4


def test_primary_decode_path_does_not_touch_the_warnings_filters(monkeypatch) -> None:
    """The latitude-band check must not depend on process-global warnings state.

    The check used to be implemented by promoting a C-extension RuntimeWarning to an
    error, which mutates the process-global ``warnings`` filters.  Two threads then
    raced: one restoring an older filter list dropped the other's promotion and the
    band-inconsistent code was accepted (and, being cached, stayed accepted).  Race
    timing makes a concurrency test flaky, so assert the invariant directly.
    """
    from grid_core.app.engines.mgrs import geometry

    assert geometry._RAW_MGRS_TO_UTM is not None, "the direct C binding must be in use"

    def forbidden(*args, **kwargs):
        raise AssertionError("decode_utm must not manipulate process-global warnings filters")

    monkeypatch.setattr(geometry, "warnings", SimpleNamespace(catch_warnings=forbidden, filterwarnings=forbidden))
    decode_utm.cache_clear()

    assert decode_utm(VALID_CODE) == (32, "N", 412345.0, 5267890.0)
    with pytest.raises(ValidationError, match="invalid latitude band"):
        decode_utm(BAND_MISMATCH_CODE)
    with pytest.raises(ValidationError, match="Cannot decode UTM MGRS code"):
        decode_utm("32**1234")


def test_fallback_decode_path_holds_the_warnings_lock(monkeypatch) -> None:
    """If the direct binding is unavailable, the warnings window must be serialised."""
    from grid_core.app.engines.mgrs import geometry

    class CountingLock:
        def __init__(self) -> None:
            self._lock = threading.Lock()
            self.acquisitions = 0

        def __enter__(self):
            self._lock.acquire()
            self.acquisitions += 1
            return self

        def __exit__(self, *exc_info) -> bool:
            self._lock.release()
            return False

    counting_lock = CountingLock()
    monkeypatch.setattr(geometry, "_RAW_MGRS_TO_UTM", None)
    monkeypatch.setattr(geometry, "_WARNINGS_LOCK", counting_lock)
    decode_utm.cache_clear()

    assert decode_utm(VALID_CODE) == (32, "N", 412345.0, 5267890.0)
    with pytest.raises(ValidationError, match="invalid latitude band"):
        decode_utm(BAND_MISMATCH_CODE)
    assert counting_lock.acquisitions == 2


def test_concurrent_fallback_decode_always_rejects_a_band_mismatch(monkeypatch) -> None:
    """Every mismatch decode must be rejected while valid decodes run concurrently."""
    from grid_core.app.engines.mgrs import geometry

    monkeypatch.setattr(geometry, "_RAW_MGRS_TO_UTM", None)
    valid_codes = [f"32TMT{100000 + index}" for index in range(64)]
    leaked: list[str] = []
    wrongly_rejected: list[str] = []

    def decode_valid(index: int) -> None:
        try:
            decode_utm(valid_codes[index % len(valid_codes)])
        except ValidationError as exc:  # a valid code must never be rejected
            wrongly_rejected.append(str(exc))

    def decode_mismatch() -> None:
        try:
            decode_utm(BAND_MISMATCH_CODE)
        except ValidationError:
            return
        leaked.append(BAND_MISMATCH_CODE)

    decode_utm.cache_clear()
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = []
        for round_index in range(64):
            futures.extend(pool.submit(decode_valid, (round_index * 6 + offset) % len(valid_codes)) for offset in range(6))
            futures.extend(pool.submit(decode_mismatch) for _ in range(2))
        for future in futures:
            future.result()

    assert leaked == []
    assert wrongly_rejected == []
