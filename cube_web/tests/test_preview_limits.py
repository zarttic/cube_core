"""Preview size limits: a single cover/children request must not freeze the host.

Background: on 2026-09-17 the web node lost 16 GB of anonymous memory in 90 seconds
and had to be hard reset.  ``/v1/grid/cover`` and ``/v1/topology/children`` had no
bound, and a 3°x3° geohash L7 cover extrapolates to ~4.8M cells / ~16 GB.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from cube_web.services import preview_limits
from cube_web.services.preview_limits import (
    DEFAULT_MAX_PREVIEW_CELLS,
    DEFAULT_MAX_PREVIEW_CHILDREN,
    cover_preview_cells,
    ensure_preview_children_within_limit,
    estimate_cover_cells,
    preview_max_cells,
    preview_max_children,
    resolve_request_bbox,
    truncation_notice,
)


class _FakeSdk:
    """Cell size of 1°x1° for every probe, so estimates are exact and readable."""

    def __init__(self, *, factor: int = 32, cell_span: float = 1.0, locate_fails: bool = False,
                 cells_per_bbox: float = 0.0) -> None:
        self.factor = factor
        self.cell_span = cell_span
        self.locate_fails = locate_fails
        self.cells_per_bbox = cells_per_bbox
        self.cover_calls: list[list[float] | None] = []
        self.children_calls: list[int] = []

    def locate(self, **_kwargs):
        if self.locate_fails:
            raise RuntimeError("outside the valid domain")
        return SimpleNamespace(bbox=[0.0, 0.0, self.cell_span, self.cell_span])

    def cover(self, **kwargs):
        bbox = kwargs.get("bbox")
        self.cover_calls.append(bbox)
        if self.cells_per_bbox:
            return [object() for _ in range(int(self.cells_per_bbox))]
        return [object() for _ in range(estimate_cover_cells(
            self, grid_type=preview_limits.grid_type_value(kwargs.get("grid_type")),
            grid_level=int(kwargs.get("requested_grid_level")), bbox=bbox,
        ) or 1)]

    def children(self, _address, target_grid_level: int):
        self.children_calls.append(target_grid_level)
        return [object() for _ in range(self.factor)]


def _address(level: int) -> SimpleNamespace:
    return SimpleNamespace(grid_type="geohash", grid_level=level, space_code="ww", topology_code=None)


def test_default_limits_are_ten_thousand(monkeypatch) -> None:
    monkeypatch.delenv("CUBE_WEB_PREVIEW_MAX_CELLS", raising=False)
    monkeypatch.delenv("CUBE_WEB_PREVIEW_MAX_CHILDREN", raising=False)

    assert preview_max_cells() == DEFAULT_MAX_PREVIEW_CELLS == 10_000
    assert preview_max_children() == DEFAULT_MAX_PREVIEW_CHILDREN == 10_000


def test_limits_are_configurable_and_ignore_bad_values(monkeypatch) -> None:
    monkeypatch.setenv("CUBE_WEB_PREVIEW_MAX_CELLS", "250")
    monkeypatch.setenv("CUBE_WEB_PREVIEW_MAX_CHILDREN", "not-a-number")

    assert preview_max_cells() == 250
    assert preview_max_children() == DEFAULT_MAX_PREVIEW_CHILDREN


def test_resolve_request_bbox_prefers_bbox_and_falls_back_to_geometry() -> None:
    assert resolve_request_bbox([1, 2, 3, 4], None) == [1.0, 2.0, 3.0, 4.0]
    assert resolve_request_bbox(None, {"type": "Polygon", "coordinates": [[[1, 2], [3, 2], [3, 5], [1, 2]]]}) == [
        1.0, 2.0, 3.0, 5.0,
    ]
    assert resolve_request_bbox([3, 4, 1, 2], None) is None  # inverted bbox
    assert resolve_request_bbox(None, None) is None


def test_estimate_cover_cells_uses_measured_cell_size() -> None:
    sdk = _FakeSdk(cell_span=0.5)

    assert estimate_cover_cells(sdk, grid_type="geohash", grid_level=6, bbox=[0.0, 0.0, 2.0, 1.0]) == 4 * 2


def test_estimate_cover_cells_returns_none_without_extent_or_domain() -> None:
    assert estimate_cover_cells(_FakeSdk(), grid_type="geohash", grid_level=6, bbox=None) is None
    assert estimate_cover_cells(_FakeSdk(locate_fails=True), grid_type="mgrs", grid_level=6, bbox=[0, 0, 1, 1]) is None


def _cover(sdk, *, bbox, geometry=None, level=7):
    return cover_preview_cells(
        sdk, grid_type="geohash", grid_level=level, cover_mode="intersect",
        boundary_type="polygon", bbox=bbox, geometry=geometry, crs="EPSG:4326",
    )


def test_cover_within_limit_returns_everything_untruncated() -> None:
    sdk = _FakeSdk(cell_span=0.01)  # 1°x1° -> 10,000 cells, exactly at the limit

    cells, truncated = _cover(sdk, bbox=[0.0, 0.0, 1.0, 1.0])

    assert truncated is False
    assert len(cells) == 10_000
    assert len(sdk.cover_calls) == 1  # a single cover, no stripping


def test_cover_over_limit_is_truncated_instead_of_rejected() -> None:
    sdk = _FakeSdk(cell_span=0.001)  # 1°x1° -> 1,000,000 cells estimated

    cells, truncated = _cover(sdk, bbox=[0.0, 0.0, 1.0, 1.0])

    assert truncated is True
    assert len(cells) == DEFAULT_MAX_PREVIEW_CELLS
    # each cover stayed inside one strip, so no call ever asked for the whole extent
    assert all(call is not None and call != [0.0, 0.0, 1.0, 1.0] for call in sdk.cover_calls)
    assert len(sdk.cover_calls) >= 1


def test_cover_strips_never_exceed_the_limit_per_call() -> None:
    sdk = _FakeSdk(cell_span=0.001)

    _cover(sdk, bbox=[0.0, 0.0, 1.0, 1.0])

    for strip in sdk.cover_calls:
        assert estimate_cover_cells(sdk, grid_type="geohash", grid_level=7, bbox=strip) <= DEFAULT_MAX_PREVIEW_CELLS


def test_cover_truncates_when_the_estimate_under_counts() -> None:
    sdk = _FakeSdk(cell_span=0.01, cells_per_bbox=DEFAULT_MAX_PREVIEW_CELLS + 500)

    cells, truncated = _cover(sdk, bbox=[0.0, 0.0, 1.0, 1.0])

    assert truncated is True
    assert len(cells) == DEFAULT_MAX_PREVIEW_CELLS


def test_cover_geometry_only_over_limit_still_rejected() -> None:
    sdk = _FakeSdk(cell_span=0.0005)  # 1°x1° geometry -> 4,000,000 cells
    geometry = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]}

    with pytest.raises(HTTPException) as excinfo:
        _cover(sdk, bbox=None, geometry=geometry)

    assert excinfo.value.status_code == 413
    assert excinfo.value.detail["code"] == "preview_limit_exceeded"
    assert "剖分" not in excinfo.value.detail["message"]


def test_truncation_notice_names_the_limit() -> None:
    assert truncation_notice(10_000) == "最多显示 10,000 个格网，多余格网已截断"


def test_children_within_limit_passes_small_gap() -> None:
    sdk = _FakeSdk(factor=32)

    total = ensure_preview_children_within_limit(sdk, address=_address(4), target_grid_level=6)

    assert total == 32**2 == 1_024
    assert sdk.children_calls == [5]


def test_children_over_limit_is_rejected_before_generating() -> None:
    sdk = _FakeSdk(factor=32)  # 32^3 = 32,768 > 10,000

    with pytest.raises(HTTPException) as excinfo:
        ensure_preview_children_within_limit(sdk, address=_address(4), target_grid_level=7)

    assert excinfo.value.status_code == 413
    assert excinfo.value.detail["estimated"] == 32_768
    assert "32,768" in excinfo.value.detail["message"]
    # only the single-level probe was materialised, never the full child list
    assert sdk.children_calls == [5]


def test_children_with_mgrs_branching_factor_is_rejected_earlier() -> None:
    sdk = _FakeSdk(factor=100)  # MGRS subdivides by 100 per level

    # gap 2 is exactly 10,000 children (at the limit); gap 3 is 1,000,000
    assert ensure_preview_children_within_limit(sdk, address=_address(1), target_grid_level=3) == 10_000
    with pytest.raises(HTTPException) as excinfo:
        ensure_preview_children_within_limit(sdk, address=_address(1), target_grid_level=4)
    assert excinfo.value.detail["estimated"] == 1_000_000


def test_children_same_or_coarser_level_defers_to_engine_validation() -> None:
    sdk = _FakeSdk()

    assert ensure_preview_children_within_limit(sdk, address=_address(6), target_grid_level=6) == 0
    assert sdk.children_calls == []


def test_children_engine_failure_is_not_masked() -> None:
    class _Failing(_FakeSdk):
        def children(self, _address, target_grid_level: int):
            raise ValueError("target_grid_level must be greater")

    assert ensure_preview_children_within_limit(
        _Failing(), address=_address(4), target_grid_level=5,
    ) == 0


def test_routes_use_the_guard(monkeypatch) -> None:
    """The guard must be wired into both preview endpoints, not just unit-testable."""
    from cube_web.routes import sdk as sdk_routes

    source = sdk_routes.create_sdk_router.__wrapped__.__code__ if hasattr(sdk_routes.create_sdk_router, "__wrapped__") else None
    del source  # routes are closures; assert on the module source instead

    import inspect

    text = inspect.getsource(sdk_routes)
    assert "cover_preview_cells(" in text
    assert "truncation_notice(" in text
    assert "ensure_preview_children_within_limit(" in text


def test_preview_limits_module_exports_env_names() -> None:
    assert preview_limits.MAX_CELLS_ENV == "CUBE_WEB_PREVIEW_MAX_CELLS"
    assert preview_limits.MAX_CHILDREN_ENV == "CUBE_WEB_PREVIEW_MAX_CHILDREN"


class _StrictGridTypeSdk(_FakeSdk):
    """A fake that rejects anything but the real SDK string values.

    Guards against the ``str(GridType.GEOHASH) == "GridType.GEOHASH"`` mistake that
    silently disabled the estimate and made a 3°x3° L7 preview cover the whole
    extent (160 s) instead of one bounded strip.
    """

    VALID = {"geohash", "mgrs", "isea4h"}

    def locate(self, **kwargs):
        assert kwargs.get("grid_type") in self.VALID, f"bad grid_type: {kwargs.get('grid_type')!r}"
        return super().locate(**kwargs)


def test_cover_accepts_grid_type_enum_values() -> None:
    from grid_core.app.core.enums import BoundaryType, GridType

    sdk = _StrictGridTypeSdk(cell_span=0.001)  # over the limit -> must go through strips

    cells, truncated = cover_preview_cells(
        sdk, grid_type=GridType.GEOHASH, grid_level=7, cover_mode="intersect",
        boundary_type=BoundaryType.POLYGON, bbox=[0.0, 0.0, 1.0, 1.0], geometry=None, crs="EPSG:4326",
    )

    assert truncated is True and len(cells) == DEFAULT_MAX_PREVIEW_CELLS
    # never cover the whole extent in one call: every call must be a bounded strip
    assert all(call != [0.0, 0.0, 1.0, 1.0] for call in sdk.cover_calls)
    # the first strip already reaches the limit, so exactly one bounded cover happens
    assert len(sdk.cover_calls) == 1
    assert sdk.cover_calls[0][3] < 1.0
