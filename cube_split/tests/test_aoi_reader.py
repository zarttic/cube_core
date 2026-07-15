from __future__ import annotations

import pytest

from cube_split.read.aoi_reader import read_aoi_rgb


def test_aoi_reader_uses_frozen_sdk_cover_signature(monkeypatch, tmp_path):
    calls: list[dict[str, object]] = []

    class FakeSDK:
        def cover_compact(self, grid_type, requested_grid_level, cover_mode, bbox, crs):
            calls.append(
                {
                    "grid_type": grid_type,
                    "requested_grid_level": requested_grid_level,
                    "cover_mode": cover_mode,
                    "bbox": bbox,
                    "crs": crs,
                }
            )
            return []

    monkeypatch.setattr("cube_split.read.aoi_reader.CubeEncoderSDK", FakeSDK)
    monkeypatch.setattr("cube_split.read.aoi_reader._query_value_refs", lambda **_kwargs: [])
    with pytest.raises(RuntimeError, match="No cube rows matched"):
        read_aoi_rgb(
            bbox=[116.3, 39.8, 116.4, 39.9],
            time_bucket="20260424",
            bands=["sr_b2"],
            output=str(tmp_path / "aoi.tif"),
            grid_type="geohash",
        )

    assert calls == [
        {
            "grid_type": "geohash",
            "requested_grid_level": 7,
            "cover_mode": "intersect",
            "bbox": [116.3, 39.8, 116.4, 39.9],
            "crs": "EPSG:4326",
        }
    ]
