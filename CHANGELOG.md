# Changelog

All notable SDK/API changes are documented in this file.

## [0.2.0] - 2026-03-11
- Added installable Python SDK facade: `grid_core.sdk.CubeEncoderSDK`.
- Implemented `mgrs` `cover_mode=minimal`.
- Added minimal coarsening behavior for `geohash`/`mgrs`/`isea4h`.
- Added engine stability tests for dateline, zone boundaries, and polar scenarios.
- Added perf smoke JSON artifact export and CI artifact upload.
- Added SDK package build + wheel/sdist install smoke jobs in CI.

## [0.1.0] - 2026-03-11
- First installable package metadata (`pyproject.toml`).
- Core capabilities: `geohash`/`mgrs`/`isea4h` grid, topology, and ST code APIs.
