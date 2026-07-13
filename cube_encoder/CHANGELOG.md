# 变更记录

本文记录 SDK/API 的重要变更。

## Unreleased

- Add `plane_grid` ST code parsing/validation for source-CRS logical partition rows.
- Keep `mgrs` available in the encoder SDK while removing it from the Web production partition selector.

## [0.2.0] - 2026-03-11
- 新增可安装 Python SDK facade：`grid_core.sdk.CubeEncoderSDK`。
- 实现 `mgrs` 的 `cover_mode=minimal`。
- 为 `s2`、`mgrs`、`isea4h` 增加 minimal 合并降级行为。
- 增加日期变更线、分区边界和极区场景的引擎稳定性测试。
- 增加性能烟测 JSON 产物导出和 CI artifact 上传。
- 在 CI 中增加 SDK 包构建、wheel 安装和 sdist 安装烟测。

## [0.1.0] - 2026-03-11
- 首次增加可安装包元数据（`pyproject.toml`）。
- 核心能力：`s2`、`mgrs`、`isea4h` 格网、拓扑和 ST code API。
