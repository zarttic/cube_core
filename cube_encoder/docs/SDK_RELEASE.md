# SDK 发布规范

## 版本规则

- 使用 SemVer：`MAJOR.MINOR.PATCH`。
- `MAJOR`：不向后兼容的 SDK/API 契约变更。
- `MINOR`：向后兼容的新能力，例如新增引擎能力、新 SDK 方法或行为扩展。
- `PATCH`：不破坏契约的 bug 修复、性能优化或文档变更。

## 发布检查清单

1. 更新 `pyproject.toml` 版本号。
2. 在 `CHANGELOG.md` 增加发布记录。
3. 确认测试通过：
   - `python3.11 -m pytest -q tests`
   - `python3.11 -m grid_core.app.perf_smoke`
4. 确认 CI 中的包构建和安装检查通过：
   - `python3.11 -m build`
   - wheel 安装烟测
   - sdist 安装烟测

## 性能基线管理

- 阈值集中维护在 `.github/perf-thresholds.env`。
- CI 每次运行导出 `perf-smoke.json` artifact，用于趋势检查。

## API/SDK 兼容性说明

- 响应中允许新增可选字段。
- 同一个 `MAJOR` 版本内，既有字段语义必须保持稳定。
