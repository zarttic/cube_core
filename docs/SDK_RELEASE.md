# SDK Release Policy

## Versioning rule
- Use SemVer: `MAJOR.MINOR.PATCH`.
- `MAJOR`: backward-incompatible SDK/API contract change.
- `MINOR`: backward-compatible feature (new engine capability, new SDK method, behavior extension).
- `PATCH`: bug fix/performance/documentation changes without contract break.

## Required release checklist
1. Update `pyproject.toml` version.
2. Add release note entry in `CHANGELOG.md`.
3. Ensure tests pass:
   - `python -m pytest -q tests`
   - `python -m grid_core.app.perf_smoke`
4. Ensure package build/install checks pass in CI:
   - `python -m build`
   - wheel install smoke test
   - sdist install smoke test

## Perf baseline governance
- Thresholds are centralized in `.github/perf-thresholds.env`.
- CI exports `perf-smoke.json` artifact for every run to support trend inspection.

## API/SDK compatibility note
- New optional fields are allowed in responses.
- Existing field semantics must remain stable within the same MAJOR version.
