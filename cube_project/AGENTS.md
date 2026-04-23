# Repository Guidelines

## Project Structure & Module Organization

Python monorepo under `cube_project/`.

- `cube_project/cube_encoder/` contains the core grid SDK and API models in `grid_core/`, with tests in `tests/`.
- `cube_project/cube_split/` contains partitioning, Ray ingest, AOI reading, and jobs in `cube_split/`, with tests in `tests/`.
- `cube_project/cube_web/` contains the FastAPI host in `cube_web/app.py`, static assets in `cube_web/web/`, and tests in `tests/`.
- Docs live in each package’s `docs/` directory where present.

`cube_encoder` is the SDK provider. Other packages must consume encoder capability through `grid_core.sdk.CubeEncoderSDK` or the web SDK backend, not duplicate grid logic.

## Build, Test, and Development Commands

```bash
PYTHONPATH=cube_project/cube_encoder:cube_project/cube_split:cube_project/cube_web pytest cube_project/cube_encoder/tests cube_project/cube_split/tests cube_project/cube_web/tests
```

Runs all package tests.

```bash
cd cube_project/cube_encoder && python -m build
```

Builds the `cube-encoder` distribution.

```bash
PYTHONPATH=cube_project/cube_encoder:cube_project/cube_web uvicorn cube_web.app:app --host 0.0.0.0 --port 50040
```

Runs the web UI with the in-repo SDK backend.

## Coding Style & Naming Conventions

Use Python 3.11+, 4-space indentation, type hints for public functions, and focused modules. Package names are lowercase with underscores, for example `grid_core`, `cube_split`, and `cube_web`. Tests use `test_*.py` files and descriptive `test_*` functions. Keep frontend code in plain HTML/CSS/JS.

## Execution Rules

- Prefer the smallest effective change; avoid unrelated refactors.
- Do not adjust public interfaces across packages unless the task explicitly requires it.
- When changing API behavior, check the `cube_web` call chain and update tests together.
- Before adding dependencies, confirm an existing dependency cannot solve the need.
- Do not casually move directories or rename public modules.

## Testing Guidelines

The project uses `pytest`. Add or update tests beside the package being changed. For SDK/API changes, cover service behavior and FastAPI endpoints where applicable. Before pushing, run the full cross-package pytest command above; for narrow web changes, also run:

```bash
PYTHONPATH=../cube_encoder:. pytest tests
```

from `cube_project/cube_web/`.

## Commit & Pull Request Guidelines

Recent history uses short imperative messages, sometimes with prefixes such as `feat:`, `docs:`, or `feat(partition):`. Keep commits focused and user-visible, for example `Update cube web SDK backend and UI`.

Use `gh` CLI for GitHub publishing. Before every push, run the full cross-package pytest command and include the result in the PR or handoff. PRs should include a summary, affected paths, validation results, UI screenshots, and related issues or notes when available.

## Security & Configuration Tips

Do not commit local data, caches, `.pytest_cache/`, `__pycache__/`, virtual environments, or large ingest inputs. Keep service endpoints configurable; avoid hard-coding machine-specific IPs.
