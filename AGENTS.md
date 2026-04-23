# Repository Guidelines

## Project Structure & Module Organization

This repository is organized as a Python monorepo under `cube_project/`.

- `cube_project/cube_encoder/` contains the core grid SDK and API models in `grid_core/`, with tests in `tests/`.
- `cube_project/cube_split/` contains partitioning, Ray ingest, AOI reading, and job code in `cube_split/`, with tests in `tests/`.
- `cube_project/cube_web/` contains the FastAPI web host in `cube_web/app.py`, static pages and assets in `cube_web/web/`, and tests in `tests/`.
- Project notes and implementation docs live in each package’s `docs/` directory where present.

## Build, Test, and Development Commands

Run commands from the repository root unless noted.

```bash
PYTHONPATH=cube_project/cube_encoder:cube_project/cube_split:cube_project/cube_web pytest cube_project/cube_encoder/tests cube_project/cube_split/tests cube_project/cube_web/tests
```

Runs the full test suite across all three packages.

```bash
cd cube_project/cube_encoder && python -m build
```

Builds the `cube-encoder` package distribution.

```bash
PYTHONPATH=cube_project/cube_encoder:cube_project/cube_web uvicorn cube_web.app:app --host 0.0.0.0 --port 50040
```

Runs the web UI locally using the in-repo SDK backend.

## Coding Style & Naming Conventions

Use Python 3.11+ and follow existing style: 4-space indentation, type hints for public functions, and small focused modules. Keep package names lowercase with underscores, for example `grid_core`, `cube_split`, and `cube_web`. Tests use `test_*.py` filenames and descriptive `test_*` functions. Keep frontend assets in plain HTML/CSS/JS under `cube_web/web/`.

## Testing Guidelines

The project uses `pytest`. Add or update tests beside the package being changed. For SDK/API changes, cover both service behavior and FastAPI endpoint behavior where applicable. Before pushing, run the full cross-package pytest command above; for narrow web changes, also run:

```bash
PYTHONPATH=../cube_encoder:. pytest tests
```

from `cube_project/cube_web/`.

## Commit & Pull Request Guidelines

Recent history uses short imperative messages, sometimes with prefixes such as `feat:`, `docs:`, or scoped forms like `feat(partition):`. Keep commits focused and describe the user-visible change, for example `Update cube web SDK backend and UI`.

Pull requests should include a short summary, affected package paths, validation commands and results, and screenshots for visible UI changes. Link related issues or design notes when available.

## Security & Configuration Tips

Do not commit local data, generated caches, `.pytest_cache/`, `__pycache__/`, virtual environments, or large ingest inputs. Keep service endpoints configurable; avoid hard-coding machine-specific IPs in committed frontend code.
