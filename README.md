# cube_core

Python monorepo for grid encoding, remote-sensing partitioning, ingest/readback,
and the web demo shell.

## Packages

- `cube_encoder`: grid locate/cover, topology, space-time code APIs, and the
  `grid_core.sdk.CubeEncoderSDK` provider.
- `cube_split`: optical/product/carbon partitioning, Ray/local execution,
  ingest to PostgreSQL/MinIO or local backends, quality checks, and AOI readback.
- `cube_web`: FastAPI host, Vue-built static UI, in-process SDK API facade,
  partition demo endpoints, and quality-report endpoints.

`cube_encoder` owns grid logic. Other packages consume it through
`grid_core.sdk.CubeEncoderSDK` or the web SDK facade.

## Documentation

- `cube_encoder/docs/README.md`: encoder architecture, SDK/API boundary, release
  notes, and historical design index.
- `cube_split/docs/README.md`: partition, ingest, quality, manifest, and readback
  documentation.
- `cube_web/docs/README.md`: web host, routes, frontend build, and tests.
- `AGENTS.md`: repository operating notes, default test commands, and local
  infrastructure references.

## Development Commands

Default cross-package tests from the repository root:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web pytest cube_encoder/tests cube_split/tests
```

The `cube_web/tests` suite is intentionally excluded from the default command
until its static-file response issue is fixed. For narrow web changes, run:

```bash
cd cube_web
PYTHONPATH=../cube_encoder:. pytest tests
```

Build the encoder package:

```bash
cd cube_encoder
python -m build
```

Run the web UI with the in-repo SDK backend:

```bash
PYTHONPATH=cube_encoder:cube_web uvicorn cube_web.app:app --host 0.0.0.0 --port 50040
```

## Current Workflow Snapshot

1. Use `cube_split` to standardize raster assets to COG where needed.
2. Use `cube_encoder` through the SDK to generate `space_code`, `st_code`, and
   cell/window metadata.
3. Write partition outputs under `cube_split/data/ray_output/.../run_*`.
4. Ingest metadata/assets into PostgreSQL + MinIO, or use SQLite/local storage
   for local debugging.
5. Run quality checks from `cube_split.quality` or the `cube_web` quality API.
6. Query AOI readback through `cube_split.read`.

Historical and one-off run reports have been consolidated into the package docs;
current commands in package README files take precedence over archived notes.
