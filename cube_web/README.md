# cube_web

`cube_web` hosts the FastAPI web shell, the Vue-built static UI, and web-facing
API facades for encoder SDK operations, partition demos, and quality reports.

Detailed web documentation: [docs/README.md](docs/README.md).

## Boundary

- `cube_web` owns HTTP hosting, static assets, visualization UX, API request
  shaping, partition demo orchestration, and quality-report presentation.
- `cube_encoder` owns grid locate, cover, topology, and space-time code behavior.
- `cube_split` owns partition, ingest, quality-check implementation, and AOI
  readback workflows.

## Run

From the repository root:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web uvicorn cube_web.app:app --host 0.0.0.0 --port 50040
```

For local frontend development:

```bash
cd cube_web/frontend
npm install
npm run dev
```

The built frontend assets are served from `cube_web/cube_web/web/`.

## API Surface

All API routes are under `/v1`:

- `/v1/grid/*`, `/v1/topology/*`, `/v1/code/*`: in-process
  `CubeEncoderSDK` facade.
- `/v1/partition/{data_type}/demo`: synchronous partition demo for
  `optical`, `carbon`, or `product`.
- `/v1/partition/{data_type}/retry`: retry using the previous request payload.
- `/v1/partition/{data_type}/tasks/demo` and `/tasks/retry`: asynchronous
  partition task submission.
- `/v1/quality/{optical|product}/run`, `/latest`, `/report`, `/report/pdf`,
  `/history`: quality report workflow.

## Tests

From this package:

```bash
PYTHONPATH=../cube_encoder:../cube_split:. pytest tests
```

From the workspace root, run the cross-package pytest command in `AGENTS.md`.
