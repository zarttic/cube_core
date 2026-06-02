# cube_web

`cube_web` hosts the FastAPI web shell, the Vue-built static UI, and web-facing
API facades for encoder SDK operations, managed partition runs, and quality
reports.

Detailed web documentation: [docs/README.md](docs/README.md).

## Boundary

- `cube_web` owns HTTP hosting, static assets, visualization UX, API request
  shaping, partition task orchestration, and quality-report presentation.
- `cube_encoder` owns grid locate, cover, topology, and space-time code behavior.
- `cube_split` owns partition, ingest, quality-check implementation, and AOI
  readback workflows.

## Run

From the repository root:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.8 -m uvicorn cube_web.app:app --host 0.0.0.0 --port 50040
```

Quality reports and managed partition tasks use PostgreSQL storage. Set
`CUBE_WEB_POSTGRES_DSN`, `POSTGRES_DSN`, or `DATABASE_URL` before using those
workflows.

Auth enforcement is controlled at runtime with `CUBE_WEB_AUTH_REQUIRED`.
Set `CUBE_WEB_AUTH_REQUIRED=false` for local self-test to skip the frontend
login redirect and the backend `/v1/*` bearer-token check.

Partition runs read Ray, MinIO, and PostgreSQL settings from runtime
configuration. Set `CUBE_WEB_RAY_ADDRESS`, `CUBE_WEB_MINIO_ENDPOINT`,
`CUBE_WEB_MINIO_ACCESS_KEY`, `CUBE_WEB_MINIO_SECRET_KEY`, and
`CUBE_WEB_MINIO_BUCKET` when using the distributed backends. MinIO credentials
may also be sourced from the node-local MinIO service environment.

Bundled demo partition batches are opt-in. Set
`CUBE_WEB_LOAD_DEMO_PARTITION_SCHEMAS=1` only in a demo environment; production
startup leaves the partition batch table untouched.

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
- `/v1/partition/{data_type}/run`: synchronous partition run for
  `optical`, `carbon`, `radar`, or `product`.
- `/v1/partition/{data_type}/demo`: backwards-compatible alias for older demo
  clients.
- `/v1/partition/{data_type}/retry`: retry using the previous request payload.
- `/v1/partition/{data_type}/tasks/run` and `/tasks/retry`: asynchronous
  partition task submission.
- `/v1/partition/{data_type}/tasks/demo`: backwards-compatible async alias.
- `/v1/quality/{optical|product|carbon}/run`, `/latest`, `/report`,
  `/report/pdf`, `/report/txt`, `/history`: quality report workflow.

## Tests

From this package:

```bash
PYTHONPATH=../cube_encoder:../cube_split:. python3.8 -m pytest tests
```

From the workspace root, run the cross-package pytest command in `AGENTS.md`.
