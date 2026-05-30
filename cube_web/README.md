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

Quality reports use PostgreSQL storage. Local development defaults to the
Podman PostgreSQL service at `postgresql://postgres:postgres@127.0.0.1:55432/cube`;
set `CUBE_WEB_POSTGRES_DSN` or `DATABASE_URL` to override it.

Auth enforcement is controlled at runtime with `CUBE_WEB_AUTH_REQUIRED`.
Set `CUBE_WEB_AUTH_REQUIRED=false` for local self-test to skip the frontend
login redirect and the backend `/v1/*` bearer-token check.

Partition demos default to the configured infrastructure cluster: Ray Client
`ray://10.136.1.13:10001`, MinIO API `10.136.1.14:9000`, and bucket `cube`.
The default PostgreSQL DSN above is used for metadata. Set
`CUBE_WEB_RAY_ADDRESS`, `CUBE_WEB_POSTGRES_DSN`, `CUBE_WEB_MINIO_ENDPOINT`,
`CUBE_WEB_MINIO_ACCESS_KEY`, `CUBE_WEB_MINIO_SECRET_KEY`, and
`CUBE_WEB_MINIO_BUCKET` to override them.

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
- `/v1/quality/{optical|product|carbon}/run`, `/latest`, `/report`,
  `/report/pdf`, `/report/txt`, `/history`: quality report workflow.

## Tests

From this package:

```bash
PYTHONPATH=../cube_encoder:../cube_split:. pytest tests
```

From the workspace root, run the cross-package pytest command in `AGENTS.md`.
