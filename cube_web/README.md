# cube_web

`cube_web` hosts the FastAPI web shell and static demo pages for cube visualization. It does not implement grid logic. Grid operations come from `cube_encoder` through the configured encoder backend.

Detailed web documentation: [docs/README.md](docs/README.md).

## Boundary

- `cube_web` owns HTTP hosting for pages, static assets, and visualization UX.
- `cube_encoder` owns grid locate, cover, topology, and space-time code behavior.
- `cube_split` owns partition, ingest, and AOI readback workflows.

## Encoder Backend

By default the frontend targets:

```text
http://127.0.0.1:50012
```

Override the encoder base at runtime:

```text
http://127.0.0.1:50040/encoding?encoderBase=http://127.0.0.1:50012
```

## Run

```bash
python -m venv .venv
source .venv/bin/activate
cd ../cube_encoder
python -m pip install --upgrade pip build
python -m build
python -m pip install --force-reinstall dist/*.whl
cd ../cube_web
pip install -r requirements.txt
uvicorn cube_web.app:app --host 0.0.0.0 --port 50040 --reload
```

## Tests

From this package:

```bash
PYTHONPATH=../cube_encoder:. pytest tests
```

From the workspace root, run the cross-package pytest command in `AGENTS.md`.
