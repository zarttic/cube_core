# cube_encoder

`cube_encoder` is the core grid SDK and API provider for the cube project. It owns discrete grid encoding, space-time code generation, and topology operations. Other packages should consume these capabilities through `grid_core.sdk.CubeEncoderSDK` or the HTTP API, not by duplicating grid logic.

中文说明见 [README.zh-CN.md](README.zh-CN.md).

## Capabilities

- Grid locate and cover for `geohash`, `mgrs`, and H3-backed `isea4h`.
- Space-time code generation and parsing.
- Topology operations: neighbors, parent, children, geometry, and batch geometry.
- Python SDK entry point: `grid_core.sdk.CubeEncoderSDK`.
- FastAPI service exposing the same core capabilities under `/v1`.

## Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn grid_core.app.main:app --host 0.0.0.0 --port 50012 --reload
```

## SDK Usage

```bash
pip install -e .
```

```python
from datetime import datetime, timezone

from grid_core.sdk import CubeEncoderSDK

sdk = CubeEncoderSDK()
cell = sdk.locate(grid_type="geohash", level=7, point=[116.391, 39.907])
neighbors = sdk.neighbors(grid_type="geohash", code=cell.space_code, k=1)
st_code = sdk.generate_st_code(
    grid_type="geohash",
    level=7,
    space_code=cell.space_code,
    timestamp=datetime(2026, 3, 9, 15, 30, tzinfo=timezone.utc),
    time_granularity="minute",
    version="v1",
).st_code
```

Build/install as package:

```bash
python -m build
pip install dist/cube_encoder-*.whl
```

## API Examples

```bash
curl -X POST http://127.0.0.1:50012/v1/grid/locate \
  -H 'Content-Type: application/json' \
  -d '{"grid_type":"geohash","level":7,"point":[116.391,39.907]}'

curl -X POST http://127.0.0.1:50012/v1/code/st \
  -H 'Content-Type: application/json' \
  -d '{"grid_type":"geohash","level":7,"space_code":"wtw3sjq","timestamp":"2026-03-09T15:30:00Z","time_granularity":"minute","version":"v1"}'
```

## Tests

From this package:

```bash
python -m pytest -q tests
python -m grid_core.app.perf_smoke
```

From this workspace root:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web pytest cube_encoder/tests cube_split/tests cube_web/tests
```

## Documentation

- [Documentation index](docs/README.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Ingest and storage design archive](docs/INGEST_STORAGE_DESIGN.md)
- [Project history](docs/PROJECT_HISTORY.md)
- [SDK release policy](docs/SDK_RELEASE.md)
- [Development log](docs/DEVELOPMENT_LOG.md)
- [Bug log](docs/BUG_LOG.md)
- [Changelog](CHANGELOG.md)

## Package Boundary

- `cube_encoder`: core grid, topology, and space-time coding capabilities.
- `cube_split`: partition, ingest, and AOI readback workflows.
- `cube_web`: web pages and visualization.
