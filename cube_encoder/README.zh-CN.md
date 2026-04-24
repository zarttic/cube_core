# Cube Encoder

`cube_encoder` 是 cube 项目的底层格网编码 SDK 与 API 提供方，负责空间格网剖分、时空编码、拓扑元操作和统一能力输出。其他包应通过 `grid_core.sdk.CubeEncoderSDK` 或 HTTP API 使用这些能力，不应复制格网实现逻辑。

英文说明见 [README.md](README.md)。

## 核心能力

- 支持 `geohash`、`mgrs`、H3-backed `isea4h` 的点定位与几何覆盖。
- 支持时空编码生成、批量生成与解析。
- 支持邻接、父级、子级、编码转几何、批量编码转几何。
- Python SDK 入口：`grid_core.sdk.CubeEncoderSDK`。
- FastAPI 服务入口：`/v1`。

## 快速开始

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn grid_core.app.main:app --host 0.0.0.0 --port 50012 --reload
```

## SDK 使用

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

构建并安装 wheel：

```bash
python -m build
pip install dist/cube_encoder-*.whl
```

## API 示例

```bash
curl -X POST http://127.0.0.1:50012/v1/grid/locate \
  -H 'Content-Type: application/json' \
  -d '{"grid_type":"geohash","level":7,"point":[116.391,39.907]}'

curl -X POST http://127.0.0.1:50012/v1/code/st \
  -H 'Content-Type: application/json' \
  -d '{"grid_type":"geohash","level":7,"space_code":"wtw3sjq","timestamp":"2026-03-09T15:30:00Z","time_granularity":"minute","version":"v1"}'
```

## 测试

在本包内运行：

```bash
python -m pytest -q tests
python -m grid_core.app.perf_smoke
```

在当前工作区根目录运行跨包测试：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web pytest cube_encoder/tests cube_split/tests cube_web/tests
```

## 文档入口

- [文档索引](docs/README.md)
- [架构说明](docs/ARCHITECTURE.md)
- [入库与存储设计归档](docs/INGEST_STORAGE_DESIGN.md)
- [项目历史](docs/PROJECT_HISTORY.md)
- [SDK 发布规范](docs/SDK_RELEASE.md)
- [开发日志](docs/DEVELOPMENT_LOG.md)
- [Bug 日志](docs/BUG_LOG.md)
- [变更记录](CHANGELOG.md)

## 职责边界

- `cube_encoder`：底层格网、拓扑、时空编码能力。
- `cube_split`：剖分、入库与 AOI 读取链路。
- `cube_web`：Web 页面与可视化展示。
