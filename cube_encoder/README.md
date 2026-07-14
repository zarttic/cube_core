# cube_encoder

更新时间：2026-07-14

`cube_encoder` 是 cube 项目的底层格网编码 SDK 与 API 提供方，负责离散格网编码、
时空编码、拓扑元操作和统一能力输出。其他包应通过 `grid_core.sdk.CubeEncoderSDK`
或 HTTP API 使用这些能力，不应复制格网实现逻辑。

## 核心能力

- 生产格网严格限定为三类：`geohash`（经纬度格网，logical）、`mgrs`（平面格网，logical）、`isea4h`（六边形格网，entity）。
- 支持三类格网的点定位、几何覆盖、拓扑元操作和 ST code 生成/解析。
- `isea4h` 为纯 Python 实现，对齐 DGGRID v8.44（ISEA 投影、HEXAGON、PURE aperture 4、WGS84 authalic 半径、朝向 `(11.25°, 58.28252559°, 0°)`）；运行时不依赖 H3 或 DGGRID，`space_code` 为 DGGRID `SEQNUM`（1 基十进制字符串）。
- `mgrs` 结果同时携带标准 `space_code`（标准 UTM/UPS MGRS）与 `topology_code`（`mgrs-topo-v1:<domain>:<level>:<space_code>`）；`geohash`/`isea4h` 的 `topology_code` 为空。
- 请求层级字段统一为 `requested_grid_level`；返回单元保留其实际 `grid_level`（含 `minimal` 覆盖的混合层级）。
- 原生层级范围：Geohash `1..12`、MGRS 精度 `0..5`、ISEA4H 分辨率 `0..15`。
- 支持时空编码生成、批量生成与解析；邻接、父级、子级、编码转几何、批量编码转几何。
- Python SDK 入口：`grid_core.sdk.CubeEncoderSDK`。
- FastAPI 服务入口：`/v1`。

## 快速开始

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python3.11 -m pip install -e ".[dev]"
python3.11 -m uvicorn grid_core.app.main:app --host 0.0.0.0 --port 50012 --reload
```

## SDK 使用

```bash
python3.11 -m pip install -e .
```

```python
from datetime import datetime, timezone

from grid_core.sdk import CubeEncoderSDK

sdk = CubeEncoderSDK()
cell = sdk.locate(grid_type="geohash", requested_grid_level=6, point=[116.391, 39.907])
neighbors = sdk.neighbors(address=cell, k=1)
st_code = sdk.generate_st_code(
    address=cell,
    timestamp=datetime(2026, 3, 9, 15, 30, tzinfo=timezone.utc),
    time_granularity="minute",
).st_code
```

构建并安装 wheel：

```bash
python3.11 -m build
python3.11 -m pip install dist/cube_encoder-*.whl
```

## API 示例

```bash
curl -X POST http://127.0.0.1:50012/v1/grid/locate \
  -H 'Content-Type: application/json' \
  -d '{"grid_type":"geohash","requested_grid_level":6,"point":[116.391,39.907]}'

curl -X POST http://127.0.0.1:50012/v1/code/st \
  -H 'Content-Type: application/json' \
  -d '{"address":{"grid_type":"geohash","grid_level":6,"space_code":"wx4g0b"},"timestamp":"2026-03-09T15:30:00Z","time_granularity":"minute"}'
```

## 测试

在本包内运行：

```bash
python3.11 -m pytest -q tests
python3.11 -m grid_core.app.perf_smoke
```

在仓库根目录运行跨包测试：

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests cube_split/tests
```

## 文档入口

- [文档索引](docs/README.md)
- [架构说明](docs/ARCHITECTURE.md)
- [SDK 发布规范](docs/SDK_RELEASE.md)
- [变更记录](CHANGELOG.md)

## 职责边界

- `cube_encoder`：底层格网、拓扑和时空编码能力。
- `cube_split`：剖分、入库、质检和 AOI 读取链路。
- `cube_web`：Web 管理入口、SDK facade、托管剖分 API 和质检报告展示。
