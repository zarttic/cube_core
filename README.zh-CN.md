# Cube Encoder（球离散格网剖分与编码引擎）

`cube_encoder` 是一个面向遥感数据场景的底层能力引擎，聚焦“空间对象离散格网剖分 + 时空编码”两类核心能力。

## 项目目标

- 将点、线、面、范围框等空间对象映射为离散格网单元
- 为格网单元生成统一空间编码与时空编码
- 提供邻接、层级、几何反算等编码元操作
- 以统一接口对上层系统输出能力（SDK / HTTP API）

## 当前实现状态（MVP）

当前版本能力状态：

- 空间剖分
  - Geohash：点定位（`/v1/grid/locate`）、几何覆盖（`/v1/grid/cover`，支持 `geometry` 与 `bbox`）
  - MGRS（第一阶段增强）：点定位（`/v1/grid/locate`）、几何覆盖（`/v1/grid/cover`，当前支持 `intersect/contain/minimal`）
- 编码能力
  - 时空编码生成：`/v1/code/st`（支持 `geohash/mgrs/isea4h` 前缀编码）
  - 批量时空编码生成：`/v1/code/st/batch`
  - 时空编码解析：`/v1/code/parse`
- 元操作能力
  - Geohash：邻接计算（`/v1/topology/neighbors`）、父级推导（`/v1/topology/parent`）、子级推导（`/v1/topology/children`）、编码转几何（`/v1/topology/geometry`）
  - MGRS（第一阶段增强）：邻接计算（`/v1/topology/neighbors`）、父级推导（`/v1/topology/parent`）、子级推导（`/v1/topology/children`）、编码转几何（`/v1/topology/geometry`）
  - ISEA4H（第一阶段）：基于 Uber H3 的点定位、几何覆盖、邻接/父子级、编码转几何

## 技术栈

- Python 3.11
- FastAPI
- Pydantic
- Shapely
- pytest

## 快速开始

```bash
pip install -r requirements.txt
uvicorn grid_core.app.main:app --reload
```

作为 Python SDK 使用：

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

启动后可访问：

- 健康检查：`GET /health`
- API 前缀：`/v1`
- 前端可视化：`/v1/demo/map`（支持 API/SDK 两种调用模式，支持 `locate/cover/neighbors/parent/children` 地图展示，并支持绘制 polygon/rectangle 做 cover 预览）
- 批量拓扑几何接口：`/v1/topology/geometries`（用于前端高性能渲染）

## 测试

```bash
python -m pytest -q tests
```

## 性能烟测

```bash
python -m grid_core.app.perf_smoke
```

## 文档规范（开发过程记录）

为满足可追溯开发管理，项目内置以下文档：

- 开发任务记录：`docs/DEVELOPMENT_LOG.md`
- 当前状态与后续计划：`docs/STATUS_AND_PLAN.md`
- Bug 排查记录：`docs/BUG_LOG.md`
- 记录流程规范：`docs/DOC_WORKFLOW.md`

每次开发与排障都需要追加记录（append-only）。

## 后续规划

- ISEA4H 算法分阶段落地
- 批量能力与覆盖精度策略增强（`minimal` 后续可引入跨层级最小覆盖优化）
