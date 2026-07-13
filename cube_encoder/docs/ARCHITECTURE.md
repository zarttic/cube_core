# cube_encoder 架构说明

更新时间：2026-07-13
适用范围：`cube_encoder`

## 1. 定位

`cube_encoder` 是 cube 项目的底层格网能力提供方，聚焦两类能力：

- 将点、线、面、范围框映射为离散格网单元。
- 为格网单元生成空间编码、时空编码，并提供编码元操作。

它不负责对象存储、索引入库、遥感对象管理、数据分发或缓存。`cube_split` 和 `cube_web` 必须通过 SDK 或 API 调用它。

## 2. 能力范围

当前能力包括：

- 空间剖分：`/v1/grid/locate`、`/v1/grid/cover`。
- 时空编码：`/v1/code/st`、`/v1/code/st/batch`、`/v1/code/parse`。
- 拓扑操作：`/v1/topology/neighbors`、`/v1/topology/parent`、`/v1/topology/children`、`/v1/topology/geometry`、`/v1/topology/geometries`。
- SDK 封装：`grid_core.sdk.CubeEncoderSDK`。

支持的格网体系：

- `s2`：定位、覆盖、拓扑和几何反算。
- `mgrs`：定位、覆盖、拓扑和几何反算。
- `tile_matrix`：Web Mercator 平面瓦片格网定位、覆盖和几何反算。
- `isea4h`：基于 Uber H3 的第一阶段可运行能力。
- `plane_grid`：只注册为 ST code 类型，用于承载 `cube_split` 源平面窗口产生的编码；当前没有 encoder engine，因此不能调用 locate、cover 或 topology。

## 3. 分层结构

```text
调用方
  -> SDK / HTTP API
  -> 统一请求与响应模型
  -> grid service / code service / topology service
  -> s2 / mgrs / tile_matrix / isea4h engine
  -> geometry / projection / timecode utilities
```

设计原则：

- API 和 SDK 共享同一套服务层能力。
- 引擎实现可替换，但输出模型保持稳定。
- CRS 默认按 `EPSG:4326` 对外表达，特殊投影细节封装在引擎内部。
- `cover_mode=minimal` 允许返回低于请求层级的格网单元，用于减少复杂边界的冗余覆盖。
- `plane_grid` 不走上述全球拓扑链路；调用 `/v1/grid/cover` 或 `CubeEncoderSDK.cover(grid_type="plane_grid")` 当前会失败，这是有意保留的后续重构边界。

## 4. 调用边界

推荐调用方式：

```python
from grid_core.sdk import CubeEncoderSDK

sdk = CubeEncoderSDK()
cells = sdk.cover(
    grid_type="s2",
    level=7,
    geometry={
        "type": "Polygon",
        "coordinates": [[[116.3, 39.8], [116.5, 39.8], [116.5, 40.0], [116.3, 40.0], [116.3, 39.8]]],
    },
    cover_mode="intersect",
)
```

跨包规则：

- `cube_split` 使用 `CubeEncoderSDK` 计算覆盖、编码和 AOI 对应的 `space_code[]`。
- `cube_web` 可调用 HTTP API，也可经 web backend 间接调用 SDK。
- 不允许在 `cube_split` 或 `cube_web` 中复制格网覆盖、邻接或时空编码逻辑。

## 5. 测试与质量

当前运行基线为 Python 3.11；本机 `python3.11 --version` 为 Python 3.11.6。

本包测试入口：

```bash
python3.11 -m pytest -q tests
```

性能烟测入口：

```bash
python3.11 -m grid_core.app.perf_smoke
```

涉及 SDK/API 行为变化时，应同时检查 `cube_split` 和 `cube_web` 的调用链。
