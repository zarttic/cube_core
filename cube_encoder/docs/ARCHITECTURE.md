# cube_encoder 架构说明

更新时间：2026-07-17
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

支持的格网体系（生产格网严格限定为三类）：

- `geohash`（经纬度格网，logical）：定位、覆盖、拓扑和几何反算；`space_code` 为 base32 geohash，层级 `1..12`。
- `mgrs`（平面格网，logical）：标准 UTM/UPS MGRS 定位、覆盖、拓扑和几何反算；以标准 `space_code` 作为唯一格网身份，`topology_code` 为空，精度 `0..5`。
- `isea4h`（六边形格网，entity）：纯 Python 实现，对齐 DGGRID v8.44（ISEA 投影、HEXAGON、PURE aperture 4、WGS84 authalic 半径、朝向 `(11.25°, 58.28252559°, 0°)`）；`space_code` 为 DGGRID `SEQNUM`，分辨率 `0..15`；运行时不依赖 H3 或 DGGRID，固定权威向量由 DGGRID 生成并提交为测试基线。

Current production grid contract: `geohash` and `mgrs` use logical partitioning; `isea4h` uses entity partitioning. Native levels are Geohash `1..12`, MGRS `0..5`, and ISEA4H `0..15`.

## 3. 分层结构

```text
调用方
  -> SDK / HTTP API
  -> 统一请求与响应模型
  -> grid service / code service / topology service
  -> geohash / mgrs / isea4h engine
  -> geometry / projection / timecode utilities
```

设计原则：

- API 和 SDK 共享同一套服务层能力。
- 引擎实现可替换，但输出模型保持稳定。
- CRS 默认按 `EPSG:4326` 对外表达，特殊投影细节封装在引擎内部。
- ISEA4H cover 从 AOI 各连通分量定位局部格网，并沿六边拓扑遍历实际相交候选，不按目标层级枚举全球格网；候选上限按本次请求实际访问的格网计数。`intersect` 只保留正面积相交单元，`contain` 只保留被 AOI 完整覆盖的单元；`minimal` 可返回低层级单元。
- `cover_mode=minimal` 允许返回低于请求层级的格网单元，用于减少复杂边界的冗余覆盖。
- 请求层级字段统一为 `requested_grid_level`；返回单元保留实际 `grid_level`。拓扑与几何操作以 `GridAddress` 为入参，因为 ISEA4H 的 seqnum 只有连同分辨率才有意义；MGRS 使用标准 `space_code` 标识格网。
- ISEA4H `space_code` 为未补零的十进制 DGGRID SEQNUM，且 `cell_count(r) = 10 * 4**r + 2`。`minimal` cover 可以返回不同于请求层级的 cell；运行时不依赖 H3 或 DGGRID。

## 4. 调用边界

推荐调用方式：

```python
from grid_core.sdk import CubeEncoderSDK

sdk = CubeEncoderSDK()
cells = sdk.cover(
    grid_type="geohash",
    requested_grid_level=6,
    cover_mode="intersect",
    boundary_type="polygon",
    geometry={
        "type": "Polygon",
        "coordinates": [[[116.3, 39.8], [116.5, 39.8], [116.5, 40.0], [116.3, 40.0], [116.3, 39.8]]],
    },
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
