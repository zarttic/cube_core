# cube_encoder 文档索引

更新时间：2026-09-10

`cube_encoder` 是底层格网编码 SDK（包名 `grid_core`、分发名 `cube-encoder`），提供格网定位、覆盖、
拓扑和时空编码能力。剖分与入库执行链路见 [cube_split/docs/README.md](../../cube_split/docs/README.md)，
Web 入口与托管剖分 API 见 [cube_web/docs/README.md](../../cube_web/docs/README.md)。

## 主文档

- [ARCHITECTURE.md](ARCHITECTURE.md)：引擎职责、分层、API 与 SDK 边界。
- [SDK_RELEASE.md](SDK_RELEASE.md)：版本号规则、发布检查与兼容性要求。
- [../CHANGELOG.md](../CHANGELOG.md)：对外变更记录。

## 安装与运行

```bash
cd cube_encoder
python3.11 -m pip install -e .                     # 本地安装
python3.11 -m uvicorn grid_core.app.main:app --port 50012   # 独立 HTTP 服务（可选）
python3.11 -m grid_core.app.perf_smoke            # 性能烟测，发布前必跑
python3.11 -m build                                # 构建 wheel / sdist
```

包名 `cube-encoder`，版本见 `pyproject.toml`（当前 0.2.0），`requires-python >=3.11`。

## 生产格网矩阵

| 格网 | 层级范围 | Web 默认层级 | 剖分方式（由 Web 派生） | 说明 |
| --- | --- | --- | --- | --- |
| `geohash` | 1–12 | 4 | logical | 经纬度格网 |
| `mgrs` | 0–5 | 1 | logical | 标准 MGRS（精度 0–5 对应 100km–1m） |
| `isea4h` | 1–6 | 6 | entity | 六边形格网，纯 Python 实现，对齐 DGGRID v8.44 |

- 生产入口严格限定这三类；`s2`、`tile_matrix`、`plane_grid` 已从代码移除，传入旧值会得到
  `ValidationError`（请求模型 `extra="forbid"`，也拒绝历史字段）。
- 剖分方式不由调用方选择：`isea4h` → `entity`，其余 → `logical`。
- `isea4h` 的 `space_code` 为未补零十进制 DGGRID SEQNUM，`cell_count(r) = 10 * 4**r + 2`；
  请求使用 `requested_grid_level`，返回单元保留实际 `grid_level`。

## SDK 用法

```python
from grid_core.sdk import CubeEncoderSDK

sdk = CubeEncoderSDK()
cell = sdk.locate("geohash", requested_grid_level=6, point=(116.4, 39.9))
cells = sdk.cover("mgrs", requested_grid_level=2, cover_mode="intersect", bbox=(100, 23, 104, 27))
neighbors = sdk.neighbors(cell.address, k=1)
st_code = sdk.generate_st_code(cell.address, timestamp=..., time_granularity=...)
```

主要方法（`grid_core/sdk/client.py`）：

| 方法 | 作用 |
| --- | --- |
| `locate` / `locate_space_code` / `locate_space_codes` | 点定位到格网单元或空间编码 |
| `cover` / `cover_compact` | 区域覆盖，返回 `GridCell` / `CompactGridCell` 列表 |
| `neighbors` / `parent` / `children` | 拓扑关系 |
| `code_to_geometry` / `code_to_bbox` / `codes_to_geometries` | 编码转几何 |
| `generate_st_code` / `generate_st_codes` / `parse_st_code` | 时空编码生成与解析 |

## 覆盖（cover）

- `cover_mode`：`intersect`、`contain`、`minimal`；`boundary_type`：`bbox`、`polygon`。
- 只支持 `EPSG:4326`；`bbox` 会转换为 polygon，`cover_compact` 返回紧凑单元。
- MGRS 的 `minimal` 会做粗化处理；geohash 的 `minimal` 仅在 `compact=True` 时返回混合层级。
- **显示用连续方格（`preview_cells`）不在本包**：它是 `cube_web` 的 Web 覆盖响应扩展，
  仅当 `preview_mode=continuous` 且格网为 `mgrs` 时生成，不改变生产单元与入库几何。

## 时空编码

格式 `<prefix>:<grid_level>:<space_code>:<time_code>`，前缀 `gh`、`mgrs`、`i4h`。
时间粒度支持 `second`、`minute`、`hour`、`day`、`month`，时间码使用 UTC 且要求带时区。
HTTP 接口：`POST /v1/code/st`、`/v1/code/parse`、`/v1/code/st/batch`。

## HTTP 服务（可选）

`grid_core.app.main:app`，前缀 `/v1`，端口 50012：
`POST /v1/grid/locate`、`POST /v1/grid/cover`、`POST /v1/topology/{neighbors,geometry,geometries,parent,children}`、
`POST /v1/code/st`、`/v1/code/parse`、`/v1/code/st/batch`。

## 异常体系

`GridCoreError`（`GRID_CORE_ERROR`）为基类，派生 `ValidationError`（`VALIDATION_ERROR`）、
`NotImplementedCapabilityError`（`NOT_IMPLEMENTED_CAPABILITY`）、`ParseError`（`PARSE_ERROR`）。
HTTP 映射：`ValidationError` → 422，`NotImplementedCapabilityError` → 501，其余 → 400，
响应体为 `{"error": {"code", "message"}}`。

## 测试

```bash
PYTHONPATH=cube_encoder python3.11 -m pytest cube_encoder/tests
```

- 主要覆盖：geohash / mgrs / isea4h 引擎、地址与拓扑、边界、契约模型、错误处理、SDK 集成、性能烟测。
- `tests/fixtures/isea4h/` 是 DGGRID 生成的权威向量（jsonl + sha256 清单）。
- `tests/integration/test_grid_real_aoi.py` 标记 `real_aoi`，需要环境变量 `CUBE_GRID_REAL_AOI_URI`，
  未设置时用例直接失败（真实栅格门禁）。
