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
  - MGRS（第一阶段）：点定位（`/v1/grid/locate`）
- 编码能力
  - 时空编码生成：`/v1/code/st`（支持 `geohash/mgrs/isea4h` 前缀编码）
  - 批量时空编码生成：`/v1/code/st/batch`
  - 时空编码解析：`/v1/code/parse`
- 元操作能力
  - Geohash：邻接计算（`/v1/topology/neighbors`）、父级推导（`/v1/topology/parent`）、子级推导（`/v1/topology/children`）、编码转几何（`/v1/topology/geometry`）
  - MGRS（第一阶段增强）：邻接计算（`/v1/topology/neighbors`）、父级推导（`/v1/topology/parent`）、子级推导（`/v1/topology/children`）、编码转几何（`/v1/topology/geometry`）
  - ISEA4H：已接入统一路由骨架，当前返回明确未实现错误

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

启动后可访问：

- 健康检查：`GET /health`
- API 前缀：`/v1`

## 测试

```bash
python -m pytest -q tests
```

## 文档规范（开发过程记录）

为满足可追溯开发管理，项目内置以下文档：

- 开发任务记录：`docs/DEVELOPMENT_LOG.md`
- Bug 排查记录：`docs/BUG_LOG.md`
- 记录流程规范：`docs/DOC_WORKFLOW.md`

每次开发与排障都需要追加记录（append-only）。

## 后续规划

- 完善 MGRS 覆盖能力
- ISEA4H 算法分阶段落地
- 批量能力与覆盖精度策略增强（`minimal` 后续可引入跨层级最小覆盖优化）
