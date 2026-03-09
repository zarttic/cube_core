# 球离散格网剖分与编码子系统设计文档

## 1. 文档说明

本文档用于指导“球离散格网剖分与编码子系统”的开发实现。  
该子系统仅聚焦于以下两类核心能力：

1. **格网剖分**
   - 将点、线、面、范围框等空间对象映射为离散格网单元
   - 支持多种格网体系
   - 支持定位、覆盖、边界表达、层级操作

2. **格网编码**
   - 为格网单元生成统一空间编码与时空编码
   - 支持编码解析、反算、标准化表达和元操作

> **明确边界：本系统不负责对象存储、索引入库、对象挂载、逻辑存储组织等内容。**  
> 本系统是遥感数据管理系统的**底层剖分与编码能力引擎**，向上提供统一服务。

---

## 2. 系统目标

建设一个统一的、可扩展的球离散格网剖分与编码子系统，作为遥感数据管理系统的底层空间离散化与时空编码基础设施。

### 2.1 目标能力

- 支持三类格网体系：
  - 经纬度格网（Geohash）
  - 平面格网（MGRS）
  - 六边形格网（ISEA4H DGGS）
- 支持三类核心操作：
  - 空间剖分
  - 编码生成/解析
  - 编码元操作
- 支持统一接口与统一输出模型
- 支持后续作为 SDK 或微服务对上输出能力

---

## 3. 系统边界

## 3.1 本系统负责

- 空间对象剖分为格网单元
- 格网单元编码生成与解析
- 时空编码组装与拆解
- 格网编码的邻接、层级、几何反算
- 统一接口定义
- 标准结果输出

## 3.2 本系统不负责

- 遥感对象物理存储
- 对象与格网映射关系持久化
- 数据库表设计
- 检索索引构建
- 数据对象管理
- 数据分发与缓存存储

---

## 4. 总体设计思路

本系统按能力划分为三层：

```text
L1 格网剖分层
L2 编码生成层
L3 编码元操作层
```

对应三大模块：

1. **全球离散格网划分模块**
2. **时空格网编码模块**
3. **格网编码元操作模块**

同时通过一个统一接口层向上暴露服务能力。

---

## 5. 系统架构

```text
┌───────────────────────────────────────┐
│            上层业务/调用方             │
│ 遥感接入 │ 检索分析 │ 聚合统计 │ 可视化 │
└───────────────────────────────────────┘
                  │
                  ▼
┌───────────────────────────────────────┐
│             统一接口层                │
│ 统一请求模型 / 统一响应模型 / 路由分发 │
└───────────────────────────────────────┘
                  │
                  ▼
┌───────────────────────────────────────┐
│          核心能力模块层               │
│ 1. 全球离散格网划分模块               │
│ 2. 时空格网编码模块                   │
│ 3. 格网编码元操作模块                 │
└───────────────────────────────────────┘
                  │
                  ▼
┌───────────────────────────────────────┐
│            格网引擎实现层             │
│ Geohash Engine │ MGRS Engine │ ISEA4H │
└───────────────────────────────────────┘
                  │
                  ▼
┌───────────────────────────────────────┐
│            公共基础组件层             │
│ geometry │ projection │ timecode │ util │
└───────────────────────────────────────┘
```

---

## 6. 模块设计

# 6.1 全球离散格网划分模块

## 6.1.1 模块职责

负责将输入空间对象剖分为指定格网体系和层级下的格网单元集合，并输出标准化结果。

## 6.1.2 支持的格网类型

### 1）经纬度格网
基于 Geohash。

### 2）平面格网
基于 MGRS。

### 3）六边形格网
基于 ISEA4H DGGS。

## 6.1.3 输入类型

- 点 `Point`
- 线 `LineString`
- 面 `Polygon`
- 多面 `MultiPolygon`
- 范围框 `BBox`

## 6.1.4 核心能力

- 点定位
- 线覆盖
- 面覆盖
- 范围框覆盖
- 覆盖模式控制
- 边界表达输出
- 单元中心点输出
- 统一结果封装

## 6.1.5 覆盖模式

| 模式 | 说明 |
|---|---|
| intersect | 单元与目标几何相交即纳入 |
| contain | 单元完全包含于目标几何中 |
| minimal | 在保证覆盖前提下最少单元集合 |

## 6.1.6 输入参数

```json
{
  "grid_type": "geohash",
  "level": 7,
  "cover_mode": "intersect",
  "boundary_type": "bbox",
  "geometry": {},
  "crs": "EPSG:4326"
}
```

## 6.1.7 输出结构

```json
{
  "grid_type": "geohash",
  "level": 7,
  "cover_mode": "intersect",
  "cells": [
    {
      "cell_id": "wtw3sjq",
      "space_code": "wtw3sjq",
      "center": [116.391, 39.907],
      "bbox": [116.389, 39.906, 116.392, 39.908],
      "geometry": {
        "type": "Polygon",
        "coordinates": []
      },
      "metadata": {
        "zone": null,
        "facet": null,
        "precision": 7
      }
    }
  ],
  "statistics": {
    "cell_count": 1
  },
  "warnings": []
}
```

---

# 6.2 时空格网编码模块

## 6.2.1 模块职责

负责对格网单元生成标准化空间编码和时空编码，并提供编码解析与校验能力。

## 6.2.2 编码对象

编码模块面向：

- 已生成的格网单元
- 已知空间编码
- 已知时间戳
- 已知层级信息

## 6.2.3 编码结构

建议统一采用以下格式：

```text
<grid_prefix>:<level>:<space_code>:<time_code>:<version>
```

### 示例

```text
gh:7:wtw3sjq:202603091530:v1
mgrs:5:50SMG1234:2026030915:v1
hx:9:F12A03C21:20260309:v1
```

## 6.2.4 空间编码规则

### Geohash
- 直接使用 Geohash 字符串作为空间编码

### MGRS
- 使用标准 MGRS 字符串作为空间编码

### ISEA4H
- 使用自定义六边形单元 ID 作为空间编码

## 6.2.5 时间编码规则

| 粒度 | 格式 |
|---|---|
| second | YYYYMMDDHHMMSS |
| minute | YYYYMMDDHHMM |
| hour | YYYYMMDDHH |
| day | YYYYMMDD |
| month | YYYYMM |

统一使用 UTC。

## 6.2.6 核心能力

- 空间编码生成
- 时间编码生成
- 时空编码组装
- 时空编码解析
- 编码合法性校验
- 批量编码生成

---

# 6.3 格网编码元操作模块

## 6.3.1 模块职责

负责基于格网编码进行空间拓扑关系计算和编码几何反算。

## 6.3.2 能力分类

### 一）空间拓扑运算
- 邻接单元计算
- k 邻域扩展
- 父单元推导
- 子单元推导
- 层级包含关系判定
- 相交关系判定

### 二）坐标双向映射
- 编码转中心点
- 编码转 bbox
- 编码转 polygon
- 坐标转编码

## 6.3.3 输入输出目标

输入为：
- 空间编码
- 时空编码
- 坐标
- 层级参数
- 邻域参数

输出为：
- 编码集合
- 中心点
- 几何边界
- 布尔判定
- 父子关系结果

---

## 7. 核心抽象模型设计

# 7.1 GridCell

表示一个空间格网单元。

```json
{
  "grid_type": "geohash",
  "level": 7,
  "cell_id": "wtw3sjq",
  "space_code": "wtw3sjq",
  "center": [116.391, 39.907],
  "bbox": [116.389, 39.906, 116.392, 39.908],
  "geometry": {
    "type": "Polygon",
    "coordinates": []
  },
  "metadata": {}
}
```

### 字段说明

| 字段 | 说明 |
|---|---|
| grid_type | 格网类型 |
| level | 格网层级 |
| cell_id | 单元唯一标识 |
| space_code | 空间编码 |
| center | 中心点 |
| bbox | 包围盒 |
| geometry | 边界几何 |
| metadata | 扩展属性 |

---

# 7.2 STCode

表示时空编码结果。

```json
{
  "grid_type": "geohash",
  "level": 7,
  "space_code": "wtw3sjq",
  "time_code": "202603091530",
  "version": "v1",
  "st_code": "gh:7:wtw3sjq:202603091530:v1"
}
```

---

# 7.3 TopologyResult

用于表示拓扑运算结果。

```json
{
  "input_code": "wtw3sjq",
  "operation": "neighbors",
  "result_codes": ["wtw3sjr", "wtw3sjm", "wtw3sjn"],
  "metadata": {
    "k": 1
  }
}
```

---

## 8. 格网引擎设计

# 8.1 抽象基类

所有格网引擎应实现统一接口。

```python
class BaseGridEngine:
    def locate_point(self, lon: float, lat: float, level: int):
        pass

    def cover_geometry(self, geometry: dict, level: int, cover_mode: str):
        pass

    def code_to_geometry(self, code: str):
        pass

    def code_to_center(self, code: str):
        pass

    def neighbors(self, code: str, k: int = 1):
        pass

    def parent(self, code: str):
        pass

    def children(self, code: str, target_level: int):
        pass
```

---

# 8.2 GeohashEngine

## 职责
提供经纬度格网剖分与编码能力。

## 必须实现
- 点转 Geohash
- Geohash 转 bbox
- Geohash 转 polygon
- 邻接单元
- 父子层级
- 面覆盖

## 开发优先级
最高。建议首先完成并作为 MVP 主体。

---

# 8.3 MGRSEngine

## 职责
提供平面格网剖分与编码能力。

## 必须实现
- 经纬度转 MGRS
- MGRS 解析
- MGRS 边界获取
- 不同精度格网生成
- 跨带处理

## 开发优先级
中等。建议在 Geohash 稳定后补充。

---

# 8.4 ISEA4HEngine

## 职责
提供六边形全球离散格网剖分与编码能力。

## 必须实现
- 点定位
- 单元 ID 生成
- 六边形边界反算
- 父子层级
- 覆盖单元生成

## 开发优先级
分阶段推进。建议先定义接口，再逐步补齐算法。

---

## 9. 统一接口设计

系统需要对上提供统一调用入口，可作为 SDK 接口或 HTTP API 的服务定义依据。

---

# 9.1 空间剖分接口

## 9.1.1 点定位

### 输入

```json
{
  "grid_type": "geohash",
  "level": 7,
  "point": [116.391, 39.907]
}
```

### 输出

```json
{
  "cell": {
    "cell_id": "wtw3sjq",
    "space_code": "wtw3sjq",
    "center": [116.391, 39.907],
    "bbox": [116.389, 39.906, 116.392, 39.908]
  }
}
```

---

## 9.1.2 几何覆盖

### 输入

```json
{
  "grid_type": "geohash",
  "level": 6,
  "cover_mode": "intersect",
  "geometry": {
    "type": "Polygon",
    "coordinates": []
  }
}
```

### 输出

```json
{
  "cells": []
}
```

---

# 9.2 编码接口

## 9.2.1 时空编码生成

### 输入

```json
{
  "grid_type": "geohash",
  "level": 7,
  "space_code": "wtw3sjq",
  "timestamp": "2026-03-09T15:30:00Z",
  "time_granularity": "minute",
  "version": "v1"
}
```

### 输出

```json
{
  "st_code": "gh:7:wtw3sjq:202603091530:v1"
}
```

---

## 9.2.2 时空编码解析

### 输入

```json
{
  "st_code": "gh:7:wtw3sjq:202603091530:v1"
}
```

### 输出

```json
{
  "grid_type": "geohash",
  "level": 7,
  "space_code": "wtw3sjq",
  "time_code": "202603091530",
  "version": "v1"
}
```

---

# 9.3 元操作接口

## 9.3.1 邻接计算

### 输入

```json
{
  "grid_type": "geohash",
  "code": "wtw3sjq",
  "k": 1
}
```

### 输出

```json
{
  "result_codes": []
}
```

---

## 9.3.2 编码转几何

### 输入

```json
{
  "grid_type": "geohash",
  "code": "wtw3sjq",
  "boundary_type": "polygon"
}
```

### 输出

```json
{
  "geometry": {
    "type": "Polygon",
    "coordinates": []
  }
}
```

---

## 10. 工作流程设计

# 10.1 格网剖分流程

```text
输入空间对象
  ↓
参数校验
  ↓
几何合法性检查
  ↓
选择格网引擎
  ↓
执行定位/覆盖
  ↓
计算边界与中心点
  ↓
标准化封装
  ↓
返回结果
```

---

# 10.2 时空编码流程

```text
输入 grid_type + level + space_code + timestamp
  ↓
参数校验
  ↓
时间标准化为 UTC
  ↓
生成 time_code
  ↓
组装 st_code
  ↓
输出结果
```

---

# 10.3 元操作流程

```text
输入编码/坐标
  ↓
校验与解析
  ↓
识别格网类型
  ↓
路由到对应引擎
  ↓
执行邻接/父子/边界/定位运算
  ↓
标准化输出
```

---

## 11. 编码规范设计

# 11.1 空间编码规范

### Geohash
直接使用 Geohash 字符串。

### MGRS
直接使用标准 MGRS 编码。

### ISEA4H
采用统一六边形单元编号规范，例如：

```text
<facet>-<level>-<local_id>
```

或

```text
HX<level><global_id>
```

具体实现需在开发时固定规则。

---

# 11.2 时空编码规范

统一格式：

```text
<grid_prefix>:<level>:<space_code>:<time_code>:<version>
```

### 规范要求
- 可逆解析
- 全局唯一
- 支持版本演进
- 字段定界明确
- 不依赖外部状态

---

## 12. 开发阶段规划

# 12.1 第一阶段：MVP

优先实现：

- GeohashEngine
- GridCell 模型
- STCode 编码/解析
- 点定位
- 面覆盖
- 编码转 bbox / polygon
- 邻接单元
- 统一接口模型

---

# 12.2 第二阶段：增强版

增加：

- MGRSEngine
- 跨带处理
- 父子层级推导
- 批量编码
- 更完整的覆盖模式
- 坐标双向映射能力

---

# 12.3 第三阶段：高级版

增加：

- ISEA4HEngine
- 六边形覆盖计算
- 跨面片连续性处理
- 高阶拓扑关系
- 大范围剖分优化

---

## 13. 项目结构建议

```text
grid_core/
├── app/
│   ├── api/
│   │   ├── grid.py
│   │   ├── code.py
│   │   └── topology.py
│   ├── core/
│   │   ├── enums.py
│   │   ├── exceptions.py
│   │   └── config.py
│   ├── models/
│   │   ├── grid_cell.py
│   │   ├── st_code.py
│   │   ├── request.py
│   │   └── response.py
│   ├── engines/
│   │   ├── base.py
│   │   ├── geohash_engine.py
│   │   ├── mgrs_engine.py
│   │   └── isea4h_engine.py
│   ├── services/
│   │   ├── grid_service.py
│   │   ├── code_service.py
│   │   └── topology_service.py
│   ├── utils/
│   │   ├── geometry.py
│   │   ├── projection.py
│   │   ├── timecode.py
│   │   └── validator.py
│   └── main.py
├── tests/
├── docs/
└── requirements.txt
```

---

## 14. 技术选型建议

| 能力 | 技术 |
|---|---|
| 服务框架 | FastAPI |
| 数据模型校验 | Pydantic |
| 几何处理 | Shapely |
| 坐标转换 | pyproj |
| Geohash 编码 | geohash 库 |
| MGRS 编码 | mgrs |
| 时间标准化 | datetime / zoneinfo |
| 测试 | pytest |

---

## 15. 非功能要求

### 15.1 性能目标
- 点定位：单次 < 20ms
- 单面覆盖：< 500ms
- 编码生成：< 5ms
- 编码解析：< 5ms

### 15.2 可扩展性
- 可增加新格网体系
- 可扩展新编码版本
- 可扩展批量模式

### 15.3 可维护性
- 模块清晰
- 接口统一
- 输出标准化
- 算法与接口分离

---

## 16. 最终设计结论

本子系统应被设计为一个**纯能力型的格网剖分与编码引擎**，而不是存储系统的一部分。

其核心定位为：

- 对空间对象进行离散格网剖分
- 对格网单元进行统一空间编码与时空编码
- 对格网编码执行拓扑与几何元操作
- 以统一接口向上层提供服务

### 推荐实施策略
- 第一优先：完成 Geohash 主能力闭环
- 第二优先：补充 MGRS 兼容能力
- 第三优先：逐步实现 ISEA4H 六边形全球格网能力

---


