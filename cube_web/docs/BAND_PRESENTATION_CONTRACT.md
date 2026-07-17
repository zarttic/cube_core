# 波段命名与展示契约

剖分页面将 Scene 展示为“数据单元”。当前剖分提交仍以 `scene_id` 为事务单位，
执行时从 `scene_assets` 和 `scene_bands` 物化该数据单元的资产和波段。
前端不生成或改写波段编码。

## 统一字段

| 字段 | 用途 |
| --- | --- |
| `band_code` | 稳定标识，用于排序、筛选和后端关联。 |
| `band_name` | 人类可读名称；未提供时回退为 `band_code`。 |
| `band_type` | 波段业务类型，例如 `spectral`、`polarization`或 `variable`。 |
| `unit` | 变量或测量单位，有值时以方括号跟随波段名称展示。 |
| `display_order` | 同一数据单元内的稳定展示顺序。 |

统一展示格式为 `band_code · band_name [unit]`；名称与编码相同时只显示一次，
单位为空时不显示方括号。

## 产品类型

| 数据类型 | 页面类型名 | 默认 `band_type` | 典型编码 |
| --- | --- | --- | --- |
| `optical` | 光谱波段 | `spectral` | `B04`、`B08` |
| `radar` | 极化通道 | `polarization` | `VV`、`VH`、`HH`、`HV` |
| `product` | 产品变量 | `variable` | `NDVI` 或产品定义的变量名 |
| `carbon` | 观测变量 | `variable` | `xco2` 或源产品变量名 |

光学波段编码的光谱含义由载入系统提供的 `band_name` 决定。
不同传感器的 `B01`、`B04` 含义可能不同，Web 端不使用固定对照表猜测。
波段筛选同时匹配编码、名称、类型和单位。
