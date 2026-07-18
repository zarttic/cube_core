# 波段命名与展示契约

剖分页面将 Scene 展示为“数据单元”。当前剖分提交仍以 `scene_id` 为事务单位，
执行时从 `scene_assets` 和 `scene_bands` 物化该数据单元的资产和波段。
前端不生成、推断或改写波段编码和类型。

## 统一字段

| 字段 | 用途 |
| --- | --- |
| `band_code` | 稳定标识，用于排序、筛选和后端关联。 |
| `band_name` | 人类可读名称；未提供时回退为 `band_code`。 |
| `band_type` | 波段业务类型，例如 `spectral`、`polarization`或 `variable`。 |
| `unit` | 变量或测量单位，有值时以方括号跟随波段名称展示。 |
| `display_order` | 同一数据单元内的稳定展示顺序。 |

## 载入边界

`band_code` 和 `band_type` 必须在载入时规范化并写入 `scene_bands`：

- 优先保留载入系统提供的 `band_code`、`code` 或 `band`。
- 缺少编码时，后端根据 `data_type + product_type + asset_id + band_index` 生成确定性的
  `auto-<data_type>-<hash>-<order>` 编码，并在 attributes 中记录生成标记和依据。
- 缺少类型时，后端按数据类型确定：光学 `spectral`、雷达 `polarization`、信息产品和
  碳卫星 `variable`；传入不支持的类型时拒绝载入。
- 同一稳定资产及相同波段顺序在不同载入批次中得到相同编码。载入方调整波段顺序时应
  同时提供原生编码，避免把不同顺序解释为新的波段身份。

前端只消费持久化字段。历史记录缺少 `band_code` 或 `band_type` 时显示“数据异常”，不得
回退生成 `band-1`、猜测类型或把 Scene 的松散字段当作正式波段。

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
