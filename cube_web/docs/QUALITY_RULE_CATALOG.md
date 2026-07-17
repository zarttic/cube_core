# 质检规则契约

当前规则集版本为 `2026.07.14-v1`，以
`cube_web.services.quality_rules.default_rule_registry()` 的运行时注册结果为准。
前端通过 `GET /v1/quality/rules` 展示同一份定义，不单独维护规则矩阵。

## 公共必选项

下列规则适用于光学遥感、雷达遥感、信息产品和碳卫星：

| 规则代码 | 质检项 |
| --- | --- |
| `index_schema` | 索引结构完整性 |
| `output_count_consistency` | 输出数量一致性 |
| `output_reference_integrity` | 输出引用完整性 |
| `grid_method_agreement` | 格网与剖分方式一致性 |
| `cell_bbox_validity` | 格网边界有效性 |
| `time_bucket_consistency` | 时间分桶一致性 |
| `asset_readability` | 资产可读性 |
| `asset_crs` | 资产坐标系 |
| `window_bounds` | 像素窗口边界 |

## 公共可选项

| 规则代码 | 质检项 | 适用产品 |
| --- | --- | --- |
| `pixel_sample` | 像素抽样 | 全部四类产品 |
| `metadata_completeness` | 元数据完整性 | 全部四类产品 |
| `declared_metadata_defects` | 已声明元数据缺陷 | 全部四类产品 |

## 产品专用必选项

| 适用产品 | 规则代码 | 质检项 |
| --- | --- | --- |
| 信息产品 | `product_year_consistency` | 产品年份一致性 |
| 碳卫星 | `carbon_schema` | 碳卫星数据结构 |
| 碳卫星 | `carbon_coordinates` | 碳卫星坐标有效性 |
| 碳卫星 | `carbon_xco2_range` | XCO2 数值范围 |
| 碳卫星 | `carbon_quality_flags` | 碳卫星质量标识 |
| 碳卫星 | `carbon_observation_duplicates` | 碳卫星观测重复 |
| 碳卫星 | `carbon_footprints` | 碳卫星观测足迹 |

规则快照会写入每次质检运行。历史记录按当时快照解释，不会因后续规则集变更而改写。
质检错误导出使用流式接口，不受页面分页限制。
