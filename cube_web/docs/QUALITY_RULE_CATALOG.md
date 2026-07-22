# 质检规则契约

当前规则集版本为 `2026.07.21-v8`，以
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
| `window_bounds` | 像素窗口边界 |

## 数据类型专属可选项

| 适用产品 | 规则代码 | 质检项 |
| --- | --- | --- |
| 光学、雷达、信息产品 | `asset_crs` | 声明 CRS 合法且与影像内 CRS 一致 |
| 光学遥感 | `optical_band_contract` | 光谱波段必须为 `spectral`；明确的 QA/气溶胶质量层允许使用 `variable` |
| 雷达遥感 | `radar_band_contract` | 极化通道编码与类型规范 |
| 碳卫星 | `carbon_schema` | 碳卫星数据结构 |
| 碳卫星 | `carbon_coordinates` | 碳卫星坐标有效性 |
| 碳卫星 | `carbon_xco2_range` | XCO2 数值范围 |
| 碳卫星 | `carbon_quality_flags` | 碳卫星质量标识 |

这些规则默认启用，管理员可在质检规则页取消勾选；设置只影响之后新建的质检运行。

已移除、不再参与新质检运行的规则：`product_band_contract`（信息产品变量规范）、
`carbon_observation_duplicates`（碳卫星观测重复）、`carbon_footprints`（碳卫星观测足迹）。
历史质检记录仍按当时写入的规则快照解释。

规则快照会写入每次质检运行。历史记录按当时快照解释，不会因后续规则集变更而改写。
质检错误导出使用流式接口，不受页面分页限制。

生产 `asset_readability` 会从 MinIO 下载缓存并真实打开数据；读取器也支持本地路径用于
测试和诊断，但生产领域表只接受 `s3://`。COG 使用 rasterio，NetCDF 使用 netCDF4，
非 NetCDF 数据模型的 HDF5 使用 GDAL/rasterio fallback。读取错误只记录
通用错误类型，不保存连接异常文本或凭据。碳卫星使用观测结构、坐标、XCO2、质量标识
专项规则，不执行栅格像素抽样。
