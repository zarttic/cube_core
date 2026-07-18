const ruleLabels = {
  index_schema: '索引结构完整性', output_count_consistency: '输出数量一致性',
  output_reference_integrity: '输出引用完整性', grid_method_agreement: '格网与剖分方式一致性',
  cell_bbox_validity: '格网边界有效性', time_bucket_consistency: '时间分桶一致性',
  asset_readability: '数据单元可读性', asset_crs: '数据单元坐标系', window_bounds: '像素窗口边界',
  pixel_sample: '像素抽样', optical_band_contract: '光学波段规范', radar_band_contract: '雷达极化通道规范',
  product_band_contract: '信息产品变量规范', metadata_completeness: '元数据完整性',
  declared_metadata_defects: '已声明元数据缺陷', declared_metadata_warnings: '已声明元数据警告',
  product_year_consistency: '产品年份一致性', carbon_schema: '碳卫星数据结构',
  carbon_coordinates: '碳卫星坐标有效性', carbon_xco2_range: 'XCO2 数值范围',
  carbon_quality_flags: '碳卫星质量标识', carbon_observation_duplicates: '碳卫星观测重复',
  carbon_footprints: '碳卫星观测足迹',
};

const errorLabels = {
  missing_product_year: '缺少产品年份', product_year_mismatch: '产品年份与数据时间不一致',
  missing_carbon_indexes: '缺少碳卫星观测索引', missing_crs: '缺少坐标系',
  missing_output_version: '缺少输出版本', missing_st_code: '缺少时空编码',
  object_reader_unavailable: '数据读取器不可用', output_count_mismatch: '输出数量不一致',
  detail_grid_mismatch: '明细格网信息不一致', invalid_bbox: '格网边界无效',
  invalid_window: '像素窗口无效', missing_reference: '缺少数据引用', duplicate_observation: '观测记录重复',
  unreadable_asset: '数据单元无法读取', checksum_mismatch: '校验和不一致', missing_band: '缺少波段',
  invalid_band_type: '波段类型无效', missing_carbon_fields: '缺少碳卫星字段', invalid_coordinate: '坐标无效',
  xco2_out_of_range: 'XCO2 数值超出范围', invalid_quality_flag: '质量标识无效', missing_footprint: '缺少观测足迹',
};

export function qualityRuleLabel(code) {
  return ruleLabels[String(code || '')] || `未知质检规则（${code || '-'}）`;
}

export function qualityErrorLabel(code) {
  return errorLabels[String(code || '')] || `未分类错误（${code || '-'}）`;
}
