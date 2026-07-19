const ruleLabels = {
  index_schema: '索引结构完整性', output_count_consistency: '输出数量一致性',
  output_reference_integrity: '输出引用完整性', grid_method_agreement: '格网与剖分方式一致性',
  cell_bbox_validity: '格网边界有效性', time_bucket_consistency: '时间分桶一致性',
  asset_readability: '数据单元可读性', asset_crs: '数据单元坐标系', window_bounds: '像素窗口边界',
  optical_band_contract: '光学波段规范', radar_band_contract: '雷达极化通道规范',
  product_band_contract: '信息产品变量规范',
  carbon_schema: '碳卫星数据结构',
  carbon_coordinates: '碳卫星坐标有效性', carbon_xco2_range: 'XCO2 数值范围',
  carbon_quality_flags: '碳卫星质量标识', carbon_observation_duplicates: '碳卫星观测重复',
  carbon_footprints: '碳卫星观测足迹',
};

const errorLabels = {
  missing_st_code: '缺少时空编码', missing_tile_reference: '缺少瓦片引用',
  missing_output_version: '缺少输出版本', tile_grid_mismatch: '瓦片格网信息不一致',
  tile_kind_mismatch: '瓦片类型不一致', detail_grid_mismatch: '明细格网信息不一致',
  invalid_bbox: '格网边界无效', missing_time_bucket: '缺少时间分桶',
  time_bucket_mismatch: '时间分桶不一致', invalid_carbon_source: '碳卫星源数据地址无效',
  invalid_cog_uri: 'COG 数据地址无效', invalid_checksum: '校验和格式无效',
  object_reader_unavailable: '数据读取服务不可用', source_object_unreadable: '源数据无法读取',
  missing_crs: '缺少坐标系', invalid_crs: '坐标系无效',
  crs_metadata_mismatch: '声明坐标系与文件不一致', missing_band_metadata: '缺少波段元数据',
  invalid_band_type: '波段类型无效', window_out_of_bounds: '像素窗口超出范围',
  missing_carbon_indexes: '缺少碳卫星观测索引', missing_carbon_fields: '缺少碳卫星字段',
  duplicate_observation_id: '观测记录标识重复', missing_footprint: '缺少观测足迹',
  invalid_coordinates: '观测坐标无效', xco2_out_of_range: 'XCO2 数值超出范围',
  missing_quality_flag: '缺少质量标识', output_count_mismatch: '输出数量不一致',
};

export function qualityRuleLabel(code) {
  return ruleLabels[String(code || '')] || `未知质检规则（${code || '-'}）`;
}

export function qualityErrorLabel(code) {
  return errorLabels[String(code || '')] || `未分类错误（${code || '-'}）`;
}

const metadataRules = new Set([
  'optical_band_contract', 'radar_band_contract', 'product_band_contract',
]);
const sourceRules = new Set([
  'asset_readability', 'asset_crs', 'carbon_schema', 'carbon_coordinates',
  'carbon_xco2_range', 'carbon_quality_flags', 'carbon_observation_duplicates', 'carbon_footprints',
]);

const systemErrorCodes = new Set(['object_reader_unavailable']);
const metadataErrorCodes = new Set([
  'missing_crs', 'invalid_crs', 'missing_band_metadata', 'invalid_band_type',
]);
const sourceErrorCodes = new Set([
  'invalid_carbon_source', 'invalid_cog_uri', 'invalid_checksum', 'source_object_unreadable',
  'missing_carbon_indexes', 'missing_carbon_fields',
  'duplicate_observation_id', 'missing_footprint', 'invalid_coordinates', 'xco2_out_of_range',
  'missing_quality_flag',
]);

export function qualityRecoveryLabel(ruleCode, errorCode = '') {
  const code = String(ruleCode || '');
  const error = String(errorCode || '');
  if (systemErrorCodes.has(error)) return '系统依赖问题 · 恢复服务后重新质检';
  if (error === 'crs_metadata_mismatch') return '坐标系问题 · 核对声明，源文件错误时退回载入';
  if (['missing_time_bucket', 'time_bucket_mismatch'].includes(error)) return '时间或索引问题 · 核对载入时间后局部重建';
  if (metadataErrorCodes.has(error) || metadataRules.has(code)) return '元数据问题 · 修正后重新质检';
  if (sourceErrorCodes.has(error) || sourceRules.has(code)) return '源数据问题 · 退回载入系统修正';
  return '剖分产物问题 · 原批次局部重建';
}
