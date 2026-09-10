/** Rules removed from the active registry; hide from catalogs and live run summaries. */
export const RETIRED_QUALITY_RULE_CODES = new Set([
  'product_band_contract',
  'carbon_observation_duplicates',
  'carbon_footprints',
  'carbon_quality_flags',
]);

const ruleLabels = {
  index_schema: '索引结构完整性', output_count_consistency: '输出数量一致性',
  output_reference_integrity: '输出引用完整性', grid_method_agreement: '格网与剖分方式一致性',
  cell_bbox_validity: '格网边界有效性', time_bucket_consistency: '时间分桶一致性',
  asset_readability: '数据单元可读性', asset_crs: '数据单元坐标系', window_bounds: '像素窗口边界',
  optical_band_contract: '光学波段规范', radar_band_contract: '雷达极化通道规范',
  carbon_schema: '碳卫星数据结构',
  carbon_coordinates: '碳卫星坐标有效性', carbon_xco2_range: 'XCO2 数值范围',
  carbon_sif_range: 'SIF 数值范围',
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
  invalid_coordinates: '观测坐标无效', xco2_out_of_range: 'XCO2 数值超出范围',
  sif_out_of_range: 'SIF 数值超出范围',
  missing_quality_flag: '缺少质量标识', output_count_mismatch: '输出数量不一致',
};

export function isActiveQualityRuleCode(code) {
  return Boolean(code) && !RETIRED_QUALITY_RULE_CODES.has(String(code));
}

export function filterActiveQualityRules(items) {
  return (items || []).filter((item) => isActiveQualityRuleCode(item?.code || item?.rule_code));
}

export function qualityRuleLabel(code) {
  const key = String(code || '');
  if (RETIRED_QUALITY_RULE_CODES.has(key)) return `已停用规则（${key}）`;
  return ruleLabels[key] || `未知质检规则（${key || '-'}）`;
}

export function qualityErrorLabel(code) {
  return errorLabels[String(code || '')] || `未分类错误（${code || '-'}）`;
}

const executionExceptionLabels = {
  filenotfounderror: '文件不存在',
  permissionerror: '没有文件访问权限',
  oserror: '系统读写异常',
  runtimeerror: '运行时异常',
  valueerror: '数据校验异常',
  typeerror: '类型错误',
  keyerror: '字段缺失',
  timeouterror: '处理超时',
  connectionerror: '连接异常',
  rasterioioerror: '栅格文件读取异常',
  cple_openfailederror: '栅格文件打开失败',
  botocoreerror: '对象存储访问异常',
  exception: '任务异常',
};

const executionTextReplacements = [
  [/quality outbox dispatch failed/gi, '质检任务派发失败'],
  [/quality rule implementation unavailable\s*:\s*/gi, '质检规则实现不可用：'],
  [/quality rule execution failed/gi, '质检规则执行失败'],
  [/quality rule failed\s*:\s*/gi, '质检规则失败：'],
  [/quality run lease expired/gi, '质检任务租约已过期'],
  [/quality run execution failed/gi, '质检任务执行失败'],
  [/source object does not exist or cannot be opened/gi, '源数据不存在或无法打开'],
  [/source object could not be opened/gi, '源数据对象无法打开'],
  [/source object is unavailable/gi, '源数据对象不可用'],
  [/failed to deserialize exception/gi, 'Ray 任务异常，未能读取底层错误'],
  [/no such (?:key|object|bucket)/gi, '源数据对象不存在'],
  [/failed to fetch/gi, '网络连接失败'],
  [/network error/gi, '网络错误'],
  [/request timeout|timed out|timeout/gi, '请求超时'],
  [/not found/gi, '未找到'],
  [/unauthorized/gi, '未授权'],
  [/forbidden|permission denied/gi, '无权访问'],
  [/bad request/gi, '请求参数错误'],
  [/internal server error/gi, '服务器内部错误'],
  [/index rows have no ST code/gi, '索引记录缺少时空编码'],
  [/index references a missing tile/gi, '索引引用的瓦片不存在'],
  [/quality target has no output version/gi, '质检目标没有输出版本'],
  [/tile grid does not match output version/gi, '瓦片格网与输出版本不一致'],
  [/grid cell bbox must be \[west, south, east, north\] in WGS84 bounds/gi, '格网单元边界必须是 WGS84 范围内的［西、南、东、北］'],
  [/index has no time bucket/gi, '索引记录缺少时间分桶'],
  [/time bucket does not match acquisition date/gi, '时间分桶与采集日期不一致'],
  [/source asset must use an s3 COG URI/gi, '源数据必须使用 s3 COG 地址'],
  [/source asset checksum must be a SHA-256 hex digest/gi, '源数据校验和必须是 SHA-256 十六进制摘要'],
  [/quality object reader is unavailable/gi, '数据读取服务不可用'],
  [/carbon source must be an s3 NetCDF\/HDF5\/SIF asset matching source_format/gi, '碳卫星源数据必须是与 source_format 匹配的 s3 NetCDF/HDF5/SIF 文件'],
  [/source asset CRS is required/gi, '源数据必须声明坐标系'],
  [/source asset CRS is not a valid coordinate reference system/gi, '源数据坐标系无效'],
  [/declared CRS does not match the source raster CRS/gi, '声明的坐标系与源栅格坐标系不一致'],
  [/source asset has no normalized band metadata/gi, '源数据缺少标准化波段元数据'],
];

const executionCodeLabels = { ...ruleLabels, ...errorLabels };
const apiCodeLabels = {
  dataset_not_found: '数据集不存在或已被删除，请刷新数据后重试',
  output_version_not_found: '输出版本不存在，请重新载入或选择当前版本',
  output_version_not_completed: '输出版本尚未完成，暂时不能进行质检',
  quality_run_not_found: '质检任务不存在或已被清理，请刷新列表',
  quality_trigger_conflict: '质检任务状态已变化，请刷新后再操作',
  partition_batch_active: '该剖分批次已有任务在运行，请等待当前任务结束',
  partition_batch_archived: '该剖分批次已归档，不能再次提交',
  request_timeout: '请求超时；请先到任务列表确认任务是否已提交',
  internal_error: '服务器内部错误，请根据请求 ID 联系管理员',
};

/** Convert backend quality failures into readable, non-sensitive Chinese UI text. */
export function qualityExecutionErrorLabel(value) {
  const code = value && typeof value === 'object' ? String(value.code || '').trim() : '';
  const text = String(value && typeof value === 'object' ? value.message : value ?? '').trim();
  if (!text) return '';

  const codeLabel = apiCodeLabels[code];
  if (codeLabel) {
    const requestId = value && typeof value === 'object' ? String(value.requestId || '').trim() : '';
    return requestId ? `${codeLabel}（请求 ID：${requestId}）` : codeLabel;
  }

  let label = text.replace(/s3:\/\/[^\s"'<>]+/gi, '[对象地址已隐藏]');
  for (const [pattern, replacement] of executionTextReplacements) {
    label = label.replace(pattern, replacement);
  }
  for (const [code, replacement] of Object.entries(executionCodeLabels)) {
    label = label.replace(new RegExp(`\\b${code}\\b`, 'gi'), replacement);
  }
  label = label.replace(/\s*;\s*caused by\s*/gi, '；根因：');
  label = label.replace(
    /\b(FileNotFoundError|PermissionError|OSError|RuntimeError|ValueError|TypeError|KeyError|TimeoutError|ConnectionError|RasterioIOError|CPLE_OpenFailedError|BotocoreError|Exception)\b(?:\s*:\s*)?/gi,
    (match, type) => `${executionExceptionLabels[type.toLowerCase()] || type}${match.includes(':') ? '：' : ''}`,
  );
  label = label.replace(/\b(Exception|Error)\b/gi, (match) => executionExceptionLabels[match.toLowerCase()] || '异常');

  return /[\u4e00-\u9fff]/.test(label) ? label : `质检执行异常：${label}`;
}

const metadataRules = new Set([
  'optical_band_contract', 'radar_band_contract',
]);
const sourceRules = new Set([
  'asset_readability', 'asset_crs', 'carbon_schema', 'carbon_coordinates',
  'carbon_xco2_range', 'carbon_sif_range',
]);

const systemErrorCodes = new Set(['object_reader_unavailable']);
const metadataErrorCodes = new Set([
  'missing_crs', 'invalid_crs', 'missing_band_metadata', 'invalid_band_type',
]);
const sourceErrorCodes = new Set([
  'invalid_carbon_source', 'invalid_cog_uri', 'invalid_checksum', 'source_object_unreadable',
  'missing_carbon_indexes', 'missing_carbon_fields',
  'invalid_coordinates', 'xco2_out_of_range', 'sif_out_of_range',
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
