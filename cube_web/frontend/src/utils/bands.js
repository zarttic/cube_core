const dataUnitTypeLabels = Object.freeze({
  optical: '光谱波段',
  radar: '极化通道',
  product: '产品变量',
  carbon: '观测变量',
});

export function dataUnitTypeLabel(dataType) {
  return dataUnitTypeLabels[dataType] || '数据波段';
}

export function sceneBands(scene, dataType = '') {
  const rawBands = Array.isArray(scene?.bands) && scene.bands.length
    ? scene.bands
    : Array.isArray(scene?.attributes?.bands) ? scene.attributes.bands : [];
  const candidates = rawBands.length ? rawBands : [{}];
  return candidates.map((rawBand, index) => {
    const band = typeof rawBand === 'object' && rawBand !== null ? rawBand : {};
    const code = String(band.band_code || '').trim();
    const type = String(band.band_type || '').trim();
    const contractErrors = [];
    if (!code) contractErrors.push('缺少波段编码');
    if (!type) contractErrors.push('缺少波段类型');
    return {
      band_unit_id: String(band.band_unit_id || '').trim(),
      asset_id: String(band.asset_id || '').trim(),
      band_code: code,
      band_name: String(band.band_name || '').trim(),
      band_type: type,
      unit: band.unit ? String(band.unit) : '',
      display_order: Number(band.display_order ?? index),
      grid_statuses: Array.isArray(band.grid_statuses) ? band.grid_statuses : [],
      contract_errors: contractErrors,
    };
  }).sort((left, right) => left.display_order - right.display_order || left.band_code.localeCompare(right.band_code));
}

export function bandDisplayLabel(band) {
  if (Array.isArray(band?.contract_errors) && band.contract_errors.length) {
    return `数据异常：${band.contract_errors.join('、')}`;
  }
  const name = String(band?.band_name || '').trim();
  const code = String(band?.band_code || '').trim();
  const identity = name && name !== code ? `${code} · ${name}` : code || name || '未命名波段';
  return band?.unit ? `${identity} [${band.unit}]` : identity;
}

export function sceneMatchesBand(scene, dataType, keyword) {
  const query = String(keyword || '').trim().toLowerCase();
  if (!query) return true;
  return sceneBands(scene, dataType).some((band) => (
    [band.band_code, band.band_name, band.band_type, band.unit].join(' ').toLowerCase().includes(query)
  ));
}
