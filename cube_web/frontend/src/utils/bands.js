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
  const fallback = scene?.band || scene?.variable;
  const candidates = rawBands.length ? rawBands : fallback ? [fallback] : [];
  return candidates.map((rawBand, index) => {
    const band = typeof rawBand === 'object' && rawBand !== null ? rawBand : { band_code: String(rawBand) };
    const code = String(band.band_code || band.code || band.band || band.variable || '').trim();
    return {
      band_code: code || `band-${index + 1}`,
      band_name: String(band.band_name || band.name || code || `波段 ${index + 1}`),
      band_type: String(band.band_type || (['product', 'carbon'].includes(dataType) ? 'variable' : dataType === 'radar' ? 'polarization' : 'spectral')),
      unit: band.unit ? String(band.unit) : '',
      display_order: Number(band.display_order ?? index),
    };
  }).sort((left, right) => left.display_order - right.display_order || left.band_code.localeCompare(right.band_code));
}

export function bandDisplayLabel(band) {
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
