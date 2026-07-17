export const gridDefinitions = Object.freeze([
  { value: 'geohash', label: '经纬度格网', minLevel: 1, maxLevel: 12 },
  { value: 'mgrs', label: '平面格网', minLevel: 0, maxLevel: 5 },
  { value: 'isea4h', label: '六边形格网', minLevel: 0, maxLevel: 15 },
]);

export const fixedPartitionOptions = Object.freeze({
  cover_mode: 'intersect',
  time_granularity: 'day',
  max_cells_per_asset: 0,
});

export function withFixedPartitionOptions(partition = {}) {
  return { ...partition, ...fixedPartitionOptions };
}

const mgrsLabels = ['100 km', '10 km', '1 km', '100 m', '10 m', '1 m'];

export function gridDefinition(gridType) {
  return gridDefinitions.find((item) => item.value === gridType) || null;
}

export function derivedPartitionMethod(gridType) {
  if (gridType === 'geohash' || gridType === 'mgrs') return 'logical';
  if (gridType === 'isea4h') return 'entity';
  return '';
}

export function nativeLevelLabel(gridType, level) {
  const numericLevel = Number(level);
  if (gridType === 'geohash') return `第 ${numericLevel} 级 · 精度 ${numericLevel}`;
  if (gridType === 'mgrs') return `第 ${numericLevel} 级 · ${mgrsLabels[numericLevel] || ''}`;
  if (gridType === 'isea4h') return `第 ${numericLevel} 级 · 分辨率 ${numericLevel}`;
  return '';
}
