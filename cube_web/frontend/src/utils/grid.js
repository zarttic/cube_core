export const gridDefinitions = Object.freeze([
  { value: 'geohash', label: 'Geohash', minLevel: 1, maxLevel: 12 },
  { value: 'mgrs', label: '扩展 MGRS', minLevel: 0, maxLevel: 5 },
  { value: 'isea4h', label: 'ISEA4H', minLevel: 0, maxLevel: 15 },
]);

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
  if (gridType === 'geohash') return `Geohash 精度 ${numericLevel}`;
  if (gridType === 'mgrs') return mgrsLabels[numericLevel] || '';
  if (gridType === 'isea4h') return `ISEA4H 分辨率 ${numericLevel}`;
  return '';
}
