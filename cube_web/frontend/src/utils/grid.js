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

export function gridDefinition(gridType) {
  return gridDefinitions.find((item) => item.value === gridType) || null;
}

export function derivedPartitionMethod(gridType) {
  if (gridType === 'geohash' || gridType === 'mgrs') return 'logical';
  if (gridType === 'isea4h') return 'entity';
  return '';
}

export function nativeLevelLabel(gridType, level) {
  if (!gridDefinition(gridType)) return '';
  const numericLevel = Number(level);
  return `层级 ${numericLevel}`;
}

export function recommendedGridLevel(resolutionM, gridType) {
  const resolution = Number(resolutionM);
  if (!Number.isFinite(resolution) || resolution <= 0) {
    return gridType === 'isea4h' ? 6 : gridType === 'mgrs' ? 1 : 5;
  }
  if (gridType === 'isea4h') return resolution < 10 ? 12 : resolution <= 30 ? 11 : 8;
  if (gridType === 'mgrs') return resolution < 10 ? 1 : 0;
  if (resolution >= 1000) return 2;
  if (resolution >= 500) return 3;
  if (resolution < 10) return 6;
  if (resolution <= 30) return 5;
  return 4;
}
