import { describe, expect, it } from 'vitest';

import { derivedPartitionMethod, gridDefinitions, nativeLevelLabel } from '@/utils/grid';

describe('grid contract', () => {
  it('exposes only production grids and derives their methods', () => {
    expect(gridDefinitions.map((item) => item.value)).toEqual(['geohash', 'mgrs', 'isea4h']);
    expect(gridDefinitions.map((item) => item.label)).toEqual(['经纬度格网', '平面格网', '六边形格网']);
    expect(derivedPartitionMethod('geohash')).toBe('logical');
    expect(derivedPartitionMethod('mgrs')).toBe('logical');
    expect(derivedPartitionMethod('isea4h')).toBe('entity');
    expect(nativeLevelLabel('mgrs', 3)).toBe('第 3 级 · 100 m');
    expect(nativeLevelLabel('geohash', 6)).toBe('第 6 级 · 精度 6');
    expect(nativeLevelLabel('isea4h', 7)).toBe('第 7 级 · 分辨率 7');
  });
});
