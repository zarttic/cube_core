import { describe, expect, it } from 'vitest';

import { derivedPartitionMethod, gridDefinitions, nativeLevelLabel, recommendedGridLevel } from '@/utils/grid';

describe('grid contract', () => {
  it('exposes only production grids and derives their methods', () => {
    expect(gridDefinitions.map((item) => item.value)).toEqual(['geohash', 'mgrs', 'isea4h']);
    expect(gridDefinitions.map((item) => item.label)).toEqual(['经纬度格网', '平面格网', '六边形格网']);
    expect(derivedPartitionMethod('geohash')).toBe('logical');
    expect(derivedPartitionMethod('mgrs')).toBe('logical');
    expect(derivedPartitionMethod('isea4h')).toBe('entity');
    expect(nativeLevelLabel('mgrs', 3)).toBe('层级 3');
    expect(nativeLevelLabel('geohash', 6)).toBe('层级 6');
    expect(nativeLevelLabel('isea4h', 7)).toBe('层级 7');
  });

  it('recommends a frozen level from the dataset resolution for each grid', () => {
    expect(recommendedGridLevel(10, 'geohash')).toBe(5);
    expect(recommendedGridLevel(10, 'mgrs')).toBe(0);
    expect(recommendedGridLevel(10, 'isea4h')).toBe(11);
    expect(recommendedGridLevel(undefined, 'geohash')).toBe(5);
  });
});
