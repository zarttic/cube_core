import { describe, expect, it } from 'vitest';

import { derivedPartitionMethod, gridDefinitions, nativeLevelLabel } from '@/utils/grid';

describe('grid contract', () => {
  it('exposes only production grids and derives their methods', () => {
    expect(gridDefinitions.map((item) => item.value)).toEqual(['geohash', 'mgrs', 'isea4h']);
    expect(derivedPartitionMethod('geohash')).toBe('logical');
    expect(derivedPartitionMethod('mgrs')).toBe('logical');
    expect(derivedPartitionMethod('isea4h')).toBe('entity');
    expect(nativeLevelLabel('mgrs', 3)).toBe('100 m');
  });
});
