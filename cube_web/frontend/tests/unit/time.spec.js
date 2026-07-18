import { describe, expect, it } from 'vitest';

import { formatShanghaiRange, formatShanghaiTime } from '@/utils/time';

describe('Shanghai time formatting', () => {
  it('formats offset and UTC timestamps in Asia/Shanghai', () => {
    expect(formatShanghaiTime('2026-07-18T03:39:26.519723+08:00')).toBe('2026-07-18 03:39:26');
    expect(formatShanghaiTime('2026-07-17T19:39:26.519Z')).toBe('2026-07-18 03:39:26');
  });

  it('keeps date-only values and handles empty ranges', () => {
    expect(formatShanghaiTime('2026-07-18')).toBe('2026-07-18');
    expect(formatShanghaiRange(null, null)).toBe('- 至 -');
  });
});
