import { describe, expect, it, vi } from 'vitest';

import { createLogger, getRecentLogs } from '@/utils/logger';

describe('logger', () => {
  it('records entries with scope, level, and normalized error detail', () => {
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const logger = createLogger('test-scope');

    logger.error('测试失败', new Error('boom'));

    const entry = getRecentLogs().at(-1);
    expect(entry).toMatchObject({ level: 'error', scope: 'test-scope', message: '测试失败' });
    expect(entry.detail).toMatchObject({ message: 'boom' });
    expect(consoleSpy).toHaveBeenCalled();
    consoleSpy.mockRestore();
  });

  it('keeps the buffer bounded', () => {
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const logger = createLogger('buffer');

    for (let index = 0; index < 60; index += 1) {
      logger.error(`entry-${index}`);
    }

    expect(getRecentLogs()).toHaveLength(50);
    expect(getRecentLogs().at(-1).message).toBe('entry-59');
    consoleSpy.mockRestore();
  });

  it('does not fail when optional detail is absent', () => {
    const consoleSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const logger = createLogger('plain');

    logger.warn('无详情');

    expect(getRecentLogs().at(-1)).toMatchObject({ level: 'warn', message: '无详情' });
    expect(getRecentLogs().at(-1).detail).toBeUndefined();
    consoleSpy.mockRestore();
  });
});
