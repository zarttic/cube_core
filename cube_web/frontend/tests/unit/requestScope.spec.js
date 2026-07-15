import { describe, expect, it } from 'vitest';

import { createRequestScope } from '@/api/requestScope';

describe('request scope', () => {
  it('rejects a stale response after a newer request begins', () => {
    const scope = createRequestScope();
    const first = scope.begin();
    const second = scope.begin();

    expect(first.signal.aborted).toBe(true);
    expect(scope.isCurrent(first.token)).toBe(false);
    expect(scope.isCurrent(second.token)).toBe(true);
    scope.dispose();
  });
});
