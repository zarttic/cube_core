import { describe, expect, it, vi } from 'vitest';

import { combineAbortSignals, download, request } from '@/api/client';

describe('api client', () => {
  it('uses the supplied method and merges cancellation signals', async () => {
    const caller = new AbortController();
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('{"ok":true}', { status: 200 }));
    const combined = combineAbortSignals([caller.signal]);

    await request('/v1/example', { method: 'PATCH', body: { enabled: true }, signal: combined.signal });

    expect(fetchMock).toHaveBeenCalledWith('/v1/example', expect.objectContaining({ method: 'PATCH' }));
    caller.abort();
    expect(combined.signal.aborted).toBe(true);
    combined.dispose();
    fetchMock.mockRestore();
  });

  it('downloads an authenticated response using its Content-Disposition filename', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('id,message\n1,test\n', {
      status: 200,
      headers: {
        'Content-Disposition': 'attachment; filename="quality.csv"',
        'Content-Type': 'text/csv',
      },
    }));

    const result = await download('/v1/quality/records/run-1/errors/export?format=csv');

    expect(result.filename).toBe('quality.csv');
    expect(result.blob.type).toBe('text/csv');
    fetchMock.mockRestore();
  });
});
