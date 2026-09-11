import { describe, expect, it, vi } from 'vitest';

import { combineAbortSignals, download, formatApiErrorMessage, request } from '@/api/client';

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

  it('formats FastAPI validation details as readable Chinese text', async () => {
    expect(formatApiErrorMessage([
      { loc: ['body', 'worker_container_limit'], msg: 'Value error, 容器数量必须是大于等于 0 的整数' },
      { loc: ['body', 'grid_type'], msg: 'Field required' },
    ])).toBe('容器数量必须是大于等于 0 的整数；格网类型：字段不能为空');

    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response(JSON.stringify({
      detail: [{ loc: ['body', 'worker_container_limit'], msg: 'Value error, 容器数量必须是大于等于 0 的整数' }],
    }), { status: 422, headers: { 'Content-Type': 'application/json' } }));

    await expect(request('/v1/partition/runs', { method: 'POST', body: {} }))
      .rejects.toThrow('容器数量必须是大于等于 0 的整数');
    fetchMock.mockRestore();
  });
});

describe('api client auth signalling', () => {
  it('emits an auth failure for 401 responses on protected paths', async () => {
    const { onAuthFailure } = await import('@/api/authEvents');
    const listener = vi.fn();
    const unsubscribe = onAuthFailure(listener);
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response(JSON.stringify({
      error: { code: 'unauthorized', message: '认证失败', request_id: 'req-401' },
    }), { status: 401, headers: { 'Content-Type': 'application/json' } }));

    await expect(request('/v1/partition/tasks')).rejects.toThrow('认证失败');

    expect(listener).toHaveBeenCalledTimes(1);
    expect(listener.mock.calls[0][0]).toMatchObject({ status: 401, isApiError: true, requestId: 'req-401' });
    unsubscribe();
    fetchMock.mockRestore();
  });

  it('can opt out of auth failure signalling', async () => {
    const { onAuthFailure } = await import('@/api/authEvents');
    const listener = vi.fn();
    const unsubscribe = onAuthFailure(listener);
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response(JSON.stringify({
      error: { code: 'unauthorized', message: '认证失败' },
    }), { status: 401, headers: { 'Content-Type': 'application/json' } }));

    await expect(request('/v1/partition/tasks', { authRedirect: false })).rejects.toThrow('认证失败');

    expect(listener).not.toHaveBeenCalled();
    unsubscribe();
    fetchMock.mockRestore();
  });
});
