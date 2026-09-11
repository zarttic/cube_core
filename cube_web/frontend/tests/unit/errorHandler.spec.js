import { ElMessage } from 'element-plus';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { flush } from '@/api/errorReporter';
import { installGlobalErrorHandlers, notifyApiError } from '@/utils/errorHandler';

function apiError(overrides = {}) {
  const error = new Error(overrides.message || '请求失败');
  Object.assign(error, { status: 500, code: 'internal_error', requestId: 'req-9' }, overrides);
  return error;
}

describe('errorHandler', () => {
  beforeEach(() => {
    vi.stubEnv('VITE_ERROR_REPORT_SAMPLE', '1');
    vi.stubGlobal('navigator', { userAgent: 'vitest-agent', sendBeacon: undefined });
  });

  afterEach(() => {
    vi.unstubAllEnvs();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it('toasts and reports a 5xx error with its request id', () => {
    const toast = vi.spyOn(ElMessage, 'error').mockImplementation(() => {});
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('', { status: 202 }));

    notifyApiError(apiError(), { scope: 'ConfigView' });
    flush();

    expect(toast).toHaveBeenCalledWith('请求失败（请求 ID：req-9）');
    const payload = JSON.parse(fetchMock.mock.calls[0][1].body);
    expect(payload.events[0]).toMatchObject({ scope: 'ConfigView', status: 500, request_id: 'req-9' });
  });

  it('suppresses duplicate toasts inside the dedupe window', () => {
    const toast = vi.spyOn(ElMessage, 'error').mockImplementation(() => {});
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('', { status: 202 }));

    notifyApiError(apiError(), { scope: 'QualityView' });
    notifyApiError(apiError(), { scope: 'QualityView' });
    flush();

    expect(toast).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it('logs and reports silently for polling paths', () => {
    const toast = vi.spyOn(ElMessage, 'error').mockImplementation(() => {});
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('', { status: 202 }));

    notifyApiError(apiError({ status: 503, message: '轮询失败' }), { silent: true, scope: 'quality-poll' });
    flush();

    expect(toast).not.toHaveBeenCalled();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it('ignores expected statuses', () => {
    const toast = vi.spyOn(ElMessage, 'error').mockImplementation(() => {});
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('', { status: 202 }));

    notifyApiError(apiError({ status: 404, message: '不存在' }), { expectedStatuses: [404] });
    flush();

    expect(toast).not.toHaveBeenCalled();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('reports Vue component errors through the global handler', () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('', { status: 202 }));
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const app = { config: {} };

    installGlobalErrorHandlers(app);
    app.config.errorHandler(new Error('render exploded'), null, 'render');
    flush();

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const payload = JSON.parse(fetchMock.mock.calls[0][1].body);
    expect(payload.events[0].message).toContain('render exploded');
    expect(payload.events[0].scope).toBe('vue');
    consoleSpy.mockRestore();
  });

  it('uses a custom display message when provided', () => {
    const toast = vi.spyOn(ElMessage, 'error').mockImplementation(() => {});
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('', { status: 202 }));

    notifyApiError(apiError({ status: 422, requestId: '' }), { scope: 'custom', message: '自定义提示' });
    flush();

    expect(toast).toHaveBeenCalledWith('自定义提示');
    const payload = JSON.parse(fetchMock.mock.calls[0][1].body);
    expect(payload.events[0].message).toBe('自定义提示');
  });
});
