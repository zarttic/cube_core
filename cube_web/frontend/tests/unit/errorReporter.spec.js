import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { flush, reportEvent } from '@/api/errorReporter';

describe('errorReporter', () => {
  beforeEach(() => {
    vi.stubEnv('VITE_ERROR_REPORT_SAMPLE', '1');
    vi.stubGlobal('navigator', { userAgent: 'vitest-agent', sendBeacon: undefined });
  });

  afterEach(() => {
    vi.unstubAllEnvs();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it('batches events and posts them to the client error endpoint', () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('', { status: 202 }));

    reportEvent({ message: 'boom', scope: 'api', status: 500, request_id: 'req-1' });
    flush();

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe('/v1/client-errors');
    const payload = JSON.parse(options.body);
    expect(payload.events[0]).toMatchObject({ message: 'boom', scope: 'api', status: 500, request_id: 'req-1' });
    expect(payload.client.ua).toBe('vitest-agent');
  });

  it('prefers sendBeacon when the browser supports it', () => {
    const beacon = vi.fn().mockReturnValue(true);
    vi.stubGlobal('navigator', { userAgent: 'vitest-agent', sendBeacon: beacon });
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('', { status: 202 }));

    reportEvent({ message: 'beacon me' });
    flush();

    expect(beacon).toHaveBeenCalledTimes(1);
    expect(beacon.mock.calls[0][0]).toBe('/v1/client-errors');
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('drops events excluded by sampling', () => {
    vi.stubEnv('VITE_ERROR_REPORT_SAMPLE', '0');
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('', { status: 202 }));

    reportEvent({ message: 'sampled-out' });
    flush();

    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('swallows transport failures', () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockRejectedValue(new Error('offline'));

    reportEvent({ message: 'network down' });

    expect(() => flush()).not.toThrow();
    expect(fetchMock).toHaveBeenCalled();
  });
});
