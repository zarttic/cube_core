const JSON_HEADERS = { 'Content-Type': 'application/json' };
const REQUEST_TIMEOUT_MS = 30000;

export function accessToken() {
  return localStorage.getItem('access_token') || '';
}

export function authHeaders(headers = {}) {
  const token = accessToken();
  return token ? { ...headers, Authorization: `Bearer ${token}` } : headers;
}

async function parseResponse(response) {
  const text = await response.text();
  let body = {};
  if (text) {
    try {
      body = JSON.parse(text);
    } catch (_error) {
      if (!response.ok) {
        const error = new Error(text.trim() || `请求失败: ${response.status}`);
        error.status = response.status;
        throw error;
      }
      const error = new Error('服务返回了非 JSON 响应');
      error.status = response.status;
      throw error;
    }
  }
  if (!response.ok) {
    const message = body?.error?.message || body?.detail || `请求失败: ${response.status}`;
    const error = new Error(message);
    error.status = response.status;
    throw error;
  }
  return body;
}

function timeoutSignal(timeoutMs = REQUEST_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);
  return {
    signal: controller.signal,
    clear() {
      window.clearTimeout(timer);
    },
  };
}

export function combineAbortSignals(signals = []) {
  const activeSignals = signals.filter(Boolean);
  if (!activeSignals.length) return { signal: undefined, dispose() {} };
  const controller = new AbortController();
  const abort = () => controller.abort();
  activeSignals.forEach((signal) => {
    if (signal.aborted) abort();
    else signal.addEventListener('abort', abort, { once: true });
  });
  return {
    signal: controller.signal,
    dispose() {
      activeSignals.forEach((signal) => signal.removeEventListener('abort', abort));
    },
  };
}

export async function request(path, { method = 'GET', body, headers = {}, signal, responseType = 'json' } = {}) {
  const timeout = timeoutSignal();
  const combined = combineAbortSignals([signal, timeout.signal]);
  try {
    const response = await fetch(path, {
      method,
      headers: authHeaders(body === undefined ? headers : { ...JSON_HEADERS, ...headers }),
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      signal: combined.signal,
    });
    if (responseType === 'blob') {
      if (!response.ok) await parseResponse(response);
      return response;
    }
    return await parseResponse(response);
  } finally {
    combined.dispose();
    timeout.clear();
  }
}

export function requestJson(path, payload = {}, options = {}) {
  return request(path, { ...options, method: options.method || 'POST', body: payload });
}

export function requestGet(path, options = {}) {
  return request(path, { ...options, method: 'GET' });
}

export function requestPost(path, payload = {}, options = {}) {
  return requestJson(path, payload, options);
}

export async function download(path, options = {}) {
  const response = await request(path, { ...options, responseType: 'blob' });
  const disposition = response.headers.get('Content-Disposition') || '';
  const filenameMatch = disposition.match(/filename="?([^";]+)"?/i);
  return {
    blob: await response.blob(),
    filename: filenameMatch?.[1] || 'download',
  };
}

export function apiPrefixes() {
  return {
    gridPrefix: '/v1/grid',
    codePrefix: '/v1/code',
    topologyPrefix: '/v1/topology',
    partitionPrefix: '/v1/partition',
    qualityPrefix: '/v1/quality',
    configPrefix: '/v1/config',
  };
}
