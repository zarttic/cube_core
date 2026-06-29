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

export async function requestJson(path, payload = {}) {
  const { signal, clear } = timeoutSignal();
  const response = await fetch(path, {
    method: 'POST',
    headers: authHeaders(JSON_HEADERS),
    body: JSON.stringify(payload),
    signal,
  });
  try {
    return await parseResponse(response);
  } finally {
    clear();
  }
}

export async function requestGet(path) {
  const { signal, clear } = timeoutSignal();
  const response = await fetch(path, {
    method: 'GET',
    headers: authHeaders(),
    signal,
  });
  try {
    return await parseResponse(response);
  } finally {
    clear();
  }
}

export async function requestPost(path, payload = {}) {
  return requestJson(path, payload);
}

export function apiPrefixes() {
  return {
    gridPrefix: '/v1/grid',
    codePrefix: '/v1/code',
    topologyPrefix: '/v1/topology',
    partitionPrefix: '/v1/partition',
    qualityPrefix: '/v1/quality',
    ingestPrefix: '/v1/ingest',
    configPrefix: '/v1/config',
  };
}
