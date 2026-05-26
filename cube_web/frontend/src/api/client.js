const JSON_HEADERS = { 'Content-Type': 'application/json' };

export async function requestJson(path, payload = {}) {
  const response = await fetch(path, {
    method: 'POST',
    headers: JSON_HEADERS,
    body: JSON.stringify(payload),
  });
  const text = await response.text();
  const body = text ? JSON.parse(text) : {};
  if (!response.ok) {
    const message = body?.error?.message || body?.detail || `请求失败: ${response.status}`;
    throw new Error(message);
  }
  return body;
}

export function apiPrefixes() {
  return {
    gridPrefix: '/v1/grid',
    codePrefix: '/v1/code',
    topologyPrefix: '/v1/topology',
    partitionPrefix: '/v1/partition',
    qualityPrefix: '/v1/quality',
    ingestPrefix: '/v1/ingest',
  };
}
