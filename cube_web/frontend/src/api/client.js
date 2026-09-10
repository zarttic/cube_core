const JSON_HEADERS = { 'Content-Type': 'application/json' };
const REQUEST_TIMEOUT_MS = 30000;
const API_FIELD_LABELS = {
  worker_container_limit: '容器数量',
  partition_run_id: '剖分执行 ID',
  source_batch_ids: '来源载入批次',
  requested_grid_level: '格网层级',
  partition_method: '剖分方式',
  grid_type: '格网类型',
  datasets: '数据集',
};

function normalizeValidationMessage(value) {
  const message = String(value || '').trim();
  if (!message) return '';
  return message
    .replace(/^value error,\s*/i, '')
    .replace(/^field required$/i, '字段不能为空')
    .replace(/^extra inputs are not permitted$/i, '包含不支持的字段')
    .replace(/^input should be a valid integer.*$/i, '必须是整数')
    .replace(/^input should be a valid string.*$/i, '必须是文本')
    .replace(/^input should be a valid dictionary.*$/i, '必须是对象')
    .replace(/^input should be a valid list.*$/i, '必须是列表');
}

export function formatApiErrorMessage(value, fallback = '请求失败') {
  if (Array.isArray(value)) {
    const messages = value.map((item) => {
      if (typeof item === 'string') return normalizeValidationMessage(item);
      if (!item || typeof item !== 'object') return normalizeValidationMessage(item);
      const message = normalizeValidationMessage(item.msg || item.message || item.detail);
      if (!message) return '';
      const location = Array.isArray(item.loc)
        ? item.loc.map((part) => API_FIELD_LABELS[part] || part).filter((part) => part !== 'body').join('、')
        : '';
      return location && !message.includes(location) ? `${location}：${message}` : message;
    }).filter(Boolean);
    return messages.join('；') || fallback;
  }
  if (value && typeof value === 'object') {
    return formatApiErrorMessage(value.message || value.msg || value.detail, fallback);
  }
  return normalizeValidationMessage(value) || fallback;
}

export function accessToken() {
  return localStorage.getItem('access_token') || '';
}

export function authHeaders(headers = {}) {
  const token = accessToken();
  return token ? { ...headers, Authorization: `Bearer ${token}` } : headers;
}

async function parseResponse(response) {
  const text = await response.text();
  const requestId = response.headers.get('X-Request-ID') || '';
  let body = {};
  if (text) {
    try {
      body = JSON.parse(text);
    } catch (_error) {
      if (!response.ok) {
        const error = new Error(response.status >= 500 ? '服务器暂时不可用，请稍后重试' : (text.trim() || `请求失败（${response.status}）`));
        error.status = response.status;
        error.code = 'non_json_error';
        error.requestId = requestId;
        error.retryable = response.status >= 500 || response.status === 408 || response.status === 429;
        throw error;
      }
      const error = new Error('服务返回了非 JSON 响应');
      error.status = response.status;
      error.code = 'invalid_success_response';
      error.requestId = requestId;
      throw error;
    }
  }
  if (!response.ok) {
    const apiError = body?.error && typeof body.error === 'object' ? body.error : {};
    // The backend keeps the structured validation list in `detail`; prefer it
    // over the generic envelope message so field names survive the transport.
    const messageValue = Array.isArray(body?.detail) ? body.detail : (apiError.message ?? body?.detail);
    const message = formatApiErrorMessage(
      messageValue,
      `请求失败（${response.status}）`,
    );
    const error = new Error(message);
    error.status = response.status;
    error.code = apiError.code || (typeof body?.detail === 'object' ? body.detail.code : '') || '';
    error.requestId = apiError.request_id || requestId;
    error.retryable = response.status >= 500 || response.status === 408 || response.status === 429;
    error.detail = body?.detail;
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

export async function request(path, { method = 'GET', body, headers = {}, signal, responseType = 'json', timeoutMs = REQUEST_TIMEOUT_MS } = {}) {
  const timeout = timeoutSignal(timeoutMs);
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
  } catch (caught) {
    if (caught?.name === 'AbortError' && timeout.signal.aborted && !signal?.aborted) {
      const error = new Error(`请求超时，请到任务列表确认任务状态后再重试（${method} ${path}）`);
      error.name = 'RequestTimeoutError';
      error.status = 408;
      error.code = 'request_timeout';
      error.retryable = true;
      error.uncertain = method !== 'GET' && method !== 'HEAD';
      throw error;
    }
    throw caught;
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
