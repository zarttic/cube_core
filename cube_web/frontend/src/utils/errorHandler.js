// Unified error handling: one place decides how an API or UI error is logged,
// reported, de-duplicated, and surfaced to the user.

import { ElMessage } from 'element-plus';

import { reportEvent } from '@/api/errorReporter';
import { createLogger } from '@/utils/logger';

const apiLogger = createLogger('api');
const uiLogger = createLogger('ui');

const DEFAULT_DEDUPE_MS = 3000;
const DEDUPE_WINDOW_MS = 60000;
const recentKeys = new Map();

function currentRoute() {
  if (typeof window === 'undefined') return '';
  return `${window.location.pathname}${window.location.hash || ''}`;
}

function buildEntry(error, scope) {
  const status = Number.isFinite(error?.status) ? error.status : undefined;
  return {
    ts: new Date().toISOString(),
    level: status === undefined || status >= 500 ? 'error' : 'warn',
    scope,
    message: String(error?.message || '未知错误'),
    stack: typeof error?.stack === 'string' ? error.stack : undefined,
    route: currentRoute(),
    request_id: String(error?.requestId || ''),
    status,
    code: String(error?.code || ''),
  };
}

function dedupeKey(entry) {
  return `${entry.scope}|${entry.status ?? ''}|${entry.code}|${entry.message}`;
}

function isDuplicate(key, dedupeMs) {
  const now = Date.now();
  const previous = recentKeys.get(key);
  recentKeys.set(key, now);
  if (recentKeys.size > 100) {
    for (const [candidate, timestamp] of recentKeys) {
      if (now - timestamp > DEDUPE_WINDOW_MS) recentKeys.delete(candidate);
    }
  }
  return previous !== undefined && now - previous < dedupeMs;
}

/**
 * Handle an API error uniformly.
 *
 * @param {Error} error - normalized error thrown by src/api/client.js.
 * @param {object} [options]
 * @param {boolean} [options.silent] - log/report but do not toast (polling paths).
 * @param {number[]} [options.expectedStatuses] - statuses that are not errors for this caller.
 * @param {number} [options.dedupeMs] - suppress identical toasts within this window.
 * @param {string} [options.scope] - logger scope, usually the view or store name.
 */
export function notifyApiError(error, { silent = false, expectedStatuses = [], dedupeMs = DEFAULT_DEDUPE_MS, scope = 'api', message } = {}) {
  if (!error) return;
  if (expectedStatuses.includes(error.status)) return;
  const entry = buildEntry(error, scope);
  if (message) entry.message = String(message);
  if (isDuplicate(dedupeKey(entry), dedupeMs)) return;
  apiLogger.warn(entry.message, {
    status: entry.status,
    code: entry.code,
    requestId: entry.request_id,
  });
  reportEvent(entry);
  if (silent) return;
  const needsRequestId = entry.status >= 500 && entry.request_id && !entry.message.includes(entry.request_id);
  const text = needsRequestId ? `${entry.message}（请求 ID：${entry.request_id}）` : entry.message;
  ElMessage.error(text);
}

function reportGlobal(scope, message, error) {
  const entry = buildEntry(error, scope);
  entry.message = String(message || entry.message);
  if (isDuplicate(dedupeKey(entry), DEFAULT_DEDUPE_MS)) return;
  uiLogger.error(entry.message, error);
  reportEvent(entry);
}

export function installGlobalErrorHandlers(app) {
  app.config.errorHandler = (error, _instance, info) => {
    const reason = error instanceof Error && error.message ? `：${error.message}` : '';
    reportGlobal('vue', `组件错误（${info}）${reason}`, error);
  };

  if (typeof window === 'undefined') return;

  window.addEventListener('error', (event) => {
    const error = event.error instanceof Error ? event.error : new Error(event.message || '未捕获错误');
    reportGlobal('window', event.message || error.message, error);
  });

  window.addEventListener('unhandledrejection', (event) => {
    const reason = event.reason;
    const error = reason instanceof Error ? reason : new Error(String(reason ?? '未处理的 Promise 拒绝'));
    reportGlobal('promise', error.message, error);
  });
}
