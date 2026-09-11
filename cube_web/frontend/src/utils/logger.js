// Lightweight front-end logger: a bounded in-memory buffer for diagnostics plus
// console output. Production keeps only warnings and errors on the console; the
// error reporter forwards the same entries to the backend.

const MAX_BUFFER_ENTRIES = 50;
const LOG_BUFFER = [];

function normalizeDetail(detail) {
  if (!detail) return undefined;
  if (detail instanceof Error) {
    return { name: detail.name, message: detail.message, stack: detail.stack };
  }
  return detail;
}

function emitToConsole(level, scope, message, detail) {
  const visible = import.meta.env.DEV || level === 'warn' || level === 'error';
  if (!visible) return;
  const method = typeof console[level] === 'function' ? level : 'log';
  if (detail === undefined) console[method](`[${scope}] ${message}`);
  else console[method](`[${scope}] ${message}`, detail);
}

function recordEntry(level, scope, message, detail) {
  const entry = {
    ts: new Date().toISOString(),
    level,
    scope,
    message: String(message ?? ''),
    detail: normalizeDetail(detail),
  };
  LOG_BUFFER.push(entry);
  if (LOG_BUFFER.length > MAX_BUFFER_ENTRIES) LOG_BUFFER.shift();
  emitToConsole(level, scope, entry.message, entry.detail);
  return entry;
}

export function createLogger(scope) {
  return {
    debug(message, detail) {
      return recordEntry('debug', scope, message, detail);
    },
    info(message, detail) {
      return recordEntry('info', scope, message, detail);
    },
    warn(message, detail) {
      return recordEntry('warn', scope, message, detail);
    },
    error(message, detail) {
      return recordEntry('error', scope, message, detail);
    },
  };
}

export function getRecentLogs() {
  return LOG_BUFFER.map((entry) => ({ ...entry }));
}
