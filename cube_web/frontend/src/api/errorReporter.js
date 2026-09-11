// Batched browser error reporting. Failures are swallowed on purpose: reporting
// must never surface a second error to the user or recurse into client.js.

const ENDPOINT = '/v1/client-errors';
const MAX_BUFFERED_EVENTS = 20;
const MAX_BATCH_EVENTS = 10;
const FLUSH_DELAY_MS = 5000;

const pending = [];
let flushTimer = null;
let listenersInstalled = false;

function sampleRate() {
  const value = Number(import.meta.env.VITE_ERROR_REPORT_SAMPLE);
  if (!Number.isFinite(value)) return 1;
  return Math.min(Math.max(value, 0), 1);
}

function buildPayload(events) {
  return JSON.stringify({
    client: {
      version: String(import.meta.env.VITE_APP_VERSION || ''),
      ua: typeof navigator === 'undefined' ? '' : String(navigator.userAgent || '').slice(0, 300),
    },
    events,
  });
}

function sendBatch(events) {
  if (!events.length) return;
  const payload = buildPayload(events);
  if (typeof navigator !== 'undefined' && typeof navigator.sendBeacon === 'function') {
    try {
      if (navigator.sendBeacon(ENDPOINT, new Blob([payload], { type: 'application/json' }))) return;
    } catch {
      // Fall through to fetch.
    }
  }
  if (typeof fetch !== 'function') return;
  fetch(ENDPOINT, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: payload,
    keepalive: true,
  }).catch(() => {
    // Reporting is best effort.
  });
}

function scheduleFlush() {
  if (flushTimer !== null || typeof window === 'undefined') return;
  flushTimer = window.setTimeout(() => {
    flushTimer = null;
    flush();
  }, FLUSH_DELAY_MS);
}

export function reportEvent(event) {
  if (!event || typeof event !== 'object') return;
  if (Math.random() > sampleRate()) return;
  pending.push({
    ts: new Date().toISOString(),
    level: 'error',
    ...event,
  });
  if (pending.length > MAX_BUFFERED_EVENTS) {
    pending.splice(0, pending.length - MAX_BUFFERED_EVENTS);
  }
  scheduleFlush();
}

export function flush() {
  if (flushTimer !== null && typeof window !== 'undefined') {
    window.clearTimeout(flushTimer);
    flushTimer = null;
  }
  while (pending.length) {
    sendBatch(pending.splice(0, MAX_BATCH_EVENTS));
  }
}

export function installErrorReporter() {
  if (listenersInstalled || typeof window === 'undefined') return () => {};
  listenersInstalled = true;
  const onVisibilityChange = () => {
    if (document.visibilityState === 'hidden') flush();
  };
  window.addEventListener('pagehide', flush);
  document.addEventListener('visibilitychange', onVisibilityChange);
  return () => {
    listenersInstalled = false;
    window.removeEventListener('pagehide', flush);
    document.removeEventListener('visibilitychange', onVisibilityChange);
  };
}
