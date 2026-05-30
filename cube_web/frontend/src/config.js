function parseFlag(value, fallback = false) {
  if (value === undefined || value === null || String(value).trim() === '') return fallback;
  return ['1', 'true', 'yes', 'on'].includes(String(value).trim().toLowerCase());
}

export const AUTH_CONFIG = {
  CLIENT_ID: import.meta.env.VITE_AUTH_CLIENT_ID || 'system_ard',
  MAIN_SYSTEM_URL: (import.meta.env.VITE_AUTH_MAIN_SYSTEM_URL || 'http://10.136.1.14:5177').replace(/\/$/, ''),
  REDIRECT_URI: import.meta.env.VITE_AUTH_REDIRECT_URI || 'http://10.136.1.14:50040/callback',
};

const authRuntimeState = {
  required: parseFlag(import.meta.env.VITE_AUTH_REQUIRED, false),
};

export function authRequired() {
  return authRuntimeState.required;
}

export async function loadAuthRuntimeConfig() {
  try {
    const response = await fetch('/api/config', { method: 'GET' });
    if (!response.ok) return { ...AUTH_CONFIG, auth_required: authRuntimeState.required };
    const body = await response.json();
    if (body && typeof body === 'object') {
      if (body.client_id) AUTH_CONFIG.CLIENT_ID = String(body.client_id);
      if (body.main_system_url) AUTH_CONFIG.MAIN_SYSTEM_URL = String(body.main_system_url).replace(/\/$/, '');
      if (body.redirect_uri) AUTH_CONFIG.REDIRECT_URI = String(body.redirect_uri);
      authRuntimeState.required = parseFlag(body.auth_required, authRuntimeState.required);
    }
  } catch {
    // Fall back to bundled defaults when the runtime config endpoint is unavailable.
  }
  return { ...AUTH_CONFIG, auth_required: authRuntimeState.required };
}
