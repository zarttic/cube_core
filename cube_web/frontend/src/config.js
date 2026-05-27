export const AUTH_CONFIG = {
  CLIENT_ID: import.meta.env.VITE_AUTH_CLIENT_ID || 'system_ard',
  MAIN_SYSTEM_URL: (import.meta.env.VITE_AUTH_MAIN_SYSTEM_URL || 'http://10.136.1.14:5177').replace(/\/$/, ''),
  REDIRECT_URI: import.meta.env.VITE_AUTH_REDIRECT_URI || 'http://10.136.1.14:50040/callback',
};

export const AUTH_REQUIRED = String(import.meta.env.VITE_AUTH_REQUIRED || 'true').toLowerCase() !== 'false';
