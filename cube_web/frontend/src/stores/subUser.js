import { computed, reactive } from 'vue';

import { requestGet, requestPost } from '@/api/client';
import { AUTH_CONFIG, authRequired } from '@/config';

const state = reactive({
  token: localStorage.getItem('access_token') || '',
  userInfo: readUserInfo(),
});

function readUserInfo() {
  try {
    return JSON.parse(localStorage.getItem('user_info') || '{}');
  } catch {
    return {};
  }
}

function persistToken(token) {
  state.token = token || '';
  if (token) {
    localStorage.setItem('access_token', token);
  } else {
    localStorage.removeItem('access_token');
  }
}

function persistUserInfo(userInfo) {
  state.userInfo = userInfo || {};
  if (userInfo && Object.keys(userInfo).length) {
    localStorage.setItem('user_info', JSON.stringify(userInfo));
  } else {
    localStorage.removeItem('user_info');
  }
}

function safeTargetPath(targetPath) {
  const value = String(targetPath || window.location.pathname || '/');
  if (!value.startsWith('/') || value.startsWith('//')) return '/';
  return value;
}

function authRedirectUri() {
  const base = new URL(AUTH_CONFIG.REDIRECT_URI, window.location.origin);
  base.hash = '';
  return base.toString();
}

export function useSubUserStore() {
  const username = computed(() => state.userInfo.username || '');
  const role = computed(() => state.userInfo.role || '普通用户');
  const avatarUrl = computed(() => state.userInfo.avatarUrl || state.userInfo.avatar_url || '');
  const isAuthenticated = computed(() => Boolean(state.token));

  async function fetchUserInfo() {
    if (!state.token) return null;
    const userInfo = await requestGet('/api/me');
    persistUserInfo(userInfo);
    return userInfo;
  }

  async function exchangeCode(code, stateValue = '') {
    const query = new URLSearchParams({ code });
    if (stateValue) query.set('state', stateValue);
    const response = await requestGet(`/api/callback?${query.toString()}`);
    const token = response.access_token || response.token;
    if (!token) throw new Error('登录回调未返回 access_token');
    persistToken(token);
    await fetchUserInfo();
    return response;
  }

  function redirectToAuth(targetPath = window.location.pathname || '/') {
    const target = safeTargetPath(targetPath);
    const query = new URLSearchParams({
      target,
      redirect_uri: authRedirectUri(),
    });
    window.location.href = `/api/auth/login?${query.toString()}`;
  }

  async function logout() {
    const currentToken = state.token || localStorage.getItem('access_token') || '';
    if (currentToken) {
      try {
        await requestPost('/api/logout', {});
      } catch {
        // Local logout should succeed even when the upstream auth service is unavailable.
      }
    }
    persistToken('');
    persistUserInfo({});
    if (authRequired()) {
      window.location.replace(`${AUTH_CONFIG.MAIN_SYSTEM_URL}/?logout=true`);
      return;
    }
    window.location.replace(safeTargetPath(window.location.pathname || '/'));
  }

  return {
    state,
    username,
    role,
    avatarUrl,
    isAuthenticated,
    persistToken,
    fetchUserInfo,
    exchangeCode,
    redirectToAuth,
    logout,
  };
}
