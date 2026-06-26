<script setup>
import { computed, onBeforeUnmount, onMounted, ref } from 'vue';
import { navItems, normalizePath, portalHomeUrl } from '@/data/navigation';
import PartitionView from '@/views/PartitionView.vue';
import EncodingView from '@/views/EncodingView.vue';
import { authRequired, loadAuthRuntimeConfig } from '@/config';
import { useSubUserStore } from '@/stores/subUser';

const currentPath = ref(normalizePath(window.location.pathname));
const authReady = ref(false);
const userStore = useSubUserStore();

const pageMap = {
  '/partition': PartitionView,
  '/encoding': EncodingView,
};

const currentView = computed(() => pageMap[currentPath.value] || PartitionView);
const currentNavItems = computed(() => navItems());

function syncPathFromLocation() {
  const normalized = normalizePath(window.location.pathname);
  if (normalized !== '/' && normalized !== window.location.pathname) {
    window.history.replaceState({}, '', normalized);
  }
  currentPath.value = normalized;
}

function goInternal(path) {
  if (path === currentPath.value) return;
  window.history.pushState({}, '', path);
  currentPath.value = path;
}

function handlePopState() {
  syncPathFromLocation();
}

function isNavActive(item) {
  if (item.path === currentPath.value) return true;
  return false;
}

function targetFromAuthState(stateValue) {
  if (!stateValue) return '';
  try {
    const payload = JSON.parse(decodeURIComponent(window.atob(stateValue)));
    const target = safeLocalTarget(payload.target);
    if (target) return target;
  } catch {
    return '';
  }
  return '';
}

function safeLocalTarget(value) {
  const target = String(value || '');
  if (target.startsWith('/') && !target.startsWith('//')) return target;
  return '';
}

function redirectToPortalHomeIfNeeded() {
  if (currentPath.value !== '/') return false;
  const target = portalHomeUrl;
  if (!target || target === window.location.href) return false;
  window.location.replace(target);
  return true;
}

async function initializeAuth() {
  authReady.value = false;
  await loadAuthRuntimeConfig();
  const params = new URLSearchParams(window.location.search);
  const code = params.get('code');
  const state = params.get('state') || '';
  if (code) {
    await userStore.exchangeCode(code, state);
    const target = targetFromAuthState(state) || safeLocalTarget(params.get('target')) || '/';
    window.history.replaceState({}, '', target);
    syncPathFromLocation();
    if (redirectToPortalHomeIfNeeded()) return;
    authReady.value = true;
    return;
  }
  syncPathFromLocation();
  if (redirectToPortalHomeIfNeeded()) return;
  if (localStorage.getItem('access_token')) {
    try {
      await userStore.fetchUserInfo();
      if (redirectToPortalHomeIfNeeded()) return;
      authReady.value = true;
      return;
    } catch {
      localStorage.removeItem('access_token');
      localStorage.removeItem('user_info');
    }
  }
  if (authRequired()) {
    userStore.redirectToAuth(window.location.pathname + window.location.search);
    return;
  }
  authReady.value = true;
}

async function handleLogout() {
  await userStore.logout();
}

onMounted(async () => {
  window.addEventListener('popstate', handlePopState);
  await initializeAuth();
});
onBeforeUnmount(() => window.removeEventListener('popstate', handlePopState));
</script>

<template>
  <div class="app-shell">
    <header class="portal-header">
      <div class="portal-header-inner">
        <div class="portal-brand">
          <div class="portal-logo" aria-hidden="true">
            <svg viewBox="0 0 24 24">
              <circle cx="12" cy="12" r="10"></circle>
              <path d="M2 12h20"></path>
              <path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10"></path>
            </svg>
          </div>
          <div class="portal-brand-text">
            <div class="portal-brand-title">分析就绪数据剖分管理系统</div>
            <div class="portal-brand-subtitle">Analysis-Ready Data Partitioning Management System</div>
          </div>
        </div>
        <nav class="portal-nav" aria-label="主导航">
          <template v-for="item in currentNavItems" :key="item.label">
            <a
              v-if="item.kind === 'internal'"
              :href="item.path"
              :class="{ active: isNavActive(item) }"
              @click.prevent="goInternal(item.path)"
            >
              {{ item.label }}
            </a>
            <a v-else :href="item.url">{{ item.label }}</a>
          </template>
        </nav>
        <div class="portal-header-side">
          <div class="service-role-switch">
            <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" class="service-user-icon">
              <circle cx="12" cy="7" r="4"></circle>
              <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"></path>
            </svg>
            <span>{{ userStore.username.value || '访客' }} · {{ userStore.role.value || '普通用户' }}</span>
            <button class="service-auth-btn service-auth-btn-compact" type="button" @click="handleLogout">退出</button>
          </div>
        </div>
      </div>
    </header>

    <main>
      <component v-if="authReady" :is="currentView" />
    </main>
  </div>
</template>
