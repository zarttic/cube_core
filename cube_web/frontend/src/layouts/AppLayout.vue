<script setup>
import { computed } from 'vue';

import { authRequired } from '@/config';
import { navItems } from '@/data/navigation';
import { useSubUserStore } from '@/stores/subUser';
import nodcLogo from '@/assets/nodc-logo.png';

const userStore = useSubUserStore();
const navigationAccess = computed(() => ({
  isSuperAdmin: !authRequired() || userStore.isSuperAdmin.value,
  can: (permission) => !authRequired() || userStore.can(permission),
}));

async function handleLogout() {
  await userStore.logout();
}
</script>

<template>
  <div class="app-shell">
    <header class="portal-header">
      <div class="portal-header-inner">
        <div class="portal-brand">
          <img
            class="portal-brand-image"
            :src="nodcLogo"
            alt="国家对地观测科学数据中心 National Earth Observation Science Data Center"
          />
        </div>
        <nav class="portal-nav" aria-label="主导航">
          <template v-for="item in navItems(navigationAccess)" :key="item.label">
            <RouterLink v-if="item.kind === 'internal' || item.kind === 'admin'" :to="item.path" active-class="active active-nav">{{ item.label }}</RouterLink>
            <a v-else :href="item.url">{{ item.label }}</a>
          </template>
        </nav>
        <div class="portal-header-side">
          <div class="service-role-switch">
            <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" class="service-user-icon">
              <circle cx="12" cy="7" r="4" />
              <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2" />
            </svg>
            <span>{{ userStore.username.value || userStore.role.value || '普通用户' }}</span>
            <span v-if="userStore.username.value"> · {{ userStore.role.value || '普通用户' }}</span>
            <button v-if="userStore.isAuthenticated.value" class="service-auth-btn service-auth-btn-compact" type="button" @click="handleLogout">退出</button>
          </div>
        </div>
      </div>
    </header>
    <main><slot /></main>
  </div>
</template>
