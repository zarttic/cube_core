<script setup>
import { computed } from 'vue';

import { navItems } from '@/data/navigation';
import { useSubUserStore } from '@/stores/subUser';

const userStore = useSubUserStore();
const isAdmin = computed(() => userStore.role.value === '管理员');

async function handleLogout() {
  await userStore.logout();
}
</script>

<template>
  <div class="app-shell">
    <header class="portal-header">
      <div class="portal-header-inner">
        <div class="portal-brand">
          <div class="portal-logo" aria-hidden="true">
            <svg viewBox="0 0 24 24">
              <circle cx="12" cy="12" r="10" />
              <path d="M2 12h20" />
              <path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10" />
            </svg>
          </div>
          <div class="portal-brand-text">
            <div class="portal-brand-title">分析就绪数据剖分管理系统</div>
            <div class="portal-brand-subtitle">Analysis-Ready Data Partitioning Management System</div>
          </div>
        </div>
        <nav class="portal-nav" aria-label="主导航">
          <template v-for="item in navItems(isAdmin)" :key="item.label">
            <RouterLink v-if="item.kind === 'internal'" :to="item.path" active-class="active active-nav">{{ item.label }}</RouterLink>
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
