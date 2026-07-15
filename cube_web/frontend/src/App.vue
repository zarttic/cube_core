<script setup>
import { onMounted, ref } from 'vue';

import { authRequired, loadAuthRuntimeConfig } from '@/config';
import AppLayout from '@/layouts/AppLayout.vue';
import router, { resolveApplicationAuthReady, safeLocalTarget } from '@/router';
import { useSubUserStore } from '@/stores/subUser';

const authReady = ref(false);
const userStore = useSubUserStore();

function targetFromState(stateValue) {
  if (!stateValue) return '';
  try {
    return safeLocalTarget(JSON.parse(decodeURIComponent(window.atob(stateValue))).target);
  } catch {
    return '';
  }
}

async function initializeApplicationAuth() {
  await loadAuthRuntimeConfig();
  const params = new URLSearchParams(window.location.search);
  try {
    if (router.currentRoute.value.path === '/callback' && params.get('code')) {
      const state = params.get('state') || '';
      await userStore.exchangeCode(params.get('code'), state);
      await router.replace(targetFromState(state) || safeLocalTarget(params.get('target')) || '/partition');
    } else if (localStorage.getItem('access_token')) {
      try {
        await userStore.fetchUserInfo();
      } catch {
        userStore.persistToken('');
        userStore.persistUserInfo({});
      }
    }
    if (authRequired() && !userStore.isAuthenticated.value) {
      userStore.redirectToAuth(router.currentRoute.value.fullPath);
      return;
    }
    authReady.value = true;
  } finally {
    resolveApplicationAuthReady();
  }
}

onMounted(initializeApplicationAuth);
</script>

<template>
  <AppLayout>
    <RouterView v-if="authReady" />
  </AppLayout>
</template>
