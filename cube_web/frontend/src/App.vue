<script setup>
import { onMounted, ref } from 'vue';

import { authRequired, loadAuthRuntimeConfig } from '@/config';
import AppLayout from '@/layouts/AppLayout.vue';
import router, { resolveApplicationAuthReady, safeLocalTarget } from '@/router';
import { useSubUserStore } from '@/stores/subUser';

const authReady = ref(false);
const authError = ref('');
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
  const isCallback = window.location.pathname === '/callback' && Boolean(params.get('code'));
  let callbackTarget = '';
  try {
    if (isCallback) {
      const state = params.get('state') || '';
      await userStore.exchangeCode(params.get('code'), state);
      callbackTarget = targetFromState(state) || safeLocalTarget(params.get('target')) || '/partition';
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
  } catch (error) {
    userStore.persistToken('');
    userStore.persistUserInfo({});
    authError.value = '登录回调处理失败，请重新登录。';
    authReady.value = true;
    console.error('Authentication callback failed', error);
  } finally {
    resolveApplicationAuthReady();
  }
  if (callbackTarget) await router.replace(callbackTarget);
}

function retryLogin() {
  authError.value = '';
  userStore.redirectToAuth('/');
}

onMounted(initializeApplicationAuth);
</script>

<template>
  <AppLayout>
    <div v-if="authError" class="auth-error" role="alert">
      <p>{{ authError }}</p>
      <el-button type="primary" @click="retryLogin">重新登录</el-button>
    </div>
    <RouterView v-else-if="authReady" />
  </AppLayout>
</template>

<style scoped>
.auth-error {
  padding: 32px;
  text-align: center;
}

.auth-error p {
  margin: 0 0 16px;
}
</style>
