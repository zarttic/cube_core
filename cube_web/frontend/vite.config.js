import { fileURLToPath, URL } from 'node:url';

import vue from '@vitejs/plugin-vue';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [vue()],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  build: {
    outDir: '../cube_web/web',
    emptyOutDir: true,
  },
  server: {
    proxy: {
      '/v1': process.env.VITE_DEV_API_TARGET || 'http://127.0.0.1:50040',
      '/api': process.env.VITE_DEV_API_TARGET || 'http://127.0.0.1:50040',
      '/health': process.env.VITE_DEV_API_TARGET || 'http://127.0.0.1:50040',
    },
  },
});
