import { fileURLToPath, URL } from 'node:url';

import vue from '@vitejs/plugin-vue';
import { defineConfig } from 'vite';
import { viteStaticCopy } from 'vite-plugin-static-copy';

const cesiumSource = 'node_modules/cesium/Build/Cesium';
const cesiumBaseUrl = 'cesiumStatic';
const backendTarget = process.env.VITE_DEV_API_TARGET || 'http://127.0.0.1:50039';

export default defineConfig({
  define: {
    CESIUM_BASE_URL: JSON.stringify(`/${cesiumBaseUrl}`),
  },
  plugins: [
    vue(),
    viteStaticCopy({
      targets: [
        { src: `${cesiumSource}/ThirdParty/**/*`, dest: cesiumBaseUrl, rename: { stripBase: 4 } },
        { src: `${cesiumSource}/Workers/**/*`, dest: cesiumBaseUrl, rename: { stripBase: 4 } },
        { src: `${cesiumSource}/Assets/**/*`, dest: cesiumBaseUrl, rename: { stripBase: 4 } },
        { src: `${cesiumSource}/Widgets/**/*`, dest: cesiumBaseUrl, rename: { stripBase: 4 } },
      ],
    }),
  ],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
  server: {
    host: '0.0.0.0',
    port: 50040,
    strictPort: true,
    proxy: {
      '/v1': backendTarget,
      '/api': backendTarget,
      '/health': backendTarget,
    },
  },
  preview: {
    host: '0.0.0.0',
    port: 50040,
    strictPort: true,
  },
  test: {
    include: ['tests/unit/**/*.spec.js'],
    environment: 'jsdom',
    setupFiles: ['./tests/unit/setup.js'],
    globals: true,
  },
});
