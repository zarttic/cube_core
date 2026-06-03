import { fileURLToPath, URL } from 'node:url';

import vue from '@vitejs/plugin-vue';
import { defineConfig } from 'vite';
import { viteStaticCopy } from 'vite-plugin-static-copy';

const cesiumSource = 'node_modules/cesium/Build/Cesium';
const cesiumBaseUrl = 'cesiumStatic';

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
