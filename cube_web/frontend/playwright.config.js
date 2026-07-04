import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests/e2e',
  timeout: 180000,
  expect: {
    timeout: 15000,
  },
  fullyParallel: false,
  workers: 1,
  reporter: 'list',
  use: {
    baseURL: 'http://127.0.0.1:50040',
    headless: true,
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'off',
  },
  webServer: [
    {
      command: 'PYTHONPATH=../cube_encoder:../cube_split:. python3.11 -m uvicorn cube_web.app:app --host 127.0.0.1 --port 50039',
      port: 50039,
      reuseExistingServer: false,
      cwd: '..',
      timeout: 120000,
      env: {
        CUBE_WEB_AUTH_REQUIRED: '0',
      },
    },
    {
      command: 'npm run dev -- --host 127.0.0.1 --port 50040',
      port: 50040,
      reuseExistingServer: false,
      cwd: '.',
      timeout: 120000,
      env: {
        VITE_DEV_API_TARGET: 'http://127.0.0.1:50039',
        VITE_AUTH_REQUIRED: '0',
      },
    },
  ],
});
