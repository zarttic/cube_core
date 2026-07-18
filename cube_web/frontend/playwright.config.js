import { defineConfig } from '@playwright/test';

const devServerPort = Number(process.env.PLAYWRIGHT_DEV_SERVER_PORT || '50040');
const includeRealSystem = process.env.PLAYWRIGHT_REAL_SYSTEM === '1';

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
    baseURL: `http://127.0.0.1:${devServerPort}`,
    headless: true,
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'off',
  },
  projects: [
    {
      name: 'app-e2e',
      testIgnore: /(ui-workflows|real-system)\.spec\.js/,
    },
    {
      name: 'ui-workflows',
      testMatch: /ui-workflows\.spec\.js/,
    },
    ...(includeRealSystem ? [{ name: 'real-system', testMatch: /real-system\.spec\.js/ }] : []),
  ],
  webServer: [
    {
      command: `npm run dev -- --host 127.0.0.1 --port ${devServerPort}`,
      port: devServerPort,
      reuseExistingServer: false,
      cwd: '.',
      timeout: 120000,
      env: {
        VITE_AUTH_REQUIRED: '0',
      },
    },
  ],
});
