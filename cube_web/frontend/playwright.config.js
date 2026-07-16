import { defineConfig } from '@playwright/test';

const devServerPort = Number(process.env.PLAYWRIGHT_DEV_SERVER_PORT || '50040');

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
      name: 'm4-e2e',
      testIgnore: /m5-acceptance\.spec\.js/,
    },
    {
      name: 'm5-acceptance',
      testMatch: /m5-acceptance\.spec\.js/,
    },
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
