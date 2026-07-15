import { expect, test } from '@playwright/test';

import { installApiRoutes } from './fixtures.js';

test('routed dataset drawer resets between records', async ({ page }) => {
  await installApiRoutes(page);
  await page.goto('/datasets');
  await page.getByTestId('dataset-row-dataset-a').click();
  await expect(page.getByTestId('dataset-detail-drawer')).toContainText('Dataset A');
  await page.getByTestId('dataset-detail-tab-assets').click();
  await expect(page.getByTestId('dataset-detail-drawer')).toContainText('asset-a');
  await page.getByTestId('dataset-detail-tab-grid').click();
  await expect(page.getByTestId('dataset-detail-drawer')).toContainText('wx4g0e');
  await page.getByTestId('dataset-detail-close').click();
  await page.getByTestId('dataset-row-dataset-b').click();
  await expect(page.getByTestId('dataset-detail-drawer')).toContainText('Dataset B');
  await expect(page.getByTestId('dataset-detail-drawer')).not.toContainText('Dataset A');
});

test('partition grid uses teleported production options and derived method', async ({ page }) => {
  await installApiRoutes(page);
  await page.goto('/partition');
  await page.getByTestId('partition-grid-type').click();
  const options = page.locator('.el-select-dropdown:visible .el-select-dropdown__item');
  await expect(options).toHaveText(['Geohash', '扩展 MGRS', 'ISEA4H']);
  await options.filter({ hasText: 'ISEA4H' }).click();
  await expect(page.getByText('实体剖分', { exact: true })).toBeVisible();
});

test('quality drawer discards stale detail response', async ({ page }) => {
  const { qualityRunARequested, releaseQualityRunA } = await installApiRoutes(page, { deferQualityRunA: true });
  await page.goto('/quality');
  await page.getByTestId('quality-row-quality-run-a').click();
  await Promise.race([qualityRunARequested, page.waitForTimeout(5_000).then(() => { throw new Error('quality-run-a detail request did not start'); })]);
  await page.getByTestId('quality-detail-close').click();
  await page.getByTestId('quality-row-quality-run-b').click();
  await expect(page.getByTestId('quality-detail-drawer')).toContainText('quality-run-b');
  await releaseQualityRunA();
  await expect(page.getByTestId('quality-detail-drawer')).toContainText('quality-run-b');
  await expect(page.getByTestId('quality-detail-drawer')).not.toContainText('quality-run-a');
}, { timeout: 15_000 });

test('quality exports omit visible page parameters', async ({ page }) => {
  const requests = [];
  await installApiRoutes(page);
  page.on('request', (request) => {
    if (request.url().includes('/errors/export')) requests.push(request.url());
  });
  await page.goto('/quality');
  await page.getByTestId('quality-row-quality-run-a').click();
  await page.getByTestId('quality-detail-tab-errors').click();
  await page.getByTestId('quality-export-all').click();
  await expect.poll(() => requests.length).toBe(1);
  expect(requests[0]).toContain('format=csv');
  expect(requests[0]).not.toMatch(/rule_code=|page=|page_size=/);
  await page.locator('.error-filters .el-select').click();
  await page.locator('.el-select-dropdown:visible .el-select-dropdown__item').filter({ hasText: 'asset_readability' }).click();
  await page.getByTestId('quality-export-filtered').click();
  await expect.poll(() => requests.length).toBe(2);
  expect(requests[1]).toContain('rule_code=asset_readability');
  expect(requests[1]).not.toMatch(/(?:\?|&)page=|(?:\?|&)page_size=/);
});

test('mobile partition controls remain individually reachable', async ({ page }) => {
  await installApiRoutes(page);
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/partition');
  const reset = page.getByRole('button', { name: '重置' });
  const submit = page.getByRole('button', { name: '提交剖分' });
  await expect(reset).toBeVisible();
  await expect(submit).toBeVisible();
  const [resetBox, submitBox] = await Promise.all([reset.boundingBox(), submit.boundingBox()]);
  expect(resetBox.x + resetBox.width <= submitBox.x || submitBox.x + submitBox.width <= resetBox.x || resetBox.y + resetBox.height <= submitBox.y || submitBox.y + submitBox.height <= resetBox.y).toBe(true);
});
