import { expect, test } from '@playwright/test';

const prefix = process.env.REAL_ACCEPTANCE_PREFIX;

test('real system exposes the complete production workflow without route mocks', async ({ page }) => {
  test.skip(!prefix, 'REAL_ACCEPTANCE_PREFIX is required');
  const consoleErrors = [];
  const failedResponses = [];
  page.on('console', (message) => {
    if (message.type() === 'error') consoleErrors.push(message.text());
  });
  page.on('response', (response) => {
    if (response.status() >= 400) failedResponses.push(`${response.status()} ${response.url()}`);
  });

  await page.goto('/partition');
  for (const [module, button, dataset] of [
    ['optical', /已载入光学遥感数据/, 'optical-standard'],
    ['carbon', /已载入碳卫星数据/, 'carbon-standard'],
    ['radar', /已载入雷达遥感数据/, 'radar-standard'],
    ['product', /已载入信息产品数据/, 'product-standard'],
  ]) {
    await page.getByTestId(`partition-module-${module}`).click();
    await page.getByRole('button', { name: button }).click();
    await page.getByTestId(`load-batch-${prefix}-batch`).click();
    await expect(page.getByRole('dialog')).toContainText(`${prefix}-${dataset}`);
    await page.getByRole('dialog').locator('.el-drawer__close-btn').click();
  }

  await page.goto('/data-management');
  const datasetAction = page.getByTestId(`dataset-row-${prefix}-optical-standard`);
  await expect(datasetAction.locator('xpath=ancestor::tr')).toContainText('已完成');
  await datasetAction.click();
  await expect(page.getByTestId('dataset-detail-drawer')).toContainText(`${prefix}-optical-standard`);
  await page.getByTestId('dataset-detail-close').click();

  await page.goto('/quality');
  await page.getByRole('textbox', { name: '数据集 ID' }).fill(`${prefix}-quality-fail-product`);
  await page.getByRole('button', { name: '查询' }).click();
  const failedRow = page.locator(`[data-testid^="quality-row-"]`).filter({ hasText: `${prefix}-quality-fail-product` }).first();
  await failedRow.click();
  await expect(page.getByTestId('quality-detail-drawer')).toContainText('失败');
  const download = page.waitForEvent('download');
  await page.getByTestId('quality-detail-drawer').getByTestId('quality-export-all').click();
  expect((await download).suggestedFilename()).toMatch(/\.csv$/);

  await page.goto('/partition');
  await page.getByTestId('partition-module-ingest').click();
  await expect(page.locator('body')).toContainText(`${prefix}-carbon-standard`);
  await expect(page.getByTestId('partition-module-tasks')).toHaveCount(0);

  expect(consoleErrors).toEqual([]);
  expect(failedResponses).toEqual([]);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth + 1)).toBe(true);
});
