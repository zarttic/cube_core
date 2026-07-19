import { expect, test } from '@playwright/test';

import { installApiRoutes, qualityExportCsvFixture } from './fixtures.js';

function callbackState(target = '/partition') {
  return Buffer.from(encodeURIComponent(JSON.stringify({ target }))).toString('base64');
}

test('cold auth callback exchanges the code before entering a protected route', async ({ page }) => {
  await installApiRoutes(page);
  const callbackRequests = [];
  const meRequests = [];
  const loginRequests = [];
  page.on('request', (request) => {
    const path = new URL(request.url()).pathname;
    if (path === '/api/callback') callbackRequests.push(request.url());
    if (path === '/api/me') meRequests.push(request.url());
    if (path === '/api/auth/login') loginRequests.push(request.url());
  });
  await page.route('**/api/config', (route) => route.fulfill({
    contentType: 'application/json',
    body: JSON.stringify({ auth_required: true, redirect_uri: '/callback', navigation: [] }),
  }));
  await page.route('**/api/callback?**', (route) => route.fulfill({
    contentType: 'application/json',
    body: JSON.stringify({ access_token: 'callback-token' }),
  }));
  await page.route('**/api/me', (route) => route.fulfill({
    contentType: 'application/json',
    body: JSON.stringify({ username: 'admin', role: '管理员' }),
  }));

  await page.goto(`/callback?code=one-time-code&state=${encodeURIComponent(callbackState())}`);

  await expect(page).toHaveURL(/\/partition$/);
  await expect(page.getByRole('heading', { name: '光学遥感空间预览' })).toBeVisible();
  expect(callbackRequests).toHaveLength(1);
  expect(meRequests).toHaveLength(1);
  expect(loginRequests).toHaveLength(0);
});

test('failed auth callback stops instead of starting another login redirect', async ({ page }) => {
  await installApiRoutes(page);
  const loginRequests = [];
  page.on('request', (request) => {
    if (new URL(request.url()).pathname === '/api/auth/login') loginRequests.push(request.url());
  });
  await page.route('**/api/config', (route) => route.fulfill({
    contentType: 'application/json',
    body: JSON.stringify({ auth_required: true, redirect_uri: '/callback', navigation: [] }),
  }));
  await page.route('**/api/callback?**', (route) => route.fulfill({
    status: 401,
    contentType: 'application/json',
    body: JSON.stringify({ detail: 'invalid authorization code' }),
  }));

  await page.goto(`/callback?code=expired-code&state=${encodeURIComponent(callbackState())}`);

  await expect(page.getByRole('alert')).toContainText('登录回调处理失败');
  expect(loginRequests).toHaveLength(0);
});

test('routed dataset drawer resets between records', async ({ page }) => {
  await installApiRoutes(page);
  await page.goto('/data-management');
  await page.getByTestId('dataset-row-dataset-a').click();
  await expect(page.getByTestId('dataset-detail-drawer')).toContainText('Dataset A');
  await page.getByTestId('dataset-detail-tab-grid').click();
  await expect(page.getByTestId('dataset-detail-drawer')).toContainText('wx4g0e');
  await page.getByTestId('dataset-detail-close').click();
  await page.getByTestId('dataset-row-dataset-b').click();
  await expect(page.getByTestId('dataset-detail-drawer')).toContainText('Dataset B');
  await expect(page.getByTestId('dataset-detail-drawer')).not.toContainText('Dataset A');
});

test('data management unifies datasets, load batches and ingest runs', async ({ page }) => {
  await installApiRoutes(page);
  await page.goto('/data-management');

  await expect(page.getByRole('heading', { name: '数据管理与入库' })).toBeVisible();
  await expect(page.getByRole('tab')).toHaveText(['数据集', '入库运行']);
  await expect(page.getByRole('link', { name: '数据集管理' })).toHaveCount(0);

  await page.getByRole('tab', { name: '入库运行' }).click();
  await expect(page.getByTestId('ingest-row-ingest-run-a')).toBeVisible();
});

test('partition dataset grid uses production options and keeps the recommended level locked', async ({ page }) => {
  await installApiRoutes(page);
  await page.goto('/partition');
  await page.getByRole('button', { name: /已载入光学遥感数据/ }).click();
  const datasetDrawer = page.getByRole('dialog', { name: '光学遥感待剖分数据队列' });
  await datasetDrawer.getByText('Batch A', { exact: true }).click();
  await datasetDrawer.getByText('Scene A').click();

  const datasetGrid = datasetDrawer.getByTestId('dataset-grid-dataset-a');
  await datasetGrid.click();
  const options = page.locator('.el-select-dropdown:visible .el-select-dropdown__item');
  await expect(options).toHaveText(['经纬度格网', '平面格网', '六边形格网']);
  await options.filter({ hasText: '六边形格网' }).click();
  const datasetGridLevel = datasetDrawer.getByTestId('dataset-grid-level-dataset-a').getByRole('combobox');
  await expect(datasetGridLevel).toBeDisabled();
  await datasetDrawer.getByTestId('unlock-grid-level-dataset-a').click();
  await expect(datasetGridLevel).toBeEnabled();

  await page.keyboard.press('Escape');
  await expect(page.getByTestId('partition-grid-type')).toHaveCount(0);
  await expect(page.getByText('覆盖方式', { exact: true })).toHaveCount(0);
  await expect(page.getByText('时间粒度', { exact: true })).toHaveCount(0);
  await expect(page.getByText('每数据单元最大格网单元数', { exact: true })).toHaveCount(0);
});

test('quality drawer discards stale detail response', async ({ page }) => {
  const { qualityRunARequested, releaseQualityRunA } = await installApiRoutes(page, { deferQualityRunA: true });
  await page.goto('/quality');
  await page.getByTestId('quality-row-quality-run-a').click();
  await Promise.race([qualityRunARequested, page.waitForTimeout(5_000).then(() => { throw new Error('quality-run-a detail request did not start'); })]);
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

test('quality record table exposes workflow fields, rule matrix and direct full export', async ({ page }) => {
  await installApiRoutes(page);
  await page.goto('/quality');

  await expect(page.getByRole('heading', { name: '全部质检记录' })).toBeVisible();
  for (const label of ['数据集', '批次', '产品类型', '剖分状态', '质检状态', '错误数', '质检时间', '操作']) {
    await expect(page.getByRole('columnheader', { name: label })).toBeVisible();
  }

  await page.getByRole('button', { name: '质检规则' }).click();
  await expect(page.getByRole('heading', { name: '质检规则' })).toBeVisible();
  await expect(page.getByText('数据单元可读性')).toBeVisible();
  await expect(page.getByText('必选', { exact: true })).toBeVisible();
  await expect(page.getByText('可选', { exact: true })).toBeVisible();
  await page.keyboard.press('Escape');

  const download = page.waitForEvent('download');
  await page.getByTestId('quality-export-row-quality-run-a').click();
  await expect((await download).suggestedFilename()).toBe(qualityExportCsvFixture.filename);
});

test('mobile partition controls remain individually reachable', async ({ page }) => {
  await installApiRoutes(page);
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/partition');
  const actionBar = page.locator('.config-panel .action-buttons');
  const reset = actionBar.getByRole('button', { name: '重置' });
  const submit = actionBar.getByRole('button', { name: '提交剖分' });
  await expect(reset).toBeVisible();
  await expect(submit).toBeVisible();
  const [resetBox, submitBox] = await Promise.all([reset.boundingBox(), submit.boundingBox()]);
  expect(Math.abs(resetBox.y - submitBox.y)).toBeLessThan(5);
  expect(resetBox.x + resetBox.width <= submitBox.x || submitBox.x + submitBox.width <= resetBox.x).toBe(true);
});

test('partition keeps one product per subpage and exposes quality and ingest pages', async ({ page }) => {
  await installApiRoutes(page);
  await page.goto('/partition');

  await expect(page.getByRole('heading', { name: '光学遥感空间预览' })).toBeVisible();
  await expect(page.locator('.module-nav .module-tab')).toHaveCount(7);
  await expect(page.locator('.partition-module-nav')).toHaveCount(0);
  await page.getByRole('button', { name: /已载入光学遥感数据/ }).click();
  const datasetDrawer = page.getByRole('dialog', { name: '光学遥感待剖分数据队列' });
  await expect(datasetDrawer).toBeVisible();
  await datasetDrawer.getByText('Batch A', { exact: true }).click();
  await expect(datasetDrawer.getByText('Dataset A')).toBeVisible();
  await expect(datasetDrawer.getByText('B04 · 红光')).toBeVisible();
  await datasetDrawer.getByText('Scene A').click();
  await expect(page.getByTestId('selected-load-batches')).toContainText('load-batch-a');
  await expect(page.getByText(/跨产品共/)).toHaveCount(0);
  await expect(page.locator('.form-group').filter({ hasText: '来源载入批次' }).locator('input[type="text"]')).toHaveCount(0);
  await page.keyboard.press('Escape');
  await expect(page.getByText('格网类型', { exact: true })).toHaveCount(0);
  await expect(page.getByText('格网层级', { exact: true })).toHaveCount(0);
  await expect(page.getByText('格网显示', { exact: true })).toHaveCount(0);

  for (const [module, heading] of [
    ['carbon', '碳卫星空间预览'],
    ['radar', '雷达遥感空间预览'],
    ['product', '信息产品空间预览'],
  ]) {
    await page.getByTestId('partition-module-' + module).click();
    await expect(page.getByRole('heading', { name: heading })).toBeVisible();
    if (module === 'carbon') {
      await expect(page.getByTestId('selected-load-batches')).toContainText('未关联批次');
      await expect(page.getByTestId('selected-load-batches')).not.toContainText('load-batch-a');
    }
  }

  await page.getByTestId('partition-module-quality').click();
  await expect(page.getByRole('heading', { name: '全部质检记录' })).toBeVisible();
  await expect(page.getByTestId('quality-row-quality-run-a')).toBeVisible();

  await page.getByTestId('partition-module-ingest').click();
  await expect(page.getByRole('heading', { name: '数据管理与入库' })).toBeVisible();
  await page.getByRole('tab', { name: '入库运行' }).click();
  await expect(page.getByTestId('ingest-row-ingest-run-a')).toBeVisible();

  await expect(page.getByTestId('partition-module-tasks')).toHaveCount(0);
});
