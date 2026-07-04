import { expect, test } from '@playwright/test';

const runId = Date.now();
const apiBaseUrl = 'http://127.0.0.1:50039';

const opticalBatchId = `E2E_OPTICAL_${runId}`;
const opticalBatchName = `E2E optical ${runId}`;
const opticalResetBatchId = `E2E_OPTICAL_RESET_${runId}`;
const opticalResetBatchName = `E2E optical reset ${runId}`;
const opticalSourceUri = 's3://cube/cube/source/optocal/Shandong_mosaic_2015Q3_sr_band3_cut/Shandong_mosaic_2015Q3_sr_band3_cut.tif';
const opticalAsset = {
  asset_id: 'optical:Shandong_mosaic_2015Q3:sr_band3',
  source_uri: opticalSourceUri,
  scene_id: 'Shandong_mosaic_2015Q3',
  acq_time: '2015-07-01T00:00:00Z',
  bands: ['sr_band3'],
  band: 'sr_band3',
  resolution: 30,
  corners: [
    [114.75737732592705, 38.50352140009955],
    [122.77491413824946, 38.50352140009955],
    [122.77491413824946, 33.85704125649375],
    [114.75737732592705, 33.85704125649375],
  ],
  sensor: 'optical_mosaic',
  product_family: 'other',
};

const productOkSourceUri = 's3://cube/cube/smoke/all_partition_flows/20260630164638/sources/product/smoke_product_2026.tif';
const productMissingSourceUri = `s3://cube/cube/smoke/all_partition_flows/E2E_MANUAL_REQUIRED_${runId}/sources/product/missing-scene.tif`;
const productBatchId = `E2E_MANUAL_REQUIRED_${runId}`;
const productBatchName = `E2E manual required ${runId}`;

function productAsset(sourceUri, sceneId, assetId) {
  return {
    asset_id: assetId,
    source_uri: sourceUri,
    scene_id: sceneId,
    acq_time: '2026-01-01T00:00:00Z',
    bands: ['product_value'],
    band: 'product_value',
    corners: [[100.6, 25.2], [100.63, 25.2], [100.63, 25.17], [100.6, 25.17]],
    resolution: 30,
    sensor: 'data_product',
    product_family: 'product',
    product_name: 'smoke_product',
    product_year: 2026,
  };
}

async function importSchema(request, data) {
  const response = await request.post(`${apiBaseUrl}/v1/partition/schemas/import`, { data });
  expect(response.ok()).toBeTruthy();
}

async function importOpticalBatch(request) {
  await importSchema(request, {
    batch_id: opticalBatchId,
    batch_name: opticalBatchName,
    data_type: 'optical',
    assets: [opticalAsset],
    normalized_payload: {
      partition_method: 'logical',
      grid_type: 's2',
      grid_level: 2,
      grid_level_mode: 'manual',
      input_dir: '/tmp/cube_real_http/optical_input',
      batch_id: opticalBatchId,
      batch_name: opticalBatchName,
      selected_assets: [opticalAsset],
      partition_backend: 'ray',
      ray_parallelism: 2,
      chunk_size: 1,
      max_cells_per_asset: 50,
      dataset: `e2e_optical_${runId}`,
      asset_storage_backend: 'minio',
      metadata_backend: 'sqlite',
      db_path: `/tmp/${opticalBatchId}.db`,
    },
  });
  await importSchema(request, {
    batch_id: opticalResetBatchId,
    batch_name: opticalResetBatchName,
    data_type: 'optical',
    assets: [{ ...opticalAsset, asset_id: 'optical:Shandong_mosaic_2015Q3:sr_band3:reset' }],
    normalized_payload: {
      partition_method: 'logical',
      grid_type: 's2',
      grid_level: 2,
      grid_level_mode: 'manual',
      input_dir: '/tmp/cube_real_http/optical_input',
      batch_id: opticalResetBatchId,
      batch_name: opticalResetBatchName,
      selected_assets: [{ ...opticalAsset, asset_id: 'optical:Shandong_mosaic_2015Q3:sr_band3:reset' }],
      partition_backend: 'ray',
      ray_parallelism: 2,
      chunk_size: 1,
      max_cells_per_asset: 50,
      dataset: `e2e_optical_reset_${runId}`,
      asset_storage_backend: 'minio',
      metadata_backend: 'sqlite',
      db_path: `/tmp/${opticalResetBatchId}.db`,
    },
  });
}

async function importProductManualRequiredBatch(request) {
  const okAsset = productAsset(productOkSourceUri, 'ok-scene', 'ok-asset');
  const missingAsset = productAsset(productMissingSourceUri, 'missing-scene', 'missing-asset');
  await importSchema(request, {
    batch_id: productBatchId,
    batch_name: productBatchName,
    data_type: 'product',
    assets: [okAsset, missingAsset],
    normalized_payload: {
      partition_method: 'logical',
      grid_type: 's2',
      grid_level: 4,
      grid_level_mode: 'manual',
      batch_id: productBatchId,
      batch_name: productBatchName,
      selected_assets: [okAsset, missingAsset],
      partition_backend: 'ray',
      ray_parallelism: 2,
      chunk_size: 1,
      max_cells_per_asset: 8,
      target_crs: 'EPSG:4326',
      dataset: `e2e_product_${runId}`,
      asset_storage_backend: 'minio',
      metadata_backend: 'postgres',
    },
  });
}

async function openModule(page, name) {
  await page.goto('/partition');
  await expect(page.getByRole('button', { name })).toBeVisible();
  await page.getByRole('button', { name }).click();
}

async function openDataDrawer(page) {
  await page.getByRole('button', { name: '已载入数据' }).click();
  const drawer = page.locator('.el-drawer').filter({ hasText: '已载入' }).last();
  await expect(drawer).toBeVisible({ timeout: 20000 });
  return drawer;
}

async function openBatchDetailFromDataDrawer(page, batchName) {
  const drawer = await openDataDrawer(page);
  const batchCard = drawer.locator('.batch-card').filter({ hasText: batchName }).first();
  await expect(batchCard).toBeVisible({ timeout: 20000 });
  await batchCard.getByRole('button', { name: '详情' }).click();
  const detail = page.locator('.el-drawer').filter({ hasText: batchName }).last();
  await expect(detail.getByRole('heading', { name: batchName })).toBeVisible({ timeout: 20000 });
  return detail;
}

async function waitForToast(page, text, timeout = 60000) {
  await expect(page.locator('.el-message').filter({ hasText: text }).last()).toBeVisible({ timeout });
}

async function closeDrawer(page, drawer) {
  await page.keyboard.press('Escape');
  await expect(drawer).toBeHidden({ timeout: 20000 });
}

async function runProductBatchToManualRequired(page) {
  await openModule(page, '信息产品');
  const detail = await openBatchDetailFromDataDrawer(page, productBatchName);
  const summary = detail.locator('.partition-batch-detail-summary');
  const summaryText = (await summary.textContent()) || '';
  if (!summaryText.includes('人工确认')) {
    await detail.getByRole('button', { name: /开始执行|再次执行|继续重试/ }).click();
  }
  await expect(detail.locator('.partition-batch-detail-summary')).toContainText('人工确认', { timeout: 90000 });
  return detail;
}

test.describe('portal browser regression', () => {
  test.beforeAll(async ({ request }) => {
    await importOpticalBatch(request);
    await importProductManualRequiredBatch(request);
  });

  test.skip('encoding page covers division, encode, decode and topology flows', async ({ page }) => {
    await page.goto('/partition');
    await expect(page.getByText('分析就绪数据剖分管理系统')).toBeVisible();
    await page.getByRole('link', { name: '全球离散格网模型与编码' }).click();
    await expect(page.getByRole('button', { name: '格网划分' })).toBeVisible();

    await page.getByRole('button', { name: '执行演示' }).click();
    await expect(page.getByText('格网编码')).toBeVisible({ timeout: 20000 });

    await page.getByRole('button', { name: '时空编码' }).click();
    await page.getByRole('button', { name: '执行编码' }).click();
    await expect(page.getByText('完整时空编码')).toBeVisible({ timeout: 20000 });

    await page.locator('select.form-select').nth(0).selectOption('tile_matrix');
    await page.getByRole('button', { name: '执行编码' }).click();
    await expect(page.getByText('瓦片行列')).toBeVisible({ timeout: 20000 });

    await page.locator('select.form-select').nth(0).selectOption('isea4h');
    await page.getByRole('button', { name: '执行编码' }).click();
    await expect(page.getByText('完整时空编码')).toBeVisible({ timeout: 20000 });

    await page.locator('label.radio-label').filter({ hasText: '解码 (编码→坐标)' }).click();
    await page.getByPlaceholder('例如: tm:8:8/420/71:202603091530').fill('s2:6:35f1:24021214');
    await page.getByRole('button', { name: '执行编码' }).click();
    await expect(page.getByText('时间编码')).toBeVisible({ timeout: 20000 });

    await page.getByRole('button', { name: '元操作' }).click();
    await page.getByRole('button', { name: '执行拓扑运算' }).click();
    await expect(page.getByText('结果数量')).toBeVisible({ timeout: 20000 });

    await page.getByText('子级').click();
    await page.getByRole('button', { name: '执行拓扑运算' }).click();
    await expect(page.getByText('目标层级')).toBeVisible({ timeout: 20000 });

    await page.getByRole('button', { name: '执行坐标转换' }).click();
    await expect(page.getByText('转换方向')).toBeVisible({ timeout: 20000 });
  });

  test('optical batch run, detail tabs and active task drawer work end to end', async ({ page }) => {
    await openModule(page, '光学遥感');
    const detail = await openBatchDetailFromDataDrawer(page, opticalBatchName);

    await detail.getByRole('button', { name: /开始执行|再次执行/ }).click();
    await waitForToast(page, '批次执行完成');
    await expect(page.getByRole('button', { name: /运行状态 已完成/ })).toBeVisible({ timeout: 20000 });

    await detail.getByRole('button', { name: '资产' }).click();
    const assetRow = detail.locator('.partition-asset-table tbody tr').filter({ hasText: opticalSourceUri }).first();
    await expect(assetRow).toBeVisible();
    await expect(assetRow.getByText('Shandong_mosaic_2015Q3', { exact: true })).toBeVisible();

    await detail.getByRole('button', { name: '尝试历史' }).click();
    await expect(detail.locator('.partition-attempt-item')).toHaveCount(1, { timeout: 20000 });
    await expect(detail.getByText('自动执行')).toBeVisible();

    await detail.getByRole('button', { name: '概览' }).click();
    await expect(detail.getByText(`${opticalBatchId} · 光学遥感数据`)).toBeVisible();
  });

  test('product manual-required flow exposes retryable asset only and detail state resets across batches', async ({ page }) => {
    const manualDetail = await runProductBatchToManualRequired(page);
    await expect(manualDetail.locator('.partition-batch-detail-subtitle')).toContainText(productBatchId, { timeout: 20000 });

    await manualDetail.getByRole('button', { name: '资产' }).click();
    await expect(manualDetail.getByText(productOkSourceUri)).toBeVisible();
    await expect(manualDetail.getByText(productMissingSourceUri)).toBeVisible();

    const rows = manualDetail.locator('.partition-asset-table .el-table__body-wrapper tbody tr');
    await expect(rows).toHaveCount(2, { timeout: 20000 });
    await expect(manualDetail.getByText('可重试资产 1 条')).toBeVisible();

    await manualDetail.locator('.partition-asset-table tbody tr').filter({ hasText: productMissingSourceUri }).locator('label').first().click();
    await expect(manualDetail.getByText('已选 1 条')).toBeVisible();

    await closeDrawer(page, manualDetail);
    await openModule(page, '光学遥感');
    const opticalDetail = await openBatchDetailFromDataDrawer(page, opticalResetBatchName);
    await opticalDetail.getByRole('button', { name: '资产' }).click();
    await expect(opticalDetail.getByText('已选 0 条')).toBeVisible({ timeout: 20000 });
    await expect(opticalDetail.getByText('可重试资产 0 条')).toBeVisible();
  });

  test('quality workspace and history drawer respond with filters and manual queue detail', async ({ page }) => {
    const manualDetail = await runProductBatchToManualRequired(page);
    await closeDrawer(page, manualDetail);

    await openModule(page, '自动化质检');
    await expect(page.getByText('质检总览')).toBeVisible();
    await expect(page.getByText('人工处置队列')).toBeVisible();
    await page.getByRole('button', { name: '刷新队列' }).click();

    await page.locator('.config-panel .el-select').first().click();
    await page.getByText('数据产品').click();
    const manualCard = page.locator('.quality-manual-batch').filter({ hasText: productBatchId }).first();
    await expect(manualCard).toBeVisible({ timeout: 20000 });

    await manualCard.getByRole('button', { name: '详情' }).click();
    const detail = page.locator('.el-drawer').filter({ hasText: productBatchName }).last();
    await expect(detail.getByRole('heading', { name: productBatchName })).toBeVisible({ timeout: 20000 });
    await expect(detail.locator('.partition-batch-detail-summary')).toContainText('人工确认');

    await closeDrawer(page, detail);
    await page.getByRole('button', { name: /条记录/ }).click();
    const historyDrawer = page.locator('.el-drawer').filter({ hasText: '历史质检记录' }).last();
    await expect(historyDrawer.getByRole('heading', { name: '历史质检记录' })).toBeVisible({ timeout: 20000 });
    await historyDrawer.getByPlaceholder('按数据集、批次或路径筛选').fill('run_');
    await historyDrawer.locator('.quality-history-filterbar .el-select').click();
    await page.locator('.el-select-dropdown__item').filter({ hasText: '通过' }).last().click();
    await expect(historyDrawer.locator('.quality-history-table')).toBeVisible({ timeout: 20000 });
  });

  test('task queue page opens batch details and preserves actionable controls', async ({ page }) => {
    await runProductBatchToManualRequired(page);
    const initialDetail = page.locator('.el-drawer').filter({ hasText: productBatchName }).last();
    await closeDrawer(page, initialDetail);

    await openModule(page, '剖分任务队列');
    await expect(page.getByRole('heading', { name: '剖分任务队列' })).toBeVisible();
    await expect(page.locator('.partition-task-table')).toBeVisible();

    const manualTaskRow = page.locator('.partition-task-table tbody tr').filter({ hasText: productBatchName }).first();
    await expect(manualTaskRow).toBeVisible({ timeout: 20000 });
    await manualTaskRow.getByRole('button', { name: '详情' }).click();

    const taskDetail = page.locator('.el-drawer').filter({ hasText: productBatchName }).last();
    await expect(taskDetail.getByRole('heading', { name: productBatchName })).toBeVisible({ timeout: 20000 });
    await expect(taskDetail.getByRole('button', { name: /继续重试|重试批次|重新执行/ })).toBeVisible();
    await expect(taskDetail.getByRole('button', { name: '不再处理' })).toBeVisible();
  });
});
