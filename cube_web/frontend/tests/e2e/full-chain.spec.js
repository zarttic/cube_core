// Complete headless-browser chain test.
//
// Covers every production data type (optical / radar / product / carbon) twice:
//   1. the partition page flow (select load batch -> select bands -> grid
//      preview -> submit partition run), and
//   2. the dataset-side grid re-partition flow (数据管理 -> 数据集详情 -> 选择
//      未剖分格网类型 -> 加入剖分批次 -> /partition 提交).
//
// All API responses are mocked from the MinIO-backed fixture inventory, and a
// catch-all route refuses any /v1 or /api request that is not explicitly
// mocked. That makes the suite side-effect free: no real backend, OpenGauss or
// MinIO data is created, so nothing is left behind after the run.

import { expect, test } from '@playwright/test';

import { derivedPartitionMethod, installFullChainRoutes } from './full-chain-fixtures.js';

const LOAD_BATCH_SPECS = [
  {
    key: 'optical', module: 'optical', label: '光学遥感', dataType: 'optical',
    batch: 'optical-batch', dataset: 'optical-standard', datasetTitle: '光学遥感标准验收数据集',
    gridType: 'geohash', gridTypeLabel: '经纬度格网', level: 4, switchGrid: false, carbon: false,
    bandUnitIds: ['band-optical-b02', 'band-optical-b03', 'band-optical-b04'],
    bandCount: 3, previewCells: 2,
  },
  {
    key: 'radar', module: 'radar', label: '雷达遥感', dataType: 'radar',
    batch: 'radar-batch', dataset: 'radar-standard', datasetTitle: '雷达遥感标准验收数据集',
    gridType: 'mgrs', gridTypeLabel: '平面格网', level: 1, switchGrid: true, carbon: false,
    bandUnitIds: ['band-radar-vv', 'band-radar-vh'],
    bandCount: 2, previewCells: 2,
  },
  {
    key: 'product', module: 'product', label: '信息产品', dataType: 'product',
    batch: 'product-batch', dataset: 'product-standard', datasetTitle: '信息产品标准验收数据集',
    gridType: 'isea4h', gridTypeLabel: '六边形格网', level: 6, switchGrid: true, carbon: false,
    bandUnitIds: ['band-product-value-1', 'band-product-value-2'],
    bandCount: 2, previewCells: 4,
  },
  {
    key: 'carbon', module: 'carbon', label: '碳卫星', dataType: 'carbon',
    batch: 'carbon-batch', dataset: 'carbon-standard', datasetTitle: '碳卫星标准验收数据集',
    gridType: 'geohash', gridTypeLabel: '经纬度格网', level: 4, switchGrid: false, carbon: true,
    bandUnitIds: ['band-carbon-xco2'],
    bandCount: 1, previewCells: 2,
  },
];

const REPARTITION_SPECS = [
  {
    key: 'optical', module: 'optical', label: '光学遥感', dataType: 'optical',
    batch: 'optical-batch', dataset: 'optical-standard', datasetTitle: '光学遥感标准验收数据集',
    gridType: 'mgrs', gridTypeLabel: '平面格网', level: 0,
    partitionedBand: 'band-optical-b02',
    selectBandUnitIds: ['band-optical-b03', 'band-optical-b04'],
    expandSceneIds: ['optical-scene'],
    sceneIds: ['optical-scene'],
  },
  {
    key: 'radar', module: 'radar', label: '雷达遥感', dataType: 'radar',
    batch: 'radar-batch', dataset: 'radar-standard', datasetTitle: '雷达遥感标准验收数据集',
    gridType: 'isea4h', gridTypeLabel: '六边形格网', level: 11,
    partitionedBand: 'band-radar-vv',
    selectBandUnitIds: ['band-radar-vh'],
    expandSceneIds: ['radar-scene'],
    sceneIds: ['radar-scene'],
  },
  {
    key: 'product', module: 'product', label: '信息产品', dataType: 'product',
    batch: 'product-batch', dataset: 'product-standard', datasetTitle: '信息产品标准验收数据集',
    gridType: 'mgrs', gridTypeLabel: '平面格网', level: 0,
    partitionedBand: 'band-product-value-1',
    selectBandUnitIds: ['band-product-value-2'],
    expandSceneIds: ['product-scene-1', 'product-scene-2'],
    sceneIds: ['product-scene-2'],
  },
  {
    key: 'carbon', module: 'carbon', label: '碳卫星', dataType: 'carbon',
    batch: 'carbon-batch', dataset: 'carbon-standard', datasetTitle: '碳卫星标准验收数据集',
    gridType: 'mgrs', gridTypeLabel: '平面格网', level: 0,
    partitionedBand: null,
    selectBandUnitIds: ['band-carbon-xco2'],
    expandSceneIds: ['carbon-scene'],
    sceneIds: ['carbon-scene'],
  },
];

function assertRunPayload(body, { batchId, gridType, level, bandUnitIds, selectionId = null, sourceBatchId = null }) {
  expect(body).not.toHaveProperty('batch_id');
  expect(body.selection_source).toBe('load_batch');
  expect(body.partition_run_id).toMatch(/^partition-run-/);
  expect(Array.isArray(body.source_batch_ids)).toBe(true);
  expect(body.source_batch_ids).toContain(batchId);
  expect(body.datasets.length).toBe(1);
  const dataset = body.datasets[0];
  expect(dataset.partition).toMatchObject({
    grid_type: gridType,
    requested_grid_level: level,
    partition_method: derivedPartitionMethod(gridType),
  });
  expect(dataset.scene_ids.length).toBeGreaterThan(0);
  expect(dataset.band_unit_ids).toEqual(bandUnitIds);
  expect(dataset.grid_config_locked).toBeUndefined();
  if (selectionId) expect(dataset.selection_id).toBe(selectionId);
  if (sourceBatchId) expect(dataset.source_batch_id).toBe(sourceBatchId);
}

test('partition page walks the full run chain for all data types with MinIO-backed mocks', async ({ page }) => {
  test.setTimeout(300_000);
  const api = await installFullChainRoutes(page);
  await page.goto('/partition');

  for (const spec of LOAD_BATCH_SPECS) {
    await page.getByTestId(`partition-module-${spec.module}`).click();
    await page.getByRole('button', { name: new RegExp(`已载入${spec.label}数据`) }).click();
    const drawer = page.getByRole('dialog', { name: `${spec.label}待剖分数据队列` });
    await drawer.getByTestId(`load-batch-${spec.batch}`).click();

    // The batch tree starts collapsed; expand it before touching dataset controls.
    const batchTree = drawer.getByTestId(`batch-tree-${spec.batch}`);
    await expect(batchTree).toBeVisible();
    await batchTree.locator('.partition-batch-tree-header').click();
    await expect(drawer.getByTestId(`dataset-tree-${spec.batch}-${spec.dataset}`)).toBeVisible();
    // Select every selectable band of the dataset.
    await drawer.getByTestId(`select-dataset-${spec.dataset}`).click();

    if (spec.switchGrid) {
      await drawer.getByTestId(`dataset-grid-${spec.dataset}`).click();
      await page.locator('.el-select-dropdown:visible .el-select-dropdown__item', { hasText: spec.gridTypeLabel }).click();
    }
    await expect(drawer.locator('.partition-drawer-heading')).toContainText(`已选 ${spec.bandCount} 个波段`);
    await drawer.locator('.el-drawer__close-btn').click();

    if (spec.carbon) {
      await page.getByTestId('load-carbon-footprints').click();
      await expect(page.locator('.map-overlay-actions')).toContainText('1 个足迹');
    }
    await page.getByTestId('load-map').click();
    if (spec.carbon) {
      await expect(page.locator('.el-message__content', { hasText: '已加载 2 个格网单元和 1 个足迹。' }).last()).toBeVisible();
    } else {
      await expect(page.locator('.el-message__content', { hasText: `已加载 ${spec.previewCells} 个格网单元。` }).last()).toBeVisible();
    }

    const runRequest = page.waitForRequest((request) => (
      request.url().includes('/v1/partition/runs') && request.method() === 'POST'
    ));
    await page.getByRole('button', { name: '提交剖分' }).click();
    const runBody = (await runRequest).postDataJSON();
    assertRunPayload(runBody, {
      batchId: spec.batch,
      gridType: spec.gridType,
      level: spec.level,
      bandUnitIds: spec.bandUnitIds,
      selectionId: `${spec.batch}:${spec.dataset}`,
      sourceBatchId: spec.batch,
    });
    await expect(page.locator('.el-message__content', { hasText: '剖分任务已提交。' }).last()).toBeVisible();
  }

  expect(api.state.tasks.map((task) => task.data_type)).toEqual(['optical', 'radar', 'product', 'carbon']);
  expect(api.unmocked).toEqual([]);
});

test('dataset-side grid re-partition covers all data types and submits through the partition chain', async ({ page }) => {
  test.setTimeout(300_000);
  const api = await installFullChainRoutes(page);

  for (const spec of REPARTITION_SPECS) {
    await page.goto('/data-management');
    await page.getByTestId(`dataset-row-${spec.dataset}`).click();
    const detailDrawer = page.getByTestId('dataset-detail-drawer');
    await expect(detailDrawer).toContainText(spec.datasetTitle);
    await page.getByTestId('dataset-detail-tab-scenes').click();

    // Pick the unpartitioned grid type in the repartition toolbar.
    await detailDrawer.locator('.repartition-controls label').first().locator('.el-select').click();
    await page.locator('.el-select-dropdown:visible .el-select-dropdown__item', { hasText: spec.gridTypeLabel }).click();

    for (const sceneId of spec.expandSceneIds) {
      await detailDrawer.getByTestId(`managed-scene-${sceneId}`).locator('.scene-toggle').click();
    }
    if (spec.partitionedBand) {
      await expect(detailDrawer.getByTestId(`managed-band-${spec.partitionedBand}`).locator('.el-checkbox')).toBeDisabled();
    }
    for (const bandUnitId of spec.selectBandUnitIds) {
      await detailDrawer.getByTestId(`managed-band-${bandUnitId}`).locator('.el-checkbox').click();
    }

    const reloadRequest = page.waitForRequest((request) => (
      request.url().includes('/v1/partition/reload-batches') && request.method() === 'POST'
    ));
    const reloadResponse = page.waitForResponse((response) => (
      response.url().includes('/v1/partition/reload-batches') && response.request().method() === 'POST'
    ));
    await page.getByRole('button', { name: '加入剖分批次' }).click();
    await page.getByRole('dialog', { name: '确认加入剖分批次' }).getByRole('button', { name: '确认加入' }).click();

    const reloadBody = (await reloadRequest).postDataJSON();
    expect(reloadBody.data_type).toBe(spec.dataType);
    expect(reloadBody.datasets.length).toBe(1);
    const reloadSelection = reloadBody.datasets[0];
    expect(reloadSelection.dataset_id).toBe(spec.dataset);
    expect(reloadSelection.grid_config_locked).toBe(true);
    expect(reloadSelection.partition).toMatchObject({
      grid_type: spec.gridType,
      requested_grid_level: spec.level,
      partition_method: derivedPartitionMethod(spec.gridType),
    });
    expect([...reloadSelection.band_unit_ids].sort()).toEqual([...spec.selectBandUnitIds].sort());
    expect(reloadSelection.scenes.map((scene) => scene.scene_id)).toEqual(spec.sceneIds);
    expect(reloadBody.source_batch_ids).toContain(spec.batch);

    const reloadResponseBody = await (await reloadResponse).json();
    const reloadBatchId = reloadResponseBody.load_batch_id;
    expect(reloadBatchId).toMatch(/^dataset-reload-/);
    const formalSelection = reloadResponseBody.selection.datasets[0];
    expect(formalSelection.selection_source).toBe('dataset_reload');
    expect(formalSelection.selection_id).toBe(`${reloadBatchId}:${spec.dataset}`);
    expect(formalSelection.source_batch_id).toBe(reloadBatchId);
    expect(formalSelection.scenes.every((scene) => scene.source_batch_ids.includes(reloadBatchId))).toBe(true);

    await expect(page).toHaveURL(/\/partition\?/);
    const partitionDrawer = page.getByRole('dialog', { name: `${spec.label}待剖分数据队列` });
    await expect(partitionDrawer).toBeVisible();
    await expect(partitionDrawer).toContainText(spec.datasetTitle);
    await expect(partitionDrawer.getByTestId(`dataset-grid-${spec.dataset}`).locator('input')).toBeDisabled();
    await partitionDrawer.locator('.el-drawer__close-btn').click();

    const runRequest = page.waitForRequest((request) => (
      request.url().includes('/v1/partition/runs') && request.method() === 'POST'
    ));
    await page.getByRole('button', { name: '提交剖分' }).click();
    const runBody = (await runRequest).postDataJSON();
    assertRunPayload(runBody, {
      batchId: reloadBatchId,
      gridType: spec.gridType,
      level: spec.level,
      bandUnitIds: spec.selectBandUnitIds,
      selectionId: `${reloadBatchId}:${spec.dataset}`,
      sourceBatchId: reloadBatchId,
    });
    await expect(page.locator('.el-message__content', { hasText: '剖分任务已提交。' }).last()).toBeVisible();
  }

  expect(api.state.tasks.map((task) => task.data_type)).toEqual(['optical', 'radar', 'product', 'carbon']);
  expect(api.state.requests.filter((request) => request.kind === 'reload')).toHaveLength(4);
  expect(api.unmocked).toEqual([]);
});
