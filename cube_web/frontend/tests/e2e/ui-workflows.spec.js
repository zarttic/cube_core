import { expect, test } from '@playwright/test';

import {
  datasetAssetsFixtureA,
  datasetDetailFixtureA,
  datasetDetailFixtureB,
  datasetGridFixtureA,
  datasetListFixture,
  installApiRoutes,
  qualityDetailFixtureA,
  qualityDetailFixtureB,
  qualityErrorsFixtureA,
  qualityExportCsvFixture,
  qualityExportJsonFixture,
  qualityRecordsFixture,
  qualityResultsFixtureA,
} from './fixtures.js';

const ACCEPTANCE_CONTRACT = {
  datasetA: 'dataset-a',
  datasetATitle: 'Dataset A',
  datasetB: 'dataset-b',
  datasetBTitle: 'Dataset B',
  qualityRunA: 'quality-run-a',
  qualityRunB: 'quality-run-b',
  datasetRouteA: '**/v1/partition/datasets/dataset-a',
  datasetRouteB: '**/v1/partition/datasets/dataset-b',
  qualityRouteA: '**/v1/quality/records/quality-run-a',
  qualityRouteB: '**/v1/quality/records/quality-run-b',
  exportBase: '/v1/quality/records/quality-run-a/errors/export',
  exportRoute: '**/v1/quality/records/quality-run-a/errors/export*',
};

test('UI workflows keep the dataset and quality fixture contract', () => {
  expect(datasetListFixture.items.map((item) => item.dataset_id)).toEqual([ACCEPTANCE_CONTRACT.datasetA, ACCEPTANCE_CONTRACT.datasetB]);
  expect(datasetDetailFixtureA.dataset_title).toBe(ACCEPTANCE_CONTRACT.datasetATitle);
  expect(datasetDetailFixtureB.dataset_title).toBe(ACCEPTANCE_CONTRACT.datasetBTitle);
  expect(datasetAssetsFixtureA.items[0].source_asset_id).toBe('asset-a');
  expect(datasetGridFixtureA.items[0].space_code).toBe('wx4g0e');
  expect(qualityRecordsFixture.items.map((item) => item.quality_run_id)).toEqual([ACCEPTANCE_CONTRACT.qualityRunA, ACCEPTANCE_CONTRACT.qualityRunB]);
  expect(qualityDetailFixtureA.quality_run_id).toBe(ACCEPTANCE_CONTRACT.qualityRunA);
  expect(qualityDetailFixtureB.quality_run_id).toBe(ACCEPTANCE_CONTRACT.qualityRunB);
  expect(qualityResultsFixtureA.items[0].rule_code).toBe('asset_readability');
  expect(qualityErrorsFixtureA.items[0].rule_code).toBe('asset_readability');
  expect(qualityExportCsvFixture).toMatchObject({ contentType: 'text/csv', filename: 'dataset-a_quality-run-a_errors.csv' });
  expect(qualityExportJsonFixture).toMatchObject({ contentType: 'application/json', filename: 'dataset-a_quality-run-a_errors.json' });
  expect(ACCEPTANCE_CONTRACT).toMatchObject({
    datasetRouteA: '**/v1/partition/datasets/dataset-a',
    datasetRouteB: '**/v1/partition/datasets/dataset-b',
    qualityRouteA: '**/v1/quality/records/quality-run-a',
    qualityRouteB: '**/v1/quality/records/quality-run-b',
    exportBase: '/v1/quality/records/quality-run-a/errors/export',
    exportRoute: '**/v1/quality/records/quality-run-a/errors/export*',
  });
});

test('UI workflows discard the delayed quality detail response', async ({ page }) => {
  test.setTimeout(15_000);
  const { releaseQualityRunA } = await installApiRoutes(page, { deferQualityRunA: true });

  await page.goto('/quality');
  await page.getByTestId('quality-row-quality-run-a').click();
  await page.getByTestId('quality-row-quality-run-b').click();
  await expect(page.getByTestId('quality-detail-drawer')).toContainText(ACCEPTANCE_CONTRACT.qualityRunB);
  await releaseQualityRunA();
  await expect(page.getByTestId('quality-detail-drawer')).toContainText(ACCEPTANCE_CONTRACT.qualityRunB);
  await expect(page.getByTestId('quality-detail-drawer')).not.toContainText(ACCEPTANCE_CONTRACT.qualityRunA);
});

test('UI workflows check grid options, drawer reset, and complete quality exports', async ({ page }) => {
  test.setTimeout(30_000);
  const exports = [];
  await installApiRoutes(page);
  page.on('request', (request) => {
    if (request.url().includes(ACCEPTANCE_CONTRACT.exportBase)) exports.push(request);
  });

  await page.goto('/partition');
  await page.getByRole('button', { name: /已载入光学遥感数据/ }).click();
  const datasetDrawer = page.getByRole('dialog', { name: '光学遥感待剖分数据队列' });
  await datasetDrawer.getByText('Batch A', { exact: true }).click();
  await datasetDrawer.getByText('Scene A').click();
  await datasetDrawer.getByTestId('dataset-grid-dataset-a').click();
  const gridOptions = page.locator('.el-select-dropdown:visible .el-select-dropdown__item');
  await expect(gridOptions).toHaveText(['经纬度格网', '平面格网', '六边形格网']);
  await expect(datasetDrawer.getByTestId('dataset-grid-level-dataset-a').getByRole('combobox')).toBeDisabled();

  await page.goto('/data-management');
  await page.getByTestId('dataset-row-dataset-a').click();
  await expect(page.getByTestId('dataset-detail-drawer')).toContainText(ACCEPTANCE_CONTRACT.datasetATitle);
  await page.getByTestId('dataset-detail-tab-grid').click();
  await expect(page.getByTestId('dataset-detail-drawer')).toContainText('wx4g0e');
  await page.getByTestId('dataset-detail-close').click();
  await page.getByTestId('dataset-row-dataset-b').click();
  await expect(page.getByTestId('dataset-detail-drawer')).toContainText(ACCEPTANCE_CONTRACT.datasetBTitle);
  await expect(page.getByTestId('dataset-detail-drawer')).not.toContainText(ACCEPTANCE_CONTRACT.datasetATitle);
  await page.getByTestId('dataset-detail-close').click();

  await page.goto('/quality');
  await page.getByTestId('quality-row-quality-run-a').click();
  await page.getByTestId('quality-detail-tab-results').click();
  await expect(page.getByTestId('quality-detail-drawer')).toContainText('asset_readability');
  await page.getByTestId('quality-detail-tab-errors').click();
  await expect(page.getByTestId('quality-detail-drawer')).toContainText('Fixture warning');
  await expect(page.locator('.quality-location-tree')).toContainText('LC08_L1TP_120029_20240622');
  await expect(page.locator('.quality-location-tree')).toContainText('bqa · 质量评估');

  const csvDownload = page.waitForEvent('download');
  await page.getByTestId('quality-export-all').click();
  const csv = await csvDownload;
  await expect.poll(() => exports.length).toBe(1);
  expect(csv.suggestedFilename()).toBe(qualityExportCsvFixture.filename);
  expect(exports[0].url()).toContain('format=csv');
  expect(exports[0].url()).not.toMatch(/rule_code=|(?:\?|&)page=|(?:\?|&)page_size=/);

  await page.locator('.error-filters .el-select').click();
  const ruleOptions = page.locator('.el-select-dropdown:visible .el-select-dropdown__item');
  await expect(ruleOptions).toHaveText(['asset_readability']);
  await ruleOptions.click();
  const filteredCsvDownload = page.waitForEvent('download');
  await page.getByTestId('quality-export-filtered').click();
  const filteredCsv = await filteredCsvDownload;
  await expect.poll(() => exports.length).toBe(2);
  expect(filteredCsv.suggestedFilename()).toBe(qualityExportCsvFixture.filename);
  expect(exports[1].url()).toContain('rule_code=asset_readability');
  expect(exports[1].url()).not.toMatch(/(?:\?|&)page=|(?:\?|&)page_size=/);

  const jsonDownload = page.waitForEvent('download');
  await page.getByRole('button', { name: '导出全部 JSON' }).click();
  const json = await jsonDownload;
  await expect.poll(() => exports.length).toBe(3);
  expect(json.suggestedFilename()).toBe(qualityExportJsonFixture.filename);
  expect(exports[2].url()).toContain('format=json');
  expect(exports[2].url()).not.toMatch(/rule_code=|(?:\?|&)page=|(?:\?|&)page_size=/);

  const filteredJsonDownload = page.waitForEvent('download');
  await page.getByRole('button', { name: '导出当前筛选结果 JSON' }).click();
  const filteredJson = await filteredJsonDownload;
  await expect.poll(() => exports.length).toBe(4);
  expect(filteredJson.suggestedFilename()).toBe(qualityExportJsonFixture.filename);
  expect(exports[3].url()).toContain('format=json');
  expect(exports[3].url()).toContain('rule_code=asset_readability');
  expect(exports[3].url()).not.toMatch(/(?:\?|&)page=|(?:\?|&)page_size=/);
});
