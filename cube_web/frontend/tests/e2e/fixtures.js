export const datasetListFixture = {
  items: [
    { dataset_id: 'dataset-a', dataset_code: 'DS-A', dataset_title: 'Dataset A', batch_id: 'batch-a', data_type: 'optical', product_type: 'L2A', grid_type: 'geohash', requested_grid_level: 6, partition_status: 'completed', quality_status: 'pass', publish_status: 'active' },
    { dataset_id: 'dataset-b', dataset_code: 'DS-B', dataset_title: 'Dataset B', batch_id: 'batch-b', data_type: 'optical', product_type: 'L2A', grid_type: 'isea4h', requested_grid_level: 6, partition_status: 'completed', quality_status: 'warn', publish_status: 'unpublished' },
  ], total: 2, page: 1, page_size: 20,
};

export const datasetDetailFixtureA = { ...datasetListFixture.items[0], current_output_version: 'v-a', grid_level_display_name: 'Geohash precision 6' };
export const datasetDetailFixtureB = { ...datasetListFixture.items[1], current_output_version: 'v-b', grid_level_display_name: 'ISEA4H resolution 6' };
export const datasetAssetsFixtureA = { items: [{ source_asset_id: 'asset-a', cog_uri: 's3://cube/source/a.tif', checksum: 'a'.repeat(64), crs: 'EPSG:4326' }], total: 1, page: 1, page_size: 20 };
export const datasetGridFixtureA = { items: [{ output_id: 'cell-a', space_code: 'wx4g0e' }], total: 1, page: 1, page_size: 20 };
const emptyPage = { items: [], total: 0, page: 1, page_size: 20 };

export const qualityRecordsFixture = {
  items: [
    { quality_run_id: 'quality-run-a', dataset_id: 'dataset-a', dataset_code: 'DS-A', output_version: 'v-a', status: 'pass', results_complete: true, quality_sequence: 1, is_current: true, error_count: 0, warning_count: 0, trigger: 'automatic' },
    { quality_run_id: 'quality-run-b', dataset_id: 'dataset-b', dataset_code: 'DS-B', output_version: 'v-b', status: 'warn', results_complete: true, quality_sequence: 1, is_current: true, error_count: 1, warning_count: 1, trigger: 'automatic' },
  ], total: 2, page: 1, page_size: 20,
};
export const qualityDetailFixtureA = { ...qualityRecordsFixture.items[0], rule_set_version: 'rules-v1' };
export const qualityDetailFixtureB = { ...qualityRecordsFixture.items[1], rule_set_version: 'rules-v1' };
export const qualityResultsFixtureA = { items: [{ rule_code: 'asset_readability', status: 'pass', finding_count: 1, error_count: 0, warning_count: 0 }], total: 1, page: 1, page_size: 20 };
export const qualityErrorsFixtureA = { items: [{ quality_error_id: 'error-a', rule_code: 'asset_readability', error_code: 'metadata_warning', field: 'metadata', message: 'Fixture warning' }], total: 1, page: 1, page_size: 20 };
export const qualityExportCsvFixture = { contentType: 'text/csv', filename: 'dataset-a_quality-run-a_errors.csv', body: 'quality_error_id,message\nerror-a,Fixture warning\n' };
export const qualityExportJsonFixture = { contentType: 'application/json', filename: 'dataset-a_quality-run-a_errors.json', body: JSON.stringify(qualityErrorsFixtureA.items) };

export async function installApiRoutes(page, { deferQualityRunA = false } = {}) {
  let signalQualityRunA;
  let releaseQualityRunAResponse;
  const qualityRunARequested = new Promise((resolve) => { signalQualityRunA = resolve; });
  const qualityRunAResponse = new Promise((resolve) => { releaseQualityRunAResponse = resolve; });
  const json = (route, body) => route.fulfill({ contentType: 'application/json', body: JSON.stringify(body) });
  const datasetDetail = (id) => id === 'dataset-a' ? datasetDetailFixtureA : datasetDetailFixtureB;

  await page.route('**/api/config', (route) => json(route, { auth_required: false, navigation: [] }));
  await page.route('**/v1/partition/datasets?**', (route) => json(route, datasetListFixture));
  await page.route('**/v1/partition/datasets/dataset-a', (route) => json(route, datasetDetailFixtureA));
  await page.route('**/v1/partition/datasets/dataset-b', (route) => json(route, datasetDetailFixtureB));
  for (const id of ['dataset-a', 'dataset-b']) {
    for (const detail of ['assets', 'bands', 'tiles', 'indexes', 'grid', 'quality', 'publications']) {
      await page.route(`**/v1/partition/datasets/${id}/${detail}?**`, (route) => json(route, id === 'dataset-a' && detail === 'assets' ? datasetAssetsFixtureA : id === 'dataset-a' && detail === 'grid' ? datasetGridFixtureA : emptyPage));
    }
  }
  await page.route(/\/v1\/partition\/(?:tasks|(?:optical|radar|product|carbon)\/tasks)\/run$/, async (route) => {
    const body = route.request().postDataJSON();
    const valid = body?.batch_id && Array.isArray(body.datasets) && body.datasets[0]?.assets?.length && body.datasets[0]?.bands?.length
      && body.grid_type && body.requested_grid_level !== undefined && body.partition_method
      && body.datasets.every((dataset) => dataset.partition?.grid_type && dataset.partition?.requested_grid_level !== undefined && dataset.partition?.partition_method)
      && !Object.hasOwn(body, 'dataset_ids') && !Object.hasOwn(body, 'grid_level_mode');
    await json(route, valid ? { task_id: 'task-fixture', status: 'queued', tile_count: 0, index_count: 0, grid_cell_count: 0 } : { detail: 'invalid request' }, valid ? 202 : 400);
  });
  await page.route('**/v1/partition/batches?**', (route) => json(route, { batches: [] }));
  await page.route('**/v1/partition/tasks?**', (route) => json(route, { tasks: [], total: 0, page: 1, page_size: 20 }));
  await page.route('**/v1/quality/records?**', (route) => json(route, qualityRecordsFixture));
  await page.route('**/v1/quality/records/quality-run-a', async (route) => {
    if (deferQualityRunA) {
      signalQualityRunA();
      await qualityRunAResponse;
    }
    await json(route, qualityDetailFixtureA);
  });
  await page.route('**/v1/quality/records/quality-run-b', (route) => json(route, qualityDetailFixtureB));
  await page.route('**/v1/quality/records/quality-run-a/results?**', (route) => json(route, qualityResultsFixtureA));
  await page.route('**/v1/quality/records/quality-run-a/errors?**', (route) => json(route, qualityErrorsFixtureA));
  await page.route('**/v1/quality/records/quality-run-b/results?**', (route) => json(route, emptyPage));
  await page.route('**/v1/quality/records/quality-run-b/errors?**', (route) => json(route, emptyPage));
  await page.route('**/v1/quality/records/quality-run-a/errors/export*', async (route) => {
    const format = new URL(route.request().url()).searchParams.get('format');
    const fixture = format === 'json' ? qualityExportJsonFixture : qualityExportCsvFixture;
    await route.fulfill({ contentType: fixture.contentType, headers: { 'Content-Disposition': `attachment; filename="${fixture.filename}"` }, body: fixture.body });
  });
  await page.route('**/v1/quality/runs', (route) => json(route, { quality_run_id: 'quality-run-new', status: 'queued' }));
  return {
    qualityRunARequested,
    async releaseQualityRunA() {
      releaseQualityRunAResponse();
    },
  };
}
