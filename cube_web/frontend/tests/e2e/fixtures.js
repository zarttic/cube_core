export const datasetListFixture = {
  items: [
    { dataset_id: 'dataset-a', dataset_code: 'DS-A', dataset_title: 'Dataset A', batch_id: 'batch-a', data_type: 'optical', product_type: 'L2A', grid_type: 'geohash', requested_grid_level: 6, partition_status: 'completed', quality_status: 'pass', publish_status: 'active' },
    { dataset_id: 'dataset-b', dataset_code: 'DS-B', dataset_title: 'Dataset B', batch_id: 'batch-b', data_type: 'optical', product_type: 'L2A', grid_type: 'isea4h', requested_grid_level: 6, partition_status: 'completed', quality_status: 'warn', publish_status: 'unpublished' },
  ], total: 2, page: 1, page_size: 20,
};

export const datasetDetailFixtureA = { ...datasetListFixture.items[0], current_output_version: 'v-a', grid_level_display_name: 'Geohash precision 6' };
export const datasetDetailFixtureB = { ...datasetListFixture.items[1], current_output_version: 'v-b', grid_level_display_name: 'ISEA4H resolution 6' };
export const datasetAssetsFixtureA = { items: [{ source_asset_id: 'asset-a', cog_uri: 's3://cube/source/a.tif', checksum: 'a'.repeat(64), crs: 'EPSG:4326' }], total: 1, page: 1, page_size: 20 };
export const datasetGridFixtureA = { items: [{ output_id: 'wx4g0e', space_code: 'wx4g0e' }], total: 1, page: 1, page_size: 20 };
const emptyPage = { items: [], total: 0, page: 1, page_size: 20 };
export const loadBatchFixture = {
  load_batches: [{ load_batch_id: 'load-batch-a', batch_name: 'Batch A', status: 'succeeded', scene_count: 2, dataset_count: 1 }],
};
export const loadBatchScenesFixture = {
  load_batch: loadBatchFixture.load_batches[0],
  scene_count: 2,
  datasets: [{
    dataset_id: 'dataset-a', dataset_code: 'DS-A', dataset_title: 'Dataset A', data_type: 'optical', product_type: 'L2A',
    scenes: [
      { scene_id: 'scene-a', scene_key: 'Scene A', dataset_id: 'dataset-a', load_batch_id: 'load-batch-a', load_status: 'succeeded', source_asset_id: 'asset-a', bbox: [100, 20, 101, 21], crs: 'EPSG:4326', bands: [{ band_code: 'B04', band_name: '红光', band_type: 'spectral' }] },
      { scene_id: 'scene-b', scene_key: 'Scene B', dataset_id: 'dataset-a', load_batch_id: 'load-batch-a', load_status: 'succeeded', source_asset_id: 'asset-b', bbox: [101, 20, 102, 21], crs: 'EPSG:4326', bands: [{ band_code: 'B08', band_name: '近红外', band_type: 'spectral' }] },
    ],
  }],
};
export const ingestRunsFixture = {
  items: [{ ingest_run_id: 'ingest-run-a', partition_run_id: 'partition-run-a', dataset_id: 'dataset-a', dataset_code: 'DS-A', status: 'completed', scene_count: 1, completed_scene_count: 1, failed_scene_count: 0 }],
  total: 1,
  page: 1,
  page_size: 20,
  summary: { run_count: 1, scene_count: 1, completed_scene_count: 1, failed_scene_count: 0 },
};

export const qualityRecordsFixture = {
  items: [
    { quality_run_id: 'quality-run-a', dataset_id: 'dataset-a', dataset_code: 'DS-A', batch_id: 'batch-a', data_type: 'optical', product_type: 'L2A', partition_status: 'completed', output_version: 'v-a', status: 'pass', results_complete: true, quality_sequence: 1, is_current: true, error_count: 0, warning_count: 0, trigger: 'automatic', completed_at: '2026-07-17T10:00:00+08:00' },
    { quality_run_id: 'quality-run-b', dataset_id: 'dataset-b', dataset_code: 'DS-B', batch_id: 'batch-b', data_type: 'optical', product_type: 'L2A', partition_status: 'completed', output_version: 'v-b', status: 'warn', results_complete: true, quality_sequence: 1, is_current: true, error_count: 1, warning_count: 1, trigger: 'automatic', completed_at: '2026-07-17T10:05:00+08:00' },
  ], total: 2, page: 1, page_size: 20,
};
export const qualityDetailFixtureA = { ...qualityRecordsFixture.items[0], rule_set_version: 'rules-v1' };
export const qualityDetailFixtureB = { ...qualityRecordsFixture.items[1], rule_set_version: 'rules-v1' };
export const qualityResultsFixtureA = { items: [{ rule_code: 'asset_readability', status: 'pass', finding_count: 1, error_count: 0, warning_count: 0 }], total: 1, page: 1, page_size: 20 };
export const qualityErrorsFixtureA = { items: [{ quality_error_id: 'error-a', rule_code: 'asset_readability', error_code: 'metadata_warning', scene_id: 'scene-a', scene_name: 'LC08_L1TP_120029_20240622', source_asset_id: 'asset-a', band_code: 'bqa', band_name: '质量评估', field: 'metadata', message: 'Fixture warning' }], total: 1, page: 1, page_size: 20 };
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
  await page.route('**/v1/datasets?**', (route) => json(route, { ...datasetListFixture, summary: { dataset_count: 2, scene_count: 2, ready_scene_count: 2, failed_scene_count: 0 } }));
  await page.route('**/v1/datasets/dataset-a', (route) => json(route, datasetDetailFixtureA));
  await page.route('**/v1/datasets/dataset-b', (route) => json(route, datasetDetailFixtureB));
  for (const id of ['dataset-a', 'dataset-b']) {
    for (const detail of ['scenes', 'assets', 'bands', 'outputs', 'grid', 'tiles', 'indexes', 'ingest-records', 'quality', 'publications', 'provenance']) {
      await page.route(`**/v1/datasets/${id}/${detail}?**`, (route) => json(route, id === 'dataset-a' && detail === 'assets' ? datasetAssetsFixtureA : id === 'dataset-a' && detail === 'grid' ? datasetGridFixtureA : emptyPage));
    }
  }
  await page.route('**/v1/partition/datasets?**', (route) => json(route, datasetListFixture));
  await page.route('**/v1/partition/datasets/dataset-a', (route) => json(route, datasetDetailFixtureA));
  await page.route('**/v1/partition/datasets/dataset-b', (route) => json(route, datasetDetailFixtureB));
  for (const id of ['dataset-a', 'dataset-b']) {
    for (const detail of ['assets', 'bands', 'tiles', 'indexes', 'grid', 'quality', 'publications']) {
      await page.route(`**/v1/partition/datasets/${id}/${detail}?**`, (route) => json(route, id === 'dataset-a' && detail === 'assets' ? datasetAssetsFixtureA : id === 'dataset-a' && detail === 'grid' ? datasetGridFixtureA : emptyPage));
    }
  }
  await page.route('**/v1/partition/runs', async (route) => {
    const body = route.request().postDataJSON();
    const valid = body?.partition_run_id?.startsWith('partition-run-') && Array.isArray(body.source_batch_ids)
      && body.source_batch_ids.includes('load-batch-a') && !body.source_batch_ids.includes(body.partition_run_id)
      && Array.isArray(body.datasets) && body.datasets[0]?.scene_ids?.length
      && body.datasets.every((dataset) => dataset.partition?.grid_type && dataset.partition?.requested_grid_level !== undefined && dataset.partition?.partition_method)
      && !Object.hasOwn(body, 'batch_id');
    await json(route, valid ? { partition_run_id: body.partition_run_id, source_batch_ids: body.source_batch_ids, task_id: 'task-fixture', status: 'queued', data_type: 'optical', operation: 'run' } : { detail: 'invalid request' });
  });
  await page.route('**/v1/partition/load-batches/load-batch-a/scenes?**', (route) => json(route, loadBatchScenesFixture));
  await page.route('**/v1/partition/load-batches?**', (route) => json(route, loadBatchFixture));
  await page.route('**/v1/partition/tasks?**', (route) => json(route, { tasks: [], total: 0, page: 1, page_size: 20 }));
  await page.route('**/v1/ingest-runs?**', (route) => json(route, ingestRunsFixture));
  await page.route('**/v1/quality/records?**', (route) => json(route, qualityRecordsFixture));
  await page.route('**/v1/quality/rules', (route) => json(route, {
    rule_set_version: '2026.07.14-v1',
    items: [
      { code: 'asset_readability', name: '数据单元可读性', mandatory: true, applicability: { data_types: ['optical', 'radar', 'product', 'carbon'] }, implementation_version: '1.0.0' },
    ],
  }));
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
