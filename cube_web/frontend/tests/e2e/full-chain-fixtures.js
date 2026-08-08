// Full-chain E2E fixtures.
//
// The dataset/scene/band layout mirrors the production acceptance manifest in
// cube_web/scripts/run_real_partition_acceptance.py, and every s3:// URI
// mirrors the real objects in the MinIO `cube` bucket:
//   optical  -> cube/source/optocal/Shandong_mosaic_2020Q3_sr_band{2,3,4}_cut/...
//   radar    -> cube/source/radar/yangzhou_s1_2018_2020/COG/... (COG form of the
//               real .dat/.hdr pairs prepared by the acceptance job)
//   product  -> cube/source/product/1980-2020年滇中地区30米生态安全评价数据集（第一版）_*.tif
//   carbon   -> cube/source/carbon/oco2_LtCO2_201231_B11014Ar_220729012824s(1).nc4
//
// Everything is mocked in the browser. The spec installs a catch-all route that
// refuses any /v1 or /api request not covered by an explicit mock, so no real
// backend, OpenGauss row, or MinIO object is ever touched and no test data can
// be left behind.

export const DATA_TYPES = ['optical', 'radar', 'product', 'carbon'];

const LEVEL_RANGES = Object.freeze({
  geohash: [1, 12],
  mgrs: [0, 5],
  isea4h: [0, 15],
});

export function derivedPartitionMethod(gridType) {
  return gridType === 'isea4h' ? 'entity' : 'logical';
}

const CHECKSUM = 'a'.repeat(64);
const EMPTY_PAGE = { items: [], total: 0, page: 1, page_size: 20 };

function partitionedAwaitingIngest(bandUnitId, gridType, gridLevel) {
  return {
    band_unit_id: bandUnitId,
    grid_type: gridType,
    grid_level: gridLevel,
    partition_status: 'completed',
    quality_status: 'pass',
    // Intentionally not ingest-completed: the partition-page batch drawer
    // treats any already-ingested band as consumed until a target grid is
    // selected, so the load-batch flow must see these bands as selectable.
    // The dataset re-partition drawer still blocks them for the grid type that
    // already has a completed partition.
    ingest_status: 'pending',
    output_version: `v-${bandUnitId}-${gridType}-${gridLevel}`,
    error_message: null,
    attempt_no: 1,
    updated_at: '2026-07-20T08:00:00+08:00',
  };
}

const DATASETS = [
  {
    dataset_id: 'optical-standard',
    dataset_code: 'OPT-2020Q3',
    dataset_title: '光学遥感标准验收数据集',
    data_type: 'optical',
    product_type: 'L2A',
    batch: 'optical-batch',
    batch_name: '光学 2020Q3 标准载入批次',
    crs: 'EPSG:4326',
    resolution_native: 10,
    resolution_unit: 'm',
    resolution_m: 10,
    suggested_grid_type: 'geohash',
    suggested_grid_levels: { geohash: 4, mgrs: 1, isea4h: 6 },
    time_start: '2020-07-10T00:00:00+08:00',
    time_end: '2020-07-10T23:59:59+08:00',
    scenes: [
      {
        scene_id: 'optical-scene',
        scene_key: 'GF1-EAST-20200710',
        acquisition_time: '2020-07-10T02:31:00+08:00',
        bbox: [114.75, 33.85, 122.77, 38.5],
        assets: [
          {
            asset_id: 'optical-asset',
            cog_uri: 's3://cube/cube/source/optocal/Shandong_mosaic_2020Q3_sr_band2_cut/Shandong_mosaic_2020Q3_sr_band2_cut.tif',
            bbox: [114.75, 33.85, 122.77, 38.5],
            crs: 'EPSG:4326',
          },
        ],
        bands: [
          {
            band_unit_id: 'band-optical-b02', asset_id: 'optical-asset', band_code: 'B02', band_name: '蓝光',
            band_type: 'spectral', unit: 'reflectance', display_order: 0,
            grid_statuses: [partitionedAwaitingIngest('band-optical-b02', 'geohash', 6), partitionedAwaitingIngest('band-optical-b02', 'mgrs', 1)],
          },
          {
            band_unit_id: 'band-optical-b03', asset_id: 'optical-asset', band_code: 'B03', band_name: '绿光',
            band_type: 'spectral', unit: 'reflectance', display_order: 1,
            grid_statuses: [partitionedAwaitingIngest('band-optical-b03', 'geohash', 6)],
          },
          {
            band_unit_id: 'band-optical-b04', asset_id: 'optical-asset', band_code: 'B04', band_name: '红光',
            band_type: 'spectral', unit: 'reflectance', display_order: 2,
            grid_statuses: [partitionedAwaitingIngest('band-optical-b04', 'geohash', 6)],
          },
        ],
      },
    ],
  },
  {
    dataset_id: 'radar-standard',
    dataset_code: 'S1-YANGZHOU-2018',
    dataset_title: '雷达遥感标准验收数据集',
    data_type: 'radar',
    product_type: 'S1-SLC',
    batch: 'radar-batch',
    batch_name: '雷达扬州 S1 标准载入批次',
    crs: 'EPSG:4326',
    resolution_native: 10,
    resolution_unit: 'm',
    resolution_m: 10,
    suggested_grid_type: 'geohash',
    suggested_grid_levels: { geohash: 4, mgrs: 1, isea4h: 6 },
    time_start: '2018-06-03T00:00:00+08:00',
    time_end: '2018-06-03T23:59:59+08:00',
    scenes: [
      {
        scene_id: 'radar-scene',
        scene_key: 'YANGZHOU-S1-20180603',
        acquisition_time: '2018-06-03T17:30:00+08:00',
        bbox: [119.2, 32.0, 120.0, 32.8],
        assets: [
          {
            asset_id: 'radar-vv',
            cog_uri: 's3://cube/cube/source/radar/yangzhou_s1_2018_2020/COG/20180603_VV.tif',
            bbox: [119.2, 32.0, 119.6, 32.4],
            crs: 'EPSG:4326',
          },
          {
            asset_id: 'radar-vh',
            cog_uri: 's3://cube/cube/source/radar/yangzhou_s1_2018_2020/COG/20180603_VH.tif',
            bbox: [119.6, 32.4, 120.0, 32.8],
            crs: 'EPSG:4326',
          },
        ],
        bands: [
          {
            band_unit_id: 'band-radar-vv', asset_id: 'radar-vv', band_code: 'VV', band_name: 'VV 同极化',
            band_type: 'polarization', unit: 'dB', display_order: 0,
            grid_statuses: [partitionedAwaitingIngest('band-radar-vv', 'isea4h', 6)],
          },
          {
            band_unit_id: 'band-radar-vh', asset_id: 'radar-vh', band_code: 'VH', band_name: 'VH 交叉极化',
            band_type: 'polarization', unit: 'dB', display_order: 1,
            grid_statuses: [],
          },
        ],
      },
    ],
  },
  {
    dataset_id: 'product-standard',
    dataset_code: 'ECO-DIANZHONG',
    dataset_title: '信息产品标准验收数据集',
    data_type: 'product',
    product_type: '生态安全评价',
    batch: 'product-batch',
    batch_name: '信息产品滇中生态安全载入批次',
    crs: 'EPSG:32648',
    resolution_native: 30,
    resolution_unit: 'm',
    resolution_m: 30,
    suggested_grid_type: 'geohash',
    suggested_grid_levels: { geohash: 4, mgrs: 1, isea4h: 6 },
    time_start: '1980-01-01T00:00:00+08:00',
    time_end: '2020-12-31T23:59:59+08:00',
    scenes: [
      {
        scene_id: 'product-scene-1',
        scene_key: 'ECO-1980',
        acquisition_time: '1980-01-01T00:00:00+08:00',
        bbox: [99.0, 23.0, 105.0, 27.0],
        assets: [
          {
            asset_id: 'product-asset-1',
            cog_uri: 's3://cube/cube/source/product/1980-2020年滇中地区30米生态安全评价数据集（第一版）_1980年.tif',
            bbox: [99.0, 23.0, 105.0, 27.0],
            crs: 'EPSG:32648',
          },
        ],
        bands: [
          {
            band_unit_id: 'band-product-value-1', asset_id: 'product-asset-1', band_code: 'VALUE',
            band_name: '生态安全评价', band_type: 'variable', unit: 'score', display_order: 0,
            grid_statuses: [partitionedAwaitingIngest('band-product-value-1', 'mgrs', 1)],
          },
        ],
      },
      {
        scene_id: 'product-scene-2',
        scene_key: 'ECO-2020',
        acquisition_time: '2020-01-01T00:00:00+08:00',
        bbox: [99.5, 23.2, 104.5, 26.8],
        assets: [
          {
            asset_id: 'product-asset-2',
            cog_uri: 's3://cube/cube/source/product/1980-2020年滇中地区30米生态安全评价数据集（第一版）_2020年.tif',
            bbox: [99.5, 23.2, 104.5, 26.8],
            crs: 'EPSG:32648',
          },
        ],
        bands: [
          {
            band_unit_id: 'band-product-value-2', asset_id: 'product-asset-2', band_code: 'VALUE',
            band_name: '生态安全评价', band_type: 'variable', unit: 'score', display_order: 0,
            grid_statuses: [],
          },
        ],
      },
    ],
  },
  {
    dataset_id: 'carbon-standard',
    dataset_code: 'OCO2-STD',
    dataset_title: '碳卫星标准验收数据集',
    data_type: 'carbon',
    product_type: 'xco2',
    batch: 'carbon-batch',
    batch_name: '碳卫星 OCO2 标准载入批次',
    crs: 'EPSG:4326',
    resolution_native: 3000,
    resolution_unit: 'm',
    resolution_m: 3000,
    suggested_grid_type: 'geohash',
    suggested_grid_levels: { geohash: 2, mgrs: 1, isea4h: 5 },
    time_start: '2020-12-31T00:00:00+08:00',
    time_end: '2020-12-31T23:59:59+08:00',
    scenes: [
      {
        scene_id: 'carbon-scene',
        scene_key: 'OCO2-201231',
        acquisition_time: '2020-12-31T03:40:00+08:00',
        bbox: [99.0, 22.0, 105.0, 28.0],
        assets: [
          {
            asset_id: 'carbon-asset',
            source_uri: 's3://cube/cube/source/carbon/oco2_LtCO2_201231_B11014Ar_220729012824s(1).nc4',
            source_kind: 'observation',
            source_format: 'netcdf',
            bbox: null,
            crs: null,
          },
        ],
        bands: [
          {
            band_unit_id: 'band-carbon-xco2', asset_id: 'carbon-asset', band_code: 'XCO2',
            band_name: '二氧化碳柱浓度', band_type: 'variable', unit: 'ppm', display_order: 0,
            grid_statuses: [partitionedAwaitingIngest('band-carbon-xco2', 'isea4h', 5)],
          },
        ],
      },
    ],
  },
];

const DATASETS_BY_ID = Object.fromEntries(DATASETS.map((dataset) => [dataset.dataset_id, dataset]));

function allBands(dataset) {
  return dataset.scenes.flatMap((scene) => scene.bands);
}

function gridSummary(dataset) {
  const bands = allBands(dataset);
  const summary = {};
  for (const gridType of Object.keys(LEVEL_RANGES)) {
    const statuses = bands.flatMap((band) => band.grid_statuses.filter((status) => status.grid_type === gridType));
    summary[gridType] = {
      partition: statuses.filter((status) => status.partition_status === 'completed').length,
      quality: statuses.filter((status) => ['pass', 'warn'].includes(status.quality_status)).length,
      ingest: statuses.filter((status) => status.ingest_status === 'completed').length,
      total: bands.length,
    };
  }
  return summary;
}

function datasetListItem(dataset) {
  const sceneCount = dataset.scenes.length;
  return {
    dataset_id: dataset.dataset_id,
    dataset_code: dataset.dataset_code,
    dataset_title: dataset.dataset_title,
    data_type: dataset.data_type,
    product_type: dataset.product_type,
    product_families: [],
    scene_count: sceneCount,
    time_start: dataset.time_start,
    time_end: dataset.time_end,
    current_output_version: 'v1',
    ingest_status: 'completed',
    quality_status: 'pass',
    publish_status: 'unpublished',
    archived: false,
  };
}

function datasetOverview(dataset) {
  const longs = dataset.scenes.map((scene) => scene.bbox);
  return {
    ...datasetListItem(dataset),
    ready_scene_count: dataset.scenes.length,
    failed_scene_count: 0,
    resolution_m: dataset.resolution_m,
    resolution_native: dataset.resolution_native,
    resolution_unit: dataset.resolution_unit,
    crs: dataset.crs,
    bbox: [
      Math.min(...longs.map((bbox) => bbox[0])),
      Math.min(...longs.map((bbox) => bbox[1])),
      Math.max(...longs.map((bbox) => bbox[2])),
      Math.max(...longs.map((bbox) => bbox[3])),
    ],
    description: 'E2E mock dataset mirroring the MinIO acceptance inventory',
    keywords: [],
    attributes: {},
    grid_summary: gridSummary(dataset),
  };
}

function scenesTab(dataset) {
  return {
    items: dataset.scenes.map((scene) => ({
      scene_id: scene.scene_id,
      scene_key: scene.scene_key,
      status: 'available',
      acquisition_time: scene.acquisition_time,
      source_batch_ids: [dataset.batch],
      eligible_source_batch_ids: [dataset.batch],
      bands: scene.bands.map((band) => ({
        band_unit_id: band.band_unit_id,
        asset_id: band.asset_id,
        band_code: band.band_code,
        band_name: band.band_name,
        band_type: band.band_type,
        unit: band.unit,
        display_order: band.display_order,
        attributes: {},
        publication_status: 'unpublished',
        grid_statuses: band.grid_statuses,
      })),
    })),
    total: dataset.scenes.length,
    page: 1,
    page_size: 20,
  };
}

function loadBatchScenesPayload(dataset, batchId, sceneIds = null) {
  const scenes = sceneIds
    ? dataset.scenes.filter((scene) => sceneIds.includes(scene.scene_id))
    : dataset.scenes;
  return {
    load_batch: {
      load_batch_id: batchId,
      batch_name: dataset.batch_name,
      status: 'succeeded',
      scene_count: scenes.length,
      dataset_count: 1,
    },
    scene_count: scenes.length,
    datasets: [
      {
        dataset_id: dataset.dataset_id,
        dataset_code: dataset.dataset_code,
        dataset_title: dataset.dataset_title,
        data_type: dataset.data_type,
        product_type: dataset.product_type,
        resolution_m: dataset.resolution_m,
        resolution_native: dataset.resolution_native,
        resolution_unit: dataset.resolution_unit,
        crs: dataset.crs,
        suggested_grid_type: dataset.suggested_grid_type,
        suggested_grid_levels: dataset.suggested_grid_levels,
        scenes: scenes.map((scene) => ({
          scene_id: scene.scene_id,
          scene_key: scene.scene_key,
          load_batch_id: batchId,
          load_status: 'succeeded',
          eligible_source_batch_ids: [batchId],
          source_asset_id: scene.assets[0].asset_id,
          source_uri: scene.assets[0].cog_uri || scene.assets[0].source_uri,
          bbox: scene.bbox,
          crs: scene.crs,
          acquisition_time: scene.acquisition_time,
          bands: scene.bands.map((band) => ({
            band_unit_id: band.band_unit_id,
            asset_id: band.asset_id,
            band_code: band.band_code,
            band_name: band.band_name,
            band_type: band.band_type,
            unit: band.unit,
            display_order: band.display_order,
            grid_statuses: band.grid_statuses,
          })),
        })),
      },
    ],
  };
}

function staticBatch(dataset) {
  return {
    load_batch_id: dataset.batch,
    batch_name: dataset.batch_name,
    source_type: 'loader',
    status: 'succeeded',
    scene_count: dataset.scenes.length,
    dataset_count: 1,
    data_type: dataset.data_type,
    created_at: '2026-07-18T08:00:00+08:00',
  };
}

function typeForBatch(state, batchId) {
  const dataset = DATASETS.find((item) => item.batch === batchId);
  if (dataset) return dataset.data_type;
  const reload = state.reloadBatches.get(batchId);
  return reload ? reload.dataType : null;
}

function validatePartition(partition) {
  if (!partition || typeof partition !== 'object') return 'partition is required';
  const range = LEVEL_RANGES[partition.grid_type];
  const method = derivedPartitionMethod(partition.grid_type);
  if (!range) return 'unsupported grid_type';
  if (!Number.isInteger(partition.requested_grid_level)
    || partition.requested_grid_level < range[0]
    || partition.requested_grid_level > range[1]) {
    return 'requested_grid_level out of range';
  }
  if (partition.partition_method !== method) return 'partition_method does not match grid_type';
  return null;
}

function validateReload(body) {
  if (!body || typeof body !== 'object') return 'missing body';
  if (!DATA_TYPES.includes(body.data_type)) return 'invalid data_type';
  if (!Array.isArray(body.datasets) || body.datasets.length !== 1) return 'reload must contain exactly one dataset';
  const selection = body.datasets[0] || {};
  const dataset = DATASETS_BY_ID[selection.dataset_id];
  if (!dataset) return `unknown dataset: ${selection.dataset_id}`;
  if (dataset.data_type !== body.data_type) return 'data_type does not match the selected dataset';
  if (selection.grid_config_locked !== true) return 'grid_config_locked must be true';
  const partitionError = validatePartition(selection.partition);
  if (partitionError) return partitionError;
  if (!Array.isArray(selection.band_unit_ids) || !selection.band_unit_ids.length) return 'band_unit_ids are required';
  if (!Array.isArray(selection.scenes) || !selection.scenes.length) return 'scenes are required';
  const knownBandUnitIds = new Set(allBands(dataset).map((band) => band.band_unit_id));
  if (selection.band_unit_ids.some((bandUnitId) => !knownBandUnitIds.has(bandUnitId))) {
    return 'band_unit_ids do not belong to the selected dataset';
  }
  const knownSceneIds = new Set(dataset.scenes.map((scene) => scene.scene_id));
  if (selection.scenes.some((scene) => !knownSceneIds.has(scene.scene_id))) {
    return 'scene does not belong to the selected dataset';
  }
  if (!Array.isArray(body.source_batch_ids) || !body.source_batch_ids.length) return 'source_batch_ids are required';
  return null;
}

function validateRun(body, state) {
  if (!body || typeof body !== 'object') return 'missing body';
  if (!/^partition-run-/.test(String(body.partition_run_id || ''))) return 'invalid partition_run_id';
  if (!['load_batch', 'dataset'].includes(body.selection_source)) return 'invalid selection_source';
  if (!Array.isArray(body.source_batch_ids) || !body.source_batch_ids.length) return 'source_batch_ids are required';
  if (new Set(body.source_batch_ids).size !== body.source_batch_ids.length) return 'duplicate source_batch_ids';
  if (body.source_batch_ids.some((batchId) => !typeForBatch(state, batchId))) return 'unknown source load batch';
  if (Object.hasOwn(body, 'batch_id')) return 'batch_id must not be sent';
  if (!Array.isArray(body.datasets) || !body.datasets.length) return 'datasets are required';
  for (const dataset of body.datasets) {
    if (!DATASETS_BY_ID[dataset.dataset_id]) return `unknown dataset: ${dataset.dataset_id}`;
    if (!Array.isArray(dataset.scene_ids) || !dataset.scene_ids.length) return 'scene_ids are required';
    if (!Array.isArray(dataset.band_unit_ids) || !dataset.band_unit_ids.length) return 'band_unit_ids are required';
    if (Object.hasOwn(dataset, 'grid_config_locked')) return 'grid_config_locked must not be sent to runs';
    const partitionError = validatePartition(dataset.partition);
    if (partitionError) return partitionError;
    if (dataset.source_batch_id && !body.source_batch_ids.includes(dataset.source_batch_id)) {
      return 'dataset source_batch_id is not selected';
    }
  }
  return null;
}

export async function installFullChainRoutes(page) {
  const state = {
    reloadBatches: new Map(),
    tasks: [],
    runCount: 0,
    reloadCount: 0,
    requests: [],
  };
  const unmocked = [];
  const json = (route, body, status = 200) => route.fulfill({
    status,
    contentType: 'application/json',
    body: JSON.stringify(body),
  });

  // Catch-all guard registered first (lowest priority): any /v1 or /api call
  // that no explicit mock covers is refused loudly instead of reaching a real
  // service. This is what makes the suite provably side-effect free.
  await page.route('**/*', async (route) => {
    const url = route.request().url();
    const pathname = new URL(url).pathname;
    if (pathname.startsWith('/v1/') || pathname.startsWith('/api/')) {
      unmocked.push(url);
      await json(route, { detail: 'request not mocked; refusing to touch real services' }, 500);
      return;
    }
    await route.fallback();
  });

  await page.route('**/api/config', (route) => json(route, { auth_required: false, navigation: [] }));

  await page.route('**/v1/datasets?**', (route) => json(route, {
    items: DATASETS.map(datasetListItem),
    total: DATASETS.length,
    page: 1,
    page_size: 20,
    summary: {
      dataset_count: DATASETS.length,
      scene_count: DATASETS.reduce((total, dataset) => total + dataset.scenes.length, 0),
      ready_scene_count: DATASETS.reduce((total, dataset) => total + dataset.scenes.length, 0),
      failed_scene_count: 0,
    },
  }));
  for (const dataset of DATASETS) {
    const id = dataset.dataset_id;
    await page.route(`**/v1/datasets/${id}`, (route) => json(route, datasetOverview(dataset)));
    await page.route(`**/v1/datasets/${id}/role-restrictions`, (route) => json(route, { hidden_roles: [] }));
    await page.route(`**/v1/datasets/${id}/scenes?**`, (route) => json(route, scenesTab(dataset)));
    for (const detail of [
      'assets', 'bands', 'outputs', 'grid', 'tiles', 'indexes',
      'ingest-records', 'quality', 'publications', 'provenance',
    ]) {
      await page.route(`**/v1/datasets/${id}/${detail}?**`, (route) => json(route, EMPTY_PAGE));
    }
  }

  await page.route('**/v1/partition/load-batches?**', async (route) => {
    const dataType = new URL(route.request().url()).searchParams.get('data_type');
    const batches = DATASETS.map((dataset) => staticBatch(dataset));
    for (const reload of state.reloadBatches.values()) {
      if (!dataType || reload.dataType === dataType) batches.push(reload.batchSummary);
    }
    await json(route, { load_batches: batches });
  });

  await page.route('**/v1/partition/load-batches/*/scenes?**', async (route) => {
    const parts = new URL(route.request().url()).pathname.split('/').filter(Boolean);
    const batchId = decodeURIComponent(parts[parts.length - 2]);
    const dataset = DATASETS.find((item) => item.batch === batchId);
    if (dataset) {
      await json(route, loadBatchScenesPayload(dataset, batchId));
      return;
    }
    const reload = state.reloadBatches.get(batchId);
    if (reload) {
      await json(route, reload.scenesPayload);
      return;
    }
    await json(route, { detail: `unknown load batch: ${batchId}` }, 404);
  });

  await page.route('**/v1/partition/reload-batches', async (route) => {
    const body = route.request().postDataJSON();
    state.requests.push({ kind: 'reload', body });
    const error = validateReload(body);
    if (error) {
      await json(route, { detail: error }, 422);
      return;
    }
    const selection = body.datasets[0];
    const dataset = DATASETS_BY_ID[selection.dataset_id];
    const loadBatchId = `dataset-reload-${++state.reloadCount}`;
    const formalSelection = {
      ...selection,
      selection_id: `${loadBatchId}:${dataset.dataset_id}`,
      source_batch_id: loadBatchId,
      selection_source: 'dataset_reload',
      grid_config_locked: true,
      scenes: selection.scenes.map((scene) => ({
        ...scene,
        source_batch_ids: [loadBatchId],
        eligible_source_batch_ids: [loadBatchId],
        load_batch_id: loadBatchId,
      })),
    };
    const reload = {
      loadBatchId,
      dataType: dataset.data_type,
      datasetId: dataset.dataset_id,
      formalSelection,
      batchSummary: {
        load_batch_id: loadBatchId,
        batch_name: String(body.draft_name || '').trim() || loadBatchId,
        source_type: 'dataset_reload',
        status: 'succeeded',
        scene_count: selection.scenes.length,
        dataset_count: 1,
        data_type: dataset.data_type,
        attributes: { reload_selection: { datasets: [formalSelection] } },
        created_at: new Date().toISOString(),
      },
      scenesPayload: loadBatchScenesPayload(
        dataset,
        loadBatchId,
        selection.scenes.map((scene) => scene.scene_id),
      ),
    };
    state.reloadBatches.set(loadBatchId, reload);
    await json(route, { ...reload.batchSummary, selection: { datasets: [formalSelection] } }, 201);
  });

  await page.route('**/v1/partition/runs', async (route) => {
    const body = route.request().postDataJSON();
    state.requests.push({ kind: 'run', body });
    const error = validateRun(body, state);
    if (error) {
      await json(route, { detail: error }, 422);
      return;
    }
    const taskId = `partition-task-${++state.runCount}`;
    const dataType = typeForBatch(state, body.source_batch_ids[0]);
    state.tasks.push({
      task_id: taskId,
      batch_id: body.partition_run_id,
      data_type: dataType,
      status: 'queued',
      operation: 'run',
      created_at: new Date().toISOString(),
    });
    await json(route, {
      partition_run_id: body.partition_run_id,
      source_batch_ids: body.source_batch_ids,
      task_id: taskId,
      status: 'queued',
      data_type: dataType,
      operation: 'run',
    }, 202);
  });

  await page.route('**/v1/partition/tasks?**', (route) => json(route, {
    tasks: state.tasks,
    total: state.tasks.length,
    page: 1,
    page_size: 20,
  }));

  await page.route('**/v1/grid/cover', async (route) => {
    const body = route.request().postDataJSON();
    const level = Number(body.requested_grid_level || 0);
    const [west, south, east, north] = (body.bbox || []).map(Number);
    await json(route, {
      cells: [
        { grid_type: body.grid_type, grid_level: level, space_code: `cell-${west}-${south}`, topology_code: null, bbox: body.bbox, geometry: null },
        { grid_type: body.grid_type, grid_level: level, space_code: `cell-${east}-${north}`, topology_code: null, bbox: body.bbox, geometry: null },
      ],
    });
  });

  const carbonFootprint = {
    scene_id: 'carbon-scene',
    source_batch_id: 'carbon-batch',
    observation_id: 'OCO2-201231-1',
    source_index: 0,
    geometry: {
      type: 'Polygon',
      coordinates: [[[99.0, 22.0], [100.0, 23.0], [101.0, 22.0], [99.0, 22.0]]],
    },
  };

  await page.route('**/v1/partition/carbon/footprints', async (route) => {
    const body = route.request().postDataJSON();
    await json(route, {
      items: [{ ...carbonFootprint, scene_id: body.scene_ids[0], source_batch_id: body.source_batch_ids[0] }],
      truncated: false,
      unavailable_sources: [],
    });
  });

  await page.route('**/v1/partition/carbon/grid-preview', async (route) => {
    const body = route.request().postDataJSON();
    const level = Number(body.requested_grid_level || 0);
    await json(route, {
      items: [{ ...carbonFootprint, scene_id: body.scene_ids[0], source_batch_id: body.source_batch_ids[0] }],
      cells: [
        {
          grid_type: body.grid_type, grid_level: level, space_code: 'carboncell1', topology_code: null,
          bbox: null,
          geometry: { type: 'Polygon', coordinates: [[[99.0, 22.0], [100.0, 23.0], [101.0, 22.0], [99.0, 22.0]]] },
        },
        {
          grid_type: body.grid_type, grid_level: level, space_code: 'carboncell2', topology_code: null,
          bbox: null,
          geometry: { type: 'Polygon', coordinates: [[[99.5, 22.5], [100.5, 23.5], [101.5, 22.5], [99.5, 22.5]]] },
        },
      ],
      cell_limit_reached: false,
      unavailable_sources: [],
    });
  });

  return { state, unmocked };
}
