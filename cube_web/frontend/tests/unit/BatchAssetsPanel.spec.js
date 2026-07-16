import { flushPromises, mount } from '@vue/test-utils';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('@/api/client', () => ({ requestGet: vi.fn() }));

import { requestGet } from '@/api/client';
import BatchAssetsPanel from '@/views/partition/BatchAssetsPanel.vue';

const dataset = { dataset_id: 'dataset-a', dataset_code: 'DS-A', dataset_title: 'Dataset A', data_type: 'optical', product_type: 'L2A' };
const wrappers = [];

function page(items, total = items.length) {
  return { items, total, page: 1, page_size: 500 };
}

const selectStubs = {
  'el-alert': true,
  'el-table': true,
  'el-table-column': true,
  'el-select': { template: '<select><slot /></select>' },
  'el-option': { template: '<option><slot /></option>' },
};

beforeEach(() => requestGet.mockReset());
afterEach(() => wrappers.splice(0).forEach((wrapper) => wrapper.unmount()));

describe('BatchAssetsPanel', () => {
  it('loads every asset page before emitting a strict dataset input', async () => {
    requestGet.mockResolvedValueOnce(page([dataset]));
    const wrapper = mount(BatchAssetsPanel, {
      props: { modelValue: [], defaultGridType: 'isea4h', defaultRequestedGridLevel: 4 },
      global: {
        directives: { loading: {} },
        stubs: selectStubs,
      },
    });
    wrappers.push(wrapper);
    await flushPromises();

    const assets = Array.from({ length: 501 }, (_item, index) => ({ source_asset_id: `asset-${index}`, cog_uri: `s3://cube/source/${index}.tif` }));
    requestGet.mockImplementation((url) => {
      if (typeof url !== 'string') return Promise.resolve(page([]));
      if (url.includes('/assets?page=1')) return Promise.resolve(page(assets.slice(0, 500), 501));
      if (url.includes('/assets?page=2')) return Promise.resolve({ ...page(assets.slice(500), 501), page: 2 });
      return Promise.resolve(page([{ source_asset_id: 'asset-0', band_code: 'B01', band_name: 'Blue', band_type: 'spectral', display_order: 1 }]));
    });

    await wrapper.vm.updateSelected(['dataset-a']);
    const [[emitted]] = wrapper.emitted('update:modelValue');
    expect(emitted[0].assets).toHaveLength(501);
    expect(emitted[0].partition).toEqual({ grid_type: 'isea4h', requested_grid_level: 4, partition_method: 'entity' });
    expect(requestGet).toHaveBeenCalledWith(expect.stringContaining('/assets?page=2&page_size=500'));
  });

  it('updates each selected dataset partition independently', async () => {
    const wrapper = mount(BatchAssetsPanel, {
      props: {
        modelValue: [
          { ...dataset, partition: { grid_type: 'geohash', requested_grid_level: 6, partition_method: 'logical' } },
          { ...dataset, dataset_id: 'dataset-b', partition: { grid_type: 'isea4h', requested_grid_level: 4, partition_method: 'entity' } },
        ],
      },
      global: { directives: { loading: {} }, stubs: selectStubs },
    });
    wrappers.push(wrapper);

    wrapper.vm.updatePartition('dataset-a', { grid_type: 'mgrs' });
    const [[emitted]] = wrapper.emitted('update:modelValue');
    expect(emitted).toEqual(expect.arrayContaining([
      expect.objectContaining({ dataset_id: 'dataset-a', partition: { grid_type: 'mgrs', requested_grid_level: 0, partition_method: 'logical' } }),
      expect.objectContaining({ dataset_id: 'dataset-b', partition: { grid_type: 'isea4h', requested_grid_level: 4, partition_method: 'entity' } }),
    ]));
  });

  it('keeps a selected dataset partition while additional datasets are loaded', async () => {
    const selectedDataset = {
      ...dataset,
      assets: [{ source_asset_id: 'asset-a' }],
      bands: [{ source_asset_id: 'asset-a', band_code: 'B01' }],
      partition: { grid_type: 'mgrs', requested_grid_level: 2, partition_method: 'logical' },
    };
    const datasetB = { ...dataset, dataset_id: 'dataset-b', dataset_code: 'DS-B' };
    const wrapper = mount(BatchAssetsPanel, {
      props: { modelValue: [selectedDataset] },
      global: { directives: { loading: {} }, stubs: selectStubs },
    });
    wrappers.push(wrapper);
    wrapper.vm.available = [dataset, datasetB];
    requestGet.mockImplementation((url) => Promise.resolve(page(typeof url === 'string' && url.includes('/assets')
      ? [{ source_asset_id: 'asset-b', cog_uri: 's3://cube/source/b.tif' }]
      : [{ source_asset_id: 'asset-b', band_code: 'B01', band_name: 'Blue', band_type: 'spectral', display_order: 1 }])));

    await wrapper.vm.updateSelected(['dataset-a', 'dataset-b']);
    const [[emitted]] = wrapper.emitted('update:modelValue');
    expect(emitted).toEqual(expect.arrayContaining([
      expect.objectContaining({ dataset_id: 'dataset-a', partition: selectedDataset.partition }),
      expect.objectContaining({ dataset_id: 'dataset-b', partition: { grid_type: 'geohash', requested_grid_level: 6, partition_method: 'logical' } }),
    ]));
    expect(requestGet).not.toHaveBeenCalledWith(expect.stringContaining('/dataset-a/'));
  });

  it('preserves a carbon raw source instead of rewriting it as a COG', async () => {
    const carbonDataset = { ...dataset, dataset_id: 'dataset-carbon', data_type: 'carbon', product_type: 'oco2_lite' };
    const wrapper = mount(BatchAssetsPanel, {
      props: { modelValue: [] },
      global: { directives: { loading: {} }, stubs: selectStubs },
    });
    wrappers.push(wrapper);
    wrapper.vm.available = [carbonDataset];
    requestGet.mockImplementation((url) => Promise.resolve(page(typeof url === 'string' && url.includes('/assets')
      ? [{ source_asset_id: 'asset-carbon', source_uri: 's3://cube/cube/source/carbon/observation.nc4', source_format: 'netcdf' }]
      : [{ source_asset_id: 'asset-carbon', band_code: 'xco2', band_name: 'XCO2', band_type: 'variable', display_order: 1 }])));

    await wrapper.vm.updateSelected(['dataset-carbon']);
    const [[emitted]] = wrapper.emitted('update:modelValue');
    expect(emitted[0].assets[0]).toMatchObject({ source_uri: 's3://cube/cube/source/carbon/observation.nc4', source_format: 'netcdf' });
    expect(emitted[0].assets[0]).not.toHaveProperty('cog_uri');
  });
});
