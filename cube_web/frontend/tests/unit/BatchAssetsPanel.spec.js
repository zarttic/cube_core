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

beforeEach(() => requestGet.mockReset());
afterEach(() => wrappers.splice(0).forEach((wrapper) => wrapper.unmount()));

describe('BatchAssetsPanel', () => {
  it('loads every asset page before emitting a strict dataset input', async () => {
    requestGet.mockResolvedValueOnce(page([dataset]));
    const wrapper = mount(BatchAssetsPanel, {
      props: { modelValue: [] },
      global: {
        directives: { loading: {} },
        stubs: { 'el-alert': true, 'el-table': true, 'el-table-column': true },
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
    expect(requestGet).toHaveBeenCalledWith(expect.stringContaining('/assets?page=2&page_size=500'));
  });
});
