import { flushPromises, mount } from '@vue/test-utils';
import { createPinia, setActivePinia } from 'pinia';
import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('@/api/client', () => ({ requestGet: vi.fn(), requestPost: vi.fn() }));

import { requestGet } from '@/api/client';
import QualityView from '@/views/QualityView.vue';

const stubs = {
  AppTable: { template: '<div><slot /></div>' },
  PartitionQualityDrawer: { template: '<div />' },
  StatusTag: { template: '<span />' },
  'el-alert': { template: '<div />' },
  'el-button': { template: '<button><slot /></button>' },
  'el-dialog': { template: '<div><slot /></div>' },
  'el-drawer': { template: '<div><slot /></div>' },
  'el-form': { template: '<form><slot /></form>' },
  'el-form-item': { template: '<div><slot /></div>' },
  'el-input': { props: ['modelValue'], template: '<input :value="modelValue" />' },
  'el-option': { template: '<option><slot /></option>' },
  'el-select': { template: '<select><slot /></select>' },
  'el-pagination': { template: '<div />' },
  'el-table-column': { template: '<div />' },
  'el-checkbox': { template: '<label><slot /></label>' },
  'el-tag': { template: '<span><slot /></span>' },
};

beforeEach(() => {
  setActivePinia(createPinia());
  requestGet.mockReset().mockResolvedValue({ items: [], total: 0, page: 1, page_size: 20 });
});

describe('QualityView partition batch list', () => {
  it('uses server-side search and pagination for automated quality batches', async () => {
    requestGet.mockResolvedValue({
      items: [{ partition_run_id: 'partition-run-1', status: 'completed' }],
      total: 41,
      page: 1,
      page_size: 20,
    });
    const wrapper = mount(QualityView, { global: { stubs } });
    await flushPromises();

    expect(requestGet).toHaveBeenNthCalledWith(1, '/v1/partition/runs?page=1&page_size=20');
    expect(wrapper.vm.pageState.total).toBe(41);

    wrapper.vm.filters.keyword = 'landsat';
    wrapper.vm.filters.dataType = 'optical';
    await wrapper.vm.applyFilters();

    expect(requestGet).toHaveBeenLastCalledWith(
      '/v1/partition/runs?keyword=landsat&data_type=optical&page=1&page_size=20',
    );
  });
});
