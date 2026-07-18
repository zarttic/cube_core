import { mount } from '@vue/test-utils';
import { afterEach, describe, expect, it } from 'vitest';

import GridParameters from '@/views/partition/GridParameters.vue';

const wrappers = [];
afterEach(() => wrappers.splice(0).forEach((wrapper) => wrapper.unmount()));

describe('GridParameters', () => {
  it('shows production grids, derived method and server-owned load batch IDs', () => {
    const wrapper = mount(GridParameters, {
      props: {
        modelValue: { gridType: 'mgrs', requestedGridLevel: 2, coverMode: 'intersect', timeGranularity: 'day', maxCellsPerAsset: 0 },
        sourceBatchIds: ['REFLECT_20260717143357_8E3F', 'mock-two-optical-datasets-20260717-01'],
        selectedDatasetCount: 2,
        selectedCount: 3,
      },
      global: {
        stubs: {
          'el-form': { template: '<form><slot /></form>' },
          'el-form-item': { template: '<div><slot /></div>' },
          'el-select': { props: ['disabled'], template: '<select :disabled="disabled"><slot /></select>' },
          'el-option': { props: ['label'], template: '<span>{{ label }}</span>' },
          'el-input': { props: ['modelValue', 'readonly'], template: '<input :value="modelValue" :readonly="readonly" />' },
          'el-input-number': { template: '<input type="number" />' },
          'el-button': { template: '<button><slot /></button>' },
          'el-tag': { template: '<span><slot /></span>' },
          'el-tooltip': { props: ['content'], template: '<span class="tooltip-stub" :data-content="content"><slot /></span>' },
          ElInput: { props: ['modelValue', 'readonly'], template: '<input :value="modelValue" :readonly="readonly" />' },
        },
      },
    });
    wrappers.push(wrapper);
    expect(wrapper.text()).not.toContain('格网设置');
    expect(wrapper.text()).not.toContain('格网类型');
    expect(wrapper.text()).not.toContain('格网层级');
    expect(wrapper.text()).not.toContain('格网显示');
    expect(wrapper.text()).not.toContain('剖分方式');
    expect(wrapper.find('.queue-header-meta').text()).toBe('打开列表');
    expect(wrapper.text()).not.toContain('S2');
    expect(wrapper.text()).not.toContain('MGRS');
    expect(wrapper.get('.queue-selected-summary').text()).toContain('2 个数据集 · 3 个波段');
    expect(wrapper.text()).not.toContain('跨产品');
    expect(wrapper.get('[data-testid="selected-load-batches"]').text()).toContain('2 个批次');
    expect(wrapper.get('[data-testid="selected-load-batches"]').text()).toContain('REFLECT_20260717143357_8E3F');
    expect(wrapper.get('[data-testid="selected-load-batches"]').text()).toContain('mock-two-optical-datasets-20260717-01');
    expect(wrapper.findAll('.tooltip-stub').map((item) => item.attributes('data-content'))).toEqual([
      'REFLECT_20260717143357_8E3F',
      'mock-two-optical-datasets-20260717-01',
    ]);
    expect(wrapper.find('input[type="text"]').exists()).toBe(false);
    expect(wrapper.text()).not.toContain('覆盖方式');
    expect(wrapper.text()).not.toContain('时间粒度');
    expect(wrapper.text()).not.toContain('每数据单元最大格网单元数');
  });

});
