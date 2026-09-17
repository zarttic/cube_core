import { mount } from '@vue/test-utils';
import { afterEach, describe, expect, it } from 'vitest';

import GridParameters from '@/views/partition/GridParameters.vue';

const wrappers = [];
afterEach(() => wrappers.splice(0).forEach((wrapper) => wrapper.unmount()));

describe('GridParameters', () => {
  it('shows production grids, derived method and server-owned load batch IDs', async () => {
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
          'el-input-number': {
            name: 'ElInputNumber',
            emits: ['update:modelValue'],
            template: '<input type="number" />',
          },
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
    expect(wrapper.findAll('.source-batch-tags .tooltip-stub').map((item) => item.attributes('data-content'))).toEqual([
      'REFLECT_20260717143357_8E3F',
      'mock-two-optical-datasets-20260717-01',
    ]);
    expect(wrapper.find('input[type="text"]').exists()).toBe(false);
    expect(wrapper.text()).not.toContain('覆盖方式');
    expect(wrapper.text()).not.toContain('时间粒度');
    expect(wrapper.text()).not.toContain('每数据单元最大格网单元数');
    expect(wrapper.get('.worker-container-form-group label').text()).toBe('最多的容器数量');
    expect(wrapper.get('[data-testid="worker-container-limit-tooltip"]').attributes('data-content')).toBe(
      '限制本次任务最多使用的容器数量；0 表示按系统默认值运行。',
    );
    expect(wrapper.text()).not.toContain('0 表示按系统默认值运行');

    await wrapper.findComponent({ name: 'ElInputNumber' }).vm.$emit('update:modelValue', 3);
    expect(wrapper.emitted('update:modelValue')[0][0]).toMatchObject({ workerContainerLimit: 3 });
  });

});

describe('GridParameters container limit', () => {
  const containerLimitStubs = {
    'el-input-number': { name: 'ElInputNumber', emits: ['update:modelValue'], template: '<input type="number" />' },
    'el-tooltip': { props: ['content'], template: '<span><slot /></span>' },
    'el-button': { template: '<button><slot /></button>' },
    'el-tag': { template: '<span><slot /></span>' },
  };

  function mountContainerLimit(workerContainerLimit) {
    const wrapper = mount(GridParameters, {
      props: {
        modelValue: { workerContainerLimit },
        selectedDatasetCount: 1,
        selectedCount: 1,
        sourceBatchIds: [],
      },
      global: { stubs: containerLimitStubs },
    });
    wrappers.push(wrapper);
    return wrapper;
  }

  it('coerces negative, fractional and empty limits back to the default 0', async () => {
    const wrapper = mountContainerLimit(0);
    const input = wrapper.findComponent({ name: 'ElInputNumber' });
    for (const value of [-5, -1, 2.5, null, undefined, 4]) {
      await input.vm.$emit('update:modelValue', value);
    }
    expect(wrapper.emitted('update:modelValue').map(([payload]) => payload.workerContainerLimit))
      .toEqual([0, 0, 0, 0, 0, 4]);
  });

  it('blocks minus and exponent keys so the field cannot display a negative number', () => {
    const wrapper = mountContainerLimit(0);
    const field = wrapper.get('input[type="number"]');
    for (const key of ['-', '+', 'e', 'E']) {
      const blocked = new KeyboardEvent('keydown', { key, bubbles: true, cancelable: true });
      field.element.dispatchEvent(blocked);
      expect(blocked.defaultPrevented).toBe(true);
    }
    for (const key of ['0', '7', 'Backspace', 'ArrowDown']) {
      const allowed = new KeyboardEvent('keydown', { key, bubbles: true, cancelable: true });
      field.element.dispatchEvent(allowed);
      expect(allowed.defaultPrevented).toBe(false);
    }
  });
});
