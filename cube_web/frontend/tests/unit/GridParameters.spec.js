import { mount } from '@vue/test-utils';
import { afterEach, describe, expect, it } from 'vitest';

import GridParameters from '@/views/partition/GridParameters.vue';

const wrappers = [];
afterEach(() => wrappers.splice(0).forEach((wrapper) => wrapper.unmount()));

describe('GridParameters', () => {
  it('shows only production grids and a read-only derived method', () => {
    const wrapper = mount(GridParameters, {
      props: { modelValue: { gridType: 'mgrs', requestedGridLevel: 2, coverMode: 'intersect', timeGranularity: 'day', maxCellsPerAsset: 0 } },
      global: {
        stubs: {
          'el-form': { template: '<form><slot /></form>' },
          'el-form-item': { template: '<div><slot /></div>' },
          'el-select': { template: '<div><slot /></div>' },
          'el-option': { props: ['label'], template: '<span>{{ label }}</span>' },
          'el-input': { props: ['modelValue', 'readonly'], template: '<input :value="modelValue" :readonly="readonly" />' },
          'el-input-number': { template: '<input type="number" />' },
          'el-button': { template: '<button><slot /></button>' },
          ElInput: { props: ['modelValue', 'readonly'], template: '<input :value="modelValue" :readonly="readonly" />' },
        },
      },
    });
    wrappers.push(wrapper);
    expect(wrapper.text()).toContain('扩展 MGRS');
    expect(wrapper.text()).not.toContain('S2');
    expect(wrapper.text()).not.toContain('平面格网');
    expect(wrapper.vm.partitionMethod).toBe('logical');
  });
});
