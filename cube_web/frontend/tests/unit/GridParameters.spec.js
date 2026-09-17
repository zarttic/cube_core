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
    expect(wrapper.get('.worker-container-form-group label').text()).toBe('最多的容器数量');
    expect(wrapper.get('[data-testid="worker-container-limit-tooltip"]').attributes('data-content')).toBe(
      '限制本次任务最多使用的容器数量；0 表示按系统默认值运行。',
    );
    expect(wrapper.text()).not.toContain('0 表示按系统默认值运行');
    expect(wrapper.get('[data-testid="worker-container-limit"]').attributes('inputmode')).toBe('numeric');
    expect(wrapper.get('[data-testid="worker-container-note"]').text()).toBe('不限制单任务并发，按系统默认值运行');

    await wrapper.get('[data-testid="worker-container-limit"]').setValue('3');
    expect(wrapper.emitted('update:modelValue')[0][0]).toMatchObject({ workerContainerLimit: 3 });
  });

});

describe('GridParameters container limit', () => {
  const containerLimitStubs = {
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

  const limitOf = (wrapper, index = 0) => wrapper.emitted('update:modelValue')[index][0].workerContainerLimit;

  /** 模拟父组件：点击后把 emit 出的值写回 prop（组件本身是受控的）。 */
  async function clickAndSync(wrapper, selector) {
    const before = wrapper.emitted('update:modelValue')?.length ?? 0;
    await wrapper.get(selector).trigger('click');
    const emitted = wrapper.emitted('update:modelValue') ?? [];
    if (emitted.length > before) {
      await wrapper.setProps({ modelValue: { workerContainerLimit: limitOf(wrapper, emitted.length - 1) } });
    }
  }

  it('keeps the field non-negative and integral while typing', async () => {
    const wrapper = mountContainerLimit(0);
    const field = wrapper.get('[data-testid="worker-container-limit"]');

    // 编辑期只保留开头的数字串：负数/小数/乱输入都被整流。
    for (const [typed, expected] of [['-5', 0], ['2.5', 2], ['abc', 0], ['', 0], ['07', 7], ['12abc', 12]]) {
      await field.setValue(typed);
      expect(limitOf(wrapper, wrapper.emitted('update:modelValue').length - 1)).toBe(expected);
      expect(field.element.value).toBe(typed.match(/^\d*/)[0]);
    }
  });

  it('writes the normalised value back when the field loses focus', async () => {
    const wrapper = mountContainerLimit(12);
    const field = wrapper.get('[data-testid="worker-container-limit"]');

    await field.setValue('07');
    await field.trigger('blur');

    expect(limitOf(wrapper)).toBe(7);
    expect(field.element.value).toBe('12');
  });

  it('blocks minus and exponent keys so the field cannot display a negative number', () => {
    const wrapper = mountContainerLimit(0);
    const field = wrapper.get('[data-testid="worker-container-limit"]');
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

  it('steps with the −/＋ buttons and stops at zero', async () => {
    const wrapper = mountContainerLimit(2);
    const decrease = '[data-testid="worker-container-decrease"]';
    const increase = '[data-testid="worker-container-increase"]';

    await clickAndSync(wrapper, decrease);
    expect(limitOf(wrapper)).toBe(1);
    await clickAndSync(wrapper, increase);
    expect(limitOf(wrapper, 1)).toBe(2);

    await wrapper.setProps({ modelValue: { workerContainerLimit: 0 } });
    expect(wrapper.get(decrease).attributes('disabled')).toBeDefined();
    await wrapper.get(decrease).trigger('click');
    expect(wrapper.emitted('update:modelValue')).toHaveLength(2);
  });

  it('applies the preset chips and mirrors the result in the note', async () => {
    const wrapper = mountContainerLimit(0);

    await clickAndSync(wrapper, '[data-testid="worker-container-preset-4"]');
    expect(limitOf(wrapper)).toBe(4);
    expect(wrapper.get('[data-testid="worker-container-note"]').text()).toBe('本次任务同时最多占用 4 个计算容器');
    expect(wrapper.get('[data-testid="worker-container-preset-4"]').classes()).toContain('is-active');

    await clickAndSync(wrapper, '[data-testid="worker-container-preset-0"]');
    expect(limitOf(wrapper, 1)).toBe(0);
    expect(wrapper.get('[data-testid="worker-container-note"]').text()).toBe('不限制单任务并发，按系统默认值运行');
  });
});
