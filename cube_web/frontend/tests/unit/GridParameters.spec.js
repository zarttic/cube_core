import { mount } from '@vue/test-utils';
import { afterEach, describe, expect, it } from 'vitest';

import GridParameters from '@/views/partition/GridParameters.vue';

const wrappers = [];
afterEach(() => wrappers.splice(0).forEach((wrapper) => wrapper.unmount()));

const tooltipStub = { props: ['content'], template: '<span class="tooltip-stub" :data-content="content"><slot /></span>' };
// el-switch 在单测环境没有全局注册，用一个点击取反的按钮模拟它。
const switchStub = {
  name: 'ElSwitch',
  props: ['modelValue'],
  emits: ['update:modelValue'],
  template: '<button type="button" @click="$emit(\'update:modelValue\', !modelValue)" />',
};
const toggleSelector = '[data-testid="worker-container-limit-toggle"]';
const fieldSelector = '[data-testid="worker-container-limit"]';
const noteSelector = '[data-testid="worker-container-note"]';

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
          'el-tooltip': tooltipStub,
          'el-switch': switchStub,
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
    expect(wrapper.get('.worker-container-form-group label').text()).toBe('容器限制');
    expect(wrapper.get('[data-testid="worker-container-limit-tooltip"]').attributes('data-content')).toBe(
      '默认不设置单任务容器限制，按系统默认并发运行；开启后可限制本次任务最多使用的容器数量。',
    );
    expect(wrapper.text()).not.toContain('按系统默认值运行');
    expect(wrapper.get(toggleSelector).attributes('aria-label')).toBe('设置容器限制');
    // 未开启时只有开关：没有数字框、没有 1／2／4／8 预设、也没有小字说明。
    expect(wrapper.find(noteSelector).exists()).toBe(false);
    expect(wrapper.find(fieldSelector).exists()).toBe(false);
    expect(wrapper.find('[data-testid="worker-container-decrease"]').exists()).toBe(false);
    expect(wrapper.findAll('.limit-chip')).toHaveLength(0);

    await wrapper.get(toggleSelector).trigger('click');
    expect(wrapper.emitted('update:modelValue')[0][0]).toMatchObject({ workerContainerLimit: 4 });
  });

});

describe('GridParameters container limit', () => {
  const containerLimitStubs = {
    'el-tooltip': tooltipStub,
    'el-button': { template: '<button><slot /></button>' },
    'el-tag': { template: '<span><slot /></span>' },
    'el-switch': switchStub,
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
  const noteOf = (wrapper) => wrapper.get(noteSelector).text();

  /** 模拟父组件：点击后把 emit 出的值写回 prop（组件本身是受控的）。 */
  async function clickAndSync(wrapper, selector) {
    const before = wrapper.emitted('update:modelValue')?.length ?? 0;
    await wrapper.get(selector).trigger('click');
    const emitted = wrapper.emitted('update:modelValue') ?? [];
    if (emitted.length > before) {
      await wrapper.setProps({ modelValue: { workerContainerLimit: limitOf(wrapper, emitted.length - 1) } });
    }
  }

  it('renders nothing but the switch until the limit is switched on', () => {
    const wrapper = mountContainerLimit(0);

    expect(wrapper.find(noteSelector).exists()).toBe(false);
    expect(wrapper.find(fieldSelector).exists()).toBe(false);
    expect(wrapper.find('[data-testid="worker-container-decrease"]').exists()).toBe(false);
    expect(wrapper.find('[data-testid="worker-container-increase"]').exists()).toBe(false);
    expect(wrapper.findAll('.limit-chip')).toHaveLength(0);
  });

  it('shows the number editor when the limit is switched on', async () => {
    const wrapper = mountContainerLimit(0);

    await clickAndSync(wrapper, toggleSelector);

    expect(limitOf(wrapper)).toBe(4);
    expect(noteOf(wrapper)).toBe('本次任务同时最多占用 4 个计算容器');
    expect(wrapper.get(fieldSelector).attributes('inputmode')).toBe('numeric');
    expect(wrapper.get(fieldSelector).element.value).toBe('4');
    expect(wrapper.get('[data-testid="worker-container-decrease"]').attributes('disabled')).toBeUndefined();
    expect(wrapper.findAll('.limit-chip')).toHaveLength(0);
  });

  it('re-enables with the previously entered value instead of the default', async () => {
    const wrapper = mountContainerLimit(0);

    await clickAndSync(wrapper, toggleSelector);
    const field = wrapper.get(fieldSelector);
    await field.setValue('7');
    await wrapper.setProps({ modelValue: { workerContainerLimit: 7 } });
    expect(limitOf(wrapper, 1)).toBe(7);

    await clickAndSync(wrapper, toggleSelector);
    expect(limitOf(wrapper, 2)).toBe(0);
    expect(wrapper.find(fieldSelector).exists()).toBe(false);

    await clickAndSync(wrapper, toggleSelector);
    expect(limitOf(wrapper, 3)).toBe(7);
    expect(wrapper.get(fieldSelector).element.value).toBe('7');
  });

  it('switching the limit off falls back to the system default concurrency', async () => {
    const wrapper = mountContainerLimit(4);
    expect(noteOf(wrapper)).toBe('本次任务同时最多占用 4 个计算容器');

    await clickAndSync(wrapper, toggleSelector);

    expect(limitOf(wrapper)).toBe(0);
    expect(wrapper.find(noteSelector).exists()).toBe(false);
    expect(wrapper.find(fieldSelector).exists()).toBe(false);
  });

  it('keeps digits only while typing', async () => {
    const wrapper = mountContainerLimit(2);
    const field = wrapper.get(fieldSelector);

    // 编辑期只保留开头的数字串：负数/小数/乱输入都被整流。
    for (const [typed, expected] of [['2.5', 2], ['07', 7], ['12abc', 12]]) {
      await field.setValue(typed);
      expect(limitOf(wrapper, wrapper.emitted('update:modelValue').length - 1)).toBe(expected);
      expect(field.element.value).toBe(typed.match(/^\d*/)[0]);
    }
  });

  it('ignores an empty draft and a typed zero without switching the limit off', async () => {
    const wrapper = mountContainerLimit(2);
    const field = wrapper.get(fieldSelector);

    await field.setValue('');
    expect(wrapper.emitted('update:modelValue')).toBeUndefined();
    expect(field.element.value).toBe('');

    await field.setValue('0');
    expect(wrapper.emitted('update:modelValue')).toBeUndefined();
    expect(wrapper.find(fieldSelector).exists()).toBe(true);
    expect(noteOf(wrapper)).toBe('本次任务同时最多占用 2 个计算容器');

    await field.setValue('-5abc');
    expect(wrapper.emitted('update:modelValue')).toBeUndefined();
    expect(field.element.value).toBe('');

    await field.trigger('blur');
    expect(field.element.value).toBe('2');
  });

  it('writes the normalised value back when the field loses focus', async () => {
    const wrapper = mountContainerLimit(12);
    const field = wrapper.get(fieldSelector);

    await field.setValue('07');
    await field.trigger('blur');

    expect(limitOf(wrapper)).toBe(7);
    expect(field.element.value).toBe('12');
  });

  it('blocks minus and exponent keys so the field cannot display a negative number', () => {
    const wrapper = mountContainerLimit(1);
    const field = wrapper.get(fieldSelector);
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

  it('steps with the −/＋ buttons and never goes below one', async () => {
    const wrapper = mountContainerLimit(2);
    const decrease = '[data-testid="worker-container-decrease"]';
    const increase = '[data-testid="worker-container-increase"]';

    await clickAndSync(wrapper, decrease);
    expect(limitOf(wrapper)).toBe(1);
    expect(wrapper.get(decrease).attributes('disabled')).toBeDefined();
    await wrapper.get(decrease).trigger('click');
    expect(wrapper.emitted('update:modelValue')).toHaveLength(1);

    await clickAndSync(wrapper, increase);
    expect(limitOf(wrapper, 1)).toBe(2);
    expect(wrapper.get(decrease).attributes('disabled')).toBeUndefined();
  });
});
