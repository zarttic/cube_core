import { afterEach, describe, expect, it, vi } from 'vitest';

import { installOverflowTitles } from '@/utils/overflowTitle';

function makeElement(text, { title, overflow = { scrollWidth: 200, clientWidth: 100 } } = {}) {
  const el = document.createElement('span');
  el.textContent = text;
  if (title !== undefined) el.setAttribute('title', title);
  Object.defineProperty(el, 'scrollWidth', { value: overflow.scrollWidth, configurable: true });
  Object.defineProperty(el, 'clientWidth', { value: overflow.clientWidth, configurable: true });
  document.body.appendChild(el);
  return el;
}

function hover(el, style) {
  const styleSpy = vi.spyOn(window, 'getComputedStyle').mockReturnValue(style);
  el.dispatchEvent(new MouseEvent('mouseover', { bubbles: true }));
  styleSpy.mockRestore();
}

let uninstall = null;

afterEach(() => {
  if (uninstall) uninstall();
  uninstall = null;
  vi.restoreAllMocks();
});

describe('installOverflowTitles', () => {
  it('adds a native title only for clipped leaf elements that overflow', () => {
    uninstall = installOverflowTitles(document);
    const clipped = { overflowX: 'hidden', overflowY: 'hidden' };

    const truncated = makeElement('超长数据集标题');
    const fits = makeElement('短标题', { overflow: { scrollWidth: 100, clientWidth: 100 } });
    const wrapped = makeElement('会换行的说明文字');

    hover(truncated, clipped);
    hover(fits, clipped);
    hover(wrapped, { overflowX: 'visible', overflowY: 'visible' });

    expect(truncated.getAttribute('title')).toBe('超长数据集标题');
    expect(fits.hasAttribute('title')).toBe(false);
    expect(wrapped.hasAttribute('title')).toBe(false);
  });

  it('keeps author-provided titles untouched', () => {
    uninstall = installOverflowTitles(document);
    const authored = makeElement('长内容', { title: '作者自定提示' });

    hover(authored, { overflowX: 'hidden', overflowY: 'hidden' });

    expect(authored.getAttribute('title')).toBe('作者自定提示');
  });

  it('ignores containers with child elements so tables and pages keep their own tooltips', () => {
    uninstall = installOverflowTitles(document);
    const container = document.createElement('div');
    container.innerHTML = '<span>子元素</span>';
    Object.defineProperty(container, 'scrollWidth', { value: 400, configurable: true });
    Object.defineProperty(container, 'clientWidth', { value: 100, configurable: true });
    document.body.appendChild(container);

    hover(container, { overflowX: 'hidden', overflowY: 'hidden' });

    expect(container.hasAttribute('title')).toBe(false);
  });

  it('skips elements that already own a tooltip (el-table cells and opt-out markers)', () => {
    uninstall = installOverflowTitles(document);
    const clipped = { overflowX: 'hidden', overflowY: 'hidden' };

    const table = document.createElement('div');
    table.className = 'el-table';
    const cell = makeElement('表格内截断文本');
    cell.classList.add('cell');
    table.appendChild(cell);
    document.body.appendChild(table);

    const optOut = document.createElement('div');
    optOut.setAttribute('data-overflow-title-off', '');
    const tagged = makeElement('已有自定义提示');
    optOut.appendChild(tagged);
    document.body.appendChild(optOut);

    hover(cell, clipped);
    hover(tagged, clipped);

    expect(cell.hasAttribute('title')).toBe(false);
    expect(tagged.hasAttribute('title')).toBe(false);
  });
});
