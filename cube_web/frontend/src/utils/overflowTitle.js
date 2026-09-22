/**
 * Global truncation affordance: any leaf element whose text is visually clipped
 * by CSS (overflow hidden/clip + ellipsis, multi-line clamp, ...) gets a native
 * `title` attribute on hover so the full content stays reachable.
 *
 * Kept opt-in per hovered element instead of scanning the whole DOM: the
 * document can be large and Element Plus re-renders tables constantly.
 */

const OVERFLOW_TITLE_FLAG = 'overflowTitle';

function isClippedAxis(value) {
  return value === 'hidden' || value === 'clip';
}

function truncatedText(el) {
  if (!(el instanceof HTMLElement)) return '';
  if (el.childElementCount > 0) return '';
  if (el.isContentEditable) return '';
  // Element Plus already renders its own tooltip for table cells (show-overflow-tooltip)
  // and for explicit el-tooltip triggers; skip those to avoid double tooltips.
  if (el.closest('[data-overflow-title-off]')) return '';
  if (el.classList.contains('cell') && el.closest('.el-table')) return '';
  const text = (el.textContent || '').trim();
  if (!text) return '';
  const style = window.getComputedStyle(el);
  const clipX = isClippedAxis(style.overflowX);
  const clipY = isClippedAxis(style.overflowY);
  if (!clipX && !clipY) return '';
  const overflowX = clipX && el.scrollWidth > el.clientWidth + 1;
  const overflowY = clipY && el.scrollHeight > el.clientHeight + 1;
  return overflowX || overflowY ? text : '';
}

function applyOverflowTitle(event) {
  const el = event.target;
  if (!(el instanceof HTMLElement)) return;
  const tracked = el.dataset[OVERFLOW_TITLE_FLAG] !== undefined;
  // Respect author-provided tooltips; only fill in the ones we generated.
  if (el.hasAttribute('title') && !tracked) return;
  const text = truncatedText(el);
  if (!text) {
    if (tracked) {
      delete el.dataset[OVERFLOW_TITLE_FLAG];
      el.removeAttribute('title');
    }
    return;
  }
  if (tracked && el.dataset[OVERFLOW_TITLE_FLAG] === text && el.getAttribute('title') === text) return;
  el.dataset[OVERFLOW_TITLE_FLAG] = text;
  el.setAttribute('title', text);
}

export function installOverflowTitles(target = document) {
  target.addEventListener('mouseover', applyOverflowTitle, { capture: true, passive: true });
  return () => target.removeEventListener('mouseover', applyOverflowTitle, { capture: true });
}
