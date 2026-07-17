import { runtimeNavigation } from '@/config';

export const portalHomeUrl = 'http://10.3.100.165:5176/#/home';

const localNavPaths = {
  分析就绪数据剖分: '/partition',
  质量运行记录: '/quality',
  全球离散格网模型与编码: '/encoding',
};

const hiddenNavLabels = new Set(['数据库入库管理', '数据管理与入库', '自动化质检', '系统配置']);

const defaultNavItems = [
  { label: '首页', kind: 'external', url: portalHomeUrl },
  { label: 'ARD数据载入', kind: 'external', url: '/ard' },
  { label: '分析就绪数据剖分', kind: 'internal', path: '/partition' },
  { label: '剖分数据服务', kind: 'external', url: '/partition' },
  { label: '资源调度', kind: 'external', url: '/dispatch' },
  { label: '后台管理', kind: 'external', url: '/admin' },
  { label: '全球离散格网模型与编码', kind: 'internal', path: '/encoding' },
];

const headerLabelOrder = [
  '首页',
  'ARD数据载入',
  '分析就绪数据剖分',
  '剖分数据服务',
  '资源调度',
  '后台管理',
  '全球离散格网模型与编码',
];

const publicNavLabels = new Set(['全球离散格网模型与编码']);

function normalizeNavItem(item) {
  if (!item?.label) return null;
  if (hiddenNavLabels.has(item.label)) return null;
  if (item.label === '数据集管理') return null;
  if (item.label === '首页') return { label: '首页', kind: 'external', url: portalHomeUrl };
  if (localNavPaths[item.label]) return { label: item.label, kind: 'internal', path: localNavPaths[item.label] };
  if (item.kind === 'external' && item.url) return item;
  if (item.url) return { label: item.label, kind: 'external', url: item.url };
  if (item.path) return { label: item.label, kind: 'external', url: item.path };
  return null;
}

export function navItems(isAdmin = true) {
  const itemsByLabel = new Map();
  defaultNavItems.forEach((item) => {
    const normalized = normalizeNavItem(item);
    if (normalized) itemsByLabel.set(normalized.label, normalized);
  });
  runtimeNavigation().forEach((item) => {
    const normalized = normalizeNavItem(item);
    if (normalized) itemsByLabel.set(normalized.label, normalized);
  });
  const items = [...itemsByLabel.values()].filter((item) => isAdmin || publicNavLabels.has(item.label));
  return [
    ...headerLabelOrder.flatMap((label) => items.filter((item) => item.label === label)),
    ...items.filter((item) => !headerLabelOrder.includes(item.label)),
  ];
}

export function normalizePath(pathname) {
  if (pathname === '/index.html') return '/';
  if (pathname === '/partition.html') return '/partition';
  if (pathname === '/encoding.html') return '/encoding';
  if (pathname === '/config' || pathname === '/config.html') return '/config';
  if (pathname === '/门户首页.html') return '/';
  if (pathname === '/' || pathname === '/partition' || pathname === '/data-management' || pathname === '/quality' || pathname === '/encoding' || pathname === '/config') return pathname;
  return '/partition';
}
