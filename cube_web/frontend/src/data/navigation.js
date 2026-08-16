import { runtimeNavigation } from '@/config';

export const portalHomeUrl = 'http://10.3.100.165:5176/#/home';

const localNavPaths = {
  分析就绪数据剖分: '/partition',
  质量运行记录: '/quality',
  全球离散格网模型与编码: '/encoding',
  系统配置: '/config',
};

const hiddenNavLabels = new Set(['数据库入库管理', '数据管理与入库', '自动化质检']);

const navPermissions = {
  ARD数据载入: 'data_import:view',
  分析就绪数据剖分: 'data_import:view',
  剖分数据服务: 'data_import:view',
  全球离散格网模型与编码: 'data_import:view',
  资源调度: 'resource_schedule:view',
  系统配置: 'system_config:view',
};

const adminNavigationPermissions = [
  'user_manage:view',
  'resource_schedule:view',
  'security_audit:view',
  'system_config:view',
  'order_manage:view',
  'storage:view',
  'team_manage:view',
  'monitor:view',
];

const defaultNavItems = [
  { label: '首页', kind: 'external', url: portalHomeUrl },
  { label: 'ARD数据载入', kind: 'external', url: '/ard' },
  { label: '分析就绪数据剖分', kind: 'admin', path: '/partition' },
  { label: '剖分数据服务', kind: 'external', url: '/partition' },
  { label: '资源调度', kind: 'external', url: '/dispatch' },
  { label: '后台管理', kind: 'external', url: '/admin' },
  { label: '全球离散格网模型与编码', kind: 'internal', path: '/encoding' },
  { label: '系统配置', kind: 'internal', path: '/config' },
];

const headerLabelOrder = [
  '首页',
  'ARD数据载入',
  '分析就绪数据剖分',
  '剖分数据服务',
  '资源调度',
  '后台管理',
  '全球离散格网模型与编码',
  '系统配置',
];

function normalizeNavItem(item) {
  if (!item?.label) return null;
  if (hiddenNavLabels.has(item.label)) return null;
  if (item.label === '数据集管理') return null;
  if (item.label === '首页') return { label: '首页', kind: 'external', url: portalHomeUrl };
  const permission = item.permission || navPermissions[item.label];
  const permissions = item.permissions || (item.label === '后台管理' ? adminNavigationPermissions : null);
  if (item.label === '分析就绪数据剖分') return { label: item.label, kind: 'internal', path: '/partition', permission };
  if (localNavPaths[item.label]) return { label: item.label, kind: 'internal', path: localNavPaths[item.label], permission };
  if (item.kind === 'admin') return { ...item, permission, permissions };
  if (item.kind === 'external' && item.url) return { ...item, permission, permissions };
  if (item.url) return { label: item.label, kind: 'external', url: item.url, permission, permissions };
  if (item.path) return { label: item.label, kind: 'external', url: item.path, permission, permissions };
  return null;
}

export function navItems(access = true) {
  const legacyIsAdmin = typeof access === 'boolean' ? access : undefined;
  const isSuperAdmin = typeof access === 'boolean' ? access : access?.isSuperAdmin !== false;
  const can = typeof access === 'object' && typeof access?.can === 'function' ? access.can : null;
  const itemsByLabel = new Map();
  defaultNavItems.forEach((item) => {
    const normalized = normalizeNavItem(item);
    if (normalized) itemsByLabel.set(normalized.label, normalized);
  });
  runtimeNavigation().forEach((item) => {
    const normalized = normalizeNavItem(item);
    if (normalized) itemsByLabel.set(normalized.label, normalized);
  });
  const items = [...itemsByLabel.values()].filter((item) => {
    if (isSuperAdmin) return true;
    if (legacyIsAdmin === false && !can) return item.kind !== 'admin' && !item.permission && !item.permissions;
    if (!item.permission && !item.permissions) return true;
    if (!can) return false;
    if (item.permission && can(item.permission)) return true;
    return Array.isArray(item.permissions) && item.permissions.some((permission) => can(permission));
  });
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
