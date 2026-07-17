const presentations = {
  partition: {
    pending: ['等待中', 'info'],
    queued: ['已排队', 'info'],
    running: ['运行中', 'primary'],
    completed: ['已完成', 'success'],
    failed: ['失败', 'danger'],
    cancelled: ['已取消', 'info'],
  },
  ingest: {
    pending: ['等待中', 'info'],
    queued: ['已排队', 'info'],
    running: ['运行中', 'primary'],
    completed: ['已完成', 'success'],
    partial_failure: ['部分失败', 'warning'],
    failed: ['失败', 'danger'],
    cancelled: ['已取消', 'info'],
  },
  scene: {
    discovered: ['已发现', 'info'],
    validated: ['已校验', 'success'],
    ready: ['可处理', 'success'],
    processing: ['处理中', 'primary'],
    completed: ['已完成', 'success'],
    failed: ['失败', 'danger'],
    archived: ['已归档', 'info'],
  },
  quality: {
    pending: ['等待中', 'info'],
    queued: ['已排队', 'info'],
    running: ['运行中', 'primary'],
    completed: ['已完成', 'success'],
    failed: ['失败', 'danger'],
    cancelled: ['已取消', 'info'],
    pass: ['通过', 'success'],
    warn: ['告警', 'warning'],
    fail: ['失败', 'danger'],
    error: ['异常', 'danger'],
  },
  publication: {
    publishing: ['发布中', 'primary'],
    active: ['已发布', 'success'],
    withdrawing: ['撤回中', 'warning'],
    failed: ['失败', 'danger'],
    withdrawn: ['已撤回', 'info'],
    unpublished: ['未发布', 'info'],
  },
};

export function statusPresentation(domain, value) {
  const normalized = String(value || '').toLowerCase();
  const fallback = Object.values(presentations).find((items) => items[normalized])?.[normalized];
  const presentation = presentations[domain]?.[normalized] || fallback;
  if (!presentation) return { label: value || '未知', type: 'info' };
  return { label: presentation[0], type: presentation[1] };
}
