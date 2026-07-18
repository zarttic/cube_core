const SHANGHAI_FORMATTER = new Intl.DateTimeFormat('zh-CN', {
  timeZone: 'Asia/Shanghai',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hourCycle: 'h23',
});

export function formatShanghaiTime(value, fallback = '-') {
  if (value === undefined || value === null || String(value).trim() === '') return fallback;
  const text = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  const date = new Date(text);
  if (Number.isNaN(date.getTime())) return fallback;
  const parts = Object.fromEntries(
    SHANGHAI_FORMATTER.formatToParts(date)
      .filter((part) => part.type !== 'literal')
      .map((part) => [part.type, part.value]),
  );
  return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second}`;
}

export function formatShanghaiRange(start, end, fallback = '-') {
  return `${formatShanghaiTime(start, fallback)} 至 ${formatShanghaiTime(end, fallback)}`;
}
