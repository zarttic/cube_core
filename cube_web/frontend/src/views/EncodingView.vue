<script setup>
import { computed, ref } from 'vue';
import { ElMessage } from 'element-plus';

import LeafletMap from '@/components/LeafletMap.vue';
import { apiPrefixes, requestJson } from '@/api/client';

const activeModule = ref('division');
const loading = ref(false);

const division = ref({
  gridType: 'geohash',
  inputType: 'point',
  level: 6,
  lat: 39.9042,
  lng: 116.4074,
  radius: 10,
});

const encoding = ref({
  operation: 'encode',
  gridType: 'geohash',
  timeGranularity: 'minute',
  timestamp: '',
  decodeInput: '',
  level: 6,
  lat: 39.9042,
  lng: 116.4074,
});

const topology = ref({
  operation: 'neighbors',
  gridType: 'geohash',
  level: 6,
  lat: 39.9042,
  lng: 116.4074,
  neighborK: 1,
  targetLevel: 6,
});

const conversion = ref({
  convertDir: 'code2coord',
  targetLevel: 6,
});

const encodingParts = ref({
  level: '6',
  space: 'wx4g0b',
  time: '24021214',
  full: 'G6-wx4g0b-24021214',
});

const resultRows = ref([]);
const topologyRows = ref([]);
const conversionRows = ref([]);
const markers = ref([{ position: [39.9042, 116.4074], label: '北京样例点' }]);
const drawnCircle = ref(null);
const gridGeometries = ref([]);

const resultTitle = computed(() => {
  if (activeModule.value === 'division') return '生成结果';
  if (activeModule.value === 'encoding') return '编码结果';
  return '元操作结果';
});

const emptyText = computed(() => {
  if (activeModule.value === 'division') return '设置参数并执行演示以查看结果';
  if (activeModule.value === 'encoding') return '设置参数并执行编码/解码操作';
  return '请在左侧分别执行拓扑运算或坐标转换';
});

const gridTypeBadges = {
  geohash: '经纬度',
  mgrs: 'UTM 分带',
  isea4h: '离散全球',
};

const activeGridType = computed(() => {
  if (activeModule.value === 'division') return division.value.gridType;
  if (activeModule.value === 'encoding') return encoding.value.gridType;
  return topology.value.gridType;
});

const activeGridBadge = computed(() => gridTypeBadges[activeGridType.value] || gridTypeBadges.geohash);

const contextualMapHint = computed(() => {
  if (activeModule.value === 'division') {
    if (division.value.gridType === 'mgrs') {
      return division.value.inputType === 'draw'
        ? 'MGRS 为 UTM 带内平面格网；拖拽圈画后会按经纬度面展示覆盖单元并自动聚焦。'
        : 'MGRS 为 UTM 带内平面格网；点击地图后会自动定位到当前分带单元并放大显示。';
    }
    return division.value.inputType === 'draw'
      ? '选择“圈画”后按住拖拽绘制范围'
      : '选择“点”后点击地图选点';
  }
  if (activeModule.value === 'operations') {
    return topology.value.gridType === 'mgrs'
      ? '点击地图选择 MGRS 基准点；结果会按 WGS84 面显示，跨分带时编码会自动切换。'
      : '点击地图选择基准点';
  }
  if (encoding.value.operation === 'decode') {
    return '输入完整编码后执行解码';
  }
  return encoding.value.gridType === 'mgrs'
    ? '点击地图选择 MGRS 编码点；结果会展示当前分带下的空间编码。'
    : '点击地图选择编码点';
});

const legendItems = computed(() => {
  if (activeModule.value === 'encoding') return [];
  const primaryLabel = activeGridType.value === 'mgrs'
    ? 'MGRS 单元'
    : activeModule.value === 'operations'
      ? '中心单元'
      : '选中单元';
  return [
    { colorClass: activeGridType.value === 'mgrs' ? 'mgrs' : 'active', label: primaryLabel },
    { colorClass: 'neighbor', label: '邻接单元' },
    { colorClass: 'covered', label: activeModule.value === 'operations' ? '父单元' : '覆盖区域' },
  ];
});

function gridGeometryStyle(gridType, variant) {
  const isMgrs = gridType === 'mgrs';
  if (variant === 'focus') {
    return isMgrs
      ? { color: '#9141ac', fillColor: '#9141ac', fillOpacity: 0.24, weight: 3 }
      : { color: '#1a5fb4', fillColor: '#1a5fb4', fillOpacity: 0.18, weight: 2 };
  }
  if (variant === 'cover') {
    return isMgrs
      ? { color: '#c061cb', fillColor: '#c061cb', fillOpacity: 0.2, weight: 2.5 }
      : { color: '#f5c211', fillColor: '#f5c211', fillOpacity: 0.16, weight: 2 };
  }
  if (variant === 'base') {
    return isMgrs
      ? { color: '#9141ac', fillColor: '#9141ac', fillOpacity: 0.16, weight: 3 }
      : { color: '#1a5fb4', fillColor: '#1a5fb4', fillOpacity: 0.12, weight: 3 };
  }
  if (variant === 'parent') {
    return { color: '#f5c211', fillColor: '#f5c211', fillOpacity: 0.16, weight: 2 };
  }
  if (variant === 'conversion') {
    return isMgrs
      ? { color: '#9141ac', fillColor: '#9141ac', fillOpacity: 0.18, weight: 3 }
      : { color: '#26a269', fillColor: '#26a269', fillOpacity: 0.16, weight: 3 };
  }
  return { color: '#26a269', fillColor: '#26a269', fillOpacity: 0.16, weight: 2 };
}

function parseBboxText(text) {
  if (!text.trim()) return null;
  const values = text.split(',').map((item) => Number(item.trim()));
  if (values.length !== 4 || values.some(Number.isNaN)) {
    throw new Error('BBox 格式应为 minLon,minLat,maxLon,maxLat');
  }
  return values;
}

function buildBboxFromRadius(lng, lat, radiusKm) {
  const latDelta = radiusKm / 111;
  const lngDelta = radiusKm / (111 * Math.cos((lat * Math.PI) / 180));
  return [lng - lngDelta, lat - latDelta, lng + lngDelta, lat + latDelta];
}

function bboxToPolygonGeometry(bbox) {
  const [minLon, minLat, maxLon, maxLat] = bbox;
  return {
    type: 'Polygon',
    coordinates: [[
      [minLon, minLat],
      [maxLon, minLat],
      [maxLon, maxLat],
      [minLon, maxLat],
      [minLon, minLat],
    ]],
  };
}

function extractGeometryBounds(geometry) {
  const coords = [];
  const walk = (node) => {
    if (!Array.isArray(node)) return;
    if (node.length >= 2 && typeof node[0] === 'number' && typeof node[1] === 'number') {
      coords.push(node);
      return;
    }
    node.forEach(walk);
  };
  walk(geometry.coordinates);
  if (coords.length === 0) return [0, 0, 0, 0];
  const lons = coords.map((point) => point[0]);
  const lats = coords.map((point) => point[1]);
  return [Math.min(...lons), Math.min(...lats), Math.max(...lons), Math.max(...lats)];
}

function setRows(rows) {
  resultRows.value = rows.filter((row) => row.value !== undefined && row.value !== null && row.value !== '');
}

function updateDivisionMarker(label) {
  markers.value = [{
    position: [Number(division.value.lat), Number(division.value.lng)],
    label,
  }];
}

function handleDivisionPointSelected(point) {
  if (activeModule.value !== 'division' || division.value.inputType !== 'point') return;
  division.value.lat = Number(point.lat.toFixed(6));
  division.value.lng = Number(point.lng.toFixed(6));
  drawnCircle.value = null;
  gridGeometries.value = [];
  updateDivisionMarker('选中点');
}

function handleDivisionCircleDrawn(circle) {
  if (activeModule.value !== 'division' || division.value.inputType !== 'draw') return;
  division.value.lat = Number(circle.lat.toFixed(6));
  division.value.lng = Number(circle.lng.toFixed(6));
  division.value.radius = Number(Math.max(circle.radiusKm, 0.1).toFixed(3));
  drawnCircle.value = {
    lat: division.value.lat,
    lng: division.value.lng,
    radiusKm: division.value.radius,
  };
  gridGeometries.value = [];
  updateDivisionMarker(`圈画中心，半径 ${division.value.radius}km`);
}

function handleEncodingPointSelected(point) {
  if (activeModule.value !== 'encoding' || encoding.value.operation === 'decode') return;
  encoding.value.lat = Number(point.lat.toFixed(6));
  encoding.value.lng = Number(point.lng.toFixed(6));
  markers.value = [{
    position: [encoding.value.lat, encoding.value.lng],
    label: '时空编码点',
  }];
  gridGeometries.value = [];
  drawnCircle.value = null;
}

function handleTopologyPointSelected(point) {
  if (activeModule.value !== 'operations') return;
  topology.value.lat = Number(point.lat.toFixed(6));
  topology.value.lng = Number(point.lng.toFixed(6));
  markers.value = [{
    position: [topology.value.lat, topology.value.lng],
    label: '元操作基准点',
  }];
  gridGeometries.value = [];
  drawnCircle.value = null;
}

function handleMapPointSelected(point) {
  handleDivisionPointSelected(point);
  handleEncodingPointSelected(point);
  handleTopologyPointSelected(point);
}

function geometryFromCell(cell) {
  if (cell.geometry) return cell.geometry;
  if (cell.bbox) return bboxToPolygonGeometry(cell.bbox);
  return null;
}

async function geometryItemsFromCodes(topologyPrefix, gridType, codes, style) {
  const uniqueCodes = Array.from(new Set(codes.filter(Boolean))).slice(0, 120);
  if (!uniqueCodes.length) return [];
  const data = await requestJson(`${topologyPrefix}/geometries`, {
    grid_type: gridType,
    codes: uniqueCodes,
    boundary_type: 'polygon',
  });
  return uniqueCodes
    .map((code) => ({
      geometry: data.geometries?.[code],
      label: code,
      ...style,
    }))
    .filter((item) => item.geometry);
}

async function runGridDivision() {
  const { gridPrefix } = apiPrefixes();
  const config = division.value;
  if (config.inputType === 'point') {
    const data = await requestJson(`${gridPrefix}/locate`, {
      grid_type: config.gridType,
      level: config.level,
      point: [Number(config.lng), Number(config.lat)],
    });
    const geometry = geometryFromCell(data.cell);
    gridGeometries.value = geometry ? [{
      geometry,
      label: data.cell.space_code,
      ...gridGeometryStyle(config.gridType, 'focus'),
    }] : [];
    setRows([
      { label: '操作', value: 'locate' },
      { label: '格网类型', value: config.gridType },
      { label: '格网编码', value: data.cell.space_code, code: true },
      { label: '层级', value: String(data.cell.level) },
      ...(config.gridType === 'mgrs' && data.cell.metadata?.zone
        ? [{ label: 'MGRS 分带', value: data.cell.metadata.zone }]
        : []),
      ...(config.gridType === 'mgrs' && data.cell.metadata?.precision !== undefined
        ? [{ label: 'MGRS 精度', value: String(data.cell.metadata.precision) }]
        : []),
      { label: '中心坐标', value: `${data.cell.center[1].toFixed(6)}, ${data.cell.center[0].toFixed(6)}` },
      { label: '时间', value: new Date().toLocaleString('zh-CN') },
    ]);
    return;
  }

  const bbox = buildBboxFromRadius(Number(config.lng), Number(config.lat), Number(config.radius));
  const data = await requestJson(`${gridPrefix}/cover`, {
    grid_type: config.gridType,
    level: config.level,
    cover_mode: 'intersect',
    boundary_type: 'polygon',
    geometry: null,
    bbox,
    crs: 'EPSG:4326',
  });
  gridGeometries.value = data.cells
    .map((cell) => ({
      geometry: geometryFromCell(cell),
      label: cell.space_code,
      ...gridGeometryStyle(config.gridType, 'cover'),
    }))
    .filter((item) => item.geometry);
  setRows([
    { label: '操作', value: '圈画 cover' },
    { label: '格网类型', value: config.gridType },
    { label: '层级', value: String(config.level) },
    { label: '单元数量', value: String(data.statistics?.cell_count || data.cells.length) },
    { label: '示例编码', value: data.cells.slice(0, 8).map((cell) => cell.space_code).join(', '), code: true },
    { label: '时间', value: new Date().toLocaleString('zh-CN') },
  ]);
}

async function runGridEncoding() {
  const { gridPrefix, topologyPrefix, codePrefix } = apiPrefixes();
  const config = encoding.value;
  const timestamp = config.timestamp ? new Date(config.timestamp).toISOString() : new Date().toISOString();

  if (config.operation === 'decode') {
    const parsed = await requestJson(`${codePrefix}/parse`, { st_code: config.decodeInput.trim() });
    await requestJson(`${topologyPrefix}/geometry`, {
      grid_type: parsed.grid_type,
      code: parsed.space_code,
      boundary_type: 'polygon',
    });
    encodingParts.value = {
      level: String(parsed.level),
      space: parsed.space_code,
      time: parsed.time_code,
      full: config.decodeInput.trim(),
    };
    setRows([
      { label: '操作', value: '解码' },
      { label: '完整编码', value: config.decodeInput.trim(), code: true },
      { label: '格网类型', value: parsed.grid_type },
      { label: '空间编码', value: parsed.space_code, code: true },
      { label: '时间编码', value: parsed.time_code },
      { label: '版本', value: parsed.version },
    ]);
    return;
  }

  const located = await requestJson(`${gridPrefix}/locate`, {
    grid_type: config.gridType,
    level: config.level,
    point: [Number(config.lng), Number(config.lat)],
  });
  const codeResp = await requestJson(`${codePrefix}/st`, {
    grid_type: config.gridType,
    level: config.level,
    space_code: located.cell.space_code,
    timestamp,
    time_granularity: config.timeGranularity,
    version: 'v1',
  });
  const parts = codeResp.st_code.split(':');
  encodingParts.value = {
    level: parts[1] || String(config.level),
    space: parts[2] || located.cell.space_code,
    time: parts[3] || '-',
    full: codeResp.st_code,
  };
  setRows([
    { label: '操作', value: '点选编码' },
    { label: '格网类型', value: config.gridType },
    { label: '点选坐标', value: `${Number(config.lat).toFixed(6)}, ${Number(config.lng).toFixed(6)}` },
    { label: '空间编码', value: located.cell.space_code, code: true },
    ...(config.gridType === 'mgrs' && located.cell.metadata?.zone
      ? [{ label: 'MGRS 分带', value: located.cell.metadata.zone }]
      : []),
    { label: '完整时空编码', value: codeResp.st_code, code: true },
    { label: '时间粒度', value: config.timeGranularity },
    { label: '时间', value: new Date(timestamp).toLocaleString('zh-CN') },
  ]);
}

async function resolveTopologySelection(gridPrefix) {
  const config = topology.value;
  const located = await requestJson(`${gridPrefix}/locate`, {
    grid_type: config.gridType,
    level: config.level,
    point: [Number(config.lng), Number(config.lat)],
  });
  return {
    baseCodes: [located.cell.space_code],
    selectionPoint: [Number(config.lng), Number(config.lat)],
  };
}

async function appendConversionRows(rows, ctx) {
  const { gridPrefix, topologyPrefix, representativeCode, selectionPoint } = ctx;
  const config = topology.value;
  const conversionConfig = conversion.value;
  if (conversionConfig.convertDir === 'code2coord') {
    if (!representativeCode) {
      rows.push({ label: '转换方向', value: '编码 -> 坐标' });
      rows.push({ label: '转换结果', value: '-' });
      return;
    }
    const bboxResp = await requestJson(`${topologyPrefix}/geometry`, {
      grid_type: config.gridType,
      code: representativeCode,
      boundary_type: 'bbox',
    });
    const bbox = bboxResp.geometry.bbox;
    rows.push({ label: '转换方向', value: '编码 -> 坐标' });
    rows.push({ label: '转换结果', value: `${((bbox[1] + bbox[3]) / 2).toFixed(6)}, ${((bbox[0] + bbox[2]) / 2).toFixed(6)}` });
    return;
  }

  const [lng, lat] = selectionPoint || [116.4074, 39.9042];
  const locateResp = await requestJson(`${gridPrefix}/locate`, {
    grid_type: config.gridType,
    level: conversionConfig.targetLevel,
    point: [lng, lat],
  });
  rows.push({ label: '转换方向', value: '坐标 -> 编码' });
  rows.push({ label: '转换层级', value: String(conversionConfig.targetLevel) });
  rows.push({ label: '转换结果', value: locateResp.cell.space_code, code: true });
}

function setOperationRows(target, rows) {
  target.value = rows.filter((row) => row.value !== undefined && row.value !== null && row.value !== '');
}

async function runTopologyOperation() {
  loading.value = true;
  topologyRows.value = [];
  gridGeometries.value = [];
  try {
    const { gridPrefix, topologyPrefix } = apiPrefixes();
    const config = topology.value;
    const selection = await resolveTopologySelection(gridPrefix);
    let baseCodes = selection.baseCodes;
    const originalBaseCount = baseCodes.length;
    if (baseCodes.length > 120) baseCodes = baseCodes.slice(0, 120);

    const resultCodes = new Set();
    const failedCodes = [];
    if (config.operation === 'neighbors') {
      for (const code of baseCodes) {
        try {
          const data = await requestJson(`${topologyPrefix}/neighbors`, { grid_type: config.gridType, code, k: config.neighborK });
          data.result_codes.forEach((item) => resultCodes.add(item));
        } catch {
          failedCodes.push(code);
        }
      }
    } else if (config.operation === 'parent') {
      for (const code of baseCodes) {
        try {
          const data = await requestJson(`${topologyPrefix}/parent`, { grid_type: config.gridType, code });
          resultCodes.add(data.parent_code);
        } catch {
          failedCodes.push(code);
        }
      }
    } else if (config.operation === 'children') {
      for (const code of baseCodes) {
        try {
          const data = await requestJson(`${topologyPrefix}/children`, {
            grid_type: config.gridType,
            code,
            target_level: config.targetLevel,
          });
          data.child_codes.forEach((item) => resultCodes.add(item));
        } catch {
          failedCodes.push(code);
        }
      }
    }

    const rows = [
      { label: '格网类型', value: config.gridType },
      { label: '点选坐标', value: `${Number(config.lat).toFixed(6)}, ${Number(config.lng).toFixed(6)}` },
      { label: '基准编码数', value: String(originalBaseCount) },
      { label: '运算类型', value: config.operation },
      { label: '输入编码样例', value: baseCodes.slice(0, 8).join(', '), code: true },
      { label: '结果数量', value: String(resultCodes.size) },
      { label: '结果编码样例', value: Array.from(resultCodes).slice(0, 8).join(', '), code: true },
    ];
    if (config.operation === 'neighbors') rows.splice(5, 0, { label: 'k', value: String(config.neighborK) });
    if (config.operation === 'children') rows.splice(5, 0, { label: '目标层级', value: String(config.targetLevel) });
    if (failedCodes.length) rows.push({ label: '跳过编码数', value: String(failedCodes.length) });
    setOperationRows(topologyRows, rows);
    const resultStyle = config.operation === 'parent'
      ? gridGeometryStyle(config.gridType, 'parent')
      : gridGeometryStyle(config.gridType, 'result');
    const baseGeometryItems = await geometryItemsFromCodes(topologyPrefix, config.gridType, baseCodes, gridGeometryStyle(config.gridType, 'base'));
    const resultGeometryItems = await geometryItemsFromCodes(topologyPrefix, config.gridType, Array.from(resultCodes), resultStyle);
    gridGeometries.value = [...resultGeometryItems, ...baseGeometryItems];
    ElMessage.success('拓扑运算完成');
  } catch (error) {
    ElMessage.error(error.message);
  } finally {
    loading.value = false;
  }
}

async function runCoordinateConversion() {
  loading.value = true;
  conversionRows.value = [];
  gridGeometries.value = [];
  try {
    const { gridPrefix, topologyPrefix } = apiPrefixes();
    const config = topology.value;
    const selection = await resolveTopologySelection(gridPrefix);
    const baseCode = selection.baseCodes[0];
    const rows = [
      { label: '格网类型', value: config.gridType },
      { label: '点选坐标', value: `${Number(config.lat).toFixed(6)}, ${Number(config.lng).toFixed(6)}` },
      { label: '基准编码', value: baseCode, code: true },
    ];
    await appendConversionRows(rows, {
      gridPrefix,
      topologyPrefix,
      representativeCode: baseCode,
      selectionPoint: selection.selectionPoint,
    });
    setOperationRows(conversionRows, rows);
    const renderCodes = conversion.value.convertDir === 'code2coord'
      ? [baseCode]
      : [rows.find((row) => row.label === '转换结果')?.value];
    gridGeometries.value = await geometryItemsFromCodes(topologyPrefix, config.gridType, renderCodes, gridGeometryStyle(config.gridType, 'conversion'));
    ElMessage.success('坐标转换完成');
  } catch (error) {
    ElMessage.error(error.message);
  } finally {
    loading.value = false;
  }
}

async function runDemo() {
  loading.value = true;
  resultRows.value = [];
  try {
    if (activeModule.value === 'division') await runGridDivision();
    if (activeModule.value === 'encoding') await runGridEncoding();
    ElMessage.success('操作完成');
  } catch (error) {
    ElMessage.error(error.message);
  } finally {
    loading.value = false;
  }
}
</script>

<template>
  <section>
    <section class="module-nav">
      <div class="container">
        <div class="module-tabs">
          <button class="module-tab" :class="{ active: activeModule === 'division' }" @click="activeModule = 'division'">格网划分</button>
          <button class="module-tab" :class="{ active: activeModule === 'encoding' }" @click="activeModule = 'encoding'">时空编码</button>
          <button class="module-tab" :class="{ active: activeModule === 'operations' }" @click="activeModule = 'operations'">元操作</button>
        </div>
      </div>
    </section>

    <main class="main-content-area">
      <div class="container">
        <div class="module-content active">
          <div class="workspace">
            <div class="workspace-sidebar">
              <div class="config-panel">
                <template v-if="activeModule === 'division'">
                  <h3>参数配置</h3>
                  <div class="form-group">
                    <label>输入类型</label>
                    <select v-model="division.inputType" class="form-select">
                      <option value="point">点</option>
                      <option value="draw">圈画</option>
                    </select>
                  </div>
                  <div class="form-group">
                    <label>格网类型</label>
                    <div class="radio-group">
                      <label class="radio-label"><input v-model="division.gridType" type="radio" value="geohash"><span class="radio-custom"></span><span>Geohash 经纬度格网</span></label>
                      <label class="radio-label"><input v-model="division.gridType" type="radio" value="mgrs"><span class="radio-custom"></span><span>MGRS 平面格网</span></label>
                      <label class="radio-label"><input v-model="division.gridType" type="radio" value="isea4h"><span class="radio-custom"></span><span>ISEA4H 六边形格网</span></label>
                    </div>
                  </div>
                  <div class="form-group">
                    <label>精度/层级</label>
                    <div class="range-group">
                      <input v-model.number="division.level" type="range" min="1" max="12" class="form-range">
                      <span class="range-value">{{ division.level }}级</span>
                    </div>
                  </div>
                </template>

                <template v-if="activeModule === 'encoding'">
                  <h3>编码设置</h3>
                  <div class="form-group">
                    <label>操作类型</label>
                    <div class="radio-group">
                      <label class="radio-label"><input v-model="encoding.operation" type="radio" value="encode"><span class="radio-custom"></span><span>编码 (坐标→编码)</span></label>
                      <label class="radio-label"><input v-model="encoding.operation" type="radio" value="decode"><span class="radio-custom"></span><span>解码 (编码→坐标)</span></label>
                    </div>
                  </div>
                  <div class="form-group">
                    <label>格网类型</label>
                    <select v-model="encoding.gridType" class="form-select">
                      <option value="geohash">Geohash 经纬度格网</option>
                      <option value="mgrs">MGRS 平面格网</option>
                      <option value="isea4h">ISEA4H 六边形格网</option>
                    </select>
                  </div>
                  <div class="form-group">
                    <label>时间粒度</label>
                    <select v-model="encoding.timeGranularity" class="form-select">
                      <option value="second">秒</option>
                      <option value="minute">分钟</option>
                      <option value="hour">小时</option>
                      <option value="day">日</option>
                    </select>
                  </div>
                  <div class="form-group">
                    <label>时间戳</label>
                    <input v-model="encoding.timestamp" type="datetime-local" class="form-input">
                  </div>
                  <div v-if="encoding.operation !== 'decode'" class="form-group">
                    <label>点选坐标</label>
                    <div class="task-note">
                      <span>请在右侧地图点击选择编码点。</span>
                    </div>
                    <div class="result-item">
                      <div class="result-label">当前点位</div>
                      <div class="result-value">{{ Number(encoding.lat).toFixed(6) }}, {{ Number(encoding.lng).toFixed(6) }}</div>
                    </div>
                  </div>
                  <div v-else class="form-group">
                    <label>格网编码</label>
                    <input v-model="encoding.decodeInput" type="text" class="form-input" placeholder="例如: gh:6:wx4g0b:202603091530:v1">
                  </div>
                </template>

                <template v-if="activeModule === 'operations'">
                  <h3>元操作输入</h3>
                  <section class="operation-section">
                    <div class="form-group">
                      <label>格网类型</label>
                      <div class="radio-group">
                        <label class="radio-label"><input v-model="topology.gridType" type="radio" value="geohash"><span class="radio-custom"></span><span>Geohash 经纬度格网</span></label>
                        <label class="radio-label"><input v-model="topology.gridType" type="radio" value="mgrs"><span class="radio-custom"></span><span>MGRS 平面格网</span></label>
                        <label class="radio-label"><input v-model="topology.gridType" type="radio" value="isea4h"><span class="radio-custom"></span><span>ISEA4H 六边形格网</span></label>
                      </div>
                    </div>
                    <div class="form-group">
                      <label>基准层级</label>
                      <div class="range-group">
                        <input v-model.number="topology.level" type="range" min="1" max="12" class="form-range">
                        <span class="range-value">{{ topology.level }}级</span>
                      </div>
                    </div>
                    <div class="form-group">
                      <label>基准坐标</label>
                      <div class="task-note">
                        <span>请在右侧地图点击选择元操作基准点。</span>
                      </div>
                      <div class="result-item">
                        <div class="result-label">当前点位</div>
                        <div class="result-value">{{ Number(topology.lat).toFixed(6) }}, {{ Number(topology.lng).toFixed(6) }}</div>
                      </div>
                    </div>
                  </section>

                  <div class="operation-columns">
                    <section class="operation-section">
                      <h3>拓扑运算</h3>
                      <div class="form-group">
                        <label>运算类型</label>
                        <select v-model="topology.operation" class="form-select">
                          <option value="neighbors">邻接单元计算</option>
                          <option value="parent">父单元推导</option>
                          <option value="children">子单元生成</option>
                        </select>
                      </div>
                      <div v-if="topology.operation === 'neighbors'" class="form-group">
                        <label>邻域阶数 (k)</label>
                        <input v-model.number="topology.neighborK" type="number" class="form-input" min="1" max="3">
                      </div>
                      <div v-if="topology.operation === 'children'" class="form-group">
                        <label>子单元目标层级</label>
                        <input v-model.number="topology.targetLevel" type="number" class="form-input" min="1" max="12">
                      </div>
                      <div class="form-group action-buttons compact">
                        <button class="btn btn-secondary" type="button" @click="topologyRows = []">清空拓扑结果</button>
                        <button class="btn btn-primary" type="button" :disabled="loading" @click="runTopologyOperation">执行拓扑运算</button>
                      </div>
                    </section>

                    <section class="operation-section">
                      <h3>坐标转换</h3>
                      <div class="form-group">
                        <label>转换方向</label>
                        <select v-model="conversion.convertDir" class="form-select">
                          <option value="code2coord">编码 → 坐标</option>
                          <option value="coord2code">坐标 → 编码</option>
                        </select>
                      </div>
                      <div class="form-group">
                        <label>转换目标层级</label>
                        <input v-model.number="conversion.targetLevel" type="number" class="form-input" min="1" max="12">
                      </div>
                      <div class="form-group action-buttons compact">
                        <button class="btn btn-secondary" type="button" @click="conversionRows = []">清空转换结果</button>
                        <button class="btn btn-primary" type="button" :disabled="loading" @click="runCoordinateConversion">执行坐标转换</button>
                      </div>
                    </section>
                  </div>
                </template>

                <div v-if="activeModule !== 'operations'" class="form-group action-buttons">
                  <button class="btn btn-secondary" type="button" @click="resultRows = []">重置</button>
                  <button class="btn btn-primary" type="button" :disabled="loading" @click="runDemo">
                    {{ activeModule === 'encoding' ? '执行编码' : '执行演示' }}
                  </button>
                </div>
              </div>
            </div>

            <div class="workspace-main">
              <div class="map-panel">
                <div class="panel-header">
                  <div class="panel-header-main">
                    <h3>{{ activeModule === 'division' ? '地图可视化' : activeModule === 'encoding' ? '编码地图展示' : '拓扑关系地图展示' }}</h3>
                    <span class="grid-type-badge" :class="`grid-type-badge--${activeGridType}`">{{ activeGridBadge }}</span>
                  </div>
                  <span class="map-hint">
                    {{ contextualMapHint }}
                  </span>
                </div>
                <LeafletMap
                  :markers="markers"
                  :geometries="activeModule === 'division' || activeModule === 'operations' ? gridGeometries : []"
                  :circle="activeModule === 'division' ? drawnCircle : null"
                  :interaction-mode="activeModule === 'division' ? division.inputType : activeModule === 'encoding' && encoding.operation !== 'decode' ? 'point' : activeModule === 'operations' ? 'point' : 'none'"
                  @point-selected="handleMapPointSelected"
                  @circle-drawn="handleDivisionCircleDrawn"
                />
                <div v-if="activeModule !== 'encoding'" class="visual-legend">
                  <div v-for="item in legendItems" :key="`${activeModule}-${item.colorClass}-${item.label}`" class="legend-item">
                    <span class="legend-color" :class="item.colorClass"></span>
                    <span>{{ item.label }}</span>
                  </div>
                </div>
                <div v-else class="encoding-structure compact">
                  <div class="encoding-part level-part"><div class="part-label">层级</div><div class="part-value">{{ encodingParts.level }}</div></div>
                  <div class="encoding-plus">+</div>
                  <div class="encoding-part space-part"><div class="part-label">空间</div><div class="part-value">{{ encodingParts.space }}</div></div>
                  <div class="encoding-plus">+</div>
                  <div class="encoding-part time-part"><div class="part-label">时间</div><div class="part-value">{{ encodingParts.time }}</div></div>
                  <div class="encoding-equals">=</div>
                  <div class="encoding-part result-part"><div class="part-label">完整编码</div><div class="part-value">{{ encodingParts.full }}</div></div>
                </div>
              </div>
            </div>

            <div class="workspace-result">
              <div class="result-panel">
                <h3>{{ resultTitle }}</h3>
                <div class="results-content">
                  <template v-if="activeModule === 'operations'">
                    <div class="result-section">
                      <h4 class="result-section-title">拓扑运算</h4>
                      <template v-if="topologyRows.length">
                        <div v-for="row in topologyRows" :key="`topology-${row.label}`" class="result-item">
                          <div class="result-label">{{ row.label }}</div>
                          <div class="result-value" :class="{ code: row.code }">{{ row.value }}</div>
                        </div>
                      </template>
                      <div v-else class="empty-state compact">
                        <p>选择点位并执行拓扑运算</p>
                      </div>
                    </div>
                    <div class="result-section">
                      <h4 class="result-section-title">坐标转换</h4>
                      <template v-if="conversionRows.length">
                        <div v-for="row in conversionRows" :key="`conversion-${row.label}`" class="result-item">
                          <div class="result-label">{{ row.label }}</div>
                          <div class="result-value" :class="{ code: row.code }">{{ row.value }}</div>
                        </div>
                      </template>
                      <div v-else class="empty-state compact">
                        <p>选择点位并执行坐标转换</p>
                      </div>
                    </div>
                  </template>
                  <template v-else-if="resultRows.length">
                    <div v-for="row in resultRows" :key="row.label" class="result-item">
                      <div class="result-label">{{ row.label }}</div>
                      <div class="result-value" :class="{ code: row.code }">{{ row.value }}</div>
                    </div>
                  </template>
                  <div v-else class="empty-state">
                    <p>{{ emptyText }}</p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </main>
  </section>
</template>
