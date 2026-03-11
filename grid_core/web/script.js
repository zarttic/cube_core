// 全局状态
let currentPage = 'grid-division';
let isProcessing = false;
let maps = {}; // 存储所有地图实例
let mapLayers = {}; // 存储地图图层
let drawItems = null;
let drawnGeometry = null;

// 初始化
document.addEventListener('DOMContentLoaded', () => {
    initModuleTabs();
    initFormHandlers();
    initDemoHandlers();
    initMaps();
    setDefaultTimestamp();
    initGridDemos();
    console.log('系统初始化完成');
});

// 初始化模块标签切换
function initModuleTabs() {
    const moduleTabs = document.querySelectorAll('.module-tab');

    moduleTabs.forEach(tab => {
        tab.addEventListener('click', () => {
            const moduleId = tab.getAttribute('data-module');
            if (!moduleId) return;

            // 更新标签状态
            moduleTabs.forEach(t => t.classList.remove('active'));
            tab.classList.add('active');

            // 切换模块内容
            document.querySelectorAll('.module-content').forEach(content => {
                content.classList.remove('active');
            });
            const targetModule = document.getElementById(moduleId);
            if (targetModule) {
                targetModule.classList.add('active');
            }

            currentPage = moduleId;

            // 延迟刷新地图
            setTimeout(() => {
                invalidateMapSize();
            }, 100);
        });
    });
}

// 刷新地图尺寸
function invalidateMapSize() {
    Object.values(maps).forEach(map => {
        if (map && map.invalidateSize) {
            map.invalidateSize();
        }
    });
}

// 初始化所有地图
function initMaps() {
    setTimeout(() => {
        // 格网划分地图
        maps.division = createMap('divisionMap', [39.9042, 116.4074], 10);
        if (maps.division && typeof L.Control.Draw !== 'undefined') {
            drawItems = new L.FeatureGroup().addTo(maps.division);
            const drawControl = new L.Control.Draw({
                draw: {
                    marker: false,
                    circle: false,
                    circlemarker: false,
                    polyline: false,
                    polygon: true,
                    rectangle: true,
                },
                edit: {
                    featureGroup: drawItems,
                    remove: true,
                },
            });
            maps.division.addControl(drawControl);

            maps.division.on(L.Draw.Event.CREATED, (e) => {
                drawItems.clearLayers();
                drawItems.addLayer(e.layer);
                drawnGeometry = e.layer.toGeoJSON().geometry;
                const geometryJson = document.getElementById('geometryJson');
                if (geometryJson) {
                    geometryJson.value = JSON.stringify(drawnGeometry);
                }
            });

            maps.division.on(L.Draw.Event.EDITED, () => {
                const layer = drawItems.getLayers()[0];
                drawnGeometry = layer ? layer.toGeoJSON().geometry : null;
                const geometryJson = document.getElementById('geometryJson');
                if (geometryJson) {
                    geometryJson.value = drawnGeometry ? JSON.stringify(drawnGeometry) : '';
                }
            });

            maps.division.on(L.Draw.Event.DELETED, () => {
                drawnGeometry = null;
                const geometryJson = document.getElementById('geometryJson');
                if (geometryJson) {
                    geometryJson.value = '';
                }
            });
        }

        // 时空编码地图
        maps.encoding = createMap('encodingMap', [39.9042, 116.4074], 10);

        // 拓扑运算地图
        maps.topology = createMap('topologyMap', [39.9042, 116.4074], 10);

        // 光学遥感地图
        maps.optical = createMap('opticalMap', [39.9042, 116.4074], 8);

        // 碳卫星地图
        maps.carbon = createMap('carbonMap', [39.9042, 116.4074], 8);

        // 雷达地图
        maps.radar = createMap('radarMap', [39.9042, 116.4074], 8);

        // 产品地图
        maps.product = createMap('productMap', [39.9042, 116.4074], 8);

        // 添加点击事件
        Object.keys(maps).forEach(key => {
            if (maps[key]) {
                maps[key].on('click', function(e) {
                    const lat = e.latlng.lat.toFixed(4);
                    const lng = e.latlng.lng.toFixed(4);

                    // 更新坐标输入框
                    const latInput = document.getElementById('lat');
                    const lngInput = document.getElementById('lng');
                    if (latInput && lngInput && currentPage === 'grid-division') {
                        latInput.value = lat;
                        lngInput.value = lng;

                        // 添加标记
                        addMarkerToMap(maps.division, parseFloat(lat), parseFloat(lng), '选中位置');
                    }
                });
            }
        });
    }, 100);
}

// 创建地图
function createMap(containerId, center, zoom) {
    const container = document.getElementById(containerId);
    if (!container) return null;

    const map = L.map(containerId, {
        center: center,
        zoom: zoom,
        zoomControl: false
    });

    // 添加缩放控件到右下角
    L.control.zoom({
        position: 'bottomright'
    }).addTo(map);

    // 添加 OpenStreetMap 底图
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '© OpenStreetMap contributors',
        maxZoom: 19
    }).addTo(map);

    // 存储图层组
    mapLayers[containerId] = L.layerGroup().addTo(map);

    return map;
}

// 添加标记到地图
function addMarkerToMap(map, lat, lng, title) {
    if (!map) return;

    L.marker([lat, lng])
        .addTo(map)
        .bindPopup(title)
        .openPopup();
}

// 清除地图图层
function clearMapLayers(mapId) {
    if (mapLayers[mapId]) {
        mapLayers[mapId].clearLayers();
    }
}

// 绘制 Geohash 矩形
function drawGeohashOnMap(map, lat, lng, level, isActive = true) {
    if (!map) return;

    const precision = Math.pow(2, level) * 0.001;
    const bounds = [
        [lat - precision, lng - precision * 2],
        [lat + precision, lng + precision * 2]
    ];

    const rect = L.rectangle(bounds, {
        className: isActive ? 'grid-overlay-rect active' : 'grid-overlay-rect',
        weight: 2,
        opacity: 0.8,
        fillOpacity: 0.3
    }).addTo(map);

    // 添加标签
    L.marker([lat, lng], {
        icon: L.divIcon({
            className: 'grid-label-marker',
            html: `<div style="background:rgba(26,95,180,0.9);color:white;padding:2px 6px;border-radius:3px;font-size:10px;">L${level}</div>`,
            iconSize: [40, 20],
            iconAnchor: [20, 10]
        })
    }).addTo(map);

    return rect;
}

// 绘制 H3 六边形
function drawH3HexagonOnMap(map, lat, lng, resolution, isActive = true) {
    if (!map || typeof h3 === 'undefined') {
        return drawGeohashOnMap(map, lat, lng, resolution, isActive);
    }

    try {
        const h3Index = h3.latLngToCell(lat, lng, resolution);
        const boundary = h3.cellToBoundary(h3Index);
        const latLngs = boundary.map(coord => [coord[0], coord[1]]);

        const hexagon = L.polygon(latLngs, {
            className: isActive ? 'h3-hexagon active' : 'h3-hexagon',
            weight: 2,
            opacity: 0.8,
            fillOpacity: 0.3
        }).addTo(map);

        const center = h3.cellToLatLng(h3Index);
        L.marker([center[0], center[1]], {
            icon: L.divIcon({
                className: 'h3-label-marker',
                html: `<div style="background:rgba(145,65,172,0.9);color:white;padding:2px 4px;border-radius:3px;font-size:9px;white-space:nowrap;">${h3Index.substring(0, 8)}...</div>`,
                iconSize: [60, 20],
                iconAnchor: [30, 10]
            })
        }).addTo(map);

        return { polygon: hexagon, h3Index };
    } catch (error) {
        console.error('H3 绘制失败:', error);
        return null;
    }
}

// 绘制 H3 邻接六边形
function drawH3Neighbors(map, h3Index, k = 1) {
    if (!map || typeof h3 === 'undefined' || !h3Index) return [];

    try {
        const neighbors = h3.gridDisk(h3Index, k);

        neighbors.forEach(neighborIndex => {
            if (neighborIndex !== h3Index) {
                const boundary = h3.cellToBoundary(neighborIndex);
                const latLngs = boundary.map(coord => [coord[0], coord[1]]);

                L.polygon(latLngs, {
                    className: 'h3-hexagon neighbor',
                    weight: 1.5,
                    opacity: 0.6,
                    fillOpacity: 0.2
                }).addTo(map);
            }
        });
    } catch (error) {
        console.error('H3 邻接绘制失败:', error);
    }
}

// 生成邻接 Geohash 单元
function drawNeighborGeohashes(map, lat, lng, level) {
    if (!map) return;

    const precision = Math.pow(2, level) * 0.001;
    const directions = [
        { lat: 1, lng: 0 }, { lat: 1, lng: 1 }, { lat: 0, lng: 1 }, { lat: -1, lng: 1 },
        { lat: -1, lng: 0 }, { lat: -1, lng: -1 }, { lat: 0, lng: -1 }, { lat: 1, lng: -1 }
    ];

    directions.forEach(dir => {
        const nLat = lat + dir.lat * precision * 2;
        const nLng = lng + dir.lng * precision * 2;

        const bounds = [
            [nLat - precision, nLng - precision * 2],
            [nLat + precision, nLng + precision * 2]
        ];

        L.rectangle(bounds, {
            className: 'grid-overlay-rect neighbor',
            weight: 1.5,
            opacity: 0.6,
            fillOpacity: 0.2
        }).addTo(map);
    });
}

// 表单处理
function initFormHandlers() {
    // 层级滑块
    const levelSlider = document.getElementById('gridLevel');
    if (levelSlider) {
        levelSlider.addEventListener('input', (e) => {
            document.getElementById('levelValue').textContent = e.target.value + '级';
        });
    }

    // 编码操作切换
    const encodeOps = document.querySelectorAll('input[name="encodeOp"]');
    encodeOps.forEach(op => {
        op.addEventListener('change', (e) => {
            const encodeInput = document.querySelector('.encode-input');
            const decodeInput = document.querySelector('.decode-input');

            if (encodeInput && decodeInput) {
                if (e.target.value === 'decode') {
                    encodeInput.style.display = 'none';
                    decodeInput.style.display = 'block';
                } else {
                    encodeInput.style.display = 'block';
                    decodeInput.style.display = 'none';
                }
            }
        });
    });

    // 数据源切换 - 光学遥感
    initDataSourceToggle('opticalDataSource', 'loadedDataQueueGroup', 'localFileGroup');

    // 数据源切换 - 碳卫星
    initDataSourceToggle('carbonDataSource', 'carbonQueueGroup', 'carbonLocalGroup');

    // 数据源切换 - 雷达
    initDataSourceToggle('radarDataSource', 'radarQueueGroup', 'radarLocalGroup');

    // 数据源切换 - 信息产品
    initDataSourceToggle('productDataSource', 'productQueueGroup', 'productLocalGroup');

    // 队列项选择
    initQueueItemSelection();

    // 文件上传显示
    initFileUpload('opticalFile', 'opticalFileName');
    initFileUpload('carbonFile', 'carbonFileName');
    initFileUpload('radarFile', 'radarFileName');
    initFileUpload('productFile', 'productFileName');
    initFileUpload('qcFile', 'qcFileName');

    const clearDrawBtn = document.getElementById('clearDrawBtn');
    if (clearDrawBtn) {
        clearDrawBtn.addEventListener('click', clearDivisionDrawings);
    }
}

// 初始化数据源切换
function initDataSourceToggle(selectId, queueGroupId, localGroupId) {
    const select = document.getElementById(selectId);
    const queueGroup = document.getElementById(queueGroupId);
    const localGroup = document.getElementById(localGroupId);

    if (select && queueGroup && localGroup) {
        select.addEventListener('change', (e) => {
            if (e.target.value === 'loaded') {
                queueGroup.style.display = 'block';
                localGroup.style.display = 'none';
            } else {
                queueGroup.style.display = 'none';
                localGroup.style.display = 'block';
            }
        });
    }
}

// 初始化队列项选择
function initQueueItemSelection() {
    const queueLists = document.querySelectorAll('.queue-list');
    queueLists.forEach(list => {
        list.addEventListener('click', (e) => {
            const item = e.target.closest('.queue-item');
            if (item) {
                // 移除同组其他选中状态
                list.querySelectorAll('.queue-item').forEach(i => i.classList.remove('selected'));
                // 添加当前选中状态
                item.classList.add('selected');
                // 获取批次ID
                const batchId = item.getAttribute('data-batch-id');
                console.log('选中数据批次:', batchId);
            }
        });
    });
}

// 刷新数据队列
function refreshDataQueue(type) {
    console.log(`刷新 ${type} 数据队列...`);
    // 模拟从载入子系统获取最新数据队列
    const btn = event.target.closest('.btn-refresh');
    if (btn) {
        btn.style.opacity = '0.6';
        btn.innerHTML = '<svg style="width:12px;height:12px;" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/></svg> 刷新中...';

        setTimeout(() => {
            btn.style.opacity = '1';
            btn.innerHTML = '<svg style="width:12px;height:12px;" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M23 4v6h-6M1 20v-6h6M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg> 刷新队列';
            console.log(`${type} 数据队列已刷新`);
        }, 1000);
    }
}

// 文件上传初始化
function initFileUpload(inputId, nameId) {
    const input = document.getElementById(inputId);
    const nameSpan = document.getElementById(nameId);

    if (input && nameSpan) {
        input.addEventListener('change', (e) => {
            const files = e.target.files;
            if (files.length > 0) {
                nameSpan.textContent = files.length > 1 ?
                    `${files.length} 个文件` : files[0].name;
            }
        });
    }
}

// 演示处理
function initDemoHandlers() {
    const gridTypes = document.querySelectorAll('input[name="gridType"]');
    gridTypes.forEach(type => {
        type.addEventListener('change', (e) => {
            updateGridTypeDisplay(e.target.value);
        });
    });
}

// 更新格网类型显示
function updateGridTypeDisplay(type) {
    const typeNames = {
        'geohash': 'Geohash 经纬度格网',
        'mgrs': 'MGRS 平面格网',
        'isea4h': 'ISEA4H 六边形格网'
    };
    console.log('格网类型切换为:', typeNames[type]);
}

// 设置默认时间戳
function setDefaultTimestamp() {
    const timeInput = document.getElementById('encodeTime');
    if (timeInput) {
        const now = new Date();
        const localISOTime = new Date(now.getTime() - now.getTimezoneOffset() * 60000)
            .toISOString()
            .slice(0, 16);
        timeInput.value = localISOTime;
    }
}

// 初始化首页格网演示
function initGridDemos() {
    // Geohash 演示
    const geohashDemo = document.getElementById('geohashDemo');
    if (geohashDemo) {
        geohashDemo.innerHTML = createGeohashSVG();
    }

    // MGRS 演示
    const mgrsDemo = document.getElementById('mgrsDemo');
    if (mgrsDemo) {
        mgrsDemo.innerHTML = createMGRSSVG();
    }

    // 六边形演示
    const hexagonDemo = document.getElementById('hexagonDemo');
    if (hexagonDemo) {
        hexagonDemo.innerHTML = createHexagonSVG();
    }
}

// 创建 Geohash SVG 演示
function createGeohashSVG() {
    return `<svg viewBox="0 0 120 120" width="120" height="120">
        <defs>
            <linearGradient id="geohashGrad" x1="0%" y1="0%" x2="100%" y2="100%">
                <stop offset="0%" style="stop-color:#1a5fb4;stop-opacity:0.8" />
                <stop offset="100%" style="stop-color:#3584e4;stop-opacity:0.6" />
            </linearGradient>
        </defs>
        <rect x="10" y="10" width="25" height="25" fill="url(#geohashGrad)" stroke="#1a5fb4" stroke-width="1"/>
        <rect x="35" y="10" width="25" height="25" fill="url(#geohashGrad)" stroke="#1a5fb4" stroke-width="1" opacity="0.7"/>
        <rect x="60" y="10" width="25" height="25" fill="url(#geohashGrad)" stroke="#1a5fb4" stroke-width="1" opacity="0.5"/>
        <rect x="85" y="10" width="25" height="25" fill="url(#geohashGrad)" stroke="#1a5fb4" stroke-width="1" opacity="0.3"/>
        <rect x="10" y="35" width="25" height="25" fill="url(#geohashGrad)" stroke="#1a5fb4" stroke-width="1" opacity="0.7"/>
        <rect x="35" y="35" width="25" height="25" fill="url(#geohashGrad)" stroke="#1a5fb4" stroke-width="2"/>
        <rect x="60" y="35" width="25" height="25" fill="url(#geohashGrad)" stroke="#1a5fb4" stroke-width="1" opacity="0.7"/>
        <rect x="85" y="35" width="25" height="25" fill="url(#geohashGrad)" stroke="#1a5fb4" stroke-width="1" opacity="0.5"/>
        <rect x="10" y="60" width="25" height="25" fill="url(#geohashGrad)" stroke="#1a5fb4" stroke-width="1" opacity="0.5"/>
        <rect x="35" y="60" width="25" height="25" fill="url(#geohashGrad)" stroke="#1a5fb4" stroke-width="1" opacity="0.7"/>
        <rect x="60" y="60" width="25" height="25" fill="url(#geohashGrad)" stroke="#1a5fb4" stroke-width="1" opacity="0.5"/>
        <rect x="85" y="60" width="25" height="25" fill="url(#geohashGrad)" stroke="#1a5fb4" stroke-width="1" opacity="0.3"/>
        <rect x="10" y="85" width="25" height="25" fill="url(#geohashGrad)" stroke="#1a5fb4" stroke-width="1" opacity="0.3"/>
        <rect x="35" y="85" width="25" height="25" fill="url(#geohashGrad)" stroke="#1a5fb4" stroke-width="1" opacity="0.5"/>
        <rect x="60" y="85" width="25" height="25" fill="url(#geohashGrad)" stroke="#1a5fb4" stroke-width="1" opacity="0.3"/>
        <rect x="85" y="85" width="25" height="25" fill="url(#geohashGrad)" stroke="#1a5fb4" stroke-width="1" opacity="0.2"/>
    </svg>`;
}

// 创建 MGRS SVG 演示
function createMGRSSVG() {
    return `<svg viewBox="0 0 120 120" width="120" height="120">
        <defs>
            <linearGradient id="mgrsGrad" x1="0%" y1="0%" x2="100%" y2="100%">
                <stop offset="0%" style="stop-color:#9141ac;stop-opacity:0.8" />
                <stop offset="100%" style="stop-color:#c061cb;stop-opacity:0.6" />
            </linearGradient>
        </defs>
        <rect x="5" y="5" width="50" height="50" fill="url(#mgrsGrad)" stroke="#9141ac" stroke-width="1.5"/>
        <rect x="65" y="5" width="50" height="50" fill="url(#mgrsGrad)" stroke="#9141ac" stroke-width="1.5"/>
        <rect x="5" y="65" width="50" height="50" fill="url(#mgrsGrad)" stroke="#9141ac" stroke-width="1.5"/>
        <rect x="65" y="65" width="50" height="50" fill="url(#mgrsGrad)" stroke="#9141ac" stroke-width="1.5"/>
        <line x1="30" y1="5" x2="30" y2="55" stroke="white" stroke-width="0.5" opacity="0.5"/>
        <line x1="55" y1="30" x2="5" y2="30" stroke="white" stroke-width="0.5" opacity="0.5"/>
        <line x1="90" y1="5" x2="90" y2="55" stroke="white" stroke-width="0.5" opacity="0.5"/>
        <line x1="115" y1="30" x2="65" y2="30" stroke="white" stroke-width="0.5" opacity="0.5"/>
        <text x="30" y="35" text-anchor="middle" fill="white" font-size="10" font-weight="bold">50T</text>
        <text x="90" y="35" text-anchor="middle" fill="white" font-size="10" font-weight="bold">51T</text>
    </svg>`;
}

// 创建六边形 SVG 演示
function createHexagonSVG() {
    return `<svg viewBox="0 0 120 120" width="120" height="120">
        <defs>
            <linearGradient id="hexGrad" x1="0%" y1="0%" x2="100%" y2="100%">
                <stop offset="0%" style="stop-color:#26a269;stop-opacity:0.8" />
                <stop offset="100%" style="stop-color:#57e389;stop-opacity:0.6" />
            </linearGradient>
        </defs>
        <polygon points="60,10 90,25 90,55 60,70 30,55 30,25" fill="url(#hexGrad)" stroke="#26a269" stroke-width="2"/>
        <polygon points="30,55 60,70 60,100 30,115 0,100 0,70" fill="url(#hexGrad)" stroke="#26a269" stroke-width="1" opacity="0.6"/>
        <polygon points="90,55 120,70 120,100 90,115 60,100 60,70" fill="url(#hexGrad)" stroke="#26a269" stroke-width="1" opacity="0.6"/>
        <polygon points="60,70 90,85 90,115 60,130 30,115 30,85" fill="url(#hexGrad)" stroke="#26a269" stroke-width="1" opacity="0.4" transform="translate(0,-10)"/>
    </svg>`;
}

function apiPrefixes() {
    const source = document.getElementById('source')?.value || 'api';
    return {
        gridPrefix: source === 'api' ? '/v1/grid' : '/v1/demo/sdk',
        topologyPrefix: source === 'api' ? '/v1/topology' : '/v1/demo/sdk/topology',
        codePrefix: '/v1/code',
    };
}

async function requestJson(url, payload) {
    const response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
        throw new Error(data?.error?.message || `HTTP ${response.status}`);
    }
    return data;
}

function clearMapDataLayers(map) {
    if (!map) return;
    map.eachLayer((layer) => {
        if (!(layer instanceof L.TileLayer) && layer !== drawItems) {
            map.removeLayer(layer);
        }
    });
    if (drawItems && map === maps.division) {
        drawItems.addTo(map);
    }
}

function styleByGridType(gridType, active = false) {
    const colors = {
        geohash: '#1a5fb4',
        mgrs: '#9141ac',
        isea4h: '#26a269',
    };
    const color = colors[gridType] || '#1a5fb4';
    return {
        color,
        weight: active ? 2.2 : 1.6,
        opacity: active ? 0.95 : 0.78,
        fillColor: color,
        fillOpacity: active ? 0.25 : 0.12,
    };
}

function bboxToPolygonGeometry(bbox) {
    const [minLon, minLat, maxLon, maxLat] = bbox;
    return {
        type: 'Polygon',
        coordinates: [[[minLon, minLat], [maxLon, minLat], [maxLon, maxLat], [minLon, maxLat], [minLon, minLat]]],
    };
}

function drawGeometryOnMap(map, geometry, gridType, label = '') {
    const geoLayer = L.geoJSON(geometry, {
        style: styleByGridType(gridType, true),
    }).addTo(map);
    if (label) {
        geoLayer.bindTooltip(label, { sticky: true });
    }
}

function drawCellOnMap(map, cell, gridType, label = '') {
    const geometry = cell.geometry || bboxToPolygonGeometry(cell.bbox);
    drawGeometryOnMap(map, geometry, gridType, label || cell.space_code);
}

function fitMapIfHasLayer(map) {
    const bounds = [];
    map.eachLayer((layer) => {
        if (layer.getBounds) {
            bounds.push(layer.getBounds());
        }
    });
    if (bounds.length > 0) {
        const merged = bounds[0];
        for (let i = 1; i < bounds.length; i += 1) {
            merged.extend(bounds[i]);
        }
        map.fitBounds(merged, { padding: [20, 20] });
    }
}

function parseBboxText(text) {
    if (!text || !text.trim()) return null;
    const values = text.split(',').map((part) => Number(part.trim()));
    if (values.length !== 4 || values.some((item) => Number.isNaN(item))) {
        throw new Error('BBox 输入格式错误，应为 minLon,minLat,maxLon,maxLat');
    }
    return values;
}

function buildBboxFromRadius(lon, lat, radiusKm) {
    const dLat = radiusKm / 111.32;
    const cosLat = Math.max(0.15, Math.cos((lat * Math.PI) / 180));
    const dLon = radiusKm / (111.32 * cosLat);
    return [lon - dLon, lat - dLat, lon + dLon, lat + dLat];
}

function updateResultHtml(targetId, rows) {
    const el = document.getElementById(targetId);
    if (!el) return;
    el.innerHTML = rows
        .map(
            (row) => `
        <div class="result-item">
            <div class="result-label">${row.label}</div>
            <div class="result-value ${row.code ? 'code' : ''}">${row.value}</div>
        </div>
    `
        )
        .join('');
}

function clearDivisionDrawings() {
    if (drawItems) {
        drawItems.clearLayers();
    }
    drawnGeometry = null;
    const geometryJson = document.getElementById('geometryJson');
    if (geometryJson) geometryJson.value = '';
}

// 执行演示
async function runDemo() {
    if (isProcessing) {
        console.log('请等待当前操作完成');
        return;
    }

    isProcessing = true;
    console.log('开始执行演示...');

    try {
        switch (currentPage) {
            case 'grid-division':
                await runGridDivision();
                break;
            case 'grid-encoding':
                await runGridEncoding();
                break;
            case 'grid-operations':
                await runGridOperations();
                break;
            case 'partition-optical':
                runOpticalPartition();
                return;
            case 'partition-carbon':
                runCarbonPartition();
                return;
            case 'partition-radar':
                runRadarPartition();
                return;
            case 'partition-product':
                runProductPartition();
                return;
            case 'partition-qc':
                runQualityControl();
                return;
            default:
                break;
        }
        isProcessing = false;
    } catch (error) {
        console.error(error);
        alert(`执行失败: ${error.message || error}`);
        isProcessing = false;
    }
}

// 重置演示
function resetDemo() {
    // 重置表单
    const forms = document.querySelectorAll('form');
    forms.forEach(form => form.reset());

    // 重置数据源选择为"从载入子系统获取"
    const dataSources = ['opticalDataSource', 'carbonDataSource', 'radarDataSource', 'productDataSource'];
    dataSources.forEach(id => {
        const select = document.getElementById(id);
        if (select) {
            select.value = 'loaded';
            select.dispatchEvent(new Event('change'));
        }
    });

    // 重置队列选择状态
    document.querySelectorAll('.queue-item').forEach((item, index) => {
        item.classList.remove('selected');
        if (index === 0) item.classList.add('selected');
    });

    // 清除地图图层
    Object.keys(maps).forEach(key => {
        if (maps[key]) {
            maps[key].eachLayer(layer => {
                if (!(layer instanceof L.TileLayer) && !(layer instanceof L.Control.Zoom)) {
                    maps[key].removeLayer(layer);
                }
            });
        }
    });

    // 重置结果显示
    const resultContainers = document.querySelectorAll('.results-content');
    resultContainers.forEach(container => {
        const pageId = container.id;
        const iconMap = {
            'divisionResults': '<svg class="empty-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="9" y1="21" x2="9" y2="9"/></svg>',
            'encodingResults': '<svg class="empty-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>',
            'operationResults': '<svg class="empty-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>',
            'opticalResults': '<svg class="empty-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/></svg>',
            'carbonResults': '<svg class="empty-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="12" cy="12" r="10"/><path d="M2 12h20"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg>',
            'radarResults': '<svg class="empty-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M4.93 4.93a10 10 0 0 1 14.14 0"/><path d="M7.76 7.76a6 6 0 0 1 8.48 0"/><circle cx="12" cy="12" r="2"/></svg>',
            'productResults': '<svg class="empty-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M21.21 15.89A10 10 0 1 1 8 2.83"/><path d="M22 12A10 10 0 0 0 12 2v10z"/></svg>',
            'qcResults': '<svg class="empty-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><polyline points="9 11 12 14 22 4"/><path d="M21 12v7a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11"/></svg>'
        };
        const textMap = {
            'divisionResults': '设置参数并执行演示以查看结果',
            'encodingResults': '设置参数并执行编码/解码操作',
            'operationResults': '选择运算类型并执行操作',
            'opticalResults': '从载入子系统选择数据并开始剖分',
            'carbonResults': '从载入子系统选择数据并执行剖分',
            'radarResults': '从载入子系统选择数据并开始剖分',
            'productResults': '从载入子系统选择数据并执行剖分',
            'qcResults': '选择数据并启动质检流程'
        };
        container.innerHTML = `
            <div class="empty-state">
                ${iconMap[pageId] || '<svg class="empty-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"/></svg>'}
                <p>${textMap[pageId] || '执行操作以查看结果'}</p>
            </div>
        `;
    });

    // 重置质检进度
    const qcPercent = document.getElementById('qcPercent');
    const qcProgress = document.getElementById('qcProgress');
    if (qcPercent) qcPercent.textContent = '0%';
    if (qcProgress) qcProgress.style.strokeDashoffset = '283';

    // 重置计数器
    ['qcPassed', 'qcWarning', 'qcFailed'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.textContent = '0';
    });

    // 重置层级显示
    const levelValue = document.getElementById('levelValue');
    if (levelValue) levelValue.textContent = '6级';

    console.log('演示已重置');
    isProcessing = false;
}

// 1. 格网划分演示
async function runGridDivision() {
    const map = maps.division;
    if (!map) return;
    clearMapDataLayers(map);

    const { gridPrefix } = apiPrefixes();
    const gridType = document.querySelector('input[name="gridType"]:checked')?.value || 'geohash';
    const inputType = document.getElementById('inputType')?.value || 'point';
    const level = parseInt(document.getElementById('gridLevel')?.value || 6, 10);
    const lat = parseFloat(document.getElementById('lat')?.value || 39.9042);
    const lng = parseFloat(document.getElementById('lng')?.value || 116.4074);
    const radius = parseFloat(document.getElementById('radius')?.value || 10);
    const coverMode = document.getElementById('coverMode')?.value || 'intersect';
    const boundaryType = document.getElementById('boundaryType')?.value || 'polygon';
    const geometryInput = document.getElementById('geometryJson')?.value || '';
    const bboxInput = document.getElementById('bboxInput')?.value || '';

    if (inputType === 'point') {
        const data = await requestJson(`${gridPrefix}/locate`, {
            grid_type: gridType,
            level,
            point: [lng, lat],
        });
        drawCellOnMap(map, data.cell, gridType);
        addMarkerToMap(map, lat, lng, `定位点 ${lat.toFixed(4)}, ${lng.toFixed(4)}`);
        fitMapIfHasLayer(map);
        updateResultHtml('divisionResults', [
            { label: '操作', value: 'locate' },
            { label: '格网类型', value: gridType },
            { label: '格网编码', value: data.cell.space_code, code: true },
            { label: '层级', value: String(data.cell.level) },
            { label: '中心坐标', value: `${data.cell.center[1].toFixed(6)}, ${data.cell.center[0].toFixed(6)}` },
            { label: '时间', value: new Date().toLocaleString('zh-CN') },
        ]);
        return;
    }

    let bbox = parseBboxText(bboxInput);
    if (!bbox) {
        bbox = buildBboxFromRadius(lng, lat, radius);
    }
    let geometry = null;
    if (geometryInput.trim()) {
        geometry = JSON.parse(geometryInput);
    } else if (drawnGeometry && inputType === 'polygon') {
        geometry = drawnGeometry;
    }

    const data = await requestJson(`${gridPrefix}/cover`, {
        grid_type: gridType,
        level,
        cover_mode: coverMode,
        boundary_type: boundaryType,
        geometry: geometry,
        bbox: geometry ? null : bbox,
        crs: 'EPSG:4326',
    });
    data.cells.forEach((cell) => drawCellOnMap(map, cell, gridType));
    if (geometry) {
        drawGeometryOnMap(map, geometry, gridType, '请求几何');
    } else if (bbox) {
        drawGeometryOnMap(map, bboxToPolygonGeometry(bbox), gridType, '请求范围');
    }
    fitMapIfHasLayer(map);
    updateResultHtml('divisionResults', [
        { label: '操作', value: 'cover' },
        { label: '格网类型', value: gridType },
        { label: '层级', value: String(level) },
        { label: '覆盖模式', value: coverMode },
        { label: '单元数量', value: String(data.statistics?.cell_count || data.cells.length) },
        { label: '时间', value: new Date().toLocaleString('zh-CN') },
    ]);
}

// 2. 时空编码演示
async function runGridEncoding() {
    const { gridPrefix, topologyPrefix, codePrefix } = apiPrefixes();
    const map = maps.encoding;
    if (!map) return;
    clearMapDataLayers(map);

    const operation = document.querySelector('input[name="encodeOp"]:checked')?.value || 'encode';
    const gridType = document.getElementById('encodeGridType')?.value || 'geohash';
    const level = parseInt(document.getElementById('gridLevel')?.value || 6, 10);
    const timeGranularity = document.getElementById('timeGranularity')?.value || 'minute';
    const timeInput = document.getElementById('encodeTime')?.value || '';
    const timestamp = timeInput ? new Date(timeInput).toISOString() : new Date().toISOString();
    const encodeInput = document.getElementById('encodeInput')?.value || '';
    const decodeInput = document.getElementById('decodeInput')?.value?.trim() || '';

    if (operation === 'decode') {
        const parsed = await requestJson(`${codePrefix}/parse`, { st_code: decodeInput });
        const geometryResp = await requestJson(`${topologyPrefix}/geometry`, {
            grid_type: parsed.grid_type,
            code: parsed.space_code,
            boundary_type: 'polygon',
        });
        drawGeometryOnMap(map, geometryResp.geometry, parsed.grid_type, parsed.space_code);
        fitMapIfHasLayer(map);
        document.getElementById('levelPart').textContent = String(parsed.level);
        document.getElementById('spacePart').textContent = parsed.space_code;
        document.getElementById('timePart').textContent = parsed.time_code;
        document.getElementById('fullCode').textContent = decodeInput;
        updateResultHtml('encodingResults', [
            { label: '操作', value: '解码' },
            { label: '完整编码', value: decodeInput, code: true },
            { label: '格网类型', value: parsed.grid_type },
            { label: '空间编码', value: parsed.space_code, code: true },
            { label: '时间编码', value: parsed.time_code },
            { label: '版本', value: parsed.version },
        ]);
        return;
    }

    const lines = encodeInput
        .split('\n')
        .map((line) => line.trim())
        .filter(Boolean);
    const points = lines
        .map((line) => line.split(',').map((v) => Number(v.trim())))
        .filter((pair) => pair.length === 2 && !Number.isNaN(pair[0]) && !Number.isNaN(pair[1]));
    if (points.length === 0) {
        throw new Error('请输入坐标，格式为: 纬度,经度');
    }

    if (operation === 'batch') {
        const items = [];
        for (const [lat, lon] of points) {
            const located = await requestJson(`${gridPrefix}/locate`, {
                grid_type: gridType,
                level,
                point: [lon, lat],
            });
            items.push({ space_code: located.cell.space_code, timestamp });
            drawCellOnMap(map, located.cell, gridType);
        }
        const batchResp = await requestJson(`${codePrefix}/st/batch`, {
            grid_type: gridType,
            level,
            time_granularity: timeGranularity,
            version: 'v1',
            items,
        });
        fitMapIfHasLayer(map);
        document.getElementById('levelPart').textContent = String(level);
        document.getElementById('spacePart').textContent = `${items.length} cells`;
        document.getElementById('timePart').textContent = batchResp.st_codes[0]?.split(':')[3] || '-';
        document.getElementById('fullCode').textContent = `${batchResp.st_codes.length} 条批量编码`;
        updateResultHtml('encodingResults', [
            { label: '操作', value: '批量编码' },
            { label: '格网类型', value: gridType },
            { label: '处理点位', value: String(items.length) },
            { label: '生成编码', value: String(batchResp.statistics?.count || batchResp.st_codes.length) },
            { label: '示例编码', value: batchResp.st_codes[0] || '-', code: true },
        ]);
        return;
    }

    const [lat, lon] = points[0];
    const located = await requestJson(`${gridPrefix}/locate`, {
        grid_type: gridType,
        level,
        point: [lon, lat],
    });
    const codeResp = await requestJson(`${codePrefix}/st`, {
        grid_type: gridType,
        level,
        space_code: located.cell.space_code,
        timestamp,
        time_granularity: timeGranularity,
        version: 'v1',
    });
    drawCellOnMap(map, located.cell, gridType);
    addMarkerToMap(map, lat, lon, located.cell.space_code);
    fitMapIfHasLayer(map);
    const parts = codeResp.st_code.split(':');
    document.getElementById('levelPart').textContent = parts[1] || String(level);
    document.getElementById('spacePart').textContent = parts[2] || located.cell.space_code;
    document.getElementById('timePart').textContent = parts[3] || '-';
    document.getElementById('fullCode').textContent = codeResp.st_code;
    updateResultHtml('encodingResults', [
        { label: '操作', value: '编码' },
        { label: '格网类型', value: gridType },
        { label: '空间编码', value: located.cell.space_code, code: true },
        { label: '完整时空编码', value: codeResp.st_code, code: true },
        { label: '时间粒度', value: timeGranularity },
        { label: '时间', value: new Date(timestamp).toLocaleString('zh-CN') },
    ]);
}

// 3. 拓扑运算演示
async function runGridOperations() {
    const { gridPrefix, topologyPrefix } = apiPrefixes();
    const map = maps.topology;
    if (!map) return;
    clearMapDataLayers(map);

    const gridType = document.querySelector('input[name="gridType"]:checked')?.value || 'geohash';
    const opType = document.getElementById('topologyOp')?.value || 'neighbors';
    const k = parseInt(document.getElementById('neighborK')?.value || 1, 10);
    const targetLevel = parseInt(document.getElementById('targetLevel')?.value || 6, 10);
    const convertDir = document.getElementById('convertDir')?.value || 'code2coord';
    let code1 = document.getElementById('opCode1')?.value?.trim() || '';
    const code2 = document.getElementById('opCode2')?.value?.trim() || '';
    const lat = parseFloat(document.getElementById('lat')?.value || 39.9042);
    const lng = parseFloat(document.getElementById('lng')?.value || 116.4074);
    const level = parseInt(document.getElementById('gridLevel')?.value || 6, 10);
    const baseRows = [{ label: '格网类型', value: gridType }];

    if (!code1) {
        const located = await requestJson(`${gridPrefix}/locate`, {
            grid_type: gridType,
            level,
            point: [lng, lat],
        });
        code1 = located.cell.space_code;
        const codeInput = document.getElementById('opCode1');
        if (codeInput) codeInput.value = code1;
    }

    if (opType === 'neighbors') {
        const data = await requestJson(`${topologyPrefix}/neighbors`, { grid_type: gridType, code: code1, k });
        const previewCodes = data.result_codes.slice(0, 200);
        if (previewCodes.length > 0) {
            const geometries = await requestJson(`${topologyPrefix}/geometries`, {
                grid_type: gridType,
                codes: previewCodes,
                boundary_type: 'polygon',
            });
            previewCodes.forEach((code) => {
                if (geometries.geometries[code]) {
                    drawGeometryOnMap(map, geometries.geometries[code], gridType, code);
                }
            });
        }
        fitMapIfHasLayer(map);
        const rows = [
            { label: '运算类型', value: 'neighbors' },
            { label: '输入编码', value: code1, code: true },
            { label: 'k', value: String(k) },
            { label: '结果数量', value: String(data.statistics?.count || data.result_codes.length) },
            { label: '预览编码', value: previewCodes.slice(0, 8).join(', '), code: true },
        ];
        await appendConversionRows(rows, convertDir, { code1, targetLevel, lat, lng, gridType, gridPrefix, topologyPrefix });
        updateResultHtml('operationResults', [...baseRows, ...rows]);
        return;
    }

    if (opType === 'parent') {
        const data = await requestJson(`${topologyPrefix}/parent`, { grid_type: gridType, code: code1 });
        const geo = await requestJson(`${topologyPrefix}/geometry`, {
            grid_type: gridType,
            code: data.parent_code,
            boundary_type: 'polygon',
        });
        drawGeometryOnMap(map, geo.geometry, gridType, data.parent_code);
        fitMapIfHasLayer(map);
        const rows = [
            { label: '运算类型', value: 'parent' },
            { label: '输入编码', value: code1, code: true },
            { label: '父编码', value: data.parent_code, code: true },
            { label: '执行时间', value: new Date().toLocaleString('zh-CN') },
        ];
        await appendConversionRows(rows, convertDir, { code1: data.parent_code, targetLevel, lat, lng, gridType, gridPrefix, topologyPrefix });
        updateResultHtml('operationResults', [...baseRows, ...rows]);
        return;
    }

    if (opType === 'children') {
        const data = await requestJson(`${topologyPrefix}/children`, {
            grid_type: gridType,
            code: code1,
            target_level: targetLevel,
        });
        const previewCodes = data.child_codes.slice(0, 200);
        if (previewCodes.length > 0) {
            const geometries = await requestJson(`${topologyPrefix}/geometries`, {
                grid_type: gridType,
                codes: previewCodes,
                boundary_type: 'polygon',
            });
            previewCodes.forEach((code) => {
                if (geometries.geometries[code]) {
                    drawGeometryOnMap(map, geometries.geometries[code], gridType, code);
                }
            });
        }
        fitMapIfHasLayer(map);
        const rows = [
            { label: '运算类型', value: 'children' },
            { label: '输入编码', value: code1, code: true },
            { label: '目标层级', value: String(targetLevel) },
            { label: '结果数量', value: String(data.statistics?.count || data.child_codes.length) },
            { label: '预览编码', value: previewCodes.slice(0, 8).join(', '), code: true },
        ];
        await appendConversionRows(rows, convertDir, { code1, targetLevel, lat, lng, gridType, gridPrefix, topologyPrefix });
        updateResultHtml('operationResults', [...baseRows, ...rows]);
        return;
    }

    if (!code2) {
        throw new Error('contains/intersect 需要输入对比编码');
    }
    const geoA = await requestJson(`${topologyPrefix}/geometry`, {
        grid_type: gridType,
        code: code1,
        boundary_type: 'polygon',
    });
    const geoB = await requestJson(`${topologyPrefix}/geometry`, {
        grid_type: gridType,
        code: code2,
        boundary_type: 'polygon',
    });
    const relation = evaluateGeometryRelation(geoA.geometry, geoB.geometry);

    drawGeometryOnMap(map, geoA.geometry, gridType, code1);
    drawGeometryOnMap(map, geoB.geometry, gridType, code2);
    fitMapIfHasLayer(map);
    const rows = [
        { label: '运算类型', value: opType },
        { label: '编码A', value: code1, code: true },
        { label: '编码B', value: code2, code: true },
        {
            label: '结果',
            value: opType === 'contains'
                ? (relation.contains ? '包含' : '不包含')
                : (relation.intersect ? '相交' : '不相交'),
        },
        {
            label: '判定方式',
            value: relation.method,
        },
    ];
    await appendConversionRows(rows, convertDir, { code1, targetLevel, lat, lng, gridType, gridPrefix, topologyPrefix });
    updateResultHtml('operationResults', [...baseRows, ...rows]);
}

function geometryToFeature(geometry) {
    return { type: 'Feature', properties: {}, geometry };
}

function evaluateGeometryRelation(geometryA, geometryB) {
    if (window.turf?.booleanContains && window.turf?.booleanIntersects) {
        const featureA = geometryToFeature(geometryA);
        const featureB = geometryToFeature(geometryB);
        return {
            contains: window.turf.booleanContains(featureA, featureB),
            intersect: window.turf.booleanIntersects(featureA, featureB),
            method: 'Polygon 精确判定',
        };
    }

    const [aMinLon, aMinLat, aMaxLon, aMaxLat] = extractGeometryBounds(geometryA);
    const [bMinLon, bMinLat, bMaxLon, bMaxLat] = extractGeometryBounds(geometryB);
    const contains = aMinLon <= bMinLon && aMinLat <= bMinLat && aMaxLon >= bMaxLon && aMaxLat >= bMaxLat;
    const intersect = !(aMaxLon < bMinLon || aMinLon > bMaxLon || aMaxLat < bMinLat || aMinLat > bMaxLat);
    return {
        contains,
        intersect,
        method: 'BBox 近似判定（turf 未加载）',
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
    const lons = coords.map((p) => p[0]);
    const lats = coords.map((p) => p[1]);
    return [Math.min(...lons), Math.min(...lats), Math.max(...lons), Math.max(...lats)];
}

async function appendConversionRows(rows, convertDir, ctx) {
    const { code1, targetLevel, lat, lng, gridType, gridPrefix, topologyPrefix } = ctx;
    if (convertDir === 'code2coord') {
        const bboxResp = await requestJson(`${topologyPrefix}/geometry`, {
            grid_type: gridType,
            code: code1,
            boundary_type: 'bbox',
        });
        const bbox = bboxResp.geometry.bbox;
        const centerLon = (bbox[0] + bbox[2]) / 2;
        const centerLat = (bbox[1] + bbox[3]) / 2;
        rows.push({ label: '转换方向', value: '编码 -> 坐标' });
        rows.push({ label: '转换结果', value: `${centerLat.toFixed(6)}, ${centerLon.toFixed(6)}` });
        return;
    }

    const locateResp = await requestJson(`${gridPrefix}/locate`, {
        grid_type: gridType,
        level: targetLevel,
        point: [lng, lat],
    });
    rows.push({ label: '转换方向', value: '坐标 -> 编码' });
    rows.push({ label: '转换层级', value: String(targetLevel) });
    rows.push({ label: '转换结果', value: locateResp.cell.space_code, code: true });
}

// 4. 光学遥感剖分演示
function runOpticalPartition() {
    // 获取选中的数据批次
    const selectedItem = document.querySelector('#opticalDataQueue .queue-item.selected');
    const batchId = selectedItem ? selectedItem.getAttribute('data-batch-id') : 'B20240307001';
    const batchName = selectedItem ? selectedItem.querySelector('.item-name').textContent : 'Landsat-8_OLI_20240307';

    setTimeout(() => {
        const resultsContainer = document.getElementById('opticalResults');
        if (!resultsContainer) return;

        resultsContainer.innerHTML = `
            <div class="result-item">
                <div class="result-label">剖分状态</div>
                <div class="result-value" style="color:var(--success);">✓ 完成</div>
            </div>
            <div class="result-item">
                <div class="result-label">数据源</div>
                <div class="result-value">载入子系统</div>
            </div>
            <div class="result-item">
                <div class="result-label">批次ID</div>
                <div class="result-value code">${batchId}</div>
            </div>
            <div class="result-item">
                <div class="result-label">数据名称</div>
                <div class="result-value">${batchName}</div>
            </div>
            <div class="result-item">
                <div class="result-label">生成数据块</div>
                <div class="result-value">256 个</div>
            </div>
            <div class="result-item">
                <div class="result-label">格网类型</div>
                <div class="result-value">Geohash 逻辑剖分</div>
            </div>
            <div class="result-item">
                <div class="result-label">输出格式</div>
                <div class="result-value">COG (GeoTIFF)</div>
            </div>
            <div class="result-item">
                <div class="result-label">总数据量</div>
                <div class="result-value">2.4 GB</div>
            </div>
            <div class="result-item">
                <div class="result-label">处理时间</div>
                <div class="result-value">45.2 秒</div>
            </div>
        `;
        isProcessing = false;
    }, 800);
}

// 5. 碳卫星剖分演示
function runCarbonPartition() {
    const selectedItem = document.querySelector('#carbonDataQueue .queue-item.selected');
    const batchId = selectedItem ? selectedItem.getAttribute('data-batch-id') : 'C20240307001';
    const batchName = selectedItem ? selectedItem.querySelector('.item-name').textContent : 'OCO-2_XCO2_20240307';

    setTimeout(() => {
        const resultsContainer = document.getElementById('carbonResults');
        if (!resultsContainer) return;

        resultsContainer.innerHTML = `
            <div class="result-item">
                <div class="result-label">剖分状态</div>
                <div class="result-value" style="color:var(--success);">✓ 完成</div>
            </div>
            <div class="result-item">
                <div class="result-label">数据源</div>
                <div class="result-value">载入子系统</div>
            </div>
            <div class="result-item">
                <div class="result-label">批次ID</div>
                <div class="result-value code">${batchId}</div>
            </div>
            <div class="result-item">
                <div class="result-label">数据名称</div>
                <div class="result-value">${batchName}</div>
            </div>
            <div class="result-item">
                <div class="result-label">Footprint 映射</div>
                <div class="result-value">✓ 完成</div>
            </div>
            <div class="result-item">
                <div class="result-label">覆盖格网数</div>
                <div class="result-value">89 个</div>
            </div>
            <div class="result-item">
                <div class="result-label">有效数据比例</div>
                <div class="result-value">86.5%</div>
            </div>
            <div class="result-item">
                <div class="result-label">数据立方体</div>
                <div class="result-value">XCO2 时间序列</div>
            </div>
            <div class="result-item">
                <div class="result-label">输出格式</div>
                <div class="result-value">NetCDF4</div>
            </div>
        `;
        isProcessing = false;
    }, 800);
}

// 6. 雷达剖分演示
function runRadarPartition() {
    const selectedItem = document.querySelector('#radarDataQueue .queue-item.selected');
    const batchId = selectedItem ? selectedItem.getAttribute('data-batch-id') : 'R20240307001';
    const batchName = selectedItem ? selectedItem.querySelector('.item-name').textContent : 'Sentinel-1A_GRD_20240307';

    setTimeout(() => {
        const resultsContainer = document.getElementById('radarResults');
        if (!resultsContainer) return;

        resultsContainer.innerHTML = `
            <div class="result-item">
                <div class="result-label">剖分状态</div>
                <div class="result-value" style="color:var(--success);">✓ 完成</div>
            </div>
            <div class="result-item">
                <div class="result-label">数据源</div>
                <div class="result-value">载入子系统</div>
            </div>
            <div class="result-item">
                <div class="result-label">批次ID</div>
                <div class="result-value code">${batchId}</div>
            </div>
            <div class="result-item">
                <div class="result-label">数据名称</div>
                <div class="result-value">${batchName}</div>
            </div>
            <div class="result-item">
                <div class="result-label">生成数据块</div>
                <div class="result-value">128 个</div>
            </div>
            <div class="result-item">
                <div class="result-label">极化方式</div>
                <div class="result-value">VV + VH</div>
            </div>
            <div class="result-item">
                <div class="result-label">剖分策略</div>
                <div class="result-value">实体剖分</div>
            </div>
            <div class="result-item">
                <div class="result-label">输出格式</div>
                <div class="result-value">GeoTIFF + 元数据</div>
            </div>
        `;
        isProcessing = false;
    }, 800);
}

// 7. 信息产品剖分演示
function runProductPartition() {
    const selectedItem = document.querySelector('#productDataQueue .queue-item.selected');
    const batchId = selectedItem ? selectedItem.getAttribute('data-batch-id') : 'P20240307001';
    const batchName = selectedItem ? selectedItem.querySelector('.item-name').textContent : 'LandCover_GLC_2023';

    setTimeout(() => {
        const resultsContainer = document.getElementById('productResults');
        if (!resultsContainer) return;

        resultsContainer.innerHTML = `
            <div class="result-item">
                <div class="result-label">剖分状态</div>
                <div class="result-value" style="color:var(--success);">✓ 完成</div>
            </div>
            <div class="result-item">
                <div class="result-label">数据源</div>
                <div class="result-value">载入子系统</div>
            </div>
            <div class="result-item">
                <div class="result-label">批次ID</div>
                <div class="result-value code">${batchId}</div>
            </div>
            <div class="result-item">
                <div class="result-label">数据名称</div>
                <div class="result-value">${batchName}</div>
            </div>
            <div class="result-item">
                <div class="result-label">格网单元数</div>
                <div class="result-value">512 个</div>
            </div>
            <div class="result-item">
                <div class="result-label">类别统计</div>
                <div class="result-value">森林 45.2%, 农田 28.6%, 建筑 15.3%, 水体 11.9%</div>
            </div>
            <div class="result-item">
                <div class="result-label">输出格式</div>
                <div class="result-value">GeoTIFF + GeoJSON</div>
            </div>
        `;
        isProcessing = false;
    }, 800);
}

// 8. 质检演示
function runQualityControl() {
    let progress = 0;
    const qcPercent = document.getElementById('qcPercent');
    const qcProgress = document.getElementById('qcProgress');
    const qcPassed = document.getElementById('qcPassed');
    const qcWarning = document.getElementById('qcWarning');
    const qcFailed = document.getElementById('qcFailed');

    const interval = setInterval(() => {
        progress += 5;
        if (qcPercent) qcPercent.textContent = `${progress}%`;
        if (qcProgress) qcProgress.style.strokeDashoffset = 283 - (283 * progress / 100);

        // 更新统计
        if (qcPassed) qcPassed.textContent = Math.floor(progress * 0.8);
        if (qcWarning) qcWarning.textContent = Math.floor(progress * 0.15);
        if (qcFailed) qcFailed.textContent = Math.floor(progress * 0.05);

        if (progress >= 100) {
            clearInterval(interval);
            displayQCResults();
            isProcessing = false;
        }
    }, 100);
}

// 显示质检结果
function displayQCResults() {
    const resultsContainer = document.getElementById('qcResults');
    if (!resultsContainer) return;

    resultsContainer.innerHTML = `
        <div class="result-item">
            <div class="result-label">质检结果</div>
            <div class="result-value" style="color:var(--success);font-weight:600;">✓ 通过</div>
        </div>
        <div class="result-item">
            <div class="result-label">检测文件数</div>
            <div class="result-value">256 个</div>
        </div>
        <div class="result-item">
            <div class="result-label">通过</div>
            <div class="result-value" style="color:var(--success);">205 个 (80.1%)</div>
        </div>
        <div class="result-item">
            <div class="result-label">警告</div>
            <div class="result-value" style="color:var(--warning);">38 个 (14.8%)</div>
        </div>
        <div class="result-item">
            <div class="result-label">失败</div>
            <div class="result-value" style="color:var(--danger);">13 个 (5.1%)</div>
        </div>
        <div class="result-item">
            <div class="result-label">质检报告</div>
            <div class="result-value">已生成 PDF 报告</div>
        </div>
    `;
}

// 获取当前位置
function getCurrentLocation() {
    if (navigator.geolocation) {
        navigator.geolocation.getCurrentPosition(
            (position) => {
                const lat = position.coords.latitude.toFixed(4);
                const lng = position.coords.longitude.toFixed(4);

                const latInput = document.getElementById('lat');
                const lngInput = document.getElementById('lng');
                if (latInput && lngInput) {
                    latInput.value = lat;
                    lngInput.value = lng;
                }

                if (maps.division) {
                    maps.division.setView([parseFloat(lat), parseFloat(lng)], 12);
                    addMarkerToMap(maps.division, parseFloat(lat), parseFloat(lng), '当前位置');
                }

                console.log(`已获取当前位置: ${lat}, ${lng}`);
            },
            (error) => {
                console.error('获取位置失败:', error.message);
                alert('无法获取当前位置，请手动输入坐标');
            }
        );
    } else {
        alert('您的浏览器不支持地理位置功能');
    }
}

// 清除日志（兼容旧代码）
function clearLogs() {
    console.clear();
}

// 添加日志（兼容旧代码）
function addLog(message, type = 'info') {
    const timestamp = new Date().toLocaleTimeString('zh-CN');
    console.log(`[${timestamp}] [${type.toUpperCase()}] ${message}`);
}
