<script setup>
import { onBeforeUnmount, onMounted, ref, watch } from 'vue';
import {
  ArcGisMapServerImageryProvider,
  buildModuleUrl,
  Cartesian2,
  Cartesian3,
  Cartographic,
  Color,
  CustomDataSource,
  EllipsoidTerrainProvider,
  HeightReference,
  ImageryLayer,
  LabelStyle,
  Math as CesiumMath,
  PolygonHierarchy,
  Rectangle,
  SceneMode,
  ScreenSpaceEventHandler,
  ScreenSpaceEventType,
  TileMapServiceImageryProvider,
  VerticalOrigin,
  Viewer,
} from 'cesium';
import 'cesium/Build/Cesium/Widgets/widgets.css';

import earthBlueMarbleUrl from '@/assets/globe/earth-blue-marble.jpg';

const props = defineProps({
  center: { type: Array, default: () => [39.9042, 116.4074] },
  zoom: { type: Number, default: 7 },
  markers: { type: Array, default: () => [] },
  geometries: { type: Array, default: () => [] },
  circle: { type: Object, default: null },
  interactionMode: { type: String, default: 'none' },
});

const emit = defineEmits(['point-selected', 'circle-drawn']);

const EARTH_KM = 6371;
const DEFAULT_PREVIEW_IMAGERY_URL = 'https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer';
const PREVIEW_IMAGERY_URL = (import.meta.env.VITE_GLOBE_IMAGERY_URL || DEFAULT_PREVIEW_IMAGERY_URL).trim();
const MIN_3D_ZOOM_DISTANCE = 5000;
const MAX_ZOOM_DISTANCE = 32000000;

const mapEl = ref(null);
const fallbackSphere = ref(null);
const useFallback = ref(false);
const mapMode = ref('3d');

let viewer;
let overlaySource;
let handler;
let drawStart = null;
let draftCircle = null;
let pointerDown = null;
let lastFocusSignature = '';
let previewLayer = null;
let lifecycleGeneration = 0;
let imageryGeneration = 0;
let resizeObserver = null;

function viewerIsCurrent(candidate, generation) {
  return generation === lifecycleGeneration && candidate && candidate === viewer && !candidate.isDestroyed();
}

function destroyUnusedLayer(layer) {
  if (layer && typeof layer.isDestroyed === 'function' && !layer.isDestroyed()) layer.destroy();
}

function toRad(value) {
  return CesiumMath.toRadians(Number(value));
}

function toDeg(value) {
  return CesiumMath.toDegrees(value);
}

function normalizeLng(lng) {
  return ((((Number(lng) + 180) % 360) + 360) % 360) - 180;
}

function isValidLatLng(lat, lng) {
  return Number.isFinite(Number(lat)) && Number.isFinite(Number(lng));
}

function canCreateWebglContext() {
  if (!window.WebGLRenderingContext) return false;
  const canvas = document.createElement('canvas');
  try {
    return Boolean(canvas.getContext('webgl2') || canvas.getContext('webgl'));
  } catch {
    return false;
  }
}

function cssColor(color, opacity = 1) {
  try {
    return Color.fromCssColorString(color || '#2f91ea').withAlpha(opacity);
  } catch {
    return Color.fromCssColorString('#2f91ea').withAlpha(opacity);
  }
}

function haversineKm(a, b) {
  const lat1 = toRad(a.lat);
  const lat2 = toRad(b.lat);
  const dLat = lat2 - lat1;
  const dLng = toRad(b.lng - a.lng);
  const sinLat = Math.sin(dLat / 2);
  const sinLng = Math.sin(dLng / 2);
  const value = sinLat * sinLat + Math.cos(lat1) * Math.cos(lat2) * sinLng * sinLng;
  return EARTH_KM * 2 * Math.atan2(Math.sqrt(value), Math.sqrt(Math.max(0, 1 - value)));
}

function destinationPoint(center, radiusKm, bearingDeg) {
  const distance = radiusKm / EARTH_KM;
  const bearing = toRad(bearingDeg);
  const lat1 = toRad(center.lat);
  const lng1 = toRad(center.lng);
  const sinLat1 = Math.sin(lat1);
  const cosLat1 = Math.cos(lat1);
  const sinDistance = Math.sin(distance);
  const cosDistance = Math.cos(distance);
  const lat2 = Math.asin(sinLat1 * cosDistance + cosLat1 * sinDistance * Math.cos(bearing));
  const lng2 = lng1 + Math.atan2(
    Math.sin(bearing) * sinDistance * cosLat1,
    cosDistance - sinLat1 * Math.sin(lat2),
  );
  return { lat: toDeg(lat2), lng: normalizeLng(toDeg(lng2)) };
}

function flattenGeoJson(geometry) {
  if (!geometry) return [];
  if (geometry.type === 'Feature') return flattenGeoJson(geometry.geometry);
  if (geometry.type === 'FeatureCollection') {
    return (geometry.features || []).flatMap((feature) => flattenGeoJson(feature));
  }
  return [geometry];
}

function collectCoordinatePairs(geometry, output = []) {
  if (!geometry) return output;
  if (geometry.type === 'Feature') return collectCoordinatePairs(geometry.geometry, output);
  if (geometry.type === 'FeatureCollection') {
    (geometry.features || []).forEach((feature) => collectCoordinatePairs(feature, output));
    return output;
  }

  function walk(value) {
    if (!Array.isArray(value)) return;
    if (typeof value[0] === 'number' && typeof value[1] === 'number') {
      const lng = Number(value[0]);
      const lat = Number(value[1]);
      if (isValidLatLng(lat, lng)) output.push({ lat, lng });
      return;
    }
    value.forEach(walk);
  }

  walk(geometry.coordinates);
  return output;
}

function geometrySignature() {
  return JSON.stringify({
    markers: props.markers.map((marker) => [marker.position?.[0], marker.position?.[1], marker.label || '']),
    geometries: props.geometries.map((item) => [item.label || '', item.geometry]),
    circle: props.circle,
  });
}

function degreesArrayFromRing(ring) {
  return (ring || [])
    .flatMap((coordinate) => [Number(coordinate[0]), Number(coordinate[1])])
    .filter((value) => Number.isFinite(value));
}

function polygonHierarchy(coordinates) {
  const outer = Cartesian3.fromDegreesArray(degreesArrayFromRing(coordinates?.[0] || []));
  const holes = (coordinates || [])
    .slice(1)
    .map((ring) => new PolygonHierarchy(Cartesian3.fromDegreesArray(degreesArrayFromRing(ring))))
    .filter((hole) => hole.positions.length >= 3);
  return new PolygonHierarchy(outer, holes);
}

function addPolygon(coordinates, item, id) {
  const hierarchy = polygonHierarchy(coordinates);
  if (hierarchy.positions.length < 3) return;
  const fillOpacity = item.fillOpacity ?? 0.32;
  const fillColor = cssColor(item.fillColor || item.color || '#2f91ea', fillOpacity);
  const strokeColor = cssColor(item.color || '#2f91ea', 0.95);
  overlaySource.entities.add({
    id,
    polygon: {
      hierarchy,
      material: fillColor,
      heightReference: HeightReference.CLAMP_TO_GROUND,
    },
    polyline: {
      positions: [...hierarchy.positions, hierarchy.positions[0]],
      width: Math.max(1.5, Number(item.weight || 2)),
      material: strokeColor,
      clampToGround: true,
    },
  });
}

function addGeometry(item, itemIndex) {
  flattenGeoJson(item.geometry).forEach((geometry, geometryIndex) => {
    if (geometry.type === 'Polygon') {
      addPolygon(geometry.coordinates, item, `polygon-${itemIndex}-${geometryIndex}`);
      return;
    }
    if (geometry.type === 'MultiPolygon') {
      (geometry.coordinates || []).forEach((coordinates, polygonIndex) => {
        addPolygon(coordinates, item, `polygon-${itemIndex}-${geometryIndex}-${polygonIndex}`);
      });
      return;
    }
    if (geometry.type === 'LineString') {
      addPolyline(geometry.coordinates, item, `line-${itemIndex}-${geometryIndex}`);
      return;
    }
    if (geometry.type === 'MultiLineString') {
      (geometry.coordinates || []).forEach((coordinates, lineIndex) => {
        addPolyline(coordinates, item, `line-${itemIndex}-${geometryIndex}-${lineIndex}`);
      });
    }
  });
}

function addPolyline(coordinates, item, id) {
  const positions = Cartesian3.fromDegreesArray(degreesArrayFromRing(coordinates));
  if (positions.length < 2) return;
  overlaySource.entities.add({
    id,
    polyline: {
      positions,
      width: Math.max(1.5, Number(item.weight || 2)),
      material: cssColor(item.color || '#2f91ea', 0.95),
      clampToGround: true,
    },
  });
}

function addMarker(marker, index) {
  const [lat, lng] = marker.position || [];
  if (!isValidLatLng(lat, lng)) return null;
  const entity = overlaySource.entities.add({
    id: `marker-${index}`,
    position: Cartesian3.fromDegrees(Number(lng), Number(lat), 0),
    point: {
      pixelSize: 10,
      color: Color.fromCssColorString('#f5c211'),
      outlineColor: Color.BLACK.withAlpha(0.7),
      outlineWidth: 1,
      heightReference: HeightReference.CLAMP_TO_GROUND,
      disableDepthTestDistance: Number.POSITIVE_INFINITY,
    },
    label: marker.label ? {
      text: marker.label,
      font: '12px sans-serif',
      fillColor: Color.WHITE,
      outlineColor: Color.BLACK.withAlpha(0.8),
      outlineWidth: 2,
      style: LabelStyle.FILL_AND_OUTLINE,
      pixelOffset: new Cartesian2(0, -18),
      verticalOrigin: VerticalOrigin.BOTTOM,
      heightReference: HeightReference.CLAMP_TO_GROUND,
      disableDepthTestDistance: Number.POSITIVE_INFINITY,
    } : undefined,
  });
  return { lat: Number(lat), lng: Number(lng), entity };
}

function addCircle(circle) {
  if (!circle || !isValidLatLng(circle.lat, circle.lng) || !Number.isFinite(Number(circle.radiusKm))) return null;
  const center = { lat: Number(circle.lat), lng: Number(circle.lng) };
  const radiusKm = Math.max(0.001, Number(circle.radiusKm));
  const radiusMeters = radiusKm * 1000;
  overlaySource.entities.add({
    id: 'drawn-circle-fill',
    position: Cartesian3.fromDegrees(center.lng, center.lat, 0),
    ellipse: {
      semiMajorAxis: radiusMeters,
      semiMinorAxis: radiusMeters,
      material: Color.fromCssColorString('#f5c211').withAlpha(0.2),
      heightReference: HeightReference.CLAMP_TO_GROUND,
    },
  });
  overlaySource.entities.add({
    id: 'drawn-circle-outline',
    polyline: {
      positions: Cartesian3.fromDegreesArray(
        Array.from({ length: 121 }, (_, index) => destinationPoint(center, radiusKm, index * 3))
          .flatMap((point) => [point.lng, point.lat]),
      ),
      width: 3,
      material: Color.fromCssColorString('#f5c211'),
      clampToGround: true,
    },
  });
  return center;
}

function renderLayers({ refocus = true } = {}) {
  if (!viewer || !overlaySource || useFallback.value) return;
  overlaySource.entities.removeAll();
  const focusPoints = [];

  props.geometries.forEach((item, index) => {
    addGeometry(item, index);
    collectCoordinatePairs(item.geometry).forEach((point) => focusPoints.push(point));
  });

  const markerPoints = props.markers.map(addMarker).filter(Boolean);
  const circlePoint = addCircle(draftCircle || props.circle);

  const signature = geometrySignature();
  const shouldRefocus = refocus && signature !== lastFocusSignature;
  lastFocusSignature = signature;

  if (!shouldRefocus) return;
  if (focusPoints.length) {
    focusToPoints(focusPoints);
  } else if (markerPoints.length) {
    focusToPoints(markerPoints);
  } else if (circlePoint) {
    focusToPoints([circlePoint]);
  } else {
    focusDefault();
  }
}

function cameraHeight() {
  if (mapMode.value === '2d') {
    const height = 26000000 / (2 ** Math.max(0, (Number(props.zoom) || 7) - 2));
    return Math.max(150000, Math.min(18000000, height));
  }
  const height = 23000000 - (Number(props.zoom) || 7) * 1800000;
  return Math.max(300000, Math.min(18000000, height));
}

function focusToPoints(points) {
  if (!viewer || !points.length) return;
  let west = Number.POSITIVE_INFINITY;
  let east = Number.NEGATIVE_INFINITY;
  let south = Number.POSITIVE_INFINITY;
  let north = Number.NEGATIVE_INFINITY;
  points.forEach((point) => {
    const lat = Number(point.lat);
    const lng = Number(point.lng);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
    west = Math.min(west, lng);
    east = Math.max(east, lng);
    south = Math.min(south, lat);
    north = Math.max(north, lat);
  });
  if (![west, east, south, north].every(Number.isFinite)) return;
  if (points.length === 1 || Math.abs(east - west) < 0.01 || Math.abs(north - south) < 0.01) {
    viewer.camera.flyTo({
      destination: Cartesian3.fromDegrees(west, south, cameraHeight()),
      duration: 0.35,
    });
    return;
  }
  viewer.camera.flyTo({
    destination: Rectangle.fromDegrees(west, south, east, north),
    duration: 0.35,
  });
}

function focusDefault() {
  const [lat, lng] = props.center || [];
  if (!isValidLatLng(lat, lng) || !viewer) return;
  viewer.camera.setView({
    destination: Cartesian3.fromDegrees(Number(lng), Number(lat), cameraHeight()),
  });
}

function screenToLatLng(screenPosition) {
  if (useFallback.value) return fallbackScreenToLatLng(screenPosition);
  if (!viewer || !screenPosition) return null;
  let cartesian = null;
  if (viewer.scene.pickPositionSupported) {
    cartesian = viewer.scene.pickPosition(screenPosition);
  }
  if (!cartesian) {
    cartesian = viewer.camera.pickEllipsoid(screenPosition, viewer.scene.globe.ellipsoid);
  }
  if (!cartesian) return null;
  const cartographic = Cartographic.fromCartesian(cartesian);
  return {
    lat: toDeg(cartographic.latitude),
    lng: normalizeLng(toDeg(cartographic.longitude)),
  };
}

function pointerEventPosition(event) {
  return { x: event.clientX, y: event.clientY };
}

function fallbackScreenToLatLng(screenPosition) {
  if (!fallbackSphere.value || !screenPosition) return null;
  const rect = fallbackSphere.value.getBoundingClientRect();
  const x = ((screenPosition.x - rect.left) / rect.width) * 2 - 1;
  const y = ((screenPosition.y - rect.top) / rect.height) * 2 - 1;
  if (x * x + y * y > 1) return null;
  const [centerLat, centerLng] = props.center || [0, 0];
  return {
    lat: CesiumMath.clamp(Number(centerLat) - y * 80, -85, 85),
    lng: normalizeLng(Number(centerLng) + x * 160),
  };
}

function setCameraInteraction(enabled) {
  if (!viewer) return;
  const controller = viewer.scene.screenSpaceCameraController;
  controller.enableRotate = enabled && mapMode.value === '3d';
  controller.enableTranslate = enabled;
  controller.enableZoom = enabled;
  controller.enableTilt = enabled && mapMode.value === '3d';
  controller.enableLook = enabled && mapMode.value === '3d';
}

function setupInteractions() {
  handler = new ScreenSpaceEventHandler(viewer.scene.canvas);
  handler.setInputAction((event) => {
    pointerDown = event.position;
    if (props.interactionMode !== 'draw') return;
    const point = screenToLatLng(event.position);
    if (!point) return;
    drawStart = point;
    draftCircle = { lat: point.lat, lng: point.lng, radiusKm: 0.001 };
    setCameraInteraction(false);
    renderLayers({ refocus: false });
  }, ScreenSpaceEventType.LEFT_DOWN);

  handler.setInputAction((event) => {
    if (props.interactionMode !== 'draw' || !drawStart || !draftCircle) return;
    const point = screenToLatLng(event.endPosition);
    if (!point) return;
    draftCircle = {
      lat: drawStart.lat,
      lng: drawStart.lng,
      radiusKm: Math.max(0.001, haversineKm(drawStart, point)),
    };
    renderLayers({ refocus: false });
  }, ScreenSpaceEventType.MOUSE_MOVE);

  handler.setInputAction((event) => {
    if (props.interactionMode === 'draw' && drawStart && draftCircle) {
      emit('circle-drawn', draftCircle);
      drawStart = null;
      draftCircle = null;
      pointerDown = null;
      setCameraInteraction(true);
      return;
    }
    if (props.interactionMode !== 'point' || !pointerDown) return;
    const movement = Math.hypot(event.position.x - pointerDown.x, event.position.y - pointerDown.y);
    pointerDown = null;
    if (movement > 6) return;
    const point = screenToLatLng(event.position);
    if (point) emit('point-selected', point);
  }, ScreenSpaceEventType.LEFT_UP);
}

function createNaturalEarthLayer() {
  return ImageryLayer.fromProviderAsync(
    TileMapServiceImageryProvider.fromUrl(buildModuleUrl('Assets/Textures/NaturalEarthII')),
  );
}

async function createPreviewBaseLayer() {
  if (!PREVIEW_IMAGERY_URL) return createNaturalEarthLayer();
  try {
    const provider = await ArcGisMapServerImageryProvider.fromUrl(PREVIEW_IMAGERY_URL, {
      enablePickFeatures: false,
    });
    return new ImageryLayer(provider);
  } catch (error) {
    console.warn('Preview imagery unavailable, falling back to NaturalEarthII.', error);
    return createNaturalEarthLayer();
  }
}

function replaceBaseLayer(layer) {
  if (!viewer || !layer) return;
  viewer.imageryLayers.removeAll(true);
  previewLayer = null;
  viewer.imageryLayers.add(layer, 0);
}

function installPreviewLayer(layer) {
  if (!viewer || !layer) return;
  if (previewLayer && viewer.imageryLayers.contains(previewLayer)) viewer.imageryLayers.remove(previewLayer, true);
  previewLayer = layer;
  viewer.imageryLayers.add(layer);
}

async function setMapMode(mode) {
  if (mode === mapMode.value) return;
  mapMode.value = mode;
  drawStart = null;
  draftCircle = null;
  pointerDown = null;
  lastFocusSignature = '';
  setCameraInteraction(true);
  const requestGeneration = ++imageryGeneration;

  if (!viewer || useFallback.value) return;
  if (mode === '2d') {
    replaceBaseLayer(createNaturalEarthLayer());
    viewer.scene.morphTo2D(0);
  } else {
    const layer = await createPreviewBaseLayer();
    if (requestGeneration !== imageryGeneration || mapMode.value !== '3d' || !viewer || viewer.isDestroyed()) {
      destroyUnusedLayer(layer);
      return;
    }
    installPreviewLayer(layer);
    viewer.scene.morphTo3D(0);
  }
  renderLayers();
}

async function initViewer(generation) {
  if (!canCreateWebglContext()) {
    useFallback.value = true;
    return;
  }

  try {
    viewer = new Viewer(mapEl.value, {
      animation: false,
      baseLayer: createNaturalEarthLayer(),
      baseLayerPicker: false,
      fullscreenButton: false,
      geocoder: false,
      homeButton: false,
      infoBox: false,
      navigationHelpButton: false,
      sceneModePicker: false,
      selectionIndicator: false,
      sceneMode: SceneMode.SCENE3D,
      timeline: false,
      terrainProvider: new EllipsoidTerrainProvider(),
    });
  } catch (error) {
    console.warn('Cesium viewer initialization failed, falling back to static globe.', error);
    useFallback.value = true;
    return;
  }
  viewer.scene.globe.depthTestAgainstTerrain = false;
  viewer.scene.globe.enableLighting = false;
  viewer.scene.screenSpaceCameraController.minimumZoomDistance = MIN_3D_ZOOM_DISTANCE;
  viewer.scene.screenSpaceCameraController.maximumZoomDistance = MAX_ZOOM_DISTANCE;

  overlaySource = new CustomDataSource('cube-map-overlays');
  const currentViewer = viewer;
  await currentViewer.dataSources.add(overlaySource);
  if (!viewerIsCurrent(currentViewer, generation)) return;
  setupInteractions();
  renderLayers();

  const requestGeneration = ++imageryGeneration;
  createPreviewBaseLayer().then((layer) => {
    if (!viewerIsCurrent(currentViewer, generation) || requestGeneration !== imageryGeneration || mapMode.value !== '3d') {
      destroyUnusedLayer(layer);
      return;
    }
    installPreviewLayer(layer);
  }).catch((error) => {
    console.warn('Preview imagery replacement failed; keeping NaturalEarthII.', error);
  });
}

function handleFallbackPointerDown(event) {
  if (!useFallback.value) return;
  const position = pointerEventPosition(event);
  pointerDown = position;
  if (props.interactionMode !== 'draw') return;
  const point = screenToLatLng(position);
  if (!point) return;
  event.preventDefault();
  drawStart = point;
  draftCircle = { lat: point.lat, lng: point.lng, radiusKm: 0.001 };
}

function handleFallbackPointerMove(event) {
  if (!useFallback.value || props.interactionMode !== 'draw' || !drawStart || !draftCircle) return;
  const point = screenToLatLng(pointerEventPosition(event));
  if (!point) return;
  draftCircle = {
    lat: drawStart.lat,
    lng: drawStart.lng,
    radiusKm: Math.max(0.001, haversineKm(drawStart, point)),
  };
}

function handleFallbackPointerUp(event) {
  if (!useFallback.value) return;
  const position = pointerEventPosition(event);
  if (props.interactionMode === 'draw' && drawStart && draftCircle) {
    emit('circle-drawn', draftCircle);
    drawStart = null;
    draftCircle = null;
    pointerDown = null;
    return;
  }
  if (props.interactionMode !== 'point' || !pointerDown) return;
  const movement = Math.hypot(position.x - pointerDown.x, position.y - pointerDown.y);
  pointerDown = null;
  if (movement > 6) return;
  const point = screenToLatLng(position);
  if (point) emit('point-selected', point);
}

function handleFallbackPointerLeave() {
  if (!useFallback.value || props.interactionMode !== 'draw' || !drawStart) return;
  drawStart = null;
  draftCircle = null;
}

onMounted(() => {
  const generation = ++lifecycleGeneration;
  if (typeof ResizeObserver !== 'undefined' && mapEl.value) {
    resizeObserver = new ResizeObserver(() => {
      if (!viewer || viewer.isDestroyed()) return;
      viewer.resize();
      viewer.scene.requestRender();
    });
    resizeObserver.observe(mapEl.value);
  }
  initViewer(generation);
});

watch(() => props.markers, () => renderLayers(), { deep: true });
watch(() => props.geometries, () => renderLayers(), { deep: true });
watch(() => props.circle, () => renderLayers(), { deep: true });
watch(() => props.center, () => {
  lastFocusSignature = '';
  focusDefault();
}, { deep: true });
watch(() => props.interactionMode, () => {
  if (props.interactionMode !== 'draw') {
    drawStart = null;
    draftCircle = null;
    setCameraInteraction(true);
    renderLayers({ refocus: false });
  }
});

onBeforeUnmount(() => {
  lifecycleGeneration += 1;
  imageryGeneration += 1;
  resizeObserver?.disconnect();
  resizeObserver = null;
  handler?.destroy();
  handler = null;
  viewer?.destroy();
  viewer = null;
  previewLayer = null;
});
</script>

<template>
  <div
    ref="mapEl"
    class="globe-map cesium-map"
    :class="{
      'is-picking': ['point', 'draw'].includes(props.interactionMode),
      'is-fallback-picking': useFallback && ['point', 'draw'].includes(props.interactionMode),
      'is-map-2d': mapMode === '2d',
    }"
  >
    <div v-if="!useFallback" class="map-mode-switch" aria-label="地图模式">
      <button
        type="button"
        class="map-mode-button"
        :class="{ active: mapMode === '3d' }"
        :aria-pressed="mapMode === '3d'"
        @mousedown.stop
        @pointerdown.stop
        @click.stop="setMapMode('3d')"
      >
        3D
      </button>
      <button
        type="button"
        class="map-mode-button"
        :class="{ active: mapMode === '2d' }"
        :aria-pressed="mapMode === '2d'"
        @mousedown.stop
        @pointerdown.stop
        @click.stop="setMapMode('2d')"
      >
        2D
      </button>
    </div>
    <div
      v-if="useFallback"
      class="globe-fallback"
      @pointerdown="handleFallbackPointerDown"
      @pointermove="handleFallbackPointerMove"
      @pointerup="handleFallbackPointerUp"
      @pointerleave="handleFallbackPointerLeave"
    >
      <div ref="fallbackSphere" class="globe-fallback-sphere">
        <img class="globe-fallback-texture" :src="earthBlueMarbleUrl" alt="">
        <div class="globe-fallback-shade"></div>
      </div>
    </div>
  </div>
</template>
