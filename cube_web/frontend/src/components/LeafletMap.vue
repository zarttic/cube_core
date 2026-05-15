<script setup>
import { onBeforeUnmount, onMounted, ref, watch } from 'vue';
import L from 'leaflet';
import markerIcon2x from 'leaflet/dist/images/marker-icon-2x.png';
import markerIcon from 'leaflet/dist/images/marker-icon.png';
import markerShadow from 'leaflet/dist/images/marker-shadow.png';

L.Icon.Default.mergeOptions({
  iconRetinaUrl: markerIcon2x,
  iconUrl: markerIcon,
  shadowUrl: markerShadow,
});

const props = defineProps({
  center: { type: Array, default: () => [39.9042, 116.4074] },
  zoom: { type: Number, default: 7 },
  markers: { type: Array, default: () => [] },
  geometries: { type: Array, default: () => [] },
  circle: { type: Object, default: null },
  interactionMode: { type: String, default: 'none' },
});

const emit = defineEmits(['point-selected', 'circle-drawn']);

const mapEl = ref(null);
let map;
let markerLayer;
let drawingLayer;
let geometryLayer;
let draftCircle;
let drawStart = null;

function renderMarkers() {
  if (!map || !markerLayer) return;
  markerLayer.clearLayers();
  props.markers.forEach((marker) => {
    L.marker(marker.position).bindPopup(marker.label || '').addTo(markerLayer);
  });
}

function renderGeometries() {
  if (!map || !geometryLayer) return;
  geometryLayer.clearLayers();
  props.geometries.forEach((item) => {
    if (!item.geometry) return;
    L.geoJSON(item.geometry, {
      style: {
        color: item.color || '#1a5fb4',
        weight: item.weight || 2,
        fillColor: item.fillColor || item.color || '#1a5fb4',
        fillOpacity: item.fillOpacity ?? 0.18,
      },
    }).bindPopup(item.label || '').addTo(geometryLayer);
  });
  const bounds = geometryLayer.getBounds?.();
  if (bounds && bounds.isValid()) {
    map.fitBounds(bounds.pad(0.15));
  }
}

function renderCircle() {
  if (!drawingLayer) return;
  drawingLayer.clearLayers();
  if (!props.circle) return;
  L.circle([props.circle.lat, props.circle.lng], {
    radius: props.circle.radiusKm * 1000,
    color: '#f5c211',
    weight: 2,
    fillColor: '#f5c211',
    fillOpacity: 0.18,
  }).addTo(drawingLayer);
}

function latLngDistanceMeters(a, b) {
  return map ? map.distance(a, b) : 0;
}

function handleClick(event) {
  if (props.interactionMode !== 'point') return;
  emit('point-selected', {
    lat: event.latlng.lat,
    lng: event.latlng.lng,
  });
}

function handleMouseDown(event) {
  if (props.interactionMode !== 'draw') return;
  drawStart = event.latlng;
  if (map) map.dragging.disable();
  if (drawingLayer) drawingLayer.clearLayers();
  draftCircle = L.circle(drawStart, {
    radius: 1,
    color: '#f5c211',
    weight: 2,
    fillColor: '#f5c211',
    fillOpacity: 0.18,
  }).addTo(drawingLayer);
}

function handleMouseMove(event) {
  if (props.interactionMode !== 'draw' || !drawStart || !draftCircle) return;
  draftCircle.setRadius(Math.max(1, latLngDistanceMeters(drawStart, event.latlng)));
}

function handleMouseUp(event) {
  if (props.interactionMode !== 'draw' || !drawStart || !draftCircle) return;
  const radiusMeters = Math.max(1, latLngDistanceMeters(drawStart, event.latlng));
  draftCircle.setRadius(radiusMeters);
  emit('circle-drawn', {
    lat: drawStart.lat,
    lng: drawStart.lng,
    radiusKm: radiusMeters / 1000,
  });
  drawStart = null;
  draftCircle = null;
  if (map) map.dragging.enable();
}

function clearDrawingLayer() {
  if (!drawingLayer) return;
  drawingLayer.clearLayers();
  drawStart = null;
  draftCircle = null;
}

onMounted(() => {
  map = L.map(mapEl.value, { zoomControl: true }).setView(props.center, props.zoom);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; OpenStreetMap contributors',
  }).addTo(map);
  markerLayer = L.layerGroup().addTo(map);
  drawingLayer = L.layerGroup().addTo(map);
  geometryLayer = L.layerGroup().addTo(map);
  map.on('click', handleClick);
  map.on('mousedown', handleMouseDown);
  map.on('mousemove', handleMouseMove);
  map.on('mouseup', handleMouseUp);
  renderMarkers();
  renderGeometries();
  renderCircle();
  setTimeout(() => map.invalidateSize(), 0);
});

watch(() => props.markers, renderMarkers, { deep: true });
watch(() => props.geometries, renderGeometries, { deep: true });
watch(() => props.circle, renderCircle, { deep: true });

watch(() => props.interactionMode, () => {
  if (!props.circle) clearDrawingLayer();
  if (map) map.dragging.enable();
});

onBeforeUnmount(() => {
  if (map) {
    map.off('click', handleClick);
    map.off('mousedown', handleMouseDown);
    map.off('mousemove', handleMouseMove);
    map.off('mouseup', handleMouseUp);
    map.remove();
    map = null;
  }
});
</script>

<template>
  <div ref="mapEl" class="leaflet-map"></div>
</template>
