<script setup>
import { computed, onMounted, ref } from 'vue';
import { ElMessage, ElMessageBox } from 'element-plus';
import { Refresh, RefreshLeft, Check, Connection, DataLine } from '@element-plus/icons-vue';

import { apiPrefixes, requestJson } from '@/api/client';

const loading = ref(false);
const saving = ref(false);
const resetting = ref(false);
const updatedAt = ref('');
const runtime = ref({});
const config = ref(emptyConfig());

const optical = computed(() => config.value.partition.optical);
const ingest = computed(() => config.value.ingest.optical);
const quality = computed(() => config.value.quality.optical);
const gridTypeLabels = {
  geohash: 'GeoHash格网',
  mgrs: 'MGRS格网',
  isea4h: '六边形格网',
};

function formatGridType(gridType) {
  return gridTypeLabels[gridType] || gridType || '-';
}

function emptyConfig() {
  return {
    partition: {
      optical: {
        grid_type: 'geohash',
        grid_level: 5,
        target_crs: 'EPSG:4326',
        cover_mode: 'intersect',
        time_granularity: 'day',
        max_cells_per_asset: 0,
        partition_backend: 'ray',
        ray_parallelism: 0,
        partition_prefix_len: 3,
        chunk_size: 0,
        product_family: 'auto',
        sample_mean: false,
      },
    },
    ingest: {
      optical: {
        dataset: 'optical',
        sensor: 'optical_mosaic',
        quality_rule: 'best_quality_wins',
        allow_failed_quality: false,
        metadata_backend: 'postgres',
        asset_storage_backend: 'minio',
        minio_endpoint: '',
        minio_bucket: 'cube',
        minio_prefix: 'cube/entity',
        minio_secure: false,
        minio_upload_workers: 8,
      },
    },
    quality: {
      optical: {
        target_crs: 'EPSG:4326',
        history_limit: 20,
      },
    },
  };
}

function mergeConfig(nextConfig) {
  const defaults = emptyConfig();
  config.value = {
    partition: {
      optical: { ...defaults.partition.optical, ...(nextConfig?.partition?.optical || {}) },
    },
    ingest: {
      optical: { ...defaults.ingest.optical, ...(nextConfig?.ingest?.optical || {}) },
    },
    quality: {
      optical: { ...defaults.quality.optical, ...(nextConfig?.quality?.optical || {}) },
    },
  };
}

function applyResponse(response) {
  mergeConfig(response.config || response.defaults || emptyConfig());
  runtime.value = response.runtime || {};
  updatedAt.value = response.updated_at || '';
}

async function loadConfig() {
  loading.value = true;
  try {
    const { configPrefix } = apiPrefixes();
    applyResponse(await requestJson(`${configPrefix}/get`, {}));
  } catch (error) {
    ElMessage.error(error.message);
  } finally {
    loading.value = false;
  }
}

async function saveConfig() {
  saving.value = true;
  try {
    const { configPrefix } = apiPrefixes();
    applyResponse(await requestJson(`${configPrefix}/update`, { config: config.value }));
    ElMessage.success('配置已保存');
  } catch (error) {
    ElMessage.error(error.message);
  } finally {
    saving.value = false;
  }
}

async function resetConfig() {
  try {
    await ElMessageBox.confirm('确认恢复系统默认配置？当前保存的参数会被覆盖。', '恢复默认', {
      confirmButtonText: '恢复默认',
      cancelButtonText: '取消',
      type: 'warning',
    });
  } catch {
    return;
  }
  resetting.value = true;
  try {
    const { configPrefix } = apiPrefixes();
    applyResponse(await requestJson(`${configPrefix}/reset`, {}));
    ElMessage.success('已恢复默认配置');
  } catch (error) {
    ElMessage.error(error.message);
  } finally {
    resetting.value = false;
  }
}

function formatUpdatedAt(value) {
  if (!value) return '尚未保存';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString('zh-CN');
}

onMounted(loadConfig);
</script>

<template>
  <main class="config-page">
    <section class="config-toolbar">
      <div>
        <div class="config-eyebrow">配置管理</div>
        <h1>剖分系统参数配置</h1>
        <p>集中管理光学剖分、自动质检与演示入库的默认参数。</p>
      </div>
      <div class="config-actions">
        <el-button :icon="Refresh" :loading="loading" @click="loadConfig">重新加载</el-button>
        <el-button :icon="RefreshLeft" :loading="resetting" @click="resetConfig">恢复默认</el-button>
        <el-button type="primary" :icon="Check" :loading="saving" @click="saveConfig">保存配置</el-button>
      </div>
    </section>

    <section v-loading="loading" class="config-layout">
      <div class="config-main-panel">
        <el-tabs>
          <el-tab-pane label="光学剖分">
            <el-form label-position="top" class="config-form">
              <div class="config-form-grid">
                <el-form-item label="格网类型">
                  <el-select v-model="optical.grid_type">
                    <el-option label="GeoHash格网" value="geohash" />
                    <el-option label="MGRS格网" value="mgrs" />
                    <el-option label="六边形格网" value="isea4h" />
                  </el-select>
                </el-form-item>
                <el-form-item label="格网层级">
                  <el-input-number v-model="optical.grid_level" :min="optical.grid_type === 'geohash' ? 1 : 0" :max="optical.grid_type === 'mgrs' ? 5 : 15" />
                </el-form-item>
                <el-form-item label="目标 CRS">
                  <el-select v-model="optical.target_crs" filterable allow-create>
                    <el-option label="EPSG:4326" value="EPSG:4326" />
                    <el-option label="EPSG:3857" value="EPSG:3857" />
                  </el-select>
                </el-form-item>
                <el-form-item label="覆盖模式">
                  <el-select v-model="optical.cover_mode">
                    <el-option label="相交" value="intersect" />
                    <el-option label="包含" value="contain" />
                  </el-select>
                </el-form-item>
                <el-form-item label="时间粒度">
                  <el-select v-model="optical.time_granularity">
                    <el-option label="秒" value="second" />
                    <el-option label="分钟" value="minute" />
                    <el-option label="小时" value="hour" />
                    <el-option label="日" value="day" />
                    <el-option label="月" value="month" />
                  </el-select>
                </el-form-item>
                <el-form-item label="单资产最大格网数">
                  <el-input-number v-model="optical.max_cells_per_asset" :min="0" :step="1000" />
                </el-form-item>
              </div>

              <div class="config-section-title">执行参数</div>
              <div class="config-form-grid">
                <el-form-item label="执行后端">
                  <el-select v-model="optical.partition_backend">
                    <el-option label="Ray" value="ray" />
                    <el-option label="Thread" value="thread" />
                    <el-option label="Process" value="process" />
                  </el-select>
                </el-form-item>
                <el-form-item label="Ray 并行度">
                  <el-input-number v-model="optical.ray_parallelism" :min="0" :max="512" />
                </el-form-item>
                <el-form-item label="分区前缀长度">
                  <el-input-number v-model="optical.partition_prefix_len" :min="1" :max="12" />
                </el-form-item>
                <el-form-item label="Chunk Size">
                  <el-input-number v-model="optical.chunk_size" :min="0" :step="1000" />
                </el-form-item>
                <el-form-item label="产品族">
                  <el-input v-model="optical.product_family" />
                </el-form-item>
                <el-form-item label="样本均值">
                  <el-switch v-model="optical.sample_mean" />
                </el-form-item>
              </div>
            </el-form>
          </el-tab-pane>

          <el-tab-pane label="质检报告">
            <el-form label-position="top" class="config-form">
              <div class="config-form-grid">
                <el-form-item label="光学质检目标 CRS">
                  <el-select v-model="quality.target_crs" filterable allow-create>
                    <el-option label="EPSG:4326" value="EPSG:4326" />
                  </el-select>
                </el-form-item>
                <el-form-item label="历史记录默认条数">
                  <el-input-number v-model="quality.history_limit" :min="1" :max="200" />
                </el-form-item>
              </div>
            </el-form>
          </el-tab-pane>

          <el-tab-pane label="入库演示">
            <el-form label-position="top" class="config-form">
              <div class="config-form-grid">
                <el-form-item label="默认 Dataset">
                  <el-input v-model="ingest.dataset" />
                </el-form-item>
                <el-form-item label="默认 Sensor">
                  <el-input v-model="ingest.sensor" />
                </el-form-item>
                <el-form-item label="质量策略">
                  <el-select v-model="ingest.quality_rule">
                    <el-option label="质量优先" value="best_quality_wins" />
                    <el-option label="最新优先" value="latest_wins" />
                  </el-select>
                </el-form-item>
                <el-form-item label="允许失败质检入库">
                  <el-switch v-model="ingest.allow_failed_quality" />
                </el-form-item>
                <el-form-item label="实体元数据">
                  <el-select v-model="ingest.metadata_backend">
                    <el-option label="不写入" value="none" />
                    <el-option label="PostgreSQL" value="postgres" />
                  </el-select>
                </el-form-item>
                <el-form-item label="实体瓦片存储">
                  <el-select v-model="ingest.asset_storage_backend">
                    <el-option label="本地目录" value="local" />
                    <el-option label="MinIO" value="minio" />
                  </el-select>
                </el-form-item>
                <el-form-item label="MinIO Endpoint">
                  <el-input v-model="ingest.minio_endpoint" placeholder="由 CUBE_WEB_MINIO_ENDPOINT 或 MINIO_ENDPOINT 提供" />
                </el-form-item>
                <el-form-item label="MinIO Bucket">
                  <el-input v-model="ingest.minio_bucket" />
                </el-form-item>
                <el-form-item label="MinIO Prefix">
                  <el-input v-model="ingest.minio_prefix" />
                </el-form-item>
                <el-form-item label="MinIO 上传线程">
                  <el-input-number v-model="ingest.minio_upload_workers" :min="1" :max="128" />
                </el-form-item>
                <el-form-item label="MinIO TLS">
                  <el-switch v-model="ingest.minio_secure" />
                </el-form-item>
              </div>
            </el-form>
          </el-tab-pane>
        </el-tabs>
      </div>

      <aside class="config-side-panel">
        <div class="config-status-card">
          <div class="status-icon"><DataLine /></div>
          <div>
            <span>配置存储</span>
            <strong>PostgreSQL JSONB</strong>
            <small>{{ runtime.postgres_dsn || '-' }}</small>
          </div>
        </div>
        <div class="config-status-card">
          <div class="status-icon"><Connection /></div>
          <div>
            <span>Ray 地址</span>
            <strong>{{ runtime.ray_address || '未设置环境变量' }}</strong>
            <small>剖分后端为 ray 时使用</small>
          </div>
        </div>
        <div class="config-status-card">
          <div class="status-icon"><Connection /></div>
          <div>
            <span>MinIO 存储</span>
            <strong>{{ runtime.minio?.endpoint || '未设置环境变量' }}</strong>
            <small>Bucket: {{ runtime.minio?.bucket || '-' }} · TLS: {{ runtime.minio?.secure ? '启用' : '关闭' }}</small>
          </div>
        </div>
        <div class="config-kv-list">
          <div class="config-kv">
            <span>配置域</span>
            <strong>{{ runtime.config_scope || 'cube_web' }}</strong>
          </div>
          <div class="config-kv">
            <span>更新时间</span>
            <strong>{{ formatUpdatedAt(updatedAt) }}</strong>
          </div>
          <div class="config-kv">
            <span>当前格网</span>
            <strong>{{ formatGridType(optical.grid_type) }} / {{ optical.grid_level }} 级</strong>
          </div>
        </div>
      </aside>
    </section>
  </main>
</template>
