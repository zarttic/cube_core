import { computed, reactive, ref } from 'vue';
import { defineStore } from 'pinia';

import { requestGet, requestJson, requestPost } from '@/api/client';
import { pageQuery, normalizePageResponse } from '@/api/pagination';
import { createRequestScope } from '@/api/requestScope';

const detailTabs = [
  'overview', 'scenes', 'bands', 'outputs', 'grid', 'tiles', 'indexes',
  'ingest-records', 'quality', 'publications', 'provenance',
];
const paginatedTabs = detailTabs.filter((tab) => tab !== 'overview');

function emptyDetail() {
  return Object.fromEntries(detailTabs.map((tab) => [tab, null]));
}

function emptyTabPages() {
  return Object.fromEntries(paginatedTabs.map((tab) => [tab, { page: 1, pageSize: 20, total: 0 }]));
}

export const useDatasetsStore = defineStore('datasets', () => {
  const filters = reactive({
    keyword: '', dataType: '', productType: '', ingestStatus: '', qualityStatus: '',
    publishStatus: '', archived: '', timeStart: '', timeEnd: '', sortBy: 'updated_at', sortOrder: 'desc',
  });
  const pageState = reactive({ page: 1, pageSize: 20, total: 0 });
  const records = ref([]);
  const summary = ref({ dataset_count: 0, scene_count: 0, ready_scene_count: 0, failed_scene_count: 0 });
  const loading = ref(false);
  const error = ref('');
  const actionLoading = ref(false);
  const hiddenRoles = ref([]);
  const roleRestrictionsLoading = ref(false);
  const selectedDatasetId = ref('');
  const detailVisible = ref(false);
  const detailLoading = ref(false);
  const detail = ref(emptyDetail());
  const activeTab = ref('overview');
  const tabPages = reactive(emptyTabPages());
  const listScope = createRequestScope();
  const detailScope = createRequestScope();
  const roleRestrictionsScope = createRequestScope();
  const tabScopes = Object.fromEntries(paginatedTabs.map((tab) => [tab, createRequestScope()]));
  let detailGeneration = 0;

  const selectedDataset = computed(() => detail.value.overview);

  function listParameters() {
    return {
      keyword: filters.keyword.trim(), data_type: filters.dataType, product_type: filters.productType,
      ingest_status: filters.ingestStatus, quality_status: filters.qualityStatus,
      publish_status: filters.publishStatus, archived: filters.archived,
      time_start: filters.timeStart, time_end: filters.timeEnd,
      page: pageState.page, page_size: pageState.pageSize, sort_by: filters.sortBy, sort_order: filters.sortOrder,
    };
  }

  async function loadList() {
    const request = listScope.begin();
    loading.value = true;
    error.value = '';
    try {
      const response = await requestGet(`/v1/datasets?${pageQuery(listParameters())}`, { signal: request.signal });
      if (!listScope.isCurrent(request.token)) return;
      const page = normalizePageResponse(response, pageState.page, pageState.pageSize);
      records.value = page.items;
      summary.value = response.summary || summary.value;
      Object.assign(pageState, { total: page.total, page: page.page, pageSize: page.pageSize });
      if (!records.value.length && pageState.total > 0 && pageState.page > 1) {
        pageState.page = Math.max(1, Math.ceil(pageState.total / pageState.pageSize));
        await loadList();
      }
    } catch (requestError) {
      if (request.signal.aborted || !listScope.isCurrent(request.token)) return;
      error.value = requestError.message || '数据集加载失败';
      throw requestError;
    } finally {
      if (listScope.isCurrent(request.token)) loading.value = false;
    }
  }

  function resetDetail(nextDatasetId = '') {
    detailScope.cancel();
    roleRestrictionsScope.cancel();
    Object.values(tabScopes).forEach((scope) => scope.cancel());
    detailGeneration += 1;
    selectedDatasetId.value = nextDatasetId;
    detailVisible.value = Boolean(nextDatasetId);
    activeTab.value = 'overview';
    detail.value = emptyDetail();
    hiddenRoles.value = [];
    roleRestrictionsLoading.value = false;
    Object.assign(tabPages, emptyTabPages());
  }

  async function openDetail(datasetId) {
    if (!datasetId) return;
    resetDetail(datasetId);
    const generation = detailGeneration;
    const request = detailScope.begin();
    detailLoading.value = true;
    error.value = '';
    try {
      const overview = await requestGet(`/v1/datasets/${encodeURIComponent(datasetId)}`, { signal: request.signal });
      if (selectedDatasetId.value !== datasetId || generation !== detailGeneration || !detailScope.isCurrent(request.token)) return;
      detail.value.overview = overview;
      const restrictionRequest = roleRestrictionsScope.begin();
      roleRestrictionsLoading.value = true;
      try {
        const restrictions = await requestGet(`/v1/datasets/${encodeURIComponent(datasetId)}/role-restrictions`, { signal: restrictionRequest.signal });
        if (selectedDatasetId.value === datasetId && generation === detailGeneration && roleRestrictionsScope.isCurrent(restrictionRequest.token)) {
          hiddenRoles.value = restrictions.hidden_roles || [];
        }
      } catch (requestError) {
        if (!restrictionRequest.signal.aborted && selectedDatasetId.value === datasetId && generation === detailGeneration && roleRestrictionsScope.isCurrent(restrictionRequest.token)) {
          error.value = requestError.message || '数据集权限加载失败';
        }
      } finally {
        if (selectedDatasetId.value === datasetId && generation === detailGeneration && roleRestrictionsScope.isCurrent(restrictionRequest.token)) roleRestrictionsLoading.value = false;
      }
    } catch (requestError) {
      if (request.signal.aborted || selectedDatasetId.value !== datasetId || generation !== detailGeneration || !detailScope.isCurrent(request.token)) return;
      error.value = requestError.message || '数据集详情加载失败';
      throw requestError;
    } finally {
      if (selectedDatasetId.value === datasetId && generation === detailGeneration && detailScope.isCurrent(request.token)) detailLoading.value = false;
    }
  }

  async function loadDetailTab(tab = activeTab.value) {
    if (!paginatedTabs.includes(tab) || !selectedDatasetId.value) return;
    const datasetId = selectedDatasetId.value;
    const generation = detailGeneration;
    const request = tabScopes[tab].begin();
    const page = tabPages[tab];
    try {
      const query = pageQuery({ page: page.page, page_size: page.pageSize });
      const response = await requestGet(`/v1/datasets/${encodeURIComponent(datasetId)}/${tab}?${query}`, { signal: request.signal });
      if (selectedDatasetId.value !== datasetId || generation !== detailGeneration || !tabScopes[tab].isCurrent(request.token)) return;
      const normalized = normalizePageResponse(response, page.page, page.pageSize);
      detail.value[tab] = normalized;
      Object.assign(page, { page: normalized.page, pageSize: normalized.pageSize, total: normalized.total });
    } catch (requestError) {
      if (request.signal.aborted || selectedDatasetId.value !== datasetId || generation !== detailGeneration || !tabScopes[tab].isCurrent(request.token)) return;
      error.value = requestError.message || '数据集明细加载失败';
      throw requestError;
    }
  }

  async function setActiveTab(tab) {
    activeTab.value = tab;
    await loadDetailTab(tab);
  }

  async function setTabPage(tab, page) {
    if (!tabPages[tab]) return;
    tabPages[tab].page = page;
    await loadDetailTab(tab);
  }

  async function setTabPageSize(tab, pageSize) {
    if (!tabPages[tab]) return;
    Object.assign(tabPages[tab], { pageSize, page: 1 });
    await loadDetailTab(tab);
  }

  async function runAction(path, payload = {}, method = 'POST', refreshTab = '') {
    if (!selectedDatasetId.value) return;
    actionLoading.value = true;
    error.value = '';
    try {
      const response = method === 'PATCH'
        ? await requestJson(path, payload, { method: 'PATCH' })
        : await requestPost(path, payload);
      await openDetail(selectedDatasetId.value);
      if (refreshTab) {
        activeTab.value = refreshTab;
        await loadDetailTab(refreshTab);
      }
      await loadList();
      return response;
    } catch (requestError) {
      error.value = requestError.message || '数据集操作失败';
      throw requestError;
    } finally {
      actionLoading.value = false;
    }
  }

  function updateMetadata(payload) {
    const id = encodeURIComponent(selectedDatasetId.value);
    return runAction(`/v1/datasets/${id}`, payload, 'PATCH');
  }

  async function updateRoleRestrictions(roles) {
    if (!selectedDatasetId.value) return;
    actionLoading.value = true;
    error.value = '';
    try {
      const response = await requestJson(`/v1/datasets/${encodeURIComponent(selectedDatasetId.value)}/role-restrictions`, {
        hidden_roles: roles,
      }, { method: 'PUT' });
      hiddenRoles.value = response.hidden_roles || [];
      return response;
    } catch (requestError) {
      error.value = requestError.message || '数据集权限保存失败';
      throw requestError;
    } finally {
      actionLoading.value = false;
    }
  }

  function reassignScene(sceneId, targetDatasetId, reason) {
    const id = encodeURIComponent(selectedDatasetId.value);
    return runAction(`/v1/datasets/${id}/scenes/${encodeURIComponent(sceneId)}/reassign`, {
      target_dataset_id: targetDatasetId, reason,
    }, 'POST', 'scenes');
  }

  function rerunQuality() {
    return runAction(`/v1/datasets/${encodeURIComponent(selectedDatasetId.value)}/quality-runs`, {}, 'POST', 'quality');
  }

  function requestIngest() {
    return runAction(`/v1/datasets/${encodeURIComponent(selectedDatasetId.value)}/ingest`, {}, 'POST', 'ingest-records');
  }

  function retryBandIngest(bandUnitId) {
    return runAction(`/v1/datasets/${encodeURIComponent(selectedDatasetId.value)}/bands/${encodeURIComponent(bandUnitId)}/ingest-retry`, {}, 'POST', 'ingest-records');
  }

  function publish(targets = []) {
    return runAction(`/v1/datasets/${encodeURIComponent(selectedDatasetId.value)}/publish`, { targets }, 'POST', 'publications');
  }

  function withdraw(publicationId) {
    return runAction(`/v1/datasets/${encodeURIComponent(selectedDatasetId.value)}/publications/${encodeURIComponent(publicationId)}/withdraw`, {}, 'POST', 'publications');
  }

  function archive(reason) {
    return runAction(`/v1/datasets/${encodeURIComponent(selectedDatasetId.value)}/archive`, { reason }, 'POST');
  }

  function closeDetail() {
    resetDetail();
    detailLoading.value = false;
  }

  function dispose() {
    listScope.dispose();
    resetDetail();
  }

  return {
    filters, pageState, records, summary, loading, error, actionLoading, hiddenRoles, roleRestrictionsLoading, selectedDatasetId, selectedDataset,
    detailVisible, detailLoading, detail, activeTab, tabPages, loadList, openDetail, loadDetailTab,
    setActiveTab, setTabPage, setTabPageSize, updateMetadata, updateRoleRestrictions, reassignScene, rerunQuality, requestIngest,
    retryBandIngest, publish, withdraw, archive, closeDetail, dispose,
  };
});
