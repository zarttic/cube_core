import { computed, reactive, ref } from 'vue';
import { defineStore } from 'pinia';

import { requestGet, requestJson, requestPost } from '@/api/client';
import { pageQuery, normalizePageResponse } from '@/api/pagination';
import { createRequestScope } from '@/api/requestScope';
import { notifyApiError } from '@/utils/errorHandler';

const detailTabs = [
  'overview', 'scenes', 'bands', 'outputs', 'grid', 'tiles', 'indexes',
  'ingest-records', 'quality', 'publications', 'provenance',
];
const paginatedTabs = detailTabs.filter((tab) => tab !== 'overview');
const GRID_DELETE_POLL_MS = 1000;
// 数据集删除改由后台任务执行（单次请求曾阻塞 13 分钟并拖慢整个库，2026-09-19）。
const DELETION_POLL_MS = 2000;
const DELETION_POLL_LIMIT_MS = 60 * 60 * 1000;

function emptyDetail() {
  return Object.fromEntries(detailTabs.map((tab) => [tab, null]));
}

function emptyTabPages() {
  return Object.fromEntries(paginatedTabs.map((tab) => [
    tab,
    { page: 1, pageSize: tab === 'grid' ? 120 : 20, total: 0 },
  ]));
}

export const useDatasetsStore = defineStore('datasets', () => {
  const filters = reactive({
    keyword: '', dataType: '',
    publishStatus: '', timeStart: '', timeEnd: '', sortBy: 'updated_at', sortOrder: 'desc',
  });
  const pageState = reactive({ page: 1, pageSize: 20, total: 0 });
  const records = ref([]);
  const summary = ref({ dataset_count: 0, scene_count: 0, ready_scene_count: 0, failed_scene_count: 0 });
  const loading = ref(false);
  const error = ref('');
  const actionLoading = ref(false);
  const hiddenRoles = ref([]);
  const roleRestrictionsLoading = ref(false);
  const pendingGridDeletes = reactive({});
  // deletionId -> {deletionId, datasetId, datasetTitle, status, progress, startedAt}
  const pendingDeletions = reactive({});
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
  const deletePollTimers = new Map();
  const deletionPollTimers = new Map();

  const selectedDataset = computed(() => detail.value.overview);
  const deletingDatasetIds = computed(
    () => new Set(Object.values(pendingDeletions).map((job) => job.datasetId)),
  );
  const activeDeletions = computed(() => Object.values(pendingDeletions));

  function listParameters() {
    return {
      keyword: filters.keyword.trim(), data_type: filters.dataType,
      publish_status: filters.publishStatus,
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
      // A dataset whose deletion is queued is gone as far as the user is
      // concerned; the worker removes the rows in the background.
      records.value = page.items.filter((row) => !deletingDatasetIds.value.has(row.dataset_id));
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

  async function runAction(path, payload = {}, method = 'POST', refreshTab = '', options = {}) {
    if (!selectedDatasetId.value) return;
    actionLoading.value = true;
    error.value = '';
    try {
      const response = method === 'PATCH'
        ? await requestJson(path, payload, { method: 'PATCH' })
        : method === 'DELETE'
          ? await requestJson(path, payload, { method: 'DELETE' })
        : await requestPost(path, payload);
      if (options.refresh !== false) {
        await openDetail(selectedDatasetId.value);
        if (refreshTab) {
          activeTab.value = refreshTab;
          await loadDetailTab(refreshTab);
        }
        await loadList();
      }
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

  function requestIngest() {
    return runAction(`/v1/datasets/${encodeURIComponent(selectedDatasetId.value)}/ingest`, {}, 'POST', 'ingest-records');
  }

  function retryBandIngest(bandUnitId) {
    return runAction(`/v1/datasets/${encodeURIComponent(selectedDatasetId.value)}/bands/${encodeURIComponent(bandUnitId)}/ingest-retry`, {}, 'POST', 'ingest-records');
  }

  function gridDeleteKey(datasetId, bandUnitId, gridType) {
    return `${datasetId}::${bandUnitId}::${gridType}`;
  }

  async function refreshAfterGridDelete(datasetId) {
    if (selectedDatasetId.value !== datasetId) return;
    await openDetail(datasetId);
    if (selectedDatasetId.value !== datasetId) return;
    activeTab.value = 'scenes';
    await loadDetailTab('scenes');
    await loadList();
  }

  async function pollGridDelete(datasetId, key, taskId) {
    if (pendingGridDeletes[key]?.taskId !== taskId) return;
    deletePollTimers.delete(key);
    try {
      const task = await requestGet(`/v1/partition/tasks/${encodeURIComponent(taskId)}`);
      if (pendingGridDeletes[key]?.taskId !== taskId) return;
      if (task.status === 'completed') {
        delete pendingGridDeletes[key];
        try {
          await refreshAfterGridDelete(datasetId);
        } catch (requestError) {
          notifyApiError(requestError, { silent: true, scope: 'grid-delete-refresh' });
          if (selectedDatasetId.value === datasetId) {
            error.value = `波段格网已删除，但页面刷新失败：${requestError.message || '请稍后刷新'}`;
          }
        }
        return;
      }
      if (['failed', 'cancelled'].includes(task.status)) {
        delete pendingGridDeletes[key];
        if (selectedDatasetId.value === datasetId) error.value = task.error || '波段格网删除失败';
        return;
      }
    } catch (requestError) {
      if (pendingGridDeletes[key]?.taskId !== taskId) return;
      notifyApiError(requestError, { silent: true, scope: 'grid-delete-poll' });
      delete pendingGridDeletes[key];
      if (selectedDatasetId.value === datasetId) error.value = requestError.message || '波段格网删除任务查询失败';
      return;
    }
    if (pendingGridDeletes[key]?.taskId === taskId) {
      deletePollTimers.set(key, setTimeout(() => {
        pollGridDelete(datasetId, key, taskId);
      }, GRID_DELETE_POLL_MS));
    }
  }

  async function deleteBandGrid(bandUnitId, gridType) {
    const datasetId = selectedDatasetId.value;
    const response = await runAction(
      `/v1/datasets/${encodeURIComponent(selectedDatasetId.value)}/bands/${encodeURIComponent(bandUnitId)}/grids/${encodeURIComponent(gridType)}`,
      {}, 'DELETE', 'scenes', { refresh: false },
    );
    if (response?.task_id && !['completed', 'failed', 'cancelled'].includes(response.status)) {
      const key = gridDeleteKey(datasetId, bandUnitId, gridType);
      pendingGridDeletes[key] = { taskId: response.task_id, status: response.status };
      void pollGridDelete(datasetId, key, response.task_id);
    }
    try {
      await refreshAfterGridDelete(datasetId);
    } catch (requestError) {
      if (selectedDatasetId.value === datasetId) {
        error.value = `删除任务已提交，但页面刷新失败：${requestError.message || '请稍后刷新'}`;
      }
    }
    return response;
  }

  function publish(targets = []) {
    return runAction(`/v1/datasets/${encodeURIComponent(selectedDatasetId.value)}/publish`, { targets }, 'POST', 'publications');
  }

  function withdraw(publicationId) {
    return runAction(`/v1/datasets/${encodeURIComponent(selectedDatasetId.value)}/publications/${encodeURIComponent(publicationId)}/withdraw`, {}, 'POST', 'publications');
  }

  async function pollDatasetDeletion(deletionId) {
    if (!pendingDeletions[deletionId]) return;
    deletionPollTimers.delete(deletionId);
    try {
      const record = await requestGet(`/v1/datasets/deletions/${encodeURIComponent(deletionId)}`);
      if (!pendingDeletions[deletionId]) return;
      Object.assign(pendingDeletions[deletionId], {
        status: record.status,
        progress: record.progress || null,
        // 失败重试时把原因带上，否则界面只显示“后台执行中”，看不出任务在打转。
        attemptCount: record.attempt_count ?? pendingDeletions[deletionId].attemptCount ?? 0,
        errorMessage: record.error_message || '',
      });
      const job = pendingDeletions[deletionId];
      if (record.status === 'succeeded') {
        delete pendingDeletions[deletionId];
        try {
          await loadList();
        } catch (refreshError) {
          notifyApiError(refreshError, { silent: true, scope: 'dataset-delete-refresh' });
          error.value = '删除已完成，但列表刷新失败，请手动刷新';
        }
        return;
      }
      if (record.status === 'failed') {
        delete pendingDeletions[deletionId];
        try {
          await loadList();
        } catch (refreshError) {
          notifyApiError(refreshError, { silent: true, scope: 'dataset-delete-refresh' });
        }
        // loadList() clears the error banner, so the failure goes last.
        error.value = record.error_message || '数据集删除失败';
        return;
      }
    } catch (requestError) {
      if (!pendingDeletions[deletionId]) return;
      notifyApiError(requestError, { silent: true, scope: 'dataset-delete-poll' });
    }
    const job = pendingDeletions[deletionId];
    if (!job) return;
    if (Date.now() - job.startedAt > DELETION_POLL_LIMIT_MS) {
      error.value = '删除任务仍在后台执行，请稍后在删除任务状态中确认';
      return;
    }
    deletionPollTimers.set(deletionId, setTimeout(() => {
      void pollDatasetDeletion(deletionId);
    }, DELETION_POLL_MS));
  }

  async function deleteDataset() {
    const datasetId = selectedDatasetId.value;
    if (!datasetId) return;
    actionLoading.value = true;
    error.value = '';
    try {
      const response = await requestJson(`/v1/datasets/${encodeURIComponent(datasetId)}`, {}, { method: 'DELETE' });
      const deletionId = String(response?.deletion_id || '');
      if (!deletionId) {
        // 服务端同步完成（旧契约）：直接刷新列表。
        resetDetail();
        await loadList();
        return response;
      }
      pendingDeletions[deletionId] = {
        deletionId,
        datasetId,
        datasetTitle: response.dataset_title || '',
        status: response.status || 'queued',
        progress: response.progress || null,
        attemptCount: response.attempt_count || 0,
        errorMessage: '',
        startedAt: Date.now(),
      };
      // Reset all detail identity and tab state before loading the list again;
      // otherwise a reopened drawer can display the deleted dataset briefly.
      resetDetail();
      await loadList();
      void pollDatasetDeletion(deletionId);
      return response;
    } catch (requestError) {
      error.value = requestError.message || '数据集删除失败';
      throw requestError;
    } finally {
      actionLoading.value = false;
    }
  }

  // 删除不再向用户弹提示：提交后列表立刻不再显示该数据集，后台完成后自动刷新。
  // 失败由错误横幅（error）呈现。
  function dismissDeletionNotice() {}

  function closeDetail() {
    resetDetail();
    detailLoading.value = false;
  }

  function dispose() {
    listScope.dispose();
    deletePollTimers.forEach((timer) => clearTimeout(timer));
    deletionPollTimers.forEach((timer) => clearTimeout(timer));
    deletePollTimers.clear();
    Object.keys(pendingGridDeletes).forEach((key) => delete pendingGridDeletes[key]);
    resetDetail();
  }

  return {
    filters, pageState, records, summary, loading, error, actionLoading, hiddenRoles, roleRestrictionsLoading, pendingGridDeletes, selectedDatasetId, selectedDataset,
    detailVisible, detailLoading, detail, activeTab, tabPages, loadList, openDetail, loadDetailTab,
    setActiveTab, setTabPage, setTabPageSize, updateMetadata, updateRoleRestrictions, reassignScene, requestIngest,
    retryBandIngest, deleteBandGrid, publish, withdraw, deleteDataset, closeDetail, dispose,
    pendingDeletions, activeDeletions, deletingDatasetIds,
  };
});
