import { reactive, ref } from 'vue';
import { defineStore } from 'pinia';

import { requestGet, requestPost } from '@/api/client';
import { normalizePageResponse, pageQuery } from '@/api/pagination';
import { createRequestScope } from '@/api/requestScope';

export const useIngestRunsStore = defineStore('ingest-runs', () => {
  const filters = reactive({ keyword: '', datasetId: '', status: '', sortBy: 'created_at', sortOrder: 'desc' });
  const pageState = reactive({ page: 1, pageSize: 20, total: 0 });
  const records = ref([]);
  const summary = ref({ run_count: 0, scene_count: 0, completed_scene_count: 0, failed_scene_count: 0 });
  const loading = ref(false);
  const error = ref('');
  const selectedRunId = ref('');
  const detailVisible = ref(false);
  const detail = ref(null);
  const detailLoading = ref(false);
  const actionLoading = ref(false);
  const manualCandidates = ref([]);
  const manualCandidatesLoading = ref(false);
  const manualFilters = reactive({ keyword: '', datasetId: '' });
  const manualPageState = reactive({ page: 1, pageSize: 20, total: 0 });
  const listScope = createRequestScope();
  const manualListScope = createRequestScope();
  const detailScope = createRequestScope();
  let detailGeneration = 0;

  async function loadList() {
    const request = listScope.begin();
    loading.value = true;
    error.value = '';
    try {
      const query = pageQuery({
        keyword: filters.keyword.trim(), dataset_id: filters.datasetId.trim(), status: filters.status,
        page: pageState.page, page_size: pageState.pageSize, sort_by: filters.sortBy, sort_order: filters.sortOrder,
      });
      const response = await requestGet(`/v1/ingest-runs?${query}`, { signal: request.signal });
      if (!listScope.isCurrent(request.token)) return;
      const page = normalizePageResponse(response, pageState.page, pageState.pageSize);
      records.value = page.items;
      summary.value = response.summary || summary.value;
      Object.assign(pageState, { page: page.page, pageSize: page.pageSize, total: page.total });
      if (!records.value.length && pageState.total > 0 && pageState.page > 1) {
        pageState.page = Math.max(1, Math.ceil(pageState.total / pageState.pageSize));
        await loadList();
      }
    } catch (requestError) {
      if (request.signal.aborted || !listScope.isCurrent(request.token)) return;
      error.value = requestError.message || '入库运行加载失败';
      throw requestError;
    } finally {
      if (listScope.isCurrent(request.token)) loading.value = false;
    }
  }

  function resetDetail(nextRunId = '') {
    detailScope.cancel();
    detailGeneration += 1;
    selectedRunId.value = nextRunId;
    detailVisible.value = Boolean(nextRunId);
    detail.value = null;
  }

  async function openDetail(runId) {
    if (!runId) return;
    resetDetail(runId);
    const generation = detailGeneration;
    const request = detailScope.begin();
    detailLoading.value = true;
    error.value = '';
    try {
      const response = await requestGet(`/v1/ingest-runs/${encodeURIComponent(runId)}`, { signal: request.signal });
      if (selectedRunId.value !== runId || generation !== detailGeneration || !detailScope.isCurrent(request.token)) return;
      detail.value = response;
    } catch (requestError) {
      if (request.signal.aborted || selectedRunId.value !== runId || generation !== detailGeneration || !detailScope.isCurrent(request.token)) return;
      error.value = requestError.message || '入库运行详情加载失败';
      throw requestError;
    } finally {
      if (selectedRunId.value === runId && generation === detailGeneration && detailScope.isCurrent(request.token)) detailLoading.value = false;
    }
  }

  async function runAction(action, payload = {}) {
    const runId = selectedRunId.value;
    if (!runId) return;
    actionLoading.value = true;
    error.value = '';
    try {
      const response = await requestPost(`/v1/ingest-runs/${encodeURIComponent(runId)}/${action}`, payload);
      await openDetail(runId);
      await loadList();
      return response;
    } catch (requestError) {
      error.value = requestError.message || '入库运行操作失败';
      throw requestError;
    } finally {
      actionLoading.value = false;
    }
  }

  function retryFailedBandUnits(bandUnitIds = []) {
    return runAction('retry', { band_unit_ids: bandUnitIds });
  }

  function cancelRun(reason = '') {
    return runAction('cancel', { reason });
  }

  async function requestManualCollection(partitionRunId, bandUnitIds) {
    const id = String(partitionRunId || '').trim();
    const ids = (Array.isArray(bandUnitIds) ? bandUnitIds : []).map((value) => String(value || '').trim()).filter(Boolean);
    if (!id || !ids.length) return;
    actionLoading.value = true;
    error.value = '';
    try {
      const response = await requestPost(`/v1/ingest-runs/collections/${encodeURIComponent(id)}/ingest`, { band_unit_ids: ids });
      await loadList();
      await loadManualCandidates();
      return response;
    } catch (requestError) {
      error.value = requestError.message || '手动入库提交失败';
      throw requestError;
    } finally {
      actionLoading.value = false;
    }
  }

  async function loadManualCandidates({ resetPage = false } = {}) {
    if (resetPage) manualPageState.page = 1;
    const request = manualListScope.begin();
    manualCandidatesLoading.value = true;
    try {
      const query = pageQuery({
        keyword: manualFilters.keyword.trim(),
        dataset_id: manualFilters.datasetId.trim(),
        page: manualPageState.page,
        page_size: manualPageState.pageSize,
      });
      const response = await requestGet(`/v1/ingest-runs/collections?${query}`, { signal: request.signal });
      if (!manualListScope.isCurrent(request.token)) return manualCandidates.value;
      const page = normalizePageResponse(response, manualPageState.page, manualPageState.pageSize);
      manualCandidates.value = (Array.isArray(page.items) ? page.items : []).filter((item) => (
        Number(item.quality_pass_count || 0) > Number(item.ingested_count || 0)
      ));
      Object.assign(manualPageState, { page: page.page, pageSize: page.pageSize, total: page.total });
      if (!manualCandidates.value.length && manualPageState.total > 0 && manualPageState.page > 1) {
        manualPageState.page = Math.max(1, Math.ceil(manualPageState.total / manualPageState.pageSize));
        await loadManualCandidates();
      }
      return manualCandidates.value;
    } catch (requestError) {
      if (request.signal.aborted || !manualListScope.isCurrent(request.token)) return manualCandidates.value;
      error.value = requestError.message || '待入库集合加载失败';
      throw requestError;
    } finally {
      if (manualListScope.isCurrent(request.token)) manualCandidatesLoading.value = false;
    }
  }

  function closeDetail() {
    resetDetail();
    detailLoading.value = false;
  }

  function dispose() {
    listScope.dispose();
    manualListScope.dispose();
    resetDetail();
  }

  return {
    filters, pageState, records, summary, loading, error, selectedRunId, detailVisible, detail,
    detailLoading, actionLoading, manualCandidates, manualCandidatesLoading, manualFilters, manualPageState,
    loadList, openDetail, retryFailedBandUnits, cancelRun,
    requestManualCollection, loadManualCandidates, closeDetail, dispose,
  };
});
