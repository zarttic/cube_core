import { reactive, ref } from 'vue';
import { defineStore } from 'pinia';

import { download, requestGet, requestPost } from '@/api/client';
import { normalizePageResponse, pageQuery } from '@/api/pagination';
import { createRequestScope } from '@/api/requestScope';

function emptyDetail() {
  return null;
}

export const useQualityStore = defineStore('quality', () => {
  const filters = reactive({
    keyword: '',
    datasetId: '',
    outputVersion: '',
    dataType: '',
    status: '',
    trigger: '',
    requestedBy: '',
    currentOnly: '',
    startedFrom: '',
    startedTo: '',
    sortBy: 'generated_at',
    sortOrder: 'desc',
  });
  const errorFilters = reactive({ ruleCode: '', errorCode: '', field: '' });
  const records = ref([]);
  const pageState = reactive({ page: 1, pageSize: 20, total: 0 });
  const loading = ref(false);
  const error = ref('');
  const selectedQualityRunId = ref('');
  const detailVisible = ref(false);
  const detailLoading = ref(false);
  const detail = ref(emptyDetail());
  const results = ref([]);
  const resultsPage = reactive({ page: 1, pageSize: 20, total: 0 });
  const errors = ref([]);
  const errorPage = ref(1);
  const errorPageSize = ref(20);
  const errorTotal = ref(0);
  const selectedErrorRows = ref([]);
  const activeTab = ref('results');
  const exportFilename = ref('');
  const exporting = ref(false);
  const rerunning = ref(false);
  const ruleCatalog = ref(null);
  const ruleCatalogLoading = ref(false);
  const listScope = createRequestScope();
  const detailScope = createRequestScope();
  const resultsScope = createRequestScope();
  const errorsScope = createRequestScope();
  let detailGeneration = 0;

  function listParameters() {
    return {
      keyword: filters.keyword.trim(),
      dataset_id: filters.datasetId.trim(),
      output_version: filters.outputVersion.trim(),
      data_type: filters.dataType,
      status: filters.status,
      trigger: filters.trigger,
      requested_by: filters.requestedBy.trim(),
      current_only: filters.currentOnly,
      started_from: filters.startedFrom,
      started_to: filters.startedTo,
      page: pageState.page,
      page_size: pageState.pageSize,
      sort_by: filters.sortBy,
      sort_order: filters.sortOrder,
    };
  }

  function errorParameters(includePage = true) {
    return {
      rule_code: errorFilters.ruleCode.trim(),
      error_code: errorFilters.errorCode.trim(),
      field: errorFilters.field.trim(),
      ...(includePage ? { page: errorPage.value, page_size: errorPageSize.value } : {}),
    };
  }

  async function loadList() {
    const request = listScope.begin();
    loading.value = true;
    error.value = '';
    try {
      const response = await requestGet(`/v1/quality/records?${pageQuery(listParameters())}`, { signal: request.signal });
      if (!listScope.isCurrent(request.token)) return;
      const page = normalizePageResponse(response, pageState.page, pageState.pageSize);
      records.value = page.items;
      Object.assign(pageState, { page: page.page, pageSize: page.pageSize, total: page.total });
      if (!records.value.length && pageState.total > 0 && pageState.page > 1) {
        pageState.page = Math.max(1, Math.ceil(pageState.total / pageState.pageSize));
        await loadList();
      }
    } catch (requestError) {
      if (request.signal.aborted || !listScope.isCurrent(request.token)) return;
      error.value = requestError.message || '质检记录加载失败';
      throw requestError;
    } finally {
      if (listScope.isCurrent(request.token)) loading.value = false;
    }
  }

  async function loadRuleCatalog() {
    if (ruleCatalog.value) return ruleCatalog.value;
    ruleCatalogLoading.value = true;
    try {
      const response = await requestGet('/v1/quality/rules');
      ruleCatalog.value = response;
      return response;
    } finally {
      ruleCatalogLoading.value = false;
    }
  }

  function resetDetail(nextQualityRunId = '') {
    detailScope.cancel();
    resultsScope.cancel();
    errorsScope.cancel();
    detailGeneration += 1;
    selectedQualityRunId.value = nextQualityRunId;
    detailVisible.value = Boolean(nextQualityRunId);
    detail.value = emptyDetail();
    results.value = [];
    errors.value = [];
    selectedErrorRows.value = [];
    activeTab.value = 'errors';
    Object.assign(resultsPage, { page: 1, pageSize: 20, total: 0 });
    errorPage.value = 1;
    errorPageSize.value = 20;
    errorTotal.value = 0;
    Object.assign(errorFilters, { ruleCode: '', errorCode: '', field: '' });
    exportFilename.value = '';
  }

  async function openDetail(qualityRunId) {
    if (!qualityRunId) return;
    resetDetail(qualityRunId);
    const generation = detailGeneration;
    const request = detailScope.begin();
    detailLoading.value = true;
    error.value = '';
    try {
      const response = await requestGet(`/v1/quality/records/${encodeURIComponent(qualityRunId)}`, { signal: request.signal });
      if (selectedQualityRunId.value !== qualityRunId || generation !== detailGeneration || !detailScope.isCurrent(request.token)) return;
      detail.value = response;
    } catch (requestError) {
      if (request.signal.aborted || selectedQualityRunId.value !== qualityRunId || generation !== detailGeneration || !detailScope.isCurrent(request.token)) return;
      error.value = requestError.message || '质检详情加载失败';
      throw requestError;
    } finally {
      if (selectedQualityRunId.value === qualityRunId && generation === detailGeneration && detailScope.isCurrent(request.token)) {
        detailLoading.value = false;
      }
    }
  }

  async function loadResults() {
    const qualityRunId = selectedQualityRunId.value;
    if (!qualityRunId) return;
    const generation = detailGeneration;
    const request = resultsScope.begin();
    try {
      const query = pageQuery({ page: resultsPage.page, page_size: resultsPage.pageSize });
      const response = await requestGet(`/v1/quality/records/${encodeURIComponent(qualityRunId)}/results?${query}`, { signal: request.signal });
      if (selectedQualityRunId.value !== qualityRunId || generation !== detailGeneration || !resultsScope.isCurrent(request.token)) return;
      const page = normalizePageResponse(response, resultsPage.page, resultsPage.pageSize);
      results.value = page.items;
      Object.assign(resultsPage, { page: page.page, pageSize: page.pageSize, total: page.total });
    } catch (requestError) {
      if (request.signal.aborted || selectedQualityRunId.value !== qualityRunId || generation !== detailGeneration || !resultsScope.isCurrent(request.token)) return;
      error.value = requestError.message || '质检规则结果加载失败';
      throw requestError;
    }
  }

  async function loadErrors() {
    const qualityRunId = selectedQualityRunId.value;
    if (!qualityRunId) return;
    const generation = detailGeneration;
    const request = errorsScope.begin();
    try {
      const response = await requestGet(
        `/v1/quality/records/${encodeURIComponent(qualityRunId)}/errors?${pageQuery(errorParameters(true))}`,
        { signal: request.signal },
      );
      if (selectedQualityRunId.value !== qualityRunId || generation !== detailGeneration || !errorsScope.isCurrent(request.token)) return;
      const page = normalizePageResponse(response, errorPage.value, errorPageSize.value);
      errors.value = page.items;
      errorPage.value = page.page;
      errorPageSize.value = page.pageSize;
      errorTotal.value = page.total;
    } catch (requestError) {
      if (request.signal.aborted || selectedQualityRunId.value !== qualityRunId || generation !== detailGeneration || !errorsScope.isCurrent(request.token)) return;
      error.value = requestError.message || '质检错误明细加载失败';
      throw requestError;
    }
  }

  async function setActiveTab(tab) {
    activeTab.value = tab;
    selectedErrorRows.value = [];
    if (tab === 'results') await loadResults();
    if (tab === 'errors') await loadErrors();
  }

  function saveDownload(result, fallbackName) {
    if (!result?.blob || typeof URL.createObjectURL !== 'function') return;
    const link = document.createElement('a');
    const url = URL.createObjectURL(result.blob);
    link.href = url;
    link.download = result.filename || fallbackName;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
  }

  async function exportErrors(format, filtered) {
    const qualityRunId = selectedQualityRunId.value;
    if (!qualityRunId) return null;
    exporting.value = true;
    try {
      const query = pageQuery({ format, ...(filtered ? errorParameters(false) : {}) });
      const result = await download(`/v1/quality/records/${encodeURIComponent(qualityRunId)}/errors/export?${query}`);
      exportFilename.value = result?.filename || '';
      saveDownload(result, `quality-errors.${format}`);
      return result;
    } finally {
      exporting.value = false;
    }
  }

  async function exportRunErrors(row, format = 'csv') {
    if (!row?.quality_run_id) return null;
    exporting.value = true;
    try {
      const result = await download(`/v1/quality/records/${encodeURIComponent(row.quality_run_id)}/errors/export?${pageQuery({ format })}`);
      saveDownload(result, `${row.dataset_code || 'dataset'}-quality-errors.${format}`);
      return result;
    } finally {
      exporting.value = false;
    }
  }

  async function rerun(datasetId, outputVersion) {
    if (!datasetId || !outputVersion) return null;
    rerunning.value = true;
    try {
      return await requestPost('/v1/quality/runs', { dataset_id: datasetId, output_version: outputVersion });
    } finally {
      rerunning.value = false;
    }
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
    filters,
    errorFilters,
    records,
    pageState,
    loading,
    error,
    selectedQualityRunId,
    detailVisible,
    detailLoading,
    detail,
    results,
    resultsPage,
    errors,
    errorPage,
    errorPageSize,
    errorTotal,
    selectedErrorRows,
    activeTab,
    exportFilename,
    exporting,
    rerunning,
    ruleCatalog,
    ruleCatalogLoading,
    loadList,
    openDetail,
    loadResults,
    loadErrors,
    setActiveTab,
    exportErrors,
    exportRunErrors,
    loadRuleCatalog,
    rerun,
    closeDetail,
    dispose,
  };
});
