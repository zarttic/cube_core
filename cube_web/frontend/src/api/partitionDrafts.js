import { requestPost } from '@/api/client';

function sourceBatchIds(selection) {
  return [...new Set((selection.scenes || []).flatMap((scene) => [
    ...(Array.isArray(scene.source_batch_ids) ? scene.source_batch_ids : []),
    scene.load_batch_id,
  ]).map((value) => String(value || '').trim()).filter(Boolean))];
}

export function createPartitionDraft(selection) {
  return requestPost('/v1/partition/drafts', {
    data_type: selection.data_type,
    draft_name: selection.draft_name,
    source_batch_ids: sourceBatchIds(selection),
    datasets: [selection],
  });
}
