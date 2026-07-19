import { describe, expect, it } from 'vitest';

import { qualityErrorLabel, qualityRecoveryLabel } from '@/utils/qualityLabels';

describe('quality recovery labels', () => {
  it('routes metadata, source and partition-output findings to distinct repair paths', () => {
    expect(qualityRecoveryLabel('optical_band_contract')).toContain('修正后重新质检');
    expect(qualityRecoveryLabel('asset_readability')).toContain('退回载入系统');
    expect(qualityRecoveryLabel('index_schema')).toContain('原批次局部重建');
  });

  it('uses the finding code to distinguish system, metadata and source failures within one rule', () => {
    expect(qualityRecoveryLabel('asset_readability', 'object_reader_unavailable')).toContain('恢复服务后重新质检');
    expect(qualityRecoveryLabel('asset_crs', 'missing_crs')).toContain('修正后重新质检');
    expect(qualityRecoveryLabel('asset_crs', 'crs_metadata_mismatch')).toContain('核对声明');
    expect(qualityRecoveryLabel('time_bucket_consistency', 'time_bucket_mismatch')).toContain('核对载入时间');
    expect(qualityRecoveryLabel('asset_readability', 'source_object_unreadable')).toContain('退回载入系统');
  });

  it('provides Chinese labels for current built-in finding codes', () => {
    expect(qualityErrorLabel('missing_tile_reference')).toBe('缺少瓦片引用');
    expect(qualityErrorLabel('window_out_of_bounds')).toBe('像素窗口超出范围');
    expect(qualityErrorLabel('duplicate_observation_id')).toBe('观测记录标识重复');
    expect(qualityErrorLabel('missing_product_year')).toContain('未分类错误');
  });
});
