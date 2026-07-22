import { describe, expect, it } from 'vitest';

import {
  filterActiveQualityRules,
  qualityErrorLabel,
  qualityRecoveryLabel,
  qualityRuleLabel,
  RETIRED_QUALITY_RULE_CODES,
} from '@/utils/qualityLabels';

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
    expect(qualityErrorLabel('missing_product_year')).toContain('未分类错误');
  });

  it('filters retired rules out of catalog and run item lists', () => {
    expect(RETIRED_QUALITY_RULE_CODES.has('product_band_contract')).toBe(true);
    expect(RETIRED_QUALITY_RULE_CODES.has('carbon_observation_duplicates')).toBe(true);
    expect(RETIRED_QUALITY_RULE_CODES.has('carbon_footprints')).toBe(true);
    expect(qualityRuleLabel('carbon_footprints')).toContain('已停用');
    const filtered = filterActiveQualityRules([
      { code: 'asset_readability', name: '数据单元可读性' },
      { rule_code: 'product_band_contract', status: 'pass' },
      { rule_code: 'carbon_observation_duplicates', status: 'fail' },
      { rule_code: 'carbon_footprints', status: 'fail' },
      { rule_code: 'carbon_schema', status: 'pass' },
    ]);
    expect(filtered.map((item) => item.code || item.rule_code)).toEqual([
      'asset_readability',
      'carbon_schema',
    ]);
  });
});
