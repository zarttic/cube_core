import { describe, expect, it } from 'vitest';

import { bandDisplayLabel, dataUnitTypeLabel, sceneBands, sceneMatchesBand } from '@/utils/bands';

describe('band presentation contract', () => {
  it('uses stable codes, supplied names and units across product types', () => {
    const bands = sceneBands({ bands: [{ band_code: 'NDVI', band_name: '归一化植被指数', band_type: 'variable', unit: '1' }] }, 'product');
    expect(bands[0]).toMatchObject({ band_code: 'NDVI', band_name: '归一化植被指数', band_type: 'variable', unit: '1' });
    expect(bandDisplayLabel(bands[0])).toBe('NDVI · 归一化植被指数 [1]');
    expect(dataUnitTypeLabel('optical')).toBe('光谱波段');
    expect(dataUnitTypeLabel('radar')).toBe('极化通道');
  });

  it('reports invalid band records without inventing identity or type', () => {
    const [band] = sceneBands({ bands: [{ band_name: '红光' }] }, 'optical');

    expect(band).toMatchObject({ band_code: '', band_name: '红光', band_type: '' });
    expect(band.contract_errors).toEqual(['缺少波段编码', '缺少波段类型']);
    expect(bandDisplayLabel(band)).toBe('数据异常：缺少波段编码、缺少波段类型');
    expect(sceneMatchesBand({ bands: [{ band_name: '红光' }] }, 'optical', '红光')).toBe(true);
    expect(bandDisplayLabel(sceneBands({}, 'optical')[0])).toBe('数据异常：缺少波段编码、缺少波段类型');
  });

  it('matches code, name, type and unit without changing scene identity', () => {
    const scene = { scene_id: 'scene-a', bands: [{ band_code: 'VV', band_name: 'VV', band_type: 'polarization', unit: 'dB' }] };
    expect(sceneMatchesBand(scene, 'radar', 'polar')).toBe(true);
    expect(sceneMatchesBand(scene, 'radar', 'db')).toBe(true);
    expect(sceneMatchesBand(scene, 'radar', 'B04')).toBe(false);
    expect(scene.scene_id).toBe('scene-a');
  });
});
