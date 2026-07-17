# M6 全覆盖 Mock 与 MinIO Source 验收

M6 验收分为两层。默认 pytest 使用确定性的 `s3://cube/cube/source/...` 元数据 mock，
不连接 OpenGauss、Ray 或 MinIO；真实 runner 只读列举和 `stat` MinIO source 对象，随后在
`/tmp` 生成同一结构的 manifest，再运行同一组离线 API/service 集成测试。

覆盖场景包括：

- 单载入批次包含多个 Dataset 和 Scene；
- 同一 Dataset 跨载入批次、同一 Scene 跨载入批次；
- 不同 Scene 重复 checksum、多资产一景；
- optical、radar、product、carbon 四类数据；
- Scene 完成、失败、部分失败、显式重试、取消；
- 质检 pass、warn、fail，质检通过后的自动入库门禁；
- 相同 Scene 输出重复投递的幂等性；
- 发布、撤回、Scene 重归属及 provenance；
- Geohash、标准 MGRS、ISEA4H 的真实 SDK Polygon，其中 ISEA4H 为闭合的六边形。

## 离线测试

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest \
  cube_web/tests/test_m6_mock_integration.py \
  cube_web/tests/test_m6_mock_runner.py -v
```

## 真实 MinIO Source Runner

runner 通过 `cube_split.runtime_config` 读取进程环境、`CUBE_WEB_ENV_FILE` 或本地
`.cube_web.env`。它不会输出凭据，也不会下载、上传、修改或删除对象。

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web \
python3.11 cube_web/scripts/run_m6_mock_acceptance.py
```

runner 在 `cube/source/` 下自动识别 source 对象，并对选中的每个对象调用
`stat_object`。验收要求至少 2 个 optical 对象，以及 radar、product、carbon 各 1 个；
对象缺失或为空时直接非零退出，不允许 skip。

当供应商目录名称无法自动识别时，可以显式指定 URI。多个 URI 使用逗号分隔，URI 必须
属于当前配置的 bucket，且仍会执行 `stat_object`：

```bash
export CUBE_M6_RADAR_SOURCE_URIS=s3://cube/cube/source/vendor/s1-a.tif
python3.11 cube_web/scripts/run_m6_mock_acceptance.py
```

仅生成 manifest 而不运行 pytest：

```bash
python3.11 cube_web/scripts/run_m6_mock_acceptance.py \
  --prepare-only --output /tmp/cube-m6-acceptance/manifest.json
```

manifest 中的 `mock_identity_sha256` 由 bucket、对象键、size 和 ETag 生成，仅用于构造
重复数据场景，不冒充对象内容的 SHA-256。manifest 只允许写入 `/tmp`，不得提交到仓库。
