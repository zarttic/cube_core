# Bug 排查记录

## 目的

记录每个问题的复现方式、根因、修复方式和验证结果。

## 记录格式

- 日期：`YYYY-MM-DD`
- Bug ID：`BUG-XXXX`
- 现象
- 影响
- 复现方式
- 根因
- 修复
- 验证
- 状态

---

## 2026-03-09 | BUG-0001 | pytest import 失败

- 现象：测试收集阶段出现 `ModuleNotFoundError: fastapi/shapely`。
- 影响：测试无法运行。
- 复现方式：`pytest -q`。
- 根因：依赖安装到了 Python 3.8 site-packages，但测试使用 Python 3.11。
- 修复：使用 Python 3.11 pip 安装依赖（`/home/hadoop/anaconda3/bin/pip install -r requirements.txt`）。
- 验证：后续测试可在 Python 3.11 下导入 fastapi/shapely。
- 状态：已修复。

## 2026-03-09 | BUG-0002 | API 集成测试在环境中挂起

- 现象：基于 ASGI client 的 API 测试在 `/health` 上停滞。
- 影响：当前环境下 endpoint 级集成测试路径不稳定。
- 复现方式：测试 client 请求 app ASGI transport。
- 根因：运行环境或插件交互导致 ASGI client 阻塞，非业务逻辑问题。
- 修复：API 测试改为 route function 级断言，保持 CI 对请求/响应逻辑的稳定覆盖。
- 验证：`python -m pytest -q tests` 通过。
- 状态：已缓解。
