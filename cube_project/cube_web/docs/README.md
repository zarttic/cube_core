# cube_web 文档

更新时间：2026-04-24  
适用范围：`cube_web`

## 1. 定位

`cube_web` 提供 cube 项目的 Web 演示页面和可视化入口。它只负责页面托管、静态资源和交互展示，不实现格网剖分、编码或拓扑算法。

## 2. 调用边界

页面需要格网能力时，应调用 `cube_encoder`：

- 默认 encoder base: `http://127.0.0.1:50012`
- 运行时覆盖方式: `?encoderBase=http://127.0.0.1:50012`

示例：

```text
http://127.0.0.1:50040/encoding?encoderBase=http://127.0.0.1:50012
```

## 3. 运行方式

先安装并启动 `cube_encoder`，再启动 Web 服务：

```bash
PYTHONPATH=../cube_encoder:. uvicorn cube_web.app:app --host 0.0.0.0 --port 50040 --reload
```

如果需要按包安装：

```bash
cd ../cube_encoder
python -m build
python -m pip install --force-reinstall dist/*.whl
cd ../cube_web
pip install -r requirements.txt
uvicorn cube_web.app:app --host 0.0.0.0 --port 50040 --reload
```

## 4. 测试

```bash
PYTHONPATH=../cube_encoder:. pytest tests
```

涉及 API 行为变化时，需要同时检查 `cube_encoder` 的 SDK/API 测试和 Web 端调用路径。
