# Cube 使用手册站

这是一个独立的纯静态文档网站，面向第一次使用 Cube 的操作员、数据工程师和应用开发者。

内容按“开始使用 → 理解数据 → 完成一批 → 进阶参考”组织。长章节位于 `content/`，包括环境、导入、剖分、质检、波段级入库、发布、API、生产检查表和排障；`assets/` 保存实际前端页面截图。

## 本地预览

```bash
python3.11 -m http.server 50041 --directory docs_site
```

打开 <http://localhost:50041>。

无头浏览器验收截图保存在 `assets/docs-home.png` 和 `assets/docs-mobile.png`，分别对应桌面首页与移动端剖分章节。

## 内容基准

文档内容以当前仓库的 `README.md`、`cube_web/docs/`、`docs/LOADER_SCHEMA_HANDOFF.md` 和实际前端代码为准。生产格网只描述 `geohash`、`mgrs`、`isea4h`；运行时基础设施使用 OpenGauss、Ray 和 MinIO。
