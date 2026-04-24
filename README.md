# cube_core

This repository is organized as a small Python monorepo.

Projects:

- `cube_encoder`
  - core grid, topology, and space-time coding capabilities
- `cube_split`
  - partition, ingest, and AOI readback workflows
- `cube_web`
  - standalone web pages and visualization

Recommended local test commands:

```bash
cd cube_encoder
python -m pip install --upgrade pip build
python -m build
python -m pip install --force-reinstall dist/*.whl
pytest -q

cd ../cube_split
python -m pip install -r requirements.txt
pytest -q

cd ../cube_web
python -m pip install -r requirements.txt
pytest -q
```
