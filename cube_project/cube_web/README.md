# cube_web

`cube_web` hosts the standalone web pages for cube demos and visualization.

It does not implement grid logic itself. The pages call `cube_encoder` APIs directly. By default the frontend targets:

- `http://127.0.0.1:50012`

You can override the encoder base at runtime with a query parameter:

- `http://127.0.0.1:50040/encoding?encoderBase=http://127.0.0.1:50012`

## Run

```bash
python -m venv .venv
source .venv/bin/activate
cd ../cube_encoder
python -m pip install --upgrade pip build
python -m build
python -m pip install --force-reinstall dist/*.whl
cd ../cube_web
pip install -r requirements.txt
uvicorn cube_web.app:app --host 0.0.0.0 --port 50040 --reload
```
