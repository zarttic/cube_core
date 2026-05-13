# MinIO 集群剖分数据读取接入说明

本文面向外部系统或第三方开发人员，说明如何从 MinIO 集群读取已经完成剖分入库的 Landsat 光学数据。文档包含数据说明、访问端点、读取链路、环境准备、示例 AOI 读取、结果校验和常见问题。

## 0. 对外接入摘要

本批数据来自 `optical_demo` 目录中的 10 个 Landsat zip 场景，不包含以下 tar 文件：

```text
LC08_L2SP_120030_20260204_20260217_02_T1.tar
```

已完成的处理内容：

- 原始 zip 解压为 100 个 TIF 资产。
- 每个 TIF 转为 COG（Cloud Optimized GeoTIFF）。
- 按 `geohash` 7 级网格生成空间索引和读取窗口。
- COG 对象写入 MinIO 集群。
- 元数据写入 PostgreSQL 的 `rs_raw_scene_asset`、`rs_cube_cell_fact` 和 `rs_ingest_job`。

已验证数据量：

| 项目 | 数量 |
| --- | ---: |
| COG 对象 | 100 |
| `rs_raw_scene_asset` 行数 | 100 |
| `rs_cube_cell_fact` 行数 | 1860 |
| `index_rows.jsonl` 行数 | 2439 |

本批数据的主要时间桶：

```text
20211013
20211020
20211027
20211223
```

### 0.1 访问端点

MinIO 数据端：

| 配置项 | 值 |
| --- | --- |
| S3 endpoint | `10.136.1.14:9000` |
| 协议 | HTTP |
| bucket | `cube` |
| 对象前缀 | `cube/raw/dataset=landsat8/sensor=L8/` |

元数据端：

| 配置项 | 值 |
| --- | --- |
| 数据库 | PostgreSQL |
| DSN | `postgresql://postgres:postgres@127.0.0.1:55432/cube` |
| 读取主表 | `rs_cube_cell_fact` |
| 本批入库 job | `optical-demo-except-lc08-tar-minio-cluster-postgres-20260507` |

MinIO access key、secret key 和 PostgreSQL 账号应由管理员通过安全渠道单独发放，不应写入对外文档或代码仓库。外部读者如果不在同一台机器上，需要将 PostgreSQL DSN 替换为可从外部访问的地址。

### 0.2 标准读取链路

读取不是直接列 MinIO 对象后整景下载，而是通过“元数据索引 + COG window”按需读取：

1. 输入 AOI bbox、时间桶和波段列表。
2. 使用 `CubeEncoderSDK` 按 `geohash` 7 级计算 AOI 覆盖的 `space_code[]`。
3. 在 PostgreSQL 中查询 `rs_cube_cell_fact`。
4. 读取 `value_ref_uri`。
5. 将 `value_ref_uri` 解析为 MinIO COG 对象和像素窗口。
6. 使用 GDAL `/vsis3/` 从 MinIO COG 中按窗口读取。
7. 将多个窗口拼接为 AOI GeoTIFF。

`value_ref_uri` 示例：

```text
s3://cube/cube/raw/dataset=landsat8/sensor=L8/acq_date=2021/10/13/scene_id=LO81190292021286BJC00/version=v1/LO81190292021286BJC00_B1_cog.tif#window=0,1630,670,2701
```

其中：

| 字段 | 含义 |
| --- | --- |
| `s3://cube` | bucket 为 `cube` |
| `cube/raw/.../*.tif` | MinIO 对象 key |
| `#window=` | COG 内部读取窗口 |
| `0,1630,670,2701` | `col_off,row_off,width,height` |

### 0.3 外部环境准备

推荐 Python 环境：

```bash
export PYTHON=/home/hadoop/anaconda3/bin/python
export PYTHONPATH=/home/lyjdev/projects/cube_project/cube_encoder:/home/lyjdev/projects/cube_project/cube_split
```

需要以下依赖：

- `rasterio`
- `numpy`
- `psycopg`
- `minio`
- `grid_core`
- `cube_split`

外部读者应设置以下环境变量：

```bash
export MINIO_ENDPOINT=10.136.1.14:9000
export MINIO_ACCESS_KEY=<由管理员提供>
export MINIO_SECRET_KEY=<由管理员提供>
export MINIO_BUCKET=cube

export POSTGRES_DSN='postgresql://<user>:<password>@<host>:<port>/<database>'
```

### 0.4 快速连通性检查

检查 MinIO 健康状态：

```bash
curl -I "http://${MINIO_ENDPOINT}/minio/health/live"
```

预期响应包含：

```text
HTTP/1.1 200 OK
Server: MinIO
```

检查 bucket 和对象前缀：

```bash
$PYTHON - <<'PY'
import os
from minio import Minio

client = Minio(
    os.environ["MINIO_ENDPOINT"],
    access_key=os.environ["MINIO_ACCESS_KEY"],
    secret_key=os.environ["MINIO_SECRET_KEY"],
    secure=False,
)

bucket = os.environ.get("MINIO_BUCKET", "cube")
print("buckets:", [b.name for b in client.list_buckets()])

prefix = "cube/raw/dataset=landsat8/sensor=L8/acq_date=2021/"
objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
print("object_count:", len(objects))
print("first_object:", objects[0].object_name if objects else "NONE")
PY
```

本批数据在该前缀下的预期对象数为 `100`。

检查 PostgreSQL 元数据：

```bash
$PYTHON - <<'PY'
import os
import psycopg

run_id = "optical-demo-except-lc08-tar-minio-cluster-postgres-20260507"

with psycopg.connect(os.environ["POSTGRES_DSN"]) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT status, stats_json FROM rs_ingest_job WHERE job_id = %s", (run_id,))
        print("job:", cur.fetchone())

        cur.execute("SELECT COUNT(*) FROM rs_raw_scene_asset WHERE run_id = %s", (run_id,))
        print("raw_asset_rows:", cur.fetchone()[0])

        cur.execute("SELECT COUNT(*) FROM rs_cube_cell_fact WHERE run_id = %s", (run_id,))
        print("cube_fact_rows:", cur.fetchone()[0])
PY
```

预期：

```text
status = succeeded
raw_asset_rows = 100
cube_fact_rows = 1860
```

### 0.5 AOI 读取示例

以下 AOI 已实测通过：

| 参数 | 值 |
| --- | --- |
| `bbox` | `[121.70108263478448, 44.50032838492438, 123.11134196037203, 45.22982309128671]` |
| `time_bucket` | `20211013` |
| `bands` | `b1,b2,b3` |
| `grid_type` | `geohash` |
| `grid_level` | `7` |
| `cover_mode` | `intersect` |
| `cube_version` | `v1` |

读取并输出 GeoTIFF：

```bash
$PYTHON - <<'PY'
import os

from cube_split.read.aoi_reader import read_aoi_rgb

output = read_aoi_rgb(
    bbox=[
        121.70108263478448,
        44.50032838492438,
        123.11134196037203,
        45.22982309128671,
    ],
    time_bucket="20211013",
    bands=["b1", "b2", "b3"],
    output="/tmp/cube_aoi_readback_b123.tif",
    postgres_dsn=os.environ["POSTGRES_DSN"],
    minio_endpoint=os.environ["MINIO_ENDPOINT"],
    minio_access_key=os.environ["MINIO_ACCESS_KEY"],
    minio_secret_key=os.environ["MINIO_SECRET_KEY"],
    grid_type="geohash",
    grid_level=7,
    cover_mode="intersect",
    cube_version="v1",
)

print(output)
PY
```

校验输出文件：

```bash
$PYTHON - <<'PY'
import rasterio

path = "/tmp/cube_aoi_readback_b123.tif"
with rasterio.open(path) as ds:
    print("count:", ds.count)
    print("width:", ds.width)
    print("height:", ds.height)
    print("crs:", ds.crs)
    print("bounds:", tuple(round(v, 6) for v in ds.bounds))
    print("descriptions:", ds.descriptions)

    for idx in range(1, ds.count + 1):
        arr = ds.read(idx)
        print(
            f"band {idx}:",
            "min", int(arr.min()),
            "max", int(arr.max()),
            "nonzero", int((arr != 0).sum()),
        )
PY
```

本次实测结果：

```text
count: 3
width: 5715
height: 6988
crs: EPSG:32651
descriptions: ('b1', 'b2', 'b3')
band 1: max 21649, nonzero 12094623
band 2: max 23418, nonzero 12094454
band 3: max 24306, nonzero 12095854
```

只要 `count=3` 且每个波段 `nonzero` 明显大于 0，即可确认从 MinIO COG 回源读取成功。

### 0.6 直接读取单个 COG window

如果外部系统不需要 AOI 拼接，也可以直接读取 `value_ref_uri` 中指定的单个窗口。

```bash
$PYTHON - <<'PY'
import os
from urllib.parse import urlparse

import rasterio
from rasterio.windows import Window

value_ref_uri = (
    "s3://cube/cube/raw/dataset=landsat8/sensor=L8/acq_date=2021/10/13/"
    "scene_id=LO81190292021286BJC00/version=v1/"
    "LO81190292021286BJC00_B1_cog.tif#window=0,1630,670,2701"
)

base, frag = value_ref_uri.split("#", 1)
col_off, row_off, width, height = map(int, frag.split("window=", 1)[1].split(","))
parsed = urlparse(base)
vsi_path = f"/vsis3/{parsed.netloc}/{parsed.path.lstrip('/')}"

os.environ["AWS_ACCESS_KEY_ID"] = os.environ["MINIO_ACCESS_KEY"]
os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ["MINIO_SECRET_KEY"]
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_S3_ENDPOINT"] = os.environ["MINIO_ENDPOINT"]
os.environ["AWS_HTTPS"] = "NO"
os.environ["AWS_VIRTUAL_HOSTING"] = "FALSE"

with rasterio.open(vsi_path) as ds:
    arr = ds.read(1, window=Window(col_off, row_off, width, height))
    print("shape:", arr.shape)
    print("dtype:", arr.dtype)
    print("min:", int(arr.min()))
    print("max:", int(arr.max()))
    print("nonzero:", int((arr != 0).sum()))
PY
```

这种方式适合集成到已有遥感服务中：业务系统先查询 `rs_cube_cell_fact`，再按 `value_ref_uri` 回源读取。

### 0.7 外部系统集成建议

外部系统建议只依赖以下接口：

- PostgreSQL 查询：`rs_cube_cell_fact`
- MinIO S3 读取：`value_ref_uri` 指向的 COG window
- 网格计算：通过 `grid_core.sdk.CubeEncoderSDK` 或由服务端封装 AOI 查询接口

不建议外部系统直接假设对象 key 规则并扫描 MinIO。对象 key 用于存储组织，业务查询应以元数据表为准。

外部查询至少需要：

| 参数 | 示例 | 说明 |
| --- | --- | --- |
| `bbox` | `[121.70, 44.50, 123.11, 45.23]` | AOI 范围，WGS84 经纬度 |
| `time_bucket` | `20211013` | 日期桶 |
| `bands` | `b1,b2,b3` | 需要读取的波段 |
| `grid_type` | `geohash` | 本批为 `geohash` |
| `grid_level` | `7` | 本批为 7 级 |
| `cube_version` | `v1` | 数据版本 |

当前 `read_aoi_rgb` 输出 GeoTIFF：

- 波段顺序与传入 `bands` 一致。
- 每个输出 band description 会写入对应 band 名称。
- 输出 CRS 继承源 COG 的 CRS。
- 输出像素分辨率继承源 COG 的像素大小。

### 0.8 常见问题

#### 不要使用 `127.0.0.1:59000`

`127.0.0.1:59000` 曾经是 Podman 单节点 MinIO 容器端口，不是真实集群入口。该容器已经删除。真实集群入口为：

```text
10.136.1.14:9000
```

#### `InvalidAccessKeyId`

说明 access key 不属于当前 MinIO 集群。请确认：

- endpoint 是 `10.136.1.14:9000`
- access key 和 secret key 来自真实集群管理员
- 没有误用本地测试 MinIO 的默认凭据 `minioadmin/minioadmin`

#### `No cube rows matched the AOI and filters`

说明 PostgreSQL 中没有匹配的索引行。常见原因：

- `time_bucket` 填错。
- `band` 名称填错，例如本批为 `b1`、`b2`、`b3`，不是 `sr_b2`。
- AOI 不在本批影像覆盖范围内。
- 查询的 `cube_version` 不存在。

可以先用 SQL 检查可用参数：

```sql
SELECT time_bucket, band, COUNT(*)
FROM rs_cube_cell_fact
WHERE run_id = 'optical-demo-except-lc08-tar-minio-cluster-postgres-20260507'
GROUP BY time_bucket, band
ORDER BY time_bucket, band;
```

#### MinIO 对象存在但读取失败

检查 GDAL S3 环境变量：

```text
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_S3_ENDPOINT
AWS_HTTPS=NO
AWS_VIRTUAL_HOSTING=FALSE
```

如果 endpoint 使用 HTTP，但 `AWS_HTTPS` 没有设为 `NO`，GDAL 可能会用 HTTPS 访问并失败。

#### 输出 GeoTIFF 很大

AOI 范围越大，拼接后的 GeoTIFF 越大。建议外部系统先用小 bbox 做连通性验证，再按业务需要扩大 AOI。

## 附录：本机实测记录

验证时间：2026-05-07

验证内容：

- MinIO bucket 和对象数量检查。
- PostgreSQL job 状态和行数检查。
- AOI 读取生成 GeoTIFF。
- 使用 rasterio 检查输出文件的波段、尺寸、CRS 和非零像元。

验证输出：

```text
/tmp/cube_aoi_readback_b123.tif
```

输出文件大小约 `55M`，三波段均有有效像元。
