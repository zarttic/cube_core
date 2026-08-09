# Build from the repository root: docker build -f docs/kuberay-migration/templates/Dockerfile .
# The platform team should replace the base image only with a Ray version compatible
# with rayVersion in the RayCluster manifest.
ARG RAY_IMAGE=rayproject/ray:2.55.1-py311
FROM ${RAY_IMAGE}

USER root
WORKDIR /app

COPY cube_encoder /app/cube_encoder
COPY cube_split /app/cube_split
COPY cube_web /app/cube_web

# Package metadata declares the Python runtime dependencies, including rasterio,
# pyproj, netCDF4, psycopg, minio, and the pinned Ray minor version.
RUN pip install --no-cache-dir \
    /app/cube_encoder \
    /app/cube_split \
    /app/cube_web

ENV PYTHONPATH=/app:/app/cube_encoder:/app/cube_split:/app/cube_web
WORKDIR /app
USER ray
