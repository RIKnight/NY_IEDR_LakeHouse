# Dockerfile
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# System deps (small footprint)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates tini \
 && rm -rf /var/lib/apt/lists/*

# Workdir
WORKDIR /app

# Copy project files
COPY pyproject.toml /app/pyproject.toml
COPY src /app/src
COPY tests /app/tests
COPY README.md /app/README.md

# Install runtime + dev deps (duckdb, duckcli, pytest)
RUN pip install --no-cache-dir -e . && \
    pip install --no-cache-dir duckcli pytest

# Create mount points (these are empty in image; host provides actual content)
RUN mkdir -p /ingest /data

# By default: go straight to an SQL REPL against the persisted DB file
ENTRYPOINT ["duckcli"]
