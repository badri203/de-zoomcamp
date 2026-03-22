FROM python:3.13-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy dependency files first (caching layer)
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen

# Copy pipeline code
COPY ingest.py ./

# Copy data files
COPY data/raw/ data/raw/

# Run pipeline
ENTRYPOINT ["uv", "run", "ingest.py"]