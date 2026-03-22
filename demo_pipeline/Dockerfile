FROM python:3.13.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Set working directory
WORKDIR /app

# Copy dependency files first (for caching)
COPY pyproject.toml uv.lock .python-version ./

# Install dependencies
RUN uv sync --locked

# Copy pipeline files
COPY ingest.py transform.sql yellow_tripdata_2021-01.csv.gz taxi_zone_lookup.csv ./

# Add venv to PATH so we don't need uv run
ENV PATH="/app/.venv/bin:$PATH"

# Run the pipeline
CMD ["python", "ingest.py"]