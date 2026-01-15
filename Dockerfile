# Cardano Batcher Template
# Production-ready Dockerfile

# Build stage
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Runtime stage
FROM python:3.11-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsodium23 \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy application code
COPY src/ ./src/
COPY pyproject.toml .
COPY README.md .

# Install the package
RUN pip install -e .

# Create non-root user
RUN useradd -m -s /bin/bash batcher
USER batcher

# Create data directory
RUN mkdir -p /app/data

# Default environment variables
ENV BATCHER_NETWORK=preprod \
    BATCHER_NODE_PROVIDER=blockfrost \
    BATCHER_LOG_LEVEL=INFO \
    BATCHER_DATABASE_URL=sqlite+aiosqlite:///data/batcher.db

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import batcher; print('OK')" || exit 1

# Entry point
ENTRYPOINT ["python", "-m", "batcher.cli"]
CMD ["--help"]


