# Use Python 3.13 as base image (matching CI/CD)
FROM python:3.13-slim

# Add metadata labels
LABEL maintainer="Weather Source ETL Team" \
      description="Weather Source Data Pipeline" \
      version="1.0"

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PATH="/home/appuser/.local/bin:$PATH"

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY src/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and tests
COPY . .

# Create data directory and set permissions
RUN mkdir -p data && \
    chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Add healthcheck script
RUN chmod +x /app/deploy/docker/healthcheck.py

# Add healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python /app/deploy/docker/healthcheck.py || exit 1

# Create volume for data persistence
VOLUME ["/app/data"]

# Set entrypoint to use shell
ENTRYPOINT ["/bin/bash", "-c"]

# Default command (can be overridden)
CMD ["python src/main.py --help"]
