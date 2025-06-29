# Multi-stage build for crawler/API services
FROM python:3.13-slim AS base

# Add labels for better container management
LABEL org.opencontainers.image.title="Cijene API"
LABEL org.opencontainers.image.description="Croatian grocery price tracking service"
LABEL org.opencontainers.image.version="0.1.0"
LABEL org.opencontainers.image.authors="Cijene API Team"

# Set environment variables for Python optimization and locale
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUTF8=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=Europe/Zagreb \
    DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    unzip \
    ca-certificates \
    tzdata \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    && apt-get autoremove -y

# Install uv (pinned version for reproducibility)
RUN pip install --no-cache-dir uv==0.7.14

FROM base AS deps
WORKDIR /app

# Copy dependency files first (better caching)
COPY pyproject.toml uv.lock ./

FROM deps AS common

# Create non-root user with proper security settings
RUN useradd --create-home --shell /bin/bash --uid 1001 --no-log-init appuser \
    && chown -R appuser:appuser /app \
    && mkdir -p /app/data /app/output \
    && chown -R appuser:appuser /app/data /app/output

# Switch to non-root user early for security
USER appuser

# Development targets
FROM common AS development-api
EXPOSE 8000
RUN uv sync --frozen --dev
CMD ["uv", "run", "-m", "service.main", "--reload"]

FROM common AS development-crawler
RUN uv sync --frozen --dev
CMD ["uv", "run", "-m", "crawler.cli.crawl", "/app/output"]

# Production targets
FROM common AS production-api
EXPOSE 8000
RUN uv sync --frozen --no-dev
COPY --chown=appuser:appuser service/ ./service/
COPY --chown=appuser:appuser enrichment/ ./enrichment/
CMD ["uv", "run", "-m", "service.main"]

FROM common AS production-crawler
RUN uv sync --frozen --no-dev
COPY --chown=appuser:appuser crawler/ ./crawler/
CMD ["uv", "run", "-m", "crawler.cli.crawl", "/app/output"]