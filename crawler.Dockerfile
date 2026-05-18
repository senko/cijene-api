FROM python:3.13.13-alpine3.23 AS builder

ENV PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

RUN pip install --no-cache-dir uv==0.11.8

RUN apk add --no-cache \
    gcc \
    musl-dev \
    libxml2-dev \
    libxslt-dev

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY crawler/ ./crawler/
RUN uv sync --frozen --no-dev

FROM python:3.13.13-alpine3.23 AS cijene-api-dev-crawler

ENV PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

RUN addgroup -g 1000 -S app && \
    adduser -u 1000 -S app -G app && \
    mkdir -p /app/output && \
    chown -R app:app /app

RUN apk add --no-cache \
    libxml2 \
    libxslt

WORKDIR /app

COPY --from=builder /app /app

USER app

ENTRYPOINT ["python", "-m", "crawler.cli.crawl"]
CMD ["/app/output"]