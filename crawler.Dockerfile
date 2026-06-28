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
# cities.csv (city-name map) is loaded at runtime by crawler/store/cities.py.
COPY enrichment/ ./enrichment/
RUN uv sync --frozen --no-dev

FROM python:3.13.13-alpine3.23 AS crawler

ENV PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

RUN addgroup -S app && adduser -S app -G app

RUN apk add --no-cache \
    libxml2 \
    libxslt

WORKDIR /app

COPY --from=builder /app /app
RUN mkdir -p /app/output && chown -R app:app /app

USER app

ENTRYPOINT ["python", "-m", "crawler.cli.crawl"]
CMD ["/app/output"]