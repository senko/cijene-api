FROM cijene-api-dev-crawler

USER root

ENV SUPERCRONIC_URL=https://github.com/aptible/supercronic/releases/download/v0.2.45/supercronic-linux-amd64 \
    SUPERCRONIC_SHA1SUM=e894b193bea75a5ee644e700c59e30eedc804cf7 \
    SUPERCRONIC=supercronic-linux-amd64

RUN apk add --no-cache curl \
&& curl -fsSLO "$SUPERCRONIC_URL" \
&& chmod +x "$SUPERCRONIC" \
&& mv "$SUPERCRONIC" "/usr/local/bin/${SUPERCRONIC}" \
&& ln -s "/usr/local/bin/${SUPERCRONIC}" /usr/local/bin/supercronic

COPY scheduled-run.sh /app/scheduled-run.sh
RUN chmod +x /app/scheduled-run.sh

COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/scheduled-run.sh /app/entrypoint.sh

USER app
ENTRYPOINT ["/app/entrypoint.sh"]