FROM python:3.12-slim AS builder

WORKDIR /build

COPY pyproject.toml README.md ./
COPY src ./src

RUN python -m pip wheel --no-cache-dir --wheel-dir /wheels .


FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MDS_DATABASE_PATH=/data/market.sqlite3 \
    MDS_MARKETS_CONFIG_PATH=/app/config/markets.toml \
    MDS_HTTP_HOST=0.0.0.0 \
    MDS_HTTP_PORT=8080

WORKDIR /app

COPY --from=builder /wheels /wheels
COPY --chown=0:0 config ./config

RUN groupadd --gid 10001 mds \
    && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /nonexistent \
        --shell /usr/sbin/nologin mds \
    && python -m pip install --no-cache-dir /wheels/*.whl \
    && rm -rf /wheels \
    && mkdir -p /data \
    && chown 10001:10001 /data \
    && chmod 0750 /data \
    && chmod -R a-w /app

VOLUME ["/data"]
EXPOSE 8080

USER 10001:10001

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=3).close()"]

CMD ["market-data-service", "serve"]
