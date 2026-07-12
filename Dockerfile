FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MDS_DATABASE_PATH=/data/market.sqlite3 \
    MDS_MARKETS_CONFIG_PATH=/app/config/markets.toml \
    MDS_HTTP_HOST=0.0.0.0 \
    MDS_HTTP_PORT=8080

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY config ./config

RUN pip install --no-cache-dir . && mkdir -p /data

VOLUME ["/data"]
EXPOSE 8080

CMD ["market-data-service", "serve"]
