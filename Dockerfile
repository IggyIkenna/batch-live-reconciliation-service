ARG PROJECT_ID
FROM --platform=linux/amd64 asia-northeast1-docker.pkg.dev/${PROJECT_ID}/unified-trading-library/unified-trading-library:latest

WORKDIR /app

COPY pyproject.toml .
COPY batch_live_reconciliation_service/ ./batch_live_reconciliation_service/

RUN uv pip install --system --no-cache-dir -e .

ENTRYPOINT ["python", "-m", "batch_live_reconciliation_service"]
