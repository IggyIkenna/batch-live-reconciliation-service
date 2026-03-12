FROM python:3.13-slim

WORKDIR /app

COPY pyproject.toml .
COPY batch_live_reconciliation_service/ ./batch_live_reconciliation_service/

RUN uv pip install --no-cache-dir -e .

ENTRYPOINT ["python", "-m", "batch_live_reconciliation_service"]
