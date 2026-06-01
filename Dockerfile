ARG PROJECT_ID
FROM --platform=linux/amd64 asia-northeast1-docker.pkg.dev/${PROJECT_ID}/unified-trading-library/unified-trading-library:latest

WORKDIR /app/batch-live-reconciliation-service

COPY pyproject.toml uv.lock ./
COPY batch_live_reconciliation_service/ ./batch_live_reconciliation_service/
COPY scripts/ ./scripts/
COPY tests/ ./tests/
COPY cloudbuild.yaml ./
# cloud-providers.yaml copied from deployment-service by cloudbuild.yaml build step
COPY configs/cloud-providers.yaml ./configs/cloud-providers.yaml

# --no-deps: UTL base image pre-installs unified-trading-library and
# unified-api-contracts; avoids needing local path deps in build context.
RUN uv pip install --system -e . --no-deps

ENV UNIFIED_TRADING_CLOUD_PROVIDERS_YAML=/app/batch-live-reconciliation-service/configs/cloud-providers.yaml

ENTRYPOINT ["python", "-m", "batch_live_reconciliation_service"]
