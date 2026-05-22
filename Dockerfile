ARG PROJECT_ID
FROM --platform=linux/amd64 asia-northeast1-docker.pkg.dev/${PROJECT_ID}/unified-trading-library/unified-trading-library:latest

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY batch_live_reconciliation_service/ ./batch_live_reconciliation_service/
COPY scripts/ ./scripts/

# --no-deps: UTL base image pre-installs unified-trading-library and
# unified-api-contracts; avoids needing local path deps in build context.
RUN uv pip install --system -e . --no-deps

ENTRYPOINT ["python", "-m", "batch_live_reconciliation_service"]
