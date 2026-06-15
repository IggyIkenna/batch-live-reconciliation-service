ARG PROJECT_ID
# Digest-pinned UTL base image (QG STEP 5.79 -- reproducible builds + UTL/UAC provenance).
# Refreshed by the dependency-update fan-out (update-dependency-version.yml) on base-image
# republish; cloudbuild may override at build time: --build-arg BASE_IMAGE_DIGEST=sha256:...
ARG BASE_IMAGE_DIGEST=sha256:8e301dbd97433e95c3d2e79cde6608de1715bfdc0d38a83c64ebd1dd0e1ac379
FROM --platform=linux/amd64 asia-northeast1-docker.pkg.dev/${PROJECT_ID}/unified-trading-library/unified-trading-library@${BASE_IMAGE_DIGEST}

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
