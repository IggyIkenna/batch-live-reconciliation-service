ARG PROJECT_ID
# Digest-pinned UTL base image (QG STEP 5.79 -- reproducible builds + UTL/UAC provenance).
# Refreshed by the dependency-update fan-out (update-dependency-version.yml) on base-image
# republish; cloudbuild may override at build time: --build-arg BASE_IMAGE_DIGEST=sha256:...
ARG BASE_IMAGE_DIGEST=sha256:d06e0f12417e08e97a60aca4f08c84274e48972230a0de355462c740beffd942
FROM --platform=linux/amd64 asia-northeast1-docker.pkg.dev/${PROJECT_ID}/unified-trading-library/unified-trading-library@${BASE_IMAGE_DIGEST}

WORKDIR /app/batch-live-reconciliation-service

COPY pyproject.toml uv.lock ./
COPY batch_live_reconciliation_service/ ./batch_live_reconciliation_service/
COPY scripts/ ./scripts/
COPY tests/ ./tests/
COPY cloudbuild.yaml ./
# cloud-providers.yaml is UAC-packaged since 2026-06-10 (unified_api_contracts/config/) and read
# via importlib.resources as the always-available default — no local COPY/ENV needed (the prior
# COPY referenced a configs/ dir absent from the build context → build broke; mtds/instruments
# carry no such COPY). SSOT: CLAUDE.md § Bucket-name SSOT.

# --no-deps: UTL base image pre-installs unified-trading-library and
# unified-api-contracts; avoids needing local path deps in build context.
# scm-version-fix: pretend version for editable install (D13 git-tag versioning)
ARG SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${SETUPTOOLS_SCM_PRETEND_VERSION}
RUN uv pip install --system -e . --no-deps

ENTRYPOINT ["python", "-m", "batch_live_reconciliation_service"]
