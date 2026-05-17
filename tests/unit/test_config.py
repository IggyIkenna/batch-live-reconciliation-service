"""Unit tests for batch-live-reconciliation-service config."""

from __future__ import annotations

import os
from unittest.mock import patch


def test_config_bucket_derivation() -> None:
    """Test that config derives bucket names from GCP_PROJECT_ID."""
    with patch.dict(os.environ, {"GCP_PROJECT_ID": "my-project", "CLOUD_PROVIDER": "local"}):
        from batch_live_reconciliation_service.config import ReconConfig

        config = ReconConfig()
        assert config.recon_bucket == "recon-my-project"
        assert config.events_bucket == "my-project-events"
        assert config.execution_store_bucket == "execution-store-cefi-my-project"


def test_config_dry_run_default() -> None:
    """Test that dry_run defaults to False."""
    with patch.dict(os.environ, {"GCP_PROJECT_ID": "test-project", "CLOUD_PROVIDER": "local"}):
        from batch_live_reconciliation_service.config import ReconConfig

        config = ReconConfig(cloud_provider="local")
        assert config.dry_run is False


def test_config_stage_timeout_default() -> None:
    """Test that stage_timeout_seconds has a sensible default."""
    with patch.dict(os.environ, {"GCP_PROJECT_ID": "test-project", "CLOUD_PROVIDER": "local"}):
        from batch_live_reconciliation_service.config import ReconConfig

        config = ReconConfig(cloud_provider="local")
        assert config.stage_timeout_seconds == 1800
