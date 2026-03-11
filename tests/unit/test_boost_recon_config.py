"""
Unit tests for ReconConfig.

Tests bucket name derivation via GCP_PROJECT_ID env var and field defaults.
"""

from __future__ import annotations

import os
from unittest.mock import patch


def test_recon_config_derives_buckets_from_project_id() -> None:
    # gcp_project_id is populated from GCP_PROJECT_ID env var in UnifiedCloudConfig
    with patch.dict(os.environ, {"GCP_PROJECT_ID": "my-project", "CLOUD_PROVIDER": "local"}):
        from batch_live_reconciliation_service.config import ReconConfig

        cfg = ReconConfig()
        assert cfg.recon_bucket == "recon-my-project"
        assert cfg.events_bucket == "my-project-events"
        assert cfg.execution_store_bucket == "execution-store-my-project"


def test_recon_config_explicit_buckets_not_overridden() -> None:
    from batch_live_reconciliation_service.config import ReconConfig

    cfg = ReconConfig(
        cloud_provider="local",
        recon_bucket="custom-recon",
        events_bucket="custom-events",
        execution_store_bucket="custom-exec",
    )
    assert cfg.recon_bucket == "custom-recon"
    assert cfg.events_bucket == "custom-events"
    assert cfg.execution_store_bucket == "custom-exec"


def test_recon_config_defaults_stage_timeout() -> None:
    from batch_live_reconciliation_service.config import ReconConfig

    cfg = ReconConfig(cloud_provider="local")
    assert cfg.stage_timeout_seconds == 1800


def test_recon_config_dry_run_defaults_false() -> None:
    from batch_live_reconciliation_service.config import ReconConfig

    cfg = ReconConfig(cloud_provider="local")
    assert cfg.dry_run is False


def test_recon_config_dry_run_can_be_set() -> None:
    from batch_live_reconciliation_service.config import ReconConfig

    cfg = ReconConfig(cloud_provider="local", dry_run=True)
    assert cfg.dry_run is True


def test_recon_config_empty_project_derives_prefixed_bucket_names() -> None:
    from batch_live_reconciliation_service.config import ReconConfig

    cfg = ReconConfig(cloud_provider="local")
    # With no project set, derived names still contain the category prefix
    assert "recon" in cfg.recon_bucket
    assert "events" in cfg.events_bucket
    assert "execution-store" in cfg.execution_store_bucket
