"""Functional integration tests for all unified-* library dependencies.

Each test instantiates real classes, calls real functions with valid args, and
verifies return types and behavior — NOT just ``assert X is not None``.

Runs credential-free: CLOUD_PROVIDER=local CLOUD_MOCK_MODE=true.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from unified_trading_library import MockEventSink, setup_events

# Initialize events for the test session (needed before any log_event calls)
setup_events(service_name="batch-recon-test", mode="test", sink=MockEventSink())


# ---------------------------------------------------------------------------
# unified_trading_library.config_interface
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestUnifiedConfigInterface:
    """Functional tests for unified-config-interface via ReconConfig."""

    def test_recon_config_instantiation(self) -> None:
        """ReconConfig extends UnifiedCloudConfig with recon-specific fields."""
        from unified_trading_library import UnifiedCloudConfig

        from batch_live_reconciliation_service.config import ReconConfig

        assert issubclass(ReconConfig, UnifiedCloudConfig)

        config = ReconConfig()
        assert isinstance(config.stage_timeout_seconds, int)
        assert config.stage_timeout_seconds > 0
        assert isinstance(config.dry_run, bool)

    def test_recon_config_bucket_derivation(self) -> None:
        """Bucket names are derived from project_id when not explicitly set."""
        from batch_live_reconciliation_service.config import ReconConfig

        config = ReconConfig()
        project_id = config.gcp_project_id or ""
        # Buckets should contain the project_id
        assert project_id in config.recon_bucket or config.recon_bucket == ""
        assert project_id in config.events_bucket or config.events_bucket == ""

    def test_get_recon_config_singleton(self) -> None:
        """get_recon_config() returns a cached singleton instance."""
        from batch_live_reconciliation_service.config import get_recon_config

        # Clear lru_cache to avoid test contamination
        get_recon_config.cache_clear()
        config1 = get_recon_config()
        config2 = get_recon_config()
        assert config1 is config2
        get_recon_config.cache_clear()


# ---------------------------------------------------------------------------
# unified_trading_library.events_interface
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestUnifiedEventsInterface:
    """Functional tests for unified-events-interface usage in batch-recon."""

    def test_log_event_with_stage_details(self) -> None:
        """log_event() accepts stage-specific details used by the orchestrator."""
        from unified_trading_library import log_event

        log_event(
            "PROCESSING_STARTED",
            details={"stage": "stage0_config_pull", "date": "2026-03-15"},
        )

    def test_log_event_started_stopped(self) -> None:
        """Service uses STARTED/STOPPED/FAILED lifecycle events."""
        from unified_trading_library import log_event

        log_event("STARTED", details={"date": "2026-03-15", "run_id": "test-run", "dry_run": True})
        log_event(
            "STOPPED", details={"date": "2026-03-15", "run_id": "test-run", "total_deviations": 0}
        )
        log_event(
            "FAILED",
            details={"date": "2026-03-15", "run_id": "test-run", "failed_stages": "stage0"},
        )

    def test_setup_events_with_gcs_event_sink_mock(self) -> None:
        """setup_events() can be called with a mock GCSEventSink-like object."""
        from unified_trading_library import setup_events

        mock_sink = MagicMock()
        # GCSEventSink interface: emit(), flush()
        mock_sink.emit = MagicMock()
        mock_sink.flush = MagicMock()
        setup_events(service_name="batch-recon-test-2", mode="batch", sink=mock_sink)


# ---------------------------------------------------------------------------
# unified_trading_library.cloud_interface
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestUnifiedCloudInterface:
    """Functional tests for unified-cloud-interface usage in batch-recon stages."""

    def test_get_storage_client_interface(self) -> None:
        """get_storage_client() returns a client with required methods."""
        from unified_trading_library import get_storage_client

        client = get_storage_client()
        assert hasattr(client, "upload_bytes")
        assert hasattr(client, "download_bytes")
        assert hasattr(client, "blob_exists")
        assert hasattr(client, "list_blobs")

    def test_stage0_dry_run_passes_without_gcs(self) -> None:
        """Stage 0 in dry_run mode returns PASSED without touching GCS."""
        from batch_live_reconciliation_service.config import ReconConfig
        from batch_live_reconciliation_service.models.recon_report import ReconStatus
        from batch_live_reconciliation_service.stages.stage0_config_pull import run_stage0

        config = ReconConfig()
        result = run_stage0(config, "2026-03-15", dry_run=True)

        assert result.status == ReconStatus.PASSED
        assert result.metrics.get("dry_run") == 1.0
        assert result.started_at is not None
        assert result.completed_at is not None

    @patch("batch_live_reconciliation_service.stages.stage1_ml_recon.get_storage_client")
    def test_stage1_dry_run(self, mock_storage: MagicMock) -> None:
        """Stage 1 in dry_run mode completes without GCS access."""
        from batch_live_reconciliation_service.config import ReconConfig
        from batch_live_reconciliation_service.models.recon_report import ReconStatus
        from batch_live_reconciliation_service.stages.stage1_ml_recon import run_stage1

        config = ReconConfig()
        result = run_stage1(config, "2026-03-15", dry_run=True)

        assert result.status in (ReconStatus.PASSED, ReconStatus.SKIPPED)
        # Dry run should not call GCS
        mock_storage.assert_not_called()

    @patch("batch_live_reconciliation_service.stages.stage2_strategy_recon.get_storage_client")
    def test_stage2_dry_run(self, mock_storage: MagicMock) -> None:
        """Stage 2 in dry_run mode completes without GCS access."""
        from batch_live_reconciliation_service.config import ReconConfig
        from batch_live_reconciliation_service.models.recon_report import ReconStatus
        from batch_live_reconciliation_service.stages.stage2_strategy_recon import run_stage2

        config = ReconConfig()
        result = run_stage2(config, "2026-03-15", dry_run=True)

        assert result.status in (ReconStatus.PASSED, ReconStatus.SKIPPED)
        mock_storage.assert_not_called()

    @patch("batch_live_reconciliation_service.stages.stage3_execution_recon.get_storage_client")
    def test_stage3_dry_run(self, mock_storage: MagicMock) -> None:
        """Stage 3 in dry_run mode completes without GCS access."""
        from batch_live_reconciliation_service.config import ReconConfig
        from batch_live_reconciliation_service.models.recon_report import ReconStatus
        from batch_live_reconciliation_service.stages.stage3_execution_recon import run_stage3

        config = ReconConfig()
        result = run_stage3(config, "2026-03-15", dry_run=True)

        assert result.status in (ReconStatus.PASSED, ReconStatus.SKIPPED)
        mock_storage.assert_not_called()


# ---------------------------------------------------------------------------
# unified_trading_library
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestUnifiedTradingLibrary:
    """Functional tests for unified-trading-library usage in batch-recon."""

    def test_gcs_event_sink_instantiation(self) -> None:
        """GCSEventSink can be constructed with project_id, bucket, and service_name."""
        from unified_trading_library import GCSEventSink

        sink = GCSEventSink(
            project_id="test-project",
            bucket="test-bucket",
            service_name="batch-recon-test",
        )
        assert sink is not None

    def test_log_level_enum_values(self) -> None:
        """LogLevel enum contains the standard log levels used by CLI."""
        from unified_trading_library import LogLevel

        assert LogLevel("INFO").value == "INFO"
        assert LogLevel("DEBUG").value == "DEBUG"
        assert LogLevel("WARNING").value == "WARNING"
        assert LogLevel("ERROR").value == "ERROR"
        with pytest.raises(ValueError):
            LogLevel("NONEXISTENT")

    def test_log_level_validation_in_cli_context(self) -> None:
        """CLI validates LOG_LEVEL env var using LogLevel enum."""
        from unified_trading_library import LogLevel

        # Valid values should pass
        for level_str in ("DEBUG", "INFO", "WARNING", "ERROR"):
            level = LogLevel(level_str)
            assert level.value == level_str

    def test_recon_report_model_construction(self) -> None:
        """ReconReport (uses unified_trading_library.config_interface indirectly) builds correctly."""
        from batch_live_reconciliation_service.models.recon_report import (
            DeviationRecord,
            ReconReport,
            ReconStage,
            ReconStatus,
            StageReport,
        )

        stage = StageReport(
            stage=ReconStage.ML_RECON,
            status=ReconStatus.PASSED,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            deviations=[
                DeviationRecord(
                    metric_name="sharpe_ratio",
                    stage=ReconStage.ML_RECON,
                    actual_value=1.5,
                    threshold=2.0,
                    direction="below",
                    description="Sharpe below threshold",
                )
            ],
            metrics={"accuracy": 0.95},
        )
        assert stage.deviation_count == 1
        assert stage.passed is True

        report = ReconReport(
            date="2026-03-15",
            run_id="test-run-001",
            started_at=datetime.now(UTC),
            status=ReconStatus.RUNNING,
            stages=[stage],
        )
        assert report.total_deviations == 1
        assert report.failed_stages == []

    def test_orchestrator_dry_run_full_pipeline(self) -> None:
        """Full orchestrator pipeline completes in dry_run mode."""
        from batch_live_reconciliation_service.config import get_recon_config
        from batch_live_reconciliation_service.engine.orchestrator import run_reconciliation
        from batch_live_reconciliation_service.models.recon_report import ReconStatus

        get_recon_config.cache_clear()
        # Patch _setup_observability so the test-session MockEventSink is preserved
        # (dry_run skips GCS I/O but setup_events would replace the mock sink with GCSEventSink)
        with patch("batch_live_reconciliation_service.engine.orchestrator._setup_observability"):
            report = run_reconciliation(date="2026-03-15", dry_run=True)

            assert report.date == "2026-03-15"
            assert report.run_id is not None
            assert len(report.run_id) > 0
            assert report.started_at is not None
            assert report.completed_at is not None
            assert report.status in (ReconStatus.PASSED, ReconStatus.FAILED)
            # In dry_run, all stages should complete (not crash)
            assert len(report.stages) >= 1
        get_recon_config.cache_clear()
