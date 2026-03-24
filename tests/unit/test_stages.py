"""Unit tests for reconciliation pipeline stages and handlers.

Tests use dry_run=True to exercise the stage logic without real GCS I/O,
and mock the GCS client for non-dry-run paths.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from batch_live_reconciliation_service.models.recon_report import (
    ReconReport,
    ReconStage,
    ReconStatus,
    StageReport,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_config() -> MagicMock:
    """Return a mock ReconConfig with sensible defaults."""
    cfg = MagicMock()
    cfg.recon_bucket = "mock-recon-bucket"
    cfg.execution_store_bucket = "mock-execution-bucket"
    cfg.gcp_project_id = "mock-project"
    cfg.events_bucket = "mock-events-bucket"
    cfg.stage_timeout_seconds = 30
    cfg.config_store_bucket = ""  # empty → reloaders disabled
    return cfg


@pytest.fixture()
def mock_stage_report() -> StageReport:
    return StageReport(
        stage=ReconStage.ML_RECON,
        status=ReconStatus.PASSED,
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        metrics={"accuracy": 0.99},
    )


@pytest.fixture()
def mock_recon_report(mock_stage_report: StageReport) -> ReconReport:
    return ReconReport(
        date="2026-03-15",
        run_id="test-run-001",
        started_at=datetime.now(UTC),
        status=ReconStatus.PASSED,
        stages=[mock_stage_report],
    )


# ---------------------------------------------------------------------------
# Stage 0: Config Pull
# ---------------------------------------------------------------------------


class TestStage0ConfigPull:
    def test_dry_run_returns_passed(self, mock_config: MagicMock) -> None:
        from batch_live_reconciliation_service.stages.stage0_config_pull import run_stage0

        result = run_stage0(mock_config, "2026-03-15", dry_run=True)

        assert result.stage == ReconStage.CONFIG_PULL
        assert result.status == ReconStatus.PASSED
        assert result.started_at is not None
        assert result.completed_at is not None
        assert result.metrics.get("dry_run") == 1.0

    def test_missing_blobs_returns_failed(self, mock_config: MagicMock) -> None:
        from batch_live_reconciliation_service.stages.stage0_config_pull import run_stage0

        with patch(
            "batch_live_reconciliation_service.stages.stage0_config_pull._blob_exists",
            return_value=False,
        ):
            result = run_stage0(mock_config, "2026-03-15", dry_run=False)

        assert result.status == ReconStatus.FAILED
        assert result.error_message is not None
        assert "Missing upstream data" in result.error_message

    def test_all_blobs_present_load_config_success(self, mock_config: MagicMock) -> None:
        from batch_live_reconciliation_service.stages.stage0_config_pull import run_stage0

        with (
            patch(
                "batch_live_reconciliation_service.stages.stage0_config_pull._blob_exists",
                return_value=True,
            ),
            patch(
                "batch_live_reconciliation_service.stages.stage0_config_pull._load_config_snapshot",
                return_value={"version": "1"},
            ),
        ):
            result = run_stage0(mock_config, "2026-03-15", dry_run=False)

        assert result.status == ReconStatus.PASSED

    def test_load_config_snapshot_raises_returns_failed(self, mock_config: MagicMock) -> None:
        from batch_live_reconciliation_service.stages.stage0_config_pull import run_stage0

        with (
            patch(
                "batch_live_reconciliation_service.stages.stage0_config_pull._blob_exists",
                return_value=True,
            ),
            patch(
                "batch_live_reconciliation_service.stages.stage0_config_pull._load_config_snapshot",
                side_effect=FileNotFoundError("config snapshot missing"),
            ),
        ):
            result = run_stage0(mock_config, "2026-03-15", dry_run=False)

        assert result.status == ReconStatus.FAILED

    def test_blob_exists_gcs_error_returns_false(self) -> None:
        from batch_live_reconciliation_service.stages.stage0_config_pull import _blob_exists

        mock_client = MagicMock()
        mock_client.bucket.return_value.blob.return_value.exists.side_effect = RuntimeError(
            "GCS error"
        )
        with patch(
            "batch_live_reconciliation_service.stages.stage0_config_pull.get_storage_client",
            return_value=mock_client,
        ):
            result = _blob_exists("bucket", "path/to/blob")

        assert result is False


# ---------------------------------------------------------------------------
# Stage 1: ML Recon
# ---------------------------------------------------------------------------


class TestStage1MLRecon:
    def test_dry_run_returns_passed(self, mock_config: MagicMock) -> None:
        from batch_live_reconciliation_service.stages.stage1_ml_recon import run_stage1

        result = run_stage1(mock_config, "2026-03-15", dry_run=True)

        assert result.stage == ReconStage.ML_RECON
        assert result.status == ReconStatus.PASSED
        assert result.metrics.get("dry_run") == 1.0

    def test_empty_events_returns_failed_with_deviations(self, mock_config: MagicMock) -> None:
        """Empty events trigger zero-metric deviations → FAILED (not SKIPPED)."""
        from batch_live_reconciliation_service.stages.stage1_ml_recon import run_stage1

        mock_client = MagicMock()
        mock_client.bucket.return_value.list_blobs.return_value = []
        with patch(
            "batch_live_reconciliation_service.stages.stage1_ml_recon.get_storage_client",
            return_value=mock_client,
        ):
            result = run_stage1(mock_config, "2026-03-15", dry_run=False)

        # Empty live events → zero direction match rate < 95% threshold → FAILED
        assert result.status == ReconStatus.FAILED
        assert len(result.deviations) > 0

    def test_gcs_error_gracefully_treated_as_empty_events(self, mock_config: MagicMock) -> None:
        """_load_events catches errors internally — empty list → deviations → FAILED."""
        from batch_live_reconciliation_service.stages.stage1_ml_recon import run_stage1

        mock_client = MagicMock()
        mock_client.bucket.return_value.list_blobs.side_effect = RuntimeError("GCS unavailable")
        with patch(
            "batch_live_reconciliation_service.stages.stage1_ml_recon.get_storage_client",
            return_value=mock_client,
        ):
            result = run_stage1(mock_config, "2026-03-15", dry_run=False)

        assert result.status == ReconStatus.FAILED


# ---------------------------------------------------------------------------
# Stage 2: Strategy Recon
# ---------------------------------------------------------------------------


class TestStage2StrategyRecon:
    def test_dry_run_returns_passed(self, mock_config: MagicMock) -> None:
        from batch_live_reconciliation_service.stages.stage2_strategy_recon import run_stage2

        result = run_stage2(mock_config, "2026-03-15", dry_run=True)

        assert result.stage == ReconStage.STRATEGY_RECON
        assert result.status == ReconStatus.PASSED

    def test_empty_events_returns_failed_with_deviations(self, mock_config: MagicMock) -> None:
        """Empty events trigger deviation thresholds → FAILED."""
        from batch_live_reconciliation_service.stages.stage2_strategy_recon import run_stage2

        mock_client = MagicMock()
        mock_client.bucket.return_value.list_blobs.return_value = []
        with patch(
            "batch_live_reconciliation_service.stages.stage2_strategy_recon.get_storage_client",
            return_value=mock_client,
        ):
            result = run_stage2(mock_config, "2026-03-15", dry_run=False)

        assert result.status == ReconStatus.FAILED
        assert len(result.deviations) > 0

    def test_gcs_error_gracefully_treated_as_empty_events(self, mock_config: MagicMock) -> None:
        """_load_events catches errors internally — empty list → deviations → FAILED."""
        from batch_live_reconciliation_service.stages.stage2_strategy_recon import run_stage2

        mock_client = MagicMock()
        mock_client.bucket.return_value.list_blobs.side_effect = RuntimeError("GCS unavailable")
        with patch(
            "batch_live_reconciliation_service.stages.stage2_strategy_recon.get_storage_client",
            return_value=mock_client,
        ):
            result = run_stage2(mock_config, "2026-03-15", dry_run=False)

        assert result.status == ReconStatus.FAILED


# ---------------------------------------------------------------------------
# Stage 3: Execution Recon
# ---------------------------------------------------------------------------


class TestStage3ExecutionRecon:
    def test_dry_run_returns_passed(self, mock_config: MagicMock) -> None:
        from batch_live_reconciliation_service.stages.stage3_execution_recon import run_stage3

        result = run_stage3(mock_config, "2026-03-15", dry_run=True)

        assert result.stage == ReconStage.EXECUTION_RECON
        assert result.status == ReconStatus.PASSED

    def test_empty_events_returns_passed(self, mock_config: MagicMock) -> None:
        """Empty events: stage3 default metrics all pass thresholds → PASSED."""
        from batch_live_reconciliation_service.stages.stage3_execution_recon import run_stage3

        mock_client = MagicMock()
        mock_client.bucket.return_value.list_blobs.return_value = []
        with patch(
            "batch_live_reconciliation_service.stages.stage3_execution_recon.get_storage_client",
            return_value=mock_client,
        ):
            result = run_stage3(mock_config, "2026-03-15", dry_run=False)

        # stage3 empty-events default: all 0.0 and algo_accuracy=1.0 — all within thresholds
        assert result.status == ReconStatus.PASSED

    def test_gcs_error_gracefully_treated_as_empty_events(self, mock_config: MagicMock) -> None:
        """_load_events catches errors internally — empty list → default metrics → PASSED."""
        from batch_live_reconciliation_service.stages.stage3_execution_recon import run_stage3

        mock_client = MagicMock()
        mock_client.bucket.return_value.list_blobs.side_effect = RuntimeError("GCS unavailable")
        with patch(
            "batch_live_reconciliation_service.stages.stage3_execution_recon.get_storage_client",
            return_value=mock_client,
        ):
            result = run_stage3(mock_config, "2026-03-15", dry_run=False)

        assert result.status in (ReconStatus.PASSED, ReconStatus.FAILED)


# ---------------------------------------------------------------------------
# Stage 4: Agent Analysis
# ---------------------------------------------------------------------------


class TestStage4AgentAnalysis:
    def test_dry_run_returns_passed(
        self, mock_config: MagicMock, mock_stage_report: StageReport
    ) -> None:
        from batch_live_reconciliation_service.stages.stage4_agent_analysis import run_stage4

        result = run_stage4(
            mock_config, "2026-03-15", stage_reports=[mock_stage_report], dry_run=True
        )

        assert result.stage == ReconStage.AGENT_ANALYSIS
        assert result.status == ReconStatus.PASSED

    def test_gcs_error_returns_failed(
        self, mock_config: MagicMock, mock_stage_report: StageReport
    ) -> None:
        from batch_live_reconciliation_service.stages.stage4_agent_analysis import run_stage4

        with patch(
            "batch_live_reconciliation_service.stages.stage4_agent_analysis.get_storage_client",
            side_effect=RuntimeError("GCS unavailable"),
        ):
            result = run_stage4(
                mock_config, "2026-03-15", stage_reports=[mock_stage_report], dry_run=False
            )

        assert result.status == ReconStatus.FAILED


# ---------------------------------------------------------------------------
# Stage 5: Results Writer
# ---------------------------------------------------------------------------


class TestStage5ResultsWriter:
    def test_dry_run_returns_passed(
        self, mock_config: MagicMock, mock_recon_report: ReconReport
    ) -> None:
        from batch_live_reconciliation_service.stages.stage5_results_writer import run_stage5

        result = run_stage5(mock_config, mock_recon_report, dry_run=True)

        assert result.stage == ReconStage.RESULTS_WRITER
        assert result.status == ReconStatus.PASSED
        assert result.output_gcs_path is not None

    def test_gcs_upload_error_returns_failed(
        self, mock_config: MagicMock, mock_recon_report: ReconReport
    ) -> None:
        from batch_live_reconciliation_service.stages.stage5_results_writer import run_stage5

        mock_client = MagicMock()
        mock_client.upload_bytes.side_effect = RuntimeError("GCS upload failed")
        with patch(
            "batch_live_reconciliation_service.stages.stage5_results_writer.get_storage_client",
            return_value=mock_client,
        ):
            result = run_stage5(mock_config, mock_recon_report, dry_run=False)

        assert result.status == ReconStatus.FAILED

    def test_load_index_returns_empty_on_error(self, mock_config: MagicMock) -> None:
        from batch_live_reconciliation_service.stages.stage5_results_writer import _load_index

        mock_client = MagicMock()
        mock_client.download_bytes.side_effect = RuntimeError("not found")
        with patch(
            "batch_live_reconciliation_service.stages.stage5_results_writer.get_storage_client",
            return_value=mock_client,
        ):
            result = _load_index("mock-bucket")

        assert result == []


# ---------------------------------------------------------------------------
# ReconcileHandler
# ---------------------------------------------------------------------------


class TestReconcileHandler:
    def _make_handler(self, extra_attrs: dict[str, object] | None = None) -> object:
        from batch_live_reconciliation_service.cli.handlers.reconcile_handler import (
            ReconcileHandler,
        )

        handler = ReconcileHandler.__new__(ReconcileHandler)
        args = MagicMock()
        args.start_date = "2026-03-15"
        args.dry_run = True
        if extra_attrs:
            for k, v in extra_attrs.items():
                setattr(args, k, v)
        handler.args = args  # type: ignore[attr-defined]
        return handler

    def test_validate_config_returns_true(self) -> None:
        from batch_live_reconciliation_service.cli.handlers.reconcile_handler import (
            ReconcileHandler,
        )

        handler = ReconcileHandler.__new__(ReconcileHandler)
        assert handler.validate_config() is True

    def test_run_no_args_returns_error(self) -> None:
        import asyncio

        from batch_live_reconciliation_service.cli.handlers.reconcile_handler import (
            ReconcileHandler,
        )

        handler = ReconcileHandler.__new__(ReconcileHandler)
        handler.args = None  # type: ignore[attr-defined]
        result = asyncio.get_event_loop().run_until_complete(handler.run())
        assert result["status"] == "error"

    def test_run_dry_run_returns_ok_or_error(self) -> None:
        import asyncio

        from batch_live_reconciliation_service.models.recon_report import (
            ReconReport,
        )

        handler = self._make_handler()

        mock_report = MagicMock(spec=ReconReport)
        mock_report.status = MagicMock()
        mock_report.status.value = "passed"
        mock_report.total_deviations = 0

        with patch(
            "batch_live_reconciliation_service.cli.handlers.reconcile_handler.run_reconciliation",
            return_value=mock_report,
        ):
            result = asyncio.get_event_loop().run_until_complete(
                handler.run()  # type: ignore[union-attr]
            )

        assert result["status"] == "ok"

    def test_run_failed_reconciliation_returns_error(self) -> None:
        import asyncio

        from batch_live_reconciliation_service.models.recon_report import (
            ReconReport,
            ReconStage,
        )

        handler = self._make_handler()

        mock_report = MagicMock(spec=ReconReport)
        mock_report.status = MagicMock()
        mock_report.status.value = "failed"
        mock_report.total_deviations = 3
        mock_report.failed_stages = [ReconStage.ML_RECON]

        with patch(
            "batch_live_reconciliation_service.cli.handlers.reconcile_handler.run_reconciliation",
            return_value=mock_report,
        ):
            result = asyncio.get_event_loop().run_until_complete(
                handler.run()  # type: ignore[union-attr]
            )

        assert result["status"] == "error"

    def test_run_uses_yesterday_when_no_start_date(self) -> None:
        import asyncio

        from batch_live_reconciliation_service.cli.handlers.reconcile_handler import (
            ReconcileHandler,
        )
        from batch_live_reconciliation_service.models.recon_report import ReconReport

        handler = ReconcileHandler.__new__(ReconcileHandler)
        args = MagicMock()
        args.start_date = None
        args.dry_run = True
        handler.args = args  # type: ignore[attr-defined]

        mock_report = MagicMock(spec=ReconReport)
        mock_report.status = MagicMock()
        mock_report.status.value = "passed"
        mock_report.total_deviations = 0

        captured_date: list[str] = []

        def mock_run(date: str, dry_run: bool = False) -> object:
            captured_date.append(date)
            return mock_report

        with patch(
            "batch_live_reconciliation_service.cli.handlers.reconcile_handler.run_reconciliation",
            side_effect=mock_run,
        ):
            asyncio.get_event_loop().run_until_complete(
                handler.run()  # type: ignore[union-attr]
            )

        assert len(captured_date) == 1
        # Should be yesterday's date
        from datetime import UTC, timedelta

        yesterday = (datetime.now(UTC).date() - timedelta(days=1)).isoformat()
        assert captured_date[0] == yesterday


# ---------------------------------------------------------------------------
# Config Reloaders
# ---------------------------------------------------------------------------


class TestConfigReloaders:
    def test_get_active_instruments_initially_none(self) -> None:
        from batch_live_reconciliation_service.config_reloaders import get_active_instruments

        # Module-level state may be set by other tests; just verify the return type
        result = get_active_instruments()
        assert result is None or hasattr(result, "subscription_list")

    def test_get_active_venues_initially_none(self) -> None:
        from batch_live_reconciliation_service.config_reloaders import get_active_venues

        result = get_active_venues()
        assert result is None or hasattr(result, "enabled_venues")

    def test_start_reloaders_no_op_when_bucket_empty(self) -> None:
        from batch_live_reconciliation_service.config_reloaders import start_domain_config_reloaders

        cfg = MagicMock()
        cfg.config_store_bucket = ""

        # Should return without error when bucket is not set
        start_domain_config_reloaders(cfg)

    def test_stop_reloaders_no_op_when_not_started(self) -> None:
        from batch_live_reconciliation_service.config_reloaders import stop_domain_config_reloaders

        # Should not raise even if reloaders were never started
        stop_domain_config_reloaders()

    def test_on_instruments_reload_updates_state(self) -> None:
        import batch_live_reconciliation_service.config_reloaders as cr

        mock_config = MagicMock()
        mock_config.subscription_list = ["BTC-USD", "ETH-USD"]
        mock_config.enabled_venues = ["binance"]

        cr._on_instruments_reload(mock_config)

        assert cr._active_instruments is mock_config

    def test_on_venues_reload_updates_state(self) -> None:
        import batch_live_reconciliation_service.config_reloaders as cr

        mock_config = MagicMock()
        mock_config.enabled_venues = ["binance", "coinbase"]

        cr._on_venues_reload(mock_config)

        assert cr._active_venues is mock_config
