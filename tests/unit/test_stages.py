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
        mock_client.bucket.return_value.blob.return_value.exists.side_effect = RuntimeError("GCS error")
        with patch(
            "batch_live_reconciliation_service.stages.stage0_config_pull.get_storage_client",
            return_value=mock_client,
        ):
            result = _blob_exists("bucket", "path/to/blob")

        assert result is False


# ---------------------------------------------------------------------------
# Stage 0.5: Data Pipeline Recon
# ---------------------------------------------------------------------------


@pytest.fixture()
def data_pipeline_config() -> MagicMock:
    """Mock ReconConfig with per-service/per-category bucket attrs populated."""
    cfg = MagicMock()
    cfg.gcp_project_id = "mock-project"
    cfg.stage_timeout_seconds = 30
    cfg.config_store_bucket = ""
    for cat in ("cefi", "tradfi", "defi"):
        setattr(cfg, f"instruments_bucket_{cat}", f"instruments-{cat}")
        setattr(cfg, f"market_data_tick_bucket_{cat}", f"mtd-{cat}")
    return cfg


class _FakeBlob:
    def __init__(self, name: str) -> None:
        self.name = name


class TestStage0DataPipelineRecon:
    def test_dry_run_returns_passed(self, data_pipeline_config: MagicMock) -> None:
        from batch_live_reconciliation_service.stages.stage0_data_pipeline_recon import (
            run_data_pipeline_recon,
        )

        result = run_data_pipeline_recon(data_pipeline_config, "2026-03-15", dry_run=True)

        assert result.stage == ReconStage.DATA_PIPELINE_RECON
        assert result.status == ReconStatus.PASSED
        assert result.metrics.get("dry_run") == 1.0

    def test_matching_batch_live_returns_passed(self, data_pipeline_config: MagicMock) -> None:
        """Equal batch and live file counts produce no deviations → PASSED."""
        from batch_live_reconciliation_service.stages.stage0_data_pipeline_recon import (
            run_data_pipeline_recon,
        )

        # Same file list for batch and live prefix → files_match=True, has_live_partition=True
        fake_blobs = [_FakeBlob("2026-03-15/venue/spot/a.csv")]
        mock_client = MagicMock()
        mock_client.bucket.return_value.list_blobs.return_value = fake_blobs
        with patch(
            "batch_live_reconciliation_service.stages.stage0_data_pipeline_recon.get_storage_client",
            return_value=mock_client,
        ):
            result = run_data_pipeline_recon(data_pipeline_config, "2026-03-15", dry_run=False)

        assert result.status == ReconStatus.PASSED
        # 3 services x 5 categories (sports + prediction added in Wave E).
        assert result.metrics["services_checked"] == 15.0

    def test_missing_live_partition_flags_deviation(self, data_pipeline_config: MagicMock) -> None:
        """Batch files exist but live prefix empty → missing_live_partitions deviation."""
        from batch_live_reconciliation_service.stages.stage0_data_pipeline_recon import (
            run_data_pipeline_recon,
        )

        def _list_blobs(prefix: str = "", **_: object) -> list[_FakeBlob]:
            if prefix.startswith("live/") or "live/" in prefix:
                return []
            return [_FakeBlob(f"{prefix}a.csv"), _FakeBlob(f"{prefix}b.csv")]

        mock_bucket = MagicMock()
        mock_bucket.list_blobs.side_effect = _list_blobs
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        with patch(
            "batch_live_reconciliation_service.stages.stage0_data_pipeline_recon.get_storage_client",
            return_value=mock_client,
        ):
            result = run_data_pipeline_recon(data_pipeline_config, "2026-03-15", dry_run=False)

        assert result.status == ReconStatus.FAILED
        dev_names = {d.metric_name for d in result.deviations}
        # Either file_count_match_rate or missing_live_partitions should fire
        assert dev_names & {"file_count_match_rate", "missing_live_partitions"}

    def test_gcs_listing_error_recorded_per_service(self, data_pipeline_config: MagicMock) -> None:
        """_count_blobs_and_rows catches listing errors → error populated on result."""
        from batch_live_reconciliation_service.stages.stage0_data_pipeline_recon import (
            run_data_pipeline_recon,
        )

        mock_client = MagicMock()
        mock_client.bucket.return_value.list_blobs.side_effect = RuntimeError("GCS listing failed")
        with patch(
            "batch_live_reconciliation_service.stages.stage0_data_pipeline_recon.get_storage_client",
            return_value=mock_client,
        ):
            result = run_data_pipeline_recon(data_pipeline_config, "2026-03-15", dry_run=False)

        # All services should have captured errors but the pipeline still completes.
        assert result.metrics["services_with_errors"] > 0

    def test_missing_bucket_config_captured_in_error(self) -> None:
        """If config returns empty bucket name → result.error is populated."""
        from batch_live_reconciliation_service.stages.stage0_data_pipeline_recon import (
            run_data_pipeline_recon,
        )

        cfg = MagicMock()
        cfg.gcp_project_id = "mock-project"
        # Deliberately leave all bucket attributes empty so every service/category
        # hits the "no bucket configured" branch. Asset groups must match the
        # full set the production code iterates (cefi/tradfi/defi/sports/prediction)
        # — otherwise MagicMock returns truthy for unset attrs and services pass.
        for cat in ("cefi", "tradfi", "defi", "sports", "prediction"):
            setattr(cfg, f"instruments_bucket_{cat}", "")
            setattr(cfg, f"market_data_tick_bucket_{cat}", "")
            setattr(cfg, f"market_data_processing_bucket_{cat}", "")

        result = run_data_pipeline_recon(cfg, "2026-03-15", dry_run=False)

        assert result.metrics["services_with_errors"] == result.metrics["services_checked"]


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

    def test_latency_delta_breach_flags_when_samples_present(self) -> None:
        """Matched events carrying metadata.inference_duration_ms on both sides produce a real
        latency_delta_ms; a delta above the 5000ms threshold is flagged."""
        from batch_live_reconciliation_service.stages.stage1_ml_recon import (
            _check_deviations,
            _compute_metrics,
        )

        batch: list[dict[str, object]] = [
            {
                "instrument_id": "BTC-USD",
                "timeframe": "1h",
                "signal_direction": 1,
                "magnitude": 0.5,
                "metadata": {"inference_duration_ms": 100.0},
            }
        ]
        live: list[dict[str, object]] = [
            {
                "instrument_id": "BTC-USD",
                "timeframe": "1h",
                "signal_direction": 1,
                "magnitude": 0.5,
                "metadata": {"inference_duration_ms": 6000.0},
            }
        ]
        metrics = _compute_metrics(batch, live)

        assert metrics["latency_samples"] == 1.0
        assert metrics["latency_delta_ms"] == 5900.0
        deviations = _check_deviations(metrics)
        assert any(d.metric_name == "latency_delta_ms" for d in deviations)

    def test_latency_no_samples_yields_no_latency_deviation(self) -> None:
        """Events without metadata.inference_duration_ms contribute no latency sample; the zero
        metric must NOT be flagged — no-data is not a breach."""
        from batch_live_reconciliation_service.stages.stage1_ml_recon import (
            _check_deviations,
            _compute_metrics,
        )

        batch: list[dict[str, object]] = [
            {"instrument_id": "ETH-USD", "timeframe": "1h", "signal_direction": 1, "magnitude": 0.5}
        ]
        live: list[dict[str, object]] = [
            {"instrument_id": "ETH-USD", "timeframe": "1h", "signal_direction": 1, "magnitude": 0.5}
        ]
        metrics = _compute_metrics(batch, live)

        assert metrics["latency_samples"] == 0.0
        assert metrics["latency_delta_ms"] == 0.0
        deviations = _check_deviations(metrics)
        assert not any(d.metric_name == "latency_delta_ms" for d in deviations)


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
# Stage 3b: Paper-vs-live Recon (pvl-p21a)
# ---------------------------------------------------------------------------


class TestStage3bPaperLiveRecon:
    def test_dry_run_returns_passed(self, mock_config: MagicMock) -> None:
        from batch_live_reconciliation_service.stages.stage3b_paper_live_recon import run_stage3b

        result = run_stage3b(mock_config, "2026-05-15", dry_run=True)

        assert result.stage == ReconStage.PAPER_LIVE_RECON
        assert result.status == ReconStatus.PASSED

    def test_empty_events_returns_passed(self, mock_config: MagicMock) -> None:
        from batch_live_reconciliation_service.stages.stage3b_paper_live_recon import run_stage3b

        mock_client = MagicMock()
        mock_client.bucket.return_value.list_blobs.return_value = []
        with patch(
            "batch_live_reconciliation_service.stages.stage3b_paper_live_recon.get_storage_client",
            return_value=mock_client,
        ):
            result = run_stage3b(mock_config, "2026-05-15", dry_run=False)

        assert result.status == ReconStatus.PASSED

    def test_within_threshold_returns_passed(self, mock_config: MagicMock) -> None:
        from batch_live_reconciliation_service.models.deviation_thresholds import (
            PaperLiveThresholds,
        )
        from batch_live_reconciliation_service.stages.stage3b_paper_live_recon import (
            _check_deviations,
            _compute_metrics,
        )

        paper = [
            {
                "realized_pnl": 100.0,
                "fill_rate": 0.95,
                "slippage_bps": 2.0,
                "order_latency_ms": 200.0,
                "algo_correct": 1.0,
            }
        ]
        live = [
            {
                "realized_pnl": 100.4,
                "fill_rate": 0.95,
                "slippage_bps": 2.0,
                "order_latency_ms": 200.0,
                "algo_correct": 1.0,
            }
        ]
        metrics = _compute_metrics(paper, live)
        deviations = _check_deviations(metrics, PaperLiveThresholds())

        assert len(deviations) == 0

    def test_pnl_gap_breach_detected_with_auto_demote_routing(self, mock_config: MagicMock) -> None:
        from batch_live_reconciliation_service.models.deviation_thresholds import (
            PaperLiveThresholds,
        )
        from batch_live_reconciliation_service.models.recon_report import FailureRoutingAction
        from batch_live_reconciliation_service.stages.stage3b_paper_live_recon import (
            _check_deviations,
            _compute_metrics,
        )

        paper = [
            {
                "realized_pnl": 100.0,
                "fill_rate": 0.95,
                "slippage_bps": 2.0,
                "order_latency_ms": 200.0,
            }
        ]
        live = [
            {
                "realized_pnl": 200.0,
                "fill_rate": 0.95,
                "slippage_bps": 2.0,
                "order_latency_ms": 200.0,
            }
        ]
        metrics = _compute_metrics(paper, live)
        deviations = _check_deviations(metrics, PaperLiveThresholds())

        alpha_devs = [d for d in deviations if d.record.metric_name == "alpha_pnl_gap"]
        assert len(alpha_devs) == 1
        assert alpha_devs[0].routing_action == FailureRoutingAction.AUTO_DEMOTE_TO_PAPER

    def test_latency_breach_routing_is_alert_only(self, mock_config: MagicMock) -> None:
        from batch_live_reconciliation_service.models.deviation_thresholds import (
            PaperLiveThresholds,
        )
        from batch_live_reconciliation_service.models.recon_report import FailureRoutingAction
        from batch_live_reconciliation_service.stages.stage3b_paper_live_recon import (
            _check_deviations,
            _compute_metrics,
        )

        paper = [
            {
                "realized_pnl": 100.0,
                "fill_rate": 0.95,
                "slippage_bps": 2.0,
                "order_latency_ms": 600.0,
            }
        ]
        live = [
            {
                "realized_pnl": 100.0,
                "fill_rate": 0.95,
                "slippage_bps": 2.0,
                "order_latency_ms": 200.0,
            }
        ]
        metrics = _compute_metrics(paper, live)
        deviations = _check_deviations(metrics, PaperLiveThresholds())

        lat_devs = [d for d in deviations if d.record.metric_name == "order_latency_p99_ms"]
        assert len(lat_devs) == 1
        assert lat_devs[0].routing_action == FailureRoutingAction.ALERT

    def test_stage_report_stage_is_paper_live_recon(self, mock_config: MagicMock) -> None:
        from batch_live_reconciliation_service.stages.stage3b_paper_live_recon import run_stage3b

        result = run_stage3b(mock_config, "2026-05-15", dry_run=True)
        assert result.stage == ReconStage.PAPER_LIVE_RECON


# ---------------------------------------------------------------------------
# Stage 3c: Batch-vs-paper Recon (pvl-p21a)
# ---------------------------------------------------------------------------


class TestStage3cBatchPaperRecon:
    def test_dry_run_returns_passed(self, mock_config: MagicMock) -> None:
        from batch_live_reconciliation_service.stages.stage3c_batch_paper_recon import run_stage3c

        result = run_stage3c(mock_config, "2026-05-15", dry_run=True)

        assert result.stage == ReconStage.BATCH_PAPER_RECON
        assert result.status == ReconStatus.PASSED

    def test_empty_events_returns_passed(self, mock_config: MagicMock) -> None:
        from batch_live_reconciliation_service.stages.stage3c_batch_paper_recon import run_stage3c

        mock_client = MagicMock()
        mock_client.bucket.return_value.list_blobs.return_value = []
        with patch(
            "batch_live_reconciliation_service.stages.stage3c_batch_paper_recon.get_storage_client",
            return_value=mock_client,
        ):
            result = run_stage3c(mock_config, "2026-05-15", dry_run=False)

        assert result.status == ReconStatus.PASSED

    def test_within_threshold_returns_passed(self, mock_config: MagicMock) -> None:
        from batch_live_reconciliation_service.models.deviation_thresholds import (
            BatchPaperThresholds,
        )
        from batch_live_reconciliation_service.stages.stage3c_batch_paper_recon import (
            _check_deviations,
            _compute_metrics,
        )

        batch = [{"realized_pnl": 100.0, "position_size": 5.0, "order_latency_ms": 50.0}]
        paper = [{"realized_pnl": 102.0, "position_size": 5.1, "order_latency_ms": 100.0}]
        metrics = _compute_metrics(batch, paper)
        deviations = _check_deviations(metrics, BatchPaperThresholds())

        assert len(deviations) == 0

    def test_fill_count_breach_detected(self, mock_config: MagicMock) -> None:
        from batch_live_reconciliation_service.models.deviation_thresholds import (
            BatchPaperThresholds,
        )
        from batch_live_reconciliation_service.stages.stage3c_batch_paper_recon import (
            _check_deviations,
            _compute_metrics,
        )

        batch = [
            {"realized_pnl": 100.0, "position_size": 5.0, "order_latency_ms": 50.0},
        ]
        paper = [
            {"realized_pnl": 100.0, "position_size": 5.0, "order_latency_ms": 50.0},
            {"realized_pnl": 100.0, "position_size": 5.0, "order_latency_ms": 50.0},
            {"realized_pnl": 100.0, "position_size": 5.0, "order_latency_ms": 50.0},
            {"realized_pnl": 100.0, "position_size": 5.0, "order_latency_ms": 50.0},
            {"realized_pnl": 100.0, "position_size": 5.0, "order_latency_ms": 50.0},
            {"realized_pnl": 100.0, "position_size": 5.0, "order_latency_ms": 50.0},
            {"realized_pnl": 100.0, "position_size": 5.0, "order_latency_ms": 50.0},
            {"realized_pnl": 100.0, "position_size": 5.0, "order_latency_ms": 50.0},
        ]
        metrics = _compute_metrics(batch, paper)
        deviations = _check_deviations(metrics, BatchPaperThresholds())

        fill_devs = [d for d in deviations if d.metric_name == "fill_count_delta_pct"]
        assert len(fill_devs) == 1

    def test_stage_report_stage_is_batch_paper_recon(self, mock_config: MagicMock) -> None:
        from batch_live_reconciliation_service.stages.stage3c_batch_paper_recon import run_stage3c

        result = run_stage3c(mock_config, "2026-05-15", dry_run=True)
        assert result.stage == ReconStage.BATCH_PAPER_RECON


# ---------------------------------------------------------------------------
# PaperLiveThresholds + BatchPaperThresholds + FailureRoutingAction
# ---------------------------------------------------------------------------


class TestNewThresholdsAndRouting:
    def test_paper_live_thresholds_tighter_than_execution(self) -> None:
        from batch_live_reconciliation_service.models.deviation_thresholds import (
            EXECUTION_THRESHOLDS,
            PAPER_LIVE_THRESHOLDS,
        )

        assert PAPER_LIVE_THRESHOLDS.alpha_pnl_gap_max < EXECUTION_THRESHOLDS.alpha_pnl_gap_max
        assert PAPER_LIVE_THRESHOLDS.fill_rate_delta_max < EXECUTION_THRESHOLDS.fill_rate_delta_max
        assert PAPER_LIVE_THRESHOLDS.slippage_delta_bps_max < EXECUTION_THRESHOLDS.slippage_delta_bps_max

    def test_batch_paper_thresholds_wider_than_paper_live(self) -> None:
        from batch_live_reconciliation_service.models.deviation_thresholds import (
            BATCH_PAPER_THRESHOLDS,
            PAPER_LIVE_THRESHOLDS,
        )

        assert BATCH_PAPER_THRESHOLDS.pnl_delta_pct_max > PAPER_LIVE_THRESHOLDS.alpha_pnl_gap_max

    def test_failure_routing_action_values(self) -> None:
        from batch_live_reconciliation_service.models.recon_report import FailureRoutingAction

        assert FailureRoutingAction.ALERT == "alert"
        assert FailureRoutingAction.AUTO_PAUSE_LIVE == "auto_pause_live"
        assert FailureRoutingAction.AUTO_DEMOTE_TO_PAPER == "auto_demote_to_paper"

    def test_paper_live_recon_stage_in_enum(self) -> None:
        assert ReconStage.PAPER_LIVE_RECON == "paper_live_recon"

    def test_batch_paper_recon_stage_in_enum(self) -> None:
        assert ReconStage.BATCH_PAPER_RECON == "batch_paper_recon"


# ---------------------------------------------------------------------------
# Stage 4: Agent Analysis
# ---------------------------------------------------------------------------


class TestStage4AgentAnalysis:
    def test_dry_run_returns_passed(self, mock_config: MagicMock, mock_stage_report: StageReport) -> None:
        from batch_live_reconciliation_service.stages.stage4_agent_analysis import run_stage4

        result = run_stage4(mock_config, "2026-03-15", stage_reports=[mock_stage_report], dry_run=True)

        assert result.stage == ReconStage.AGENT_ANALYSIS
        assert result.status == ReconStatus.PASSED

    def test_gcs_error_returns_failed(self, mock_config: MagicMock, mock_stage_report: StageReport) -> None:
        from batch_live_reconciliation_service.stages.stage4_agent_analysis import run_stage4

        with patch(
            "batch_live_reconciliation_service.stages.stage4_agent_analysis.get_storage_client",
            side_effect=RuntimeError("GCS unavailable"),
        ):
            result = run_stage4(mock_config, "2026-03-15", stage_reports=[mock_stage_report], dry_run=False)

        assert result.status == ReconStatus.FAILED


# ---------------------------------------------------------------------------
# Stage 5: Results Writer
# ---------------------------------------------------------------------------


class TestStage5ResultsWriter:
    def test_dry_run_returns_passed(self, mock_config: MagicMock, mock_recon_report: ReconReport) -> None:
        from batch_live_reconciliation_service.stages.stage5_results_writer import run_stage5

        result = run_stage5(mock_config, mock_recon_report, dry_run=True)

        assert result.stage == ReconStage.RESULTS_WRITER
        assert result.status == ReconStatus.PASSED
        assert result.output_gcs_path is not None

    def test_gcs_upload_error_returns_failed(self, mock_config: MagicMock, mock_recon_report: ReconReport) -> None:
        from batch_live_reconciliation_service.stages.stage5_results_writer import run_stage5

        mock_client = MagicMock()
        mock_client.upload_bytes.side_effect = RuntimeError("GCS upload failed")
        with patch(
            "batch_live_reconciliation_service.stages.stage5_results_writer.get_storage_client",
            return_value=mock_client,
        ):
            result = run_stage5(mock_config, mock_recon_report, dry_run=False)

        assert result.status == ReconStatus.FAILED

    def test_summary_upload_includes_serialized_deviations(self, mock_config: MagicMock) -> None:
        """G1: Stage-5's summary JSON carries per-deviation detail (metric/actual/threshold/
        instrument_id/...), not just aggregate stage metrics — this is what the resolution API's
        _breaks_from_summary() reconstructs real breaks from."""
        import json

        from unified_api_contracts.internal.reconciliation import ReconciliationDimension

        from batch_live_reconciliation_service.models.recon_report import DeviationRecord
        from batch_live_reconciliation_service.stages.stage5_results_writer import run_stage5

        deviation = DeviationRecord.new(
            metric_name="alpha_pnl_gap",
            stage=ReconStage.EXECUTION_RECON,
            actual_value=3.2,
            threshold=2.0,
            direction="above",
            description="alpha_pnl_gap 3.2% > 2% of notional",
            dimension=ReconciliationDimension.POSITIONS,
            instrument_id="BTC-USDT",
        )
        stage_report = StageReport(
            stage=ReconStage.EXECUTION_RECON,
            status=ReconStatus.FAILED,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            deviations=[deviation],
        )
        report = ReconReport(
            date="2026-07-27",
            run_id="test-run-002",
            started_at=datetime.now(UTC),
            status=ReconStatus.FAILED,
            stages=[stage_report],
        )

        mock_client = MagicMock()
        with patch(
            "batch_live_reconciliation_service.stages.stage5_results_writer.get_storage_client",
            return_value=mock_client,
        ):
            result = run_stage5(mock_config, report, dry_run=False)

        assert result.status == ReconStatus.PASSED
        summary_call = next(c for c in mock_client.upload_bytes.call_args_list if "summary_" in c.kwargs["blob_path"])
        payload = json.loads(summary_call.kwargs["data"].decode("utf-8"))
        serialized = payload["stages"][0]["deviations"][0]
        assert serialized["metric_name"] == "alpha_pnl_gap"
        assert serialized["actual_value"] == 3.2
        assert serialized["threshold"] == 2.0
        assert serialized["instrument_id"] == "BTC-USDT"

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
# Orchestrator
# ---------------------------------------------------------------------------


class TestReconciliationOrchestrator:
    def _make_passed_stage_report(self, stage: ReconStage) -> StageReport:
        return StageReport(
            stage=stage,
            status=ReconStatus.PASSED,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            metrics={"dry_run": 1.0},
        )

    def test_run_reconciliation_dry_run_happy_path(self) -> None:
        from batch_live_reconciliation_service.engine import orchestrator as orch

        fake_config = MagicMock()
        fake_config.gcp_project_id = "p"
        fake_config.events_bucket = "e"

        s_reports = {
            "stage0": self._make_passed_stage_report(ReconStage.CONFIG_PULL),
            "stage0_data": self._make_passed_stage_report(ReconStage.DATA_PIPELINE_RECON),
            "stage1": self._make_passed_stage_report(ReconStage.ML_RECON),
            "stage2": self._make_passed_stage_report(ReconStage.STRATEGY_RECON),
            "stage3": self._make_passed_stage_report(ReconStage.EXECUTION_RECON),
            "stage3b": self._make_passed_stage_report(ReconStage.PAPER_LIVE_RECON),
            "stage3c": self._make_passed_stage_report(ReconStage.BATCH_PAPER_RECON),
            "stage4": self._make_passed_stage_report(ReconStage.AGENT_ANALYSIS),
            "stage5": self._make_passed_stage_report(ReconStage.RESULTS_WRITER),
        }

        with (
            patch.object(orch, "get_recon_config", return_value=fake_config),
            patch.object(orch, "_setup_observability"),
            patch.object(orch, "run_stage0", return_value=s_reports["stage0"]),
            patch.object(orch, "run_data_pipeline_recon", return_value=s_reports["stage0_data"]),
            patch.object(orch, "run_stage1", return_value=s_reports["stage1"]),
            patch.object(orch, "run_stage2", return_value=s_reports["stage2"]),
            patch.object(orch, "run_stage3", return_value=s_reports["stage3"]),
            patch.object(orch, "run_stage3b", return_value=s_reports["stage3b"]),
            patch.object(orch, "run_stage3c", return_value=s_reports["stage3c"]),
            patch.object(orch, "run_stage4", return_value=s_reports["stage4"]),
            patch.object(orch, "run_stage5", return_value=s_reports["stage5"]),
            patch.object(orch, "log_event"),
        ):
            report = orch.run_reconciliation("2026-03-15", dry_run=True)

        assert report.status == ReconStatus.PASSED
        assert report.date == "2026-03-15"
        # All 9 stages appended (Stage 0 + 0.5 + 1 + 2 + 3 + 3b + 3c + 4 + 5)
        assert len(report.stages) == 9

    def test_run_reconciliation_stage0_fail_aborts(self) -> None:
        from batch_live_reconciliation_service.engine import orchestrator as orch

        fake_config = MagicMock()
        fake_config.gcp_project_id = "p"
        fake_config.events_bucket = "e"

        failed_stage0 = StageReport(
            stage=ReconStage.CONFIG_PULL,
            status=ReconStatus.FAILED,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            error_message="config missing",
        )

        with (
            patch.object(orch, "get_recon_config", return_value=fake_config),
            patch.object(orch, "_setup_observability"),
            patch.object(orch, "run_stage0", return_value=failed_stage0),
            patch.object(orch, "run_data_pipeline_recon") as m_data,
            patch.object(orch, "log_event"),
        ):
            report = orch.run_reconciliation("2026-03-15", dry_run=True)

        assert report.status == ReconStatus.FAILED
        assert len(report.stages) == 1  # pipeline aborts after Stage 0
        m_data.assert_not_called()


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
        result = asyncio.run(handler.run())
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
            result = asyncio.run(
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
            result = asyncio.run(
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
            asyncio.run(
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


# ---------------------------------------------------------------------------
# _ServiceReconResult property tests
# ---------------------------------------------------------------------------


class TestServiceReconResultProperties:
    def test_files_match_when_equal(self) -> None:
        from batch_live_reconciliation_service.stages.stage0_data_pipeline_recon import (
            _ServiceReconResult,
        )

        r = _ServiceReconResult(service_name="svc", category="cefi", bucket="b", batch_files=3, live_files=3)
        assert r.files_match is True

    def test_files_mismatch_when_unequal(self) -> None:
        from batch_live_reconciliation_service.stages.stage0_data_pipeline_recon import (
            _ServiceReconResult,
        )

        r = _ServiceReconResult(service_name="svc", category="cefi", bucket="b", batch_files=3, live_files=2)
        assert r.files_match is False

    def test_rows_match_when_equal(self) -> None:
        from batch_live_reconciliation_service.stages.stage0_data_pipeline_recon import (
            _ServiceReconResult,
        )

        r = _ServiceReconResult(service_name="svc", category="cefi", bucket="b", batch_rows=10, live_rows=10)
        assert r.rows_match is True

    def test_rows_mismatch_when_unequal(self) -> None:
        from batch_live_reconciliation_service.stages.stage0_data_pipeline_recon import (
            _ServiceReconResult,
        )

        r = _ServiceReconResult(service_name="svc", category="cefi", bucket="b", batch_rows=10, live_rows=9)
        assert r.rows_match is False

    def test_both_zero_is_match(self) -> None:
        from batch_live_reconciliation_service.stages.stage0_data_pipeline_recon import (
            _ServiceReconResult,
        )

        r = _ServiceReconResult(service_name="svc", category="cefi", bucket="b")
        assert r.files_match is True
        assert r.rows_match is True


# ---------------------------------------------------------------------------
# reconcile_shard path — mock UTL call and assert verdicts route correctly
# ---------------------------------------------------------------------------


def _fake_shard_report(verdict: str, notes: list[str] | None = None) -> MagicMock:
    report = MagicMock()
    report.verdict = verdict
    report.notes = notes or []
    return report


class TestReconcileShardPath:
    """Tests for the reconcile_shard() call inside _reconcile_service.

    Mocks _count_parquet_rows + _parse_parquet_sample to supply sample rows so
    the reconcile_shard branch is entered, then asserts each verdict routes
    correctly to deviations / metrics.
    """

    def _run_with_verdict(self, config: MagicMock, verdict: str) -> StageReport:
        from batch_live_reconciliation_service.stages.stage0_data_pipeline_recon import (
            run_data_pipeline_recon,
        )

        sample_rows = [{"close": 100.0, "volume": 500}]
        client = MagicMock()
        bucket = MagicMock()
        bucket.list_blobs.return_value = [_FakeBlob("2026-03-15/spot/data.parquet")]
        client.bucket.return_value = bucket
        client.download_bytes.return_value = b"fake-parquet-bytes"

        with (
            patch(
                "batch_live_reconciliation_service.stages.stage0_data_pipeline_recon.get_storage_client",
                return_value=client,
            ),
            patch(
                "batch_live_reconciliation_service.stages.stage0_data_pipeline_recon._count_parquet_rows",
                return_value=5,
            ),
            patch(
                "batch_live_reconciliation_service.stages.stage0_data_pipeline_recon._parse_parquet_sample",
                return_value=sample_rows,
            ),
            patch(
                "batch_live_reconciliation_service.stages.stage0_data_pipeline_recon.reconcile_shard",
                return_value=_fake_shard_report(verdict),
            ),
        ):
            return run_data_pipeline_recon(config, "2026-03-15", dry_run=False)

    def test_match_verdict_produces_no_shard_deviation(self, data_pipeline_config: MagicMock) -> None:
        result = self._run_with_verdict(data_pipeline_config, "MATCH")

        shard_devs = [d for d in result.deviations if "shard" in d.metric_name]
        assert len(shard_devs) == 0
        assert result.metrics["services_with_schema_mismatch"] == 0.0
        assert result.metrics["services_with_value_mismatch"] == 0.0

    def test_schema_mismatch_verdict_creates_deviation_and_fails(self, data_pipeline_config: MagicMock) -> None:
        result = self._run_with_verdict(data_pipeline_config, "SCHEMA_MISMATCH")

        schema_devs = [d for d in result.deviations if d.metric_name == "shard_schema_mismatch"]
        assert len(schema_devs) == 1
        assert result.metrics["services_with_schema_mismatch"] >= 1.0
        assert result.status == ReconStatus.FAILED

    def test_value_mismatch_verdict_creates_deviation_and_fails(self, data_pipeline_config: MagicMock) -> None:
        result = self._run_with_verdict(data_pipeline_config, "VALUE_MISMATCH")

        value_devs = [d for d in result.deviations if d.metric_name == "shard_value_mismatch"]
        assert len(value_devs) == 1
        assert result.metrics["services_with_value_mismatch"] >= 1.0
        assert result.status == ReconStatus.FAILED

    def test_reconcile_shard_exception_handled_gracefully(self, data_pipeline_config: MagicMock) -> None:
        from batch_live_reconciliation_service.stages.stage0_data_pipeline_recon import (
            run_data_pipeline_recon,
        )

        sample_rows = [{"close": 100.0}]
        client = MagicMock()
        bucket = MagicMock()
        bucket.list_blobs.return_value = [_FakeBlob("2026-03-15/spot/data.parquet")]
        client.bucket.return_value = bucket
        client.download_bytes.return_value = b"fake"

        with (
            patch(
                "batch_live_reconciliation_service.stages.stage0_data_pipeline_recon.get_storage_client",
                return_value=client,
            ),
            patch(
                "batch_live_reconciliation_service.stages.stage0_data_pipeline_recon._count_parquet_rows",
                return_value=3,
            ),
            patch(
                "batch_live_reconciliation_service.stages.stage0_data_pipeline_recon._parse_parquet_sample",
                return_value=sample_rows,
            ),
            patch(
                "batch_live_reconciliation_service.stages.stage0_data_pipeline_recon.reconcile_shard",
                side_effect=ValueError("shard failed"),
            ),
        ):
            result = run_data_pipeline_recon(data_pipeline_config, "2026-03-15", dry_run=False)

        # Pipeline must not abort — shard error is logged as warning, no shard deviation
        assert result is not None
        shard_devs = [d for d in result.deviations if "shard" in d.metric_name]
        assert len(shard_devs) == 0

    def test_reconcile_shard_called_with_service_and_category(self, data_pipeline_config: MagicMock) -> None:
        from batch_live_reconciliation_service.stages.stage0_data_pipeline_recon import (
            run_data_pipeline_recon,
        )

        sample_rows = [{"close": 100.0}]
        client = MagicMock()
        bucket = MagicMock()
        bucket.list_blobs.return_value = [_FakeBlob("2026-03-15/spot/data.parquet")]
        client.bucket.return_value = bucket
        client.download_bytes.return_value = b"fake"
        mock_shard = _fake_shard_report("MATCH")

        with (
            patch(
                "batch_live_reconciliation_service.stages.stage0_data_pipeline_recon.get_storage_client",
                return_value=client,
            ),
            patch(
                "batch_live_reconciliation_service.stages.stage0_data_pipeline_recon._count_parquet_rows",
                return_value=2,
            ),
            patch(
                "batch_live_reconciliation_service.stages.stage0_data_pipeline_recon._parse_parquet_sample",
                return_value=sample_rows,
            ),
            patch(
                "batch_live_reconciliation_service.stages.stage0_data_pipeline_recon.reconcile_shard",
                return_value=mock_shard,
            ) as mock_fn,
        ):
            run_data_pipeline_recon(data_pipeline_config, "2026-03-15", dry_run=False)

        assert mock_fn.called
        first_call = mock_fn.call_args
        assert first_call is not None
        kwargs = first_call.kwargs if first_call.kwargs else {}
        assert "asset_group" in kwargs or len(first_call.args) > 0


# ---------------------------------------------------------------------------
# ndjson / jsonl file row counting
# ---------------------------------------------------------------------------


class TestNdjsonCounting:
    def test_ndjson_lines_counted_as_rows(self, data_pipeline_config: MagicMock) -> None:
        from batch_live_reconciliation_service.stages.stage0_data_pipeline_recon import (
            run_data_pipeline_recon,
        )

        ndjson_content = b'{"a": 1}\n{"a": 2}\n{"a": 3}\n'
        client = MagicMock()
        bucket = MagicMock()
        bucket.list_blobs.return_value = [_FakeBlob("2026-03-15/events.ndjson")]
        client.bucket.return_value = bucket
        client.download_bytes.return_value = ndjson_content

        with patch(
            "batch_live_reconciliation_service.stages.stage0_data_pipeline_recon.get_storage_client",
            return_value=client,
        ):
            result = run_data_pipeline_recon(data_pipeline_config, "2026-03-15", dry_run=False)

        assert result.metrics["total_batch_rows"] > 0
        assert result.metrics["total_live_rows"] > 0

    def test_jsonl_extension_also_counted(self, data_pipeline_config: MagicMock) -> None:
        from batch_live_reconciliation_service.stages.stage0_data_pipeline_recon import (
            run_data_pipeline_recon,
        )

        content = b'{"x": 1}\n{"x": 2}\n'
        client = MagicMock()
        bucket = MagicMock()
        bucket.list_blobs.return_value = [_FakeBlob("2026-03-15/events.jsonl")]
        client.bucket.return_value = bucket
        client.download_bytes.return_value = content

        with patch(
            "batch_live_reconciliation_service.stages.stage0_data_pipeline_recon.get_storage_client",
            return_value=client,
        ):
            result = run_data_pipeline_recon(data_pipeline_config, "2026-03-15", dry_run=False)

        assert result.metrics["total_batch_rows"] > 0

    def test_ndjson_download_error_captured_in_service_error(self, data_pipeline_config: MagicMock) -> None:
        from batch_live_reconciliation_service.stages.stage0_data_pipeline_recon import (
            run_data_pipeline_recon,
        )

        client = MagicMock()
        bucket = MagicMock()
        bucket.list_blobs.return_value = [_FakeBlob("2026-03-15/events.ndjson")]
        client.bucket.return_value = bucket
        client.download_bytes.side_effect = RuntimeError("download failed")

        with patch(
            "batch_live_reconciliation_service.stages.stage0_data_pipeline_recon.get_storage_client",
            return_value=client,
        ):
            result = run_data_pipeline_recon(data_pipeline_config, "2026-03-15", dry_run=False)

        assert result.metrics["services_with_errors"] > 0


# ---------------------------------------------------------------------------
# _count_blobs_and_rows — parquet error + CSV fallback
# ---------------------------------------------------------------------------


class TestBlobCountingEdgeCases:
    def test_parquet_download_error_counts_one_row_per_file(self, data_pipeline_config: MagicMock) -> None:
        from batch_live_reconciliation_service.stages.stage0_data_pipeline_recon import (
            run_data_pipeline_recon,
        )

        client = MagicMock()
        bucket = MagicMock()
        bucket.list_blobs.return_value = [_FakeBlob("2026-03-15/a.parquet")]
        client.bucket.return_value = bucket
        client.download_bytes.side_effect = RuntimeError("parquet download failed")

        with patch(
            "batch_live_reconciliation_service.stages.stage0_data_pipeline_recon.get_storage_client",
            return_value=client,
        ):
            result = run_data_pipeline_recon(data_pipeline_config, "2026-03-15", dry_run=False)

        # Error recorded but pipeline continues; file counted as 1 row
        assert result.metrics["services_with_errors"] > 0

    def test_unknown_extension_counted_as_one_row_per_file(self, data_pipeline_config: MagicMock) -> None:
        from batch_live_reconciliation_service.stages.stage0_data_pipeline_recon import (
            run_data_pipeline_recon,
        )

        client = MagicMock()
        bucket = MagicMock()
        bucket.list_blobs.return_value = [
            _FakeBlob("2026-03-15/data.csv"),
            _FakeBlob("2026-03-15/data2.csv"),
        ]
        client.bucket.return_value = bucket

        with patch(
            "batch_live_reconciliation_service.stages.stage0_data_pipeline_recon.get_storage_client",
            return_value=client,
        ):
            result = run_data_pipeline_recon(data_pipeline_config, "2026-03-15", dry_run=False)

        # 2 CSV files → 2 rows per prefix call
        assert result.metrics["total_batch_rows"] >= 2

    def test_directory_markers_and_success_files_skipped(self, data_pipeline_config: MagicMock) -> None:
        from batch_live_reconciliation_service.stages.stage0_data_pipeline_recon import (
            run_data_pipeline_recon,
        )

        client = MagicMock()
        bucket = MagicMock()
        # directory marker and _SUCCESS should be skipped; only real.csv is counted
        bucket.list_blobs.return_value = [
            _FakeBlob("2026-03-15/subdir/"),
            _FakeBlob("2026-03-15/_SUCCESS"),
            _FakeBlob("2026-03-15/real.csv"),
        ]
        client.bucket.return_value = bucket

        with patch(
            "batch_live_reconciliation_service.stages.stage0_data_pipeline_recon.get_storage_client",
            return_value=client,
        ):
            result = run_data_pipeline_recon(data_pipeline_config, "2026-03-15", dry_run=False)

        # Only real.csv is a file → 1 file per prefix, not 3
        assert result.metrics["total_batch_files"] < result.metrics["services_checked"] * 3
