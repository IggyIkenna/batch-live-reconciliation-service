"""
Unit tests for Stage 4 Agent Analysis.

Tests _build_agent_prompt and _write_agent_report with mocked GCS,
and run_stage4 with dry_run=True to avoid I/O.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from batch_live_reconciliation_service.models.recon_report import (
    DeviationRecord,
    ReconStage,
    ReconStatus,
    StageReport,
)
from batch_live_reconciliation_service.stages.stage4_agent_analysis import (
    _build_agent_prompt,
    _write_agent_report,
    run_stage4,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_stage_report(
    stage: ReconStage,
    status: ReconStatus = ReconStatus.PASSED,
    deviations: list[DeviationRecord] | None = None,
    metrics: dict[str, float] | None = None,
) -> StageReport:
    return StageReport(
        stage=stage,
        status=status,
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        deviations=deviations or [],
        metrics=metrics or {},
    )


def _make_deviation(metric_name: str = "signal_direction_match_rate") -> DeviationRecord:
    return DeviationRecord(
        metric_name=metric_name,
        stage=ReconStage.ML_RECON,
        actual_value=0.80,
        threshold=0.95,
        direction="below",
        description=f"{metric_name} below threshold",
    )


def _make_config() -> MagicMock:
    cfg = MagicMock()
    cfg.recon_bucket = "test-recon-bucket"
    cfg.events_bucket = "test-events-bucket"
    cfg.gcp_project_id = "test-project"
    return cfg


# ---------------------------------------------------------------------------
# _build_agent_prompt
# ---------------------------------------------------------------------------


def test_build_agent_prompt_includes_date() -> None:
    reports = [_make_stage_report(ReconStage.ML_RECON)]
    prompt = _build_agent_prompt("2026-03-11", reports)
    assert "2026-03-11" in prompt


def test_build_agent_prompt_includes_stage_names() -> None:
    reports = [
        _make_stage_report(ReconStage.ML_RECON),
        _make_stage_report(ReconStage.STRATEGY_RECON),
    ]
    prompt = _build_agent_prompt("2026-03-11", reports)
    assert "ml_recon" in prompt
    assert "strategy_recon" in prompt


def test_build_agent_prompt_includes_deviation_info() -> None:
    dev = _make_deviation("signal_direction_match_rate")
    report = _make_stage_report(ReconStage.ML_RECON, ReconStatus.FAILED, deviations=[dev])
    prompt = _build_agent_prompt("2026-03-11", [report])
    assert "signal_direction_match_rate" in prompt


def test_build_agent_prompt_zero_deviations() -> None:
    reports = [_make_stage_report(ReconStage.ML_RECON)]
    prompt = _build_agent_prompt("2026-03-11", reports)
    assert "Total deviations detected: 0" in prompt


def test_build_agent_prompt_counts_total_deviations() -> None:
    dev1 = _make_deviation("metric_a")
    dev2 = _make_deviation("metric_b")
    report = _make_stage_report(ReconStage.ML_RECON, deviations=[dev1, dev2])
    prompt = _build_agent_prompt("2026-03-11", [report])
    assert "Total deviations detected: 2" in prompt


def test_build_agent_prompt_includes_instructions() -> None:
    reports = [_make_stage_report(ReconStage.ML_RECON)]
    prompt = _build_agent_prompt("2026-03-11", reports)
    # Must ask for root cause and suggestions
    assert "root cause" in prompt.lower() or "root causes" in prompt.lower()


def test_build_agent_prompt_includes_metrics() -> None:
    report = _make_stage_report(ReconStage.ML_RECON, metrics={"coverage": 0.95})
    prompt = _build_agent_prompt("2026-03-11", [report])
    assert "coverage" in prompt
    assert "0.9500" in prompt


# ---------------------------------------------------------------------------
# _write_agent_report
# ---------------------------------------------------------------------------


def test_write_agent_report_dry_run_returns_gcs_uri() -> None:
    uri = _write_agent_report("my-bucket", "2026-03-11", "# Report", dry_run=True)
    assert uri == "gs://my-bucket/t1-recon/recon/agent_report_2026-03-11.md"


def test_write_agent_report_dry_run_no_gcs_call() -> None:
    with patch(
        "batch_live_reconciliation_service.stages.stage4_agent_analysis.get_storage_client"
    ) as mock_gcs:
        _write_agent_report("my-bucket", "2026-03-11", "# Report", dry_run=True)
        mock_gcs.assert_not_called()


def test_write_agent_report_uploads_content() -> None:
    mock_client = MagicMock()
    with patch(
        "batch_live_reconciliation_service.stages.stage4_agent_analysis.get_storage_client",
        return_value=mock_client,
    ):
        uri = _write_agent_report("my-bucket", "2026-03-11", "# Report content", dry_run=False)

    assert uri == "gs://my-bucket/t1-recon/recon/agent_report_2026-03-11.md"
    mock_client.upload_bytes.assert_called_once()
    call_kwargs = mock_client.upload_bytes.call_args
    assert call_kwargs.kwargs["bucket"] == "my-bucket"
    assert b"# Report content" in call_kwargs.kwargs["data"]


# ---------------------------------------------------------------------------
# run_stage4
# ---------------------------------------------------------------------------


def test_run_stage4_dry_run_returns_passed() -> None:
    config = _make_config()
    reports = [_make_stage_report(ReconStage.ML_RECON)]

    with patch("batch_live_reconciliation_service.stages.stage4_agent_analysis.log_event"):
        result = run_stage4(config, "2026-03-11", reports, dry_run=True)

    assert result.stage == ReconStage.AGENT_ANALYSIS
    assert result.status == ReconStatus.PASSED
    assert result.output_gcs_path is not None


def test_run_stage4_dry_run_sets_gcs_path() -> None:
    config = _make_config()
    reports = [_make_stage_report(ReconStage.ML_RECON)]

    with patch("batch_live_reconciliation_service.stages.stage4_agent_analysis.log_event"):
        result = run_stage4(config, "2026-03-11", reports, dry_run=True)

    assert "test-recon-bucket" in (result.output_gcs_path or "")


def test_run_stage4_with_deviations_writes_report() -> None:
    config = _make_config()
    dev = _make_deviation("fill_rate_delta")
    reports = [_make_stage_report(ReconStage.ML_RECON, ReconStatus.FAILED, deviations=[dev])]

    mock_client = MagicMock()
    with (
        patch(
            "batch_live_reconciliation_service.stages.stage4_agent_analysis.get_storage_client",
            return_value=mock_client,
        ),
        patch("batch_live_reconciliation_service.stages.stage4_agent_analysis.log_event"),
    ):
        result = run_stage4(config, "2026-03-11", reports, dry_run=False)

    assert result.status == ReconStatus.PASSED
    mock_client.upload_bytes.assert_called_once()


def test_run_stage4_gcs_upload_error_returns_failed() -> None:
    config = _make_config()
    reports = [_make_stage_report(ReconStage.ML_RECON)]

    mock_client = MagicMock()
    mock_client.upload_bytes.side_effect = RuntimeError("GCS unavailable")
    with (
        patch(
            "batch_live_reconciliation_service.stages.stage4_agent_analysis.get_storage_client",
            return_value=mock_client,
        ),
        patch("batch_live_reconciliation_service.stages.stage4_agent_analysis.log_event"),
    ):
        result = run_stage4(config, "2026-03-11", reports, dry_run=False)

    assert result.status == ReconStatus.FAILED
    assert result.error_message is not None
    assert "GCS unavailable" in result.error_message


def test_run_stage4_empty_stage_reports() -> None:
    config = _make_config()
    mock_client = MagicMock()
    with (
        patch(
            "batch_live_reconciliation_service.stages.stage4_agent_analysis.get_storage_client",
            return_value=mock_client,
        ),
        patch("batch_live_reconciliation_service.stages.stage4_agent_analysis.log_event"),
    ):
        result = run_stage4(config, "2026-03-11", [], dry_run=False)

    assert result.status == ReconStatus.PASSED
    assert result.metrics["total_deviations_analysed"] == 0.0


def test_run_stage4_metrics_reflect_deviation_count() -> None:
    config = _make_config()
    devs = [_make_deviation(f"metric_{i}") for i in range(3)]
    reports = [_make_stage_report(ReconStage.ML_RECON, deviations=devs)]

    with (
        patch("batch_live_reconciliation_service.stages.stage4_agent_analysis.get_storage_client"),
        patch("batch_live_reconciliation_service.stages.stage4_agent_analysis.log_event"),
    ):
        result = run_stage4(config, "2026-03-11", reports, dry_run=True)

    assert result.metrics["total_deviations_analysed"] == 3.0
