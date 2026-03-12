"""
Unit tests for the T+1 reconciliation orchestrator.

All stages are patched so no GCS/UEI I/O occurs.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from batch_live_reconciliation_service.models.recon_report import (
    ReconReport,
    ReconStage,
    ReconStatus,
    StageReport,
)
from batch_live_reconciliation_service.orchestrator import run_reconciliation

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _passed_stage(stage: ReconStage) -> StageReport:
    return StageReport(
        stage=stage,
        status=ReconStatus.PASSED,
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
    )


def _failed_stage(stage: ReconStage, error: str = "test error") -> StageReport:
    return StageReport(
        stage=stage,
        status=ReconStatus.FAILED,
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        error_message=error,
    )


def _make_mock_config() -> MagicMock:
    cfg = MagicMock()
    cfg.gcp_project_id = "test-project"
    cfg.events_bucket = "test-events"
    cfg.recon_bucket = "test-recon"
    return cfg


def _patch_all_stages(
    s0: StageReport,
    s1: StageReport,
    s2: StageReport,
    s3: StageReport,
    s4: StageReport,
    s5: StageReport,
) -> list[object]:
    """Return list of context managers patching all 6 stages + config + observability."""
    return [
        patch(
            "batch_live_reconciliation_service.orchestrator.get_recon_config",
            return_value=_make_mock_config(),
        ),
        patch("batch_live_reconciliation_service.orchestrator._setup_observability"),
        patch("batch_live_reconciliation_service.orchestrator.log_event"),
        patch("batch_live_reconciliation_service.orchestrator.run_stage0", return_value=s0),
        patch("batch_live_reconciliation_service.orchestrator.run_stage1", return_value=s1),
        patch("batch_live_reconciliation_service.orchestrator.run_stage2", return_value=s2),
        patch("batch_live_reconciliation_service.orchestrator.run_stage3", return_value=s3),
        patch("batch_live_reconciliation_service.orchestrator.run_stage4", return_value=s4),
        patch("batch_live_reconciliation_service.orchestrator.run_stage5", return_value=s5),
    ]


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_run_reconciliation_all_pass_returns_passed_report() -> None:
    s0 = _passed_stage(ReconStage.CONFIG_PULL)
    s1 = _passed_stage(ReconStage.ML_RECON)
    s2 = _passed_stage(ReconStage.STRATEGY_RECON)
    s3 = _passed_stage(ReconStage.EXECUTION_RECON)
    s4 = _passed_stage(ReconStage.AGENT_ANALYSIS)
    s5 = _passed_stage(ReconStage.RESULTS_WRITER)

    patches = _patch_all_stages(s0, s1, s2, s3, s4, s5)
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patches[5],
        patches[6],
        patches[7],
        patches[8],
    ):
        report = run_reconciliation("2026-03-11")

    assert isinstance(report, ReconReport)
    assert report.status == ReconStatus.PASSED
    assert report.date == "2026-03-11"
    assert len(report.stages) == 6


def test_run_reconciliation_sets_run_id() -> None:
    s0 = _passed_stage(ReconStage.CONFIG_PULL)
    s1 = _passed_stage(ReconStage.ML_RECON)
    s2 = _passed_stage(ReconStage.STRATEGY_RECON)
    s3 = _passed_stage(ReconStage.EXECUTION_RECON)
    s4 = _passed_stage(ReconStage.AGENT_ANALYSIS)
    s5 = _passed_stage(ReconStage.RESULTS_WRITER)

    patches = _patch_all_stages(s0, s1, s2, s3, s4, s5)
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patches[5],
        patches[6],
        patches[7],
        patches[8],
    ):
        report = run_reconciliation("2026-03-11")

    assert report.run_id != ""
    assert len(report.run_id) == 36  # UUID format


def test_run_reconciliation_sets_completed_at() -> None:
    stages = [
        _passed_stage(s)
        for s in [
            ReconStage.CONFIG_PULL,
            ReconStage.ML_RECON,
            ReconStage.STRATEGY_RECON,
            ReconStage.EXECUTION_RECON,
            ReconStage.AGENT_ANALYSIS,
            ReconStage.RESULTS_WRITER,
        ]
    ]

    patches = _patch_all_stages(*stages)
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patches[5],
        patches[6],
        patches[7],
        patches[8],
    ):
        report = run_reconciliation("2026-03-11")

    assert report.completed_at is not None


# ---------------------------------------------------------------------------
# Stage 0 failure aborts pipeline
# ---------------------------------------------------------------------------


def test_run_reconciliation_aborts_on_stage0_failure() -> None:
    s0 = _failed_stage(ReconStage.CONFIG_PULL, "upstream data missing")

    with (
        patch(
            "batch_live_reconciliation_service.orchestrator.get_recon_config",
            return_value=_make_mock_config(),
        ),
        patch("batch_live_reconciliation_service.orchestrator._setup_observability"),
        patch("batch_live_reconciliation_service.orchestrator.log_event"),
        patch("batch_live_reconciliation_service.orchestrator.run_stage0", return_value=s0),
        patch("batch_live_reconciliation_service.orchestrator.run_stage1") as mock_s1,
    ):
        report = run_reconciliation("2026-03-11")

    assert report.status == ReconStatus.FAILED
    assert len(report.stages) == 1  # Only stage 0
    mock_s1.assert_not_called()


def test_run_reconciliation_stage0_failure_sets_error_path() -> None:
    s0 = _failed_stage(ReconStage.CONFIG_PULL, "upstream data missing")

    with (
        patch(
            "batch_live_reconciliation_service.orchestrator.get_recon_config",
            return_value=_make_mock_config(),
        ),
        patch("batch_live_reconciliation_service.orchestrator._setup_observability"),
        patch("batch_live_reconciliation_service.orchestrator.log_event"),
        patch("batch_live_reconciliation_service.orchestrator.run_stage0", return_value=s0),
    ):
        report = run_reconciliation("2026-03-11")

    assert report.completed_at is not None
    assert report.failed_stages == [ReconStage.CONFIG_PULL]


# ---------------------------------------------------------------------------
# Partial failures (stages 1-3)
# ---------------------------------------------------------------------------


def test_run_reconciliation_stage1_failure_marks_overall_failed() -> None:
    s0 = _passed_stage(ReconStage.CONFIG_PULL)
    s1 = _failed_stage(ReconStage.ML_RECON, "ML events missing")
    s2 = _passed_stage(ReconStage.STRATEGY_RECON)
    s3 = _passed_stage(ReconStage.EXECUTION_RECON)
    s4 = _passed_stage(ReconStage.AGENT_ANALYSIS)
    s5 = _passed_stage(ReconStage.RESULTS_WRITER)

    patches = _patch_all_stages(s0, s1, s2, s3, s4, s5)
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patches[5],
        patches[6],
        patches[7],
        patches[8],
    ):
        report = run_reconciliation("2026-03-11")

    assert report.status == ReconStatus.FAILED
    assert ReconStage.ML_RECON in report.failed_stages


def test_run_reconciliation_multiple_stage_failures() -> None:
    s0 = _passed_stage(ReconStage.CONFIG_PULL)
    s1 = _failed_stage(ReconStage.ML_RECON)
    s2 = _failed_stage(ReconStage.STRATEGY_RECON)
    s3 = _passed_stage(ReconStage.EXECUTION_RECON)
    s4 = _passed_stage(ReconStage.AGENT_ANALYSIS)
    s5 = _passed_stage(ReconStage.RESULTS_WRITER)

    patches = _patch_all_stages(s0, s1, s2, s3, s4, s5)
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patches[5],
        patches[6],
        patches[7],
        patches[8],
    ):
        report = run_reconciliation("2026-03-11")

    assert report.status == ReconStatus.FAILED
    assert len(report.failed_stages) == 2


# ---------------------------------------------------------------------------
# Stage 4 agent report GCS path propagation
# ---------------------------------------------------------------------------


def test_run_reconciliation_propagates_agent_report_gcs_path() -> None:
    s0 = _passed_stage(ReconStage.CONFIG_PULL)
    s1 = _passed_stage(ReconStage.ML_RECON)
    s2 = _passed_stage(ReconStage.STRATEGY_RECON)
    s3 = _passed_stage(ReconStage.EXECUTION_RECON)
    s4 = StageReport(
        stage=ReconStage.AGENT_ANALYSIS,
        status=ReconStatus.PASSED,
        started_at=datetime.now(UTC),
        output_gcs_path="gs://bucket/agent_report.md",
    )
    s5 = _passed_stage(ReconStage.RESULTS_WRITER)

    patches = _patch_all_stages(s0, s1, s2, s3, s4, s5)
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patches[5],
        patches[6],
        patches[7],
        patches[8],
    ):
        report = run_reconciliation("2026-03-11")

    assert report.agent_report_gcs_path == "gs://bucket/agent_report.md"


def test_run_reconciliation_propagates_summary_gcs_path() -> None:
    s0 = _passed_stage(ReconStage.CONFIG_PULL)
    s1 = _passed_stage(ReconStage.ML_RECON)
    s2 = _passed_stage(ReconStage.STRATEGY_RECON)
    s3 = _passed_stage(ReconStage.EXECUTION_RECON)
    s4 = _passed_stage(ReconStage.AGENT_ANALYSIS)
    s5 = StageReport(
        stage=ReconStage.RESULTS_WRITER,
        status=ReconStatus.PASSED,
        started_at=datetime.now(UTC),
        output_gcs_path="gs://bucket/summary.json",
    )

    patches = _patch_all_stages(s0, s1, s2, s3, s4, s5)
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patches[5],
        patches[6],
        patches[7],
        patches[8],
    ):
        report = run_reconciliation("2026-03-11")

    assert report.summary_gcs_path == "gs://bucket/summary.json"


# ---------------------------------------------------------------------------
# dry_run flag propagation
# ---------------------------------------------------------------------------


def test_run_reconciliation_passes_dry_run_to_stages() -> None:
    stages = [
        _passed_stage(s)
        for s in [
            ReconStage.CONFIG_PULL,
            ReconStage.ML_RECON,
            ReconStage.STRATEGY_RECON,
            ReconStage.EXECUTION_RECON,
            ReconStage.AGENT_ANALYSIS,
            ReconStage.RESULTS_WRITER,
        ]
    ]

    with (
        patch(
            "batch_live_reconciliation_service.orchestrator.get_recon_config",
            return_value=_make_mock_config(),
        ),
        patch("batch_live_reconciliation_service.orchestrator._setup_observability"),
        patch("batch_live_reconciliation_service.orchestrator.log_event"),
        patch(
            "batch_live_reconciliation_service.orchestrator.run_stage0", return_value=stages[0]
        ) as mock_s0,
        patch(
            "batch_live_reconciliation_service.orchestrator.run_stage1", return_value=stages[1]
        ) as mock_s1,
        patch("batch_live_reconciliation_service.orchestrator.run_stage2", return_value=stages[2]),
        patch("batch_live_reconciliation_service.orchestrator.run_stage3", return_value=stages[3]),
        patch("batch_live_reconciliation_service.orchestrator.run_stage4", return_value=stages[4]),
        patch("batch_live_reconciliation_service.orchestrator.run_stage5", return_value=stages[5]),
    ):
        run_reconciliation("2026-03-11", dry_run=True)

    _, s0_kwargs = mock_s0.call_args
    assert s0_kwargs.get("dry_run") is True
    _, s1_kwargs = mock_s1.call_args
    assert s1_kwargs.get("dry_run") is True
