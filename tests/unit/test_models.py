"""Unit tests for recon report models."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from batch_live_reconciliation_service.models.recon_report import (
    DeviationRecord,
    ReconReport,
    ReconStage,
    ReconStatus,
    StageReport,
)


def test_stage_report_passed() -> None:
    report = StageReport(
        stage=ReconStage.ML_RECON,
        status=ReconStatus.PASSED,
        started_at=datetime.now(UTC),
    )
    assert report.passed
    assert report.deviation_count == 0


def test_stage_report_with_deviations() -> None:
    deviation = DeviationRecord(
        metric_name="signal_direction_match_rate",
        stage=ReconStage.ML_RECON,
        actual_value=0.88,
        threshold=0.95,
        direction="below",
        description="Match rate 88.0% < 95% threshold",
    )
    report = StageReport(
        stage=ReconStage.ML_RECON,
        status=ReconStatus.FAILED,
        started_at=datetime.now(UTC),
        deviations=[deviation],
    )
    assert not report.passed
    assert report.deviation_count == 1


def test_recon_report_total_deviations() -> None:
    s1 = StageReport(
        stage=ReconStage.ML_RECON,
        status=ReconStatus.FAILED,
        started_at=datetime.now(UTC),
        deviations=[
            DeviationRecord(
                metric_name="signal_direction_match_rate",
                stage=ReconStage.ML_RECON,
                actual_value=0.88,
                threshold=0.95,
                direction="below",
                description="test",
            )
        ],
    )
    s2 = StageReport(
        stage=ReconStage.STRATEGY_RECON,
        status=ReconStatus.PASSED,
        started_at=datetime.now(UTC),
    )
    report = ReconReport(
        date="2026-03-09",
        run_id="test-run",
        started_at=datetime.now(UTC),
        stages=[s1, s2],
    )
    assert report.total_deviations == 1
    assert report.failed_stages == [ReconStage.ML_RECON]


def test_deviation_thresholds_defaults() -> None:
    from batch_live_reconciliation_service.models.deviation_thresholds import (
        EXECUTION_THRESHOLDS,
        ML_THRESHOLDS,
        STRATEGY_THRESHOLDS,
    )

    assert ML_THRESHOLDS.signal_direction_match_rate_min == 0.95
    assert STRATEGY_THRESHOLDS.instruction_alignment_pct_min == 0.85
    assert EXECUTION_THRESHOLDS.fill_rate_delta_max == 0.05
