"""Unit tests for stage0_manifest_reason_check.

Tests all flag conditions:
- Both captured → no deviation
- Both empty_confirmed, same reason → no deviation
- Both empty_confirmed, different reasons → deviation
- One captured, other attempted_failed → deviation
- Both attempted_failed → deviation
- One captured, other empty_confirmed → deviation (asymmetry)
- OSError on read → fails open (empty list)
- Empty manifest → fails open
- Absent date (no pipeline_mode rows) → skip (fail-open)
- Missing pipeline_mode column → skip (fail-open)
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from batch_live_reconciliation_service.stages.stage0_manifest_reason_check import (
    check_manifest_reason_agreement,
)

_BUCKET = "features-bucket"
_AG = "cefi"
_DATE = "2026-02-10"


def _make_manifest(rows: list[dict[str, str]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _check(*rows: dict[str, str], dates: list[str] | None = None) -> list[object]:
    manifest = _make_manifest(list(rows))
    with patch(
        "batch_live_reconciliation_service.stages.stage0_manifest_reason_check.read_availability_index",
        return_value=manifest,
    ):
        return check_manifest_reason_agreement(
            bucket=_BUCKET,
            dates=dates or [_DATE],
            asset_group=_AG,
        )


def _row(pipeline_mode: str, capture_status: str, error_reason: str = "") -> dict[str, str]:
    return {
        "date": _DATE,
        "pipeline_mode": pipeline_mode,
        "capture_status": capture_status,
        "error_reason": error_reason,
    }


def test_both_captured_no_deviation() -> None:
    deviations = _check(_row("batch", "captured"), _row("live", "captured"))
    assert deviations == []


def test_both_empty_confirmed_same_reason_no_deviation() -> None:
    deviations = _check(
        _row("batch", "empty_confirmed", "EXPECTED_HOLIDAY"),
        _row("live", "empty_confirmed", "EXPECTED_HOLIDAY"),
    )
    assert deviations == []


def test_both_empty_confirmed_different_reasons_flagged() -> None:
    deviations = _check(
        _row("batch", "empty_confirmed", "EXPECTED_HOLIDAY"),
        _row("live", "empty_confirmed", "SOURCE_RETURNED_ZERO"),
    )
    assert len(deviations) == 1
    assert "reasons differ" in deviations[0].description


def test_one_captured_other_attempted_failed_flagged() -> None:
    deviations = _check(_row("batch", "captured"), _row("live", "attempted_failed"))
    assert len(deviations) == 1
    assert "one side captured while other failed" in deviations[0].description


def test_attempted_failed_vs_captured_symmetric() -> None:
    deviations = _check(_row("batch", "attempted_failed"), _row("live", "captured"))
    assert len(deviations) == 1


def test_both_attempted_failed_flagged() -> None:
    deviations = _check(_row("batch", "attempted_failed"), _row("live", "attempted_failed"))
    assert len(deviations) == 1
    assert "attempted_failed on at least one side" in deviations[0].description


def test_one_captured_other_empty_confirmed_flagged() -> None:
    deviations = _check(
        _row("batch", "captured"),
        _row("live", "empty_confirmed", "EXPECTED_HOLIDAY"),
    )
    assert len(deviations) == 1
    assert "batch=captured but live=empty_confirmed" in deviations[0].description


def test_oserror_fails_open() -> None:
    with patch(
        "batch_live_reconciliation_service.stages.stage0_manifest_reason_check.read_availability_index",
        side_effect=OSError("gcs timeout"),
    ):
        deviations = check_manifest_reason_agreement(bucket=_BUCKET, dates=[_DATE], asset_group=_AG)
    assert deviations == []


def test_valueerror_fails_open() -> None:
    with patch(
        "batch_live_reconciliation_service.stages.stage0_manifest_reason_check.read_availability_index",
        side_effect=ValueError("corrupt parquet"),
    ):
        deviations = check_manifest_reason_agreement(bucket=_BUCKET, dates=[_DATE], asset_group=_AG)
    assert deviations == []


def test_empty_manifest_fails_open() -> None:
    with patch(
        "batch_live_reconciliation_service.stages.stage0_manifest_reason_check.read_availability_index",
        return_value=pd.DataFrame(),
    ):
        deviations = check_manifest_reason_agreement(bucket=_BUCKET, dates=[_DATE], asset_group=_AG)
    assert deviations == []


def test_absent_date_no_rows_skips() -> None:
    manifest = _make_manifest(
        [
            {"date": "2026-01-01", "pipeline_mode": "batch", "capture_status": "captured", "error_reason": ""},
            {"date": "2026-01-01", "pipeline_mode": "live", "capture_status": "captured", "error_reason": ""},
        ]
    )
    with patch(
        "batch_live_reconciliation_service.stages.stage0_manifest_reason_check.read_availability_index",
        return_value=manifest,
    ):
        deviations = check_manifest_reason_agreement(bucket=_BUCKET, dates=["2026-02-10"], asset_group=_AG)
    assert deviations == []


def test_missing_pipeline_mode_column_skips() -> None:
    manifest = _make_manifest(
        [
            {"date": _DATE, "capture_status": "captured", "error_reason": ""},
        ]
    )
    with patch(
        "batch_live_reconciliation_service.stages.stage0_manifest_reason_check.read_availability_index",
        return_value=manifest,
    ):
        deviations = check_manifest_reason_agreement(bucket=_BUCKET, dates=[_DATE], asset_group=_AG)
    assert deviations == []


def test_multiple_dates_mixed_outcomes() -> None:
    rows = [
        {"date": "2026-02-10", "pipeline_mode": "batch", "capture_status": "captured", "error_reason": ""},
        {"date": "2026-02-10", "pipeline_mode": "live", "capture_status": "captured", "error_reason": ""},
        {
            "date": "2026-02-11",
            "pipeline_mode": "batch",
            "capture_status": "empty_confirmed",
            "error_reason": "EXPECTED_HOLIDAY",
        },
        {
            "date": "2026-02-11",
            "pipeline_mode": "live",
            "capture_status": "empty_confirmed",
            "error_reason": "SOURCE_RETURNED_ZERO",
        },
        {"date": "2026-02-12", "pipeline_mode": "batch", "capture_status": "captured", "error_reason": ""},
        {"date": "2026-02-12", "pipeline_mode": "live", "capture_status": "attempted_failed", "error_reason": ""},
    ]
    manifest = _make_manifest(rows)
    with patch(
        "batch_live_reconciliation_service.stages.stage0_manifest_reason_check.read_availability_index",
        return_value=manifest,
    ):
        deviations = check_manifest_reason_agreement(
            bucket=_BUCKET,
            dates=["2026-02-10", "2026-02-11", "2026-02-12"],
            asset_group=_AG,
        )
    assert len(deviations) == 2
    descriptions = [d.description for d in deviations]
    assert any("reasons differ" in d for d in descriptions)
    assert any("one side captured while other failed" in d for d in descriptions)


def test_deviation_record_has_correct_stage() -> None:
    from batch_live_reconciliation_service.models.recon_report import ReconStage

    deviations = _check(_row("batch", "captured"), _row("live", "attempted_failed"))
    assert len(deviations) == 1
    assert deviations[0].stage == ReconStage.DATA_PIPELINE_RECON
    assert deviations[0].metric_name == "manifest_reason_disagreement"
