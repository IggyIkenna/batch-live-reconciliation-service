"""
Unit tests for Stage 5 Consolidated Results Writer.

Tests _load_index and run_stage5 with mocked GCS.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from batch_live_reconciliation_service.models.recon_report import (
    ReconReport,
    ReconStage,
    ReconStatus,
    StageReport,
)
from batch_live_reconciliation_service.stages.stage5_results_writer import (
    _load_index,
    run_stage5,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config() -> MagicMock:
    cfg = MagicMock()
    cfg.recon_bucket = "test-recon-bucket"
    cfg.dry_run = False
    return cfg


def _make_report(
    date: str = "2026-03-11",
    status: ReconStatus = ReconStatus.PASSED,
    stages: list[StageReport] | None = None,
) -> ReconReport:
    return ReconReport(
        date=date,
        run_id="test-run-id",
        started_at=datetime.now(UTC),
        status=status,
        stages=stages or [],
    )


def _make_stage_report(
    stage: ReconStage = ReconStage.ML_RECON,
    status: ReconStatus = ReconStatus.PASSED,
) -> StageReport:
    return StageReport(
        stage=stage,
        status=status,
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
    )


# ---------------------------------------------------------------------------
# _load_index
# ---------------------------------------------------------------------------


def test_load_index_returns_empty_list_on_error() -> None:
    mock_client = MagicMock()
    mock_client.download_bytes.side_effect = RuntimeError("not found")
    with patch("batch_live_reconciliation_service.stages.stage5_results_writer.get_storage_client", return_value=mock_client):
        result = _load_index("test-bucket")
    assert result == []


def test_load_index_parses_json() -> None:
    data = [{"date": "2026-03-10", "status": "passed"}]
    mock_client = MagicMock()
    mock_client.download_bytes.return_value = json.dumps(data).encode("utf-8")
    with patch("batch_live_reconciliation_service.stages.stage5_results_writer.get_storage_client", return_value=mock_client):
        result = _load_index("test-bucket")
    assert len(result) == 1
    assert result[0]["date"] == "2026-03-10"


def test_load_index_invalid_json_returns_empty() -> None:
    mock_client = MagicMock()
    mock_client.download_bytes.return_value = b"not valid json {{{"
    with patch("batch_live_reconciliation_service.stages.stage5_results_writer.get_storage_client", return_value=mock_client):
        result = _load_index("test-bucket")
    assert result == []


# ---------------------------------------------------------------------------
# run_stage5 — dry_run=True
# ---------------------------------------------------------------------------


def test_run_stage5_dry_run_returns_passed() -> None:
    config = _make_config()
    report = _make_report()
    with patch("batch_live_reconciliation_service.stages.stage5_results_writer.log_event"):
        result = run_stage5(config, report, dry_run=True)
    assert result.stage == ReconStage.RESULTS_WRITER
    assert result.status == ReconStatus.PASSED


def test_run_stage5_dry_run_sets_summary_gcs_path() -> None:
    config = _make_config()
    report = _make_report(date="2026-03-11")
    with patch("batch_live_reconciliation_service.stages.stage5_results_writer.log_event"):
        result = run_stage5(config, report, dry_run=True)
    assert result.output_gcs_path == "gs://test-recon-bucket/t1-recon/recon/summary_2026-03-11.json"


def test_run_stage5_dry_run_no_gcs_writes() -> None:
    config = _make_config()
    report = _make_report()
    with patch("batch_live_reconciliation_service.stages.stage5_results_writer.get_storage_client") as mock_gcs, \
         patch("batch_live_reconciliation_service.stages.stage5_results_writer.log_event"):
        run_stage5(config, report, dry_run=True)
    mock_gcs.assert_not_called()


# ---------------------------------------------------------------------------
# run_stage5 — live writes
# ---------------------------------------------------------------------------


def test_run_stage5_uploads_summary_json() -> None:
    config = _make_config()
    stages = [_make_stage_report(ReconStage.ML_RECON), _make_stage_report(ReconStage.STRATEGY_RECON)]
    report = _make_report(date="2026-03-11", stages=stages)

    mock_client = MagicMock()
    # Empty existing index
    mock_client.download_bytes.return_value = b"[]"
    with patch("batch_live_reconciliation_service.stages.stage5_results_writer.get_storage_client", return_value=mock_client), \
         patch("batch_live_reconciliation_service.stages.stage5_results_writer.log_event"):
        result = run_stage5(config, report, dry_run=False)

    assert result.status == ReconStatus.PASSED
    # Should have called upload_bytes twice: summary + index
    assert mock_client.upload_bytes.call_count == 2


def test_run_stage5_summary_contains_all_stage_data() -> None:
    config = _make_config()
    stages = [_make_stage_report(ReconStage.ML_RECON, ReconStatus.PASSED)]
    report = _make_report(date="2026-03-11", stages=stages)

    uploaded_data: list[bytes] = []

    def capture_upload(**kwargs: object) -> None:
        uploaded_data.append(kwargs["data"])  # type: ignore[arg-type]

    mock_client = MagicMock()
    mock_client.download_bytes.return_value = b"[]"
    mock_client.upload_bytes.side_effect = lambda **kw: uploaded_data.append(kw["data"])

    with patch("batch_live_reconciliation_service.stages.stage5_results_writer.get_storage_client", return_value=mock_client), \
         patch("batch_live_reconciliation_service.stages.stage5_results_writer.log_event"):
        run_stage5(config, report, dry_run=False)

    # First upload is the summary JSON
    assert len(uploaded_data) >= 1
    summary = json.loads(uploaded_data[0].decode("utf-8"))
    assert summary["date"] == "2026-03-11"
    assert summary["run_id"] == "test-run-id"
    assert len(summary["stages"]) == 1
    assert summary["stages"][0]["stage"] == "ml_recon"


def test_run_stage5_index_updated_with_new_entry() -> None:
    config = _make_config()
    report = _make_report(date="2026-03-11")

    existing_index = [{"date": "2026-03-10", "status": "passed"}]
    uploaded_data: list[bytes] = []

    mock_client = MagicMock()
    mock_client.download_bytes.return_value = json.dumps(existing_index).encode("utf-8")
    mock_client.upload_bytes.side_effect = lambda **kw: uploaded_data.append(kw["data"])

    with patch("batch_live_reconciliation_service.stages.stage5_results_writer.get_storage_client", return_value=mock_client), \
         patch("batch_live_reconciliation_service.stages.stage5_results_writer.log_event"):
        run_stage5(config, report, dry_run=False)

    # Second upload is the index
    assert len(uploaded_data) == 2
    updated_index = json.loads(uploaded_data[1].decode("utf-8"))
    dates = [e["date"] for e in updated_index]
    assert "2026-03-11" in dates
    assert "2026-03-10" in dates


def test_run_stage5_index_deduplicates_date() -> None:
    config = _make_config()
    report = _make_report(date="2026-03-11", status=ReconStatus.PASSED)

    existing_index = [{"date": "2026-03-11", "status": "failed"}]
    uploaded_data: list[bytes] = []

    mock_client = MagicMock()
    mock_client.download_bytes.return_value = json.dumps(existing_index).encode("utf-8")
    mock_client.upload_bytes.side_effect = lambda **kw: uploaded_data.append(kw["data"])

    with patch("batch_live_reconciliation_service.stages.stage5_results_writer.get_storage_client", return_value=mock_client), \
         patch("batch_live_reconciliation_service.stages.stage5_results_writer.log_event"):
        run_stage5(config, report, dry_run=False)

    updated_index = json.loads(uploaded_data[1].decode("utf-8"))
    # Should only have one entry for 2026-03-11
    entries_for_date = [e for e in updated_index if e["date"] == "2026-03-11"]
    assert len(entries_for_date) == 1
    assert entries_for_date[0]["status"] == "passed"


def test_run_stage5_gcs_upload_error_returns_failed() -> None:
    config = _make_config()
    report = _make_report()

    mock_client = MagicMock()
    mock_client.download_bytes.return_value = b"[]"
    mock_client.upload_bytes.side_effect = RuntimeError("GCS write failed")

    with patch("batch_live_reconciliation_service.stages.stage5_results_writer.get_storage_client", return_value=mock_client), \
         patch("batch_live_reconciliation_service.stages.stage5_results_writer.log_event"):
        result = run_stage5(config, report, dry_run=False)

    assert result.status == ReconStatus.FAILED
    assert result.error_message is not None


def test_run_stage5_metrics_total_deviations() -> None:
    config = _make_config()
    from batch_live_reconciliation_service.models.recon_report import DeviationRecord

    dev = DeviationRecord(
        metric_name="fill_rate_delta",
        stage=ReconStage.EXECUTION_RECON,
        actual_value=0.1,
        threshold=0.05,
        direction="above",
        description="fill rate too high",
    )
    stages = [
        StageReport(
            stage=ReconStage.EXECUTION_RECON,
            status=ReconStatus.FAILED,
            started_at=datetime.now(UTC),
            deviations=[dev],
        )
    ]
    report = _make_report(date="2026-03-11", status=ReconStatus.FAILED, stages=stages)

    mock_client = MagicMock()
    mock_client.download_bytes.return_value = b"[]"
    with patch("batch_live_reconciliation_service.stages.stage5_results_writer.get_storage_client", return_value=mock_client), \
         patch("batch_live_reconciliation_service.stages.stage5_results_writer.log_event"):
        result = run_stage5(config, report, dry_run=False)

    assert result.metrics["total_deviations"] == 1.0
