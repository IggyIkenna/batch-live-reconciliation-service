"""
Unit tests for stage runner functions (stages 1-3) with mocked GCS and UEI.

Each run_stageN function is tested in dry_run mode (no I/O) and with
mocked GCS event loading.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from batch_live_reconciliation_service.models.recon_report import (
    ReconStage,
    ReconStatus,
    StageReport,
)
from batch_live_reconciliation_service.stages.stage1_ml_recon import run_stage1
from batch_live_reconciliation_service.stages.stage2_strategy_recon import run_stage2
from batch_live_reconciliation_service.stages.stage3_execution_recon import run_stage3


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config() -> MagicMock:
    cfg = MagicMock()
    cfg.events_bucket = "test-events-bucket"
    cfg.recon_bucket = "test-recon-bucket"
    cfg.gcp_project_id = "test-project"
    return cfg


# ---------------------------------------------------------------------------
# Stage 1 — ML Recon
# ---------------------------------------------------------------------------


def test_run_stage1_dry_run_returns_passed() -> None:
    config = _make_config()
    with patch("batch_live_reconciliation_service.stages.stage1_ml_recon.log_event"):
        result = run_stage1(config, "2026-03-11", dry_run=True)
    assert result.stage == ReconStage.ML_RECON
    assert result.status == ReconStatus.PASSED
    assert result.metrics.get("dry_run") == 1.0


def test_run_stage1_dry_run_no_gcs_calls() -> None:
    config = _make_config()
    with patch("batch_live_reconciliation_service.stages.stage1_ml_recon.get_storage_client") as mock_gcs, \
         patch("batch_live_reconciliation_service.stages.stage1_ml_recon.log_event"):
        run_stage1(config, "2026-03-11", dry_run=True)
    mock_gcs.assert_not_called()


def test_run_stage1_passes_with_perfect_data() -> None:
    config = _make_config()
    # NDJSON: one JSON object per line (not a JSON array)
    events_json = json.dumps({"instrument_id": "BTC-USD", "timeframe": "1m", "signal_direction": 1, "magnitude": 0.5})
    ndjson_bytes = events_json.encode("utf-8")

    mock_blob = MagicMock()
    mock_blob.name = "some/file.ndjson"
    mock_bucket = MagicMock()
    mock_bucket.list_blobs.return_value = [mock_blob]
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    mock_client.download_bytes.return_value = ndjson_bytes

    with patch("batch_live_reconciliation_service.stages.stage1_ml_recon.get_storage_client", return_value=mock_client), \
         patch("batch_live_reconciliation_service.stages.stage1_ml_recon.log_event"):
        result = run_stage1(config, "2026-03-11", dry_run=False)

    assert result.stage == ReconStage.ML_RECON
    assert result.status == ReconStatus.PASSED
    assert result.metrics["signal_direction_match_rate"] == pytest.approx(1.0)


def test_run_stage1_fails_with_no_live_events() -> None:
    config = _make_config()

    call_count = 0

    def load_side_effect(bucket: str, blob_path: str) -> bytes:
        nonlocal call_count
        call_count += 1
        # First call = batch (return data), second call = live (return empty)
        if call_count == 1:
            return json.dumps([
                {"instrument_id": "BTC-USD", "timeframe": "1m", "signal_direction": 1, "magnitude": 0.5}
            ]).encode("utf-8")
        return b""

    mock_blob = MagicMock()
    mock_blob.name = "file.ndjson"
    mock_bucket = MagicMock()
    mock_bucket.list_blobs.return_value = [mock_blob]
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    mock_client.download_bytes.side_effect = load_side_effect

    with patch("batch_live_reconciliation_service.stages.stage1_ml_recon.get_storage_client", return_value=mock_client), \
         patch("batch_live_reconciliation_service.stages.stage1_ml_recon.log_event"):
        result = run_stage1(config, "2026-03-11", dry_run=False)

    # When live events are empty: coverage=0, match_rate=0, mae=999 → all breach thresholds
    assert result.status == ReconStatus.FAILED
    assert len(result.deviations) > 0


def test_run_stage1_result_has_timestamps() -> None:
    config = _make_config()
    with patch("batch_live_reconciliation_service.stages.stage1_ml_recon.log_event"):
        result = run_stage1(config, "2026-03-11", dry_run=True)
    assert result.started_at is not None
    assert result.completed_at is not None


# ---------------------------------------------------------------------------
# Stage 2 — Strategy Recon
# ---------------------------------------------------------------------------


def test_run_stage2_dry_run_returns_passed() -> None:
    config = _make_config()
    with patch("batch_live_reconciliation_service.stages.stage2_strategy_recon.log_event"):
        result = run_stage2(config, "2026-03-11", dry_run=True)
    assert result.stage == ReconStage.STRATEGY_RECON
    assert result.status == ReconStatus.PASSED
    assert result.metrics.get("dry_run") == 1.0


def test_run_stage2_dry_run_no_gcs_calls() -> None:
    config = _make_config()
    with patch("batch_live_reconciliation_service.stages.stage2_strategy_recon.get_storage_client") as mock_gcs, \
         patch("batch_live_reconciliation_service.stages.stage2_strategy_recon.log_event"):
        run_stage2(config, "2026-03-11", dry_run=True)
    mock_gcs.assert_not_called()


def test_run_stage2_passes_with_matching_events() -> None:
    config = _make_config()
    events = [
        {"event_type": "INSTRUCTION", "instrument_id": "BTC-USD", "side": "BUY"},
        {"event_type": "POSITION_SNAPSHOT", "instrument_id": "BTC-USD", "net_position": 1.0, "unrealized_pnl": 100.0},
        {"event_type": "RISK_SNAPSHOT", "var_1d": 50000.0},
    ]
    ndjson = "\n".join(json.dumps(e) for e in events).encode("utf-8")

    mock_blob = MagicMock()
    mock_blob.name = "file.ndjson"
    mock_bucket = MagicMock()
    mock_bucket.list_blobs.return_value = [mock_blob]
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    mock_client.download_bytes.return_value = ndjson

    with patch("batch_live_reconciliation_service.stages.stage2_strategy_recon.get_storage_client", return_value=mock_client), \
         patch("batch_live_reconciliation_service.stages.stage2_strategy_recon.log_event"):
        result = run_stage2(config, "2026-03-11", dry_run=False)

    assert result.stage == ReconStage.STRATEGY_RECON
    assert result.status == ReconStatus.PASSED


def test_run_stage2_result_has_timestamps() -> None:
    config = _make_config()
    with patch("batch_live_reconciliation_service.stages.stage2_strategy_recon.log_event"):
        result = run_stage2(config, "2026-03-11", dry_run=True)
    assert result.started_at is not None
    assert result.completed_at is not None


def test_run_stage2_emits_log_events() -> None:
    config = _make_config()
    with patch("batch_live_reconciliation_service.stages.stage2_strategy_recon.log_event") as mock_log:
        run_stage2(config, "2026-03-11", dry_run=True)
    # In dry_run mode, only PROCESSING_STARTED is emitted before early return
    assert mock_log.call_count >= 1


# ---------------------------------------------------------------------------
# Stage 3 — Execution Recon
# ---------------------------------------------------------------------------


def test_run_stage3_dry_run_returns_passed() -> None:
    config = _make_config()
    with patch("batch_live_reconciliation_service.stages.stage3_execution_recon.log_event"):
        result = run_stage3(config, "2026-03-11", dry_run=True)
    assert result.stage == ReconStage.EXECUTION_RECON
    assert result.status == ReconStatus.PASSED
    assert result.metrics.get("dry_run") == 1.0


def test_run_stage3_dry_run_no_gcs_calls() -> None:
    config = _make_config()
    with patch("batch_live_reconciliation_service.stages.stage3_execution_recon.get_storage_client") as mock_gcs, \
         patch("batch_live_reconciliation_service.stages.stage3_execution_recon.log_event"):
        run_stage3(config, "2026-03-11", dry_run=True)
    mock_gcs.assert_not_called()


def test_run_stage3_passes_with_matching_execution_events() -> None:
    config = _make_config()
    events = [
        {"event_type": "ORDER_SUBMITTED", "algo_used": "VWAP", "algo_configured": "VWAP"},
        {"event_type": "FILL", "fill_price": 100.0, "filled_qty": 1.0, "slippage_bps": 2.0, "latency_ms": 100.0},
    ]
    ndjson = "\n".join(json.dumps(e) for e in events).encode("utf-8")

    mock_blob = MagicMock()
    mock_blob.name = "file.ndjson"
    mock_bucket = MagicMock()
    mock_bucket.list_blobs.return_value = [mock_blob]
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    mock_client.download_bytes.return_value = ndjson

    with patch("batch_live_reconciliation_service.stages.stage3_execution_recon.get_storage_client", return_value=mock_client), \
         patch("batch_live_reconciliation_service.stages.stage3_execution_recon.log_event"):
        result = run_stage3(config, "2026-03-11", dry_run=False)

    assert result.stage == ReconStage.EXECUTION_RECON
    assert result.status == ReconStatus.PASSED


def test_run_stage3_result_has_timestamps() -> None:
    config = _make_config()
    with patch("batch_live_reconciliation_service.stages.stage3_execution_recon.log_event"):
        result = run_stage3(config, "2026-03-11", dry_run=True)
    assert result.started_at is not None
    assert result.completed_at is not None


def test_run_stage3_deviations_detected_with_bad_data() -> None:
    config = _make_config()
    # Highly abnormal: algo completely wrong
    events = [
        {"event_type": "ORDER_SUBMITTED", "algo_used": "VWAP", "algo_configured": "TWAP"},
        {"event_type": "ORDER_SUBMITTED", "algo_used": "VWAP", "algo_configured": "TWAP"},
        {"event_type": "FILL", "fill_price": 1000.0, "filled_qty": 10.0, "slippage_bps": 100.0},
    ]
    ndjson = "\n".join(json.dumps(e) for e in events).encode("utf-8")

    mock_blob = MagicMock()
    mock_blob.name = "file.jsonl"
    mock_bucket = MagicMock()
    mock_bucket.list_blobs.return_value = [mock_blob]
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    mock_client.download_bytes.return_value = ndjson

    with patch("batch_live_reconciliation_service.stages.stage3_execution_recon.get_storage_client", return_value=mock_client), \
         patch("batch_live_reconciliation_service.stages.stage3_execution_recon.log_event"):
        result = run_stage3(config, "2026-03-11", dry_run=False)

    # algo accuracy = 0 < 0.99 → deviation
    assert result.status == ReconStatus.FAILED
    assert any(d.metric_name == "algo_selection_accuracy" for d in result.deviations)
