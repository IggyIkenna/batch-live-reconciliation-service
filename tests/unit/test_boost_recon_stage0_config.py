"""
Unit tests for Stage 0 Config + Data Availability Check.

Tests _blob_exists, _load_config_snapshot, and run_stage0 with mocked GCS.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from batch_live_reconciliation_service.models.recon_report import (
    ReconStage,
    ReconStatus,
)
from batch_live_reconciliation_service.stages.stage0_config_pull import (
    _blob_exists,
    _load_config_snapshot,
    run_stage0,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(
    recon_bucket: str = "recon-test",
    execution_store_bucket: str = "execution-store-test",
) -> MagicMock:
    cfg = MagicMock()
    cfg.recon_bucket = recon_bucket
    cfg.execution_store_bucket = execution_store_bucket
    return cfg


# ---------------------------------------------------------------------------
# _blob_exists
# ---------------------------------------------------------------------------


def test_blob_exists_returns_true_when_blob_exists() -> None:
    mock_blob = MagicMock()
    mock_blob.exists.return_value = True
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket

    with patch(
        "batch_live_reconciliation_service.stages.stage0_config_pull.get_storage_client",
        return_value=mock_client,
    ):
        result = _blob_exists("my-bucket", "some/path.json")

    assert result is True


def test_blob_exists_returns_false_when_blob_missing() -> None:
    mock_blob = MagicMock()
    mock_blob.exists.return_value = False
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket

    with patch(
        "batch_live_reconciliation_service.stages.stage0_config_pull.get_storage_client",
        return_value=mock_client,
    ):
        result = _blob_exists("my-bucket", "some/missing.json")

    assert result is False


def test_blob_exists_returns_false_on_exception() -> None:
    mock_client = MagicMock()
    mock_client.bucket.side_effect = RuntimeError("GCS error")

    with patch(
        "batch_live_reconciliation_service.stages.stage0_config_pull.get_storage_client",
        return_value=mock_client,
    ):
        result = _blob_exists("my-bucket", "some/path.json")

    assert result is False


# ---------------------------------------------------------------------------
# _load_config_snapshot
# ---------------------------------------------------------------------------


def test_load_config_snapshot_returns_dict() -> None:
    data = {"algo": "VWAP", "max_slippage_bps": 10}
    mock_client = MagicMock()
    mock_client.download_bytes.return_value = json.dumps(data).encode("utf-8")

    with patch(
        "batch_live_reconciliation_service.stages.stage0_config_pull.get_storage_client",
        return_value=mock_client,
    ):
        result = _load_config_snapshot("exec-store-bucket", "2026-03-11")

    assert result["algo"] == "VWAP"
    assert result["max_slippage_bps"] == 10


def test_load_config_snapshot_raises_file_not_found_on_error() -> None:
    mock_client = MagicMock()
    mock_client.download_bytes.side_effect = RuntimeError("blob not found")

    with patch(
        "batch_live_reconciliation_service.stages.stage0_config_pull.get_storage_client",
        return_value=mock_client,
    ), pytest.raises(FileNotFoundError, match="Config snapshot not found"):
        _load_config_snapshot("exec-store-bucket", "2026-03-11")


# ---------------------------------------------------------------------------
# run_stage0 — dry_run
# ---------------------------------------------------------------------------


def test_run_stage0_dry_run_returns_passed() -> None:
    config = _make_config()
    with patch("batch_live_reconciliation_service.stages.stage0_config_pull.log_event"):
        result = run_stage0(config, "2026-03-11", dry_run=True)
    assert result.stage == ReconStage.CONFIG_PULL
    assert result.status == ReconStatus.PASSED
    assert result.metrics.get("dry_run") == 1.0


def test_run_stage0_dry_run_no_gcs_calls() -> None:
    config = _make_config()
    with (
        patch(
            "batch_live_reconciliation_service.stages.stage0_config_pull.get_storage_client"
        ) as mock_gcs,
        patch("batch_live_reconciliation_service.stages.stage0_config_pull.log_event"),
    ):
        run_stage0(config, "2026-03-11", dry_run=True)
    mock_gcs.assert_not_called()


# ---------------------------------------------------------------------------
# run_stage0 — all blobs present
# ---------------------------------------------------------------------------


def test_run_stage0_passes_when_all_blobs_present() -> None:
    config = _make_config()

    def _blob_true(bucket: str, blob_path: str) -> bool:
        return True

    config_data = {"algo": "VWAP"}
    mock_client = MagicMock()
    mock_client.download_bytes.return_value = json.dumps(config_data).encode("utf-8")

    with (
        patch(
            "batch_live_reconciliation_service.stages.stage0_config_pull._blob_exists",
            side_effect=_blob_true,
        ),
        patch(
            "batch_live_reconciliation_service.stages.stage0_config_pull.get_storage_client",
            return_value=mock_client,
        ),
        patch("batch_live_reconciliation_service.stages.stage0_config_pull.log_event"),
    ):
        result = run_stage0(config, "2026-03-11", dry_run=False)

    assert result.status == ReconStatus.PASSED
    assert result.metrics["missing_count"] == 0.0


def test_run_stage0_fails_when_blobs_missing() -> None:
    config = _make_config()

    with (
        patch(
            "batch_live_reconciliation_service.stages.stage0_config_pull._blob_exists",
            return_value=False,
        ),
        patch("batch_live_reconciliation_service.stages.stage0_config_pull.log_event"),
    ):
        result = run_stage0(config, "2026-03-11", dry_run=False)

    assert result.status == ReconStatus.FAILED
    assert result.error_message is not None
    assert "2026-03-11" in result.error_message


def test_run_stage0_fails_on_config_snapshot_missing() -> None:
    config = _make_config()

    # Blobs exist but config snapshot download fails
    mock_client = MagicMock()
    mock_client.download_bytes.side_effect = RuntimeError("download failed")

    with (
        patch(
            "batch_live_reconciliation_service.stages.stage0_config_pull._blob_exists",
            return_value=True,
        ),
        patch(
            "batch_live_reconciliation_service.stages.stage0_config_pull.get_storage_client",
            return_value=mock_client,
        ),
        patch("batch_live_reconciliation_service.stages.stage0_config_pull.log_event"),
    ):
        result = run_stage0(config, "2026-03-11", dry_run=False)

    assert result.status == ReconStatus.FAILED
    assert result.error_message is not None


def test_run_stage0_error_message_lists_missing_items() -> None:
    config = _make_config()

    with (
        patch(
            "batch_live_reconciliation_service.stages.stage0_config_pull._blob_exists",
            return_value=False,
        ),
        patch("batch_live_reconciliation_service.stages.stage0_config_pull.log_event"),
    ):
        result = run_stage0(config, "2026-03-11", dry_run=False)

    assert result.error_message is not None
    assert "execution config snapshot" in result.error_message
    assert "ML t1-recon" in result.error_message
    assert "strategy t1-recon" in result.error_message


def test_run_stage0_completed_at_set() -> None:
    config = _make_config()
    with patch("batch_live_reconciliation_service.stages.stage0_config_pull.log_event"):
        result = run_stage0(config, "2026-03-11", dry_run=True)
    assert result.completed_at is not None
    assert result.started_at is not None
