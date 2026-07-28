"""Unit tests for reconciliation resolution API endpoints."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from unified_api_contracts.internal import ReconciliationAction, ReconciliationResolution

from batch_live_reconciliation_service.api.resolution_api import (
    _MOCK_BREAKS,
    BookCorrectionResponse,
    _breaks_from_summary,
    _current_breaks,
)

_MODULE = "batch_live_reconciliation_service.api.resolution_api"


class TestListBreaks:
    def test_mock_breaks_exist(self) -> None:
        assert len(_MOCK_BREAKS) >= 3

    def test_break_has_required_fields(self) -> None:
        brk = _MOCK_BREAKS[0]
        assert brk.break_id
        assert brk.venue
        assert brk.break_type
        assert brk.instrument_id


class TestReconciliationResolution:
    def test_accept_resolution_schema(self) -> None:
        resolution = ReconciliationResolution(
            break_id="BRK-001",
            action=ReconciliationAction.ACCEPT,
            note="Timing difference — batch snapshot taken before fill settled",
            resolved_by="trader@example.com",
        )
        assert resolution.action == ReconciliationAction.ACCEPT
        assert resolution.correcting_instruction_id is None

    def test_reject_resolution_with_correction(self) -> None:
        resolution = ReconciliationResolution(
            break_id="BRK-002",
            action=ReconciliationAction.REJECT,
            note="Missing fill — booking correction trade",
            resolved_by="ops@example.com",
            correcting_instruction_id="manual-20260322-143200-ABC123",
        )
        assert resolution.action == ReconciliationAction.REJECT
        assert resolution.correcting_instruction_id == "manual-20260322-143200-ABC123"

    def test_investigate_resolution(self) -> None:
        resolution = ReconciliationResolution(
            break_id="BRK-003",
            action=ReconciliationAction.INVESTIGATE,
            note="Need to check venue API logs for this period",
            resolved_by="analyst@example.com",
        )
        assert resolution.action == ReconciliationAction.INVESTIGATE

    def test_note_min_length_enforced(self) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="String should have at least 10 characters"):
            ReconciliationResolution(
                break_id="BRK-001",
                action=ReconciliationAction.ACCEPT,
                note="short",
                resolved_by="user@example.com",
            )


class TestBreaksFromSummary:
    def test_flattens_deviations_across_stages(self) -> None:
        summary = {
            "date": "2026-07-27",
            "stages": [
                {
                    "stage": "execution_recon",
                    "deviations": [
                        {
                            "metric_name": "alpha_pnl_gap",
                            "actual_value": 3.2,
                            "threshold": 2.0,
                            "instrument_id": "BTC-USDT",
                            "first_seen_at": "2026-07-27T06:00:00+00:00",
                        }
                    ],
                },
                {
                    "stage": "ml_recon",
                    "deviations": [
                        {
                            "metric_name": "signal_direction_match",
                            "actual_value": 0.80,
                            "threshold": 0.95,
                            "instrument_id": None,
                            "first_seen_at": "2026-07-27T06:00:01+00:00",
                        }
                    ],
                },
            ],
        }

        breaks = _breaks_from_summary(summary)

        assert len(breaks) == 2
        first = breaks[0]
        assert first.break_id == "2026-07-27:execution_recon:alpha_pnl_gap:BTC-USDT"
        assert first.venue == "ALL"
        assert first.instrument_id == "BTC-USDT"
        assert first.live_value == 3.2
        assert first.batch_value == 2.0
        assert first.delta == pytest.approx(1.2)
        assert first.status == "pending"

        second = breaks[1]
        # No instrument_id on the deviation → falls back to AGGREGATE.
        assert second.break_id == "2026-07-27:ml_recon:signal_direction_match:AGGREGATE"
        assert second.instrument_id == "AGGREGATE"

    def test_no_deviations_returns_empty_list(self) -> None:
        summary = {"date": "2026-07-27", "stages": [{"stage": "execution_recon", "deviations": []}]}
        assert _breaks_from_summary(summary) == []


class TestCurrentBreaks:
    def test_falls_back_to_mock_when_index_empty(self) -> None:
        mock_client = MagicMock()
        mock_client.download_bytes.side_effect = OSError("not found")
        mock_config = MagicMock(recon_bucket="recon-test")

        with (
            patch(f"{_MODULE}.get_storage_client", return_value=mock_client),
            patch(f"{_MODULE}.get_recon_config", return_value=mock_config),
        ):
            result = _current_breaks()

        assert result == _MOCK_BREAKS

    def test_falls_back_to_mock_when_summary_missing(self) -> None:
        index_bytes = json.dumps([{"date": "2026-07-27"}]).encode("utf-8")
        mock_client = MagicMock()

        def download_bytes(bucket: str, blob_path: str) -> bytes:
            if blob_path.endswith("index.json"):
                return index_bytes
            raise OSError("summary not found")

        mock_client.download_bytes.side_effect = download_bytes
        mock_config = MagicMock(recon_bucket="recon-test")

        with (
            patch(f"{_MODULE}.get_storage_client", return_value=mock_client),
            patch(f"{_MODULE}.get_recon_config", return_value=mock_config),
        ):
            result = _current_breaks()

        assert result == _MOCK_BREAKS

    def test_reads_real_breaks_from_latest_summary(self) -> None:
        index_bytes = json.dumps([{"date": "2026-07-27"}, {"date": "2026-07-26"}]).encode("utf-8")
        summary_bytes = json.dumps(
            {
                "date": "2026-07-27",
                "stages": [
                    {
                        "stage": "execution_recon",
                        "deviations": [
                            {
                                "metric_name": "fill_rate_delta",
                                "actual_value": 0.10,
                                "threshold": 0.05,
                                "instrument_id": "ETH-USD-PERP",
                                "first_seen_at": "2026-07-27T06:00:00+00:00",
                            }
                        ],
                    }
                ],
            }
        ).encode("utf-8")
        mock_client = MagicMock()

        def download_bytes(bucket: str, blob_path: str) -> bytes:
            if blob_path.endswith("index.json"):
                return index_bytes
            assert blob_path.endswith("summary_2026-07-27.json")
            return summary_bytes

        mock_client.download_bytes.side_effect = download_bytes
        mock_config = MagicMock(recon_bucket="recon-test")

        with (
            patch(f"{_MODULE}.get_storage_client", return_value=mock_client),
            patch(f"{_MODULE}.get_recon_config", return_value=mock_config),
        ):
            result = _current_breaks()

        assert len(result) == 1
        assert result[0].break_id == "2026-07-27:execution_recon:fill_rate_delta:ETH-USD-PERP"
        assert result[0].delta == pytest.approx(0.05)


class TestBookCorrectionResponse:
    def test_correction_defaults(self) -> None:
        correction = BookCorrectionResponse(
            venue="Binance",
            instrument_id="BTC-USDT",
            side="BUY",
            quantity=0.0045,
            reason="Correction for break BRK-001",
        )
        assert correction.execution_mode == "record_only"
        assert correction.category == ""

    def test_correction_with_source_reference(self) -> None:
        correction = BookCorrectionResponse(
            venue="Deribit",
            instrument_id="ETH-USD-PERP",
            side="SELL",
            quantity=321.80,
            reason="Correction for PnL break",
            source_reference="BRK-002",
        )
        assert correction.source_reference == "BRK-002"
