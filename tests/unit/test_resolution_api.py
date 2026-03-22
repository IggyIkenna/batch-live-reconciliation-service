"""Unit tests for reconciliation resolution API endpoints."""

from __future__ import annotations

import pytest
from unittest.mock import patch

from batch_live_reconciliation_service.api.resolution_api import (
    ReconciliationBreakResponse,
    ResolveResponse,
    BookCorrectionResponse,
    BookCorrectionRequest,
    _MOCK_BREAKS,
    _resolutions,
    router,
)
from unified_internal_contracts import ReconciliationAction, ReconciliationResolution


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
