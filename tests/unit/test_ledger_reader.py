"""Unit tests for the instruction-ledger → ``TradeFillRecord`` reader (P4.2).

Credential-free. Mocks the GCS storage client with in-memory JSONL blobs and
proves the LedgerRow(event_type=trade) → TradeFillRecord mapping (trade_key,
instrument_key, side-from-direction/sign, abs qty, price, fees), the
event_type filter, and the gs:// root parsing.
"""

from __future__ import annotations

import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from batch_live_reconciliation_service.engine.ledger_reader import (
    LedgerRootError,
    load_instruction_ledger_fills,
)

_TRADE_ROW = {
    "event_type": "trade",
    "trade_id": "t1",
    "venue": "binance",
    "asset_class": "perp",
    "asset_symbol": "BTC-USDT",
    "direction": "SELL",
    "delta": "-2.5",
    "price": "100.5",
    "fees_in_quote": "0.25",
    "timestamp_utc": "2026-06-19T12:00:00+00:00",
    "strategy_instruction_id": "instr-t1",
    "fill_model": "BENCHMARK",
}

_NON_TRADE_ROW = {
    "event_type": "mark_update",
    "trade_id": "m1",
    "venue": "binance",
    "asset_class": "perp",
    "asset_symbol": "BTC-USDT",
    "delta": "0",
    "price": "100.5",
    "timestamp_utc": "2026-06-19T12:00:00+00:00",
}


def _mock_storage(blob_lines: list[str]) -> MagicMock:
    blob = MagicMock()
    blob.download_as_text.return_value = "\n".join(blob_lines)
    bucket = MagicMock()
    bucket.list_blobs.return_value = [blob]
    client = MagicMock()
    client.bucket.return_value = bucket
    return client


def test_maps_trade_row_to_fill() -> None:
    """A trade row maps to a TradeFillRecord with abs qty + direction side."""
    lines = [json.dumps(_TRADE_ROW), json.dumps(_NON_TRADE_ROW)]
    with patch(
        "batch_live_reconciliation_service.engine.ledger_reader.get_storage_client",
        return_value=_mock_storage(lines),
    ):
        fills = load_instruction_ledger_fills("gs://ledgers/run-1/")

    assert len(fills) == 1  # the mark_update row is filtered out
    fill = fills[0]
    assert fill.trade_key == "t1"
    assert fill.instrument_key == "binance:perp:BTC-USDT"
    assert fill.side == "SELL"
    assert fill.qty == Decimal("2.5")  # absolute value of the signed delta
    assert fill.fill_price == Decimal("100.5")
    assert fill.fees_in_quote == Decimal("0.25")
    assert fill.venue == "binance"


def test_side_inferred_from_delta_sign_when_no_direction() -> None:
    """Missing direction → side from the sign of delta (positive = BUY)."""
    row = {**_TRADE_ROW}
    del row["direction"]
    row["delta"] = "3.0"
    with patch(
        "batch_live_reconciliation_service.engine.ledger_reader.get_storage_client",
        return_value=_mock_storage([json.dumps(row)]),
    ):
        fills = load_instruction_ledger_fills("gs://ledgers/run-1/")
    assert fills[0].side == "BUY"
    assert fills[0].qty == Decimal("3.0")


def test_no_blobs_returns_empty() -> None:
    """No instruction-ledger objects → empty list (honest absence, no raise)."""
    client = MagicMock()
    bucket = MagicMock()
    bucket.list_blobs.return_value = []
    client.bucket.return_value = bucket
    with patch(
        "batch_live_reconciliation_service.engine.ledger_reader.get_storage_client",
        return_value=client,
    ):
        fills = load_instruction_ledger_fills("gs://ledgers/run-1/")
    assert fills == []


def test_non_gs_root_raises() -> None:
    """A non-gs:// ledger_root raises LedgerRootError."""
    with pytest.raises(LedgerRootError):
        load_instruction_ledger_fills("/local/path/run-1/")
