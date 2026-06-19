"""Read a run's ``InstructionLedger`` tape into keyed ``TradeFillRecord`` rows.

The determinism stage (``daily_determinism_stage``) reconciles a paper run's
ledger against a batch rerun's ledger trade-by-trade. Each run's
``RunManifest.ledger_root`` is a ``gs://`` prefix holding the four ledgers
(``ledger_type=instruction|passive|position|pricing``). Only the
``ledger_type=instruction`` rows with ``event_type=trade`` are fills; this
reader lists + reads those JSONL objects and parses each row into a
``TradeFillRecord`` (the keyed unit ``reconcile_day`` matches on).

Mapping ``LedgerRow`` (``event_type=trade``) → ``TradeFillRecord``:
  - ``trade_id``      → ``trade_key`` (the match key; built via ``make_trade_key``)
  - ``venue`` + ``asset_class`` + ``asset_symbol`` → ``instrument_key``
    (``VENUE:INSTRUMENT_TYPE:SYMBOL``)
  - ``direction``     → ``side`` (Ledger ``Direction`` value); when absent the
    sign of ``delta`` disambiguates BUY/SELL
  - ``delta``         → ``qty`` (absolute value; the sign is carried by ``side``)
  - ``price``         → ``fill_price``
  - ``fees_in_quote`` → ``fees_in_quote``
  - ``timestamp_utc`` → ``tick_timestamp``

The ledger is read as newline-delimited JSON (the same transport the existing
recon stages use for GCS event archives) — no parquet engine dependency.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast
from urllib.parse import urlparse

from unified_api_contracts.internal import FillModel, TradeFillRecord
from unified_trading_library import get_storage_client

logger = logging.getLogger(__name__)

# Instruction-ledger trade rows carry event_type="trade" (UAC EventType.TRADE).
_TRADE_EVENT_TYPE = "trade"
_INSTRUCTION_LEDGER_KEY = "ledger_type=instruction"
# ledger_root is an already-resolved GCS URI (RunManifest.ledger_root) — this
# module only PARSES it, never composes a bucket name (so no resolve_bucket_name).
_GS_SCHEME = "gs"


class LedgerRootError(ValueError):
    """Raised when a ``ledger_root`` is not a parseable ``gs://`` prefix."""


def _parse_gs_root(ledger_root: str) -> tuple[str, str]:
    """Split a ``gs://bucket/prefix`` root into ``(bucket, prefix)``.

    Raises:
        LedgerRootError: ``ledger_root`` is not a ``gs://`` URI.
    """
    parsed = urlparse(ledger_root)
    if parsed.scheme != _GS_SCHEME or not parsed.netloc:
        raise LedgerRootError(f"ledger_root must be a {_GS_SCHEME}:// URI, got {ledger_root!r}")
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix = f"{prefix}/"
    return bucket, prefix


def _side_from_row(row: dict[str, object], qty_delta: Decimal) -> str:
    """Resolve the trade side from ``direction`` (preferred) or ``delta`` sign."""
    direction = row.get("direction")
    if isinstance(direction, str) and direction:
        return direction
    # No explicit direction → the signed delta carries it.
    return "SELL" if qty_delta < 0 else "BUY"


def _instrument_key_from_row(row: dict[str, object]) -> str:
    """Resolve ``VENUE:INSTRUMENT_TYPE:SYMBOL`` from ledger row fields.

    Prefers an explicit ``instrument_key`` the writer stamped (lossless — preserves
    the exact casing the engine emitted); falls back to reconstructing from
    ``venue`` / ``asset_class`` / ``asset_symbol`` for legacy rows that predate the
    stamped key. The explicit-key preference keeps the determinism proof's
    per-instrument rollups byte-identical (the reconstructed ``asset_class`` enum
    value is lower-case, so a reconstructed key can differ in case from the
    engine's original ``VENUE:PERP:SYMBOL``).
    """
    explicit = row.get("instrument_key")
    if isinstance(explicit, str) and explicit:
        return explicit
    venue = str(row.get("venue", ""))
    instrument_type = str(row.get("asset_class", row.get("instrument_type", "")))
    symbol = str(row.get("asset_symbol", row.get("underlying", "")))
    return f"{venue}:{instrument_type}:{symbol}"


def _parse_decimal(value: object, *, field: str, trade_id: str) -> Decimal:
    """Parse a ledger numeric field into ``Decimal`` (string/number tolerant)."""
    if value is None:
        raise ValueError(f"trade row {trade_id!r} missing required field {field!r}")
    return Decimal(str(value))


def _row_to_fill(row: dict[str, object]) -> TradeFillRecord:
    """Convert a single ``event_type=trade`` ledger row dict to ``TradeFillRecord``."""
    trade_id = str(row.get("trade_id", ""))
    if not trade_id:
        raise ValueError("instruction-ledger trade row missing trade_id (the trade_key)")

    qty_delta = _parse_decimal(row.get("delta"), field="delta", trade_id=trade_id)
    price = _parse_decimal(row.get("price"), field="price", trade_id=trade_id)
    fees = _parse_decimal(row.get("fees_in_quote", "0"), field="fees_in_quote", trade_id=trade_id)

    ts_raw = row.get("timestamp_utc")
    if not isinstance(ts_raw, str):
        raise ValueError(f"trade row {trade_id!r} missing/invalid timestamp_utc")
    tick_timestamp = datetime.fromisoformat(ts_raw)
    if tick_timestamp.tzinfo is None:
        tick_timestamp = tick_timestamp.replace(tzinfo=UTC)

    fill_model_raw = str(row.get("fill_model", FillModel.BENCHMARK.value))
    fill_model = FillModel(fill_model_raw) if fill_model_raw in FillModel.__members__.values() else FillModel.BENCHMARK

    strategy_instruction_id = str(row.get("strategy_instruction_id", trade_id))

    correlation_id_raw = row.get("correlation_id")
    client_order_id_raw = row.get("client_order_id")

    return TradeFillRecord(
        trade_key=trade_id,
        instrument_key=_instrument_key_from_row(row),
        strategy_instruction_id=strategy_instruction_id,
        tick_timestamp=tick_timestamp,
        venue=str(row.get("venue", "")),
        side=_side_from_row(row, qty_delta),
        qty=abs(qty_delta),
        fill_price=price,
        fees_in_quote=fees,
        fill_model=fill_model,
        correlation_id=correlation_id_raw if isinstance(correlation_id_raw, str) else None,
        client_order_id=client_order_id_raw if isinstance(client_order_id_raw, str) else None,
    )


def load_instruction_ledger_fills(ledger_root: str) -> list[TradeFillRecord]:
    """Load all ``event_type=trade`` rows from a run's instruction ledger.

    Lists the ``ledger_type=instruction`` JSONL objects under ``ledger_root``,
    parses each line into a ledger-row dict, keeps only ``event_type=trade``
    rows, and maps them to keyed ``TradeFillRecord`` fills.

    Args:
        ledger_root: ``gs://`` prefix of the four ledgers for the run
            (``RunManifest.ledger_root``).

    Returns:
        The run's trade fills, one per instruction-ledger trade row.

    Raises:
        LedgerRootError: ``ledger_root`` is not a ``gs://`` URI.
        ValueError: A trade row is missing a required field.
    """
    bucket_name, root_prefix = _parse_gs_root(ledger_root)
    prefix = f"{root_prefix}{_INSTRUCTION_LEDGER_KEY}/"

    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    if not blobs:
        logger.warning("[ledger-reader] No instruction-ledger objects at gs://%s/%s", bucket_name, prefix)
        return []

    fills: list[TradeFillRecord] = []
    for blob in blobs:
        raw = blob.download_as_text()
        for line in raw.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            row = cast(dict[str, object], json.loads(stripped))
            if str(row.get("event_type", "")) != _TRADE_EVENT_TYPE:
                continue
            fills.append(_row_to_fill(row))

    logger.info("[ledger-reader] Loaded %d trade fills from gs://%s/%s", len(fills), bucket_name, prefix)
    return fills
