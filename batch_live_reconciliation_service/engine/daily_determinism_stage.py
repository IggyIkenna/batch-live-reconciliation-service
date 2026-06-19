"""Daily T+1 determinism recon stage — wires ``reconcile_day`` into the pipeline.

This is the WRITE side of the determinism spine (codex
``paper-batch-live-reconciliation.md`` §4.5/§5/§6). It runs alongside the
date-level orchestrator stages 3b/3c but operates at TRADE grain:

  1. Load each run's keyed ``TradeFillRecord`` ledger from its ``ledger_root``
     (paper run = run A, batch rerun = run B), via ``load_instruction_ledger_fills``.
  2. Call ``reconcile_day(..., ReconVerdictType.DETERMINISM, day_start, day_end)``
     — the trade-by-trade ε=0 proof (any diff is a bug, classified).
  3. Roll the per-trade ``TradeDeviation``s up per ``(venue, instrument, strategy)``
     and per ``PnLFactor`` into the orchestrator's aggregate ``DeviationRecord``s.

Cadence is DAILY T+1 — one ``DailyReconReport`` per trading day (a week = 7).
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime
from decimal import Decimal

from unified_api_contracts.internal import (
    DailyReconReport,
    ReconVerdictType,
    RunManifest,
    TradeDeviation,
)
from unified_api_contracts.internal.reconciliation import ReconciliationDimension
from unified_api_contracts.internal.risk import PnLFactor

from batch_live_reconciliation_service.engine.ledger_reader import load_instruction_ledger_fills
from batch_live_reconciliation_service.engine.trade_recon import reconcile_day
from batch_live_reconciliation_service.models.recon_report import DeviationRecord, ReconStage

logger = logging.getLogger(__name__)

_BPS = Decimal(10000)


def _venue_of(key: str) -> str:
    """Extract the venue segment of a ``VENUE:INSTRUMENT_TYPE:SYMBOL`` key."""
    return key.split(":", 1)[0] if ":" in key else key


def _pnl_factor_for(deviation: TradeDeviation) -> PnLFactor:
    """Classify a per-trade deviation into a ``PnLFactor`` bucket.

    Per-trade attribution is not carried on ``TradeFillRecord``, so the factor
    is derived from WHAT diverged: a fee delta is ``FEES``, a fill-price delta
    is ``SLIPPAGE`` (execution-alpha axis), otherwise ``RESIDUAL`` (a structural
    side/qty/presence mismatch with no single economic driver).
    """
    if deviation.fees_delta != 0:
        return PnLFactor.FEES
    if deviation.fill_price_delta_bps != 0:
        return PnLFactor.SLIPPAGE
    return PnLFactor.RESIDUAL


def _is_divergent(dev: TradeDeviation) -> bool:
    """True when a per-trade deviation represents a REAL diff (not an ε=0 match).

    A matched trade with ε=0 across (side, qty, fill_price, fees) is reported by
    ``reconcile_day`` as a zero-delta ``TradeDeviation`` — it is NOT a deviation.
    A genuine divergence is a presence mismatch, a side mismatch, or any non-zero
    qty/price/fee delta.
    """
    if not (dev.present_in_a and dev.present_in_b):
        return True
    if not dev.side_match:
        return True
    return dev.qty_delta != 0 or dev.fill_price_delta_bps != 0 or dev.fees_delta != 0


def _rollup_deviations(report: DailyReconReport) -> list[DeviationRecord]:
    """Aggregate per-trade deviations per ``(venue, instrument)`` and per ``PnLFactor``.

    Produces orchestrator-shaped ``DeviationRecord``s carrying ``instrument_id``
    so the consolidated T+1 report and deployment-UI drilldown can attribute the
    determinism breach to a venue/instrument and an economic driver.
    """
    by_instrument: dict[str, Decimal] = defaultdict(lambda: Decimal(0))
    by_factor: dict[PnLFactor, Decimal] = defaultdict(lambda: Decimal(0))

    for dev in report.deviations:
        if not _is_divergent(dev):
            continue  # ε=0 matched trade — not a deviation; skip the rollup.
        abs_bps = abs(dev.fill_price_delta_bps)
        by_instrument[dev.instrument_key] = max(by_instrument[dev.instrument_key], abs_bps)
        by_factor[_pnl_factor_for(dev)] += abs_bps

    records: list[DeviationRecord] = []

    # Per-(venue, instrument) rollup — keyed by instrument_key (venue is its prefix).
    for instrument_key in sorted(by_instrument):
        max_bps = by_instrument[instrument_key]
        venue = _venue_of(instrument_key)
        records.append(
            DeviationRecord.new(
                metric_name="determinism_fill_price_delta_bps",
                stage=ReconStage.BATCH_PAPER_RECON,
                actual_value=float(max_bps),
                threshold=0.0,  # DETERMINISM verdict: ε = 0 is the only passing value.
                direction="above",
                description=(
                    f"Determinism deviation on {instrument_key} (venue={venue}): "
                    f"max |fill_price_delta| {max_bps} bps (ε=0 expected)"
                ),
                dimension=ReconciliationDimension.FILLS,
                instrument_id=instrument_key,
            )
        )

    # Per-PnLFactor rollup — aggregate divergence magnitude per economic driver.
    for factor in sorted(by_factor, key=lambda f: f.value):
        total_bps = by_factor[factor]
        records.append(
            DeviationRecord.new(
                metric_name=f"determinism_factor_{factor.value.lower()}",
                stage=ReconStage.BATCH_PAPER_RECON,
                actual_value=float(total_bps),
                threshold=0.0,
                direction="above",
                description=(
                    f"Determinism deviation attributed to PnLFactor={factor.value}: "
                    f"aggregate {total_bps} bps across trades"
                ),
                dimension=ReconciliationDimension.FILLS,
            )
        )

    return records


def run_daily_determinism_stage(
    paper_manifest: RunManifest,
    batch_manifest: RunManifest,
    day_start: datetime,
    day_end: datetime,
) -> tuple[DailyReconReport, list[DeviationRecord]]:
    """Run the trade-by-trade DETERMINISM recon for one T+1 trading day.

    Loads the paper + batch instruction ledgers, runs ``reconcile_day`` with a
    ``DETERMINISM`` verdict (paper ≡ batch, ε=0), and rolls the per-trade
    deviations up per ``(venue, instrument)`` and per ``PnLFactor``.

    Args:
        paper_manifest: The paper run's ``RunManifest`` (run A, the reference).
        batch_manifest: The batch rerun's ``RunManifest`` (run B).
        day_start: Inclusive UTC start of the reconciled T+1 day.
        day_end: Exclusive UTC end (= ``day_start + 1 day``).

    Returns:
        ``(report, rollup_deviations)`` — the single ``DailyReconReport`` for the
        day plus the aggregate ``DeviationRecord``s for the orchestrator.
    """
    logger.info(
        "[daily-determinism] paper_run=%s batch_run=%s window=[%s, %s)",
        paper_manifest.run_id,
        batch_manifest.run_id,
        day_start.isoformat(),
        day_end.isoformat(),
    )

    records_a = load_instruction_ledger_fills(paper_manifest.ledger_root)
    records_b = load_instruction_ledger_fills(batch_manifest.ledger_root)

    report = reconcile_day(
        paper_manifest.run_id,
        batch_manifest.run_id,
        records_a,
        records_b,
        ReconVerdictType.DETERMINISM,
        day_start,
        day_end,
    )

    rollup = _rollup_deviations(report)

    logger.info(
        "[daily-determinism] verdict deterministic=%s bug=%s matched=%d unmatched_a=%d unmatched_b=%d rollups=%d",
        report.is_deterministic,
        report.determinism_bug_class.value,
        report.matched,
        report.unmatched_a,
        report.unmatched_b,
        len(rollup),
    )
    return report, rollup
