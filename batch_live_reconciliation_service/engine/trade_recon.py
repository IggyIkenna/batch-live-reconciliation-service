"""Trade-by-trade ``reconcile_week`` determinism harness (Phase 4 P4.1).

Implements the determinism PROOF for paper⟷batch and the execution-alpha
measurement for live⟷paper, per the codex SSOT
``codex/09-strategy/operational/paper-batch-live-reconciliation.md`` §4.5 + §6.

The match key is ``trade_key``. Two runs over the same pinned input snapshot and
the same simulated fill model MUST produce trade-for-trade identical fills
(``is_deterministic`` with ``ε = 0``); ANY non-zero diff on a DETERMINISM verdict
is a BUG classified into one of ``{NON_DETERMINISM, INPUT_CAPTURE_GAP,
FILL_MODEL_DRIFT}`` — never "within tolerance". For EXECUTION / COMPOSITE
verdicts the per-trade ``fill_price_delta_bps`` IS the execution-alpha measurement
(expected non-zero), so ``is_deterministic`` is ``False`` and ``determinism_bug_class``
is ``NONE``.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal

from unified_api_contracts.internal import (
    DeterminismBugClass,
    ReconVerdictType,
    TradeDeviation,
    TradeFillRecord,
    WeeklyReconReport,
)

_BPS = Decimal(10000)


def _index_by_trade_key(records: Sequence[TradeFillRecord], side_label: str) -> dict[str, TradeFillRecord]:
    """Index a record list by ``trade_key``; a duplicate within one side is a data error."""
    indexed: dict[str, TradeFillRecord] = {}
    for record in records:
        if record.trade_key in indexed:
            raise ValueError(
                f"Duplicate trade_key {record.trade_key!r} in run {side_label} — "
                "trade_key must be unique within a single run"
            )
        indexed[record.trade_key] = record
    return indexed


def _matched_deviation(a: TradeFillRecord, b: TradeFillRecord) -> TradeDeviation:
    """Build the per-trade deviation for a key present in BOTH runs."""
    fill_price_delta_bps = ((b.fill_price - a.fill_price) / a.fill_price) * _BPS if a.fill_price != 0 else Decimal(0)
    timing_delta_ms = (b.tick_timestamp - a.tick_timestamp).total_seconds() * 1000.0
    return TradeDeviation(
        trade_key=a.trade_key,
        instrument_key=a.instrument_key,
        venue=a.venue,
        side_match=a.side == b.side,
        qty_delta=b.qty - a.qty,
        fill_price_delta_bps=fill_price_delta_bps,
        fees_delta=b.fees_in_quote - a.fees_in_quote,
        timing_delta_ms=timing_delta_ms,
        present_in_a=True,
        present_in_b=True,
    )


def _unmatched_deviation(record: TradeFillRecord, *, present_in_a: bool) -> TradeDeviation:
    """Build a zero-delta deviation for a key present in only ONE run."""
    return TradeDeviation(
        trade_key=record.trade_key,
        instrument_key=record.instrument_key,
        venue=record.venue,
        side_match=False,
        qty_delta=Decimal(0),
        fill_price_delta_bps=Decimal(0),
        fees_delta=Decimal(0),
        timing_delta_ms=0.0,
        present_in_a=present_in_a,
        present_in_b=not present_in_a,
    )


def _percentile_nearest_rank(values: list[Decimal], pct: float) -> Decimal:
    """Nearest-rank percentile over a sorted-on-input list of Decimals."""
    if not values:
        return Decimal(0)
    ordered = sorted(values)
    # nearest-rank: rank = ceil(pct/100 * n), 1-indexed.
    rank = max(1, math.ceil((pct / 100.0) * len(ordered)))
    return ordered[min(rank, len(ordered)) - 1]


def _classify_determinism(
    *,
    unmatched_a: int,
    unmatched_b: int,
    matched_devs: list[TradeDeviation],
) -> tuple[bool, DeterminismBugClass]:
    """Compute ``(is_deterministic, determinism_bug_class)`` for a DETERMINISM verdict."""
    fills_identical = all(
        d.side_match and d.qty_delta == 0 and d.fill_price_delta_bps == 0 and d.fees_delta == 0 for d in matched_devs
    )
    is_deterministic = unmatched_a == 0 and unmatched_b == 0 and fills_identical
    if is_deterministic:
        return True, DeterminismBugClass.NONE
    if unmatched_a > 0 or unmatched_b > 0:
        # Different trade SETS ⇒ the runs saw different inputs.
        return False, DeterminismBugClass.INPUT_CAPTURE_GAP
    if any(d.fill_price_delta_bps != 0 or d.fees_delta != 0 for d in matched_devs):
        # Same trades, different fills ⇒ fill-model drift.
        return False, DeterminismBugClass.FILL_MODEL_DRIFT
    # Same trades, same fills, but side/qty differ ⇒ the decision path is non-deterministic.
    return False, DeterminismBugClass.NON_DETERMINISM


def reconcile_week(
    run_a_id: str,
    run_b_id: str,
    records_a: Sequence[TradeFillRecord],
    records_b: Sequence[TradeFillRecord],
    verdict_type: ReconVerdictType,
    window_start: datetime,
    window_end: datetime,
) -> WeeklyReconReport:
    """Reconcile two runs trade-by-trade over a window.

    Matches on ``trade_key``. For a DETERMINISM verdict the result is the binary
    determinism proof (ε = 0 expectation, bug-classified on any diff); for an
    EXECUTION / COMPOSITE verdict the fill-price rollups are the execution-alpha
    measurement (``is_deterministic`` False, bug class NONE).

    Args:
        run_a_id: Identifier of run A (the reference, e.g. paper).
        run_b_id: Identifier of run B (the comparand, e.g. batch / live).
        records_a: Trade fills from run A.
        records_b: Trade fills from run B.
        verdict_type: DETERMINISM (paper⟷batch) / EXECUTION (live⟷paper) / COMPOSITE.
        window_start: Reconciliation window start (UTC).
        window_end: Reconciliation window end (UTC).

    Returns:
        A populated ``WeeklyReconReport``.

    Raises:
        ValueError: A duplicate ``trade_key`` within a single run's records.
    """
    index_a = _index_by_trade_key(records_a, run_a_id)
    index_b = _index_by_trade_key(records_b, run_b_id)

    keys_a = set(index_a)
    keys_b = set(index_b)
    matched_keys = keys_a & keys_b
    only_a = keys_a - keys_b
    only_b = keys_b - keys_a

    matched_devs = [_matched_deviation(index_a[k], index_b[k]) for k in matched_keys]

    deviations: list[TradeDeviation] = list(matched_devs)
    deviations.extend(_unmatched_deviation(index_a[k], present_in_a=True) for k in only_a)
    deviations.extend(_unmatched_deviation(index_b[k], present_in_a=False) for k in only_b)

    # Always populate the alpha rollups over matched trades (abs fill-price delta).
    abs_deltas = [abs(d.fill_price_delta_bps) for d in matched_devs]
    mean_fill_price_delta_bps = sum(abs_deltas, Decimal(0)) / Decimal(len(abs_deltas)) if abs_deltas else Decimal(0)
    p99_fill_price_delta_bps = _percentile_nearest_rank(abs_deltas, 99.0)

    if verdict_type == ReconVerdictType.DETERMINISM:
        is_deterministic, determinism_bug_class = _classify_determinism(
            unmatched_a=len(only_a),
            unmatched_b=len(only_b),
            matched_devs=matched_devs,
        )
    else:
        # EXECUTION / COMPOSITE: divergence is the measurement, not a bug.
        is_deterministic = False
        determinism_bug_class = DeterminismBugClass.NONE

    # Deterministic output ordering for reproducibility.
    deviations.sort(key=lambda d: d.trade_key)

    return WeeklyReconReport(
        verdict_type=verdict_type,
        run_a_id=run_a_id,
        run_b_id=run_b_id,
        window_start=window_start,
        window_end=window_end,
        total_trades_a=len(records_a),
        total_trades_b=len(records_b),
        matched=len(matched_keys),
        unmatched_a=len(only_a),
        unmatched_b=len(only_b),
        deviations=tuple(deviations),
        is_deterministic=is_deterministic,
        determinism_bug_class=determinism_bug_class,
        mean_fill_price_delta_bps=mean_fill_price_delta_bps,
        p99_fill_price_delta_bps=p99_fill_price_delta_bps,
    )
