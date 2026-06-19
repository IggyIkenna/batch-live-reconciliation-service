"""Unit tests for the trade-by-trade ``reconcile_week`` determinism harness (P4.1).

Credential-free. Exercises the verdict semantics from the codex SSOT
``paper-batch-live-reconciliation.md`` §4.5 + §6: the paper⟷batch DETERMINISM
proof (ε = 0, bug-classified on any diff) and the live⟷paper EXECUTION
alpha measurement.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from unified_api_contracts.internal import (
    DeterminismBugClass,
    FillModel,
    ReconVerdictType,
    TradeFillRecord,
)

from batch_live_reconciliation_service.engine.trade_recon import reconcile_week

_WINDOW_START = datetime(2026, 6, 19, 0, 0, 0, tzinfo=UTC)
_WINDOW_END = datetime(2026, 6, 26, 0, 0, 0, tzinfo=UTC)
_TICK = datetime(2026, 6, 19, 12, 0, 0, tzinfo=UTC)


def _fill(
    trade_key: str,
    *,
    side: str = "BUY",
    qty: str = "1.0",
    fill_price: str = "100.0",
    fees: str = "0.10",
    fill_model: FillModel = FillModel.BENCHMARK,
    tick: datetime = _TICK,
) -> TradeFillRecord:
    return TradeFillRecord(
        trade_key=trade_key,
        instrument_key="BINANCE:PERP:BTC-USDT",
        strategy_instruction_id=f"instr-{trade_key}",
        tick_timestamp=tick,
        venue="binance",
        side=side,
        qty=Decimal(qty),
        fill_price=Decimal(fill_price),
        fees_in_quote=Decimal(fees),
        fill_model=fill_model,
    )


def test_identical_runs_are_deterministic() -> None:
    """(a) Two identical runs → is_deterministic True, bug NONE, ε = 0."""
    records = [_fill("t1"), _fill("t2", fill_price="200.0")]
    report = reconcile_week(
        "paper-1",
        "batch-1",
        records,
        list(records),
        ReconVerdictType.DETERMINISM,
        _WINDOW_START,
        _WINDOW_END,
    )

    assert report.is_deterministic is True
    assert report.determinism_bug_class == DeterminismBugClass.NONE
    assert report.matched == 2
    assert report.unmatched_a == 0
    assert report.unmatched_b == 0
    assert report.mean_fill_price_delta_bps == Decimal(0)
    assert report.p99_fill_price_delta_bps == Decimal(0)
    assert all(d.qty_delta == 0 and d.fill_price_delta_bps == 0 for d in report.deviations)
    # Deterministic ordering by trade_key.
    assert [d.trade_key for d in report.deviations] == ["t1", "t2"]


def test_price_diff_is_fill_model_drift() -> None:
    """(b) One matched trade, 12.5 bps price diff → FILL_MODEL_DRIFT, not deterministic, mean ≈ 12.5."""
    # 100.0 -> 100.125 == +12.5 bps.
    a = [_fill("t1", fill_price="100.0")]
    b = [_fill("t1", fill_price="100.125")]
    report = reconcile_week("paper-1", "batch-1", a, b, ReconVerdictType.DETERMINISM, _WINDOW_START, _WINDOW_END)

    assert report.is_deterministic is False
    assert report.determinism_bug_class == DeterminismBugClass.FILL_MODEL_DRIFT
    assert report.matched == 1
    assert report.mean_fill_price_delta_bps == pytest.approx(Decimal("12.5"))
    assert report.p99_fill_price_delta_bps == pytest.approx(Decimal("12.5"))
    (dev,) = report.deviations
    assert dev.fill_price_delta_bps == pytest.approx(Decimal("12.5"))
    assert dev.present_in_a is True
    assert dev.present_in_b is True


def test_unmatched_trade_is_input_capture_gap() -> None:
    """(c) Trade present in A but not B → unmatched_a=1, INPUT_CAPTURE_GAP, not deterministic."""
    a = [_fill("t1"), _fill("t2")]
    b = [_fill("t1")]
    report = reconcile_week("paper-1", "batch-1", a, b, ReconVerdictType.DETERMINISM, _WINDOW_START, _WINDOW_END)

    assert report.is_deterministic is False
    assert report.determinism_bug_class == DeterminismBugClass.INPUT_CAPTURE_GAP
    assert report.matched == 1
    assert report.unmatched_a == 1
    assert report.unmatched_b == 0
    # The unmatched trade still emits a deviation with present_in_b False.
    unmatched = next(d for d in report.deviations if d.trade_key == "t2")
    assert unmatched.present_in_a is True
    assert unmatched.present_in_b is False
    assert unmatched.qty_delta == Decimal(0)
    assert unmatched.side_match is False


def test_side_flip_is_non_determinism() -> None:
    """(d) Same trades + identical fills but a side flip → NON_DETERMINISM."""
    a = [_fill("t1", side="BUY")]
    b = [_fill("t1", side="SELL")]
    report = reconcile_week("paper-1", "batch-1", a, b, ReconVerdictType.DETERMINISM, _WINDOW_START, _WINDOW_END)

    assert report.is_deterministic is False
    assert report.determinism_bug_class == DeterminismBugClass.NON_DETERMINISM
    assert report.matched == 1
    (dev,) = report.deviations
    assert dev.side_match is False
    assert dev.fill_price_delta_bps == Decimal(0)
    assert dev.fees_delta == Decimal(0)
    assert dev.qty_delta == Decimal(0)


def test_execution_verdict_populates_alpha_rollups() -> None:
    """(e) EXECUTION verdict over real fills → not deterministic, alpha rollups populated, bug NONE."""
    a = [
        _fill("t1", fill_price="100.0", fill_model=FillModel.BENCHMARK),
        _fill("t2", fill_price="200.0", fill_model=FillModel.BENCHMARK),
    ]
    b = [
        _fill("t1", fill_price="100.05", fill_model=FillModel.LIVE_VENUE),  # +5 bps
        _fill("t2", fill_price="200.30", fill_model=FillModel.LIVE_VENUE),  # +15 bps
    ]
    report = reconcile_week("paper-1", "live-1", a, b, ReconVerdictType.EXECUTION, _WINDOW_START, _WINDOW_END)

    assert report.is_deterministic is False
    assert report.determinism_bug_class == DeterminismBugClass.NONE
    assert report.matched == 2
    # mean(|5|, |15|) = 10 bps.
    assert report.mean_fill_price_delta_bps == pytest.approx(Decimal("10"))
    # nearest-rank p99 of [5, 15] = 15.
    assert report.p99_fill_price_delta_bps == pytest.approx(Decimal("15"))


def test_composite_verdict_is_not_a_bug() -> None:
    """COMPOSITE verdict also reports execution alpha (bug NONE, not deterministic)."""
    a = [_fill("t1", fill_price="100.0")]
    b = [_fill("t1", fill_price="100.10")]  # +10 bps
    report = reconcile_week("paper-1", "live-1", a, b, ReconVerdictType.COMPOSITE, _WINDOW_START, _WINDOW_END)

    assert report.is_deterministic is False
    assert report.determinism_bug_class == DeterminismBugClass.NONE
    assert report.mean_fill_price_delta_bps == pytest.approx(Decimal("10"))


def test_duplicate_trade_key_raises() -> None:
    """(f) Duplicate trade_key within a single run raises ValueError."""
    dupes = [_fill("t1"), _fill("t1", fill_price="105.0")]
    with pytest.raises(ValueError, match="Duplicate trade_key"):
        reconcile_week(
            "paper-1", "batch-1", dupes, [_fill("t1")], ReconVerdictType.DETERMINISM, _WINDOW_START, _WINDOW_END
        )


def test_empty_inputs_are_trivially_deterministic() -> None:
    """(g) Empty inputs → deterministic True (0 trades match trivially)."""
    report = reconcile_week("paper-1", "batch-1", [], [], ReconVerdictType.DETERMINISM, _WINDOW_START, _WINDOW_END)

    assert report.is_deterministic is True
    assert report.determinism_bug_class == DeterminismBugClass.NONE
    assert report.matched == 0
    assert report.total_trades_a == 0
    assert report.total_trades_b == 0
    assert report.deviations == ()


def test_timing_delta_ms_is_signed_milliseconds() -> None:
    """timing_delta_ms = (b - a) in milliseconds; fees_delta is additive."""
    a = [_fill("t1", fees="0.10", tick=_TICK)]
    b = [_fill("t1", fees="0.25", tick=_TICK + timedelta(milliseconds=250))]
    report = reconcile_week("paper-1", "live-1", a, b, ReconVerdictType.EXECUTION, _WINDOW_START, _WINDOW_END)

    (dev,) = report.deviations
    assert dev.timing_delta_ms == pytest.approx(250.0)
    assert dev.fees_delta == Decimal("0.15")
