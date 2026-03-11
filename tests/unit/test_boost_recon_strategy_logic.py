"""
Unit tests for Stage 2 Strategy reconciliation pure logic functions.

Covers _compute_metrics and _check_deviations without any I/O.
"""

from __future__ import annotations

import pytest

from batch_live_reconciliation_service.models.deviation_thresholds import STRATEGY_THRESHOLDS
from batch_live_reconciliation_service.models.recon_report import ReconStage
from batch_live_reconciliation_service.stages.stage2_strategy_recon import (
    _check_deviations,
    _compute_metrics,
)

# ---------------------------------------------------------------------------
# _compute_metrics
# ---------------------------------------------------------------------------


def _make_instruction(instrument_id: str, side: str) -> dict[str, object]:
    return {"event_type": "INSTRUCTION", "instrument_id": instrument_id, "side": side}


def _make_position_snapshot(
    instrument_id: str, net_position: float, unrealized_pnl: float
) -> dict[str, object]:
    return {
        "event_type": "POSITION_SNAPSHOT",
        "instrument_id": instrument_id,
        "net_position": net_position,
        "unrealized_pnl": unrealized_pnl,
    }


def _make_risk_snapshot(var_1d: float) -> dict[str, object]:
    return {"event_type": "RISK_SNAPSHOT", "var_1d": var_1d}


def test_compute_metrics_empty_live() -> None:
    batch = [_make_instruction("BTC-USD", "BUY")]
    result = _compute_metrics(batch, [])
    assert result["instruction_alignment_pct"] == 0.0
    assert result["benchmark_pnl_delta"] == 0.0
    assert result["position_snapshot_delta"] == 0.0
    assert result["var_delta_pct"] == 0.0
    assert result["live_event_count"] == 0.0
    assert result["batch_event_count"] == 1.0


def test_compute_metrics_perfect_instruction_alignment() -> None:
    instr = [_make_instruction("BTC-USD", "BUY"), _make_instruction("ETH-USD", "SELL")]
    result = _compute_metrics(instr, instr)
    assert result["instruction_alignment_pct"] == pytest.approx(1.0)


def test_compute_metrics_partial_alignment() -> None:
    batch = [_make_instruction("BTC-USD", "BUY")]
    live = [
        _make_instruction("BTC-USD", "BUY"),
        _make_instruction("ETH-USD", "SELL"),
    ]
    result = _compute_metrics(batch, live)
    # 1 matched out of 2 live instructions
    assert result["instruction_alignment_pct"] == pytest.approx(0.5)


def test_compute_metrics_zero_alignment() -> None:
    batch = [_make_instruction("ETH-USD", "SELL")]
    live = [_make_instruction("BTC-USD", "BUY")]
    result = _compute_metrics(batch, live)
    assert result["instruction_alignment_pct"] == pytest.approx(0.0)


def test_compute_metrics_pnl_delta_zero_when_matching() -> None:
    snapshot = _make_position_snapshot("BTC-USD", 1.0, 100.0)
    result = _compute_metrics([snapshot], [snapshot])
    assert result["benchmark_pnl_delta"] == pytest.approx(0.0)


def test_compute_metrics_pnl_delta_nonzero() -> None:
    batch_snap = _make_position_snapshot("BTC-USD", 1.0, 80.0)
    live_snap = _make_position_snapshot("BTC-USD", 1.0, 100.0)
    result = _compute_metrics([batch_snap], [live_snap])
    # |80 - 100| / 100 = 0.2
    assert result["benchmark_pnl_delta"] == pytest.approx(0.2)


def test_compute_metrics_position_delta_zero_when_matching() -> None:
    snap = _make_position_snapshot("BTC-USD", 5.0, 0.0)
    result = _compute_metrics([snap], [snap])
    assert result["position_snapshot_delta"] == pytest.approx(0.0)


def test_compute_metrics_position_delta_nonzero() -> None:
    batch_snap = _make_position_snapshot("BTC-USD", 3.0, 0.0)
    live_snap = _make_position_snapshot("BTC-USD", 5.0, 0.0)
    result = _compute_metrics([batch_snap], [live_snap])
    assert result["position_snapshot_delta"] == pytest.approx(2.0)


def test_compute_metrics_position_delta_missing_instrument() -> None:
    # live has BTC, batch does not
    live_snap = _make_position_snapshot("BTC-USD", 10.0, 0.0)
    result = _compute_metrics([], [live_snap])
    assert result["position_snapshot_delta"] == pytest.approx(10.0)


def test_compute_metrics_var_delta_zero_when_matching() -> None:
    risk = _make_risk_snapshot(50000.0)
    result = _compute_metrics([risk], [risk])
    assert result["var_delta_pct"] == pytest.approx(0.0)


def test_compute_metrics_var_delta_nonzero() -> None:
    batch_risk = _make_risk_snapshot(40000.0)
    live_risk = _make_risk_snapshot(50000.0)
    result = _compute_metrics([batch_risk], [live_risk])
    # |40000 - 50000| / 50000 = 0.2
    assert result["var_delta_pct"] == pytest.approx(0.2)


def test_compute_metrics_no_instructions_alignment_zero() -> None:
    # No INSTRUCTION events in live — denominator is 1 (via max)
    snap = _make_position_snapshot("BTC-USD", 1.0, 0.0)
    result = _compute_metrics([snap], [snap])
    # 0 matched / max(0, 1) = 0
    assert result["instruction_alignment_pct"] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# _check_deviations
# ---------------------------------------------------------------------------


def _passing_metrics() -> dict[str, float]:
    return {
        "instruction_alignment_pct": STRATEGY_THRESHOLDS.instruction_alignment_pct_min,
        "benchmark_pnl_delta": 0.0,
        "position_snapshot_delta": 0.0,
        "var_delta_pct": 0.0,
    }


def test_check_deviations_no_breaches() -> None:
    devs = _check_deviations(_passing_metrics())
    assert devs == []


def test_check_deviations_instruction_alignment_breach() -> None:
    metrics = _passing_metrics()
    metrics["instruction_alignment_pct"] = 0.50  # < 0.85
    devs = _check_deviations(metrics)
    names = [d.metric_name for d in devs]
    assert "instruction_alignment_pct" in names
    dev = next(d for d in devs if d.metric_name == "instruction_alignment_pct")
    assert dev.direction == "below"
    assert dev.stage == ReconStage.STRATEGY_RECON


def test_check_deviations_pnl_delta_breach() -> None:
    metrics = _passing_metrics()
    metrics["benchmark_pnl_delta"] = 0.10  # > 0.02
    devs = _check_deviations(metrics)
    names = [d.metric_name for d in devs]
    assert "benchmark_pnl_delta" in names
    dev = next(d for d in devs if d.metric_name == "benchmark_pnl_delta")
    assert dev.direction == "above"


def test_check_deviations_position_delta_breach() -> None:
    metrics = _passing_metrics()
    metrics["position_snapshot_delta"] = 5.0  # > 1.0
    devs = _check_deviations(metrics)
    names = [d.metric_name for d in devs]
    assert "position_snapshot_delta" in names
    dev = next(d for d in devs if d.metric_name == "position_snapshot_delta")
    assert dev.direction == "above"
    assert dev.actual_value == pytest.approx(5.0)


def test_check_deviations_var_delta_breach() -> None:
    metrics = _passing_metrics()
    metrics["var_delta_pct"] = 0.25  # > 0.10
    devs = _check_deviations(metrics)
    names = [d.metric_name for d in devs]
    assert "var_delta_pct" in names
    dev = next(d for d in devs if d.metric_name == "var_delta_pct")
    assert dev.direction == "above"


def test_check_deviations_all_four_breached() -> None:
    metrics = {
        "instruction_alignment_pct": 0.0,
        "benchmark_pnl_delta": 1.0,
        "position_snapshot_delta": 100.0,
        "var_delta_pct": 1.0,
    }
    devs = _check_deviations(metrics)
    assert len(devs) == 4


def test_check_deviations_descriptions_are_non_empty() -> None:
    metrics = {
        "instruction_alignment_pct": 0.0,
        "benchmark_pnl_delta": 0.0,
        "position_snapshot_delta": 0.0,
        "var_delta_pct": 0.0,
    }
    devs = _check_deviations(metrics)
    assert len(devs) == 1
    assert devs[0].description != ""
