"""
Unit tests for Stage 3 Execution reconciliation pure logic functions.

Covers _compute_metrics and _check_deviations without any I/O.
"""

from __future__ import annotations

import pytest

from batch_live_reconciliation_service.models.deviation_thresholds import EXECUTION_THRESHOLDS
from batch_live_reconciliation_service.models.recon_report import ReconStage
from batch_live_reconciliation_service.stages.stage3_execution_recon import (
    _check_deviations,
    _compute_metrics,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fill(
    fill_price: float = 100.0,
    filled_qty: float = 1.0,
    slippage_bps: float = 2.0,
    latency_ms: float | None = 50.0,
) -> dict[str, object]:
    ev: dict[str, object] = {
        "event_type": "FILL",
        "fill_price": fill_price,
        "filled_qty": filled_qty,
        "slippage_bps": slippage_bps,
    }
    if latency_ms is not None:
        ev["latency_ms"] = latency_ms
    return ev


def _order(algo_used: str = "VWAP", algo_configured: str = "VWAP") -> dict[str, object]:
    return {
        "event_type": "ORDER_SUBMITTED",
        "algo_used": algo_used,
        "algo_configured": algo_configured,
    }


# ---------------------------------------------------------------------------
# _compute_metrics
# ---------------------------------------------------------------------------


def test_compute_metrics_empty_live_returns_safe_defaults() -> None:
    result = _compute_metrics([_fill()], [])
    assert result["alpha_pnl_gap"] == 0.0
    assert result["fill_rate_delta"] == 0.0
    assert result["slippage_delta_bps"] == 0.0
    assert result["algo_selection_accuracy"] == 1.0
    assert result["order_latency_p99_ms"] == 0.0
    assert result["live_fill_count"] == 0.0


def test_compute_metrics_empty_both() -> None:
    result = _compute_metrics([], [])
    assert result["algo_selection_accuracy"] == 1.0
    assert result["alpha_pnl_gap"] == 0.0


def test_compute_metrics_perfect_match_no_deviations() -> None:
    fill = _fill(100.0, 1.0, 2.0, 50.0)
    order = _order("VWAP", "VWAP")
    result = _compute_metrics([fill, order], [fill, order])
    assert result["alpha_pnl_gap"] == pytest.approx(0.0)
    assert result["fill_rate_delta"] == pytest.approx(0.0)
    assert result["slippage_delta_bps"] == pytest.approx(0.0)
    assert result["algo_selection_accuracy"] == pytest.approx(1.0)


def test_compute_metrics_alpha_pnl_gap() -> None:
    # live: 100 * 2 = 200; batch: 100 * 1 = 100 → gap = |200-100|/200 = 0.5
    live_fill = _fill(100.0, 2.0)
    batch_fill = _fill(100.0, 1.0)
    result = _compute_metrics([batch_fill], [live_fill])
    assert result["alpha_pnl_gap"] == pytest.approx(0.5)


def test_compute_metrics_fill_rate_delta() -> None:
    # live: 1 fill / 2 submitted = 0.5; batch: 1 fill / 1 submitted = 1.0 → delta = 0.5
    live_events: list[dict[str, object]] = [_fill(), _order(), _order()]
    batch_events: list[dict[str, object]] = [_fill(), _order()]
    result = _compute_metrics(batch_events, live_events)
    assert result["fill_rate_delta"] == pytest.approx(0.5)


def test_compute_metrics_slippage_delta() -> None:
    live_fill = _fill(slippage_bps=10.0)
    batch_fill = _fill(slippage_bps=2.0)
    result = _compute_metrics([batch_fill], [live_fill])
    assert result["slippage_delta_bps"] == pytest.approx(8.0)


def test_compute_metrics_algo_accuracy_all_correct() -> None:
    orders = [_order("VWAP", "VWAP"), _order("TWAP", "TWAP")]
    result = _compute_metrics(orders, orders)
    assert result["algo_selection_accuracy"] == pytest.approx(1.0)


def test_compute_metrics_algo_accuracy_all_wrong() -> None:
    live_orders = [_order("VWAP", "TWAP"), _order("VWAP", "TWAP")]
    result = _compute_metrics(live_orders, live_orders)
    assert result["algo_selection_accuracy"] == pytest.approx(0.0)


def test_compute_metrics_algo_accuracy_partial() -> None:
    live_orders = [_order("VWAP", "VWAP"), _order("VWAP", "TWAP")]
    result = _compute_metrics(live_orders, live_orders)
    assert result["algo_selection_accuracy"] == pytest.approx(0.5)


def test_compute_metrics_p99_latency_single_fill() -> None:
    fill = _fill(latency_ms=300.0)
    result = _compute_metrics([fill], [fill])
    assert result["order_latency_p99_ms"] == pytest.approx(300.0)


def test_compute_metrics_p99_latency_no_latency_field() -> None:
    fill = _fill(latency_ms=None)
    result = _compute_metrics([fill], [fill])
    assert result["order_latency_p99_ms"] == pytest.approx(0.0)


def test_compute_metrics_fill_counts_reported() -> None:
    live_fills = [_fill(), _fill()]
    batch_fills = [_fill()]
    result = _compute_metrics(batch_fills, live_fills)
    assert result["live_fill_count"] == 2.0
    assert result["batch_fill_count"] == 1.0


# ---------------------------------------------------------------------------
# _check_deviations
# ---------------------------------------------------------------------------


def _passing_metrics() -> dict[str, float]:
    return {
        "alpha_pnl_gap": 0.0,
        "fill_rate_delta": 0.0,
        "slippage_delta_bps": 0.0,
        "algo_selection_accuracy": EXECUTION_THRESHOLDS.algo_selection_accuracy_min,
        "order_latency_p99_ms": 0.0,
    }


def test_check_deviations_no_breaches() -> None:
    devs = _check_deviations(_passing_metrics())
    assert devs == []


def test_check_deviations_alpha_pnl_gap_breach() -> None:
    metrics = _passing_metrics()
    metrics["alpha_pnl_gap"] = 0.05  # > 0.01
    devs = _check_deviations(metrics)
    names = [d.metric_name for d in devs]
    assert "alpha_pnl_gap" in names
    dev = next(d for d in devs if d.metric_name == "alpha_pnl_gap")
    assert dev.direction == "above"
    assert dev.stage == ReconStage.EXECUTION_RECON


def test_check_deviations_fill_rate_delta_breach() -> None:
    metrics = _passing_metrics()
    metrics["fill_rate_delta"] = 0.20  # > 0.05
    devs = _check_deviations(metrics)
    names = [d.metric_name for d in devs]
    assert "fill_rate_delta" in names


def test_check_deviations_slippage_breach() -> None:
    metrics = _passing_metrics()
    metrics["slippage_delta_bps"] = 15.0  # > 10.0
    devs = _check_deviations(metrics)
    names = [d.metric_name for d in devs]
    assert "slippage_delta_bps" in names


def test_check_deviations_algo_accuracy_breach() -> None:
    metrics = _passing_metrics()
    metrics["algo_selection_accuracy"] = 0.95  # < 0.99
    devs = _check_deviations(metrics)
    names = [d.metric_name for d in devs]
    assert "algo_selection_accuracy" in names
    dev = next(d for d in devs if d.metric_name == "algo_selection_accuracy")
    assert dev.direction == "below"


def test_check_deviations_latency_breach() -> None:
    metrics = _passing_metrics()
    metrics["order_latency_p99_ms"] = 700.0  # > 600.0
    devs = _check_deviations(metrics)
    names = [d.metric_name for d in devs]
    assert "order_latency_p99_ms" in names


def test_check_deviations_all_five_breached() -> None:
    metrics = {
        "alpha_pnl_gap": 1.0,
        "fill_rate_delta": 1.0,
        "slippage_delta_bps": 100.0,
        "algo_selection_accuracy": 0.0,
        "order_latency_p99_ms": 10000.0,
    }
    devs = _check_deviations(metrics)
    assert len(devs) == 5


def test_check_deviations_descriptions_non_empty() -> None:
    metrics = _passing_metrics()
    metrics["alpha_pnl_gap"] = 0.05
    devs = _check_deviations(metrics)
    assert all(len(d.description) > 0 for d in devs)
