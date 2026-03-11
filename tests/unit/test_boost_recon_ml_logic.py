"""
Unit tests for Stage 1 ML reconciliation pure logic functions.

Covers _compute_metrics and _check_deviations without any I/O.
"""

from __future__ import annotations

import pytest

from batch_live_reconciliation_service.models.deviation_thresholds import ML_THRESHOLDS
from batch_live_reconciliation_service.models.recon_report import ReconStage
from batch_live_reconciliation_service.stages.stage1_ml_recon import (
    _check_deviations,
    _compute_metrics,
)


# ---------------------------------------------------------------------------
# _compute_metrics
# ---------------------------------------------------------------------------


def test_compute_metrics_empty_live_returns_zero_coverage() -> None:
    batch = [{"instrument_id": "BTC-USD", "timeframe": "1m", "signal_direction": 1, "magnitude": 0.5}]
    result = _compute_metrics(batch, [])
    assert result["instrument_coverage_pct"] == 0.0
    assert result["signal_direction_match_rate"] == 0.0
    assert result["signal_magnitude_mae"] == 999.0
    assert result["batch_event_count"] == 1.0
    assert result["live_event_count"] == 0.0


def test_compute_metrics_empty_both() -> None:
    result = _compute_metrics([], [])
    assert result["instrument_coverage_pct"] == 0.0
    assert result["live_event_count"] == 0.0


def test_compute_metrics_perfect_match() -> None:
    events = [
        {"instrument_id": "BTC-USD", "timeframe": "1m", "signal_direction": 1, "magnitude": 0.5},
        {"instrument_id": "ETH-USD", "timeframe": "1m", "signal_direction": -1, "magnitude": 0.3},
    ]
    result = _compute_metrics(events, events)
    assert result["signal_direction_match_rate"] == 1.0
    assert result["signal_magnitude_mae"] == pytest.approx(0.0)
    assert result["instrument_coverage_pct"] == pytest.approx(1.0)


def test_compute_metrics_direction_mismatch() -> None:
    batch = [{"instrument_id": "BTC-USD", "timeframe": "1m", "signal_direction": 1, "magnitude": 0.5}]
    live = [{"instrument_id": "BTC-USD", "timeframe": "1m", "signal_direction": -1, "magnitude": 0.5}]
    result = _compute_metrics(batch, live)
    assert result["signal_direction_match_rate"] == 0.0


def test_compute_metrics_partial_coverage() -> None:
    batch = [
        {"instrument_id": "BTC-USD", "timeframe": "1m", "signal_direction": 1, "magnitude": 0.5},
    ]
    live = [
        {"instrument_id": "BTC-USD", "timeframe": "1m", "signal_direction": 1, "magnitude": 0.5},
        {"instrument_id": "ETH-USD", "timeframe": "1m", "signal_direction": 1, "magnitude": 0.3},
    ]
    result = _compute_metrics(batch, live)
    # Only 1 of 2 live instruments covered by batch
    assert result["instrument_coverage_pct"] == pytest.approx(0.5)


def test_compute_metrics_magnitude_mae() -> None:
    batch = [{"instrument_id": "X", "timeframe": "1m", "signal_direction": 1, "magnitude": 0.8}]
    live = [{"instrument_id": "X", "timeframe": "1m", "signal_direction": 1, "magnitude": 0.5}]
    result = _compute_metrics(batch, live)
    assert result["signal_magnitude_mae"] == pytest.approx(0.3, abs=1e-9)


def test_compute_metrics_keying_uses_instrument_and_timeframe() -> None:
    # Same instrument, different timeframes — should NOT match
    batch = [{"instrument_id": "BTC-USD", "timeframe": "5m", "signal_direction": 1, "magnitude": 0.5}]
    live = [{"instrument_id": "BTC-USD", "timeframe": "1m", "signal_direction": 1, "magnitude": 0.5}]
    result = _compute_metrics(batch, live)
    assert result["instrument_coverage_pct"] == 0.0


def test_compute_metrics_latency_delta_zero() -> None:
    # latency_delta_ms is always 0.0 in current implementation
    batch = [{"instrument_id": "A", "timeframe": "1m", "signal_direction": 1, "magnitude": 0.1}]
    result = _compute_metrics(batch, batch)
    assert result["latency_delta_ms"] == 0.0


def test_compute_metrics_missing_magnitude_field_defaults_zero() -> None:
    # No 'magnitude' key — should default to 0.0
    batch = [{"instrument_id": "A", "timeframe": "1m", "signal_direction": 1}]
    live = [{"instrument_id": "A", "timeframe": "1m", "signal_direction": 1}]
    result = _compute_metrics(batch, live)
    assert result["signal_magnitude_mae"] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# _check_deviations
# ---------------------------------------------------------------------------


def test_check_deviations_no_breaches_when_all_ok() -> None:
    metrics = {
        "signal_direction_match_rate": ML_THRESHOLDS.signal_direction_match_rate_min,
        "signal_magnitude_mae": 0.0,
        "instrument_coverage_pct": ML_THRESHOLDS.instrument_coverage_pct_min,
        "latency_delta_ms": 0.0,
    }
    deviations = _check_deviations(metrics)
    assert deviations == []


def test_check_deviations_direction_match_rate_below_threshold() -> None:
    metrics = {
        "signal_direction_match_rate": 0.80,  # < 0.95
        "signal_magnitude_mae": 0.0,
        "instrument_coverage_pct": 1.0,
        "latency_delta_ms": 0.0,
    }
    devs = _check_deviations(metrics)
    names = [d.metric_name for d in devs]
    assert "signal_direction_match_rate" in names
    match_dev = next(d for d in devs if d.metric_name == "signal_direction_match_rate")
    assert match_dev.direction == "below"
    assert match_dev.stage == ReconStage.ML_RECON
    assert match_dev.actual_value == pytest.approx(0.80)
    assert match_dev.threshold == ML_THRESHOLDS.signal_direction_match_rate_min


def test_check_deviations_magnitude_mae_above_threshold() -> None:
    metrics = {
        "signal_direction_match_rate": 1.0,
        "signal_magnitude_mae": 0.5,  # > 0.1
        "instrument_coverage_pct": 1.0,
        "latency_delta_ms": 0.0,
    }
    devs = _check_deviations(metrics)
    names = [d.metric_name for d in devs]
    assert "signal_magnitude_mae" in names
    mae_dev = next(d for d in devs if d.metric_name == "signal_magnitude_mae")
    assert mae_dev.direction == "above"


def test_check_deviations_coverage_below_threshold() -> None:
    metrics = {
        "signal_direction_match_rate": 1.0,
        "signal_magnitude_mae": 0.0,
        "instrument_coverage_pct": 0.50,  # < 0.90
        "latency_delta_ms": 0.0,
    }
    devs = _check_deviations(metrics)
    names = [d.metric_name for d in devs]
    assert "instrument_coverage_pct" in names


def test_check_deviations_latency_above_threshold() -> None:
    metrics = {
        "signal_direction_match_rate": 1.0,
        "signal_magnitude_mae": 0.0,
        "instrument_coverage_pct": 1.0,
        "latency_delta_ms": 6000.0,  # > 5000
    }
    devs = _check_deviations(metrics)
    names = [d.metric_name for d in devs]
    assert "latency_delta_ms" in names
    lat_dev = next(d for d in devs if d.metric_name == "latency_delta_ms")
    assert lat_dev.direction == "above"


def test_check_deviations_all_breached() -> None:
    metrics = {
        "signal_direction_match_rate": 0.50,
        "signal_magnitude_mae": 1.0,
        "instrument_coverage_pct": 0.20,
        "latency_delta_ms": 10000.0,
    }
    devs = _check_deviations(metrics)
    assert len(devs) == 4


def test_check_deviations_boundary_exact_threshold_no_breach() -> None:
    # Exactly at threshold — should NOT fire (< or > not <=/>= for direction)
    metrics = {
        "signal_direction_match_rate": ML_THRESHOLDS.signal_direction_match_rate_min,  # exactly 0.95
        "signal_magnitude_mae": ML_THRESHOLDS.signal_magnitude_mae_max,  # exactly 0.1
        "instrument_coverage_pct": ML_THRESHOLDS.instrument_coverage_pct_min,  # exactly 0.90
        "latency_delta_ms": ML_THRESHOLDS.latency_delta_ms_max,  # exactly 5000.0
    }
    devs = _check_deviations(metrics)
    assert devs == []


def test_check_deviations_deviation_description_not_empty() -> None:
    metrics = {
        "signal_direction_match_rate": 0.50,
        "signal_magnitude_mae": 0.0,
        "instrument_coverage_pct": 1.0,
        "latency_delta_ms": 0.0,
    }
    devs = _check_deviations(metrics)
    assert len(devs) == 1
    assert len(devs[0].description) > 0
