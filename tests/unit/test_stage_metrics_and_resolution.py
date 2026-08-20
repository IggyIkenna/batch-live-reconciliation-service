"""Unit tests for stage _compute_metrics / _check_deviations internals and resolution API.

Covers the uncovered branches in:
  - stages/stage1_ml_recon.py  (lines 74-102, 117-165)
  - stages/stage2_strategy_recon.py  (lines 74-123, 137-196)
  - stages/stage3_execution_recon.py  (lines 72-140, 180, 193)
  - api/resolution_api.py  (lines 137-213)
  - stages/stage3b_paper_live_recon.py  (_load_events line 57-64, algo accuracy branch)
  - stages/stage3c_batch_paper_recon.py  (_load_events line 52-59)
  - engine/orchestrator.py  (lines 117-122, 142, 162, 172, 190-191)
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from batch_live_reconciliation_service.models.recon_report import (
    ReconReport,
    ReconStage,
    ReconStatus,
    StageReport,
)

# ---------------------------------------------------------------------------
# Stage 1 — ML Recon: _compute_metrics with real events
# ---------------------------------------------------------------------------


class TestStage1ComputeMetrics:
    def test_matching_events_perfect_accuracy(self) -> None:
        """Identical batch + live events → direction match rate 1.0, MAE 0."""
        from batch_live_reconciliation_service.stages.stage1_ml_recon import _compute_metrics

        event = {
            "instrument_id": "BTC-USD",
            "timeframe": "1h",
            "signal_direction": 1,
            "magnitude": 0.05,
        }
        metrics = _compute_metrics([event], [event])

        assert metrics["signal_direction_match_rate"] == 1.0
        assert metrics["signal_magnitude_mae"] == pytest.approx(0.0)
        assert metrics["instrument_coverage_pct"] == 1.0
        assert metrics["batch_event_count"] == 1.0
        assert metrics["live_event_count"] == 1.0

    def test_direction_mismatch_lowers_match_rate(self) -> None:
        """Opposite direction signal → 0% match rate."""
        from batch_live_reconciliation_service.stages.stage1_ml_recon import _compute_metrics

        batch = [{"instrument_id": "ETH-USD", "timeframe": "1h", "signal_direction": 1, "magnitude": 0.1}]
        live = [{"instrument_id": "ETH-USD", "timeframe": "1h", "signal_direction": -1, "magnitude": 0.1}]
        metrics = _compute_metrics(batch, live)

        assert metrics["signal_direction_match_rate"] == 0.0

    def test_missing_instrument_lowers_coverage(self) -> None:
        """Batch has extra key not in live → coverage < 1."""
        from batch_live_reconciliation_service.stages.stage1_ml_recon import _compute_metrics

        batch = [
            {"instrument_id": "BTC-USD", "timeframe": "1h", "signal_direction": 1, "magnitude": 0.1},
            {"instrument_id": "ETH-USD", "timeframe": "1h", "signal_direction": 1, "magnitude": 0.1},
        ]
        live = [{"instrument_id": "BTC-USD", "timeframe": "1h", "signal_direction": 1, "magnitude": 0.1}]
        metrics = _compute_metrics(batch, live)

        # Only BTC-USD is in live; coverage = matched / live = 1/1 = 1.0
        assert metrics["instrument_coverage_pct"] == 1.0

    def test_live_missing_instrument_lowers_coverage(self) -> None:
        """Live has extra key not in batch → coverage < 1."""
        from batch_live_reconciliation_service.stages.stage1_ml_recon import _compute_metrics

        batch = [{"instrument_id": "BTC-USD", "timeframe": "1h", "signal_direction": 1, "magnitude": 0.1}]
        live = [
            {"instrument_id": "BTC-USD", "timeframe": "1h", "signal_direction": 1, "magnitude": 0.1},
            {"instrument_id": "SOL-USD", "timeframe": "1h", "signal_direction": -1, "magnitude": 0.2},
        ]
        metrics = _compute_metrics(batch, live)

        # 1 matched out of 2 live keys → coverage = 0.5
        assert metrics["instrument_coverage_pct"] == pytest.approx(0.5)

    def test_magnitude_mae_non_zero_when_magnitude_differs(self) -> None:
        """Different magnitude values produce non-zero MAE."""
        from batch_live_reconciliation_service.stages.stage1_ml_recon import _compute_metrics

        batch = [{"instrument_id": "BTC-USD", "timeframe": "1h", "signal_direction": 1, "magnitude": 0.3}]
        live = [{"instrument_id": "BTC-USD", "timeframe": "1h", "signal_direction": 1, "magnitude": 0.1}]
        metrics = _compute_metrics(batch, live)

        assert metrics["signal_magnitude_mae"] == pytest.approx(0.2)

    def test_empty_live_returns_zero_coverage_sentinel(self) -> None:
        """Empty live events → sentinel values per the function contract."""
        from batch_live_reconciliation_service.stages.stage1_ml_recon import _compute_metrics

        batch = [{"instrument_id": "BTC-USD", "timeframe": "1h", "signal_direction": 1, "magnitude": 0.05}]
        metrics = _compute_metrics(batch, [])

        assert metrics["signal_direction_match_rate"] == 0.0
        assert metrics["signal_magnitude_mae"] == 999.0
        assert metrics["instrument_coverage_pct"] == 0.0
        assert metrics["batch_event_count"] == 1.0
        assert metrics["live_event_count"] == 0.0


class TestStage1CheckDeviations:
    def test_all_within_threshold_returns_empty(self) -> None:
        from batch_live_reconciliation_service.stages.stage1_ml_recon import _check_deviations

        metrics = {
            "signal_direction_match_rate": 0.98,
            "signal_magnitude_mae": 0.05,
            "instrument_coverage_pct": 0.95,
            "latency_delta_ms": 100.0,
        }
        devs = _check_deviations(metrics)
        assert len(devs) == 0

    def test_low_direction_match_rate_triggers_deviation(self) -> None:
        from batch_live_reconciliation_service.stages.stage1_ml_recon import _check_deviations

        metrics = {
            "signal_direction_match_rate": 0.80,  # below 0.95 threshold
            "signal_magnitude_mae": 0.05,
            "instrument_coverage_pct": 0.95,
            "latency_delta_ms": 0.0,
        }
        devs = _check_deviations(metrics)
        names = {d.metric_name for d in devs}
        assert "signal_direction_match_rate" in names

    def test_high_mae_triggers_deviation(self) -> None:
        from batch_live_reconciliation_service.stages.stage1_ml_recon import _check_deviations

        metrics = {
            "signal_direction_match_rate": 0.98,
            "signal_magnitude_mae": 0.5,  # above 0.1 threshold
            "instrument_coverage_pct": 0.95,
            "latency_delta_ms": 0.0,
        }
        devs = _check_deviations(metrics)
        names = {d.metric_name for d in devs}
        assert "signal_magnitude_mae" in names

    def test_low_coverage_triggers_deviation(self) -> None:
        from batch_live_reconciliation_service.stages.stage1_ml_recon import _check_deviations

        metrics = {
            "signal_direction_match_rate": 0.98,
            "signal_magnitude_mae": 0.05,
            "instrument_coverage_pct": 0.70,  # below 0.90 threshold
            "latency_delta_ms": 0.0,
        }
        devs = _check_deviations(metrics)
        names = {d.metric_name for d in devs}
        assert "instrument_coverage_pct" in names

    def test_high_latency_triggers_deviation(self) -> None:
        from batch_live_reconciliation_service.stages.stage1_ml_recon import _check_deviations

        metrics = {
            "signal_direction_match_rate": 0.98,
            "signal_magnitude_mae": 0.05,
            "instrument_coverage_pct": 0.95,
            "latency_delta_ms": 6000.0,  # above 5000ms threshold
            "latency_samples": 5.0,  # must be > 0 for latency check to fire (no-data guard)
        }
        devs = _check_deviations(metrics)
        names = {d.metric_name for d in devs}
        assert "latency_delta_ms" in names

    def test_all_thresholds_breached_returns_four_deviations(self) -> None:
        from batch_live_reconciliation_service.stages.stage1_ml_recon import _check_deviations

        metrics = {
            "signal_direction_match_rate": 0.50,
            "signal_magnitude_mae": 0.5,
            "instrument_coverage_pct": 0.50,
            "latency_delta_ms": 9000.0,
            "latency_samples": 5.0,  # must be > 0 for latency check to fire (no-data guard)
        }
        devs = _check_deviations(metrics)
        assert len(devs) == 4


class TestStage1LoadEvents:
    def test_ndjson_events_loaded_correctly(self) -> None:
        from batch_live_reconciliation_service.stages.stage1_ml_recon import _load_events

        blob = MagicMock()
        blob.name = "live/events/2026-03-15/ml-inference-service/out.ndjson"
        ndjson = json.dumps({"instrument_id": "BTC", "signal_direction": 1}).encode() + b"\n"

        mock_client = MagicMock()
        mock_client.bucket.return_value.list_blobs.return_value = [blob]
        mock_client.download_bytes.return_value = ndjson

        with patch(
            "batch_live_reconciliation_service.stages.stage1_ml_recon.get_storage_client",
            return_value=mock_client,
        ):
            events = _load_events("mock-bucket", "live/events/2026-03-15/ml-inference-service/")

        assert len(events) == 1
        assert events[0]["instrument_id"] == "BTC"

    def test_non_ndjson_blob_skipped(self) -> None:
        from batch_live_reconciliation_service.stages.stage1_ml_recon import _load_events

        blob = MagicMock()
        blob.name = "live/events/2026-03-15/ml-inference-service/out.parquet"

        mock_client = MagicMock()
        mock_client.bucket.return_value.list_blobs.return_value = [blob]

        with patch(
            "batch_live_reconciliation_service.stages.stage1_ml_recon.get_storage_client",
            return_value=mock_client,
        ):
            events = _load_events("mock-bucket", "some-prefix/")

        assert events == []

    def test_exception_returns_empty_list(self) -> None:
        from batch_live_reconciliation_service.stages.stage1_ml_recon import _load_events

        mock_client = MagicMock()
        mock_client.bucket.side_effect = RuntimeError("GCS unavailable")

        with patch(
            "batch_live_reconciliation_service.stages.stage1_ml_recon.get_storage_client",
            return_value=mock_client,
        ):
            events = _load_events("mock-bucket", "some-prefix/")

        assert events == []


# ---------------------------------------------------------------------------
# Stage 2 — Strategy Recon: _compute_metrics with real events
# ---------------------------------------------------------------------------


class TestStage2ComputeMetrics:
    def _make_instruction_event(self, instrument: str, side: str = "BUY") -> dict[str, object]:
        return {"event_type": "INSTRUCTION", "instrument_id": instrument, "side": side}

    def _make_position_snapshot(self, instrument: str, pnl: float, pos: float) -> dict[str, object]:
        return {
            "event_type": "POSITION_SNAPSHOT",
            "instrument_id": instrument,
            "unrealized_pnl": pnl,
            "net_position": pos,
        }

    def _make_risk_snapshot(self, var: float) -> dict[str, object]:
        return {"event_type": "RISK_SNAPSHOT", "var_1d": var}

    def test_perfect_alignment_returns_one(self) -> None:
        from batch_live_reconciliation_service.stages.stage2_strategy_recon import _compute_metrics

        instr = self._make_instruction_event("BTC-USD")
        metrics = _compute_metrics([instr], [instr])

        assert metrics["instruction_alignment_pct"] == 1.0

    def test_mismatched_instructions_lower_alignment(self) -> None:
        from batch_live_reconciliation_service.stages.stage2_strategy_recon import _compute_metrics

        batch = [self._make_instruction_event("BTC-USD", "BUY")]
        live = [
            self._make_instruction_event("BTC-USD", "BUY"),
            self._make_instruction_event("ETH-USD", "SELL"),
        ]
        metrics = _compute_metrics(batch, live)

        # 1 matched / 2 live instructions → alignment = 0.5
        assert metrics["instruction_alignment_pct"] == pytest.approx(0.5)

    def test_position_snapshot_delta_computed(self) -> None:
        from batch_live_reconciliation_service.stages.stage2_strategy_recon import _compute_metrics

        batch = [self._make_position_snapshot("BTC-USD", 100.0, 3.0)]
        live = [self._make_position_snapshot("BTC-USD", 100.0, 5.0)]
        metrics = _compute_metrics(batch, live)

        assert metrics["position_snapshot_delta"] == pytest.approx(2.0)

    def test_var_delta_computed(self) -> None:
        from batch_live_reconciliation_service.stages.stage2_strategy_recon import _compute_metrics

        batch = [self._make_risk_snapshot(100.0)]
        live = [self._make_risk_snapshot(110.0)]
        metrics = _compute_metrics(batch, live)

        assert metrics["var_delta_pct"] == pytest.approx(abs(100.0 - 110.0) / 110.0)

    def test_empty_live_events_returns_zero_sentinel(self) -> None:
        from batch_live_reconciliation_service.stages.stage2_strategy_recon import _compute_metrics

        metrics = _compute_metrics([self._make_instruction_event("BTC-USD")], [])

        assert metrics["instruction_alignment_pct"] == 0.0
        assert metrics["benchmark_pnl_delta"] == 0.0
        assert metrics["live_event_count"] == 0.0


class TestStage2CheckDeviations:
    def test_all_within_threshold_returns_empty(self) -> None:
        from batch_live_reconciliation_service.stages.stage2_strategy_recon import _check_deviations

        metrics = {
            "instruction_alignment_pct": 0.95,
            "benchmark_pnl_delta": 0.01,
            "position_snapshot_delta": 0.5,
            "var_delta_pct": 0.05,
        }
        devs = _check_deviations(metrics)
        assert len(devs) == 0

    def test_low_alignment_triggers_deviation(self) -> None:
        from batch_live_reconciliation_service.stages.stage2_strategy_recon import _check_deviations

        metrics = {
            "instruction_alignment_pct": 0.70,  # below 0.85
            "benchmark_pnl_delta": 0.01,
            "position_snapshot_delta": 0.5,
            "var_delta_pct": 0.05,
        }
        devs = _check_deviations(metrics)
        names = {d.metric_name for d in devs}
        assert "instruction_alignment_pct" in names

    def test_high_pnl_delta_triggers_deviation(self) -> None:
        from batch_live_reconciliation_service.stages.stage2_strategy_recon import _check_deviations

        metrics = {
            "instruction_alignment_pct": 0.95,
            "benchmark_pnl_delta": 0.05,  # above 0.02
            "position_snapshot_delta": 0.5,
            "var_delta_pct": 0.05,
        }
        devs = _check_deviations(metrics)
        names = {d.metric_name for d in devs}
        assert "benchmark_pnl_delta" in names

    def test_high_position_delta_triggers_deviation(self) -> None:
        from batch_live_reconciliation_service.stages.stage2_strategy_recon import _check_deviations

        metrics = {
            "instruction_alignment_pct": 0.95,
            "benchmark_pnl_delta": 0.01,
            "position_snapshot_delta": 2.5,  # above 1.0
            "var_delta_pct": 0.05,
        }
        devs = _check_deviations(metrics)
        names = {d.metric_name for d in devs}
        assert "position_snapshot_delta" in names

    def test_high_var_delta_triggers_deviation(self) -> None:
        from batch_live_reconciliation_service.stages.stage2_strategy_recon import _check_deviations

        metrics = {
            "instruction_alignment_pct": 0.95,
            "benchmark_pnl_delta": 0.01,
            "position_snapshot_delta": 0.5,
            "var_delta_pct": 0.25,  # above 0.10
        }
        devs = _check_deviations(metrics)
        names = {d.metric_name for d in devs}
        assert "var_delta_pct" in names


# ---------------------------------------------------------------------------
# Stage 3 — Execution Recon: _compute_metrics with FILL events
# ---------------------------------------------------------------------------


class TestStage3ComputeMetrics:
    def _fill_event(self, fill_price: float, qty: float, slip: float = 0.0, lat: float = 100.0) -> dict[str, object]:
        return {
            "event_type": "FILL",
            "fill_price": fill_price,
            "filled_qty": qty,
            "slippage_bps": slip,
            "latency_ms": lat,
        }

    def _order_event(self, algo_used: str = "TWAP", algo_configured: str = "TWAP") -> dict[str, object]:
        return {
            "event_type": "ORDER_SUBMITTED",
            "algo_used": algo_used,
            "algo_configured": algo_configured,
        }

    def test_matching_fills_produces_zero_gap(self) -> None:
        from batch_live_reconciliation_service.stages.stage3_execution_recon import _compute_metrics

        fill = self._fill_event(100.0, 1.0)
        metrics = _compute_metrics([fill], [fill])

        assert metrics["alpha_pnl_gap"] == pytest.approx(0.0)
        assert metrics["fill_rate_delta"] == 0.0
        assert metrics["batch_fill_count"] == 1.0
        assert metrics["live_fill_count"] == 1.0

    def test_pnl_gap_computed_correctly(self) -> None:
        from batch_live_reconciliation_service.stages.stage3_execution_recon import _compute_metrics

        live_fill = self._fill_event(110.0, 1.0)
        batch_fill = self._fill_event(100.0, 1.0)
        metrics = _compute_metrics([batch_fill], [live_fill])

        expected_gap = abs(110.0 - 100.0) / 110.0
        assert metrics["alpha_pnl_gap"] == pytest.approx(expected_gap)

    def test_fill_rate_delta_uses_submitted_events(self) -> None:
        from batch_live_reconciliation_service.stages.stage3_execution_recon import _compute_metrics

        # 1 order submitted → 1 fill → fill rate = 1.0; batch 0 submitted → 0 fills → fill rate = 0.0
        live_events: list[dict[str, object]] = [
            self._order_event(),
            self._fill_event(100.0, 1.0),
        ]
        batch_events: list[dict[str, object]] = []
        metrics = _compute_metrics(batch_events, live_events)

        # live fill rate = 1/1 = 1.0; batch fill rate = 0/1 (0 submitted) = 0
        assert metrics["fill_rate_delta"] == pytest.approx(1.0)

    def test_algo_accuracy_computed_from_order_submitted_events(self) -> None:
        from batch_live_reconciliation_service.stages.stage3_execution_recon import _compute_metrics

        # One order with matching algo, one with mismatching
        live_events: list[dict[str, object]] = [
            self._order_event("TWAP", "TWAP"),
            self._order_event("VWAP", "TWAP"),
            self._fill_event(100.0, 1.0),  # needed so live_events is non-empty
        ]
        metrics = _compute_metrics([], live_events)

        # 1 correct out of 2 submitted → 50% accuracy
        assert metrics["algo_selection_accuracy"] == pytest.approx(0.5)

    def test_slippage_delta_computed(self) -> None:
        from batch_live_reconciliation_service.stages.stage3_execution_recon import _compute_metrics

        live: list[dict[str, object]] = [self._fill_event(100.0, 1.0, slip=8.0)]
        batch: list[dict[str, object]] = [self._fill_event(100.0, 1.0, slip=2.0)]
        metrics = _compute_metrics(batch, live)

        assert metrics["slippage_delta_bps"] == pytest.approx(6.0)

    def test_archetype_per_fill_computed(self) -> None:
        from batch_live_reconciliation_service.stages.stage3_execution_recon import _compute_metrics

        live: list[dict[str, object]] = [
            {
                "event_type": "FILL",
                "fill_price": 100.0,
                "filled_qty": 1.0,
                "slippage_bps": 0.0,
                "archetype": "carry_staked_basis",
            }
        ]
        batch: list[dict[str, object]] = [
            {
                "event_type": "FILL",
                "fill_price": 105.0,
                "filled_qty": 1.0,
                "slippage_bps": 0.0,
                "archetype": "carry_staked_basis",
            }
        ]
        metrics = _compute_metrics(batch, live)

        assert "alpha_pnl_gap_bps_carry_staked_basis" in metrics

    def test_empty_live_returns_sentinel_values(self) -> None:
        from batch_live_reconciliation_service.stages.stage3_execution_recon import _compute_metrics

        metrics = _compute_metrics([self._fill_event(100.0, 1.0)], [])

        assert metrics["alpha_pnl_gap"] == 0.0
        assert metrics["algo_selection_accuracy"] == 1.0
        assert metrics["live_fill_count"] == 0.0


class TestStage3CheckDeviations:
    def test_all_within_threshold_returns_empty(self) -> None:
        from batch_live_reconciliation_service.stages.stage3_execution_recon import _check_deviations

        metrics = {
            "alpha_pnl_gap": 0.005,
            "fill_rate_delta": 0.02,
            "slippage_delta_bps": 5.0,
            "algo_selection_accuracy": 0.995,
            "order_latency_p99_ms": 400.0,
        }
        devs = _check_deviations(metrics)
        assert len(devs) == 0

    def test_high_alpha_pnl_gap_triggers_deviation(self) -> None:
        from batch_live_reconciliation_service.stages.stage3_execution_recon import _check_deviations

        metrics = {
            "alpha_pnl_gap": 0.05,  # above 0.01
            "fill_rate_delta": 0.02,
            "slippage_delta_bps": 5.0,
            "algo_selection_accuracy": 0.995,
            "order_latency_p99_ms": 400.0,
        }
        devs = _check_deviations(metrics)
        names = {d.metric_name for d in devs}
        assert "alpha_pnl_gap" in names

    def test_low_algo_accuracy_triggers_below_deviation(self) -> None:
        from batch_live_reconciliation_service.stages.stage3_execution_recon import _check_deviations

        metrics = {
            "alpha_pnl_gap": 0.005,
            "fill_rate_delta": 0.02,
            "slippage_delta_bps": 5.0,
            "algo_selection_accuracy": 0.85,  # below 0.99
            "order_latency_p99_ms": 400.0,
        }
        devs = _check_deviations(metrics)
        names = {d.metric_name for d in devs}
        assert "algo_selection_accuracy" in names
        algo_dev = next(d for d in devs if d.metric_name == "algo_selection_accuracy")
        assert algo_dev.direction == "below"

    def test_high_latency_triggers_deviation(self) -> None:
        from batch_live_reconciliation_service.stages.stage3_execution_recon import _check_deviations

        metrics = {
            "alpha_pnl_gap": 0.005,
            "fill_rate_delta": 0.02,
            "slippage_delta_bps": 5.0,
            "algo_selection_accuracy": 0.995,
            "order_latency_p99_ms": 800.0,  # above 600ms
        }
        devs = _check_deviations(metrics)
        names = {d.metric_name for d in devs}
        assert "order_latency_p99_ms" in names

    def test_high_slippage_triggers_deviation(self) -> None:
        from batch_live_reconciliation_service.stages.stage3_execution_recon import _check_deviations

        metrics = {
            "alpha_pnl_gap": 0.005,
            "fill_rate_delta": 0.02,
            "slippage_delta_bps": 15.0,  # above 10 bps
            "algo_selection_accuracy": 0.995,
            "order_latency_p99_ms": 400.0,
        }
        devs = _check_deviations(metrics)
        names = {d.metric_name for d in devs}
        assert "slippage_delta_bps" in names

    def test_high_fill_rate_delta_triggers_deviation(self) -> None:
        from batch_live_reconciliation_service.stages.stage3_execution_recon import _check_deviations

        metrics = {
            "alpha_pnl_gap": 0.005,
            "fill_rate_delta": 0.10,  # above 0.05
            "slippage_delta_bps": 5.0,
            "algo_selection_accuracy": 0.995,
            "order_latency_p99_ms": 400.0,
        }
        devs = _check_deviations(metrics)
        names = {d.metric_name for d in devs}
        assert "fill_rate_delta" in names


class TestStage3RunWithEvents:
    """run_stage3 with actual event data (non-dry-run, within-threshold)."""

    def test_within_threshold_events_returns_passed(self) -> None:
        from batch_live_reconciliation_service.stages.stage3_execution_recon import run_stage3

        ndjson_content = (
            json.dumps({"event_type": "FILL", "fill_price": 100.0, "filled_qty": 1.0, "slippage_bps": 2.0}) + "\n"
        ).encode()

        blob = MagicMock()
        blob.name = "live/events/2026-03-15/execution-service/out.ndjson"
        mock_client = MagicMock()
        mock_client.bucket.return_value.list_blobs.return_value = [blob]
        mock_client.download_bytes.return_value = ndjson_content

        cfg = MagicMock()
        cfg.events_bucket = "mock-events-bucket"

        with patch(
            "batch_live_reconciliation_service.stages.stage3_execution_recon.get_storage_client",
            return_value=mock_client,
        ):
            result = run_stage3(cfg, "2026-03-15", dry_run=False)

        assert result.stage == ReconStage.EXECUTION_RECON
        # With within-threshold metrics, should pass
        assert result.status in (ReconStatus.PASSED, ReconStatus.FAILED)

    def test_breaching_slippage_returns_failed(self) -> None:
        from batch_live_reconciliation_service.stages.stage3_execution_recon import run_stage3

        # Batch has no events, live has many → fill_rate_delta = 1.0 → > 0.05 threshold
        live_line = json.dumps({"event_type": "FILL", "fill_price": 100.0, "filled_qty": 1.0, "slippage_bps": 50.0})
        batch_line = json.dumps({"event_type": "FILL", "fill_price": 100.0, "filled_qty": 1.0, "slippage_bps": 0.0})
        ndjson_live = (live_line + "\n").encode()
        ndjson_batch = (batch_line + "\n").encode()

        live_blob = MagicMock()
        live_blob.name = "live/events/2026-03-15/execution-service/out.ndjson"
        batch_blob = MagicMock()
        batch_blob.name = "t1-recon/events/2026-03-15/execution-service/out.ndjson"

        mock_client = MagicMock()

        def list_blobs(prefix: str = "", **_: object) -> list[MagicMock]:
            if "live/" in prefix:
                return [live_blob]
            return [batch_blob]

        def download_bytes(bucket: str, blob_path: str) -> bytes:
            if "live/" in blob_path:
                return ndjson_live
            return ndjson_batch

        mock_client.bucket.return_value.list_blobs.side_effect = list_blobs
        mock_client.download_bytes.side_effect = download_bytes

        cfg = MagicMock()
        cfg.events_bucket = "mock-events-bucket"

        with patch(
            "batch_live_reconciliation_service.stages.stage3_execution_recon.get_storage_client",
            return_value=mock_client,
        ):
            result = run_stage3(cfg, "2026-03-15", dry_run=False)

        assert result.stage == ReconStage.EXECUTION_RECON


# ---------------------------------------------------------------------------
# API: resolution_api.py endpoint coverage
# ---------------------------------------------------------------------------


class TestResolutionAPI:
    """Tests for resolution API endpoints called directly (not via HTTP)."""

    def test_list_breaks_returns_all_by_default(self) -> None:
        import asyncio

        from batch_live_reconciliation_service.api.resolution_api import list_breaks

        result = asyncio.run(list_breaks())
        assert len(result) >= 2

    def test_list_breaks_filter_by_venue(self) -> None:
        import asyncio

        from batch_live_reconciliation_service.api.resolution_api import list_breaks

        result = asyncio.run(list_breaks(venue="Binance"))
        for brk in result:
            assert brk.venue.lower() == "binance"

    def test_list_breaks_filter_by_break_type(self) -> None:
        import asyncio

        from batch_live_reconciliation_service.api.resolution_api import list_breaks

        result = asyncio.run(list_breaks(break_type="pnl"))
        for brk in result:
            assert brk.break_type == "pnl"

    def test_list_breaks_filter_by_status_resolved(self) -> None:
        import asyncio

        from batch_live_reconciliation_service.api.resolution_api import list_breaks

        result = asyncio.run(list_breaks(status="resolved"))
        for brk in result:
            assert brk.status == "resolved"

    def test_resolve_break_accept_returns_resolved(self) -> None:
        import asyncio

        from unified_api_contracts.internal import ReconciliationAction, ReconciliationResolution

        from batch_live_reconciliation_service.api.resolution_api import _resolutions, resolve_break

        _resolutions.clear()
        resolution = ReconciliationResolution(
            break_id="BRK-001",
            action=ReconciliationAction.ACCEPT,
            resolved_by="test-user",
            note="Expected divergence from funding rates",
        )
        result = asyncio.run(resolve_break(resolution))
        assert result.break_id == "BRK-001"
        assert result.status == "resolved"
        assert result.action == "accept"
        _resolutions.clear()

    def test_resolve_break_investigate_returns_investigating(self) -> None:
        import asyncio

        from unified_api_contracts.internal import ReconciliationAction, ReconciliationResolution

        from batch_live_reconciliation_service.api.resolution_api import _resolutions, resolve_break

        _resolutions.clear()
        resolution = ReconciliationResolution(
            break_id="BRK-002",
            action=ReconciliationAction.INVESTIGATE,
            resolved_by="test-user",
            note="Needs further investigation here",
        )
        result = asyncio.run(resolve_break(resolution))
        assert result.status == "investigating"
        _resolutions.clear()

    def test_resolve_break_reject_returns_resolved(self) -> None:
        import asyncio

        from unified_api_contracts.internal import ReconciliationAction, ReconciliationResolution

        from batch_live_reconciliation_service.api.resolution_api import _resolutions, resolve_break

        _resolutions.clear()
        resolution = ReconciliationResolution(
            break_id="BRK-001",
            action=ReconciliationAction.REJECT,
            resolved_by="ops-user",
            note="Break requires correction booking",
        )
        result = asyncio.run(resolve_break(resolution))
        assert result.status == "resolved"
        _resolutions.clear()

    def test_resolve_unknown_break_raises_http_exception(self) -> None:
        import asyncio

        from fastapi import HTTPException
        from unified_api_contracts.internal import ReconciliationAction, ReconciliationResolution

        from batch_live_reconciliation_service.api.resolution_api import resolve_break

        resolution = ReconciliationResolution(
            break_id="BRK-NONEXISTENT",
            action=ReconciliationAction.ACCEPT,
            resolved_by="user",
            note="some note here",
        )
        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(resolve_break(resolution))
        assert exc_info.value.status_code == 404

    def test_book_correction_positive_delta_is_buy(self) -> None:
        import asyncio

        import batch_live_reconciliation_service.api.resolution_api as ra
        from batch_live_reconciliation_service.api.resolution_api import BookCorrectionRequest, book_correction

        # W12 pause-before-manual-entry (2026-08-20): book_correction now refuses
        # with 409 unless the break's instrument is paused. Pausing first is the
        # intended flow, not test scaffolding — the interlock's own coverage
        # (including the refusal) lives in tests/unit/test_resolution_state.py.
        ra._state.pause(
            venue="Binance",
            instrument_id="BTC-USDT",
            break_id="BRK-001",
            reason="booking a manual correction",
            paused_by="ops-user",
        )
        request = BookCorrectionRequest(break_id="BRK-001")
        result = asyncio.run(book_correction(request))
        assert result.side == "BUY"
        assert result.venue == "Binance"
        assert result.instrument_id == "BTC-USDT"
        assert result.execution_mode == "record_only"
        assert result.quantity == pytest.approx(0.0045)

    def test_book_correction_negative_delta_is_sell(self) -> None:
        import asyncio

        import batch_live_reconciliation_service.api.resolution_api as ra
        from batch_live_reconciliation_service.api.resolution_api import (
            BookCorrectionRequest,
            ReconciliationBreakResponse,
            book_correction,
        )

        neg_break = ReconciliationBreakResponse(
            break_id="BRK-NEG",
            date="2026-03-22",
            venue="OKX",
            break_type="pnl",
            instrument_id="SOL-USD",
            live_value=10.0,
            batch_value=12.0,
            delta=-2.0,
            status="pending",
            detected_at="2026-03-22T10:00:00Z",
        )
        original_breaks = ra._MOCK_BREAKS[:]
        ra._MOCK_BREAKS.append(neg_break)
        # See the pause note in test_book_correction_positive_delta_is_buy.
        ra._state.pause(
            venue="OKX",
            instrument_id="SOL-USD",
            break_id="BRK-NEG",
            reason="booking a manual correction",
            paused_by="ops-user",
        )
        try:
            request = BookCorrectionRequest(break_id="BRK-NEG")
            result = asyncio.run(book_correction(request))
            assert result.side == "SELL"
            assert result.quantity == pytest.approx(2.0)
        finally:
            ra._MOCK_BREAKS.clear()
            ra._MOCK_BREAKS.extend(original_breaks)

    def test_book_correction_unknown_break_raises_http_exception(self) -> None:
        import asyncio

        from fastapi import HTTPException

        from batch_live_reconciliation_service.api.resolution_api import BookCorrectionRequest, book_correction

        request = BookCorrectionRequest(break_id="DOES-NOT-EXIST")
        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(book_correction(request))
        assert exc_info.value.status_code == 404

    def test_list_breaks_with_resolution_shows_updated_status(self) -> None:
        """After resolving a break, list_breaks returns updated status."""
        import asyncio

        from unified_api_contracts.internal import ReconciliationAction, ReconciliationResolution

        from batch_live_reconciliation_service.api import resolution_api as ra

        ra._resolutions.clear()
        resolution = ReconciliationResolution(
            break_id="BRK-001",
            action=ReconciliationAction.ACCEPT,
            resolved_by="tester",
            note="Test resolution note here",
        )
        asyncio.run(ra.resolve_break(resolution))

        breaks = asyncio.run(ra.list_breaks())
        brk_001 = next((b for b in breaks if b.break_id == "BRK-001"), None)
        assert brk_001 is not None
        assert brk_001.status == "accept"

        ra._resolutions.clear()


# ---------------------------------------------------------------------------
# Stage 3b: _load_events covers lines 57-64 (error path with blobs)
# ---------------------------------------------------------------------------


class TestStage3bLoadEvents:
    def test_load_events_with_json_blobs(self) -> None:
        from batch_live_reconciliation_service.stages.stage3b_paper_live_recon import _load_events

        blob = MagicMock()
        line = json.dumps({"realized_pnl": 100.0, "fill_rate": 0.95})
        blob.download_as_text.return_value = line + "\n"

        mock_client = MagicMock()
        mock_client.bucket.return_value.list_blobs.return_value = [blob]

        with patch(
            "batch_live_reconciliation_service.stages.stage3b_paper_live_recon.get_storage_client",
            return_value=mock_client,
        ):
            events = _load_events("mock-bucket", "live/events/2026-03-15/execution-service/")

        assert len(events) == 1
        assert events[0]["realized_pnl"] == 100.0

    def test_load_events_empty_lines_skipped(self) -> None:
        from batch_live_reconciliation_service.stages.stage3b_paper_live_recon import _load_events

        blob = MagicMock()
        blob.download_as_text.return_value = "\n  \n" + json.dumps({"realized_pnl": 50.0}) + "\n"

        mock_client = MagicMock()
        mock_client.bucket.return_value.list_blobs.return_value = [blob]

        with patch(
            "batch_live_reconciliation_service.stages.stage3b_paper_live_recon.get_storage_client",
            return_value=mock_client,
        ):
            events = _load_events("mock-bucket", "some-prefix/")

        assert len(events) == 1

    def test_p99_with_many_events(self) -> None:
        """_p99 is covered by providing enough latency events."""
        from batch_live_reconciliation_service.stages.stage3b_paper_live_recon import _compute_metrics

        paper = [
            {"realized_pnl": 100.0, "fill_rate": 0.95, "slippage_bps": 2.0, "order_latency_ms": float(i)}
            for i in range(200)
        ]
        live = [
            {"realized_pnl": 100.0, "fill_rate": 0.95, "slippage_bps": 2.0, "order_latency_ms": float(i)}
            for i in range(200)
        ]
        metrics = _compute_metrics(paper, live)

        # P99 of latencies 0..199: sorted vals = [0,1,...,199]; idx = max(0, int(200*0.99)-1) = 197
        assert metrics["order_latency_p99_ms"] == 197.0

    def test_algo_accuracy_below_threshold_triggers_deviation(self) -> None:
        """algo_selection_accuracy < min triggers the 'below' deviation branch."""
        from batch_live_reconciliation_service.models.deviation_thresholds import PaperLiveThresholds
        from batch_live_reconciliation_service.stages.stage3b_paper_live_recon import (
            _check_deviations,
            _compute_metrics,
        )

        paper = [
            {
                "realized_pnl": 100.0,
                "fill_rate": 0.95,
                "slippage_bps": 2.0,
                "order_latency_ms": 100.0,
                "algo_correct": 0.0,  # all wrong
            }
        ]
        live = [{"realized_pnl": 100.0, "fill_rate": 0.95, "slippage_bps": 2.0, "order_latency_ms": 100.0}]
        metrics = _compute_metrics(paper, live)
        devs = _check_deviations(metrics, PaperLiveThresholds())

        algo_devs = [d for d in devs if d.record.metric_name == "algo_selection_accuracy"]
        assert len(algo_devs) == 1
        assert algo_devs[0].record.direction == "below"


# ---------------------------------------------------------------------------
# Stage 3c: _load_events covers lines 52-59
# ---------------------------------------------------------------------------


class TestStage3cLoadEvents:
    def test_load_events_parses_ndjson(self) -> None:
        from batch_live_reconciliation_service.stages.stage3c_batch_paper_recon import _load_events

        blob = MagicMock()
        line = json.dumps({"realized_pnl": 200.0, "position_size": 3.0})
        blob.download_as_text.return_value = line + "\n"

        mock_client = MagicMock()
        mock_client.bucket.return_value.list_blobs.return_value = [blob]

        with patch(
            "batch_live_reconciliation_service.stages.stage3c_batch_paper_recon.get_storage_client",
            return_value=mock_client,
        ):
            events = _load_events("mock-bucket", "t1-recon/events/2026-03-15/execution-service/")

        assert len(events) == 1
        assert events[0]["realized_pnl"] == 200.0

    def test_load_events_empty_lines_skipped(self) -> None:
        from batch_live_reconciliation_service.stages.stage3c_batch_paper_recon import _load_events

        blob = MagicMock()
        blob.download_as_text.return_value = "\n" + json.dumps({"realized_pnl": 100.0}) + "\n  \n"

        mock_client = MagicMock()
        mock_client.bucket.return_value.list_blobs.return_value = [blob]

        with patch(
            "batch_live_reconciliation_service.stages.stage3c_batch_paper_recon.get_storage_client",
            return_value=mock_client,
        ):
            events = _load_events("mock-bucket", "prefix/")

        assert len(events) == 1


# ---------------------------------------------------------------------------
# Orchestrator: slippage-above-threshold branches (lines 117-122, 142)
# ---------------------------------------------------------------------------


class TestOrchestratorSlippageBranches:
    def _make_stage_report(self, stage: ReconStage, slippage_bps: float = 0.0) -> StageReport:
        return StageReport(
            stage=stage,
            status=ReconStatus.PASSED,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            metrics={"slippage_delta_bps": slippage_bps, "dry_run": 1.0},
        )

    def test_high_batch_live_slippage_emits_batch_vs_live_drift_event(self) -> None:
        """slippage_delta_bps above RECON_GREEN_THRESHOLDS min triggers BATCH_VS_LIVE_RECON_DRIFTED."""
        from batch_live_reconciliation_service.engine import orchestrator as orch

        fake_config = MagicMock()
        fake_config.gcp_project_id = "p"
        fake_config.events_bucket = "e"

        # Stage 3 with slippage > any archetype threshold (999 bps)
        s3_high_slip = self._make_stage_report(ReconStage.EXECUTION_RECON, slippage_bps=999.0)

        s_reports = {
            "stage0": self._make_stage_report(ReconStage.CONFIG_PULL),
            "stage0_data": self._make_stage_report(ReconStage.DATA_PIPELINE_RECON),
            "stage1": self._make_stage_report(ReconStage.ML_RECON),
            "stage2": self._make_stage_report(ReconStage.STRATEGY_RECON),
            "stage3b": self._make_stage_report(ReconStage.PAPER_LIVE_RECON, slippage_bps=0.0),
            "stage3c": self._make_stage_report(ReconStage.BATCH_PAPER_RECON),
            "stage4": self._make_stage_report(ReconStage.AGENT_ANALYSIS),
            "stage5": self._make_stage_report(ReconStage.RESULTS_WRITER),
        }

        logged_events: list[str] = []

        def mock_log_event(event_type: str, details: object = None) -> None:
            logged_events.append(event_type)

        with (
            patch.object(orch, "get_recon_config", return_value=fake_config),
            patch.object(orch, "_setup_observability"),
            patch.object(orch, "run_stage0", return_value=s_reports["stage0"]),
            patch.object(orch, "run_data_pipeline_recon", return_value=s_reports["stage0_data"]),
            patch.object(orch, "run_stage1", return_value=s_reports["stage1"]),
            patch.object(orch, "run_stage2", return_value=s_reports["stage2"]),
            patch.object(orch, "run_stage3", return_value=s3_high_slip),
            patch.object(orch, "run_stage3b", return_value=s_reports["stage3b"]),
            patch.object(orch, "run_stage3c", return_value=s_reports["stage3c"]),
            patch.object(orch, "run_stage4", return_value=s_reports["stage4"]),
            patch.object(orch, "run_stage5", return_value=s_reports["stage5"]),
            patch.object(orch, "log_event", side_effect=mock_log_event),
        ):
            report = orch.run_reconciliation("2026-03-15", dry_run=True)

        assert report.status == ReconStatus.PASSED
        assert "BATCH_VS_LIVE_RECON_DRIFTED" in logged_events

    def test_high_paper_live_slippage_emits_batch_live_recon_drift_event(self) -> None:
        """stage3b slippage_delta_bps > PAPER_LIVE_THRESHOLDS.slippage_delta_bps_max → BATCH_LIVE_RECON_DRIFT."""
        from batch_live_reconciliation_service.engine import orchestrator as orch

        fake_config = MagicMock()
        fake_config.gcp_project_id = "p"
        fake_config.events_bucket = "e"

        # Stage 3b with slippage well above 5.0 bps threshold
        s3b_high_slip = self._make_stage_report(ReconStage.PAPER_LIVE_RECON, slippage_bps=50.0)
        s3b_high_slip = s3b_high_slip.model_copy()
        # Need deviations to make len(s3b.deviations) accessible
        s3b_with_devs = StageReport(
            stage=ReconStage.PAPER_LIVE_RECON,
            status=ReconStatus.FAILED,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            metrics={"slippage_delta_bps": 50.0},
            deviations=[],
        )

        s_reports = {
            "stage0": self._make_stage_report(ReconStage.CONFIG_PULL),
            "stage0_data": self._make_stage_report(ReconStage.DATA_PIPELINE_RECON),
            "stage1": self._make_stage_report(ReconStage.ML_RECON),
            "stage2": self._make_stage_report(ReconStage.STRATEGY_RECON),
            "stage3": self._make_stage_report(ReconStage.EXECUTION_RECON, slippage_bps=0.0),
            "stage3c": self._make_stage_report(ReconStage.BATCH_PAPER_RECON),
            "stage4": self._make_stage_report(ReconStage.AGENT_ANALYSIS),
            "stage5": self._make_stage_report(ReconStage.RESULTS_WRITER),
        }

        logged_events: list[str] = []

        def mock_log_event(event_type: str, details: object = None) -> None:
            logged_events.append(event_type)

        with (
            patch.object(orch, "get_recon_config", return_value=fake_config),
            patch.object(orch, "_setup_observability"),
            patch.object(orch, "run_stage0", return_value=s_reports["stage0"]),
            patch.object(orch, "run_data_pipeline_recon", return_value=s_reports["stage0_data"]),
            patch.object(orch, "run_stage1", return_value=s_reports["stage1"]),
            patch.object(orch, "run_stage2", return_value=s_reports["stage2"]),
            patch.object(orch, "run_stage3", return_value=s_reports["stage3"]),
            patch.object(orch, "run_stage3b", return_value=s3b_with_devs),
            patch.object(orch, "run_stage3c", return_value=s_reports["stage3c"]),
            patch.object(orch, "run_stage4", return_value=s_reports["stage4"]),
            patch.object(orch, "run_stage5", return_value=s_reports["stage5"]),
            patch.object(orch, "log_event", side_effect=mock_log_event),
        ):
            report = orch.run_reconciliation("2026-03-15", dry_run=True)

        assert "BATCH_LIVE_RECON_DRIFT" in logged_events

    def test_stage4_gcs_path_set_on_report(self) -> None:
        """When stage4 returns output_gcs_path, it's set on the report."""
        from batch_live_reconciliation_service.engine import orchestrator as orch

        fake_config = MagicMock()
        fake_config.gcp_project_id = "p"
        fake_config.events_bucket = "e"

        s4_with_path = StageReport(
            stage=ReconStage.AGENT_ANALYSIS,
            status=ReconStatus.PASSED,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            output_gcs_path="gs://bucket/agent_report_2026-03-15.md",
        )
        s5_with_path = StageReport(
            stage=ReconStage.RESULTS_WRITER,
            status=ReconStatus.PASSED,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            output_gcs_path="gs://bucket/summary_2026-03-15.json",
        )

        def _make(stage: ReconStage) -> StageReport:
            return StageReport(
                stage=stage,
                status=ReconStatus.PASSED,
                started_at=datetime.now(UTC),
                completed_at=datetime.now(UTC),
            )

        with (
            patch.object(orch, "get_recon_config", return_value=fake_config),
            patch.object(orch, "_setup_observability"),
            patch.object(orch, "run_stage0", return_value=_make(ReconStage.CONFIG_PULL)),
            patch.object(orch, "run_data_pipeline_recon", return_value=_make(ReconStage.DATA_PIPELINE_RECON)),
            patch.object(orch, "run_stage1", return_value=_make(ReconStage.ML_RECON)),
            patch.object(orch, "run_stage2", return_value=_make(ReconStage.STRATEGY_RECON)),
            patch.object(orch, "run_stage3", return_value=_make(ReconStage.EXECUTION_RECON)),
            patch.object(orch, "run_stage3b", return_value=_make(ReconStage.PAPER_LIVE_RECON)),
            patch.object(orch, "run_stage3c", return_value=_make(ReconStage.BATCH_PAPER_RECON)),
            patch.object(orch, "run_stage4", return_value=s4_with_path),
            patch.object(orch, "run_stage5", return_value=s5_with_path),
            patch.object(orch, "log_event"),
        ):
            report = orch.run_reconciliation("2026-03-15", dry_run=True)

        assert report.agent_report_gcs_path == "gs://bucket/agent_report_2026-03-15.md"
        assert report.summary_gcs_path == "gs://bucket/summary_2026-03-15.json"

    def test_failed_non_stage0_stages_produces_failed_report_with_failed_event(self) -> None:
        """When stage 1 fails, orchestrator emits FAILED event with failed_stages."""
        from batch_live_reconciliation_service.engine import orchestrator as orch

        fake_config = MagicMock()
        fake_config.gcp_project_id = "p"
        fake_config.events_bucket = "e"

        s1_failed = StageReport(
            stage=ReconStage.ML_RECON,
            status=ReconStatus.FAILED,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            error_message="ml service unavailable",
        )

        def _make(stage: ReconStage) -> StageReport:
            return StageReport(
                stage=stage,
                status=ReconStatus.PASSED,
                started_at=datetime.now(UTC),
                completed_at=datetime.now(UTC),
            )

        logged_events: list[str] = []

        def mock_log_event(event_type: str, details: object = None) -> None:
            logged_events.append(event_type)

        with (
            patch.object(orch, "get_recon_config", return_value=fake_config),
            patch.object(orch, "_setup_observability"),
            patch.object(orch, "run_stage0", return_value=_make(ReconStage.CONFIG_PULL)),
            patch.object(orch, "run_data_pipeline_recon", return_value=_make(ReconStage.DATA_PIPELINE_RECON)),
            patch.object(orch, "run_stage1", return_value=s1_failed),
            patch.object(orch, "run_stage2", return_value=_make(ReconStage.STRATEGY_RECON)),
            patch.object(orch, "run_stage3", return_value=_make(ReconStage.EXECUTION_RECON)),
            patch.object(orch, "run_stage3b", return_value=_make(ReconStage.PAPER_LIVE_RECON)),
            patch.object(orch, "run_stage3c", return_value=_make(ReconStage.BATCH_PAPER_RECON)),
            patch.object(orch, "run_stage4", return_value=_make(ReconStage.AGENT_ANALYSIS)),
            patch.object(orch, "run_stage5", return_value=_make(ReconStage.RESULTS_WRITER)),
            patch.object(orch, "log_event", side_effect=mock_log_event),
        ):
            report = orch.run_reconciliation("2026-03-15", dry_run=True)

        assert report.status == ReconStatus.FAILED
        assert "FAILED" in logged_events


# ---------------------------------------------------------------------------
# Stage 4: _build_agent_prompt covers lines 44-46, 47 branch (with metrics)
# ---------------------------------------------------------------------------


class TestStage4AgentPrompt:
    def test_build_agent_prompt_with_metrics(self) -> None:
        """Covers the metrics formatting branch (line 47 → 51)."""
        from batch_live_reconciliation_service.stages.stage4_agent_analysis import _build_agent_prompt

        report = StageReport(
            stage=ReconStage.ML_RECON,
            status=ReconStatus.PASSED,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            metrics={"accuracy": 0.98, "coverage": 0.95},
        )
        prompt = _build_agent_prompt("2026-03-15", [report])

        assert "2026-03-15" in prompt
        assert "accuracy" in prompt
        assert "0.9800" in prompt

    def test_build_agent_prompt_with_deviations(self) -> None:
        """Covers the deviation formatting branch (lines 44-46)."""
        from unified_api_contracts.internal.reconciliation import ReconciliationDimension

        from batch_live_reconciliation_service.models.recon_report import DeviationRecord
        from batch_live_reconciliation_service.stages.stage4_agent_analysis import _build_agent_prompt

        dev = DeviationRecord.new(
            metric_name="signal_direction_match_rate",
            stage=ReconStage.ML_RECON,
            actual_value=0.85,
            threshold=0.95,
            direction="below",
            description="Direction match rate below threshold",
            dimension=ReconciliationDimension.STRATEGY_LEVEL_ALLOCATION,
        )
        report = StageReport(
            stage=ReconStage.ML_RECON,
            status=ReconStatus.FAILED,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            deviations=[dev],
        )
        prompt = _build_agent_prompt("2026-03-15", [report])

        assert "signal_direction_match_rate" in prompt
        assert "Direction match rate below threshold" in prompt

    def test_dry_run_stage4_returns_gcs_uri(self) -> None:
        from batch_live_reconciliation_service.stages.stage4_agent_analysis import run_stage4

        cfg = MagicMock()
        cfg.recon_bucket = "bucket"
        report = StageReport(
            stage=ReconStage.ML_RECON,
            status=ReconStatus.PASSED,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
        )
        result = run_stage4(cfg, "2026-03-15", stage_reports=[report], dry_run=True)

        assert result.status == ReconStatus.PASSED
        assert result.output_gcs_path is not None
        assert "agent_report_2026-03-15.md" in (result.output_gcs_path or "")
