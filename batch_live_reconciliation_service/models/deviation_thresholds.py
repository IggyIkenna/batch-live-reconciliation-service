"""
Deviation thresholds per reconciliation stage.

SSOT: unified-trading-codex/08-workflows/t1-batch-dag.md § Deviation Metrics and Thresholds
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MLThresholds:
    """Stage 1: ML reconciliation alert thresholds."""

    signal_direction_match_rate_min: float = 0.95  # < 95% → alert
    signal_magnitude_mae_max: float = 0.1  # > 0.1 → alert
    instrument_coverage_pct_min: float = 0.90  # < 90% → alert
    latency_delta_ms_max: float = 5000.0  # > 5000ms → alert


@dataclass(frozen=True)
class StrategyThresholds:
    """Stage 2: Strategy reconciliation alert thresholds."""

    instruction_alignment_pct_min: float = 0.85  # < 85% → alert
    benchmark_pnl_delta_max: float = 0.02  # > 2% of notional → alert
    position_snapshot_delta_max: float = 1.0  # > 1 unit per instrument → alert
    var_delta_pct_max: float = 0.10  # > 10% VaR deviation → alert


@dataclass(frozen=True)
class ExecutionThresholds:
    """Stage 3: Execution reconciliation alert thresholds."""

    alpha_pnl_gap_max: float = 0.01  # > 1% of notional → alert
    fill_rate_delta_max: float = 0.05  # > 5% → alert
    slippage_delta_bps_max: float = 10.0  # > 10 bps → alert
    algo_selection_accuracy_min: float = 0.99  # < 99% → alert
    order_latency_p99_ms_max: float = 600.0  # > 600ms (500ms gate + 100ms tolerance) → alert
    # Absolute (not delta) recon green-gate bounds — composed with RECON_GREEN_THRESHOLDS
    # per-archetype values in the orchestrator. Defaults track the most conservative
    # carry_staked_basis archetype bound (drawdown 2.0% / fill_rate 0.95).
    drawdown_pct_max: float = 2.0  # live drawdown > 2.0% → not green
    fill_rate_min: float = 0.95  # live fill rate < 95% → not green


@dataclass(frozen=True)
class DataPipelineThresholds:
    """Stage 0.5: Data pipeline reconciliation alert thresholds."""

    file_count_match_rate_min: float = 0.95  # < 95% file count match → alert
    row_count_match_rate_min: float = 0.95  # < 95% row count match → alert
    missing_live_partition_max: int = 0  # > 0 services with no live partition → alert


@dataclass(frozen=True)
class PaperLiveThresholds:
    """Stage 3b: Paper-vs-live thresholds.

    Tighter than batch-vs-live: same data + similar API conditions mean larger gaps
    indicate a paper-mode wiring bug, not normal batch/live divergence.
    """

    alpha_pnl_gap_max: float = 0.005  # > 0.5% → alert (vs batch-live 1%)
    fill_rate_delta_max: float = 0.02  # > 2% → alert (vs batch-live 5%)
    slippage_delta_bps_max: float = 5.0  # > 5 bps → alert (vs batch-live 10 bps)
    order_latency_p99_ms_max: float = 400.0  # > 400ms → alert (paper should be faster)
    algo_selection_accuracy_min: float = 0.99  # < 99% → alert (same bar as live)


@dataclass(frozen=True)
class BatchPaperThresholds:
    """Stage 3c: Batch-vs-paper thresholds.

    Bounded by matching-engine fidelity: the matching engine is a lower-fidelity
    approximation of real fills, so tolerances are wider than paper-vs-live.
    """

    pnl_delta_pct_max: float = 0.05  # > 5% → alert (matching engine less precise)
    position_delta_max: float = 5.0  # > 5 units per instrument → alert
    fill_count_delta_pct_max: float = 0.15  # > 15% → alert (slippage model differences)
    latency_model_delta_ms_max: float = 200.0  # > 200ms model vs actual → alert


@dataclass(frozen=True)
class DataPipelineThresholds:
    """Stage 0.5: Data pipeline reconciliation alert thresholds."""

    file_count_match_rate_min: float = 0.95  # < 95% file count match → alert
    row_count_match_rate_min: float = 0.95  # < 95% row count match → alert
    missing_live_partition_max: int = 0  # > 0 services with no live partition → alert


@dataclass(frozen=True)
class PaperLiveThresholds:
    """Stage 3b: Paper-vs-live thresholds.

    Tighter than batch-vs-live: same data + similar API conditions mean larger gaps
    indicate a paper-mode wiring bug, not normal batch/live divergence.
    """

    alpha_pnl_gap_max: float = 0.005  # > 0.5% → alert (vs batch-live 1%)
    fill_rate_delta_max: float = 0.02  # > 2% → alert (vs batch-live 5%)
    slippage_delta_bps_max: float = 5.0  # > 5 bps → alert (vs batch-live 10 bps)
    order_latency_p99_ms_max: float = 400.0  # > 400ms → alert (paper should be faster)
    algo_selection_accuracy_min: float = 0.99  # < 99% → alert (same bar as live)


@dataclass(frozen=True)
class BatchPaperThresholds:
    """Stage 3c: Batch-vs-paper thresholds.

    Bounded by matching-engine fidelity: the matching engine is a lower-fidelity
    approximation of real fills, so tolerances are wider than paper-vs-live.
    """

    pnl_delta_pct_max: float = 0.05  # > 5% → alert (matching engine less precise)
    position_delta_max: float = 5.0  # > 5 units per instrument → alert
    fill_count_delta_pct_max: float = 0.15  # > 15% → alert (slippage model differences)
    latency_model_delta_ms_max: float = 200.0  # > 200ms model vs actual → alert


ML_THRESHOLDS = MLThresholds()
STRATEGY_THRESHOLDS = StrategyThresholds()
EXECUTION_THRESHOLDS = ExecutionThresholds()
DATA_PIPELINE_THRESHOLDS = DataPipelineThresholds()
PAPER_LIVE_THRESHOLDS = PaperLiveThresholds()
BATCH_PAPER_THRESHOLDS = BatchPaperThresholds()
