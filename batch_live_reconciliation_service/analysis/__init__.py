"""Offline analysis helpers for batch-live-reconciliation-service.

These modules read the consolidated ``summary_{date}.json`` outputs written by
``stages/stage5_results_writer.py`` and compute distributional statistics over
the recon gate metrics — used to calibrate the deviation thresholds.
"""

from __future__ import annotations

from batch_live_reconciliation_service.analysis.threshold_distribution import (
    MetricDistribution,
    ThresholdDistributionResult,
    analyze_threshold_distribution,
)

__all__ = [
    "MetricDistribution",
    "ThresholdDistributionResult",
    "analyze_threshold_distribution",
]
