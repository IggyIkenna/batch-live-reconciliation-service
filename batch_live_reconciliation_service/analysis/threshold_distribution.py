"""Distribution analyzer for the recon green-gate metrics.

Loads a set of past ``summary_{date}.json`` files (written by
``stages/stage5_results_writer.py`` to ``t1-recon/recon/summary_{date}.json`` in
``ReconConfig.recon_bucket``) and computes the distribution of the gate metrics —
``slippage_delta_bps`` plus the absolute green-gate metrics ``drawdown_pct`` /
``fill_rate`` and the other execution deltas. For each metric it reports count,
mean, p50/p90/p95/max and how many days would pass / fail the current threshold.

Read path mirrors the cloud-interface helpers used by stage5 for writes
(``get_storage_client`` → ``bucket().list_blobs()`` + ``download_bytes()``).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import cast

from unified_trading_library import get_storage_client

from batch_live_reconciliation_service.config import ReconConfig
from batch_live_reconciliation_service.models.deviation_thresholds import (
    EXECUTION_THRESHOLDS,
    PAPER_LIVE_THRESHOLDS,
)

logger = logging.getLogger(__name__)

_SUMMARY_PREFIX = "t1-recon/recon/summary_"

# Gate metric -> (threshold value, direction) where direction is "above" (a value
# strictly greater than the threshold FAILS) or "below" (a value strictly less than
# the threshold FAILS). Thresholds resolve from the dataclass SSOT.
_GATE_METRICS: dict[str, tuple[float, str]] = {
    "slippage_delta_bps": (EXECUTION_THRESHOLDS.slippage_delta_bps_max, "above"),
    "fill_rate_delta": (EXECUTION_THRESHOLDS.fill_rate_delta_max, "above"),
    "alpha_pnl_gap": (EXECUTION_THRESHOLDS.alpha_pnl_gap_max, "above"),
    "order_latency_p99_ms": (EXECUTION_THRESHOLDS.order_latency_p99_ms_max, "above"),
    "drawdown_pct": (EXECUTION_THRESHOLDS.drawdown_pct_max, "above"),
    "fill_rate": (EXECUTION_THRESHOLDS.fill_rate_min, "below"),
    "algo_selection_accuracy": (EXECUTION_THRESHOLDS.algo_selection_accuracy_min, "below"),
}

# Paper-vs-live (stage3b) slippage uses the tighter PaperLive threshold; tracked
# separately because the same metric name can appear in multiple stages.
_PAPER_LIVE_SLIPPAGE_THRESHOLD = PAPER_LIVE_THRESHOLDS.slippage_delta_bps_max


@dataclass(frozen=True)
class MetricDistribution:
    """Distribution + pass/fail summary for a single gate metric."""

    metric_name: str
    threshold: float
    direction: str  # "above" → >threshold fails; "below" → <threshold fails
    count: int
    mean: float
    p50: float
    p90: float
    p95: float
    max_value: float
    pass_count: int
    fail_count: int


@dataclass(frozen=True)
class ThresholdDistributionResult:
    """Result of analysing gate-metric distributions over a set of summaries."""

    summary_count: int
    dates: list[str]
    distributions: dict[str, MetricDistribution] = field(default_factory=dict)


def _percentile(sorted_values: list[float], pct: float) -> float:
    """Nearest-rank percentile over a pre-sorted ascending list (0.0 <= pct <= 1.0)."""
    if not sorted_values:
        return 0.0
    if pct <= 0.0:
        return sorted_values[0]
    if pct >= 1.0:
        return sorted_values[-1]
    rank = max(0, min(len(sorted_values) - 1, int(round(pct * (len(sorted_values) - 1)))))
    return sorted_values[rank]


def _metric_values_from_summary(summary: dict[str, object]) -> dict[str, float]:
    """Flatten the per-stage ``metrics`` dicts of one summary into metric->value.

    Later stages win on key collisions; only float-coercible values are kept.
    """
    out: dict[str, float] = {}
    stages_obj: object = summary.get("stages") or []
    if not isinstance(stages_obj, list):
        return out
    for stage_obj in cast(list[object], stages_obj):
        if not isinstance(stage_obj, dict):
            continue
        metrics_obj: object = cast(dict[str, object], stage_obj).get("metrics") or {}
        if not isinstance(metrics_obj, dict):
            continue
        for key, value in cast(dict[str, object], metrics_obj).items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                out[key] = float(value)
    return out


def _build_distribution(metric_name: str, values: list[float]) -> MetricDistribution:
    threshold, direction = _GATE_METRICS[metric_name]
    sorted_values = sorted(values)
    count = len(sorted_values)
    mean = sum(sorted_values) / count if count else 0.0
    if direction == "above":
        fail_count = sum(1 for v in sorted_values if v > threshold)
    else:
        fail_count = sum(1 for v in sorted_values if v < threshold)
    return MetricDistribution(
        metric_name=metric_name,
        threshold=threshold,
        direction=direction,
        count=count,
        mean=mean,
        p50=_percentile(sorted_values, 0.50),
        p90=_percentile(sorted_values, 0.90),
        p95=_percentile(sorted_values, 0.95),
        max_value=sorted_values[-1] if sorted_values else 0.0,
        pass_count=count - fail_count,
        fail_count=fail_count,
    )


def _load_summaries(config: ReconConfig, *, limit: int | None) -> list[tuple[str, dict[str, object]]]:
    """List + load summary JSONs from ``recon_bucket`` newest-first.

    Returns ``(date, summary)`` tuples. Mirrors stage5's read helpers.
    """
    client = get_storage_client()
    bucket_obj = client.bucket(config.recon_bucket)
    blob_names: list[str] = [
        str(blob.name) for blob in bucket_obj.list_blobs(prefix=_SUMMARY_PREFIX) if str(blob.name).endswith(".json")
    ]
    blob_names.sort(reverse=True)  # filename embeds the date → lexical sort == chronological
    if limit is not None:
        blob_names = blob_names[:limit]
    summaries: list[tuple[str, dict[str, object]]] = []
    for blob_name in blob_names:
        try:
            raw = client.download_bytes(bucket=config.recon_bucket, blob_path=blob_name)
            summary = cast(dict[str, object], json.loads(raw.decode("utf-8")))
        except (ValueError, TypeError, KeyError, AttributeError, RuntimeError, OSError) as exc:
            logger.warning("Skipping unreadable summary %s: %s", blob_name, exc)
            continue
        date_obj: object = summary.get("date")
        date = date_obj if isinstance(date_obj, str) else blob_name
        summaries.append((date, summary))
    return summaries


def analyze_threshold_distribution(
    config: ReconConfig,
    *,
    limit: int | None = None,
) -> ThresholdDistributionResult:
    """Analyse gate-metric distributions across past recon summaries.

    Args:
        config: ReconConfig (``recon_bucket`` is where summaries live).
        limit: Optional cap on the number of (newest) summaries to analyse.

    Returns:
        ThresholdDistributionResult with one MetricDistribution per gate metric
        that appeared in at least one summary.
    """
    summaries = _load_summaries(config, limit=limit)
    dates: list[str] = [date for date, _ in summaries]

    collected: dict[str, list[float]] = {metric: [] for metric in _GATE_METRICS}
    for _, summary in summaries:
        flat = _metric_values_from_summary(summary)
        for metric in _GATE_METRICS:
            if metric in flat:
                collected[metric].append(flat[metric])

    distributions: dict[str, MetricDistribution] = {
        metric: _build_distribution(metric, values) for metric, values in collected.items() if values
    }
    return ThresholdDistributionResult(
        summary_count=len(summaries),
        dates=dates,
        distributions=distributions,
    )
