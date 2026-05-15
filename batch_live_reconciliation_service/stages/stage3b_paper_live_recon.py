"""
Stage 3b: Paper-vs-live execution reconciliation (pvl-p21a).

Compares paper-mode execution events against live-mode execution events for the
same date. Thresholds are tighter than batch-vs-live (stage3) because paper and
live consume the same data + similar API conditions — large gaps indicate a
paper-mode wiring bug, not normal batch/live divergence.

Failure routing:
  - alpha_pnl_gap, fill_rate_delta, slippage_delta → AUTO_DEMOTE_TO_PAPER
  - order_latency_p99, algo_selection_accuracy → ALERT only

See: models/deviation_thresholds.py (PaperLiveThresholds) +
     models/recon_report.py (FailureRoutingAction).
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import NamedTuple

from unified_trading_library import get_storage_client, log_event

from batch_live_reconciliation_service.config import ReconConfig
from batch_live_reconciliation_service.models.deviation_thresholds import (
    PAPER_LIVE_THRESHOLDS,
    PaperLiveThresholds,
)
from batch_live_reconciliation_service.models.recon_report import (
    DeviationRecord,
    FailureRoutingAction,
    ReconStage,
    ReconStatus,
    StageReport,
)

logger = logging.getLogger(__name__)


class _PaperLiveDeviation(NamedTuple):
    record: DeviationRecord
    routing_action: FailureRoutingAction


def _load_events(bucket_name: str, prefix: str) -> list[dict[str, float]]:
    """Load execution events from GCS prefix. Returns empty list on missing prefix."""
    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    if not blobs:
        logger.warning("[Stage 3b] No blobs at gs://%s/%s", bucket_name, prefix)
        return []

    events: list[dict[str, float]] = []
    for blob in blobs:
        raw = blob.download_as_text()
        for line in raw.splitlines():
            line = line.strip()
            if line:
                events.append(json.loads(line))
    return events


def _compute_metrics(
    paper_events: list[dict[str, float]],
    live_events: list[dict[str, float]],
) -> dict[str, float]:
    """Compute paper-vs-live deviation metrics."""
    if not paper_events or not live_events:
        return {
            "alpha_pnl_gap": 0.0,
            "fill_rate_delta": 0.0,
            "slippage_delta_bps": 0.0,
            "order_latency_p99_ms": 0.0,
            "algo_selection_accuracy": 1.0,
            "paper_event_count": float(len(paper_events)),
            "live_event_count": float(len(live_events)),
        }

    def _avg(events: list[dict[str, float]], key: str) -> float:
        vals = [e[key] for e in events if key in e]
        return sum(vals) / len(vals) if vals else 0.0

    def _p99(events: list[dict[str, float]], key: str) -> float:
        vals = sorted(e[key] for e in events if key in e)
        if not vals:
            return 0.0
        idx = max(0, int(len(vals) * 0.99) - 1)
        return vals[idx]

    paper_pnl = _avg(paper_events, "realized_pnl")
    live_pnl = _avg(live_events, "realized_pnl")
    alpha_pnl_gap = abs(paper_pnl - live_pnl) / max(abs(live_pnl), 1e-9)

    paper_fill_rate = _avg(paper_events, "fill_rate")
    live_fill_rate = _avg(live_events, "fill_rate")
    fill_rate_delta = abs(paper_fill_rate - live_fill_rate)

    paper_slip = _avg(paper_events, "slippage_bps")
    live_slip = _avg(live_events, "slippage_bps")
    slippage_delta_bps = abs(paper_slip - live_slip)

    order_latency_p99_ms = _p99(paper_events, "order_latency_ms")

    paper_algo_correct = [e.get("algo_correct", 1.0) for e in paper_events]
    algo_selection_accuracy = (
        sum(paper_algo_correct) / len(paper_algo_correct) if paper_algo_correct else 1.0
    )

    return {
        "alpha_pnl_gap": alpha_pnl_gap,
        "fill_rate_delta": fill_rate_delta,
        "slippage_delta_bps": slippage_delta_bps,
        "order_latency_p99_ms": order_latency_p99_ms,
        "algo_selection_accuracy": algo_selection_accuracy,
        "paper_event_count": float(len(paper_events)),
        "live_event_count": float(len(live_events)),
    }


def _check_deviations(
    metrics: dict[str, float],
    t: PaperLiveThresholds = PAPER_LIVE_THRESHOLDS,
) -> list[_PaperLiveDeviation]:
    """Check paper-vs-live metrics against thresholds. Returns deviations with routing actions."""
    results: list[_PaperLiveDeviation] = []

    above_checks = [
        (
            "alpha_pnl_gap",
            metrics["alpha_pnl_gap"],
            t.alpha_pnl_gap_max,
            (
                f"Paper-vs-live alpha PnL gap {metrics['alpha_pnl_gap']:.2%}"
                f" > {t.alpha_pnl_gap_max:.2%}"
            ),
            FailureRoutingAction.AUTO_DEMOTE_TO_PAPER,
        ),
        (
            "fill_rate_delta",
            metrics["fill_rate_delta"],
            t.fill_rate_delta_max,
            (
                f"Paper-vs-live fill rate delta {metrics['fill_rate_delta']:.2%}"
                f" > {t.fill_rate_delta_max:.2%}"
            ),
            FailureRoutingAction.AUTO_DEMOTE_TO_PAPER,
        ),
        (
            "slippage_delta_bps",
            metrics["slippage_delta_bps"],
            t.slippage_delta_bps_max,
            (
                f"Paper-vs-live slippage delta {metrics['slippage_delta_bps']:.1f}bps"
                f" > {t.slippage_delta_bps_max:.1f}bps"
            ),
            FailureRoutingAction.AUTO_DEMOTE_TO_PAPER,
        ),
        (
            "order_latency_p99_ms",
            metrics["order_latency_p99_ms"],
            t.order_latency_p99_ms_max,
            (
                f"Paper P99 latency {metrics['order_latency_p99_ms']:.0f}ms"
                f" > {t.order_latency_p99_ms_max:.0f}ms"
            ),
            FailureRoutingAction.ALERT,
        ),
    ]

    for name, actual, threshold, desc, action in above_checks:
        if actual > threshold:
            results.append(
                _PaperLiveDeviation(
                    record=DeviationRecord(
                        metric_name=name,
                        stage=ReconStage.PAPER_LIVE_RECON,
                        actual_value=actual,
                        threshold=threshold,
                        direction="above",
                        description=desc,
                    ),
                    routing_action=action,
                )
            )

    accuracy = metrics["algo_selection_accuracy"]
    if accuracy < t.algo_selection_accuracy_min:
        results.append(
            _PaperLiveDeviation(
                record=DeviationRecord(
                    metric_name="algo_selection_accuracy",
                    stage=ReconStage.PAPER_LIVE_RECON,
                    actual_value=accuracy,
                    threshold=t.algo_selection_accuracy_min,
                    direction="below",
                    description=(
                        f"Paper algo selection accuracy {accuracy:.1%}"
                        f" < {t.algo_selection_accuracy_min:.0%}"
                    ),
                ),
                routing_action=FailureRoutingAction.ALERT,
            )
        )

    return results


def run_stage3b(config: ReconConfig, date: str, dry_run: bool = False) -> StageReport:
    """Run Stage 3b: Paper-vs-live execution reconciliation (pvl-p21a)."""
    started_at = datetime.now(UTC)
    log_event("PROCESSING_STARTED", details={"stage": "stage3b_paper_live_recon", "date": date})
    logger.info("[Stage 3b] Paper-vs-live reconciliation for date=%s", date)

    if dry_run:
        return StageReport(
            stage=ReconStage.PAPER_LIVE_RECON,
            status=ReconStatus.PASSED,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            metrics={"dry_run": 1.0},
        )

    paper_events = _load_events(config.events_bucket, f"paper/events/{date}/execution-service/")
    live_events = _load_events(config.events_bucket, f"live/events/{date}/execution-service/")

    logger.info("[Stage 3b] %d paper, %d live events", len(paper_events), len(live_events))

    metrics = _compute_metrics(paper_events, live_events)
    deviations_with_routing = _check_deviations(metrics)

    deviations = [d.record for d in deviations_with_routing]
    status = ReconStatus.PASSED if not deviations else ReconStatus.FAILED

    for d in deviations_with_routing:
        logger.warning(
            "[Stage 3b] deviation=%s routing=%s desc=%s",
            d.record.metric_name,
            d.routing_action.value,
            d.record.description,
        )
        log_event(
            "PAPER_LIVE_DEVIATION",
            details={
                "metric": d.record.metric_name,
                "routing_action": d.routing_action.value,
                "actual": d.record.actual_value,
                "threshold": d.record.threshold,
                "date": date,
            },
        )

    log_event(
        "PROCESSING_COMPLETED",
        details={
            "stage": "stage3b_paper_live_recon",
            "status": status.value,
            "deviations": len(deviations),
        },
    )
    return StageReport(
        stage=ReconStage.PAPER_LIVE_RECON,
        status=status,
        started_at=started_at,
        completed_at=datetime.now(UTC),
        deviations=deviations,
        metrics=metrics,
    )
