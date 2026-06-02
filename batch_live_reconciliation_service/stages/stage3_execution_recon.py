"""
Stage 3 — Execution Reconciliation.

Compares batch T+1 execution outputs vs live fill/order events.

Batch:  t1-recon/events/{date}/execution-service/
Live:   live/events/{date}/execution-service/

Deviation metrics:
  - alpha_pnl_gap          (> 1% of notional → alert)
  - fill_rate_delta        (> 5% → alert)
  - slippage_delta_bps     (> 10 bps → alert)
  - algo_selection_accuracy (< 99% → alert)
  - order_latency_p99_ms   (> 600ms → alert)
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import cast

from unified_api_contracts.internal.reconciliation import ReconciliationDimension
from unified_trading_library import get_storage_client, log_event

from batch_live_reconciliation_service.config import ReconConfig
from batch_live_reconciliation_service.models.deviation_thresholds import EXECUTION_THRESHOLDS
from batch_live_reconciliation_service.models.recon_report import (
    DeviationRecord,
    ReconStage,
    ReconStatus,
    StageReport,
)

logger = logging.getLogger(__name__)


def _load_events(bucket: str, prefix: str) -> list[dict[str, object]]:
    client = get_storage_client()
    events: list[dict[str, object]] = []
    try:
        bucket_obj = client.bucket(bucket)
        for blob in bucket_obj.list_blobs(prefix=prefix):
            if not str(blob.name).endswith((".ndjson", ".jsonl")):
                continue
            raw = client.download_bytes(bucket=bucket, blob_path=str(blob.name))
            for line in raw.decode("utf-8").splitlines():
                line = line.strip()
                if line:
                    events.append(cast(dict[str, object], json.loads(line)))
    except (ValueError, TypeError, KeyError, AttributeError, RuntimeError) as e:
        logger.warning("Error loading events from gs://%s/%s: %s", bucket, prefix, e)
    return events


def _live_drawdown_pct(live_fills: list[dict[str, object]]) -> float:
    """Peak-to-trough drawdown (pct) of the cumulative live-fill notional P&L.

    Walks live fills in event order, accumulating ``fill_price * filled_qty``
    (signed by ``side``: SELL credits, BUY debits). The drawdown is the largest
    peak-to-trough decline expressed as a percentage of the running peak. Returns
    0.0 when there is no decline or no usable peak.
    """
    running = 0.0
    peak = 0.0
    max_drawdown_pct = 0.0
    for fill in live_fills:
        price = float(cast(float, fill.get("fill_price", 0.0)))
        qty = float(cast(float, fill.get("filled_qty", 0.0)))
        side = str(fill.get("side")).upper()
        signed = price * qty * (-1.0 if side == "BUY" else 1.0)
        running += signed
        peak = max(peak, running)
        if peak > 0.0:
            drawdown_pct = (peak - running) / peak * 100.0
            max_drawdown_pct = max(max_drawdown_pct, drawdown_pct)
    return max_drawdown_pct


def _compute_metrics(
    batch_events: list[dict[str, object]],
    live_events: list[dict[str, object]],
) -> dict[str, float]:
    if not live_events:
        return {
            "alpha_pnl_gap": 0.0,
            "fill_rate_delta": 0.0,
            "slippage_delta_bps": 0.0,
            "algo_selection_accuracy": 1.0,
            "order_latency_p99_ms": 0.0,
            # Absolute green-gate metrics — no live data → neutral pass values
            # (fill_rate at the floor; drawdown at 0) so an empty live day never
            # spuriously trips the green gate.
            "fill_rate": 1.0,
            "drawdown_pct": 0.0,
            "batch_fill_count": float(len(batch_events)),
            "live_fill_count": 0.0,
        }

    def fills(evs: list[dict[str, object]]) -> list[dict[str, object]]:
        return [e for e in evs if e.get("event_type") == "FILL"]

    live_fills = fills(live_events)
    batch_fills = fills(batch_events)

    # Alpha P&L gap: live fills P&L - batch fills P&L (aggregate)
    live_pnl = sum(
        float(cast(float, e.get("fill_price", 0.0))) * float(cast(float, e.get("filled_qty", 0.0))) for e in live_fills
    )
    batch_pnl = sum(
        float(cast(float, e.get("fill_price", 0.0))) * float(cast(float, e.get("filled_qty", 0.0))) for e in batch_fills
    )
    notional = abs(live_pnl) or 1.0
    alpha_gap = abs(live_pnl - batch_pnl) / notional

    # Per-archetype P&L gap in bps — consumed by orchestrator for RECON_GREEN_THRESHOLDS check.
    # Events carry optional `archetype` field; unknown/absent archetype is skipped.
    per_archetype_bps: dict[str, float] = {}
    for archetype in set(str(e["archetype"]) for evs in (live_fills, batch_fills) for e in evs if e.get("archetype")):
        arc_live_pnl = sum(
            float(cast(float, e.get("fill_price", 0.0))) * float(cast(float, e.get("filled_qty", 0.0)))
            for e in live_fills
            if e.get("archetype") == archetype
        )
        arc_batch_pnl = sum(
            float(cast(float, e.get("fill_price", 0.0))) * float(cast(float, e.get("filled_qty", 0.0)))
            for e in batch_fills
            if e.get("archetype") == archetype
        )
        arc_notional = abs(arc_live_pnl) or 1.0
        per_archetype_bps[archetype] = abs(arc_live_pnl - arc_batch_pnl) / arc_notional * 10_000

    # Fill rate: filled orders / submitted orders
    live_submitted = sum(1 for e in live_events if e.get("event_type") == "ORDER_SUBMITTED")
    batch_submitted = sum(1 for e in batch_events if e.get("event_type") == "ORDER_SUBMITTED")
    live_fill_rate = len(live_fills) / max(live_submitted, 1)
    batch_fill_rate = len(batch_fills) / max(batch_submitted, 1)
    fill_rate_delta = abs(live_fill_rate - batch_fill_rate)

    # Mean slippage deviation
    live_slippage = [float(cast(float, e.get("slippage_bps", 0.0))) for e in live_fills]
    batch_slippage = [float(cast(float, e.get("slippage_bps", 0.0))) for e in batch_fills]
    mean_live_slip = sum(live_slippage) / max(len(live_slippage), 1)
    mean_batch_slip = sum(batch_slippage) / max(len(batch_slippage), 1)
    slippage_delta = abs(mean_live_slip - mean_batch_slip)

    # Algo selection accuracy
    algo_events = [e for e in live_events if e.get("event_type") == "ORDER_SUBMITTED"]
    algo_correct = sum(1 for e in algo_events if e.get("algo_used") == e.get("algo_configured"))
    algo_accuracy = algo_correct / max(len(algo_events), 1)

    # P99 latency from live orders
    latencies = [float(cast(float, e.get("latency_ms", 0.0))) for e in live_fills if e.get("latency_ms")]
    latencies.sort()
    p99_latency = latencies[int(len(latencies) * 0.99)] if latencies else 0.0

    # Absolute live drawdown (pct) from the running notional P&L of live fills, in
    # event order. Peak-to-trough on the cumulative signed value; consumed by the
    # orchestrator green gate alongside RECON_GREEN_THRESHOLDS["drawdown_pct"].
    drawdown_pct = _live_drawdown_pct(live_fills)

    metrics: dict[str, float] = {
        "alpha_pnl_gap": alpha_gap,
        "fill_rate_delta": fill_rate_delta,
        "slippage_delta_bps": slippage_delta,
        "algo_selection_accuracy": algo_accuracy,
        "order_latency_p99_ms": p99_latency,
        "fill_rate": live_fill_rate,
        "drawdown_pct": drawdown_pct,
        "batch_fill_count": float(len(batch_fills)),
        "live_fill_count": float(len(live_fills)),
    }
    for archetype, bps_val in per_archetype_bps.items():
        metrics[f"alpha_pnl_gap_bps_{archetype}"] = bps_val
    return metrics


def _check_deviations(metrics: dict[str, float]) -> list[DeviationRecord]:
    t = EXECUTION_THRESHOLDS
    deviations: list[DeviationRecord] = []

    checks: list[tuple[str, float, float, str, str]] = [
        (
            "alpha_pnl_gap",
            metrics["alpha_pnl_gap"],
            t.alpha_pnl_gap_max,
            "above",
            f"Alpha P&L gap {metrics['alpha_pnl_gap']:.1%} > {t.alpha_pnl_gap_max:.0%} of notional",
        ),
        (
            "fill_rate_delta",
            metrics["fill_rate_delta"],
            t.fill_rate_delta_max,
            "above",
            f"Fill rate delta {metrics['fill_rate_delta']:.1%} > {t.fill_rate_delta_max:.0%}",
        ),
        (
            "slippage_delta_bps",
            metrics["slippage_delta_bps"],
            t.slippage_delta_bps_max,
            "above",
            (f"Slippage delta {metrics['slippage_delta_bps']:.1f}bps > {t.slippage_delta_bps_max:.0f}bps"),
        ),
        (
            "order_latency_p99_ms",
            metrics["order_latency_p99_ms"],
            t.order_latency_p99_ms_max,
            "above",
            (f"P99 latency {metrics['order_latency_p99_ms']:.0f}ms > {t.order_latency_p99_ms_max:.0f}ms"),
        ),
    ]

    for name, actual, threshold, direction, desc in checks:
        if direction == "above" and actual > threshold:
            deviations.append(
                DeviationRecord.new(
                    metric_name=name,
                    stage=ReconStage.EXECUTION_RECON,
                    actual_value=actual,
                    threshold=threshold,
                    direction=direction,
                    description=desc,
                    dimension=ReconciliationDimension.FILLS,
                )
            )

    if metrics["algo_selection_accuracy"] < t.algo_selection_accuracy_min:
        deviations.append(
            DeviationRecord.new(
                metric_name="algo_selection_accuracy",
                stage=ReconStage.EXECUTION_RECON,
                actual_value=metrics["algo_selection_accuracy"],
                threshold=t.algo_selection_accuracy_min,
                direction="below",
                description=(
                    f"Algo selection accuracy {metrics['algo_selection_accuracy']:.1%} "
                    f"< {t.algo_selection_accuracy_min:.0%}"
                ),
                dimension=ReconciliationDimension.FILLS,
            )
        )

    return deviations


def run_stage3(config: ReconConfig, date: str, dry_run: bool = False) -> StageReport:
    """Run Stage 3: Execution Reconciliation."""
    started_at = datetime.now(UTC)
    log_event("PROCESSING_STARTED", details={"stage": "stage3_execution_recon", "date": date})
    logger.info("[Stage 3] Execution reconciliation for date=%s", date)

    if dry_run:
        return StageReport(
            stage=ReconStage.EXECUTION_RECON,
            status=ReconStatus.PASSED,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            metrics={"dry_run": 1.0},
        )

    batch_events = _load_events(config.events_bucket, f"t1-recon/events/{date}/execution-service/")
    live_events = _load_events(config.events_bucket, f"live/events/{date}/execution-service/")

    logger.info("[Stage 3] %d batch, %d live events", len(batch_events), len(live_events))

    metrics = _compute_metrics(batch_events, live_events)
    deviations = _check_deviations(metrics)
    status = ReconStatus.PASSED if not deviations else ReconStatus.FAILED

    for d in deviations:
        logger.warning("[Stage 3] deviation: %s", d.description)

    log_event(
        "PROCESSING_COMPLETED",
        details={
            "stage": "stage3_execution_recon",
            "status": status.value,
            "deviations": len(deviations),
        },
    )
    return StageReport(
        stage=ReconStage.EXECUTION_RECON,
        status=status,
        started_at=started_at,
        completed_at=datetime.now(UTC),
        deviations=deviations,
        metrics=metrics,
    )
