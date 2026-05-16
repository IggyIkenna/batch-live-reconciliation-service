"""
Stage 3c: Batch-vs-paper execution reconciliation (pvl-p21a).

Compares batch-mode execution events (matching-engine simulated fills) against
paper-mode execution events (testnet / devnet real API calls). Thresholds are
wider than paper-vs-live (stage3b) because the matching engine is a lower-fidelity
approximation of real fills — differences reflect model imprecision, not bugs.

Failure routing:
  - pnl_delta, position_delta → ALERT only (model fidelity gap, not a wiring issue)
  - fill_count_delta → ALERT only
  - latency_model_delta → ALERT only

See: models/deviation_thresholds.py (BatchPaperThresholds).
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

from unified_trading_library import get_storage_client, log_event

from batch_live_reconciliation_service.config import ReconConfig
from batch_live_reconciliation_service.models.deviation_thresholds import (
    BATCH_PAPER_THRESHOLDS,
    BatchPaperThresholds,
)
from batch_live_reconciliation_service.models.recon_report import (
    DeviationRecord,
    FailureRoutingAction,
    ReconStage,
    ReconStatus,
    StageReport,
)

logger = logging.getLogger(__name__)


def _load_events(bucket_name: str, prefix: str) -> list[dict[str, float]]:
    """Load execution events from GCS prefix. Returns empty list on missing prefix."""
    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    if not blobs:
        logger.warning("[Stage 3c] No blobs at gs://%s/%s", bucket_name, prefix)
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
    batch_events: list[dict[str, float]],
    paper_events: list[dict[str, float]],
) -> dict[str, float]:
    """Compute batch-vs-paper deviation metrics."""
    if not batch_events or not paper_events:
        return {
            "pnl_delta_pct": 0.0,
            "position_delta": 0.0,
            "fill_count_delta_pct": 0.0,
            "latency_model_delta_ms": 0.0,
            "batch_event_count": float(len(batch_events)),
            "paper_event_count": float(len(paper_events)),
        }

    def _avg(events: list[dict[str, float]], key: str) -> float:
        vals = [e[key] for e in events if key in e]
        return sum(vals) / len(vals) if vals else 0.0

    batch_pnl = _avg(batch_events, "realized_pnl")
    paper_pnl = _avg(paper_events, "realized_pnl")
    pnl_delta_pct = abs(batch_pnl - paper_pnl) / max(abs(paper_pnl), 1e-9)

    batch_pos = _avg(batch_events, "position_size")
    paper_pos = _avg(paper_events, "position_size")
    position_delta = abs(batch_pos - paper_pos)

    batch_fills = float(len(batch_events))
    paper_fills = float(len(paper_events))
    fill_count_delta_pct = abs(batch_fills - paper_fills) / max(paper_fills, 1.0)

    batch_lat = _avg(batch_events, "order_latency_ms")
    paper_lat = _avg(paper_events, "order_latency_ms")
    latency_model_delta_ms = abs(batch_lat - paper_lat)

    return {
        "pnl_delta_pct": pnl_delta_pct,
        "position_delta": position_delta,
        "fill_count_delta_pct": fill_count_delta_pct,
        "latency_model_delta_ms": latency_model_delta_ms,
        "batch_event_count": batch_fills,
        "paper_event_count": paper_fills,
    }


def _check_deviations(
    metrics: dict[str, float],
    t: BatchPaperThresholds = BATCH_PAPER_THRESHOLDS,
) -> list[DeviationRecord]:
    """Check batch-vs-paper metrics against thresholds."""
    deviations: list[DeviationRecord] = []

    checks = [
        (
            "pnl_delta_pct",
            metrics["pnl_delta_pct"],
            t.pnl_delta_pct_max,
            f"Batch-vs-paper PnL delta {metrics['pnl_delta_pct']:.2%} > {t.pnl_delta_pct_max:.2%}",
        ),
        (
            "position_delta",
            metrics["position_delta"],
            t.position_delta_max,
            (f"Batch-vs-paper position delta {metrics['position_delta']:.2f} units > {t.position_delta_max:.2f}"),
        ),
        (
            "fill_count_delta_pct",
            metrics["fill_count_delta_pct"],
            t.fill_count_delta_pct_max,
            (
                f"Batch-vs-paper fill count delta {metrics['fill_count_delta_pct']:.2%}"
                f" > {t.fill_count_delta_pct_max:.2%}"
            ),
        ),
        (
            "latency_model_delta_ms",
            metrics["latency_model_delta_ms"],
            t.latency_model_delta_ms_max,
            (
                f"Batch-vs-paper latency model delta {metrics['latency_model_delta_ms']:.0f}ms"
                f" > {t.latency_model_delta_ms_max:.0f}ms"
            ),
        ),
    ]

    for name, actual, threshold, desc in checks:
        if actual > threshold:
            deviations.append(
                DeviationRecord(
                    metric_name=name,
                    stage=ReconStage.BATCH_PAPER_RECON,
                    actual_value=actual,
                    threshold=threshold,
                    direction="above",
                    description=desc,
                )
            )

    return deviations


def run_stage3c(config: ReconConfig, date: str, dry_run: bool = False) -> StageReport:
    """Run Stage 3c: Batch-vs-paper execution reconciliation (pvl-p21a)."""
    started_at = datetime.now(UTC)
    log_event("PROCESSING_STARTED", details={"stage": "stage3c_batch_paper_recon", "date": date})
    logger.info("[Stage 3c] Batch-vs-paper reconciliation for date=%s", date)

    if dry_run:
        return StageReport(
            stage=ReconStage.BATCH_PAPER_RECON,
            status=ReconStatus.PASSED,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            metrics={"dry_run": 1.0},
        )

    batch_events = _load_events(config.events_bucket, f"t1-recon/events/{date}/execution-service/")
    paper_events = _load_events(config.events_bucket, f"paper/events/{date}/execution-service/")

    logger.info("[Stage 3c] %d batch, %d paper events", len(batch_events), len(paper_events))

    metrics = _compute_metrics(batch_events, paper_events)
    deviations = _check_deviations(metrics)
    status = ReconStatus.PASSED if not deviations else ReconStatus.FAILED

    for d in deviations:
        logger.warning("[Stage 3c] deviation: %s", d.description)
        log_event(
            "BATCH_PAPER_DEVIATION",
            details={
                "metric": d.metric_name,
                "routing_action": FailureRoutingAction.ALERT.value,
                "actual": d.actual_value,
                "threshold": d.threshold,
                "date": date,
            },
        )

    log_event(
        "PROCESSING_COMPLETED",
        details={
            "stage": "stage3c_batch_paper_recon",
            "status": status.value,
            "deviations": len(deviations),
        },
    )
    return StageReport(
        stage=ReconStage.BATCH_PAPER_RECON,
        status=status,
        started_at=started_at,
        completed_at=datetime.now(UTC),
        deviations=deviations,
        metrics=metrics,
    )
