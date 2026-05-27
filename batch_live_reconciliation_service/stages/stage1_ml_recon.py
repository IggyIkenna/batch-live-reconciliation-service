"""
Stage 1 — ML Reconciliation.

Compares batch T+1 ML inference outputs vs live ML events archived to GCS.

Batch:  t1-recon/ml/{date}/
Live:   live/events/{date}/ml-inference-service/

Deviation metrics (thresholds from deviation_thresholds.py):
  - signal_direction_match_rate  (< 95% → alert)
  - signal_magnitude_mae         (> 0.1 → alert)
  - instrument_coverage_pct      (< 90% → alert)
  - latency_delta_ms             (> 5000ms → alert)
"""

from __future__ import annotations

import json
import logging
import statistics
from datetime import UTC, datetime
from typing import cast

from unified_api_contracts.internal.reconciliation import ReconciliationDimension
from unified_trading_library import get_storage_client, log_event

from batch_live_reconciliation_service.config import ReconConfig
from batch_live_reconciliation_service.models.deviation_thresholds import ML_THRESHOLDS
from batch_live_reconciliation_service.models.recon_report import (
    DeviationRecord,
    ReconStage,
    ReconStatus,
    StageReport,
)

logger = logging.getLogger(__name__)


def _load_events(bucket: str, prefix: str) -> list[dict[str, object]]:
    """Load NDJSON event files from a GCS prefix."""
    client = get_storage_client()
    events: list[dict[str, object]] = []
    try:
        bucket_obj = client.bucket(bucket)
        blobs = list(bucket_obj.list_blobs(prefix=prefix))
        for blob in blobs:
            if not str(blob.name).endswith(".ndjson") and not str(blob.name).endswith(".jsonl"):
                continue
            raw = client.download_bytes(bucket=bucket, blob_path=str(blob.name))
            for line in raw.decode("utf-8").splitlines():
                line = line.strip()
                if line:
                    events.append(cast(dict[str, object], json.loads(line)))
    except (ValueError, TypeError, KeyError, AttributeError, RuntimeError) as e:
        logger.warning("Error loading events from gs://%s/%s: %s", bucket, prefix, e)
    return events


def _inference_latency_ms(ev: dict[str, object]) -> float | None:
    """Extract ``metadata.inference_duration_ms`` from an ML inference event, if numeric.

    Returns None when the event carries no metadata or no numeric duration — callers must
    treat None as "no sample" rather than a zero latency (see ``latency_samples``).
    """
    meta = ev.get("metadata")
    if not isinstance(meta, dict):
        return None
    val = cast("dict[str, object]", meta).get("inference_duration_ms")
    if isinstance(val, bool):  # bool is an int subclass — exclude it explicitly
        return None
    if isinstance(val, (int, float)):
        return float(val)
    return None


def _compute_metrics(
    batch_events: list[dict[str, object]],
    live_events: list[dict[str, object]],
) -> dict[str, float]:
    """Compute ML reconciliation metrics from event lists."""
    if not live_events:
        return {
            "signal_direction_match_rate": 0.0,
            "signal_magnitude_mae": 999.0,
            "instrument_coverage_pct": 0.0,
            "latency_delta_ms": 0.0,
            "latency_samples": 0.0,
            "batch_event_count": float(len(batch_events)),
            "live_event_count": 0.0,
        }

    # Index events by instrument_id + timeframe for comparison
    def _key(ev: dict[str, object]) -> str:
        inst = str(ev.get("instrument_id") or "_missing")
        tf = str(ev.get("timeframe") or "_missing")
        return f"{inst}:{tf}"

    batch_idx: dict[str, dict[str, object]] = {_key(e): e for e in batch_events}
    live_idx: dict[str, dict[str, object]] = {_key(e): e for e in live_events}

    matched_keys = set(batch_idx) & set(live_idx)
    coverage = len(matched_keys) / max(len(live_idx), 1)

    direction_matches = 0
    magnitude_errors: list[float] = []
    latency_deltas: list[float] = []

    for key in matched_keys:
        b = batch_idx[key]
        live_entry = live_idx[key]
        b_dir = int(cast(int, b.get("signal_direction", 0)))
        l_dir = int(cast(int, live_entry.get("signal_direction", 0)))
        if b_dir == l_dir:
            direction_matches += 1
        b_mag = float(cast(float, b.get("magnitude", 0.0)))
        l_mag = float(cast(float, live_entry.get("magnitude", 0.0)))
        magnitude_errors.append(abs(b_mag - l_mag))
        # Per-signal inference-latency delta — only when BOTH sides report a numeric
        # metadata.inference_duration_ms; otherwise the pair contributes no sample.
        b_lat = _inference_latency_ms(b)
        l_lat = _inference_latency_ms(live_entry)
        if b_lat is not None and l_lat is not None:
            latency_deltas.append(abs(b_lat - l_lat))

    direction_match_rate = direction_matches / max(len(matched_keys), 1)
    mae = sum(magnitude_errors) / max(len(magnitude_errors), 1)
    latency_delta = statistics.median(latency_deltas) if latency_deltas else 0.0

    return {
        "signal_direction_match_rate": direction_match_rate,
        "signal_magnitude_mae": mae,
        "instrument_coverage_pct": coverage,
        "latency_delta_ms": latency_delta,
        "latency_samples": float(len(latency_deltas)),
        "batch_event_count": float(len(batch_events)),
        "live_event_count": float(len(live_events)),
    }


def _check_deviations(metrics: dict[str, float]) -> list[DeviationRecord]:
    """Return DeviationRecords for metrics that breach thresholds."""
    deviations: list[DeviationRecord] = []
    t = ML_THRESHOLDS

    if metrics["signal_direction_match_rate"] < t.signal_direction_match_rate_min:
        deviations.append(
            DeviationRecord.new(
                metric_name="signal_direction_match_rate",
                stage=ReconStage.ML_RECON,
                actual_value=metrics["signal_direction_match_rate"],
                threshold=t.signal_direction_match_rate_min,
                direction="below",
                description=(
                    f"Signal direction match rate {metrics['signal_direction_match_rate']:.1%} "
                    f"< {t.signal_direction_match_rate_min:.0%} threshold"
                ),
                dimension=ReconciliationDimension.STRATEGY_LEVEL_ALLOCATION,
            )
        )

    if metrics["signal_magnitude_mae"] > t.signal_magnitude_mae_max:
        deviations.append(
            DeviationRecord.new(
                metric_name="signal_magnitude_mae",
                stage=ReconStage.ML_RECON,
                actual_value=metrics["signal_magnitude_mae"],
                threshold=t.signal_magnitude_mae_max,
                direction="above",
                description=(
                    f"Signal magnitude MAE {metrics['signal_magnitude_mae']:.3f} "
                    f"> {t.signal_magnitude_mae_max} threshold"
                ),
                dimension=ReconciliationDimension.STRATEGY_LEVEL_ALLOCATION,
            )
        )

    if metrics["instrument_coverage_pct"] < t.instrument_coverage_pct_min:
        deviations.append(
            DeviationRecord.new(
                metric_name="instrument_coverage_pct",
                stage=ReconStage.ML_RECON,
                actual_value=metrics["instrument_coverage_pct"],
                threshold=t.instrument_coverage_pct_min,
                direction="below",
                description=(
                    f"Instrument coverage {metrics['instrument_coverage_pct']:.1%} "
                    f"< {t.instrument_coverage_pct_min:.0%} threshold"
                ),
                dimension=ReconciliationDimension.STRATEGY_LEVEL_ALLOCATION,
            )
        )

    # Only flag latency when at least one matched pair actually carried a duration on both
    # sides — a zero metric with zero samples means "no data", not "passed".
    if metrics.get("latency_samples", 0.0) > 0 and metrics["latency_delta_ms"] > t.latency_delta_ms_max:
        deviations.append(
            DeviationRecord.new(
                metric_name="latency_delta_ms",
                stage=ReconStage.ML_RECON,
                actual_value=metrics["latency_delta_ms"],
                threshold=t.latency_delta_ms_max,
                direction="above",
                description=(
                    f"Median latency delta {metrics['latency_delta_ms']:.0f}ms "
                    f"> {t.latency_delta_ms_max:.0f}ms threshold "
                    f"({int(metrics['latency_samples'])} samples)"
                ),
                dimension=ReconciliationDimension.STRATEGY_LEVEL_ALLOCATION,
            )
        )

    return deviations


def run_stage1(config: ReconConfig, date: str, dry_run: bool = False) -> StageReport:
    """
    Run Stage 1: ML Reconciliation.

    Args:
        config: ReconConfig instance
        date: YYYY-MM-DD date string
        dry_run: If True, skip GCS reads

    Returns:
        StageReport for Stage 1
    """
    started_at = datetime.now(UTC)
    log_event("PROCESSING_STARTED", details={"stage": "stage1_ml_recon", "date": date})
    logger.info("[Stage 1] ML reconciliation for date=%s", date)

    if dry_run:
        logger.info("[Stage 1] DRY RUN — skipping event comparison")
        return StageReport(
            stage=ReconStage.ML_RECON,
            status=ReconStatus.PASSED,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            metrics={"dry_run": 1.0},
        )

    batch_prefix = f"t1-recon/events/{date}/ml-inference-service/"
    live_prefix = f"live/events/{date}/ml-inference-service/"

    batch_events = _load_events(config.events_bucket, batch_prefix)
    live_events = _load_events(config.events_bucket, live_prefix)

    logger.info(
        "[Stage 1] Loaded %d batch events, %d live events",
        len(batch_events),
        len(live_events),
    )

    metrics = _compute_metrics(batch_events, live_events)
    deviations = _check_deviations(metrics)

    status = ReconStatus.PASSED if not deviations else ReconStatus.FAILED

    if deviations:
        logger.warning("[Stage 1] %d deviation(s) detected", len(deviations))
        for d in deviations:
            logger.warning("  - %s: %s", d.metric_name, d.description)
    else:
        logger.info("[Stage 1] PASSED — no deviations above threshold")

    log_event(
        "PROCESSING_COMPLETED",
        details={"stage": "stage1_ml_recon", "status": status.value, "deviations": len(deviations)},
    )
    return StageReport(
        stage=ReconStage.ML_RECON,
        status=status,
        started_at=started_at,
        completed_at=datetime.now(UTC),
        deviations=deviations,
        metrics=metrics,
    )
